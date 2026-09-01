"""Phase C4 of DLT_PLAN.md section 14 -- read-only access to the source repo.

Four questions, and deliberately nothing else:

    which file does this stack frame live in?          resolve_path
    what has touched that file on `release` since T?   commits_touching
    what did the version file say at commit X?         version_at
    did commit X itself change the version file?       changed_paths

**Nothing here writes.** No branch, no comment, no build trigger. The token
this uses is read-only and scoped to the repositories named in
`DLT_REPO_MAP`; anything beyond those four reads is out of scope by design,
because a source-control integration that can only read is one an operator can
reason about without auditing it.

**Nothing here raises into the analysis lane.** Every failure -- unreachable
server, 401, 404, timeout, tripped breaker, malformed response -- comes back
as None. A repository we cannot read means an `UNKNOWN` verdict, which costs a
verdict; an exception escaping into `/analyze-dlt` would cost the case.

**None and [] mean different things**, and the difference decides a replay.
`[]` is "the server answered, and nothing has touched this file", which C5
turns into `NO_CHANGE` and a withheld replay. `None` is "we could not look".
Collapsing them would let an unreachable Bitbucket read as "the code
definitely has not changed" and stop every replay in the system on no evidence
at all -- the same distinction `FetchResult.ok` and
`Corroboration.could_not_look` already make in the log lane. For the same
reason, a None is never cached: holding a server outage for a whole TTL would
keep returning `UNKNOWN` long after the server came back.

**Both API flavours.** Bitbucket Server/Data Center (`/rest/api/1.0`) and
Cloud (`/2.0`) have different paths, different pagination and different
timestamp encodings. C0 answers which one is in front of us; until it does,
`BITBUCKET_API_FLAVOUR` selects it explicitly and detection is the fallback.

**Path resolution prefers construction over search.** Given
`com.uidai.enu.biometric.service.impl.BioDeDuplicationServiceImpl` the path
suffix is already known from the frame (phase C2), so the first strategy is
simply to try each configured source root and see whether the file exists.
That is one call, works identically on both flavours, and handles a
multi-module layout as a config change. Listing the whole repository and
matching the suffix is the fallback for a repo nobody has configured roots
for, and it is Server-only -- Cloud has no comparable recursive listing.
"""
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Optional
from xml.etree import ElementTree

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

FLAVOUR_SERVER = "server"
FLAVOUR_CLOUD = "cloud"

DEFAULT_BRANCH = "release"
DEFAULT_VERSION_FILE = "pom.xml"
DEFAULT_SOURCE_ROOTS = ("src/main/java",)

DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_COMMIT_LIMIT = 50
#: An hour. Every read here is keyed on something that repeats within a
#: fingerprint group -- a repo, a path, a commit id -- so the TTL is what
#: actually bounds API traffic at 2,000 messages/day. A commit landing on
#: `release` does not need sub-hour detection for this purpose.
DEFAULT_CACHE_TTL_SECONDS = 3600.0

#: Ceiling on a fetched file. A pom is kilobytes; anything approaching this is
#: not a version file, and parsing it would only spend memory to learn that.
MAX_FILE_BYTES = 2 * 1024 * 1024

#: Ceiling on a repository listing, for the fallback path strategy.
MAX_LISTED_FILES = 20000

_lock = threading.Lock()
_cache: dict = {}
_session = None


# ---------------------------------------------------------------------------
# Shapes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Repo:
    """One mapped repository. Built from `DLT_REPO_MAP`, never inferred."""

    project: str
    repo: str
    branch: str = DEFAULT_BRANCH
    version_file: str = DEFAULT_VERSION_FILE
    #: Candidate source roots, tried in order. A multi-module layout lists
    #: each module's root here rather than needing code.
    source_roots: tuple = DEFAULT_SOURCE_ROOTS
    #: The package prefix this entry was matched on, for the audit trail.
    prefix: str = ""

    @property
    def slug(self) -> str:
        return f"{self.project}/{self.repo}"


@dataclass(frozen=True)
class Commit:
    id: str
    subject: str = ""
    #: Epoch milliseconds, normalised across both flavours.
    timestamp_ms: Optional[int] = None

    @property
    def short(self) -> str:
        return self.id[:10] if self.id else ""

    def as_dict(self) -> dict:
        return {"id": self.id, "short": self.short, "subject": self.subject,
                "timestamp_ms": self.timestamp_ms}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def base_url() -> str:
    return os.environ.get("BITBUCKET_BASE_URL", "").strip().rstrip("/")


def _token() -> str:
    return os.environ.get("BITBUCKET_TOKEN", "").strip()


def _username() -> str:
    return os.environ.get("BITBUCKET_USERNAME", "").strip()


def flavour() -> str:
    """Which API dialect to speak. Explicit config wins; the host is a hint."""
    configured = os.environ.get("BITBUCKET_API_FLAVOUR", "").strip().lower()
    if configured in (FLAVOUR_SERVER, FLAVOUR_CLOUD):
        return configured
    return FLAVOUR_CLOUD if "bitbucket.org" in base_url() else FLAVOUR_SERVER


def timeout_seconds() -> float:
    try:
        return max(1.0, float(os.environ.get("BITBUCKET_TIMEOUT_SECONDS",
                                             str(DEFAULT_TIMEOUT_SECONDS))))
    except (ValueError, TypeError):
        return DEFAULT_TIMEOUT_SECONDS


def commit_limit() -> int:
    try:
        return max(1, int(os.environ.get("DLT_CODE_CHECK_MAX_COMMITS",
                                         str(DEFAULT_COMMIT_LIMIT))))
    except (ValueError, TypeError):
        return DEFAULT_COMMIT_LIMIT


def cache_ttl_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get("DLT_CODE_CHECK_TTL_SECONDS",
                                             str(DEFAULT_CACHE_TTL_SECONDS))))
    except (ValueError, TypeError):
        return DEFAULT_CACHE_TTL_SECONDS


def repo_map() -> dict:
    """Package prefix -> repository spec, from `DLT_REPO_MAP`.

    A malformed map yields no repositories rather than a partial one: half a
    mapping would resolve some frames and silently mis-resolve others.
    """
    raw = os.environ.get("DLT_REPO_MAP", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError) as exc:
        logger.warning("DLT_REPO_MAP is not valid JSON; no repositories are "
                       "mapped", error=str(exc))
        return {}
    if not isinstance(parsed, dict):
        logger.warning("DLT_REPO_MAP must be a JSON object; ignoring")
        return {}
    return parsed


def configured() -> bool:
    """Is there enough config to attempt a read at all?"""
    return bool(base_url() and _token() and repo_map())


def repo_for(class_fqcn: Optional[str]) -> Optional[Repo]:
    """The repository holding `class_fqcn`, by longest matching prefix.

    None for an unmapped package -- notably `in.gov.uidai.common`, the shared
    library. A fix there is a dependency version bump in the service's pom,
    not a commit on the service's `release` branch, so resolving it against
    the service repo would be wrong rather than merely unhelpful (Trap T8).
    """
    if not class_fqcn:
        return None

    best_prefix, best_spec = None, None
    for prefix, spec in repo_map().items():
        if not isinstance(spec, dict) or not class_fqcn.startswith(prefix):
            continue
        if best_prefix is None or len(prefix) > len(best_prefix):
            best_prefix, best_spec = prefix, spec

    if best_spec is None:
        return None

    project = str(best_spec.get("project") or "").strip()
    name = str(best_spec.get("repo") or "").strip()
    if not project or not name:
        logger.warning("DLT_REPO_MAP entry is missing project or repo; ignoring",
                       prefix=best_prefix)
        return None

    roots = best_spec.get("source_roots") or best_spec.get("source_root")
    if isinstance(roots, str):
        roots = [roots]
    roots = tuple(str(r).strip("/") for r in (roots or DEFAULT_SOURCE_ROOTS) if r)

    return Repo(
        project=project,
        repo=name,
        branch=str(best_spec.get("branch") or DEFAULT_BRANCH),
        version_file=str(best_spec.get("version_file") or DEFAULT_VERSION_FILE),
        source_roots=roots or DEFAULT_SOURCE_ROOTS,
        prefix=best_prefix or "",
    )


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

def _get_session():
    global _session
    if _session is None:
        import requests
        _session = requests.Session()
    return _session


def _request(path: str, params=None, raw: bool = False):
    """One GET. Returns parsed JSON (or text when `raw`), or None.

    Wrapped by the caller in `bitbucket_breaker`; this layer converts every
    failure into None so a 404 -- which is a legitimate "that file is not
    there" answer -- is not counted as a breaker failure.
    """
    url = f"{base_url()}{path}"
    token, username = _token(), _username()

    headers = {"Authorization": f"Bearer {token}"} if token and not username else {}
    auth = (username, token) if username and token else None

    try:
        response = _get_session().get(url, headers=headers, auth=auth,
                                      params=params or {},
                                      timeout=timeout_seconds())
    except Exception as exc:
        logger.warning("Bitbucket request failed", path=path,
                       error=f"{type(exc).__name__}: {exc}")
        raise

    if response.status_code == 404:
        return None
    if response.status_code >= 400:
        logger.warning("Bitbucket returned an error", path=path,
                       status=response.status_code)
        return None

    if raw:
        content = response.content or b""
        if len(content) > MAX_FILE_BYTES:
            logger.warning("Bitbucket file exceeds the size ceiling; ignoring",
                           path=path, bytes=len(content))
            return None
        return response.text

    try:
        return response.json()
    except ValueError:
        logger.warning("Bitbucket response was not JSON", path=path)
        return None


def _call(path: str, params=None, raw: bool = False):
    """`_request` behind the circuit breaker, degrading to None."""
    if not base_url():
        return None
    try:
        from src.utils.resilience import bitbucket_breaker, retry_transient
        return bitbucket_breaker.call(retry_transient(_request), path, params, raw)
    except Exception as exc:
        # Includes CircuitBreakerError and exhausted retries. "Could not look"
        # is an unknown verdict, never an exception into the analysis lane.
        logger.warning("Bitbucket read abandoned", path=path,
                       error=f"{type(exc).__name__}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

def _cached(key: tuple, produce):
    ttl = cache_ttl_seconds()
    if ttl <= 0:
        return produce()

    now = time.monotonic()
    with _lock:
        entry = _cache.get(key)
        if entry is not None and now - entry[0] < ttl:
            return entry[1]

    value = produce()

    # A None is not cached: it usually means the server was unreachable, and
    # holding that for a whole TTL would keep returning UNKNOWN long after the
    # server came back.
    if value is not None:
        with _lock:
            _cache[key] = (now, value)
    return value


def reset_cache() -> None:
    global _session
    with _lock:
        _cache.clear()
        _session = None


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def _iso_to_ms(text) -> Optional[int]:
    """Cloud returns ISO-8601; Server returns epoch millis already."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return int(text)
    value = str(text).strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    try:
        from datetime import datetime
        normalised = value.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalised).timestamp() * 1000)
    except (ValueError, TypeError):
        return None


def commits_touching(repo: Repo, path: str,
                     since_ms: Optional[int] = None,
                     limit: Optional[int] = None) -> Optional[list]:
    """Commits on the repo's branch that touched `path`, newest first.

    **None and [] mean different things, and the difference decides a replay.**
    `[]` is "the server answered, and nothing has touched this file" -- which
    C5 turns into `NO_CHANGE` and a withheld replay. `None` is "we could not
    look", which must become `UNKNOWN` instead. Collapsing the two would let
    an unreachable Bitbucket read as "the code definitely has not changed",
    stopping every replay in the system on no evidence at all. This is the
    same distinction `FetchResult.ok` and `Corroboration.could_not_look`
    already make in the log lane.

    `since_ms` filters to commits at or after that instant. The filter is by
    commit timestamp, which is an approximation of "is this in the build that
    failed" -- a commit authored before the packet failed was very probably
    already running when it did. It bounds the search; the version comparison
    in C5 is what actually decides deployment.
    """
    if not repo or not path:
        return None

    count = limit or commit_limit()

    def produce():
        if flavour() == FLAVOUR_CLOUD:
            data = _call(
                f"/2.0/repositories/{repo.project}/{repo.repo}/commits/{repo.branch}",
                {"path": path, "pagelen": min(count, 100)})
            if data is None:
                return None
            return [
                Commit(id=v.get("hash") or "",
                       subject=((v.get("message") or "").splitlines() or [""])[0],
                       timestamp_ms=_iso_to_ms(v.get("date")))
                for v in (data.get("values") or [])
            ]

        data = _call(
            f"/rest/api/1.0/projects/{repo.project}/repos/{repo.repo}/commits",
            {"until": repo.branch, "path": path, "limit": count})
        if data is None:
            return None
        return [
            Commit(id=v.get("id") or "",
                   subject=((v.get("message") or "").splitlines() or [""])[0],
                   timestamp_ms=_iso_to_ms(v.get("authorTimestamp")))
            for v in (data.get("values") or [])
        ]

    commits = _cached(("commits", repo.slug, repo.branch, path, count), produce)
    if commits is None:
        return None

    if since_ms is None:
        return list(commits)
    # A commit with no readable timestamp is kept rather than dropped: losing
    # it could turn a real NOT_DEPLOYED into a NO_CHANGE, which is the verdict
    # that stops a replay.
    return [c for c in commits
            if c.timestamp_ms is None or c.timestamp_ms >= since_ms]


def changed_paths(repo: Repo, commit_id: str,
                  limit: int = 500) -> Optional[list]:
    """Every path one commit touched, or None when it could not be read.

    Used to ask whether the fix commit itself bumped the version file
    (Trap T6). None matters here for the same reason as in `commits_touching`:
    an unreadable diff must not read as "this commit changed nothing".
    """
    if not repo or not commit_id:
        return None

    def produce():
        if flavour() == FLAVOUR_CLOUD:
            data = _call(
                f"/2.0/repositories/{repo.project}/{repo.repo}/diffstat/{commit_id}",
                {"pagelen": min(limit, 100)})
            if data is None:
                return None
            paths = []
            for entry in data.get("values") or []:
                for side in ("new", "old"):
                    node = entry.get(side) or {}
                    if node.get("path"):
                        paths.append(node["path"])
            return sorted(set(paths))

        data = _call(
            f"/rest/api/1.0/projects/{repo.project}/repos/{repo.repo}"
            f"/commits/{commit_id}/changes", {"limit": limit})
        if data is None:
            return None
        paths = []
        for entry in data.get("values") or []:
            path = (entry.get("path") or {}).get("toString")
            if path:
                paths.append(path)
        return sorted(set(paths))

    return _cached(("changes", repo.slug, commit_id), produce)


def file_at(repo: Repo, path: str, ref: Optional[str] = None) -> Optional[str]:
    """Raw file content at `ref` (a commit id or a branch), or None."""
    if not repo or not path:
        return None
    at = ref or repo.branch

    def produce():
        if flavour() == FLAVOUR_CLOUD:
            return _call(
                f"/2.0/repositories/{repo.project}/{repo.repo}/src/{at}/{path}",
                raw=True)
        return _call(
            f"/rest/api/1.0/projects/{repo.project}/repos/{repo.repo}/raw/{path}",
            {"at": at}, raw=True)

    return _cached(("file", repo.slug, path, at), produce)


def list_files(repo: Repo) -> list:
    """Every path in the repository at its branch. Server only.

    Cloud has no comparable recursive listing, so a Cloud repository must
    configure `source_roots` -- which is the better answer on both flavours
    anyway, and is why this is only the fallback.
    """
    if not repo or flavour() == FLAVOUR_CLOUD:
        return []

    def produce():
        paths, start = [], 0
        while len(paths) < MAX_LISTED_FILES:
            data = _call(
                f"/rest/api/1.0/projects/{repo.project}/repos/{repo.repo}/files",
                {"at": repo.branch, "limit": 1000, "start": start})
            if not data:
                break
            paths.extend(data.get("values") or [])
            if data.get("isLastPage", True):
                break
            start = data.get("nextPageStart") or 0
        return paths

    return _cached(("files", repo.slug, repo.branch), produce) or []


def resolve_path(repo: Repo, path_suffix: str) -> Optional[str]:
    """The repository path for a frame's `source_path_suffix`, or None.

    Construction first: each configured source root is tried, and the first
    that actually holds the file wins. One call in the common case, identical
    on both flavours, and a multi-module layout is a config change.

    Search second, and Server-only: list the repository and keep paths ending
    in the suffix. **Exactly one match resolves.** Several means two modules
    declare the same class, and picking either would attribute a commit to the
    wrong one -- so ambiguity is `UNKNOWN`, not a coin toss.
    """
    if not repo or not path_suffix:
        return None

    for root in repo.source_roots:
        candidate = f"{root}/{path_suffix}" if root else path_suffix
        if file_at(repo, candidate) is not None:
            return candidate

    matches = [p for p in list_files(repo) if p.endswith(path_suffix)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        logger.warning("Frame resolves to several files; treating as unknown",
                       repo=repo.slug, suffix=path_suffix, matches=matches[:5])
    return None


# ---------------------------------------------------------------------------
# Version files
# ---------------------------------------------------------------------------

def _local_name(tag: str) -> str:
    """`{http://maven.apache.org/POM/4.0.0}version` -> `version`."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def parse_pom_version(text: Optional[str]) -> Optional[str]:
    """The project's own version from a pom.

    **Trap T5.** A pom declares `<parent><version>` *before* its own
    `<version>`, so a regex for the first `<version>` tag returns the parent's
    -- a different, slower-moving number. The XML is parsed and
    `/project/version` read specifically, falling back to
    `/project/parent/version` only when the project declares none, which is
    the case Maven's own inheritance rule covers.
    """
    if not text or not text.strip():
        return None

    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError as exc:
        logger.warning("pom.xml did not parse", error=str(exc))
        return None

    parent_version = None
    for child in root:
        name = _local_name(child.tag)
        if name == "version" and (child.text or "").strip():
            return child.text.strip()
        if name == "parent":
            for grandchild in child:
                if _local_name(grandchild.tag) == "version":
                    parent_version = (grandchild.text or "").strip() or None

    return parent_version


def parse_package_version(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    try:
        value = json.loads(text).get("version")
    except (ValueError, AttributeError, TypeError):
        return None
    return str(value).strip() or None if value is not None else None


def version_at(repo: Repo, ref: Optional[str] = None,
               version_file: Optional[str] = None) -> Optional[str]:
    """The version declared in the repo's version file at `ref`, or None."""
    if not repo:
        return None
    name = version_file or repo.version_file
    text = file_at(repo, name, ref)
    if text is None:
        return None
    if name.endswith(".json"):
        return parse_package_version(text)
    return parse_pom_version(text)


#: `pom.xml` at any depth, so a nested module's pom counts as the version file.
def touches_version_file(paths, version_file: str) -> bool:
    """Did this commit touch the version file? Matched on the basename, since
    changed paths are full paths and a module pom sits under a directory."""
    target = (version_file or "").rsplit("/", 1)[-1]
    if not target:
        return False
    return any(str(p).rsplit("/", 1)[-1] == target for p in paths or ())


#: Kept for callers that want to sanity-check a branch name before using it in
#: a URL path. Bitbucket accepts slashes in branch names, so this is
#: deliberately permissive about `/` and strict about everything odd.
_BRANCH_RE = re.compile(r"^[\w.\-/]+$")


def is_usable_branch(name: Optional[str]) -> bool:
    return bool(name and _BRANCH_RE.match(name))
