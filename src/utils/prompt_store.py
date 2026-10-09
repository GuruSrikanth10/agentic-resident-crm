"""
Where the agents' prompt text comes from: PROMPT_SOURCE.

  files     The default. Every prompt is read from its file in the repository,
            exactly as before this module existed.
  langfuse  Every prompt is Langfuse Prompt Management's -- the version that
            carries LANGFUSE_PROMPT_LABEL (`production`) -- and nothing falls
            back to the files. The API refuses to boot unless every prompt
            can be fetched and passes its checks (`activate`). While it runs,
            a version published in Langfuse reaches the next packets within
            LANGFUSE_PROMPT_CACHE_TTL_SECONDS (`refresh`, which rebuilds the
            agents); a prompt that can no longer be fetched or no longer
            passes its checks fails the packets that need it.

Which prompts exist is still the repository's: every prompt file is one
Langfuse prompt named after its path (`catalog`) -- the agents' prompts
(src/prompts), the opencode harness's templates and rules
(src/prompts/harness) and each service pack's text (src/service_packs). Only
the content moves. A prompt file added to the repository must be uploaded
before a langfuse deployment boots: `python -m src.tools.upload_prompts`.
AGENTS.md is not a prompt here: opencode reads it from disk.

The checks: a pack's text passes exactly what a pack file passes
(`service_registry.check_pack_texts`: no packet identifiers, no
instruction-shaped text, the size limits), every prompt is non-empty, and a
harness template's `{{> include}}` names a prompt that exists. A version that
fails them never becomes the one in use.

The prompts in use are one snapshot at a time -- {name: text}, with each
name's version -- replaced whole. A graph is built from one snapshot and
remembers it (`remember`), and a run reads every prompt from its graph's
(`pinned_to`) -- an agent built on first use, a harness template, a service
context, the fingerprint -- so a packet that is running when a version is
published finishes on the versions it started on, and is never composed from
half of one and half of another. Langfuse's SDK caches each prompt for the
TTL and refreshes it in the background, serving the cached version meanwhile,
so `refresh` costs a cache lookup per prompt; an unreachable Langfuse leaves
the last fetched versions in use, never the files. A prompt deleted in
Langfuse, or whose label was removed, is evicted from that cache and then
fails like a missing one.
"""
import contextlib
import contextvars
import hashlib
import os
import re
import threading
import weakref
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from src.utils import paths, service_registry
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

FILES = "files"
LANGFUSE = "langfuse"
SOURCES = (FILES, LANGFUSE)

DEFAULT_LABEL = "production"
DEFAULT_CACHE_TTL_SECONDS = 60

#: The prompt files, by where they live.
PROMPTS_DIR = paths.REPO_ROOT / "src" / "prompts"
HARNESS_DIR = PROMPTS_DIR / "harness"
HARNESS_RULES_DIR = HARNESS_DIR / "rules"

#: A harness template's include directive (prompt_loader).
_INCLUDE = re.compile(r"\{\{>\s*([^}]*?)\s*\}\}")


class PromptUnavailable(RuntimeError):
    """A prompt the service needs under PROMPT_SOURCE=langfuse could not be
    fetched from Langfuse, or failed its checks."""


def source() -> str:
    """PROMPT_SOURCE: `files` (the default) or `langfuse`. Anything else is a
    configuration error -- reported at boot by config_validator -- not a
    reason to guess."""
    value = (os.environ.get("PROMPT_SOURCE") or FILES).strip().lower()
    if value not in SOURCES:
        raise ValueError(f"PROMPT_SOURCE must be one of {', '.join(SOURCES)}; "
                         f"got {value!r}.")
    return value


def label() -> str:
    """LANGFUSE_PROMPT_LABEL: which version of each prompt is used."""
    return (os.environ.get("LANGFUSE_PROMPT_LABEL") or DEFAULT_LABEL).strip()


def cache_ttl_seconds() -> int:
    """LANGFUSE_PROMPT_CACHE_TTL_SECONDS: how long the SDK keeps a fetched
    version before looking for a newer one -- the longest a published edit
    waits to reach the service. An unusable value falls back to the default."""
    try:
        value = int((os.environ.get("LANGFUSE_PROMPT_CACHE_TTL_SECONDS") or "").strip())
    except ValueError:
        return DEFAULT_CACHE_TTL_SECONDS
    return value if value >= 0 else DEFAULT_CACHE_TTL_SECONDS


# ---------------------------------------------------------------------------
# The catalog
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PromptFile:
    """One prompt: its Langfuse name and the file it is uploaded from."""

    name: str
    path: Path
    #: The service pack a pack text belongs to, else None.
    pack: Optional[str] = None


def _kebab(stem: str) -> str:
    """`InvestigatorAgent` -> `investigator-agent`, `learned_rules` ->
    `learned-rules`: Langfuse prompt names are lower-case and hyphenated."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "-", stem).replace("_", "-").lower()


def _build_catalog() -> tuple:
    entries = []
    for path in sorted(PROMPTS_DIR.glob("*.md")):
        entries.append(PromptFile(f"agents/{_kebab(path.stem)}", path.resolve()))
    for path in sorted(HARNESS_DIR.glob("*.md")):
        entries.append(PromptFile(f"harness/{_kebab(path.stem)}", path.resolve()))
    for path in sorted(HARNESS_RULES_DIR.glob("*.md")):
        entries.append(PromptFile(f"harness/rules/{_kebab(path.stem)}", path.resolve()))
    # The packs the registry reads: a directory with a service.json.
    root = service_registry.packs_dir()
    pack_dirs = sorted(root.iterdir(), key=lambda entry: entry.name) if root.is_dir() else []
    for pack_dir in pack_dirs:
        if (not pack_dir.is_dir() or pack_dir.name.startswith((".", "__"))
                or not (pack_dir / service_registry.SERVICE_FILE).is_file()):
            continue
        for filename in service_registry.PACK_TEXT_FILES:
            path = pack_dir / filename
            if path.is_file():
                entries.append(PromptFile(
                    f"packs/{pack_dir.name}/{_kebab(Path(filename).stem)}",
                    path.resolve(), pack=pack_dir.name))

    seen: Dict[str, Path] = {}
    for entry in entries:
        if entry.name in seen:
            raise ValueError(f"{entry.path} and {seen[entry.name]} would both be "
                             f"the Langfuse prompt {entry.name!r}; rename one.")
        seen[entry.name] = entry.path
    return tuple(entries)


_catalog: Optional[tuple] = None
_catalog_lock = threading.Lock()


def catalog() -> tuple:
    """Every prompt, as `PromptFile`s, in a fixed order. Read from the
    repository's layout once and kept."""
    global _catalog
    if _catalog is None:
        with _catalog_lock:
            if _catalog is None:
                _catalog = _build_catalog()
    return _catalog


def _entry_for(path) -> Optional[PromptFile]:
    resolved = Path(path).resolve()
    for entry in catalog():
        if entry.path == resolved:
            return entry
    return None


def is_prompt(path) -> bool:
    """Whether `path` is one of the catalog's prompt files."""
    return _entry_for(path) is not None


# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------

def check_texts(texts: Dict[str, str]) -> list:
    """Every reason `texts` ({prompt name: text}, one per catalog entry) must
    not be used. The same checks whether the text is about to be uploaded
    (upload_prompts) or was just fetched (`_fetch_snapshot`)."""
    errors = []
    harness_paths = {entry.path for entry in catalog()
                     if entry.path.parent in (HARNESS_DIR.resolve(),
                                              HARNESS_RULES_DIR.resolve())}
    by_pack: Dict[str, dict] = {}
    for entry in catalog():
        text = texts.get(entry.name)
        if text is None:
            errors.append(f"{entry.name}: missing")
            continue
        if not text.strip():
            errors.append(f"{entry.name}: empty")
        if entry.pack is not None:
            by_pack.setdefault(entry.pack, {})[entry.path.name] = text
        if entry.path in harness_paths:
            # Resolved as prompt_loader resolves it: a file under the harness
            # directory, which must be a prompt of its own.
            for include in _INCLUDE.findall(text):
                if (HARNESS_DIR / f"{include}.md").resolve() not in harness_paths:
                    errors.append(f"{entry.name}: includes {include!r}, which is "
                                  f"not a harness prompt")
    for pack, pack_texts in sorted(by_pack.items()):
        # Worded as the registry words them (`<pack>/<file>: ...`), led by the
        # Langfuse prompt an error about one file is about.
        files = {f"{pack}/{entry.path.name}": entry.name
                 for entry in catalog() if entry.pack == pack}
        for error in service_registry.check_pack_texts(pack, pack_texts):
            name = next((name for ref, name in files.items()
                         if error.startswith((f"{ref}:", f"{ref} "))), None)
            errors.append(f"{name}: {error}" if name else error)
    return errors


# ---------------------------------------------------------------------------
# The snapshot in use (PROMPT_SOURCE=langfuse)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Snapshot:
    texts: Dict[str, str] = field(default_factory=dict)
    versions: Dict[str, int] = field(default_factory=dict)

    @property
    def revision(self) -> str:
        """Which versions these are: changes exactly when one does."""
        digest = hashlib.sha256()
        for name in sorted(self.versions):
            digest.update(f"{name}\t{self.versions[name]}\n".encode("utf-8"))
        return "langfuse:" + digest.hexdigest()[:16]


_active: Optional[_Snapshot] = None
_snapshot_lock = threading.Lock()

#: The snapshot read in this context in place of the one in use (`pinned`).
#: A context variable rather than a thread-local: LangGraph runs a graph's
#: nodes and an agent's tools on threads of its own, and copies the context
#: into them.
_pinned: contextvars.ContextVar = contextvars.ContextVar("prompt_store_pinned",
                                                         default=None)

#: The snapshot each graph was built from (`remember`), held only as long as
#: the graph is.
_built_from: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def _client():
    """The process's Langfuse client (the one tracing uses, when it is on)."""
    from src.utils import tracing

    return tracing.langfuse_client()


def _fetch_snapshot() -> _Snapshot:
    """Every catalogued prompt's labelled version, unchecked (`_checked`).
    Raises PromptUnavailable naming every prompt that failed, not just the
    first. Served from the SDK's cache when it is fresh, so it is called
    outside `_snapshot_lock`: a fetch that has to reach Langfuse must not make
    every other packet wait behind it."""
    try:
        client = _client()
    except Exception as e:
        raise PromptUnavailable(f"Could not create the Langfuse client: "
                                f"{type(e).__name__}: {e}") from e
    texts, versions, failures = {}, {}, []
    wanted_label, ttl = label(), cache_ttl_seconds()
    for entry in catalog():
        try:
            prompt = client.get_prompt(entry.name, label=wanted_label, type="text",
                                       cache_ttl_seconds=ttl)
        except Exception as e:
            failures.append(f"{entry.name} (label {wanted_label!r}): {type(e).__name__}: {e}")
            continue
        text = getattr(prompt, "prompt", None)
        if getattr(prompt, "is_fallback", False) or not isinstance(text, str):
            failures.append(f"{entry.name}: not a text prompt from Langfuse")
            continue
        texts[entry.name] = text
        versions[entry.name] = int(getattr(prompt, "version", 0) or 0)
    if failures:
        raise PromptUnavailable("Prompts could not be fetched from Langfuse "
                                "(PROMPT_SOURCE=langfuse): " + "; ".join(failures))
    return _Snapshot(texts=texts, versions=versions)


def _checked(snapshot: _Snapshot) -> _Snapshot:
    """`snapshot`, when every text passes `check_texts`; else raises."""
    errors = check_texts(snapshot.texts)
    if errors:
        raise PromptUnavailable("Prompts fetched from Langfuse failed their checks "
                                "(PROMPT_SOURCE=langfuse): " + "; ".join(errors))
    return snapshot


def _swap(snapshot: _Snapshot) -> None:
    """Make `snapshot` the one in use. Caller holds `_snapshot_lock`."""
    global _active
    previous = _active
    _active = snapshot
    if previous is None:
        logger.info("Prompts loaded from Langfuse", label=label(),
                    prompts=len(snapshot.texts), revision=snapshot.revision)
        return
    changed = {name: f"v{previous.versions.get(name)} -> v{version}"
               for name, version in snapshot.versions.items()
               if previous.versions.get(name) != version}
    logger.info("Prompts updated from Langfuse", label=label(), changed=changed,
                revision=snapshot.revision)


def activate() -> None:
    """Fetch and check every prompt, and put them in use. The API's boot step
    under PROMPT_SOURCE=langfuse (main_api.validate_prompt_source); a no-op
    for `files`. Raises PromptUnavailable when any prompt fails."""
    if source() != LANGFUSE:
        return
    snapshot = _checked(_fetch_snapshot())
    with _snapshot_lock:
        _swap(snapshot)


def refresh() -> None:
    """Put the latest published versions in use, when any changed. Called
    where a packet's agents are about to be used (`get_agent`,
    `get_dlt_agent`); the agents are rebuilt when `revision()` moves. Raises
    PromptUnavailable when a prompt can no longer be fetched or a new version
    fails its checks -- the version in use is then kept, and the packet that
    called fails."""
    if source() != LANGFUSE:
        return
    try:
        snapshot = _fetch_snapshot()
        current = _active
        # A version is immutable in Langfuse, so the same versions are the
        # same text: nothing to check or swap. Only a change is checked.
        if current is not None and snapshot.versions == current.versions:
            return
        snapshot = _checked(snapshot)
    except PromptUnavailable as error:
        # Logged here as well as raised: the caller fails a packet, and this
        # says why, naming every prompt at fault.
        logger.error("Prompts from Langfuse cannot be used", label=label(),
                     error=str(error))
        raise
    with _snapshot_lock:
        if _active is None or snapshot.versions != _active.versions:
            _swap(snapshot)


def _snapshot() -> _Snapshot:
    """The snapshot read here: the one `pinned` in this context, else the one
    in use, activating it on first use (a CLI, a test)."""
    snapshot = _pinned.get()
    if snapshot is not None:
        return snapshot
    snapshot = _active
    if snapshot is None:
        with _snapshot_lock:
            if _active is None:
                _swap(_checked(_fetch_snapshot()))
            snapshot = _active
    assert snapshot is not None
    return snapshot


def revision() -> str:
    """Which prompts are read here: `files`, or the Langfuse versions'
    revision. An agent built from one revision is rebuilt when it moves."""
    if source() != LANGFUSE:
        return FILES
    return _snapshot().revision


def current() -> Optional[_Snapshot]:
    """The snapshot read here -- the one `pinned` in this context, else the
    one in use -- or None under PROMPT_SOURCE=files, whose prompts are the
    files."""
    if source() != LANGFUSE:
        return None
    return _snapshot()


@contextlib.contextmanager
def pinned(snapshot: Optional[_Snapshot] = None):
    """Read every prompt in this context from one snapshot -- `snapshot`, else
    the one read here as this begins -- whatever `refresh` puts in use
    meanwhile. Carried into the threads LangGraph runs nodes and tools on. A
    no-op under PROMPT_SOURCE=files."""
    if source() != LANGFUSE:
        yield
        return
    token = _pinned.set(snapshot if snapshot is not None else _snapshot())
    try:
        yield
    finally:
        _pinned.reset(token)


def remember(graph, snapshot: Optional[_Snapshot]) -> None:
    """Note that `graph` was built from `snapshot` (`current()` as it was
    built), for `pinned_to`. Nothing to note for the files."""
    if snapshot is not None:
        _built_from[graph] = snapshot


def pinned_to(graph):
    """`pinned` to the snapshot `graph` was built from (`remember`): a run of
    a graph that has since been rebuilt for newer versions still reads the
    versions its agents were built from. A graph not remembered -- None, or
    one built under PROMPT_SOURCE=files -- pins the snapshot read here."""
    snapshot = None
    if graph is not None:
        try:
            snapshot = _built_from.get(graph)
        except TypeError:
            # Not weakly referenceable, so never remembered.
            snapshot = None
    return pinned(snapshot)


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

def text(path) -> str:
    """The prompt text of the prompt file at `path`: the file's, or under
    PROMPT_SOURCE=langfuse the version read here (`pinned`, else the one in
    use) -- never the file."""
    if source() != LANGFUSE:
        return Path(path).read_text(encoding="utf-8")
    entry = _entry_for(path)
    if entry is None:
        raise PromptUnavailable(f"{path} is not a catalogued prompt file, so it "
                                f"has no Langfuse prompt.")
    return _snapshot().texts[entry.name]


def pack_texts(pack_name: str) -> dict:
    """Pack `pack_name`'s texts read here under PROMPT_SOURCE=langfuse, as
    the registry holds them: {file name: text}."""
    snapshot = _snapshot()
    return {entry.path.name: snapshot.texts[entry.name]
            for entry in catalog() if entry.pack == pack_name}


def link(path) -> Optional[dict]:
    """{"name", "version"} of the prompt at `path` read here, to link to the
    generations it produces in a trace (tracing.invoke); None when the
    prompts are the files', which have no Langfuse version."""
    if source() != LANGFUSE:
        return None
    entry = _entry_for(path)
    if entry is None:
        return None
    version = _snapshot().versions.get(entry.name)
    return {"name": entry.name, "version": version} if version is not None else None


def reset() -> None:
    """Forget the catalog, the snapshot in use and the graphs' snapshots. For
    tests."""
    global _catalog, _active
    with _catalog_lock:
        _catalog = None
    with _snapshot_lock:
        _active = None
    _built_from.clear()
