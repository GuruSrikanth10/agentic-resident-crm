#!/usr/bin/env python3
"""Phase C0 of the replay-precheck plan -- the feasibility gate.

The precheck design rests on five assumptions about systems this repository
has never talked to. Every one of them is cheap to check and expensive to get
wrong, so they are checked *before* any of C4's adapter code is written:

  1. Is Bitbucket reachable, and is it Server/Data Center or Cloud? The REST
     paths differ entirely, and C4 has to pick one.
  2. Is `release` the branch production is built from, in each mapped repo?
  3. What does an image tag actually look like, and can it be ordered?
  4. **Is the version bumped in the fix commit, or at release cut?** This one
     decides whether C5 may read the version at the fix commit directly or
     must walk forward to the next version-changing commit (Trap T6). Reading
     it directly under a release-cut convention is systematically wrong, in
     the direction that causes replays which fail again.
  5. Do the service repos use multi-module Maven layouts, and which pom is the
     image tagged from?

This is a throwaway. It ships in `src/tools/` because that is where this
project keeps operator CLIs, but nothing imports it, it is not wired into the
API or a consumer, and it carries its own minimal Bitbucket client rather than
depending on C4 -- which does not exist yet, and whose shape this tool's
output is meant to determine.

Usage:

    python -m src.tools.code_check_probe --all \\
        --repo ENU/enu-biometric --branch release

    python -m src.tools.code_check_probe --bumps \\
        --repo ENU/enu-biometric --sample 40

    python -m src.tools.code_check_probe --layout \\
        --repo ENU/enu-biometric \\
        --class com.uidai.enu.biometric.service.impl.BioDeDuplicationServiceImpl

Environment:

    BITBUCKET_BASE_URL    e.g. https://bitbucket.uidai.net.in
    BITBUCKET_TOKEN       a read-only personal access token
    BITBUCKET_USERNAME    only for Cloud app passwords (Basic auth)
    BITBUCKET_FLAVOUR     force `server` or `cloud`; otherwise detected
"""
import argparse
import json
import os
import re
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

FLAVOUR_SERVER = "server"
FLAVOUR_CLOUD = "cloud"

#: Commit subjects that look like a bug fix rather than a release chore. Used
#: to separate "did the fix bump the version" from "did the release commit
#: bump the version", which is the whole point of probe 4.
_FIX_SUBJECT = re.compile(
    r"\b(fix|fixes|fixed|bug|defect|hotfix|patch|issue|resolve[sd]?|npe|error)\b",
    re.IGNORECASE,
)

#: Commit subjects that are plainly a version/release chore.
_RELEASE_SUBJECT = re.compile(
    r"\b(bump|release|version|prepare|tag|snapshot)\b",
    re.IGNORECASE,
)

DEFAULT_VERSION_FILES = ("pom.xml", "package.json")


# ---------------------------------------------------------------------------
# A minimal, self-contained Bitbucket reader
# ---------------------------------------------------------------------------

class Bitbucket:
    """Just enough of both APIs to answer the five questions. Read-only."""

    def __init__(self, base_url: str, token: str, username: str = "",
                 flavour: str = ""):
        self.base = (base_url or "").rstrip("/")
        self.token = token or ""
        self.username = username or ""
        self.flavour = flavour or ""
        self.session = requests.Session()
        self.calls = 0

    # -- transport ---------------------------------------------------------

    def _headers(self) -> dict:
        if self.token and not self.username:
            return {"Authorization": f"Bearer {self.token}"}
        return {}

    def _auth(self):
        # Cloud app passwords are Basic auth; Server PATs are Bearer.
        if self.username and self.token:
            return (self.username, self.token)
        return None

    def get(self, path: str, params=None, raw: bool = False):
        """Returns parsed JSON, or raw text when `raw`. None on any failure.

        Never raises: this tool's job is to report what is reachable, and an
        exception halfway through would lose the findings already gathered.
        """
        url = f"{self.base}{path}"
        self.calls += 1
        try:
            response = self.session.get(
                url, headers=self._headers(), auth=self._auth(),
                params=params or {}, timeout=20,
            )
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"

        if response.status_code >= 400:
            return None, f"HTTP {response.status_code}"
        if raw:
            return response.text, None
        try:
            return response.json(), None
        except ValueError:
            return None, "response was not JSON"

    # -- flavour -----------------------------------------------------------

    def detect_flavour(self) -> tuple:
        """(flavour, evidence). Server exposes /application-properties."""
        if self.flavour:
            return self.flavour, "forced by BITBUCKET_FLAVOUR"

        data, _ = self.get("/rest/api/1.0/application-properties")
        if isinstance(data, dict) and data.get("version"):
            self.flavour = FLAVOUR_SERVER
            return FLAVOUR_SERVER, f"Bitbucket Server {data['version']}"

        data, _ = self.get("/2.0/user")
        if isinstance(data, dict):
            self.flavour = FLAVOUR_CLOUD
            return FLAVOUR_CLOUD, "responded to the Cloud /2.0 API"

        if "bitbucket.org" in self.base:
            self.flavour = FLAVOUR_CLOUD
            return FLAVOUR_CLOUD, "host is bitbucket.org (unauthenticated guess)"

        return "", "could not determine"

    # -- reads -------------------------------------------------------------

    def branches(self, project: str, repo: str, filter_text: str = "") -> list:
        if self.flavour == FLAVOUR_CLOUD:
            data, _ = self.get(f"/2.0/repositories/{project}/{repo}/refs/branches",
                               {"q": f'name~"{filter_text}"'} if filter_text else None)
            return [v.get("name") for v in (data or {}).get("values", [])]

        data, _ = self.get(f"/rest/api/1.0/projects/{project}/repos/{repo}/branches",
                           {"filterText": filter_text, "limit": 50})
        return [v.get("displayId") for v in (data or {}).get("values", [])]

    def commits(self, project: str, repo: str, branch: str,
                path: str = "", limit: int = 50) -> list:
        """Newest first. Normalised to {id, subject, timestamp_ms}."""
        if self.flavour == FLAVOUR_CLOUD:
            params = {"pagelen": min(limit, 100)}
            if path:
                params["path"] = path
            data, _ = self.get(f"/2.0/repositories/{project}/{repo}/commits/{branch}",
                               params)
            return [
                {"id": v.get("hash"),
                 "subject": (v.get("message") or "").splitlines()[0] if v.get("message") else "",
                 "date": v.get("date")}
                for v in (data or {}).get("values", [])
            ]

        params = {"until": branch, "limit": limit}
        if path:
            params["path"] = path
        data, _ = self.get(f"/rest/api/1.0/projects/{project}/repos/{repo}/commits",
                           params)
        return [
            {"id": v.get("id"),
             "subject": (v.get("message") or "").splitlines()[0] if v.get("message") else "",
             "date": v.get("authorTimestamp")}
            for v in (data or {}).get("values", [])
        ]

    def changed_paths(self, project: str, repo: str, commit: str,
                      limit: int = 500) -> list:
        if self.flavour == FLAVOUR_CLOUD:
            data, _ = self.get(f"/2.0/repositories/{project}/{repo}/diffstat/{commit}",
                               {"pagelen": min(limit, 100)})
            paths = []
            for entry in (data or {}).get("values", []):
                for side in ("new", "old"):
                    node = entry.get(side) or {}
                    if node.get("path"):
                        paths.append(node["path"])
            return sorted(set(paths))

        data, _ = self.get(
            f"/rest/api/1.0/projects/{project}/repos/{repo}/commits/{commit}/changes",
            {"limit": limit})
        paths = []
        for entry in (data or {}).get("values", []):
            path = (entry.get("path") or {}).get("toString")
            if path:
                paths.append(path)
        return sorted(set(paths))

    def file_at(self, project: str, repo: str, path: str, ref: str):
        if self.flavour == FLAVOUR_CLOUD:
            text, err = self.get(f"/2.0/repositories/{project}/{repo}/src/{ref}/{path}",
                                 raw=True)
            return text, err
        return self.get(f"/rest/api/1.0/projects/{project}/repos/{repo}/raw/{path}",
                        {"at": ref}, raw=True)

    def list_files(self, project: str, repo: str, ref: str, limit: int = 2000) -> list:
        """Every file path in the repo at `ref`. Server only; Cloud paginates
        a different way and this probe does not need it there."""
        if self.flavour == FLAVOUR_CLOUD:
            return []
        paths, start = [], 0
        while len(paths) < limit:
            data, _ = self.get(f"/rest/api/1.0/projects/{project}/repos/{repo}/files",
                               {"at": ref, "limit": 1000, "start": start})
            if not data:
                break
            paths.extend(data.get("values") or [])
            if data.get("isLastPage", True):
                break
            start = data.get("nextPageStart") or 0
        return paths


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------

def probe_reachability(bb: Bitbucket) -> dict:
    """Q1 -- is Bitbucket reachable, and which API does it speak?"""
    if not bb.base:
        return {"ok": False, "detail": "BITBUCKET_BASE_URL is not set"}
    flavour, evidence = bb.detect_flavour()
    return {
        "ok": bool(flavour),
        "flavour": flavour or "unknown",
        "detail": evidence,
        "base_url": bb.base,
        "auth": "basic" if bb.username else ("bearer" if bb.token else "none"),
    }


def probe_branch(bb: Bitbucket, project: str, repo: str, branch: str) -> dict:
    """Q2 -- does the configured branch exist, and what else looks like it?"""
    exact = bb.branches(project, repo, branch)
    if exact is None:
        return {"ok": False, "detail": "branch listing failed"}
    return {
        "ok": branch in exact,
        "requested": branch,
        "matches": exact[:20],
        "detail": ("present" if branch in exact
                   else f"'{branch}' not found among {len(exact)} similar names"),
    }


def probe_images(app: str, namespace: str) -> dict:
    """Q3 -- what do the running image references actually look like?

    Reaches into `discovery` for pod listing rather than re-implementing
    namespace resolution and the fixture seam. C1 promotes this to a real,
    public helper; a probe borrowing it is fine.
    """
    try:
        from src.log_pipeline.sources.k8s import discovery
    except Exception as e:
        return {"ok": False, "detail": f"kubernetes support unavailable: {e}"}

    resolved_ns, match_spec = discovery.resolve_service(app, namespace)
    if not resolved_ns:
        return {"ok": False, "detail": "no namespace resolved; set K8S_DEFAULT_NAMESPACE"}

    try:
        pods = discovery._list_pods(resolved_ns, match_spec, 15.0)
    except Exception as e:
        return {"ok": False, "detail": f"{type(e).__name__}: {e}"}

    if not pods:
        return {"ok": False, "namespace": resolved_ns,
                "detail": f"no pods matched {match_spec.describe()}"}

    images = []
    for pod in pods:
        statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
        for status in statuses:
            images.append({
                "pod": pod.metadata.name,
                "container": status.name,
                "image": status.image,
                "image_id": status.image_id,
            })

    tags = sorted({_tag_of(i["image"]) for i in images if i.get("image")})
    return {
        "ok": bool(images),
        "namespace": resolved_ns,
        "pods": len(pods),
        "images": images[:20],
        "distinct_tags": tags,
        "detail": (f"{len(tags)} distinct tag(s) across {len(pods)} pod(s)"
                   + ("  -- ROLLING DEPLOY: take the LOWEST" if len(tags) > 1 else "")),
    }


def _tag_of(reference: str) -> str:
    """`host/ns/name/1.0.0-release.4` or `host/ns/name:1.0.0` -> the version part."""
    if not reference:
        return ""
    head = reference.split("@", 1)[0]
    if ":" in head.rsplit("/", 1)[-1]:
        return head.rsplit(":", 1)[-1]
    return head.rsplit("/", 1)[-1]


def probe_bumps(bb: Bitbucket, project: str, repo: str, branch: str,
                version_file: str, sample: int) -> dict:
    """Q4 -- is the version bumped in the fix commit, or at release cut?

    The measurement that matters: of the commits that look like *fixes*, what
    share also touched the version file? High means the version at the fix
    commit is already the first-containing version. Low, with separate
    release-chore commits doing the bumping, means C5 must walk forward.
    """
    commits = bb.commits(project, repo, branch, limit=sample)
    if not commits:
        return {"ok": False, "detail": "no commits returned"}

    fixes_total = fixes_bumping = 0
    chores_total = chores_bumping = 0
    examples = []

    for commit in commits:
        subject = commit.get("subject") or ""
        paths = bb.changed_paths(project, repo, commit["id"])
        touched = any(p.rsplit("/", 1)[-1] == version_file for p in paths)

        is_chore = bool(_RELEASE_SUBJECT.search(subject))
        is_fix = bool(_FIX_SUBJECT.search(subject)) and not is_chore

        if is_fix:
            fixes_total += 1
            fixes_bumping += 1 if touched else 0
            if len(examples) < 8:
                examples.append({
                    "commit": (commit["id"] or "")[:10],
                    "subject": subject[:90],
                    "bumped_version_file": touched,
                })
        elif is_chore:
            chores_total += 1
            chores_bumping += 1 if touched else 0

    share = (fixes_bumping / fixes_total) if fixes_total else None
    if share is None:
        verdict = "INCONCLUSIVE -- no commit in the sample looked like a fix"
    elif share >= 0.8:
        verdict = "IN-FIX-COMMIT -- reading the version at the fix commit is sound"
    elif share <= 0.2:
        verdict = "AT-RELEASE-CUT -- C5 MUST walk forward (Trap T6)"
    else:
        verdict = "MIXED -- build the forward-walk; it is correct either way"

    return {
        "ok": True,
        "sampled": len(commits),
        "version_file": version_file,
        "fix_commits": fixes_total,
        "fix_commits_touching_version_file": fixes_bumping,
        "share": None if share is None else round(share, 3),
        "release_chore_commits": chores_total,
        "release_chores_touching_version_file": chores_bumping,
        "examples": examples,
        "detail": verdict,
    }


def probe_layout(bb: Bitbucket, project: str, repo: str, branch: str,
                 class_fqcn: str) -> dict:
    """Q5 -- multi-module layout, and can a class FQCN be resolved to a path?"""
    paths = bb.list_files(project, repo, branch)
    if not paths:
        return {"ok": False,
                "detail": "file listing unavailable (Cloud, or the call failed)"}

    poms = sorted(p for p in paths if p.rsplit("/", 1)[-1] == "pom.xml")
    roots = sorted({p[: p.index("src/main/java")] or "<root>"
                    for p in paths if "src/main/java" in p})

    result = {
        "ok": True,
        "files_seen": len(paths),
        "pom_count": len(poms),
        "poms": poms[:12],
        "source_roots": roots[:12],
        "multi_module": len(poms) > 1,
    }

    if class_fqcn:
        # Strip inner classes and lambda suffixes before mapping to a file.
        outer = class_fqcn.split("$", 1)[0]
        suffix = outer.replace(".", "/") + ".java"
        hits = [p for p in paths if p.endswith(suffix)]
        result["class"] = class_fqcn
        result["resolved"] = hits
        result["resolvable"] = len(hits) == 1
        result["detail"] = (
            f"{len(hits)} path(s) match {suffix}"
            + ("" if len(hits) == 1 else "  -- AMBIGUOUS or MISSING")
        )
    else:
        result["detail"] = f"{len(poms)} pom(s), {len(roots)} source root(s)"

    return result


def probe_version_file(bb: Bitbucket, project: str, repo: str, branch: str,
                       version_file: str) -> dict:
    """Supporting check for Trap T5 -- does the pom declare a parent version
    before its own? A regex for the first <version> would read the wrong one.
    """
    text, err = bb.file_at(project, repo, version_file, branch)
    if not text:
        return {"ok": False, "detail": err or "not found"}

    if version_file.endswith(".json"):
        try:
            return {"ok": True, "version": json.loads(text).get("version"),
                    "detail": "read from package.json"}
        except ValueError:
            return {"ok": False, "detail": "package.json did not parse"}

    versions = re.findall(r"<version>\s*([^<\s]+)\s*</version>", text)
    has_parent = "<parent>" in text
    return {
        "ok": True,
        "first_version_tag": versions[0] if versions else None,
        "version_tags_seen": versions[:6],
        "declares_parent": has_parent,
        "detail": ("PARENT BLOCK PRESENT -- a naive first-<version> regex reads "
                   "the parent's version (Trap T5); C4 must parse the XML"
                   if has_parent else "no <parent> block in this pom"),
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _emit(number: int, question: str, result: dict) -> None:
    mark = "OK  " if result.get("ok") else "FAIL"
    print(f"\n[{mark}] Q{number}. {question}")
    print(f"       {result.get('detail', '')}")
    for key, value in result.items():
        if key in ("ok", "detail"):
            continue
        if isinstance(value, list) and value and isinstance(value[0], dict):
            print(f"       {key}:")
            for item in value:
                print(f"         - {json.dumps(item, ensure_ascii=False)}")
        else:
            print(f"       {key}: {json.dumps(value, ensure_ascii=False)}")


def _split_repo(spec: str) -> tuple:
    if "/" not in (spec or ""):
        raise SystemExit("--repo must be PROJECT/REPO (Server) or WORKSPACE/REPO (Cloud)")
    project, repo = spec.split("/", 1)
    return project, repo


def main() -> int:
    parser = argparse.ArgumentParser(
        description="C0 feasibility gate for the DLT replay precheck.")
    parser.add_argument("--repo", help="PROJECT/REPO or WORKSPACE/REPO")
    parser.add_argument("--branch", default="release")
    parser.add_argument("--class", dest="class_fqcn", default="",
                        help="a class FQCN from a real stack trace frame")
    parser.add_argument("--version-file", default="pom.xml",
                        choices=list(DEFAULT_VERSION_FILES))
    parser.add_argument("--app", default="", help="k8s app name (default K8S_DEFAULT_APP)")
    parser.add_argument("--namespace", default="")
    parser.add_argument("--sample", type=int, default=40,
                        help="commits to sample for the bump-convention probe")

    parser.add_argument("--all", action="store_true")
    parser.add_argument("--reach", action="store_true")
    parser.add_argument("--branches", action="store_true")
    parser.add_argument("--images", action="store_true")
    parser.add_argument("--bumps", action="store_true")
    parser.add_argument("--layout", action="store_true")
    args = parser.parse_args()

    selected = any([args.reach, args.branches, args.images, args.bumps, args.layout])
    if not selected and not args.all:
        parser.error("choose at least one probe, or --all")
    run_all = args.all or not selected

    bb = Bitbucket(
        base_url=os.environ.get("BITBUCKET_BASE_URL", ""),
        token=os.environ.get("BITBUCKET_TOKEN", ""),
        username=os.environ.get("BITBUCKET_USERNAME", ""),
        flavour=os.environ.get("BITBUCKET_FLAVOUR", ""),
    )

    print("=" * 72)
    print("DLT replay precheck -- C0 feasibility gate")
    print("=" * 72)

    needs_bitbucket = run_all or args.reach or args.branches or args.bumps or args.layout
    reachable = False

    if needs_bitbucket:
        result = probe_reachability(bb)
        reachable = result["ok"]
        _emit(1, "Is Bitbucket reachable, and which API does it speak?", result)

    if (run_all or args.branches) and reachable and args.repo:
        project, repo = _split_repo(args.repo)
        _emit(2, f"Does branch '{args.branch}' exist in {args.repo}?",
              probe_branch(bb, project, repo, args.branch))

    if run_all or args.images:
        _emit(3, "What do the running image references look like?",
              probe_images(args.app, args.namespace))

    if (run_all or args.bumps) and reachable and args.repo:
        project, repo = _split_repo(args.repo)
        _emit(4, "Is the version bumped in the fix commit, or at release cut?",
              probe_bumps(bb, project, repo, args.branch, args.version_file, args.sample))
        _emit(5, f"Does {args.version_file} declare a parent version first?",
              probe_version_file(bb, project, repo, args.branch, args.version_file))

    if (run_all or args.layout) and reachable and args.repo:
        project, repo = _split_repo(args.repo)
        _emit(6, "Multi-module layout, and can a frame resolve to one file?",
              probe_layout(bb, project, repo, args.branch, args.class_fqcn))

    if needs_bitbucket and not reachable:
        print("\nBitbucket was unreachable, so every repository probe was skipped.")
        print("Set BITBUCKET_BASE_URL and BITBUCKET_TOKEN, then re-run.")

    print(f"\n{'=' * 72}")
    print(f"{bb.calls} Bitbucket call(s) made. Record these answers in "
          f"DLT_PLAN.md before building C4.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
