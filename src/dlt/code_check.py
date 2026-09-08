"""Phase C5 of DLT_PLAN.md section 14 -- the verdict.

Composes C1 (what is running), C2 (where the failure is), C3 (how versions
order) and C4 (what the repository says) into one of four answers about
whether replaying this packet can work:

    NO_CHANGE      nothing on `release` has touched the failure site since
                   this packet failed, so a replay reproduces the dead letter
    NOT_DEPLOYED   something has, but the version carrying it is not running
    FIX_DEPLOYED   something has, and the version carrying it is running
    UNKNOWN        we could not establish any of the above

**The verdict answers deployment, never relevance.** "This commit is running"
is not "this commit fixes your bug". Relevance comes from the frame-to-file
mapping, which says only that *the code at the failure site changed*. The
casebook keeps the two claims separate so a reader can disagree with either.

**Observe-only until an operator says otherwise.** `DLT_CODE_CHECK_ENABLED`
governs whether the lookup happens at all, and it is off by default. Nothing
in this module can withhold or trigger a replay -- that is C6, behind its own
second flag, deliberately not the same switch. Running C5 alone for a few
weeks is what produces the evidence for turning C6 on, which is the posture
Open Question 2 already takes for the mis-cast detector.

**The whole call path is checked, not just the failure site.** An exception
surfaces where the bad data is *used*, which is often several frames below
where it was produced: `a()` passes something inconsistent to `b()`, which
passes it to `c()`, which throws. A developer fixes `a()`. Checking only
`c()` would find no commit and report `NO_CHANGE` -- a confident, false claim
that withholds a replay which would in fact now succeed. So every application
frame up to `DLT_CODE_CHECK_FRAMES` is resolved and queried, and `NO_CHANGE`
is a statement about the *path*, not about one file.

**Coverage is reported, never implied.** Some frames are structurally
invisible -- a shared-library frame is unmapped by design (Trap T8) -- so
`NO_CHANGE` names how many files it actually checked out of how many frames.
A frame that could not be *read* is different from one that cannot be
*mapped*: the first is transient and blocks the negative claim entirely, the
second is permanent and is merely disclosed.

**Three asymmetries, all in the same direction.** A wrong `FIX_DEPLOYED`
causes a replay that fails again; a wrong `NOT_DEPLOYED` only delays one. So:

* the *highest* candidate version is required, not the lowest -- if several
  commits touched the call path we cannot tell which is the fix, and
  waiting for all of them errs toward delay;
* the *lowest* running version is compared, not the highest -- mid-rollout a
  replay may land on any pod;
* a positive verdict needs a baseline to check Trap T9 against, while
  `NOT_DEPLOYED` does not.
"""
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from src.dlt import bitbucket, versions
from src.dlt.stacktrace import FrameLocation
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

NO_CHANGE = "NO_CHANGE"
NOT_DEPLOYED = "NOT_DEPLOYED"
FIX_DEPLOYED = "FIX_DEPLOYED"
UNKNOWN = "UNKNOWN"

VERDICTS = (NO_CHANGE, NOT_DEPLOYED, FIX_DEPLOYED, UNKNOWN)

#: Classes whose treatment can depend on source at all. C's recommendation is
#: a redrive once the dependency recovers, which no commit changes, and U has
#: no parseable frame to anchor on.
CHECKED_CLASSES = ("A", "B")

#: How many application frames of the call path to check. The reference
#: sample carries 8 after normalisation; 5 covers a realistic caller chain
#: without turning one trace into a repository crawl.
DEFAULT_FRAMES = 5

#: How many candidate commits to resolve to a version. Each costs two reads,
#: both cached; the cap stops a file with a busy week from fanning out.
DEFAULT_MAX_CANDIDATES = 10


@dataclass(frozen=True)
class CodeCheck:
    """One verdict, carrying the evidence it rests on."""

    verdict: str = UNKNOWN
    reason: str = ""
    repo: Optional[str] = None
    path: Optional[str] = None
    branch: Optional[str] = None
    #: Every file actually checked, in call-path order.
    paths_checked: tuple = ()
    #: Frames that resolved to a file, and frames that could not be mapped.
    #: Reported rather than implied: an unmapped shared-library frame is a
    #: permanent blind spot in any `NO_CHANGE` claim (Trap T8).
    frames_resolved: int = 0
    frames_unmapped: int = 0
    #: What was running when the packet failed (lowest, across pods).
    baseline_version: Optional[str] = None
    #: What is running now (lowest, across pods).
    running_version: Optional[str] = None
    #: The earliest version that carries every candidate change.
    required_version: Optional[str] = None
    #: Commits at the failure site since the packet failed, with the version
    #: each first shipped in.
    candidates: tuple = ()
    frames_examined: int = 0
    checked_at: float = 0.0
    details: dict = field(default_factory=dict)

    @property
    def blocks_replay(self) -> bool:
        """Only C6 acts on this, and only when its own flag is on."""
        return self.verdict == NO_CHANGE

    @property
    def parks_replay(self) -> bool:
        return self.verdict == NOT_DEPLOYED

    def as_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "reason": self.reason,
            "repo": self.repo,
            "path": self.path,
            "paths_checked": list(self.paths_checked),
            "frames_resolved": self.frames_resolved,
            "frames_unmapped": self.frames_unmapped,
            "branch": self.branch,
            "baseline_version": self.baseline_version,
            "running_version": self.running_version,
            "required_version": self.required_version,
            "candidates": [dict(c) for c in self.candidates],
            "frames_examined": self.frames_examined,
            "checked_at": self.checked_at,
            **({"details": self.details} if self.details else {}),
        }


def _unknown(reason: str, **extra) -> CodeCheck:
    return CodeCheck(verdict=UNKNOWN, reason=reason, checked_at=time.time(), **extra)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def enabled() -> bool:
    """Whether the lookup runs at all. Off by default."""
    return get_bool_env("DLT_CODE_CHECK_ENABLED", False)


def frames_to_check() -> int:
    """How many application frames of the call path to check.

    **Every one of them is queried, not just the first that resolves.** The
    top frame is where the exception surfaced; the bug is frequently in a
    caller further down, and a fix there leaves the failure site untouched.
    Distinct files are deduplicated first, so the four frames the reference
    sample has inside `BioDeDuplicationServiceImpl` cost one lookup, not four.
    """
    try:
        return max(1, int(os.environ.get("DLT_CODE_CHECK_FRAMES",
                                         str(DEFAULT_FRAMES))))
    except (ValueError, TypeError):
        return DEFAULT_FRAMES


def max_candidates() -> int:
    try:
        return max(1, int(os.environ.get("DLT_CODE_CHECK_MAX_CANDIDATES",
                                         str(DEFAULT_MAX_CANDIDATES))))
    except (ValueError, TypeError):
        return DEFAULT_MAX_CANDIDATES


def branch_override() -> Optional[str]:
    """A global branch override, for the case where every repo shares one.
    Per-repo `branch` in `DLT_REPO_MAP` still wins."""
    value = os.environ.get("DLT_CODE_CHECK_BRANCH", "").strip()
    return value or None


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------

def _locations(failure: dict) -> list:
    """Rebuild `FrameLocation`s from the failure record's `locations` list."""
    out = []
    for entry in (failure or {}).get("locations") or []:
        if not isinstance(entry, dict) or not entry.get("target"):
            continue
        out.append(FrameLocation(target=entry["target"],
                                 file=entry.get("file"),
                                 line=entry.get("line")))
    return out


@dataclass
class FrameProbe:
    """One frame of the call path, and what the repository said about it."""

    target: str
    #: The `bitbucket.Repo` this frame maps to, when it maps to one.
    repo: object = None
    path: Optional[str] = None
    #: None means the repository could not be read -- distinct from [], which
    #: means it answered and nothing has touched this file.
    commits: Optional[list] = None
    #: Why the frame could not be mapped, when it could not.
    reason: str = ""

    @property
    def mapped(self) -> bool:
        return self.path is not None

    @property
    def unreadable(self) -> bool:
        return self.mapped and self.commits is None


def _resolve_frame(location: FrameLocation):
    """(repo, path) for one frame, or (None, why)."""
    repo = bitbucket.repo_for(location.class_fqcn)
    if repo is None:
        return None, f"{location.class_fqcn} is not in DLT_REPO_MAP"

    override = branch_override()
    if override and repo.branch == bitbucket.DEFAULT_BRANCH:
        repo = bitbucket.Repo(project=repo.project, repo=repo.repo,
                              branch=override, version_file=repo.version_file,
                              source_roots=repo.source_roots, prefix=repo.prefix)

    path = bitbucket.resolve_path(repo, location.source_path_suffix)
    if path is None:
        return None, f"{location.source_path_suffix} not found in {repo.slug}"
    return (repo, path), ""


def _probe_path(locations, failed_at_ms) -> list:
    """Resolve and query every frame of the call path, in order.

    Distinct *files* are queried once: the reference sample has four frames
    inside `BioDeDuplicationServiceImpl`, and asking the same question four
    times would only spend API calls to get the same answer.
    """
    probes, seen = [], {}
    for location in locations:
        found, reason = _resolve_frame(location)
        if not found:
            probes.append(FrameProbe(target=location.target, reason=reason))
            continue

        repo, path = found
        if path in seen:
            # A different method in a file already checked. Carry the same
            # answer so the frame is still counted as covered.
            probes.append(FrameProbe(target=location.target, repo=repo,
                                     path=path, commits=seen[path].commits))
            continue

        commits = bitbucket.commits_touching(repo, path, since_ms=failed_at_ms)
        probe = FrameProbe(target=location.target, repo=repo, path=path,
                           commits=commits)
        seen[path] = probe
        probes.append(probe)

    return probes


def _first_containing_version(repo, commit) -> tuple:
    """The earliest version that carries `commit`, and how it was determined.

    **Trap T6.** Under a bump-in-the-fix-commit convention the version at the
    fix commit is already the answer. Under a bump-at-release-cut convention
    it is the *previous* version, and reading it there would say a build that
    predates the fix contains it -- systematically wrong, in the direction
    that causes replays which fail again.

    So the commit's own change set is checked first, and only when it did not
    touch the version file does this walk forward to the next commit on the
    branch that did. That is correct under either convention, which is why it
    is built regardless of what C0 reports.
    """
    changed = bitbucket.changed_paths(repo, commit.id)
    if changed is None:
        return None, "the commit's change set could not be read"

    if bitbucket.touches_version_file(changed, repo.version_file):
        version = bitbucket.version_at(repo, commit.id)
        if version is None:
            return None, "the version file could not be read at the fix commit"
        return version, "read at the fix commit, which bumped it"

    bumps = bitbucket.commits_touching(repo, repo.version_file,
                                       since_ms=commit.timestamp_ms)
    if bumps is None:
        return None, "later version bumps could not be read"
    if not bumps:
        # On the branch, but nothing has been cut since. No build carries it.
        return "", "no version has been cut since this commit"

    # Newest first, so the oldest entry is the first bump after the commit.
    oldest = bumps[-1]
    version = bitbucket.version_at(repo, oldest.id)
    if version is None:
        return None, "the version file could not be read at the next bump"
    return version, f"read at {oldest.short}, the next bump after this commit"


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------

def evaluate(failure: dict,
             failed_at_ms: Optional[int],
             baseline_versions=(),
             running_versions=()) -> CodeCheck:
    """Decide whether the code running now can handle this packet.

    Total: every failure mode returns a `CodeCheck`, never an exception. A
    verdict this cannot establish is `UNKNOWN`, which leaves the DLT lane
    behaving exactly as it did before this feature existed.
    """
    try:
        return _evaluate(failure, failed_at_ms, baseline_versions, running_versions)
    except Exception as exc:
        # A bug here must degrade a verdict, never cost a case.
        logger.error("Code check raised; treating the verdict as unknown",
                     error=f"{type(exc).__name__}: {exc}")
        return _unknown(f"the code check failed: {type(exc).__name__}")


def _evaluate(failure: dict, failed_at_ms, baseline_versions,
              running_versions) -> CodeCheck:
    if not enabled():
        return _unknown("DLT_CODE_CHECK_ENABLED is off")

    failure_class = (failure or {}).get("failure_class")
    if failure_class not in CHECKED_CLASSES:
        return _unknown(
            f"class {failure_class} does not depend on source: its treatment "
            f"is fixed regardless of what the repository says")

    if not bitbucket.configured():
        return _unknown("Bitbucket is not configured, or no repository is "
                        "mapped in DLT_REPO_MAP")

    locations = _locations(failure)
    if not locations:
        return _unknown("no application frame carried a source location")

    # -- probe the whole call path, not just the failure site ------------
    probes = _probe_path(locations[:frames_to_check()], failed_at_ms)
    mapped = [p for p in probes if p.mapped]
    unmapped = [p for p in probes if not p.mapped]

    base = {"frames_examined": len(probes),
            "frames_resolved": len(mapped),
            "frames_unmapped": len(unmapped)}

    if not mapped:
        return _unknown(
            "no frame could be mapped to a file in a known repository: "
            + "; ".join(p.reason for p in unmapped[:3]),
            **base)

    # Distinct files, in call-path order. The failure site is first.
    paths, seen = [], set()
    for probe in mapped:
        if probe.path not in seen:
            seen.add(probe.path)
            paths.append(probe.path)

    site = mapped[0]
    base.update(repo=site.repo.slug, path=site.path, branch=site.repo.branch,
                paths_checked=tuple(paths))

    # A file we could not READ is not a file with no commits. One transient
    # failure anywhere on the path makes a negative claim about the path
    # unsupportable, so it is UNKNOWN rather than a confident NO_CHANGE.
    unreadable = [p for p in mapped if p.unreadable]
    if unreadable:
        return _unknown(
            f"{len({p.path for p in unreadable})} of the {len(paths)} file(s) on "
            f"the call path could not be read", **base)

    baseline = versions.lowest(baseline_versions) if baseline_versions else None
    running = versions.lowest(running_versions) if running_versions else None
    base.update(baseline_version=str(baseline) if baseline else None,
                running_version=str(running) if running else None)

    with_commits = [p for p in mapped if p.commits]

    if not with_commits:
        # The cheap, deterministic negative -- and the only verdict that needs
        # no version data at all. The reason states coverage rather than
        # implying it: an unmapped shared-library frame is a permanent blind
        # spot, and a reader must be able to see it (Trap T8).
        blind = (f", though {len(unmapped)} frame(s) could not be mapped to a "
                 f"repository and were not checked" if unmapped else "")
        return CodeCheck(
            verdict=NO_CHANGE,
            reason=(f"nothing on {site.repo.branch} has touched any of the "
                    f"{len(paths)} file(s) on this call path since the packet "
                    f"failed{blind}, so a replay reproduces the same dead letter"),
            checked_at=time.time(), **base)

    # -- what version carries them? --------------------------------------
    # Versions only order within one repository, and the running version we
    # read belongs to one service. A call path spanning two mapped repos has
    # no single comparable version, so say so rather than compare across them.
    repos = {p.repo.slug for p in with_commits}
    if len(repos) > 1:
        return _unknown(
            f"changes were found in {len(repos)} repositories "
            f"({', '.join(sorted(repos))}); their versions are not comparable "
            f"against one running build", **base)

    repo = with_commits[0].repo
    candidates, required = [], None
    seen_commits = set()

    for probe in with_commits:
        for commit in (probe.commits or [])[:max_candidates()]:
            if commit.id in seen_commits:
                continue
            seen_commits.add(commit.id)

            version, how = _first_containing_version(repo, commit)
            candidates.append({
                "commit": commit.short,
                "subject": commit.subject[:120],
                "path": probe.path,
                "timestamp_ms": commit.timestamp_ms,
                "first_containing_version": version or None,
                "resolved_by": how,
            })
            if version is None:
                continue
            if version == "":
                # On the branch but never cut. Nothing running can contain it.
                required = required or ""
                continue
            if required in (None, "") or (versions.compare(version, required) or 0) > 0:
                # The HIGHEST, not the lowest: several commits touched the
                # call path and we cannot tell which is the fix, so waiting
                # for all of them errs toward delay rather than toward a
                # replay that fails.
                required = version

    base["candidates"] = tuple(candidates)

    if required == "":
        return CodeCheck(
            verdict=NOT_DEPLOYED,
            reason=(f"{len(candidates)} commit(s) have touched this call path "
                    f"since the packet failed, but no version has been cut "
                    f"since, so nothing running can contain them"),
            checked_at=time.time(), **base)

    if required is None:
        return _unknown(
            f"{len(candidates)} commit(s) touched this call path, but no "
            f"version could be resolved for any of them",
            **base)

    base["required_version"] = str(required)

    if running is None:
        return _unknown(
            "the running version could not be read, so it cannot be compared "
            f"against {required}", **base)

    # -- Trap T9 ---------------------------------------------------------
    # A fix merged without a version bump leaves the version file reading the
    # number already running. `at_least` would call that deployed and cause a
    # replay that fails again, so a positive verdict requires the version to
    # have actually moved since the failing build.
    if baseline is not None and versions.is_ahead(required, baseline) is not True:
        return _unknown(
            f"the version did not move between the failing build ({baseline}) "
            f"and the change ({required}); a bump was probably skipped, so the "
            f"version carries no signal here (Trap T9)", **base)

    deployed_now = versions.at_least(running, required)
    if deployed_now is None:
        return _unknown(
            f"{running} and {required} cannot be ordered", **base)

    if deployed_now:
        if baseline is None:
            # Without a baseline the T9 guard above never ran, so a positive
            # claim would be unguarded. NOT_DEPLOYED stays available because
            # erring toward delay is safe; FIX_DEPLOYED does not.
            return _unknown(
                f"{running} is at or beyond {required}, but no baseline "
                f"version was recorded for the failing build, so a skipped "
                f"bump could not be ruled out (Trap T9)", **base)
        changed = ", ".join(sorted({c["path"].rsplit("/", 1)[-1]
                                    for c in candidates}))
        return CodeCheck(
            verdict=FIX_DEPLOYED,
            reason=(f"{changed} changed on {repo.branch} after this packet "
                    f"failed, and the running build ({running}) is at or "
                    f"beyond the version carrying it ({required})"),
            checked_at=time.time(), **base)

    changed = ", ".join(sorted({c["path"].rsplit("/", 1)[-1] for c in candidates}))
    return CodeCheck(
        verdict=NOT_DEPLOYED,
        reason=(f"{changed} changed on {repo.branch} after this packet failed, "
                f"but the running build ({running}) is behind the version "
                f"carrying it ({required}); replay once the pods reach {required}"),
        checked_at=time.time(), **base)
