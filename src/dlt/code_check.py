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

**Three asymmetries, all in the same direction.** A wrong `FIX_DEPLOYED`
causes a replay that fails again; a wrong `NOT_DEPLOYED` only delays one. So:

* the *highest* candidate version is required, not the lowest -- if several
  commits touched the failure site we cannot tell which is the fix, and
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

DEFAULT_FRAMES = 3

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
    """How far up the stack to look for a changed file.

    The top application frame is the failure site, but a fix is often one
    frame up -- in the caller that passed the null. Searching a few and taking
    the first that maps to a repository covers that without turning a
    9-frame trace into 9 repository lookups.
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

    # -- find a frame we can look up -------------------------------------
    resolved, why_not = None, []
    examined = 0
    for location in locations[:frames_to_check()]:
        examined += 1
        found, reason = _resolve_frame(location)
        if found:
            resolved = found
            break
        why_not.append(reason)

    if resolved is None:
        return _unknown(
            "no frame could be mapped to a file in a known repository: "
            + "; ".join(why_not[:3]),
            frames_examined=examined)

    repo, path = resolved
    base = dict(repo=repo.slug, path=path, branch=repo.branch,
                frames_examined=examined)

    # -- has anything touched it? ----------------------------------------
    commits = bitbucket.commits_touching(repo, path, since_ms=failed_at_ms)
    if commits is None:
        return _unknown("the repository could not be read", **base)

    baseline = versions.lowest(baseline_versions) if baseline_versions else None
    running = versions.lowest(running_versions) if running_versions else None
    base.update(baseline_version=str(baseline) if baseline else None,
                running_version=str(running) if running else None)

    if not commits:
        # The cheap, deterministic negative -- and the only verdict that needs
        # no version data at all.
        return CodeCheck(
            verdict=NO_CHANGE,
            reason=(f"nothing on {repo.branch} has touched {path} since this "
                    f"packet failed, so a replay reproduces the same dead letter"),
            checked_at=time.time(), **base)

    # -- what version carries them? --------------------------------------
    candidates, required = [], None
    for commit in commits[:max_candidates()]:
        version, how = _first_containing_version(repo, commit)
        candidates.append({
            "commit": commit.short,
            "subject": commit.subject[:120],
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
            # The HIGHEST, not the lowest: several commits touched the failure
            # site and we cannot tell which is the fix, so waiting for all of
            # them errs toward delay rather than toward a replay that fails.
            required = version

    base["candidates"] = tuple(candidates)

    if required == "":
        return CodeCheck(
            verdict=NOT_DEPLOYED,
            reason=(f"{len(candidates)} commit(s) have touched {path} since this "
                    f"packet failed, but no version has been cut since, so "
                    f"nothing running can contain them"),
            checked_at=time.time(), **base)

    if required is None:
        return _unknown(
            f"{len(candidates)} commit(s) touched {path}, but no version "
            f"could be resolved for any of them",
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
        return CodeCheck(
            verdict=FIX_DEPLOYED,
            reason=(f"{path} changed on {repo.branch} after this packet failed, "
                    f"and the running build ({running}) is at or beyond the "
                    f"version carrying it ({required})"),
            checked_at=time.time(), **base)

    return CodeCheck(
        verdict=NOT_DEPLOYED,
        reason=(f"{path} changed on {repo.branch} after this packet failed, but "
                f"the running build ({running}) is behind the version carrying "
                f"it ({required}); replay once the pods reach {required}"),
        checked_at=time.time(), **base)
