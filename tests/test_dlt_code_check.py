"""Phase C5 -- the replay-precheck verdict.

Two properties carry the phase, and every test is one of them:

* **Every failure mode is UNKNOWN, and UNKNOWN changes nothing.** Disabled,
  unconfigured, unmappable frame, unreadable repo, unparseable version, no
  baseline -- all of them leave the DLT lane behaving exactly as it did before
  this feature existed.
* **The asymmetries all point the same way.** A wrong `FIX_DEPLOYED` causes a
  replay that fails again; a wrong `NOT_DEPLOYED` only delays one. So the
  highest candidate version is required, the lowest running version is
  compared, and a positive verdict needs a baseline to check Trap T9 against.
"""
import json

import pytest

from src.dlt import bitbucket, code_check

REPO_MAP = json.dumps({
    "com.uidai.enu.biometric": {
        "project": "ENU",
        "repo": "enu-biometric",
        "branch": "release",
        "version_file": "pom.xml",
        "source_roots": ["src/main/java"],
    },
})

FRAME = ("com.uidai.enu.biometric.service.impl."
         "BioDataBaseHelperServiceImpl.getUidOriginTrackerData")
PATH = ("src/main/java/com/uidai/enu/biometric/service/impl/"
        "BioDataBaseHelperServiceImpl.java")

FAILED_AT = 1787019608511          # 2026-08-18T02:20:08.511Z


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("DLT_CODE_CHECK_ENABLED", "DLT_CODE_CHECK_FRAMES",
                "DLT_CODE_CHECK_BRANCH", "DLT_CODE_CHECK_MAX_CANDIDATES",
                "DLT_VERSION_PATTERN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DLT_CODE_CHECK_ENABLED", "true")
    monkeypatch.setenv("BITBUCKET_BASE_URL", "https://bitbucket.example")
    monkeypatch.setenv("BITBUCKET_TOKEN", "read-only")
    monkeypatch.setenv("DLT_REPO_MAP", REPO_MAP)
    bitbucket.reset_cache()
    yield
    bitbucket.reset_cache()


def failure(cls="B", locations=None):
    return {
        "failure_class": cls,
        "frames": [FRAME],
        "locations": locations if locations is not None else [
            {"target": FRAME, "file": "BioDataBaseHelperServiceImpl.java",
             "line": 257},
        ],
    }


class FakeRepo:
    """Stands in for the whole C4 adapter, so C5's decisions are what is under
    test rather than any HTTP behaviour.

    `path`/`commits` give every frame the same file -- the single-frame shape
    most tests want. `paths` (suffix -> repo path) and `commits_by_path` model
    a real call path, where each frame is a different file.
    """

    def __init__(self, monkeypatch, *, path=PATH, commits=(), changes=None,
                 versions=None, bumps=None, paths=None, commits_by_path=None):
        self.path = path
        self.commits = commits
        self.changes = changes if changes is not None else {}
        self.versions = versions or {}
        self.bumps = bumps
        self.paths = paths
        self.commits_by_path = commits_by_path
        self.calls = []

        monkeypatch.setattr(bitbucket, "resolve_path", self._resolve)
        monkeypatch.setattr(bitbucket, "commits_touching", self._commits)
        monkeypatch.setattr(bitbucket, "changed_paths", self._changes)
        monkeypatch.setattr(bitbucket, "version_at", self._version)

    def _resolve(self, repo, suffix):
        if self.paths is not None:
            return self.paths.get(suffix)
        return self.path

    def _commits(self, repo, path, since_ms=None, limit=None):
        self.calls.append(("commits", path))
        if path == repo.version_file:
            return self.bumps
        if self.commits_by_path is not None:
            return self.commits_by_path.get(path, [])
        return self.commits

    def _changes(self, repo, commit_id, limit=500):
        return self.changes.get(commit_id)

    def _version(self, repo, ref=None, version_file=None):
        return self.versions.get(ref)


def commit(cid, subject="Fix the dedupe NPE", ts=FAILED_AT + 1000):
    return bitbucket.Commit(id=cid, subject=subject, timestamp_ms=ts)


# ---------------------------------------------------------------------------
# Everything that must yield UNKNOWN
# ---------------------------------------------------------------------------

def test_the_flag_off_makes_no_calls_at_all(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_ENABLED", "false")
    fake = FakeRepo(monkeypatch, commits=[])

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert "DLT_CODE_CHECK_ENABLED" in result.reason
    assert fake.calls == []


@pytest.mark.parametrize("cls", ["C", "U", None])
def test_classes_whose_treatment_does_not_depend_on_source_are_skipped(monkeypatch, cls):
    fake = FakeRepo(monkeypatch, commits=[])

    result = code_check.evaluate(failure(cls=cls), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert fake.calls == []


def test_no_repository_configured_is_unknown(monkeypatch):
    monkeypatch.delenv("DLT_REPO_MAP")
    assert code_check.evaluate(failure(), FAILED_AT).verdict == code_check.UNKNOWN


def test_a_failure_with_no_locations_is_unknown(monkeypatch):
    FakeRepo(monkeypatch, commits=[])
    result = code_check.evaluate(failure(locations=[]), FAILED_AT)

    assert result.verdict == code_check.UNKNOWN
    assert "source location" in result.reason


def test_an_unmappable_frame_is_unknown(monkeypatch):
    """The shared library case -- Trap T8."""
    FakeRepo(monkeypatch, commits=[])
    shared = [{"target": "in.gov.uidai.common.factory.CommonErrorFactory.build",
               "file": "CommonErrorFactory.java", "line": 25}]

    result = code_check.evaluate(failure(locations=shared), FAILED_AT)

    assert result.verdict == code_check.UNKNOWN
    assert "DLT_REPO_MAP" in result.reason


def test_a_frame_that_resolves_to_no_file_is_unknown(monkeypatch):
    FakeRepo(monkeypatch, path=None, commits=[])

    result = code_check.evaluate(failure(), FAILED_AT)

    assert result.verdict == code_check.UNKNOWN
    assert "not found" in result.reason


def test_an_unreadable_repository_is_unknown_not_no_change(monkeypatch):
    """The distinction that matters most. `None` from the adapter means "we
    could not look"; reading it as "nothing changed" would stop every replay
    in the system during a Bitbucket outage."""
    FakeRepo(monkeypatch, commits=None)

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert "could not be read" in result.reason


def test_a_bug_in_the_check_degrades_instead_of_raising(monkeypatch):
    monkeypatch.setattr(bitbucket, "resolve_path",
                        lambda repo, suffix: (_ for _ in ()).throw(RuntimeError("boom")))

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert "RuntimeError" in result.reason


# ---------------------------------------------------------------------------
# NO_CHANGE -- the cheap, deterministic negative
# ---------------------------------------------------------------------------

def test_nothing_touching_the_failure_site_is_no_change(monkeypatch):
    FakeRepo(monkeypatch, commits=[])

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.5"])

    assert result.verdict == code_check.NO_CHANGE
    assert result.path == PATH
    assert result.blocks_replay is True
    assert "reproduces the same dead letter" in result.reason


def test_no_change_needs_no_version_data_at_all(monkeypatch):
    """The negative verdict works before C1 has ever captured a version."""
    FakeRepo(monkeypatch, commits=[])

    result = code_check.evaluate(failure(), FAILED_AT, [], [])

    assert result.verdict == code_check.NO_CHANGE


# ---------------------------------------------------------------------------
# The version resolution -- Trap T6
# ---------------------------------------------------------------------------

def test_a_fix_commit_that_bumped_the_pom_is_read_directly(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml", PATH]},
             versions={"aaa": "1.0.1"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.2"])

    assert result.verdict == code_check.FIX_DEPLOYED
    assert result.required_version == "1.0.1"
    assert "bumped it" in result.candidates[0]["resolved_by"]


def test_a_fix_commit_that_did_not_bump_walks_forward_to_the_next_one(monkeypatch):
    """Trap T6. Reading the version at the fix commit under a bump-at-release-
    cut convention says a build that PREDATES the fix contains it."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": [PATH]},
             bumps=[commit("ccc", "Release 1.0.2", FAILED_AT + 9000),
                    commit("bbb", "Bump to 1.0.1", FAILED_AT + 5000)],
             versions={"aaa": "1.0.0", "bbb": "1.0.1", "ccc": "1.0.2"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.1"])

    # The OLDEST bump after the fix, not the newest, and not the fix's own.
    assert result.required_version == "1.0.1"
    assert result.verdict == code_check.FIX_DEPLOYED
    assert "bbb" in result.candidates[0]["resolved_by"]


def test_a_fix_with_no_version_cut_since_is_not_deployed(monkeypatch):
    """On the branch, but no build carries it yet."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": [PATH]},
             bumps=[],
             versions={})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.NOT_DEPLOYED
    assert "no version has been cut" in result.reason


def test_candidates_whose_version_cannot_be_resolved_leave_unknown(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": None},
             versions={})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert result.candidates[0]["first_containing_version"] is None


# ---------------------------------------------------------------------------
# The comparison, and its asymmetries
# ---------------------------------------------------------------------------

def test_the_running_build_ahead_of_the_required_version_is_deployed(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.0-release.43"})

    result = code_check.evaluate(failure(), FAILED_AT,
                                 ["1.0.0-release.40"], ["1.0.0-release.45"])

    assert result.verdict == code_check.FIX_DEPLOYED
    assert result.running_version == "1.0.0-release.45"


def test_the_running_build_behind_the_required_version_is_parked(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.0-release.43"})

    result = code_check.evaluate(failure(), FAILED_AT,
                                 ["1.0.0-release.40"], ["1.0.0-release.42"])

    assert result.verdict == code_check.NOT_DEPLOYED
    assert result.parks_replay is True
    assert "1.0.0-release.43" in result.reason


def test_the_highest_candidate_version_is_required_not_the_lowest(monkeypatch):
    """Several commits touched the failure site and we cannot tell which is
    the fix, so waiting for all of them errs toward delay rather than toward a
    replay that fails again."""
    FakeRepo(monkeypatch,
             commits=[commit("ccc", ts=FAILED_AT + 3000),
                      commit("bbb", ts=FAILED_AT + 2000),
                      commit("aaa", ts=FAILED_AT + 1000)],
             changes={"aaa": ["pom.xml"], "bbb": ["pom.xml"], "ccc": ["pom.xml"]},
             versions={"aaa": "1.0.1", "bbb": "1.0.2", "ccc": "1.0.3"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.2"])

    assert result.required_version == "1.0.3"
    assert result.verdict == code_check.NOT_DEPLOYED


def test_the_lowest_running_version_is_compared_mid_rollout(monkeypatch):
    """A replay may land on any pod, so the floor is what matters."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.2"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"],
                                 ["1.0.1", "1.0.3"])

    assert result.running_version == "1.0.1"
    assert result.verdict == code_check.NOT_DEPLOYED


def test_ten_is_not_behind_nine(monkeypatch):
    """Trap T7, end to end. A lexical comparison parks this forever."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.9"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.8"], ["1.0.10"])

    assert result.verdict == code_check.FIX_DEPLOYED


# ---------------------------------------------------------------------------
# Trap T9 -- the 5% with no version bump
# ---------------------------------------------------------------------------

def test_a_version_that_did_not_move_since_the_failure_is_unknown(monkeypatch):
    """The fix landed without a bump, so the version file still reads the
    number already running. Calling that FIX_DEPLOYED causes a replay that
    fails again."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.0"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert "T9" in result.reason


def test_a_positive_verdict_requires_a_baseline_to_check_t9_against(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.1"})

    result = code_check.evaluate(failure(), FAILED_AT, [], ["1.0.2"])

    assert result.verdict == code_check.UNKNOWN
    assert "baseline" in result.reason


def test_a_negative_verdict_does_not_require_a_baseline(monkeypatch):
    """Erring toward delay is safe, so NOT_DEPLOYED stays available."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.5"})

    result = code_check.evaluate(failure(), FAILED_AT, [], ["1.0.2"])

    assert result.verdict == code_check.NOT_DEPLOYED


def test_an_unreadable_running_version_is_unknown(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.1"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], [])

    assert result.verdict == code_check.UNKNOWN
    assert "running version" in result.reason


def test_versions_that_cannot_be_ordered_are_unknown(monkeypatch):
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.1"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["latest"])

    assert result.verdict == code_check.UNKNOWN


# ---------------------------------------------------------------------------
# Frame search
# ---------------------------------------------------------------------------

def test_an_unmappable_frame_does_not_stop_the_search(monkeypatch):
    """A shared-library frame is skipped, and a later mappable one is used."""
    monkeypatch.setenv("DLT_CODE_CHECK_FRAMES", "3")
    unmapped = {"target": "org.other.Helper.run", "file": "Helper.java", "line": 5}
    ours = {"target": FRAME, "file": "BioDataBaseHelperServiceImpl.java", "line": 257}
    FakeRepo(monkeypatch, commits=[])

    result = code_check.evaluate(failure(locations=[unmapped, ours]), FAILED_AT)

    assert result.verdict == code_check.NO_CHANGE
    assert result.frames_examined == 2


def test_the_frame_search_is_bounded(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_FRAMES", "1")
    unmapped = {"target": "org.other.Helper.run", "file": "Helper.java", "line": 5}
    ours = {"target": FRAME, "file": "BioDataBaseHelperServiceImpl.java", "line": 257}
    FakeRepo(monkeypatch, commits=[])

    result = code_check.evaluate(failure(locations=[unmapped, ours]), FAILED_AT)

    assert result.verdict == code_check.UNKNOWN
    assert result.frames_examined == 1


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------

def test_the_verdict_serialises_to_json(monkeypatch):
    FakeRepo(monkeypatch, commits=[])

    payload = json.loads(json.dumps(
        code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.1"]).as_dict()))

    assert payload["verdict"] == code_check.NO_CHANGE
    assert payload["repo"] == "ENU/enu-biometric"
    assert payload["branch"] == "release"
    assert payload["checked_at"] > 0


def test_an_empty_verdict_is_unknown_and_still_serialises():
    """The casebook always carries a code_check block. When the feature is
    off it must read UNKNOWN, not absent -- "we did not look" and "we looked
    and found nothing" must not look alike here either."""
    payload = code_check.CodeCheck().as_dict()

    assert payload["verdict"] == code_check.UNKNOWN
    assert payload["candidates"] == []
    assert payload["required_version"] is None


def test_a_global_branch_override_applies_to_repos_on_the_default(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_BRANCH", "release/2026")
    seen = {}

    def capture(repo, path, since_ms=None, limit=None):
        seen["branch"] = repo.branch
        return []

    monkeypatch.setattr(bitbucket, "resolve_path", lambda repo, suffix: PATH)
    monkeypatch.setattr(bitbucket, "commits_touching", capture)

    result = code_check.evaluate(failure(), FAILED_AT)

    assert seen["branch"] == "release/2026"
    assert result.branch == "release/2026"


# ---------------------------------------------------------------------------
# The whole call path, not just the failure site
# ---------------------------------------------------------------------------
#
# `a()` passes inconsistent data to `b()`, which passes it to `c()`, which
# throws. The exception surfaces in `c()`; the bug -- and the fix -- is in
# `a()`. Checking only the failure site finds no commit and reports a
# confident, false NO_CHANGE that withholds a replay which would now succeed.

PKG = "src/main/java/com/uidai/enu/biometric"
CALL_PATH = [
    {"target": "com.uidai.enu.biometric.ServiceC.c", "file": "ServiceC.java", "line": 90},
    {"target": "com.uidai.enu.biometric.ServiceB.b", "file": "ServiceB.java", "line": 55},
    {"target": "com.uidai.enu.biometric.ServiceA.a", "file": "ServiceA.java", "line": 12},
]
PATHS = {
    "com/uidai/enu/biometric/ServiceC.java": f"{PKG}/ServiceC.java",
    "com/uidai/enu/biometric/ServiceB.java": f"{PKG}/ServiceB.java",
    "com/uidai/enu/biometric/ServiceA.java": f"{PKG}/ServiceA.java",
}


def _call_path_repo(monkeypatch, commits_by_path, **kwargs):
    return FakeRepo(monkeypatch, paths=PATHS, commits_by_path=commits_by_path,
                    **kwargs)


def test_a_fix_in_a_caller_is_found_not_reported_as_no_change(monkeypatch):
    """The bug this exists for. Only ServiceA has a commit; the failure site
    has none."""
    _call_path_repo(
        monkeypatch,
        {f"{PKG}/ServiceC.java": [], f"{PKG}/ServiceB.java": [],
         f"{PKG}/ServiceA.java": [commit("fix123", "Fix data passed down from a()")]},
        changes={"fix123": ["pom.xml"]}, versions={"fix123": "1.0.5"})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.FIX_DEPLOYED
    assert result.frames_examined == 3
    assert result.frames_resolved == 3
    assert len(result.paths_checked) == 3
    assert result.candidates[0]["path"].endswith("ServiceA.java")
    assert "ServiceA.java" in result.reason


def test_a_caller_fix_that_has_not_deployed_is_parked_not_dismissed(monkeypatch):
    _call_path_repo(
        monkeypatch,
        {f"{PKG}/ServiceC.java": [], f"{PKG}/ServiceB.java": [],
         f"{PKG}/ServiceA.java": [commit("fix123")]},
        changes={"fix123": ["pom.xml"]}, versions={"fix123": "1.0.5"})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.1"])

    assert result.verdict == code_check.NOT_DEPLOYED
    assert result.required_version == "1.0.5"


def test_no_change_requires_every_file_on_the_path_to_be_clean(monkeypatch):
    _call_path_repo(monkeypatch, {p: [] for p in PATHS.values()})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.NO_CHANGE
    assert "3 file(s) on this call path" in result.reason


def test_commits_from_several_frames_all_become_candidates(monkeypatch):
    """Two files on the path changed; the HIGHEST version is required."""
    _call_path_repo(
        monkeypatch,
        {f"{PKG}/ServiceC.java": [commit("cfix", "tidy c()")],
         f"{PKG}/ServiceB.java": [],
         f"{PKG}/ServiceA.java": [commit("afix", "the real fix")]},
        changes={"cfix": ["pom.xml"], "afix": ["pom.xml"]},
        versions={"cfix": "1.0.2", "afix": "1.0.7"})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert len(result.candidates) == 2
    assert result.required_version == "1.0.7"


def test_the_same_commit_reached_from_two_frames_is_counted_once(monkeypatch):
    shared = commit("shared1", "one commit touching two files")
    _call_path_repo(
        monkeypatch,
        {f"{PKG}/ServiceC.java": [shared], f"{PKG}/ServiceB.java": [shared],
         f"{PKG}/ServiceA.java": []},
        changes={"shared1": ["pom.xml"]}, versions={"shared1": "1.0.5"})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert len(result.candidates) == 1


def test_two_frames_in_one_file_cost_one_lookup(monkeypatch):
    """The reference sample has four frames inside BioDeDuplicationServiceImpl.
    Asking the same question four times only spends API calls."""
    same_file = [
        {"target": "com.uidai.enu.biometric.ServiceA.a", "file": "ServiceA.java", "line": 12},
        {"target": "com.uidai.enu.biometric.ServiceA.helper", "file": "ServiceA.java", "line": 40},
        {"target": "com.uidai.enu.biometric.ServiceA.outer", "file": "ServiceA.java", "line": 77},
    ]
    fake = _call_path_repo(monkeypatch, {f"{PKG}/ServiceA.java": []})

    result = code_check.evaluate(failure(locations=same_file), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.NO_CHANGE
    assert result.frames_resolved == 3
    assert result.paths_checked == (f"{PKG}/ServiceA.java",)
    assert len([c for c in fake.calls if c[1].endswith("ServiceA.java")]) == 1


# -- coverage is reported, never implied ------------------------------------

def test_no_change_discloses_frames_it_could_not_map(monkeypatch):
    """A shared-library frame is a permanent blind spot (Trap T8). Claiming
    "nothing changed" without saying so would overstate the evidence."""
    with_library = CALL_PATH + [
        {"target": "in.gov.uidai.common.util.Helper.check",
         "file": "Helper.java", "line": 8},
    ]
    _call_path_repo(monkeypatch, {p: [] for p in PATHS.values()})

    result = code_check.evaluate(failure(locations=with_library), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.NO_CHANGE
    assert result.frames_unmapped == 1
    assert "1 frame(s) could not be mapped" in result.reason


def test_a_file_that_could_not_be_read_blocks_the_negative_claim(monkeypatch):
    """`None` from the adapter is "we could not look". One of those anywhere
    on the path makes a NO_CHANGE claim about the path unsupportable."""
    _call_path_repo(
        monkeypatch,
        {f"{PKG}/ServiceC.java": [], f"{PKG}/ServiceB.java": None,
         f"{PKG}/ServiceA.java": []})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.UNKNOWN
    assert "could not be read" in result.reason


def test_the_failure_site_is_still_the_recorded_path(monkeypatch):
    """`path` stays the frame the exception surfaced in, so the casebook field
    keeps its meaning; every file checked is in `paths_checked`."""
    _call_path_repo(monkeypatch, {p: [] for p in PATHS.values()})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT)

    assert result.path == f"{PKG}/ServiceC.java"
    assert result.paths_checked[0] == f"{PKG}/ServiceC.java"


def test_changes_spanning_two_repositories_are_not_compared(monkeypatch):
    """Versions order within one repository, and one running build was read.
    Comparing across repos would be meaningless."""
    monkeypatch.setenv("DLT_REPO_MAP", json.dumps({
        "com.uidai.enu.biometric": {"project": "ENU", "repo": "enu-biometric"},
        "com.uidai.enu.shared": {"project": "ENU", "repo": "enu-shared"},
    }))
    locations = [
        {"target": "com.uidai.enu.biometric.ServiceC.c", "file": "ServiceC.java", "line": 9},
        {"target": "com.uidai.enu.shared.Helper.help", "file": "Helper.java", "line": 4},
    ]
    paths = {"com/uidai/enu/biometric/ServiceC.java": "a/ServiceC.java",
             "com/uidai/enu/shared/Helper.java": "b/Helper.java"}
    FakeRepo(monkeypatch, paths=paths,
             commits_by_path={"a/ServiceC.java": [commit("x")],
                              "b/Helper.java": [commit("y")]},
             changes={"x": ["pom.xml"], "y": ["pom.xml"]},
             versions={"x": "1.0.1", "y": "2.0.1"})

    result = code_check.evaluate(failure(locations=locations), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.UNKNOWN
    assert "2 repositories" in result.reason


def test_the_frame_cap_bounds_the_path_that_is_checked(monkeypatch):
    """With the cap at 1, only the failure site is checked -- which is exactly
    the old behaviour, and exactly why the default is not 1."""
    monkeypatch.setenv("DLT_CODE_CHECK_FRAMES", "1")
    _call_path_repo(
        monkeypatch,
        {f"{PKG}/ServiceC.java": [], f"{PKG}/ServiceB.java": [],
         f"{PKG}/ServiceA.java": [commit("fix123")]},
        changes={"fix123": ["pom.xml"]}, versions={"fix123": "1.0.5"})

    result = code_check.evaluate(failure(locations=CALL_PATH), FAILED_AT,
                                 ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.NO_CHANGE
    assert result.frames_examined == 1


# ---------------------------------------------------------------------------
# An unreadable version must not be reported as a skipped bump
# ---------------------------------------------------------------------------

def test_an_unresolvable_version_is_not_blamed_on_a_skipped_bump(monkeypatch):
    """Trap T12. `version_at` returning an unparseable string used to flow all
    the way to the T9 guard, which reported "a bump was probably skipped" --
    pointing an investigator at a convention problem that does not exist. The
    version file was simply unreadable, and the reason must say so."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={})          # version_at now returns None for `${revision}`

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.9"])

    assert result.verdict == code_check.UNKNOWN
    assert "no version could be resolved" in result.reason
    assert "T9" not in result.reason
    assert "bump was probably skipped" not in result.reason


def test_a_genuinely_skipped_bump_still_reports_t9(monkeypatch):
    """The T9 message must survive for the case it was written for: the
    version read fine, it just did not move."""
    FakeRepo(monkeypatch,
             commits=[commit("aaa")],
             changes={"aaa": ["pom.xml"]},
             versions={"aaa": "1.0.0"})

    result = code_check.evaluate(failure(), FAILED_AT, ["1.0.0"], ["1.0.0"])

    assert result.verdict == code_check.UNKNOWN
    assert "T9" in result.reason
