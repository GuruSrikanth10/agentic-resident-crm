"""Phase C0 -- the parts of the feasibility probe that are pure logic.

The probe itself is throwaway and talks to systems this suite cannot reach.
What is tested here is the handful of decisions it makes *about* what it
reads, because those decisions are the ones that get copied forward into C3
and C4:

* pulling a version out of a Harbor image reference (C3 inherits this),
* telling a fix commit apart from a release chore (the Q4 measurement is
  worthless if this misclassifies), and
* noticing a `<parent>` block in a pom (Trap T5, which C4 must not repeat).
"""
import pytest

from src.tools.code_check_probe import (
    _FIX_SUBJECT,
    _RELEASE_SUBJECT,
    _split_repo,
    _tag_of,
    probe_bumps,
    probe_version_file,
)


# ---------------------------------------------------------------------------
# Image references
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reference,expected", [
    # The shape the operator described: the version is the last path segment.
    ("mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric/1.0.0-release.42",
     "1.0.0-release.42"),
    # The conventional shape, with a colon.
    ("mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric:1.0.0", "1.0.0"),
    # A digest pin: the digest is not a version and must not be returned as one.
    ("harbor.example/ns/app:1.2.3@sha256:abcdef0123456789", "1.2.3"),
    ("app:latest", "latest"),
    ("", ""),
])
def test_tag_is_extracted_from_an_image_reference(reference, expected):
    assert _tag_of(reference) == expected


def test_a_registry_port_is_not_mistaken_for_a_tag():
    """`host:5000/ns/app/1.0.0` -- the colon is in the *host*, not the tag."""
    assert _tag_of("registry.local:5000/ankalan/enu-biometric/1.0.0") == "1.0.0"


# ---------------------------------------------------------------------------
# Commit subject classification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("subject", [
    "Fix NPE in BioDeDuplicationServiceImpl",
    "ENU-4412 bug: candidates list was null",
    "hotfix for the dedupe loop",
    "Resolved index master data lookup",
])
def test_fix_subjects_are_recognised(subject):
    assert _FIX_SUBJECT.search(subject)


@pytest.mark.parametrize("subject", [
    "Bump version to 1.0.1",
    "Prepare release 1.0.1",
    "release: 1.0.2-SNAPSHOT",
])
def test_release_chores_are_recognised(subject):
    assert _RELEASE_SUBJECT.search(subject)


def test_a_release_chore_wins_over_a_fix_keyword():
    """`Release 1.0.1 with the NPE fix` is a chore, not a fix commit.

    probe_bumps checks the chore pattern first for exactly this reason: a
    release commit that mentions what it contains would otherwise be counted
    as a fix that bumped its own version, inflating the Q4 share and hiding
    an at-release-cut convention.
    """
    subject = "Release 1.0.1 including the dedupe NPE fix"
    assert _RELEASE_SUBJECT.search(subject)


# ---------------------------------------------------------------------------
# Q4 -- the bump convention measurement
# ---------------------------------------------------------------------------

class FakeBitbucket:
    """Just the two methods probe_bumps calls."""

    def __init__(self, commits, changes):
        self._commits = commits
        self._changes = changes
        self.calls = 0

    def commits(self, project, repo, branch, path="", limit=50):
        return self._commits

    def changed_paths(self, project, repo, commit, limit=500):
        return self._changes.get(commit, [])

    def file_at(self, project, repo, path, ref):
        return None, "not stubbed"


def _commit(cid, subject):
    return {"id": cid, "subject": subject, "date": 0}


def test_bumps_probe_reports_in_fix_commit_when_fixes_touch_the_pom():
    commits = [_commit("c1", "Fix NPE in dedupe"), _commit("c2", "Fix null candidate")]
    changes = {
        "c1": ["pom.xml", "src/main/java/A.java"],
        "c2": ["pom.xml", "src/main/java/B.java"],
    }
    result = probe_bumps(FakeBitbucket(commits, changes), "P", "R", "release",
                         "pom.xml", 40)

    assert result["ok"]
    assert result["fix_commits"] == 2
    assert result["fix_commits_touching_version_file"] == 2
    assert result["share"] == 1.0
    assert "IN-FIX-COMMIT" in result["detail"]


def test_bumps_probe_reports_at_release_cut_when_only_chores_touch_the_pom():
    """The dangerous convention (Trap T6) must be named explicitly."""
    commits = [
        _commit("c1", "Fix NPE in dedupe"),
        _commit("c2", "Fix null candidate"),
        _commit("c3", "Bump version to 1.0.1"),
    ]
    changes = {
        "c1": ["src/main/java/A.java"],
        "c2": ["src/main/java/B.java"],
        "c3": ["pom.xml"],
    }
    result = probe_bumps(FakeBitbucket(commits, changes), "P", "R", "release",
                         "pom.xml", 40)

    assert result["share"] == 0.0
    assert result["release_chore_commits"] == 1
    assert result["release_chores_touching_version_file"] == 1
    assert "AT-RELEASE-CUT" in result["detail"]
    assert "T6" in result["detail"]


def test_bumps_probe_is_inconclusive_rather_than_wrong_on_an_empty_sample():
    commits = [_commit("c1", "Refactor the consumer")]
    result = probe_bumps(FakeBitbucket(commits, {"c1": []}), "P", "R", "release",
                         "pom.xml", 40)

    assert result["share"] is None
    assert "INCONCLUSIVE" in result["detail"]


def test_a_nested_module_pom_counts_as_the_version_file():
    """Changed paths are full paths; matching must be on the basename."""
    commits = [_commit("c1", "Fix the dedupe loop")]
    changes = {"c1": ["biometric-service/pom.xml", "biometric-service/src/main/java/A.java"]}
    result = probe_bumps(FakeBitbucket(commits, changes), "P", "R", "release",
                         "pom.xml", 40)

    assert result["fix_commits_touching_version_file"] == 1


# ---------------------------------------------------------------------------
# Q5 -- Trap T5, the parent version
# ---------------------------------------------------------------------------

POM_WITH_PARENT = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>in.gov.uidai</groupId>
    <artifactId>uidai-parent</artifactId>
    <version>2.7.0</version>
  </parent>
  <artifactId>enu-biometric</artifactId>
  <version>1.0.0</version>
</project>
"""


class PomBitbucket(FakeBitbucket):
    def __init__(self, text):
        super().__init__([], {})
        self._text = text

    def file_at(self, project, repo, path, ref):
        return self._text, None


def test_a_parent_block_is_flagged_because_the_first_version_tag_is_wrong():
    result = probe_version_file(PomBitbucket(POM_WITH_PARENT), "P", "R",
                                "release", "pom.xml")

    assert result["ok"]
    assert result["declares_parent"] is True
    # The point of the probe: the *first* tag is the parent's, not the project's.
    assert result["first_version_tag"] == "2.7.0"
    assert "1.0.0" in result["version_tags_seen"]
    assert "T5" in result["detail"]


def test_package_json_version_is_read_as_json():
    result = probe_version_file(PomBitbucket('{"name": "svc", "version": "3.4.5"}'),
                                "P", "R", "release", "package.json")

    assert result["ok"]
    assert result["version"] == "3.4.5"


def test_an_unreadable_version_file_reports_failure_rather_than_raising():
    result = probe_version_file(FakeBitbucket([], {}), "P", "R", "release", "pom.xml")

    assert result["ok"] is False
    assert result["detail"]


# ---------------------------------------------------------------------------
# Argument handling
# ---------------------------------------------------------------------------

def test_repo_spec_splits_into_project_and_repo():
    assert _split_repo("ENU/enu-biometric") == ("ENU", "enu-biometric")


def test_a_repo_spec_without_a_slash_is_refused():
    with pytest.raises(SystemExit):
        _split_repo("enu-biometric")
