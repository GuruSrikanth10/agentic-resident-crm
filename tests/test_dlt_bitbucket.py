"""Phase C4 -- the read-only Bitbucket adapter.

No network. Every test drives the adapter against a recorded response map, so
what is under test is the adapter's *decisions*: which endpoint it calls for
each flavour, how it resolves a frame to a file, how it reads a version out of
a pom, and -- most importantly -- that nothing it does can raise into the
analysis lane.
"""
import json

import pytest

from src.dlt import bitbucket as B

REPO_MAP = json.dumps({
    "com.uidai.enu.biometric": {
        "project": "ENU",
        "repo": "enu-biometric",
        "branch": "release",
        "version_file": "pom.xml",
        "source_roots": ["src/main/java"],
    },
})


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("BITBUCKET_BASE_URL", "BITBUCKET_TOKEN", "BITBUCKET_USERNAME",
                "BITBUCKET_API_FLAVOUR", "BITBUCKET_TIMEOUT_SECONDS",
                "DLT_REPO_MAP", "DLT_CODE_CHECK_TTL_SECONDS",
                "DLT_CODE_CHECK_MAX_COMMITS"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("BITBUCKET_BASE_URL", "https://bitbucket.example")
    monkeypatch.setenv("BITBUCKET_TOKEN", "read-only-token")
    monkeypatch.setenv("DLT_REPO_MAP", REPO_MAP)
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "0")
    B.reset_cache()
    _reset_breaker()
    yield
    B.reset_cache()
    _reset_breaker()


def _reset_breaker():
    from src.utils.resilience import bitbucket_breaker
    bitbucket_breaker.close()


class FakeResponse:
    def __init__(self, status=200, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text if text else (json.dumps(payload) if payload is not None else "")
        self.content = self.text.encode("utf-8")

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


def _serve(monkeypatch, handler):
    """Route every GET through `handler(path, params) -> FakeResponse`."""
    calls = []

    class FakeSession:
        def get(self, url, headers=None, auth=None, params=None, timeout=None):
            path = url.replace("https://bitbucket.example", "")
            calls.append((path, dict(params or {}), headers or {}, auth))
            return handler(path, dict(params or {}))

    monkeypatch.setattr(B, "_session", FakeSession())
    return calls


def _repo():
    return B.repo_for("com.uidai.enu.biometric.service.impl.BioDeDuplicationServiceImpl")


# ---------------------------------------------------------------------------
# Repository mapping
# ---------------------------------------------------------------------------

def test_a_frame_maps_to_its_repository_by_longest_prefix(monkeypatch):
    monkeypatch.setenv("DLT_REPO_MAP", json.dumps({
        "com.uidai": {"project": "GEN", "repo": "generic"},
        "com.uidai.enu.biometric": {"project": "ENU", "repo": "enu-biometric"},
    }))

    repo = B.repo_for("com.uidai.enu.biometric.service.impl.Foo")

    assert repo.slug == "ENU/enu-biometric"
    assert repo.prefix == "com.uidai.enu.biometric"


def test_the_shared_library_package_maps_to_nothing(monkeypatch):
    """Trap T8. A fix in `in.gov.uidai.common` is a dependency bump in the
    service's pom, not a commit on the service's release branch. Resolving it
    against the service repo would be wrong, not merely unhelpful."""
    assert B.repo_for("in.gov.uidai.common.factory.CommonErrorFactory") is None


def test_an_entry_missing_its_repo_is_ignored_rather_than_half_used(monkeypatch):
    monkeypatch.setenv("DLT_REPO_MAP", json.dumps({
        "com.uidai.enu.biometric": {"project": "ENU"},
    }))
    assert B.repo_for("com.uidai.enu.biometric.Foo") is None


def test_a_malformed_repo_map_maps_nothing(monkeypatch):
    """Half a mapping would resolve some frames and mis-resolve others."""
    monkeypatch.setenv("DLT_REPO_MAP", "{not json")
    assert B.repo_map() == {}
    assert B.repo_for("com.uidai.enu.biometric.Foo") is None


def test_a_single_source_root_string_is_accepted_as_well_as_a_list(monkeypatch):
    monkeypatch.setenv("DLT_REPO_MAP", json.dumps({
        "com.uidai": {"project": "P", "repo": "r", "source_root": "svc/src/main/java"},
    }))
    assert B.repo_for("com.uidai.Foo").source_roots == ("svc/src/main/java",)


def test_configured_requires_a_url_a_token_and_a_map(monkeypatch):
    assert B.configured() is True
    monkeypatch.delenv("BITBUCKET_TOKEN")
    assert B.configured() is False


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

SUFFIX = "com/uidai/enu/biometric/service/impl/BioDeDuplicationServiceImpl.java"


def test_a_frame_resolves_by_construction_in_one_call(monkeypatch):
    def handler(path, params):
        if path.endswith(f"/raw/src/main/java/{SUFFIX}"):
            return FakeResponse(200, text="class Foo {}")
        return FakeResponse(404)

    calls = _serve(monkeypatch, handler)

    assert B.resolve_path(_repo(), SUFFIX) == f"src/main/java/{SUFFIX}"
    assert len(calls) == 1


def test_source_roots_are_tried_in_order(monkeypatch):
    monkeypatch.setenv("DLT_REPO_MAP", json.dumps({
        "com.uidai.enu.biometric": {
            "project": "ENU", "repo": "enu-biometric",
            "source_roots": ["api/src/main/java", "service/src/main/java"],
        },
    }))

    def handler(path, params):
        if "service/src/main/java" in path:
            return FakeResponse(200, text="class Foo {}")
        return FakeResponse(404)

    _serve(monkeypatch, handler)

    assert B.resolve_path(_repo(), SUFFIX) == f"service/src/main/java/{SUFFIX}"


def test_an_unconfigured_layout_falls_back_to_listing_the_repository(monkeypatch):
    full = f"biometric-service/src/main/java/{SUFFIX}"

    def handler(path, params):
        if path.endswith("/files"):
            return FakeResponse(200, {"values": ["pom.xml", full], "isLastPage": True})
        return FakeResponse(404)

    _serve(monkeypatch, handler)

    assert B.resolve_path(_repo(), SUFFIX) == full


def test_two_modules_declaring_the_same_class_is_unknown_not_a_coin_toss(monkeypatch):
    """Picking either would attribute a commit to the wrong module."""
    def handler(path, params):
        if path.endswith("/files"):
            return FakeResponse(200, {"values": [f"a/src/main/java/{SUFFIX}",
                                                 f"b/src/main/java/{SUFFIX}"],
                                      "isLastPage": True})
        return FakeResponse(404)

    _serve(monkeypatch, handler)

    assert B.resolve_path(_repo(), SUFFIX) is None


def test_a_frame_that_exists_nowhere_resolves_to_none(monkeypatch):
    _serve(monkeypatch, lambda path, params: FakeResponse(404))
    assert B.resolve_path(_repo(), SUFFIX) is None


def test_cloud_never_lists_the_repository(monkeypatch):
    """Cloud has no recursive listing; it must configure source roots."""
    monkeypatch.setenv("BITBUCKET_API_FLAVOUR", "cloud")
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(404))

    assert B.resolve_path(_repo(), SUFFIX) is None
    assert not any("/files" in path for path, *_ in calls)


# ---------------------------------------------------------------------------
# Commits
# ---------------------------------------------------------------------------

SERVER_COMMITS = {
    "values": [
        {"id": "abc1234567", "message": "Fix the dedupe NPE\n\ndetail",
         "authorTimestamp": 1787200000000},
        {"id": "def7654321", "message": "Earlier change",
         "authorTimestamp": 1787000000000},
    ]
}


def test_commits_are_read_off_the_configured_branch(monkeypatch):
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, SERVER_COMMITS))

    commits = B.commits_touching(_repo(), "src/main/java/Foo.java")

    assert [c.id for c in commits] == ["abc1234567", "def7654321"]
    assert commits[0].subject == "Fix the dedupe NPE"
    assert commits[0].timestamp_ms == 1787200000000
    assert calls[0][1]["until"] == "release"
    assert calls[0][1]["path"] == "src/main/java/Foo.java"


def test_commits_older_than_the_failure_are_dropped(monkeypatch):
    _serve(monkeypatch, lambda path, params: FakeResponse(200, SERVER_COMMITS))

    commits = B.commits_touching(_repo(), "Foo.java", since_ms=1787100000000)

    assert [c.id for c in commits] == ["abc1234567"]


def test_a_commit_with_no_readable_timestamp_is_kept(monkeypatch):
    """Dropping it could turn a NOT_DEPLOYED into a NO_CHANGE -- the verdict
    that stops a replay."""
    payload = {"values": [{"id": "aaa", "message": "x", "authorTimestamp": None}]}
    _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))

    assert len(B.commits_touching(_repo(), "Foo.java", since_ms=1787100000000)) == 1


def test_cloud_commits_use_the_cloud_endpoint_and_iso_dates(monkeypatch):
    monkeypatch.setenv("BITBUCKET_API_FLAVOUR", "cloud")
    payload = {"values": [{"hash": "cafebabe", "message": "Fix it",
                           "date": "2026-08-18T02:20:08.511+00:00"}]}
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))

    commits = B.commits_touching(_repo(), "Foo.java")

    assert calls[0][0] == "/2.0/repositories/ENU/enu-biometric/commits/release"
    assert commits[0].id == "cafebabe"
    assert commits[0].timestamp_ms == 1787019608511


def test_changed_paths_are_read_for_one_commit(monkeypatch):
    payload = {"values": [{"path": {"toString": "pom.xml"}},
                          {"path": {"toString": "src/main/java/Foo.java"}}]}
    _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))

    assert B.changed_paths(_repo(), "abc1234567") == ["pom.xml",
                                                      "src/main/java/Foo.java"]


def test_an_unreadable_diff_is_unknown_not_an_empty_change_set(monkeypatch):
    _serve(monkeypatch, lambda path, params: FakeResponse(500))
    assert B.changed_paths(_repo(), "abc1234567") is None


def test_a_nested_module_pom_counts_as_the_version_file():
    assert B.touches_version_file(["svc/pom.xml", "svc/src/A.java"], "pom.xml") is True
    assert B.touches_version_file(["svc/src/A.java"], "pom.xml") is False
    assert B.touches_version_file([], "pom.xml") is False


# ---------------------------------------------------------------------------
# Version files -- Trap T5
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

POM_INHERITING = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <parent>
    <artifactId>uidai-parent</artifactId>
    <version>2.7.0</version>
  </parent>
  <artifactId>enu-biometric</artifactId>
</project>
"""

POM_NO_NAMESPACE = """<project>
  <parent><version>9.9.9</version></parent>
  <version>3.2.1</version>
</project>
"""


def test_the_parent_version_is_not_mistaken_for_the_project_version():
    """Trap T5: `<parent><version>` appears FIRST in the document."""
    assert B.parse_pom_version(POM_WITH_PARENT) == "1.0.0"


def test_a_pom_with_no_version_of_its_own_inherits_the_parents():
    """Maven's own inheritance rule -- the module really is 2.7.0."""
    assert B.parse_pom_version(POM_INHERITING) == "2.7.0"


def test_a_pom_without_a_namespace_still_parses():
    assert B.parse_pom_version(POM_NO_NAMESPACE) == "3.2.1"


@pytest.mark.parametrize("text", ["", "   ", None, "<project><unclosed>"])
def test_an_unparseable_pom_yields_none_rather_than_raising(text):
    assert B.parse_pom_version(text) is None


def test_package_json_version_is_read_as_json():
    assert B.parse_package_version('{"name": "svc", "version": "3.4.5"}') == "3.4.5"
    assert B.parse_package_version('{"name": "svc"}') is None
    assert B.parse_package_version("not json") is None


def test_version_at_reads_the_repos_configured_version_file(monkeypatch):
    calls = _serve(monkeypatch,
                   lambda path, params: FakeResponse(200, text=POM_WITH_PARENT))

    assert B.version_at(_repo(), "abc1234567") == "1.0.0"
    assert calls[0][1]["at"] == "abc1234567"


def test_version_at_defaults_to_the_branch_when_no_ref_is_given(monkeypatch):
    calls = _serve(monkeypatch,
                   lambda path, params: FakeResponse(200, text=POM_WITH_PARENT))

    B.version_at(_repo())

    assert calls[0][1]["at"] == "release"


# ---------------------------------------------------------------------------
# Degradation -- nothing may raise into the analysis lane
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", [401, 403, 404, 500, 502])
def test_an_http_error_returns_unknown_rather_than_raising(monkeypatch, status):
    _serve(monkeypatch, lambda path, params: FakeResponse(status))
    repo = _repo()

    assert B.commits_touching(repo, "Foo.java") is None
    assert B.changed_paths(repo, "abc") is None
    assert B.file_at(repo, "pom.xml") is None
    assert B.version_at(repo) is None
    assert B.resolve_path(repo, SUFFIX) is None


def test_could_not_look_is_distinct_from_looked_and_found_nothing(monkeypatch):
    """The distinction decides a replay. `[]` becomes NO_CHANGE and withholds
    one; an unreachable server must become UNKNOWN instead, or a Bitbucket
    outage would stop every replay in the system on no evidence at all."""
    _serve(monkeypatch, lambda path, params: FakeResponse(200, {"values": []}))
    assert B.commits_touching(_repo(), "Foo.java") == []

    B.reset_cache()
    _serve(monkeypatch, lambda path, params: FakeResponse(503))
    assert B.commits_touching(_repo(), "Foo.java") is None


def test_a_transport_failure_returns_unknown_rather_than_raising(monkeypatch):
    def explode(path, params):
        raise ConnectionError("bitbucket unreachable")

    _serve(monkeypatch, explode)

    assert B.commits_touching(_repo(), "Foo.java") is None
    assert B.version_at(_repo()) is None


def test_a_non_json_response_returns_unknown(monkeypatch):
    _serve(monkeypatch, lambda path, params: FakeResponse(200, None, text="<html>"))
    assert B.commits_touching(_repo(), "Foo.java") is None


def test_an_oversized_file_is_refused(monkeypatch):
    huge = "x" * (B.MAX_FILE_BYTES + 1)
    _serve(monkeypatch, lambda path, params: FakeResponse(200, text=huge))

    assert B.file_at(_repo(), "pom.xml") is None


def test_no_base_url_means_no_calls(monkeypatch):
    monkeypatch.delenv("BITBUCKET_BASE_URL")
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, {"values": []}))

    assert B.commits_touching(_repo(), "Foo.java") is None
    assert calls == []


def test_a_tripped_breaker_degrades_instead_of_raising(monkeypatch):
    from src.utils.resilience import bitbucket_breaker

    def explode(path, params):
        raise ConnectionError("down")

    _serve(monkeypatch, explode)
    for _ in range(4):
        B.commits_touching(_repo(), "Foo.java")

    assert bitbucket_breaker.current_state == "open"
    # And the next call still returns cleanly rather than raising.
    assert B.commits_touching(_repo(), "Foo.java") is None


# ---------------------------------------------------------------------------
# Auth and caching
# ---------------------------------------------------------------------------

def test_a_bare_token_is_sent_as_a_bearer_header(monkeypatch):
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, {"values": []}))
    B.commits_touching(_repo(), "Foo.java")

    _, _, headers, auth = calls[0]
    assert headers["Authorization"] == "Bearer read-only-token"
    assert auth is None


def test_a_username_switches_to_basic_auth_for_cloud_app_passwords(monkeypatch):
    monkeypatch.setenv("BITBUCKET_USERNAME", "svc-account")
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, {"values": []}))
    B.commits_touching(_repo(), "Foo.java")

    _, _, headers, auth = calls[0]
    assert "Authorization" not in headers
    assert auth == ("svc-account", "read-only-token")


def test_repeated_reads_are_served_from_the_cache(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "900")
    B.reset_cache()
    calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, SERVER_COMMITS))

    for _ in range(4):
        B.commits_touching(_repo(), "Foo.java")

    assert len(calls) == 1


def test_a_failed_read_is_not_cached(monkeypatch):
    """Holding a server outage for a whole TTL would keep returning UNKNOWN
    long after the server came back."""
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "900")
    B.reset_cache()
    state = {"fail": True}

    def handler(path, params):
        return FakeResponse(500) if state["fail"] else FakeResponse(200, SERVER_COMMITS)

    _serve(monkeypatch, handler)

    assert B.commits_touching(_repo(), "Foo.java") is None
    state["fail"] = False
    assert len(B.commits_touching(_repo(), "Foo.java")) == 2


# ---------------------------------------------------------------------------
# Flavour selection
# ---------------------------------------------------------------------------

def test_the_flavour_defaults_to_server_for_a_self_hosted_host():
    assert B.flavour() == "server"


def test_bitbucket_org_is_detected_as_cloud(monkeypatch):
    monkeypatch.setenv("BITBUCKET_BASE_URL", "https://api.bitbucket.org")
    assert B.flavour() == "cloud"


def test_explicit_configuration_beats_detection(monkeypatch):
    monkeypatch.setenv("BITBUCKET_BASE_URL", "https://api.bitbucket.org")
    monkeypatch.setenv("BITBUCKET_API_FLAVOUR", "server")
    assert B.flavour() == "server"


def test_branch_names_with_slashes_are_usable():
    assert B.is_usable_branch("release") is True
    assert B.is_usable_branch("release/2026.08") is True
    assert B.is_usable_branch("release; rm -rf") is False
    assert B.is_usable_branch("") is False


# ---------------------------------------------------------------------------
# Trap T12 -- Maven CI-friendly versions
# ---------------------------------------------------------------------------

CI_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <parent><artifactId>uidai-parent</artifactId><version>2.7.0</version></parent>
  <artifactId>enu-biometric</artifactId>
  <version>${revision}</version>
  <properties><revision>1.0.1</revision></properties>
</project>
"""

CI_POM_TRIO = """<project>
  <version>${revision}${sha1}${changelist}</version>
  <properties>
    <revision>1.0.1</revision><sha1></sha1><changelist>-SNAPSHOT</changelist>
  </properties>
</project>
"""

CI_POM_UNDEFINED = """<project><version>${revision}</version></project>"""


def test_a_ci_friendly_version_is_resolved_from_properties():
    """`<version>${revision}</version>` is the documented multi-module idiom.
    Returning the literal would pass every "did we read something?" check and
    fail only at comparison time."""
    assert B.parse_pom_version(CI_POM) == "1.0.1"


def test_the_revision_sha1_changelist_trio_is_resolved():
    assert B.parse_pom_version(CI_POM_TRIO) == "1.0.1-SNAPSHOT"


def test_a_placeholder_with_no_property_is_unreadable_not_literal():
    """None, not `"${revision}"`. An unresolved version is not a version."""
    assert B.parse_pom_version(CI_POM_UNDEFINED) is None


def test_a_parent_version_placeholder_is_resolved_too():
    pom = """<project>
      <parent><version>${revision}</version></parent>
      <properties><revision>2.7.0</revision></properties>
    </project>"""
    assert B.parse_pom_version(pom) == "2.7.0"


@pytest.mark.parametrize("text,expected", [
    ("group=uidai\nversion=1.0.1\n", "1.0.1"),
    ("# a comment\nrevision = 1.0.2\n", "1.0.2"),
    ("name=svc\n", None),
    ("", None),
    (None, None),
])
def test_a_properties_file_can_carry_the_version(text, expected):
    assert B.parse_properties_version(text) == expected


def test_version_at_reads_a_properties_file_when_configured(monkeypatch):
    monkeypatch.setenv("DLT_REPO_MAP", json.dumps({
        "com.uidai.enu.biometric": {"project": "ENU", "repo": "enu-biometric",
                                    "version_file": "gradle.properties"},
    }))
    _serve(monkeypatch, lambda path, params: FakeResponse(200, text="version=1.0.1"))

    assert B.version_at(_repo()) == "1.0.1"


# -- a value that does not parse is not a version ---------------------------

@pytest.mark.parametrize("pom", [CI_POM_UNDEFINED,
                                 "<project><version>latest</version></project>",
                                 "<project><version>main</version></project>"])
def test_version_at_refuses_a_value_that_is_not_an_ordered_version(monkeypatch, pom):
    """Handing back unparseable text upstream makes the eventual failure read
    as a skipped version bump (Trap T9) rather than an unreadable file."""
    _serve(monkeypatch, lambda path, params: FakeResponse(200, text=pom))

    assert B.version_at(_repo()) is None


def test_version_at_still_returns_a_real_version(monkeypatch):
    _serve(monkeypatch, lambda path, params: FakeResponse(200, text=CI_POM))
    assert B.version_at(_repo()) == "1.0.1"


# ---------------------------------------------------------------------------
# Trap T13 -- which timestamp a commit is filtered on
# ---------------------------------------------------------------------------

def test_the_committer_timestamp_is_preferred_over_the_author_timestamp(monkeypatch):
    """A rebase or squash merge leaves `authorTimestamp` far in the past. A fix
    authored before the packet failed but merged after would be filtered out as
    "older than the failure", and the fix never found."""
    payload = {"values": [{"id": "aaa", "message": "Fix",
                           "authorTimestamp": 1786000000000,     # long before
                           "committerTimestamp": 1787200000000}]}  # after
    _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))

    commits = B.commits_touching(_repo(), "Foo.java")
    assert commits[0].timestamp_ms == 1787200000000

    # And it therefore survives a filter anchored on the failure.
    B.reset_cache()
    _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))
    assert len(B.commits_touching(_repo(), "Foo.java", since_ms=1787019608511)) == 1


def test_the_author_timestamp_is_the_fallback(monkeypatch):
    payload = {"values": [{"id": "aaa", "message": "Fix",
                           "authorTimestamp": 1787200000000}]}
    _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))

    assert B.commits_touching(_repo(), "Foo.java")[0].timestamp_ms == 1787200000000


# ---------------------------------------------------------------------------
# An empty result expires sooner than a populated one
# ---------------------------------------------------------------------------

def test_the_negative_ttl_defaults_shorter_than_the_positive_one(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "3600")
    assert B.negative_cache_ttl_seconds() < B.cache_ttl_seconds()
    assert B.negative_cache_ttl_seconds() == B.DEFAULT_NEGATIVE_TTL_SECONDS


def test_the_negative_ttl_never_exceeds_the_positive_one(monkeypatch):
    """Lowering the main TTL for testing must lower both."""
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "60")
    monkeypatch.setenv("DLT_CODE_CHECK_NEGATIVE_TTL_SECONDS", "900")
    assert B.negative_cache_ttl_seconds() == 60


def test_an_empty_result_is_re_fetched_sooner_than_a_populated_one(monkeypatch):
    """An empty list becomes NO_CHANGE and withholds a replay, so a stale one
    costs more than a stale list of commits."""
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "3600")
    monkeypatch.setenv("DLT_CODE_CHECK_NEGATIVE_TTL_SECONDS", "900")

    clock = {"t": 1000.0}
    monkeypatch.setattr(B.time, "monotonic", lambda: clock["t"])

    for payload, key, expected_after_20min in (({"values": []}, "empty.java", 2),
                                               (SERVER_COMMITS, "full.java", 1)):
        B.reset_cache()
        clock["t"] = 1000.0
        calls = _serve(monkeypatch, lambda path, params: FakeResponse(200, payload))

        B.commits_touching(_repo(), key)
        clock["t"] += 1200          # 20 minutes: past the negative TTL only
        B.commits_touching(_repo(), key)

        assert len(calls) == expected_after_20min, key


def test_a_malformed_negative_ttl_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_TTL_SECONDS", "3600")
    monkeypatch.setenv("DLT_CODE_CHECK_NEGATIVE_TTL_SECONDS", "soon")
    assert B.negative_cache_ttl_seconds() == B.DEFAULT_NEGATIVE_TTL_SECONDS
