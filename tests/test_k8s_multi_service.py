"""
Multi-service Kubernetes discovery (2026-08-21).

A refId travels through several services, so the logs that explain a failure
are rarely confined to one of them. `K8S_APP_NAMES` (falling back to
`ES_APP_NAMES`) lists every service to search; this file covers the three
things that make merging their results non-trivial.

**Overlap is the normal case, not an edge case.** `name_contains` matching is
a substring test, so a service named `enu-biometric` matches the pods of
`enu-biometric-abis-mw-consumer` too. Reading such a pod once per matching
service would duplicate every line it contributed -- inflating the evidence
the Investigator reasons over, and the token bill, with copies that look like
corroborating occurrences but are the same event.

**A service that cannot be searched must be announced, not inferred.** The
whole point of the gap types is separating "we looked and found nothing" from
"we could not look". A service whose namespace is unreadable contributes
nothing, and an investigation that cannot tell that from silence will happily
conclude the packet never reached it.

**A cap must not starve a service.** Truncation trims round-robin, so every
service keeps representation instead of whichever was configured last being
dropped wholesale.
"""
import json

import pytest

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import discovery
from src.log_pipeline.types import GapType


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("K8S_FIXTURE_DIR", "K8S_SERVICE_MAP", "K8S_DEFAULT_NAMESPACE",
                "K8S_DEFAULT_APP", "K8S_APP_NAMES", "ES_APP_NAMES",
                "K8S_MAX_PODS", "K8S_MAX_TOTAL_PODS", "K8S_SIDECAR_DENYLIST",
                "KUBECONFIG_PATH", "K8S_CONTEXT"):
        monkeypatch.delenv(var, raising=False)
    k8s_client_module.reset_client()
    yield
    k8s_client_module.reset_client()


def _write_pod(root, namespace, pod_name, *, containers=("app",),
               start_time=None, phase="Running"):
    pod_dir = root / namespace / pod_name
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text("line one\n", encoding="utf-8")
    meta = {"phase": phase, "labels": {}, "containers": list(containers),
            "restart_counts": {}}
    if start_time:
        meta["start_time"] = start_time
    (pod_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")


def _use_fixtures(monkeypatch, root, namespace="enu"):
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(root))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", namespace)
    (root / namespace).mkdir(parents=True, exist_ok=True)


# ======================================================================
# Configuration
# ======================================================================

def test_defaults_to_a_single_service_when_nothing_is_configured():
    """The pre-multi-service behaviour, unchanged."""
    assert discovery.app_names() == ["enu-biometric"]


def test_k8s_default_app_still_wins_when_no_list_is_set(monkeypatch):
    monkeypatch.setenv("K8S_DEFAULT_APP", "just-one")
    assert discovery.app_names() == ["just-one"]


def test_es_app_names_drives_kubernetes_too(monkeypatch):
    """One list configures both sources, so they cannot drift apart and
    silently search different parts of the packet's journey."""
    monkeypatch.setenv("ES_APP_NAMES", "svc-a,svc-b,svc-c")
    assert discovery.app_names() == ["svc-a", "svc-b", "svc-c"]


def test_k8s_app_names_overrides_es_app_names(monkeypatch):
    """A pod-name substring is not always the ES application_name value."""
    monkeypatch.setenv("ES_APP_NAMES", "es-name")
    monkeypatch.setenv("K8S_APP_NAMES", "k8s-name")
    assert discovery.app_names() == ["k8s-name"]


def test_whitespace_and_duplicates_are_cleaned(monkeypatch):
    monkeypatch.setenv("K8S_APP_NAMES", " a , b ,a,, ")
    assert discovery.app_names() == ["a", "b"]


# ======================================================================
# Fan-out
# ======================================================================

def test_pods_from_every_configured_service_are_returned(monkeypatch, tmp_path):
    _use_fixtures(monkeypatch, tmp_path)
    _write_pod(tmp_path, "enu", "svc-alpha-abc123")
    _write_pod(tmp_path, "enu", "svc-beta-def456")
    _write_pod(tmp_path, "enu", "unrelated-xyz")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-beta")

    result = discovery.discover_targets()

    assert result.ok is True
    names = sorted(t.pod_name for t in result.targets)
    assert names == ["svc-alpha-abc123", "svc-beta-def456"]
    assert sorted(result.services_searched) == ["svc-alpha", "svc-beta"]
    assert result.services_failed == []


def test_each_target_records_the_service_it_matched(monkeypatch, tmp_path):
    _use_fixtures(monkeypatch, tmp_path)
    _write_pod(tmp_path, "enu", "svc-alpha-abc123")
    _write_pod(tmp_path, "enu", "svc-beta-def456")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-beta")

    by_pod = {t.pod_name: t.app for t in discovery.discover_targets().targets}
    assert by_pod == {"svc-alpha-abc123": "svc-alpha",
                      "svc-beta-def456": "svc-beta"}


def test_an_explicit_app_argument_searches_only_that_service(monkeypatch, tmp_path):
    """The operator CLI and single-service callers must be unaffected."""
    _use_fixtures(monkeypatch, tmp_path)
    _write_pod(tmp_path, "enu", "svc-alpha-abc123")
    _write_pod(tmp_path, "enu", "svc-beta-def456")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-beta")

    result = discovery.discover_targets(app="svc-beta")

    assert [t.pod_name for t in result.targets] == ["svc-beta-def456"]
    assert result.services_searched == ["svc-beta"]


def test_services_can_live_in_different_namespaces(monkeypatch, tmp_path):
    _use_fixtures(monkeypatch, tmp_path, namespace="ns-one")
    (tmp_path / "ns-two").mkdir(parents=True, exist_ok=True)
    _write_pod(tmp_path, "ns-one", "svc-alpha-abc")
    _write_pod(tmp_path, "ns-two", "svc-beta-def")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-beta")
    monkeypatch.setenv("K8S_SERVICE_MAP",
                       json.dumps({"svc-beta": {"namespace": "ns-two"}}))

    result = discovery.discover_targets()

    assert {(t.namespace, t.pod_name) for t in result.targets} == {
        ("ns-one", "svc-alpha-abc"), ("ns-two", "svc-beta-def")}


# ======================================================================
# Overlap -- the correctness bug this would otherwise introduce
# ======================================================================

def test_a_pod_matching_two_services_is_read_only_once(monkeypatch, tmp_path):
    """`enu-biometric` is a substring of `enu-biometric-abis-mw-consumer`, so
    both service names match the same pod. Without dedupe every line from it
    would appear twice."""
    _use_fixtures(monkeypatch, tmp_path)
    _write_pod(tmp_path, "enu", "enu-biometric-abis-mw-consumer-dcdr-6b64")
    monkeypatch.setenv("K8S_APP_NAMES",
                       "enu-biometric,enu-biometric-abis-mw-consumer")

    result = discovery.discover_targets()

    assert len(result.targets) == 1
    assert result.targets[0].pod_name == "enu-biometric-abis-mw-consumer-dcdr-6b64"


def test_dedupe_is_per_container_not_just_per_pod(monkeypatch, tmp_path):
    """A multi-container pod still yields one target per container -- exactly
    once each, not once per matching service."""
    _use_fixtures(monkeypatch, tmp_path)
    _write_pod(tmp_path, "enu", "shared-svc-abc", containers=("app", "worker"))
    monkeypatch.setenv("K8S_APP_NAMES", "shared,shared-svc")

    targets = discovery.discover_targets().targets

    assert sorted(t.container for t in targets) == ["app", "worker"]
    assert len({t.target_key for t in targets}) == 2


# ======================================================================
# Partial failure
# ======================================================================

def test_one_unsearchable_service_does_not_sink_the_others(monkeypatch, tmp_path):
    _use_fixtures(monkeypatch, tmp_path, namespace="ns-real")
    _write_pod(tmp_path, "ns-real", "svc-alpha-abc")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-ghost")
    monkeypatch.setenv("K8S_SERVICE_MAP",
                       json.dumps({"svc-ghost": {"namespace": "ns-missing"}}))

    result = discovery.discover_targets()

    assert result.ok is True
    assert [t.pod_name for t in result.targets] == ["svc-alpha-abc"]
    assert result.services_searched == ["svc-alpha"]
    assert result.services_failed == ["svc-ghost"]


def test_an_unsearchable_service_raises_a_named_gap(monkeypatch, tmp_path):
    """Silence and unreachability must not look the same downstream."""
    _use_fixtures(monkeypatch, tmp_path, namespace="ns-real")
    _write_pod(tmp_path, "ns-real", "svc-alpha-abc")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-ghost")
    monkeypatch.setenv("K8S_SERVICE_MAP",
                       json.dumps({"svc-ghost": {"namespace": "ns-missing"}}))

    gaps = [g for g in discovery.discover_targets().gaps
            if g.gap_type is GapType.SERVICE_UNAVAILABLE]

    assert len(gaps) == 1
    assert "svc-ghost" in gaps[0].detail
    assert gaps[0].context["app"] == "svc-ghost"


def test_all_services_failing_is_reported_as_a_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_APP_NAMES", "svc-a,svc-b")
    # No namespace configured at all, so neither service can be searched.

    result = discovery.discover_targets()

    assert result.ok is False
    assert result.reason
    assert result.targets == []
    assert sorted(result.services_failed) == ["svc-a", "svc-b"]


# ======================================================================
# Caps
# ======================================================================

def test_per_service_cap_applies_to_each_service_independently(monkeypatch, tmp_path):
    """K8S_MAX_PODS bounds each service, so adding a service never shrinks
    another's representation."""
    _use_fixtures(monkeypatch, tmp_path)
    for i in range(3):
        _write_pod(tmp_path, "enu", f"svc-alpha-{i}",
                   start_time=f"2026-08-2{i}T00:00:00+00:00")
        _write_pod(tmp_path, "enu", f"svc-beta-{i}",
                   start_time=f"2026-08-2{i}T00:00:00+00:00")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-beta")
    monkeypatch.setenv("K8S_MAX_PODS", "2")

    result = discovery.discover_targets()

    alpha = [t for t in result.targets if t.app == "svc-alpha"]
    beta = [t for t in result.targets if t.app == "svc-beta"]
    assert len(alpha) == 2
    assert len(beta) == 2
    assert result.truncated is True


def test_the_total_cap_keeps_every_service_represented(monkeypatch, tmp_path):
    """Round-robin truncation. Concatenating would drop svc-beta entirely."""
    _use_fixtures(monkeypatch, tmp_path)
    for i in range(4):
        _write_pod(tmp_path, "enu", f"svc-alpha-{i}",
                   start_time=f"2026-08-2{i}T00:00:00+00:00")
        _write_pod(tmp_path, "enu", f"svc-beta-{i}",
                   start_time=f"2026-08-2{i}T00:00:00+00:00")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha,svc-beta")
    monkeypatch.setenv("K8S_MAX_TOTAL_PODS", "2")

    result = discovery.discover_targets()

    apps = {t.app for t in result.targets}
    assert apps == {"svc-alpha", "svc-beta"}
    assert len({t.pod_key for t in result.targets}) == 2
    assert result.truncated is True


def test_the_total_cap_is_off_by_default(monkeypatch, tmp_path):
    _use_fixtures(monkeypatch, tmp_path)
    for i in range(6):
        _write_pod(tmp_path, "enu", f"svc-alpha-{i}",
                   start_time=f"2026-08-2{i}T00:00:00+00:00")
    monkeypatch.setenv("K8S_APP_NAMES", "svc-alpha")

    result = discovery.discover_targets()

    assert len(result.targets) == 6
    assert result.truncated is False
