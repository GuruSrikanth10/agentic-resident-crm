"""
Phase 2 of KUBERNETES_LOGS_PLAN.md -- Kubernetes client and pod discovery.

Covers scenarios 7 (Pending skipped), 8 (Failed/Succeeded included),
9 (sidecars), 10 (no match), 12 (RBAC), 15 (fan-out cap), plus the exit
criterion that absent configuration degrades without raising.

Also covers the namespace-scoped RBAC constraint: this ServiceAccount has no
cluster-wide `list namespaces` / `list pods --all-namespaces` access, only
`get` on a named namespace and `list`/`get` on pods within it. Discovery
therefore never enumerates anything -- it verifies a known namespace with a
targeted read, and matches pods within it either by name substring (the
default, and the validated real-cluster pattern) or by label selector
(opt-in via `K8S_SERVICE_MAP`, for services known to be labelled reliably).
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import discovery, fixtures
from src.log_pipeline.sources.k8s.discovery import (
    MATCH_MODE_LABEL,
    MATCH_MODE_NAME_CONTAINS,
    PodMatchSpec,
)
from src.log_pipeline.types import GapType


@pytest.fixture(autouse=True)
def _isolate_k8s_env(monkeypatch):
    """Every test starts from a clean, unconfigured Kubernetes environment."""
    for var in (
        "K8S_FIXTURE_DIR", "K8S_SERVICE_MAP", "K8S_DEFAULT_NAMESPACE",
        "K8S_DEFAULT_APP", "K8S_MAX_PODS", "K8S_SIDECAR_DENYLIST",
        "KUBECONFIG_PATH", "K8S_CONTEXT", "K8S_VERIFY_SSL", "K8S_CA_CERT_PATH",
        # ES_APP_NAMES is cleared here too, not just K8S_APP_NAMES: since
        # 2026-08-21 the Kubernetes source falls back to it, so a stray value
        # from another test would silently change which services are searched.
        "K8S_APP_NAMES", "ES_APP_NAMES", "K8S_MAX_TOTAL_PODS",
    ):
        monkeypatch.delenv(var, raising=False)
    k8s_client_module.reset_client()
    yield
    k8s_client_module.reset_client()


# ======================================================================
# Fixture authoring helper
# ======================================================================

#: The default app resolved when neither `app=` nor K8S_DEFAULT_APP is set
#: (see discovery.resolve_service). Under the new default name_contains
#: mode, a pod only matches if this string is a substring of its name --
#: fixture pod names below are chosen deliberately around that.
DEFAULT_APP = "enu-biometric"


def _write_pod(root, namespace, pod_name, *, phase="Running", labels=None,
               containers=("app",), restart_counts=None, start_time=None,
               current="line one\n", previous=None):
    pod_dir = root / namespace / pod_name
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text(current, encoding="utf-8")
    if previous is not None:
        (pod_dir / "previous.log").write_text(previous, encoding="utf-8")
    meta = {
        "phase": phase,
        "labels": labels if labels is not None else {},
        "containers": list(containers),
        "restart_counts": restart_counts or {},
    }
    if start_time:
        meta["start_time"] = start_time
    (pod_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return pod_dir


def _use_fixtures(monkeypatch, root, namespace="enu"):
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(root))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", namespace)
    # namespace_exists() checks for the directory; discover_targets() always
    # verifies it first, mirroring the live read_namespace pre-flight check.
    (root / namespace).mkdir(parents=True, exist_ok=True)


# ======================================================================
# Graceful degradation -- the Phase 2 exit criterion
# ======================================================================

def test_missing_config_degrades_without_raising():
    """A log source is not a hard dependency: an unconfigured cluster must
    report unavailable, never raise (design principle: never crash)."""
    result = discovery.discover_targets()
    assert result.ok is False
    assert result.reason
    assert result.targets == []


def test_client_is_unavailable_not_exceptional_without_config(monkeypatch):
    monkeypatch.setenv("KUBECONFIG_PATH", "/nonexistent/kubeconfig.yaml")
    k8s_client_module.reset_client()

    assert k8s_client_module.get_client() is None
    assert k8s_client_module.is_available() is False
    assert "no usable Kubernetes config" in k8s_client_module.unavailable_reason()


def test_client_resolution_is_cached(monkeypatch):
    """The unavailable verdict is cached so a misconfigured deployment logs
    once, not once per packet."""
    monkeypatch.setenv("KUBECONFIG_PATH", "/nonexistent/kubeconfig.yaml")
    k8s_client_module.reset_client()

    with patch.object(k8s_client_module, "_build_client",
                      wraps=k8s_client_module._build_client) as spy:
        k8s_client_module.get_client()
        k8s_client_module.get_client()
        k8s_client_module.get_client()

    assert spy.call_count == 1


def test_missing_namespace_is_reported_clearly(monkeypatch, tmp_path):
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    result = discovery.discover_targets()
    assert result.ok is False
    assert "namespace" in result.reason


# ======================================================================
# Namespace pre-flight check -- mirrors the validated read_namespace pattern
# ======================================================================

def test_unreadable_namespace_fails_before_listing_pods(monkeypatch, tmp_path):
    """The namespace directory does not exist in fixtures (mirrors a 403/404
    on read_namespace) -- discovery must fail here, distinctly from 'no pods
    matched', and never attempt to list."""
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "does-not-exist")
    # Deliberately not creating the namespace directory.

    result = discovery.discover_targets()

    assert result.ok is False
    assert "does-not-exist" in result.reason


def test_namespace_read_denied_by_rbac_is_reported_distinctly(monkeypatch):
    """A 403 on read_namespace is a configuration error, not 'no pods
    found' -- this ServiceAccount has no cluster-wide access, so this is
    the first RBAC boundary a caller can hit."""
    from kubernetes.client.exceptions import ApiException

    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    api = MagicMock()
    api.read_namespace.side_effect = ApiException(status=403, reason="Forbidden")

    with patch.object(k8s_client_module, "get_client", return_value=api), \
         patch.object(k8s_client_module, "is_available", return_value=True):
        result = discovery.discover_targets()

    assert result.ok is False
    assert "ApiException" in result.reason
    api.list_namespaced_pod.assert_not_called()


def test_namespace_read_success_proceeds_to_list_pods(monkeypatch):
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    api = MagicMock()
    api.list_namespaced_pod.return_value = MagicMock(items=[])

    with patch.object(k8s_client_module, "get_client", return_value=api), \
         patch.object(k8s_client_module, "is_available", return_value=True):
        result = discovery.discover_targets()

    api.read_namespace.assert_called_once()
    assert api.read_namespace.call_args.kwargs["name"] == "enu"
    assert result.ok is True


def test_fixtures_namespace_exists():
    assert fixtures.namespace_exists("anything") is False  # K8S_FIXTURE_DIR unset


def test_fixtures_namespace_exists_true(monkeypatch, tmp_path):
    (tmp_path / "enu").mkdir(parents=True)
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    assert fixtures.namespace_exists("enu") is True
    assert fixtures.namespace_exists("other") is False


# ======================================================================
# Service resolution -- PodMatchSpec
# ======================================================================

def test_resolve_service_defaults_to_name_contains(monkeypatch):
    """The default mode is name-substring matching on the app name itself --
    the validated pattern, not an assumed label."""
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    namespace, match_spec = discovery.resolve_service(app="my-service")

    assert namespace == "enu"
    assert match_spec == PodMatchSpec(MATCH_MODE_NAME_CONTAINS, "my-service")


def test_service_map_can_opt_into_label_mode(monkeypatch):
    monkeypatch.setenv("K8S_SERVICE_MAP", json.dumps({
        "enu-biometric": {"namespace": "prod-enu", "label_selector": "component=bio"}
    }))
    namespace, match_spec = discovery.resolve_service(app="enu-biometric")

    assert namespace == "prod-enu"
    assert match_spec == PodMatchSpec(MATCH_MODE_LABEL, "component=bio")


def test_service_map_can_override_the_name_contains_value(monkeypatch):
    """A service's pod-name prefix need not equal its Kafka-payload app
    name."""
    monkeypatch.setenv("K8S_SERVICE_MAP", json.dumps({
        "enu-biometric": {"namespace": "ankalan", "name_contains": "centralized-rule-engine"}
    }))
    namespace, match_spec = discovery.resolve_service(app="enu-biometric")

    assert namespace == "ankalan"
    assert match_spec == PodMatchSpec(MATCH_MODE_NAME_CONTAINS, "centralized-rule-engine")


def test_malformed_service_map_is_ignored_not_fatal(monkeypatch):
    monkeypatch.setenv("K8S_SERVICE_MAP", "{not json")
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    namespace, match_spec = discovery.resolve_service(app="svc")

    assert namespace == "enu"
    assert match_spec == PodMatchSpec(MATCH_MODE_NAME_CONTAINS, "svc")


def test_explicit_namespace_beats_config(monkeypatch):
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "from-env")
    namespace, _ = discovery.resolve_service(app="svc", namespace="explicit")
    assert namespace == "explicit"


def test_match_spec_describe():
    assert "label selector" in PodMatchSpec(MATCH_MODE_LABEL, "app=x").describe()
    assert "pod name contains" in PodMatchSpec(MATCH_MODE_NAME_CONTAINS, "x").describe()


# ======================================================================
# Label selector parsing (still used by label mode)
# ======================================================================

def test_label_selector_parsing():
    assert fixtures.parse_label_selector("app=foo") == {"app": "foo"}
    assert fixtures.parse_label_selector("app=foo,tier=web") == {"app": "foo", "tier": "web"}
    assert fixtures.parse_label_selector("") == {}
    assert fixtures.parse_label_selector(None) == {}


def test_set_based_selector_is_rejected_rather_than_mis_parsed():
    """Silently mis-parsing `in (...)` would select the wrong pods."""
    with pytest.raises(ValueError):
        fixtures.parse_label_selector("app in (a,b)")


# ======================================================================
# Pod matching -- both modes
# ======================================================================

def test_name_contains_mode_filters_pods_by_substring(monkeypatch, tmp_path):
    """The default mode, and the one validated against the real cluster:
    list everything in the namespace, keep pods whose name contains the
    target string."""
    _write_pod(tmp_path, "enu", "centralized-rule-engine-7d8f9", labels={})
    _write_pod(tmp_path, "enu", "some-other-service-abc12", labels={})
    _use_fixtures(monkeypatch, tmp_path)
    monkeypatch.setenv("K8S_SERVICE_MAP", json.dumps({
        "cre": {"name_contains": "centralized-rule-engine"}
    }))

    result = discovery.discover_targets(app="cre")
    assert [t.pod_name for t in result.targets] == ["centralized-rule-engine-7d8f9"]


def test_label_mode_filters_pods_by_label_when_configured(monkeypatch, tmp_path):
    """Label mode remains available as an explicit per-service opt-in for
    services known to be labelled reliably."""
    _write_pod(tmp_path, "enu", "matching-pod", labels={"app": "enu-biometric"})
    _write_pod(tmp_path, "enu", "other-pod", labels={"app": "something-else"})
    _use_fixtures(monkeypatch, tmp_path)
    monkeypatch.setenv("K8S_SERVICE_MAP", json.dumps({
        "enu-biometric": {"label_selector": "app=enu-biometric"}
    }))

    result = discovery.discover_targets(app="enu-biometric")
    assert [t.pod_name for t in result.targets] == ["matching-pod"]


def test_name_contains_does_not_look_at_labels_at_all(monkeypatch, tmp_path):
    """A pod with no labels whatsoever must still match under the default
    mode -- this is the whole point of not assuming a labelling scheme."""
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-xyz", labels={})
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert [t.pod_name for t in result.targets] == [f"{DEFAULT_APP}-xyz"]


# ======================================================================
# Phase filtering -- scenarios 7 and 8
# ======================================================================

def test_pending_pods_are_skipped(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-running", phase="Running")
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-pending", phase="Pending")
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert [t.pod_name for t in result.targets] == [f"{DEFAULT_APP}-running"]
    assert result.pods_skipped_pending == 1


@pytest.mark.parametrize("phase", ["Running", "Failed", "Succeeded"])
def test_terminated_pods_are_included(monkeypatch, tmp_path, phase):
    """A Failed or Succeeded pod frequently holds the exact crash evidence
    the investigation needs. Filtering to Running is the classic mistake."""
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-pod", phase=phase)
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert [t.pod_name for t in result.targets] == [f"{DEFAULT_APP}-pod"]
    assert result.targets[0].phase == phase


# ======================================================================
# Container selection -- scenario 9
# ======================================================================

def test_sidecars_are_dropped(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-meshed", containers=("app", "istio-proxy"))
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert [t.container for t in result.targets] == ["app"]


def test_multi_container_pod_yields_one_target_each(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-multi", containers=("app", "worker"))
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert sorted(t.container for t in result.targets) == ["app", "worker"]


def test_all_sidecar_pod_keeps_all_containers(monkeypatch, tmp_path):
    """If the denylist would remove every container, keep them: an empty
    trace is worse than a proxy's logs."""
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-only-proxy", containers=("istio-proxy",))
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert [t.container for t in result.targets] == ["istio-proxy"]


def test_sidecar_denylist_is_configurable(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-custom", containers=("app", "my-sidecar"))
    _use_fixtures(monkeypatch, tmp_path)
    monkeypatch.setenv("K8S_SIDECAR_DENYLIST", "my-sidecar")

    result = discovery.discover_targets()
    assert [t.container for t in result.targets] == ["app"]


# ======================================================================
# Restart detection -- feeds Phase 3's previous=True read
# ======================================================================

def test_restart_count_is_captured(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-crashy", containers=("app",),
               restart_counts={"app": 3}, previous="pre-crash line\n")
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    target = result.targets[0]
    assert target.restart_count == 3
    assert target.restarted is True


def test_no_restart_means_no_previous_read(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-stable", containers=("app",))
    _use_fixtures(monkeypatch, tmp_path)

    assert discovery.discover_targets().targets[0].restarted is False


# ======================================================================
# Fan-out cap -- scenario 15
# ======================================================================

def test_max_pods_caps_fanout_and_records_a_gap(monkeypatch, tmp_path):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(5):
        _write_pod(
            tmp_path, "enu", f"{DEFAULT_APP}-{i}",
            start_time=(base + timedelta(minutes=i)).isoformat(),
        )
    _use_fixtures(monkeypatch, tmp_path)
    monkeypatch.setenv("K8S_MAX_PODS", "2")

    result = discovery.discover_targets()

    assert result.truncated is True
    assert len(result.targets) == 2
    # Most recently started survive the cap.
    assert sorted(t.pod_name for t in result.targets) == [f"{DEFAULT_APP}-3", f"{DEFAULT_APP}-4"]

    assert len(result.gaps) == 1
    assert result.gaps[0].gap_type == GapType.TRUNCATED
    assert "5 pods matched" in result.gaps[0].detail


def test_no_gap_when_under_the_cap(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", f"{DEFAULT_APP}-solo")
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()
    assert result.truncated is False
    assert result.gaps == []


# ======================================================================
# No match -- scenario 10
# ======================================================================

def test_no_matching_pods_is_empty_but_ok(monkeypatch, tmp_path):
    """Nothing matched is a successful, informative result -- not a failure."""
    _use_fixtures(monkeypatch, tmp_path)  # namespace exists; no pods written

    result = discovery.discover_targets()
    assert result.ok is True
    assert result.is_empty is True


def test_pods_present_but_none_match_the_name_is_empty_but_ok(monkeypatch, tmp_path):
    _write_pod(tmp_path, "enu", "totally-unrelated-service")
    _use_fixtures(monkeypatch, tmp_path)

    result = discovery.discover_targets()  # default app "enu-biometric" matches nothing here
    assert result.ok is True
    assert result.is_empty is True


# ======================================================================
# API path -- scenario 12 and live-cluster wiring
# ======================================================================

def _fake_pod(name, phase="Running", containers=("app",), restart=0, labels=None):
    from kubernetes.client.models import (
        V1Container, V1ContainerStatus, V1ObjectMeta, V1Pod, V1PodSpec, V1PodStatus,
    )
    return V1Pod(
        metadata=V1ObjectMeta(name=name, namespace="enu", labels=labels or {}),
        spec=V1PodSpec(containers=[V1Container(name=c) for c in containers]),
        status=V1PodStatus(
            phase=phase,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            container_statuses=[
                V1ContainerStatus(name=c, image="i", image_id="i", ready=True,
                                  restart_count=restart)
                for c in containers
            ],
        ),
    )


def test_discovery_reads_from_the_api_when_no_fixtures(monkeypatch):
    """Default mode: no label_selector is sent to the API at all -- every
    pod in the namespace is listed and filtered client-side by name."""
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    api = MagicMock()
    api.list_namespaced_pod.return_value = MagicMock(items=[
        _fake_pod(f"{DEFAULT_APP}-live-pod"),
        _fake_pod("unrelated-pod"),
    ])

    with patch.object(k8s_client_module, "get_client", return_value=api), \
         patch.object(k8s_client_module, "is_available", return_value=True):
        result = discovery.discover_targets()

    assert [t.pod_name for t in result.targets] == [f"{DEFAULT_APP}-live-pod"]

    kwargs = api.list_namespaced_pod.call_args.kwargs
    assert kwargs["namespace"] == "enu"
    assert "label_selector" not in kwargs
    # A request timeout is mandatory -- the ES client lacked one until 1.9.
    assert "_request_timeout" in kwargs


def test_discovery_sends_label_selector_to_the_api_in_label_mode(monkeypatch):
    monkeypatch.setenv("K8S_SERVICE_MAP", json.dumps({
        "enu-biometric": {"namespace": "enu", "label_selector": "app=enu-biometric"}
    }))

    api = MagicMock()
    api.list_namespaced_pod.return_value = MagicMock(items=[])

    with patch.object(k8s_client_module, "get_client", return_value=api), \
         patch.object(k8s_client_module, "is_available", return_value=True):
        discovery.discover_targets(app="enu-biometric")

    assert api.list_namespaced_pod.call_args.kwargs["label_selector"] == "app=enu-biometric"


def test_rbac_denial_on_pod_list_is_reported_not_swallowed(monkeypatch):
    """A 403 on the pod list itself (distinct from the namespace read) is a
    configuration error, never an empty result."""
    from kubernetes.client.exceptions import ApiException

    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    api = MagicMock()
    api.list_namespaced_pod.side_effect = ApiException(status=403, reason="Forbidden")

    with patch.object(k8s_client_module, "get_client", return_value=api), \
         patch.object(k8s_client_module, "is_available", return_value=True):
        result = discovery.discover_targets()

    assert result.ok is False
    assert result.is_empty is True
    assert "ApiException" in result.reason
