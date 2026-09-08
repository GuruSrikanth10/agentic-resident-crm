"""
Phase 3 of KUBERNETES_LOGS_PLAN.md -- pod log retrieval.

Covers scenarios 1, 2, 3, 6 (restarts), 13 (pod vanished), 15 (byte cap),
and the two mandatory call parameters (`timestamps=True`,
`_preload_content=False`) whose omission is silent but expensive.
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import retrieval
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.types import GapType, TimeWindow

KUBELET_TS = "2026-01-01T10:15:30.000000000Z"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("K8S_FIXTURE_DIR", "K8S_MAX_BYTES_PER_POD",
                "K8S_FETCH_CONCURRENCY", "K8S_REQUEST_TIMEOUT_SECONDS"):
        monkeypatch.delenv(var, raising=False)
    k8s_client_module.reset_client()
    yield
    k8s_client_module.reset_client()


def _target(pod="pod-a", container="app", restart=0, namespace="enu"):
    return PodTarget(
        namespace=namespace, pod_name=pod, container=container,
        restart_count=restart, phase="Running",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _write_fixture(root, namespace, pod, *, current, previous=None,
                   containers=("app",), restart_counts=None):
    pod_dir = root / namespace / pod
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text(current, encoding="utf-8")
    if previous is not None:
        (pod_dir / "previous.log").write_text(previous, encoding="utf-8")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running",
        "labels": {"app": "enu-biometric"},
        "containers": list(containers),
        "restart_counts": restart_counts or {},
    }), encoding="utf-8")


# ======================================================================
# Basic reads
# ======================================================================

def test_reads_and_parses_a_single_pod(monkeypatch, tmp_path):
    _write_fixture(tmp_path, "enu", "pod-a",
                   current=f"{KUBELET_TS} INFO one\n{KUBELET_TS} ERROR two\n")
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_pod_logs(_target(), TimeWindow.default())

    assert outcome.ok is True
    assert [r["level"] for r in outcome.records] == ["INFO", "ERROR"]
    assert all(r["pod_name"] == "pod-a" for r in outcome.records)
    assert all(r["container"] == "app" for r in outcome.records)
    assert all(r["container_instance"] == "current" for r in outcome.records)
    assert all(r["source"] == "kubernetes" for r in outcome.records)


def test_fan_out_across_replicas_merges_by_timestamp(monkeypatch, tmp_path):
    _write_fixture(tmp_path, "enu", "pod-a",
                   current="2026-01-01T10:00:02Z INFO from-a\n")
    _write_fixture(tmp_path, "enu", "pod-b",
                   current="2026-01-01T10:00:01Z INFO from-b\n")
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_all(
        [_target("pod-a"), _target("pod-b")], TimeWindow.default()
    )

    assert outcome.pods_queried == 2
    assert outcome.pods_failed == 0
    # Ordered by kubelet timestamp, not by pod iteration order. Plain-text
    # messages keep the level token: the raw line is preserved verbatim
    # rather than surgically edited.
    assert [r["message"] for r in outcome.records] == ["INFO from-b", "INFO from-a"]


def test_empty_target_list_is_handled():
    outcome = retrieval.read_all([], TimeWindow.default())
    assert outcome.records == []
    assert outcome.pods_queried == 0


# ======================================================================
# Restart handling -- scenarios 3 and 6
# ======================================================================

def test_previous_instance_is_read_after_a_restart(monkeypatch, tmp_path):
    """The pre-crash lines are usually the most valuable in the trace, and the
    default read makes them invisible."""
    _write_fixture(
        tmp_path, "enu", "crashy",
        current=f"{KUBELET_TS} INFO restarted clean\n",
        previous="2026-01-01T10:00:00Z ERROR OutOfMemoryError before crash\n",
        restart_counts={"app": 1},
    )
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_pod_logs(_target("crashy", restart=1), TimeWindow.default())

    instances = [r["container_instance"] for r in outcome.records]
    assert "previous" in instances and "current" in instances
    assert any("OutOfMemoryError" in r["message"] for r in outcome.records)


def test_previous_is_not_read_without_a_restart(monkeypatch, tmp_path):
    _write_fixture(
        tmp_path, "enu", "stable",
        current=f"{KUBELET_TS} INFO fine\n",
        previous="2026-01-01T10:00:00Z ERROR stale should not appear\n",
    )
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_pod_logs(_target("stable", restart=0), TimeWindow.default())

    assert all(r["container_instance"] == "current" for r in outcome.records)
    assert not any("stale" in r["message"] for r in outcome.records)


def test_missing_previous_log_is_not_an_error(monkeypatch, tmp_path):
    """`previous=True` returns HTTP 400 when no previous container exists --
    expected, not a failure."""
    _write_fixture(tmp_path, "enu", "no-prev",
                   current=f"{KUBELET_TS} INFO fine\n", restart_counts={"app": 2})
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_pod_logs(_target("no-prev", restart=2), TimeWindow.default())

    assert outcome.ok is True
    assert len(outcome.records) == 1


def test_api_400_on_previous_is_swallowed(monkeypatch):
    from kubernetes.client.exceptions import ApiException

    def fake_open(target, window, previous):
        if previous:
            raise ApiException(status=400, reason="previous terminated container not found")
        return iter([(f"{KUBELET_TS} INFO current only", 30, False)])

    with patch.object(retrieval, "_open_stream", side_effect=fake_open):
        outcome = retrieval.read_pod_logs(_target(restart=1), TimeWindow.default())

    assert outcome.ok is True
    assert len(outcome.records) == 1


def test_previous_records_sort_before_current_for_same_timestamp(monkeypatch, tmp_path):
    same_ts = "2026-01-01T10:00:00Z"
    _write_fixture(
        tmp_path, "enu", "pod-a",
        current=f"{same_ts} INFO current-line\n",
        previous=f"{same_ts} ERROR previous-line\n",
        restart_counts={"app": 1},
    )
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_all([_target(restart=1)], TimeWindow.default())
    assert [r["container_instance"] for r in outcome.records] == ["previous", "current"]
    assert "previous-line" in outcome.records[0]["message"]
    assert "current-line" in outcome.records[1]["message"]


# ======================================================================
# Failure handling -- scenarios 12 and 13
# ======================================================================

def test_pod_vanished_produces_a_gap_not_a_crash():
    from kubernetes.client.exceptions import ApiException

    with patch.object(retrieval, "_open_stream",
                      side_effect=ApiException(status=404, reason="Not Found")):
        outcome = retrieval.read_pod_logs(_target(), TimeWindow.default())

    assert outcome.ok is False
    assert [g.gap_type for g in outcome.gaps] == [GapType.POD_VANISHED]


def test_one_failing_pod_does_not_sink_the_others(monkeypatch, tmp_path):
    _write_fixture(tmp_path, "enu", "good", current=f"{KUBELET_TS} INFO fine\n")
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    # "missing" has no fixture directory, so its read raises.
    outcome = retrieval.read_all(
        [_target("good"), _target("missing")], TimeWindow.default()
    )

    assert outcome.pods_queried == 2
    assert outcome.pods_failed == 1
    assert len(outcome.records) == 1


def test_rbac_denial_on_log_read_is_reported():
    from kubernetes.client.exceptions import ApiException

    with patch.object(retrieval, "_open_stream",
                      side_effect=ApiException(status=403, reason="Forbidden")):
        outcome = retrieval.read_pod_logs(_target(), TimeWindow.default())

    assert outcome.ok is False
    assert "ApiException" in outcome.error


# ======================================================================
# Byte cap -- scenario 15
# ======================================================================

def test_byte_cap_truncates_and_records_a_gap(monkeypatch, tmp_path):
    big = "\n".join(f"{KUBELET_TS} INFO line-{i}" for i in range(500)) + "\n"
    _write_fixture(tmp_path, "enu", "chatty", current=big)
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_MAX_BYTES_PER_POD", "200")

    outcome = retrieval.read_pod_logs(_target("chatty"), TimeWindow.default())

    assert outcome.truncated is True
    assert len(outcome.records) < 500
    assert any(g.gap_type == GapType.TRUNCATED for g in outcome.gaps)


# ======================================================================
# The two mandatory API parameters
# ======================================================================

def test_api_call_uses_the_mandatory_parameters(monkeypatch):
    """timestamps=True gives a reliable ordering key; _preload_content=False
    keeps a huge log from being buffered into memory. Both are load-bearing."""
    api = MagicMock()
    response = MagicMock()
    response.stream.return_value = iter([b"2026-01-01T10:00:00Z INFO hello\n"])
    api.read_namespaced_pod_log.return_value = response

    with patch.object(k8s_client_module, "get_client", return_value=api):
        retrieval.read_pod_logs(_target(), TimeWindow(hours=3))

    kwargs = api.read_namespaced_pod_log.call_args.kwargs
    assert kwargs["timestamps"] is True
    assert kwargs["_preload_content"] is False
    assert kwargs["since_seconds"] == 10800          # 3h window honoured
    assert kwargs["limit_bytes"] == 10 * 1024 * 1024
    assert kwargs["container"] == "app"
    assert "_request_timeout" in kwargs


def test_streaming_reassembles_lines_split_across_chunks():
    """A line straddling two chunks must not be corrupted."""
    api = MagicMock()
    response = MagicMock()
    response.stream.return_value = iter([
        b"2026-01-01T10:00:00Z ERROR first pa",
        b"rt and second part\n2026-01-01T10:00:01Z INFO next\n",
    ])
    api.read_namespaced_pod_log.return_value = response

    with patch.object(k8s_client_module, "get_client", return_value=api):
        outcome = retrieval.read_pod_logs(_target(), TimeWindow.default())

    assert outcome.records[0]["message"] == "ERROR first part and second part"
    assert outcome.records[0]["level"] == "ERROR"


def test_unavailable_client_fails_cleanly():
    with patch.object(k8s_client_module, "get_client", return_value=None):
        outcome = retrieval.read_pod_logs(_target(), TimeWindow.default())

    assert outcome.ok is False
    assert "unavailable" in outcome.error.lower()


# ======================================================================
# Filter seam (Phase 4 supplies the real identifier filter)
# ======================================================================

def test_selector_is_applied(monkeypatch, tmp_path):
    from src.log_pipeline.sources.k8s.filtering import (
        ContextWindowSelector, build_matcher,
    )

    _write_fixture(tmp_path, "enu", "pod-a", current="\n".join([
        f"{KUBELET_TS} INFO keep evt-123",
        f"{KUBELET_TS} INFO drop this one",
        f"{KUBELET_TS} ERROR keep evt-123 too",
    ]) + "\n")
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    selector = ContextWindowSelector(build_matcher(["evt-123"]), before=0, after=0)
    outcome = retrieval.read_pod_logs(_target(), TimeWindow.default(), selector=selector)

    assert len(outcome.records) == 2
    assert all("evt-123" in r["message"] for r in outcome.records)


def test_default_filter_keeps_everything(monkeypatch, tmp_path):
    _write_fixture(tmp_path, "enu", "pod-a",
                   current=f"{KUBELET_TS} INFO a\n{KUBELET_TS} INFO b\n")
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    outcome = retrieval.read_pod_logs(_target(), TimeWindow.default())
    assert len(outcome.records) == 2


# ======================================================================
# Concurrency bound
# ======================================================================

def test_fan_out_concurrency_is_bounded(monkeypatch, tmp_path):
    for i in range(6):
        _write_fixture(tmp_path, "enu", f"pod-{i}",
                       current=f"{KUBELET_TS} INFO line\n")
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_FETCH_CONCURRENCY", "2")

    captured = {}
    real_pool = retrieval.ThreadPoolExecutor

    def spy_pool(max_workers=None, **kwargs):
        captured["max_workers"] = max_workers
        return real_pool(max_workers=max_workers, **kwargs)

    with patch.object(retrieval, "ThreadPoolExecutor", side_effect=spy_pool):
        outcome = retrieval.read_all(
            [_target(f"pod-{i}") for i in range(6)], TimeWindow.default()
        )

    assert captured["max_workers"] == 2
    assert outcome.pods_queried == 6
