"""
Phase 5 of KUBERNETES_LOGS_PLAN.md -- the operator CLI.

The CLI is the de-risking gate: it is the first thing that meets a real
cluster. These tests drive it end to end against fixtures so its exit codes
and reporting are trustworthy before anyone points it at production.
"""
import json

import pytest

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.tools import fetch_pod_logs

KUBELET_TS = "2026-01-01T10:15:30.000000000Z"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("K8S_FIXTURE_DIR", "K8S_DEFAULT_NAMESPACE", "K8S_DEFAULT_APP",
                "K8S_SERVICE_MAP", "K8S_MAX_PODS", "K8S_CONTEXT_LINES_BEFORE",
                "K8S_CONTEXT_LINES_AFTER", "K8S_DEFAULT_SINCE_HOURS",
                "KUBECONFIG_PATH"):
        monkeypatch.delenv(var, raising=False)
    k8s_client_module.reset_client()
    yield
    k8s_client_module.reset_client()


def _fixture_pod(root, namespace, pod, lines, *, containers=("app",),
                 restart_counts=None, previous=None):
    pod_dir = root / namespace / pod
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if previous:
        (pod_dir / "previous.log").write_text("\n".join(previous) + "\n", encoding="utf-8")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running",
        "labels": {"app": "enu-biometric"},
        "containers": list(containers),
        "restart_counts": restart_counts or {},
    }), encoding="utf-8")


def _run(argv, monkeypatch):
    monkeypatch.setattr("sys.argv", ["fetch_pod_logs"] + argv)
    return fetch_pod_logs.main()


# ======================================================================
# Exit codes -- the three outcomes must stay distinguishable
# ======================================================================

def test_exit_1_when_cluster_is_unavailable(monkeypatch, capsys):
    """Could-not-look."""
    monkeypatch.setenv("KUBECONFIG_PATH", "/nonexistent/kubeconfig.yaml")
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    k8s_client_module.reset_client()

    assert _run(["--identifier", "evt-1"], monkeypatch) == 1
    assert "UNAVAILABLE" in capsys.readouterr().out


def test_exit_2_when_nothing_matched(monkeypatch, tmp_path, capsys):
    """Looked-and-found-nothing is a distinct, informative outcome."""
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [f"{KUBELET_TS} INFO unrelated"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    assert _run(["--identifier", "evt-missing"], monkeypatch) == 2


def test_exit_0_when_logs_are_found(monkeypatch, tmp_path, capsys):
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        f"{KUBELET_TS} INFO processing evt-123",
        f"{KUBELET_TS} ERROR dedup rejected evt-123",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    assert _run(["--identifier", "evt-123"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert "dedup rejected evt-123" in out
    assert "enu-biometric-a" in out


def test_requires_an_identifier_or_no_filter(monkeypatch, capsys):
    assert _run([], monkeypatch) == 1
    assert "provide --identifier" in capsys.readouterr().err


# ======================================================================
# Discovery reporting -- answers Open Question 2
# ======================================================================

def test_list_pods_reports_targets_without_reading_logs(monkeypatch, tmp_path, capsys):
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [f"{KUBELET_TS} INFO x"],
                 containers=("app", "istio-proxy"))
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    assert _run(["--list-pods"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert "enu-biometric-a / app" in out
    assert "istio-proxy" not in out     # sidecar excluded
    assert "pod name contains" in out


def test_namespace_and_app_overrides_are_reported(monkeypatch, tmp_path, capsys):
    _fixture_pod(tmp_path, "other-ns", "enu-biometric-x", [f"{KUBELET_TS} INFO x"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    _run(["--list-pods", "--namespace", "other-ns", "--app", "enu-biometric"], monkeypatch)
    out = capsys.readouterr().out
    assert "other-ns" in out
    assert "pod name contains 'enu-biometric'" in out


# ======================================================================
# Identifier discovery -- answers Open Question 1
# ======================================================================

def test_multiple_identifiers_are_searched(monkeypatch, tmp_path, capsys):
    """We do not know whether services log eventId or refId; both are tried."""
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        f"{KUBELET_TS} INFO only the refid REF-9 appears here",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    assert _run(["--identifier", "EVT-1", "REF-9"], monkeypatch) == 0
    assert "REF-9" in capsys.readouterr().out


def test_no_filter_dumps_everything(monkeypatch, tmp_path, capsys):
    """The escape hatch for when nothing matches: see what the service
    actually logs."""
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        f"{KUBELET_TS} INFO line one",
        f"{KUBELET_TS} INFO line two",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    assert _run(["--no-filter"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert "line one" in out and "line two" in out


# ======================================================================
# Gap reporting -- answers Open Question 3
# ======================================================================

def test_gaps_are_reported(monkeypatch, tmp_path, capsys):
    """Rotation is detected because the only line is far newer than the
    requested window start."""
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        "2026-01-01T10:00:00Z INFO evt-123 recent only",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    _run(["--identifier", "evt-123", "--since-hours", "100000"], monkeypatch)
    out = capsys.readouterr().out
    assert "EVIDENCE GAPS" in out
    assert "LOG_ROTATION" in out


def test_no_gaps_reports_full_coverage(monkeypatch, tmp_path, capsys):
    """A long-lived pod whose stream reaches back past the requested window
    start is fully covered -- no gaps of any kind."""
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    before_window = (now - timedelta(hours=3)).isoformat()
    recent = (now - timedelta(minutes=5)).isoformat()

    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        f"{before_window} INFO evt-123 started before the window",
        f"{recent} INFO evt-123 still going",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    _run(["--identifier", "evt-123", "--since-hours", "2"], monkeypatch)
    assert "fully covered" in capsys.readouterr().out


def test_rotation_gap_uses_the_unfiltered_stream_boundary(monkeypatch, tmp_path, capsys):
    """The pod's stream reaches back before the window, but the only line
    MATCHING the identifier is recent. Rotation must NOT be reported: judging
    it from filtered records would fire this gap on nearly every fetch and
    train the agents to ignore the banner."""
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)

    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        f"{(now - timedelta(hours=3)).isoformat()} INFO unrelated older traffic",
        f"{(now - timedelta(minutes=2)).isoformat()} INFO evt-123 appears only now",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    _run(["--identifier", "evt-123", "--since-hours", "2"], monkeypatch)
    out = capsys.readouterr().out
    assert "LOG_ROTATION" not in out
    assert "evt-123 appears only now" in out


def test_parse_quality_is_reported(monkeypatch, tmp_path, capsys):
    """Surfaces the highest-risk failure mode: a format the parser cannot
    read levels from, which would make ERROR detection unreliable."""
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [
        f"{KUBELET_TS} evt-1 prose with no level at all",
        f"{KUBELET_TS} evt-1 more prose",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    _run(["--identifier", "evt-1"], monkeypatch)
    out = capsys.readouterr().out
    assert "PARSE QUALITY" in out
    assert "WARNING: most lines had no recognisable level" in out


# ======================================================================
# Output files
# ======================================================================

def test_output_file_contains_the_trace(monkeypatch, tmp_path, capsys):
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [f"{KUBELET_TS} ERROR evt-9 boom"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    out_path = tmp_path / "trace.txt"

    _run(["--identifier", "evt-9", "--output", str(out_path)], monkeypatch)

    assert "evt-9 boom" in out_path.read_text()


def test_json_output_is_structured(monkeypatch, tmp_path, capsys):
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [f"{KUBELET_TS} ERROR evt-9 boom"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    json_path = tmp_path / "out.json"

    _run(["--identifier", "evt-9", "--json", str(json_path)], monkeypatch)

    payload = json.loads(json_path.read_text())
    assert payload["identifiers"] == ["evt-9"]
    assert payload["pods"][0]["pod_name"] == "enu-biometric-a"
    assert payload["pods"][0]["matched_lines"] == 1
    assert payload["records"][0]["level"] == "ERROR"


def test_per_pod_counts_are_reported(monkeypatch, tmp_path, capsys):
    """Aggregate counts would hide one replica holding every matching line --
    exactly the signal that tells an operator the selector is too wide."""
    _fixture_pod(tmp_path, "enu", "enu-biometric-a", [f"{KUBELET_TS} INFO evt-7 here"])
    _fixture_pod(tmp_path, "enu", "enu-biometric-b", [f"{KUBELET_TS} INFO nothing relevant"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    _run(["--identifier", "evt-7"], monkeypatch)
    out = capsys.readouterr().out
    assert "enu-biometric-a/app: 1 lines" in out
    assert "enu-biometric-b/app: 0 lines" in out


def test_restart_pulls_previous_instance(monkeypatch, tmp_path, capsys):
    _fixture_pod(
        tmp_path, "enu", "enu-biometric-crashy",
        [f"{KUBELET_TS} INFO evt-5 after restart"],
        restart_counts={"app": 1},
        previous=[f"{KUBELET_TS} ERROR evt-5 OutOfMemoryError"],
    )
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")

    assert _run(["--identifier", "evt-5"], monkeypatch) == 0
    assert "OutOfMemoryError" in capsys.readouterr().out
