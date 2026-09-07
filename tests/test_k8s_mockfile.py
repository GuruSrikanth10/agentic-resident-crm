"""
Offline mock log file for the Kubernetes source (`K8S_MOCK_LOG_FILE`).

The contract these pin down is that the mock replaces the *transport only*.
Everything the live path does after bytes arrive -- identifier matching,
context windows, `identifier_match` stamping, level parsing, redaction, gap
detection, and the record shape Stages 2-4 consume -- must still happen, or
an investigation run offline is not evidence about the real one.
"""
import pytest

from src.log_pipeline.sources.k8s import mockfile
from src.log_pipeline.sources.k8s.source import KubernetesLogSource
from src.log_pipeline.types import (
    REQUIRED_RECORD_KEYS,
    FetchContext,
    GapType,
    TimeWindow,
)

TARGET = "a5d5793f-41f1-42c3-95c3-fc81a1f3bdbf"
OTHER = "db35bfda-08e9-40d1-872a-6c59bb217023"

RENDERED = f"""\
[2026-09-07T14:49:31.806106042+05:30] [enu-biometric@mock-file] [INFO] [context] Processed PostEnrolment candidate completion response for RefId :: {TARGET}
[2026-09-07T14:49:31.806217935+05:30] [enu-biometric@mock-file] [INFO] [context] Applicants 0 unparked successfully corresponding to candidate :{TARGET}
[2026-09-07T14:49:31.900000000+05:30] [enu-biometric@mock-file] [INFO] Heartbeat tick carrying no identifier
[2026-09-07T14:49:32.100000000+05:30] [enu-dedup@mock-file] [ERROR] Dedup failed for RefId :: {TARGET} reason MAN_DEDUP
\tat com.uidai.dedup.Matcher.match(Matcher.java:88)
[2026-09-07T14:49:33.000000000+05:30] [enu-biometric@mock-file] [INFO] Packet status updated to REJECTED for {OTHER}
"""


@pytest.fixture
def mock_log(tmp_path, monkeypatch):
    path = tmp_path / "found_logs.txt"
    path.write_text(RENDERED, encoding="utf-8")
    monkeypatch.setenv("K8S_MOCK_LOG_FILE", str(path))
    monkeypatch.setenv("K8S_REDACT_ENABLED", "false")
    monkeypatch.setenv("K8S_CONTEXT_LINES_BEFORE", "1")
    monkeypatch.setenv("K8S_CONTEXT_LINES_AFTER", "2")
    return path


def _fetch(event_id=TARGET, **ctx_kwargs):
    return KubernetesLogSource().fetch(
        event_id, TimeWindow(hours=2),
        FetchContext(event_id=event_id, **ctx_kwargs),
    )


def test_inactive_without_the_env_var(monkeypatch):
    monkeypatch.delenv("K8S_MOCK_LOG_FILE", raising=False)
    assert mockfile.is_active() is False


def test_inactive_when_the_file_does_not_exist(monkeypatch, tmp_path):
    monkeypatch.setenv("K8S_MOCK_LOG_FILE", str(tmp_path / "nope.txt"))
    # Falling back to the live path (which then fails loudly) beats silently
    # returning zero records, which reads as "looked, found nothing".
    assert mockfile.is_active() is False


def test_records_satisfy_the_source_contract(mock_log):
    result = _fetch()
    assert result.ok and result.records
    for record in result.records:
        for key in REQUIRED_RECORD_KEYS:
            assert record.get(key) is not None, key
        assert record["source"] == "kubernetes"


def test_identifier_match_is_recomputed_not_read_from_the_file(mock_log):
    """The `[context]` tag in the file is recomputed, not trusted.

    Lines 1-2 arrive tagged `[context]` but *do* carry the target id, so a
    fresh fetch must mark them as matches. Trusting the file's own tag would
    make every replayed trace permanently unattributed.
    """
    records = _fetch().records
    first = records[0]
    assert TARGET in first["message"]
    assert first["identifier_match"] is True
    assert "[context]" not in first["message"]


def test_context_lines_are_labelled_as_context(mock_log):
    records = _fetch().records
    heartbeat = [r for r in records if "Heartbeat" in r["message"]]
    assert heartbeat and heartbeat[0]["identifier_match"] is False


def test_two_event_ids_read_the_same_file_differently(mock_log):
    """The selector runs per fetch, exactly as against a live pod."""
    assert len(_fetch(TARGET).records) != len(_fetch(OTHER).records)


def test_rendered_format_recovers_level_app_and_pod(mock_log):
    errors = [r for r in _fetch().records if r["level"] == "ERROR"]
    assert len(errors) == 1
    assert errors[0]["app_name"] == "enu-dedup"
    assert errors[0]["pod_name"] == "mock-file"


def test_error_level_survives_so_branch_on_error_can_fire(mock_log):
    # branch_on_error keys purely off level == "ERROR"; if the mock flattened
    # levels to INFO every stuck packet would replay as a clean rejection.
    assert any(r["level"] == "ERROR" for r in _fetch().records)


def test_continuation_lines_inherit_the_preceding_timestamp(mock_log):
    """A file has no kubelet timestamp on stack-trace frames; a live read does.

    Blank timestamps would sort to the front of the trace in `cluster_logs`,
    detaching frames from the exception they belong to.
    """
    records = _fetch().records
    frame = [r for r in records if "Matcher.java" in r["message"]]
    assert frame and frame[0]["timestamp"] == "2026-09-07T14:49:32.100000000+05:30"


def test_plain_kubelet_lines_fall_through_to_parse_line(tmp_path, monkeypatch):
    path = tmp_path / "raw.txt"
    path.write_text(
        f"2026-09-07T09:19:31.806106042Z ERROR Dedup failed for {TARGET}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("K8S_MOCK_LOG_FILE", str(path))
    monkeypatch.setenv("K8S_REDACT_ENABLED", "false")
    records = _fetch().records
    assert len(records) == 1
    assert records[0]["level"] == "ERROR"


def test_byte_cap_raises_a_truncated_gap(mock_log, monkeypatch):
    monkeypatch.setenv("K8S_MAX_BYTES_PER_POD", "200")
    result = _fetch()
    assert any(g.gap_type is GapType.TRUNCATED for g in result.gaps)


def test_window_is_not_applied_by_default(mock_log):
    """A captured trace is replayed long after its timestamps.

    Honouring the look-back by default would return nothing and present it as
    a successful empty fetch -- the one failure mode a debugging aid must not
    have.
    """
    assert KubernetesLogSource().fetch(
        TARGET, TimeWindow(hours=0.001), FetchContext(event_id=TARGET)
    ).records


def test_window_is_applied_when_explicitly_enabled(mock_log, monkeypatch):
    monkeypatch.setenv("K8S_MOCK_APPLY_WINDOW", "true")
    assert not KubernetesLogSource().fetch(
        TARGET, TimeWindow(hours=0.001), FetchContext(event_id=TARGET)
    ).records


def test_redaction_runs_over_mock_records(mock_log, monkeypatch):
    monkeypatch.setenv("K8S_REDACT_ENABLED", "true")
    result = _fetch()
    # The searched identifier is allowlisted: scrubbing it would destroy the
    # investigation, exactly as on the live path.
    assert any(TARGET in r["message"] for r in result.records)


def test_diagnostics_report_the_kubernetes_source(mock_log):
    diagnostics = _fetch().diagnostics
    assert diagnostics.source == "kubernetes"
    assert diagnostics.records_returned > 0
    assert diagnostics.bytes_read > 0
    # No pod was queried; claiming otherwise would corrupt the fetch metrics.
    assert diagnostics.pods_queried == 0


def test_mock_does_not_write_a_snapshot(mock_log, monkeypatch):
    """A mock record persisted under a real event_id could later be reused by
    a live run as though it had come from the cluster."""
    calls = []
    monkeypatch.setattr(
        "src.log_pipeline.snapshot.save",
        lambda *a, **k: calls.append(a),
    )
    _fetch()
    assert calls == []
