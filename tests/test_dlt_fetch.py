"""
Phase 5 of DLT_PLAN.md -- log window derivation and the DLT fetch endpoint.

`test_window_anchors_on_the_last_attempt_not_the_original` is the Trap 2
regression. Anchoring on `kafka_original-timestamp` searches a window 43 hours
stale and returns nothing, silently -- no exception, no gap, just an empty
trace and an investigation built on it.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.api.dlt_routes import (
    FETCHED_LOGS_ARTIFACT,
    HEADERS_ARTIFACT,
    PARSED_TRACE_ARTIFACT,
    TRACE_ARTIFACT,
    build_failure,
    fetch_dlt_logs,
)
from src.dlt import case_storage
from src.dlt.headers import parse_headers
from src.dlt.window import derive_window
from src.models.dlt_schemas import DltMessage

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]

LAST_ATTEMPT_MS = 1787019608511      # 2026-08-18T02:20:08.511Z
ORIGINAL_MS = 1786864805192          # 2026-08-16T07:20:05.192Z


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_LOG_LEAD_SECONDS", "DLT_LOG_TRAIL_SECONDS",
                "DLT_MAX_LOG_AGE_SECONDS", "DLT_REFID_PATH", "DLT_REGISTRY_PATH",
                "CASEBOOK_STORAGE_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    case_storage.reset_cache()
    monkeypatch.setattr("src.api.dlt_routes.publish_to_dlt_analysis_queue",
                        lambda message: True)
    yield
    case_storage.reset_cache()


def message(ref_id="REF-1", headers=None, case_id="dlt-T-63-3352"):
    return DltMessage(case_id=case_id,
                      headers=headers if headers is not None else dict(REFERENCE),
                      payload={"packetMetaData": {"refId": ref_id}} if ref_id else {},
                      ref_id=ref_id)


# ======================================================================
# Trap 2 -- the log-window anchor
# ======================================================================

def test_window_anchors_on_the_last_attempt_not_the_original():
    window = derive_window(parse_headers(REFERENCE), now_ms=LAST_ATTEMPT_MS + 60_000)

    assert window is not None
    assert window.anchor_ms == LAST_ATTEMPT_MS
    assert window.anchor_ms != ORIGINAL_MS
    assert window.anchor_iso.startswith("2026-08-18T02:20:08")
    assert window.anchor_is_fallback is False


def test_the_two_candidate_anchors_are_43_hours_apart():
    """Which is why picking the wrong one is silent and total."""
    gap_hours = (LAST_ATTEMPT_MS - ORIGINAL_MS) / 3_600_000
    assert round(gap_hours, 1) == 43.0


def test_window_brackets_the_anchor_with_lead_and_trail(monkeypatch):
    monkeypatch.setenv("DLT_LOG_LEAD_SECONDS", "300")
    monkeypatch.setenv("DLT_LOG_TRAIL_SECONDS", "120")
    window = derive_window(parse_headers(REFERENCE), now_ms=LAST_ATTEMPT_MS)

    assert window.start_ms == LAST_ATTEMPT_MS - 300_000
    assert window.end_ms == LAST_ATTEMPT_MS + 120_000


def test_window_falls_back_to_the_original_timestamp():
    headers = {k: v for k, v in REFERENCE.items() if k != "retry_topic-backoff-timestamp"}
    window = derive_window(parse_headers(headers), now_ms=ORIGINAL_MS + 1000)

    assert window.anchor_ms == ORIGINAL_MS
    assert window.anchor_is_fallback is True


def test_no_timestamp_at_all_yields_no_window():
    assert derive_window(parse_headers({})) is None


def test_window_older_than_the_cap_is_flagged_too_old(monkeypatch):
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "3600")
    window = derive_window(parse_headers(REFERENCE),
                           now_ms=LAST_ATTEMPT_MS + 10 * 3_600_000)
    assert window.too_old is True

    fresh = derive_window(parse_headers(REFERENCE), now_ms=LAST_ATTEMPT_MS + 60_000)
    assert fresh.too_old is False


def test_time_window_is_relative_to_now():
    """The Kubernetes source takes since_seconds, not an absolute start."""
    window = derive_window(parse_headers(REFERENCE), now_ms=LAST_ATTEMPT_MS + 3_600_000)
    time_window = window.to_time_window(now_ms=LAST_ATTEMPT_MS + 3_600_000)

    # One hour since the anchor, plus the 300s lead.
    assert round(time_window.hours, 3) == round(1 + 300 / 3600, 3)


def test_malformed_window_env_falls_back_to_defaults(monkeypatch):
    monkeypatch.setenv("DLT_LOG_LEAD_SECONDS", "not-a-number")
    window = derive_window(parse_headers(REFERENCE), now_ms=LAST_ATTEMPT_MS)
    assert window.start_ms == LAST_ATTEMPT_MS - 300_000


def test_window_describe_is_readable():
    described = derive_window(parse_headers(REFERENCE),
                              now_ms=LAST_ATTEMPT_MS).describe()
    assert "2026-08-18T02:20:08" in described
    assert "anchored on" in described


# ======================================================================
# build_failure
# ======================================================================

def test_build_failure_on_the_reference_sample(monkeypatch):
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    from src.dlt import registry
    registry.clear_cache()

    headers = parse_headers(REFERENCE)
    failure = build_failure(headers, headers.exception_message)

    assert failure["failure_class"] == "A"
    assert failure["business_code"] == "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"
    assert failure["root_fqcn"] == "in.gov.uidai.common.exception.BusinessException"
    assert failure["registry_description"].startswith("UidOriginTracker record absent")
    assert failure["signature"].endswith("BioDataBaseHelperServiceImpl.getUidOriginTrackerData")
    assert len(failure["fingerprint"]) == 64
    assert failure["truncated"] is False
    assert len(failure["chain"]) == 5


def test_build_failure_on_empty_headers():
    failure = build_failure(parse_headers({}), None)
    assert failure["failure_class"] == "U"
    assert failure["truncated"] is True


# ======================================================================
# The endpoint
# ======================================================================

def test_evidence_is_persisted_before_any_analysis():
    """A parser bug must be recoverable without re-consuming Kafka."""
    result = fetch_dlt_logs(message(ref_id=None))
    storage = case_storage.get_dlt_storage()
    case_id = result["case_id"]

    assert storage.artifact_exists(case_id, HEADERS_ARTIFACT)
    assert storage.artifact_exists(case_id, TRACE_ARTIFACT)
    assert storage.artifact_exists(case_id, PARSED_TRACE_ARTIFACT)

    trace = storage.load_artifact(case_id, TRACE_ARTIFACT)
    assert "UID_ORIGIN_TRACKER_DATA_NOT_FOUND" in trace


def test_pii_in_a_stacktrace_is_redacted_but_the_ref_id_survives():
    headers = dict(REFERENCE)
    headers["kafka_exception-stacktrace"] = (
        "java.lang.RuntimeException: failed for uid 123456789012 ref REF-1\n"
        "\tat com.uidai.enu.biometric.Svc.go(Svc.java:1)\n"
    )
    fetch_dlt_logs(message(ref_id="REF-1", headers=headers))

    trace = case_storage.get_dlt_storage().load_artifact("dlt-T-63-3352", TRACE_ARTIFACT)
    assert "123456789012" not in trace
    assert "[REDACTED:AADHAAR]" in trace
    assert "REF-1" in trace, "the correlation id must survive scrubbing"


def test_missing_ref_id_records_a_gap_and_skips_the_fetch():
    result = fetch_dlt_logs(message(ref_id=None))
    assert "NO_CORRELATION_ID" in result["gaps"]

    logs = case_storage.get_dlt_storage().load_artifact(result["case_id"],
                                                        FETCHED_LOGS_ARTIFACT)
    assert "No refId available" in logs


def test_no_timestamp_records_a_gap():
    headers = {"kafka_exception-stacktrace": REFERENCE["kafka_exception-stacktrace"]}
    result = fetch_dlt_logs(message(headers=headers))
    assert "NO_TIMESTAMP" in result["gaps"]


def test_window_too_old_skips_the_fetch(monkeypatch):
    """A fetch certain to return nothing still costs a full Kubernetes
    fan-out across every pod in the namespace."""
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "1")

    called = []
    monkeypatch.setattr("src.api.dlt_routes.reduce_logs",
                        lambda *a, **kw: called.append(kw) or "logs")

    result = fetch_dlt_logs(message())
    assert "LOGS_TOO_OLD" in result["gaps"]
    assert called == [], "no fetch may be issued"


def test_fetch_searches_on_ref_id_and_persists_under_case_id(monkeypatch):
    """DLT_PLAN.md 5.5 -- the two identifiers are deliberately different."""
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "999999999")
    captured = {}

    def fake_reduce(event_id, **kwargs):
        captured["search_id"] = event_id
        captured.update(kwargs)
        return "--- reduced trace ---"

    monkeypatch.setattr("src.api.dlt_routes.reduce_logs", fake_reduce)

    result = fetch_dlt_logs(message(ref_id="REF-99"))

    assert captured["search_id"] == "REF-99"
    assert captured["storage_key"] == result["case_id"]
    assert captured["window"] is not None
    assert captured["storage"] is case_storage.get_dlt_storage()


def test_log_fetch_failure_degrades_rather_than_losing_the_case(monkeypatch):
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "999999999")

    def boom(*args, **kwargs):
        raise RuntimeError("cluster unreachable")

    monkeypatch.setattr("src.api.dlt_routes.reduce_logs", boom)

    result = fetch_dlt_logs(message())
    assert "LOG_FETCH_FAILED" in result["gaps"]
    assert result["status"] == "queued_for_analysis"

    storage = case_storage.get_dlt_storage()
    assert storage.artifact_exists(result["case_id"], TRACE_ARTIFACT), \
        "the stacktrace was already persisted and must survive"


def test_status_advances_to_logs_fetched():
    result = fetch_dlt_logs(message(ref_id=None))
    status = case_storage.get_dlt_storage().load(result["case_id"], filename="status.json")
    assert status["packet_status"]["status"] == "LOGS_FETCHED"
    assert status["packet_metadata"]["eid"] == result["case_id"]


def test_terminal_case_short_circuits():
    case_storage.get_dlt_storage().save_terminal("dlt-T-63-3352", {
        "packet_metadata": {"eid": "dlt-T-63-3352"},
        "packet_status": {"status": "NEEDS_MANUAL_REVIEW"},
    })
    assert fetch_dlt_logs(message())["status"] == "already_processed"


def test_second_call_reuses_the_persisted_logs(monkeypatch):
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "999999999")
    calls = []
    monkeypatch.setattr("src.api.dlt_routes.reduce_logs",
                        lambda *a, **kw: calls.append(1) or "logs")

    fetch_dlt_logs(message())
    fetch_dlt_logs(message())
    assert len(calls) == 1, "the endpoint must be idempotent"


def test_the_case_is_published_with_its_gaps(monkeypatch):
    published = []
    monkeypatch.setattr("src.api.dlt_routes.publish_to_dlt_analysis_queue",
                        lambda m: published.append(m) or True)

    fetch_dlt_logs(message(ref_id=None))

    assert len(published) == 1
    assert published[0]["case_id"] == "dlt-T-63-3352"
    assert "NO_CORRELATION_ID" in published[0]["evidence_gaps"]


def test_a_publish_failure_surfaces(monkeypatch):
    """It must reach the consumer as a non-2xx so the offset is not committed
    and Kafka redelivers -- otherwise the case vanishes between stages."""
    def dead(_message):
        raise RuntimeError("broker unreachable")

    monkeypatch.setattr("src.api.dlt_routes.publish_to_dlt_analysis_queue", dead)
    with pytest.raises(RuntimeError):
        fetch_dlt_logs(message(ref_id=None))


def test_dlt_artifacts_do_not_pollute_the_rejection_store(tmp_path):
    fetch_dlt_logs(message(ref_id=None))
    assert (tmp_path / "dlt_cases").exists()
    assert not (tmp_path / "casebook_dlt-T-63-3352").exists()


def test_utc_conversion_is_stable():
    """Guards against a local-timezone regression in the window ISO strings."""
    window = derive_window(parse_headers(REFERENCE), now_ms=LAST_ATTEMPT_MS)
    expected = datetime.fromtimestamp(LAST_ATTEMPT_MS / 1000, tz=timezone.utc).isoformat()
    assert window.anchor_iso == expected
