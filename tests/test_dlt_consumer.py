"""
Phase 4 of DLT_PLAN.md -- the message-adapter seam and the DLT consumer.

The most important tests here are the *rejection* ones. Phase 4 refactors live
code that runs the production rejection pipeline, and `RejectionAdapter` is
today's logic moved rather than rewritten. If any of these drift, the refactor
broke something that was working:

* the same `MessagePayload` validation, DLQ-ing the raw undecoded string
* the same `packetStatus == "REJECTED"` filter
* the same terminal-casebook dedupe
* the same identity (`eventId`) and the same request body

The DLT-specific departure worth remembering: an unparseable payload is **not**
poison on the DLT path. The evidence is in the headers, so a payload we cannot
decode costs the `refId` and nothing else.
"""
import json
from types import SimpleNamespace

import pytest

from src.dlt import case_storage
from src.utils.message_adapters import DltAdapter, RejectionAdapter, for_role

REFERENCE_HEADERS = [
    (b"kafka_original-topic", b"ENU.UPDATE.CHECKER.COMPLETION.V1"),
    (b"kafka_original-partition", b"63"),
    (b"kafka_original-offset", b"3352"),
    (b"retry_topic-backoff-timestamp", b"01A012AB41BF"),
    (b"kafka_exception-stacktrace",
     b"org.springframework.X: outer\n\tat org.springframework.A.b(A.java:1)"
     b"\nCaused by: in.gov.uidai.common.exception.BusinessException: [SOME_CODE] detail"
     b"\n\tat com.uidai.enu.biometric.Svc.go(Svc.java:1)\n\t... 3 more\n"),
]


def kafka_message(value, headers=None, topic="packet-dlt", partition=0, offset=1):
    return SimpleNamespace(value=value, headers=headers or [],
                           topic=topic, partition=partition, offset=offset)


def rejection_message(event_id="EVT-1", status="REJECTED"):
    return kafka_message(json.dumps({
        "eventId": event_id,
        "packetExecutionSummary": {"packetStatus": status},
    }).encode("utf-8"))


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_REFID_PATH", "DLT_REFID_KEYS", "CASEBOOK_STORAGE_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    case_storage.reset_cache()
    yield
    case_storage.reset_cache()


# ======================================================================
# Role selection
# ======================================================================

@pytest.mark.parametrize("role,expected", [
    ("fast", RejectionAdapter),
    ("slow", RejectionAdapter),
    ("dlt", DltAdapter),
    ("dlt_analysis", DltAdapter),
    ("something-unknown", RejectionAdapter),
])
def test_role_selects_the_right_adapter(role, expected):
    assert isinstance(for_role(role), expected)


# ======================================================================
# RejectionAdapter -- must behave exactly as before Phase 4
# ======================================================================

def test_rejection_valid_message_dispatches():
    result = RejectionAdapter().parse(rejection_message())
    assert not result.is_poison
    assert result.body["eventId"] == "EVT-1"


def test_rejection_poison_pill_carries_the_raw_string_to_the_dlq():
    """publish_to_dlq receives the raw undecoded text, not a parsed dict --
    which is what survives a JSON decode failure."""
    result = RejectionAdapter().parse(kafka_message(b"{not json"))
    assert result.is_poison
    assert result.raw_text == "{not json"
    assert result.body is None
    assert "Structural validation failed" in result.error


def test_rejection_schema_violation_is_poison():
    """Valid JSON, wrong shape: MessagePayload requires packetExecutionSummary."""
    result = RejectionAdapter().parse(kafka_message(b'{"eventId": "X"}'))
    assert result.is_poison


def test_rejection_non_utf8_payload_does_not_raise():
    assert RejectionAdapter().parse(kafka_message(b"\xff\xfe garbage")).is_poison


def test_rejection_non_rejected_packet_is_skipped():
    body = RejectionAdapter().parse(rejection_message(status="APPROVED")).body
    assert RejectionAdapter().should_skip(body) == "non-rejected packet"


def test_rejection_dedupes_on_a_terminal_casebook(monkeypatch):
    body = RejectionAdapter().parse(rejection_message()).body

    monkeypatch.setattr("src.storage.factory.get_casebook_storage",
                        lambda: SimpleNamespace(exists=lambda eid, terminal_only: True))
    assert RejectionAdapter().should_skip(body) == "terminal casebook already exists"

    monkeypatch.setattr("src.storage.factory.get_casebook_storage",
                        lambda: SimpleNamespace(exists=lambda eid, terminal_only: False))
    assert RejectionAdapter().should_skip(body) is None


def test_rejection_storage_failure_propagates(monkeypatch):
    """It must reach the consumer's outer handler, which DLQs and abandons the
    offset. Swallowing it here would recreate the G1 commit stall."""
    def boom():
        raise RuntimeError("S3 is having a moment")

    monkeypatch.setattr("src.storage.factory.get_casebook_storage", boom)
    body = RejectionAdapter().parse(rejection_message()).body
    with pytest.raises(RuntimeError):
        RejectionAdapter().should_skip(body)


def test_rejection_identity_and_timeout_casebook():
    adapter = RejectionAdapter()
    body = adapter.parse(rejection_message()).body
    assert adapter.identity_of(body) == "EVT-1"

    casebook = adapter.timeout_casebook(body)
    assert casebook["packet_status"]["status"] == "FAILED_TIMEOUT"
    assert casebook["packet_metadata"]["eid"] == "EVT-1"


# ======================================================================
# DltAdapter
# ======================================================================

def test_dlt_headers_reach_the_request_body():
    """kafkaConsumer.py read only msg.value before Phase 4; the DLT flow's
    primary evidence is in msg.headers."""
    result = DltAdapter().parse(kafka_message(b'{"refId": "R1"}', REFERENCE_HEADERS))
    assert not result.is_poison
    assert "kafka_exception-stacktrace" in result.body["headers"]
    assert result.body["headers"]["kafka_original-offset"] == "3352"


def test_dlt_case_id_uses_the_original_coordinates_not_the_dlt_ones():
    """Idempotency across a redrive depends on the *original* coordinates."""
    result = DltAdapter().parse(kafka_message(
        b"{}", REFERENCE_HEADERS, topic="packet-dlt", partition=9, offset=99))
    assert result.body["case_id"] == "dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352"


def test_dlt_case_id_falls_back_to_the_dlt_record_coordinates():
    result = DltAdapter().parse(kafka_message(b"{}", [], topic="packet-dlt",
                                              partition=4, offset=77))
    assert result.body["case_id"] == "dlt-packet-dlt-4-77"


def test_dlt_redelivery_yields_the_same_case_id():
    a = DltAdapter().parse(kafka_message(b"{}", REFERENCE_HEADERS, offset=1))
    b = DltAdapter().parse(kafka_message(b"{}", REFERENCE_HEADERS, offset=2))
    assert a.body["case_id"] == b.body["case_id"]


def test_dlt_unparseable_payload_is_not_poison():
    """The stacktrace is in the headers. Discarding the message because a
    field we read one identifier out of failed to parse would throw away the
    evidence with it."""
    result = DltAdapter().parse(kafka_message(b"{not json", REFERENCE_HEADERS))
    assert not result.is_poison
    assert result.body["payload"] is None
    assert result.body["payload_raw"] == "{not json"
    assert result.body["ref_id"] is None
    assert result.body["headers"]["kafka_exception-stacktrace"]


def test_dlt_missing_payload_is_not_poison():
    result = DltAdapter().parse(kafka_message(None, REFERENCE_HEADERS))
    assert not result.is_poison
    assert result.body["payload"] is None


def test_dlt_ref_id_is_extracted(monkeypatch):
    monkeypatch.setenv("DLT_REFID_PATH", "packetMetaData.refId")
    result = DltAdapter().parse(kafka_message(
        json.dumps({"packetMetaData": {"refId": "REF-7"}}).encode("utf-8"),
        REFERENCE_HEADERS))
    assert result.body["ref_id"] == "REF-7"


def test_dlt_large_stacktrace_survives():
    """Real traces run to tens of kilobytes."""
    big = b"java.lang.RuntimeException: x\n" + (b"\tat com.uidai.A.b(A.java:1)\n" * 2000)
    result = DltAdapter().parse(kafka_message(b"{}", [(b"kafka_exception-stacktrace", big)]))
    assert len(result.body["headers"]["kafka_exception-stacktrace"]) > 50_000


def test_dlt_non_utf8_header_does_not_raise():
    result = DltAdapter().parse(kafka_message(b"{}", [(b"weird", b"\xff\xfe")]))
    assert not result.is_poison


def test_dlt_dedupes_on_a_terminal_case():
    adapter = DltAdapter()
    body = adapter.parse(kafka_message(b"{}", REFERENCE_HEADERS)).body
    case_id = body["case_id"]

    assert adapter.should_skip(body) is None

    case_storage.get_dlt_storage().save_terminal(case_id, {
        "packet_metadata": {"eid": case_id},
        "packet_status": {"status": "NEEDS_MANUAL_REVIEW"},
    })
    assert adapter.should_skip(body) == "terminal DLT case already exists"


def test_dlt_cases_are_stored_apart_from_rejection_casebooks(tmp_path):
    """A DLT case must never appear to `list_events()` on the rejection store,
    which every accuracy and pruning tool walks."""
    case_storage.get_dlt_storage().save_terminal("dlt-T-0-1", {
        "packet_metadata": {"eid": "dlt-T-0-1"},
        "packet_status": {"status": "NEEDS_MANUAL_REVIEW"},
    })
    assert (tmp_path / "dlt_cases").exists()
    assert not (tmp_path / "casebook_dlt-T-0-1").exists()


def test_dlt_identity_and_timeout_casebook():
    adapter = DltAdapter()
    body = adapter.parse(kafka_message(
        json.dumps({"refId": "R9"}).encode("utf-8"), REFERENCE_HEADERS)).body

    assert adapter.identity_of(body) == body["case_id"]
    casebook = adapter.timeout_casebook(body)
    assert casebook["packet_status"]["status"] == "FAILED_TIMEOUT"
    assert casebook["packet_metadata"]["ref_id"] == "R9"


# ======================================================================
# Consumer wiring
# ======================================================================

def test_dlt_role_resolves_its_own_topic_group_and_endpoint(monkeypatch):
    """Each role must land on distinct topics, groups, heartbeats and ports so
    all four can run co-located without colliding."""
    import importlib

    monkeypatch.setenv("CONSUMER_ROLE", "dlt")
    monkeypatch.setenv("DLT_CONSUMER_TOPIC_NAME", "my-dlt")
    monkeypatch.setenv("DLT_CONSUMER_GROUP_ID", "my-dlt-group")

    import src.utils.kafkaConsumer as kc

    reloaded = importlib.reload(kc)
    try:
        assert reloaded.CONSUMER_ROLE == "dlt"
        assert reloaded.kafkaConsumerTopicName == "my-dlt"
        assert reloaded.kafkaConsumerGroupId == "my-dlt-group"
        assert reloaded.kafkaConsumerInternalEndpoint.endswith("/fetch-dlt-logs")
        assert isinstance(reloaded._adapter, DltAdapter)
        assert "dlt_consumer_heartbeat" in str(reloaded._heartbeat_path())
    finally:
        monkeypatch.delenv("CONSUMER_ROLE", raising=False)
        importlib.reload(kc)


def test_default_role_is_still_the_fast_consumer():
    """Every existing deployment and test sets no CONSUMER_ROLE at all."""
    import src.utils.kafkaConsumer as kc

    assert kc.CONSUMER_ROLE == "fast"
    assert isinstance(kc._adapter, RejectionAdapter)


def test_heartbeat_paths_are_all_distinct():
    from src.utils.paths import (
        CONSUMER_HEARTBEAT_PATH,
        DLT_ANALYSIS_HEARTBEAT_PATH,
        DLT_CONSUMER_HEARTBEAT_PATH,
        SLOW_CONSUMER_HEARTBEAT_PATH,
    )

    paths = {CONSUMER_HEARTBEAT_PATH, SLOW_CONSUMER_HEARTBEAT_PATH,
             DLT_CONSUMER_HEARTBEAT_PATH, DLT_ANALYSIS_HEARTBEAT_PATH}
    assert len(paths) == 4


def test_handle_one_message_dispatches_a_dlt_record(monkeypatch):
    """End-to-end through the consumer's shared path: parse, skip-check,
    submit, and record the offset."""
    import src.utils.kafkaConsumer as kc

    monkeypatch.setattr(kc, "_offset_tracker", kc.OffsetTracker())
    monkeypatch.setattr(kc, "_adapter", DltAdapter())
    monkeypatch.setattr(kc._queue_semaphore, "release", lambda: None)

    submitted = []
    monkeypatch.setattr(kc._worker_pool, "submit",
                        lambda fn, body, offsets: submitted.append(body))

    kc._handle_one_message("tp-0", kafka_message(b'{"refId": "R1"}', REFERENCE_HEADERS,
                                                 partition=0, offset=5))

    assert len(submitted) == 1
    assert submitted[0]["case_id"] == "dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352"
    assert kc._offset_tracker.in_flight() == 1, "still running until the worker finishes"


def test_handle_one_message_skips_and_commits_a_duplicate(monkeypatch):
    import src.utils.kafkaConsumer as kc

    monkeypatch.setattr(kc, "_offset_tracker", kc.OffsetTracker())
    monkeypatch.setattr(kc, "_adapter", DltAdapter())
    monkeypatch.setattr(kc._queue_semaphore, "release", lambda: None)

    submitted = []
    monkeypatch.setattr(kc._worker_pool, "submit",
                        lambda fn, body, offsets: submitted.append(body))

    case_storage.get_dlt_storage().save_terminal(
        "dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352",
        {"packet_metadata": {"eid": "x"}, "packet_status": {"status": "NEEDS_MANUAL_REVIEW"}})

    kc._handle_one_message("tp-0", kafka_message(b"{}", REFERENCE_HEADERS,
                                                 partition=0, offset=5))

    assert submitted == []
    assert kc._offset_tracker.in_flight() == 0, "a skip must free the offset"
