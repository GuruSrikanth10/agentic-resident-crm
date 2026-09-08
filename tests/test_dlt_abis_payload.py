"""
The `EnrolmentEventResponse` payload schema and key-first refId resolution.

Bound to `reference_abis_mw_response.json` -- a real dead-lettered record
captured 2026-08-20, and the first sample that carried a payload. Four
properties carry this file, and each one is a way the log lane fails silently
if it regresses:

* **The record key is the refId.** It resolves even when the payload does not
  deserialise, which is the exact case `DltAdapter` exists to survive. Before
  this, an undecodable payload meant no refId, no logs, and a guaranteed
  `UNVERIFIABLE` verdict on a case whose stacktrace was intact.

* **`event_id` is not `refId`.** The payload carries both, and they are
  different UUIDs. This project's own vocabulary calls refId "the event id"
  (DLT_PLAN.md 3), so the wrong field is the one you would reach for -- and it
  fails as an empty log query, not as an error.

* **`candidateRefId` is somebody else's packet.** The failing dedupe lookup
  iterates candidates returned by the matcher. Correlating on one would pull
  in an unrelated enrolment's log lines while missing this packet entirely.

* **Provenance survives.** A refId that fell through to the bounded search is
  a guess that landed; one read off the record key is the producer's own
  partitioning key. A casebook that cannot tell them apart cannot be triaged.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.dlt.payload import (
    decode_key,
    extract_ref_id,
    key_as_ref_id,
    paths_for_type,
    refid_keys,
    resolve_ref_id,
)
from src.models.dlt_payload_schemas import (
    MAX_SUMMARY_CANDIDATES,
    TYPE_ENROLMENT_EVENT_RESPONSE,
    EnrolmentEventResponse,
    parse_payload,
    summarise_payload,
)
from src.api.dlt_routes import (
    PAYLOAD_SUMMARY_ARTIFACT,
    build_failure,
    fetch_dlt_logs,
)
from src.dlt import case_storage, registry
from src.dlt.headers import parse_headers
from src.models.dlt_schemas import DltMessage
from src.utils.message_adapters import DltAdapter

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_abis_mw_response.json"
SAMPLE = json.loads(FIXTURE.read_text(encoding="utf-8"))

PAYLOAD = SAMPLE["payload"]
HEADERS = SAMPLE["headers"]
KEY = SAMPLE["key"]

REF_ID = "c5d21184-08f4-4c32-9e5e-5c108c33eb14"
PAYLOAD_EVENT_ID = "b733ab61-78c4-4aa9-b959-7216435c2544"
CANDIDATE_REF_ID = "80d00e27-b4be-4e69-83aa-ea65f38bf596"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_REFID_PATH", "DLT_REFID_KEYS", "DLT_REFID_PATHS_BY_TYPE",
                "DLT_MAX_LOG_AGE_SECONDS", "CASEBOOK_STORAGE_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    monkeypatch.setattr("src.api.dlt_routes.publish_to_dlt_analysis_queue",
                        lambda message: True)
    case_storage.reset_cache()
    registry.clear_cache()
    yield
    case_storage.reset_cache()


def kafka_message(value, key=None, headers=None):
    encoded = [(k.encode(), (v or "").encode()) for k, v in (headers or HEADERS).items()]
    return SimpleNamespace(
        value=value,
        key=key.encode("utf-8") if isinstance(key, str) else key,
        headers=encoded,
        topic="packet-dlt", partition=0, offset=1)


# ======================================================================
# The schema
# ======================================================================

def test_the_reference_payload_parses():
    model = parse_payload(PAYLOAD, TYPE_ENROLMENT_EVENT_RESPONSE)

    assert isinstance(model, EnrolmentEventResponse)
    assert model.ref_id == REF_ID
    assert model.category == "ONLY_IDENTIFY"
    assert model.event_type == "enr_pkt"

    block = model.abisMWResponseNewSeda
    assert block.responseStatus == "FAILURE"
    assert block.requestType == "ONLY_IDENTIFY"
    assert [(r.abisId, r.candidate_count)
            for r in block.abisResponses.abisResponse] == [
        ("ABIS1", 0), ("ABIS2", 2), ("ABIS3", 0)]


def test_event_id_is_not_the_ref_id():
    """The trap. Two UUIDs on one payload, one of which correlates to nothing."""
    model = parse_payload(PAYLOAD, TYPE_ENROLMENT_EVENT_RESPONSE)

    assert model.event_id == PAYLOAD_EVENT_ID
    assert model.ref_id == REF_ID
    assert model.event_id != model.ref_id


def test_ref_id_never_falls_back_to_event_id():
    """A payload with no ABIS block yields None, not the envelope's own id."""
    model = parse_payload({"event_id": PAYLOAD_EVENT_ID},
                          TYPE_ENROLMENT_EVENT_RESPONSE)
    assert model.ref_id is None


def test_ref_id_falls_back_to_the_nested_reference_id():
    stripped = json.loads(json.dumps(PAYLOAD))
    del stripped["abisMWResponseNewSeda"]["refId"]

    model = parse_payload(stripped, TYPE_ENROLMENT_EVENT_RESPONSE)
    assert model.ref_id == REF_ID


def test_an_unknown_field_is_kept_not_rejected():
    """An upstream schema addition must never cost us the message."""
    extended = json.loads(json.dumps(PAYLOAD))
    extended["somethingUpstreamAddedOnTuesday"] = {"nested": True}

    model = parse_payload(extended, TYPE_ENROLMENT_EVENT_RESPONSE)
    assert model is not None
    assert model.ref_id == REF_ID
    assert model.model_dump()["somethingUpstreamAddedOnTuesday"] == {"nested": True}


def test_parse_payload_returns_none_rather_than_raising():
    assert parse_payload("not a dict", TYPE_ENROLMENT_EVENT_RESPONSE) is None
    assert parse_payload(None, TYPE_ENROLMENT_EVENT_RESPONSE) is None
    assert parse_payload(PAYLOAD, "com.example.UnregisteredType") is None


# ======================================================================
# The payload summary
# ======================================================================

def test_summary_carries_what_the_failure_was_working_on():
    summary = summarise_payload(PAYLOAD, TYPE_ENROLMENT_EVENT_RESPONSE)

    assert "responseStatus=FAILURE" in summary
    assert REF_ID in summary
    assert "ABIS2: 2 candidate(s)" in summary
    assert CANDIDATE_REF_ID in summary


def test_summary_labels_the_identifiers_it_shows():
    """Every id in the summary is named, so none can be mistaken for the refId."""
    summary = summarise_payload(PAYLOAD, TYPE_ENROLMENT_EVENT_RESPONSE)

    assert "NOT the log-correlation id" in summary
    assert "OTHER enrolments' refIds" in summary
    assert "not a log anchor" in summary


def test_summary_bounds_a_long_candidate_list():
    """A wide ABIS response must not push the stacktrace out of the context."""
    wide = json.loads(json.dumps(PAYLOAD))
    one = wide["abisMWResponseNewSeda"]["abisResponses"]["abisResponse"][1]
    one["candidates"]["matchedCandidate"] = [
        {"candidateRefId": f"cand-{i:04d}", "scaledScore": i} for i in range(500)]

    summary = summarise_payload(wide, TYPE_ENROLMENT_EVENT_RESPONSE)

    assert summary.count("candidateRefId") == 0
    assert summary.count("scaledScore=") == MAX_SUMMARY_CANDIDATES
    assert "500 total" in summary
    assert "cand-0499" not in summary


def test_summary_of_an_unregistered_type_lists_keys_only():
    summary = summarise_payload(PAYLOAD, "com.uidai.enu.common.model.EventMessage")

    assert "no model registered" in summary
    assert "abisMWResponseNewSeda" in summary
    # Not a verbatim dump: the values stay out.
    assert REF_ID not in summary


def test_summary_of_nothing_is_none():
    assert summarise_payload(None) is None


# ======================================================================
# Key-first resolution
# ======================================================================

def test_the_record_key_is_the_ref_id():
    result = resolve_ref_id(PAYLOAD, key=KEY, type_id=TYPE_ENROLMENT_EVENT_RESPONSE)

    assert result.ref_id == REF_ID
    assert result.source == "record_key"
    assert result.payload_ref_id == REF_ID
    assert result.mismatch is False


def test_the_key_resolves_without_any_payload():
    """The property that matters: an undecodable payload keeps its log lane."""
    result = resolve_ref_id(None, key=KEY, type_id=TYPE_ENROLMENT_EVENT_RESPONSE)

    assert result.ref_id == REF_ID
    assert result.source == "record_key"
    assert result.payload_ref_id is None


def test_the_registered_type_path_resolves_without_a_key():
    result = resolve_ref_id(PAYLOAD, key=None, type_id=TYPE_ENROLMENT_EVENT_RESPONSE)

    assert result.ref_id == REF_ID
    assert result.source == "type_path"


def test_an_unregistered_type_falls_through_to_the_search():
    result = resolve_ref_id(PAYLOAD, key=None, type_id="com.example.Unknown")

    assert result.ref_id == REF_ID
    assert result.source == "search"


def test_a_configured_path_beats_the_registered_one(monkeypatch):
    monkeypatch.setenv("DLT_REFID_PATH",
                       "abisMWResponseNewSeda.abisResponses.referenceId")
    result = resolve_ref_id(PAYLOAD, key=None, type_id=TYPE_ENROLMENT_EVENT_RESPONSE)

    assert result.ref_id == REF_ID
    assert result.source == "configured_path"


def test_a_disagreement_prefers_the_key_and_is_recorded():
    """Silently picking one of two ids that should have been equal is how a
    misconfigured path stays invisible for months."""
    result = resolve_ref_id(PAYLOAD, key="ffffffff-0000-0000-0000-000000000000",
                            type_id=TYPE_ENROLMENT_EVENT_RESPONSE)

    assert result.ref_id == "ffffffff-0000-0000-0000-000000000000"
    assert result.payload_ref_id == REF_ID
    assert result.mismatch is True


def test_nothing_anywhere_is_a_valid_state():
    result = resolve_ref_id({}, key=None, type_id=None)

    assert result.ref_id is None
    assert result.source == "none"
    assert result.mismatch is False


# ======================================================================
# The key shape guard
# ======================================================================

@pytest.mark.parametrize("key", ["ABIS1", "9", "", "   ", None, "short"])
def test_a_key_that_is_not_an_identifier_is_declined(key):
    """A Kafka key is whatever the producer chose. Feeding a routing token to
    the log query returns nothing, or another packet's lines."""
    assert key_as_ref_id(key) is None


@pytest.mark.parametrize("key", [
    REF_ID,
    "0000c5d2-08f4-4c32-9e5e-5c108c33eb14",
    "REF.123:456-789",
])
def test_an_identifier_shaped_key_is_accepted(key):
    assert key_as_ref_id(key) == key


def test_a_declined_key_falls_through_to_the_payload():
    result = resolve_ref_id(PAYLOAD, key="ABIS1",
                            type_id=TYPE_ENROLMENT_EVENT_RESPONSE)

    assert result.ref_id == REF_ID
    assert result.source == "type_path"
    assert result.mismatch is False


def test_decode_key_handles_bytes_and_none():
    assert decode_key(KEY.encode("utf-8")) == KEY
    assert decode_key(b"  padded  ") == "padded"
    assert decode_key(None) is None
    assert decode_key(b"") is None


# ======================================================================
# The denylist
# ======================================================================

@pytest.mark.parametrize("denied", ["event_id", "eventId", "candidateRefId",
                                    "requestId"])
def test_a_denied_key_cannot_be_configured_back_in(monkeypatch, denied):
    monkeypatch.setenv("DLT_REFID_KEYS", denied)
    assert denied not in refid_keys()


def test_a_denylisted_key_does_not_poison_a_valid_configuration(monkeypatch):
    monkeypatch.setenv("DLT_REFID_KEYS", "event_id,refId")
    assert refid_keys() == ("refId",)


def test_configuring_only_denied_keys_falls_back_to_the_defaults(monkeypatch):
    monkeypatch.setenv("DLT_REFID_KEYS", "event_id,eventId")
    assert refid_keys() == ("refId", "ref_id", "referenceId")


def test_the_search_never_returns_the_payloads_own_event_id():
    """Belt and braces: even with the ABIS block gone, event_id is not an id."""
    stripped = {"event_id": PAYLOAD_EVENT_ID, "category": "ONLY_IDENTIFY"}
    assert extract_ref_id(stripped, TYPE_ENROLMENT_EVENT_RESPONSE) is None


def test_the_search_never_returns_a_candidate_ref_id():
    """A candidate belongs to a different enrolment than the one that failed."""
    stripped = json.loads(json.dumps(PAYLOAD))
    del stripped["abisMWResponseNewSeda"]["refId"]
    del stripped["abisMWResponseNewSeda"]["abisResponses"]["referenceId"]

    found = extract_ref_id(stripped, "com.example.Unknown")
    assert found != CANDIDATE_REF_ID
    assert found is None


# ======================================================================
# The type registry
# ======================================================================

def test_the_reference_type_is_registered():
    assert paths_for_type(TYPE_ENROLMENT_EVENT_RESPONSE) == (
        "abisMWResponseNewSeda.refId",
        "abisMWResponseNewSeda.abisResponses.referenceId",
    )


def test_an_unregistered_type_has_no_paths():
    assert paths_for_type("com.example.Unknown") == ()
    assert paths_for_type(None) == ()


def test_the_env_override_wins(monkeypatch):
    monkeypatch.setenv("DLT_REFID_PATHS_BY_TYPE",
                       json.dumps({TYPE_ENROLMENT_EVENT_RESPONSE: "a.b.c"}))
    assert paths_for_type(TYPE_ENROLMENT_EVENT_RESPONSE) == ("a.b.c",)


def test_the_env_override_accepts_a_list(monkeypatch):
    monkeypatch.setenv("DLT_REFID_PATHS_BY_TYPE",
                       json.dumps({"com.example.X": ["p.one", "p.two"]}))
    assert paths_for_type("com.example.X") == ("p.one", "p.two")


def test_malformed_override_config_degrades_to_the_registry(monkeypatch):
    """A typo in config must not take the consumer down."""
    monkeypatch.setenv("DLT_REFID_PATHS_BY_TYPE", "{not json")
    assert paths_for_type(TYPE_ENROLMENT_EVENT_RESPONSE) == (
        "abisMWResponseNewSeda.refId",
        "abisMWResponseNewSeda.abisResponses.referenceId",
    )


# ======================================================================
# The adapter reads the key
# ======================================================================

def test_the_adapter_resolves_the_ref_id_from_the_key():
    body = DltAdapter().parse(kafka_message(
        json.dumps(PAYLOAD).encode("utf-8"), key=KEY)).body

    assert body["ref_id"] == REF_ID
    assert body["ref_id_source"] == "record_key"
    assert body["record_key"] == KEY
    assert body["ref_id_mismatch"] is False
    assert body["case_id"] == "dlt-ENU.MWARE.DEDUPE.PROCESS.COMPLETION.V1-9-4441353"


def test_an_undecodable_payload_keeps_its_log_lane():
    """The whole point of reading the key. Header-only used to be the ceiling
    here; now only the payload summary is lost."""
    result = DltAdapter().parse(kafka_message(b"{not json at all", key=KEY))

    assert not result.is_poison
    assert result.body["ref_id"] == REF_ID
    assert result.body["ref_id_source"] == "record_key"
    assert result.body["payload"] is None
    assert result.body["payload_raw"] == "{not json at all"


def test_the_adapter_still_works_with_no_key_at_all():
    body = DltAdapter().parse(kafka_message(
        json.dumps(PAYLOAD).encode("utf-8"), key=None)).body

    assert body["ref_id"] == REF_ID
    assert body["ref_id_source"] == "type_path"


def test_the_adapter_records_a_key_payload_disagreement():
    body = DltAdapter().parse(kafka_message(
        json.dumps(PAYLOAD).encode("utf-8"),
        key="ffffffff-0000-0000-0000-000000000000")).body

    assert body["ref_id_mismatch"] is True
    assert body["payload_ref_id"] == REF_ID
    assert body["ref_id"] == "ffffffff-0000-0000-0000-000000000000"


# ======================================================================
# The fetch endpoint
# ======================================================================

def dlt_message(**overrides):
    fields = dict(
        case_id="dlt-ENU.MWARE.DEDUPE.PROCESS.COMPLETION.V1-9-4441353",
        headers=dict(HEADERS),
        payload=PAYLOAD,
        record_key=KEY,
        ref_id=REF_ID,
        ref_id_source="record_key",
        payload_ref_id=REF_ID,
    )
    fields.update(overrides)
    return DltMessage(**fields)


def test_the_reference_record_classifies_as_class_a():
    headers = parse_headers(HEADERS)
    failure = build_failure(headers, headers.exception_message)

    assert failure["failure_class"] == "A"
    assert failure["business_code"] == "INDEX_MASTER_DATA_NOT_FOUND"
    assert failure["root_fqcn"] == "in.gov.uidai.common.exception.BusinessException"
    assert failure["registry_description"].startswith("Index Master reference data")
    assert failure["signature"].endswith(
        "BioDataBaseHelperServiceImpl.getIndexMasterData")
    assert failure["truncated"] is False
    assert len(failure["chain"]) == 5


def test_the_window_anchors_43_hours_after_the_produce_time():
    """Trap 2, confirmed on a second real sample. The backoff header decodes to
    the same instant the TimestampedException in the trace names."""
    headers = parse_headers(HEADERS)

    assert headers.backoff_timestamp_ms == 1787128681005   # 2026-08-19T08:38:01.005Z
    assert headers.original_timestamp_ms == 1786973867552   # 2026-08-17T13:37:47.552Z
    assert headers.anchor_is_fallback is False
    assert "2026-08-19T08:38:01.005" in headers.stacktrace
    gap_hours = (headers.backoff_timestamp_ms - headers.original_timestamp_ms) / 3_600_000
    assert gap_hours > 40


def test_the_payload_summary_is_persisted():
    result = fetch_dlt_logs(dlt_message())
    storage = case_storage.get_dlt_storage()

    summary = storage.load_artifact(result["case_id"], PAYLOAD_SUMMARY_ARTIFACT)
    assert "responseStatus=FAILURE" in summary
    assert "ABIS2: 2 candidate(s)" in summary


def test_a_ref_id_mismatch_is_reported_as_an_evidence_gap():
    result = fetch_dlt_logs(dlt_message(
        ref_id="ffffffff-0000-0000-0000-000000000000",
        payload_ref_id=REF_ID,
        ref_id_mismatch=True))

    assert "REFID_KEY_PAYLOAD_MISMATCH" in result["gaps"]


def test_agreement_produces_no_mismatch_gap():
    result = fetch_dlt_logs(dlt_message())
    assert "REFID_KEY_PAYLOAD_MISMATCH" not in result["gaps"]


def test_a_header_only_case_still_persists_evidence():
    """No payload at all: the summary is skipped, the case is not."""
    result = fetch_dlt_logs(dlt_message(payload=None, record_key=None,
                                        ref_id=None, ref_id_source="none",
                                        payload_ref_id=None))
    storage = case_storage.get_dlt_storage()

    assert result["failure_class"] == "A"
    assert "NO_CORRELATION_ID" in result["gaps"]
    assert not storage.artifact_exists(result["case_id"], PAYLOAD_SUMMARY_ARTIFACT)
