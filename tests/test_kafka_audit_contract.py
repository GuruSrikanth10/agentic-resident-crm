"""
The AUDIT envelope (2026-09-29): the payload of the rejections topic and of
every dead-lettered record. The DLT headers and key are unchanged.

`src/models/audit_contract.py` translates the envelope into the packet event
where a record enters, so these tests check the translation against the two
real samples, and that each lane then treats the record as before: a
rejection (validationStatus "false") is investigated, a dead-lettered record
(executionStatus ON_HOLD) is left to the DLT lane, and a packet event in the
older contract passes through untouched.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.dlt import case_storage
from src.dlt.payload import rejection_contract
from src.models.audit_contract import is_audit_envelope, to_message_payload
from src.models.dlt_payload_schemas import summarise_payload
from src.models.schemas import MessagePayload
from src.utils import service_registry as sr
from src.utils.message_adapters import DltAdapter, RejectionAdapter

FIXTURES = Path(__file__).parent / "fixtures" / "audit"
REJECTION = json.loads((FIXTURES / "rejection_sample.json").read_text(encoding="utf-8"))
DLT = json.loads((FIXTURES / "dlt_sample.json").read_text(encoding="utf-8"))
REJECTION_REF_ID = "e839f515-aad5-4552-91d6-c788cd5a62b8"
DLT_REF_ID = "59a1a4be-4c25-47ea-87a2-85d70615c678"

DLT_HEADERS = [
    (b"kafka_original-topic", b"ENU.SMARTQC.PROCESS.COMPLETION.V1"),
    (b"kafka_original-partition", b"3"),
    (b"kafka_original-offset", b"41"),
    (b"kafka_exception-stacktrace",
     b"org.springframework.X: outer\n\tat org.springframework.A.b(A.java:1)"
     b"\nCaused by: java.lang.NullPointerException: boom"
     b"\n\tat com.uidai.enu.qc.Svc.go(Svc.java:1)\n\t... 3 more\n"),
]


def kafka_message(value, headers=None, topic="rejections", partition=0, offset=1):
    return SimpleNamespace(value=json.dumps(value).encode("utf-8"),
                           headers=headers or [],
                           topic=topic, partition=partition, offset=offset)


def audit(**edata):
    """The rejection sample with some `edata` fields replaced."""
    envelope = json.loads(json.dumps(REJECTION))
    envelope["edata"].update(edata)
    return envelope


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_REFID_PATH", "DLT_REFID_KEYS", "CASEBOOK_STORAGE_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr("src.storage.factory.get_casebook_storage",
                        lambda: SimpleNamespace(exists=lambda eid, terminal_only: False))
    case_storage.reset_cache()
    yield
    case_storage.reset_cache()


# ======================================================================
# The translation
# ======================================================================

def test_the_rejection_sample_becomes_a_rejected_packet_event():
    payload = to_message_payload(REJECTION)

    assert payload["eventId"] == REJECTION_REF_ID
    assert payload["packetMetaData"]["refId"] == REJECTION_REF_ID
    assert payload["packetMetaData"]["enrolmentType"] == "U"
    assert payload["packetMetaData"]["pktSource"] == "SSUP"
    assert payload["flowMetaData"] == {"stage": "REJECTINTERCEPTOR", "subStage": None}
    assert payload["sourceTopic"] == "ENU.REJECTINT.PROCESS.COMPLETION.V1"
    assert payload["sid"] == "0000002120694020260926203812"
    assert payload["sidDate"] == "2026-09-26 20:38:12"
    assert payload["eventTimestamp"] == "2026-09-29T10:04:28.980Z"
    assert payload["category"] == "AUDIT"
    assert payload["eventType"] == "REJECT INTERCEPTOR COMPLETED"
    assert payload["resubmissionSummary"] == {"resubmissionCount": 0,
                                              "resubmissionReason": None}

    summary = payload["packetExecutionSummary"]
    assert summary["packetStatus"] == "REJECTED"
    assert summary["errorData"] == [{"type": "BUSINESS_EXCEPTION",
                                     "errorReasonCode": "RESIDENT_QC_POA_DOCUMENT_NOT_APPROVED"}]
    assert (summary["isExecutionSuccess"], summary["hasExecutionErrors"]) == (True, False)
    assert (summary["isValidationSuccess"], summary["hasValidationErrors"]) == (False, True)


def test_the_dlt_sample_becomes_an_on_hold_packet_event():
    payload = to_message_payload(DLT)

    assert payload["eventId"] == DLT_REF_ID
    assert payload["flowMetaData"] == {"stage": "QC", "subStage": "SMART_QC"}
    assert payload["sourceTopic"] == "ENU.SMARTQC.PROCESS.COMPLETION.V1-dlt"
    assert payload["sidDate"] == "2026-09-27 20:35:45"
    assert payload["resubmissionSummary"]["resubmissionReason"] is None

    summary = payload["packetExecutionSummary"]
    assert summary["packetStatus"] == "ON_HOLD"
    assert summary["errorData"][0]["errorReasonCode"] == "UNHANDLED_EXCEPTION"
    assert (summary["isExecutionSuccess"], summary["hasExecutionErrors"]) == (False, True)


@pytest.mark.parametrize("execution, validation, status", [
    ("ON_HOLD", "false", "ON_HOLD"),     # dead-lettered wins over a failed validation
    ("on_hold", "true", "ON_HOLD"),
    ("COMPLETED", "false", "REJECTED"),
    ("COMPLETED", "FALSE", "REJECTED"),
    ("COMPLETED", False, "REJECTED"),
    ("COMPLETED", "true", "COMPLETED"),
    ("COMPLETED", None, "COMPLETED"),
    (None, None, None),
])
def test_the_packet_status_is_decided_by_execution_then_validation(execution, validation, status):
    payload = to_message_payload(audit(executionStatus=execution, validationStatus=validation))
    assert payload["packetExecutionSummary"]["packetStatus"] == status


@pytest.mark.parametrize("ets, expected", [
    (1790676268980, "2026-09-29T10:04:28.980Z"),
    ("not-a-number", None),
    (None, None),
    (True, None),
])
def test_event_timestamp_is_ets_in_utc_or_nothing(ets, expected):
    envelope = audit()
    envelope["ets"] = ets
    assert to_message_payload(envelope)["eventTimestamp"] == expected


@pytest.mark.parametrize("sid", ["SHORT", "0000002120694020269999999999", None, 42])
def test_a_sid_without_a_date_gives_no_sid_date(sid):
    assert to_message_payload(audit(sid=sid))["sidDate"] is None


@pytest.mark.parametrize("other", [
    {"eventId": "E", "packetExecutionSummary": {"packetStatus": "REJECTED"}},
    {"eventId": "E", "edata": {"refId": "R"},
     "packetExecutionSummary": {"packetStatus": "REJECTED"}},
    {"abisMWResponseNewSeda": {"refId": "R"}},
    {"edata": "not an object"},
    [REJECTION],
    None,
])
def test_anything_but_an_audit_envelope_is_returned_unchanged(other):
    assert not is_audit_envelope(other)
    assert to_message_payload(other) is other


def test_message_payload_accepts_the_envelope_directly():
    """The routes validate their body as MessagePayload, so an AUDIT message
    posted straight to /fetch-logs or /process-rejection works too."""
    model = MessagePayload.model_validate(REJECTION)

    assert model.eventId == REJECTION_REF_ID
    assert model.packetMetaData.refId == REJECTION_REF_ID
    assert model.packetExecutionSummary.packetStatus == "REJECTED"
    assert model.flowMetaData.stage == "REJECTINTERCEPTOR"


def test_an_envelope_without_a_ref_id_is_not_a_valid_packet_event():
    with pytest.raises(Exception):
        MessagePayload.model_validate(audit(refId=None))


# ======================================================================
# The rejection lane
# ======================================================================

def test_the_rejection_lane_investigates_a_rejection():
    adapter = RejectionAdapter()
    result = adapter.parse(kafka_message(REJECTION))

    assert not result.is_poison
    assert result.body == to_message_payload(REJECTION)
    assert adapter.identity_of(result.body) == REJECTION_REF_ID
    assert adapter.should_skip(result.body) is None


def test_the_rejection_lane_leaves_a_dead_lettered_record_to_the_dlt_lane():
    adapter = RejectionAdapter()
    result = adapter.parse(kafka_message(DLT))

    assert not result.is_poison
    assert adapter.should_skip(result.body) == "dead-lettered packet (ON_HOLD)"


def test_the_rejection_lane_skips_a_stage_that_passed():
    adapter = RejectionAdapter()
    body = adapter.parse(kafka_message(audit(validationStatus="true", errorData=[]))).body

    assert adapter.should_skip(body) == "non-rejected packet"


def test_an_envelope_without_a_ref_id_is_poison():
    result = RejectionAdapter().parse(kafka_message(audit(refId=None)))

    assert result.is_poison
    assert json.loads(result.raw_text)["edata"]["refId"] is None


def test_the_rejection_lane_resolves_the_service_from_the_envelope_stage(tmp_path):
    body = RejectionAdapter().parse(kafka_message(REJECTION)).body
    resolution = sr.resolve(body, docs_root=tmp_path)

    assert resolution.detail["stage"] == "REJECTINTERCEPTOR"
    assert resolution.detail["source_topic"] == "ENU.REJECTINT.PROCESS.COMPLETION.V1"
    assert resolution.detail["reason_code"] == "RESIDENT_QC_POA_DOCUMENT_NOT_APPROVED"


# ======================================================================
# The DLT lane
# ======================================================================

def test_a_dead_lettered_envelope_keeps_its_headers_and_gives_its_ref_id():
    result = DltAdapter().parse(kafka_message(DLT, DLT_HEADERS, topic="packet-dlt"))
    body = result.body

    assert not result.is_poison
    assert body["case_id"].startswith("dlt-ENU.SMARTQC.PROCESS.COMPLETION.V1-3-41")
    assert "kafka_exception-stacktrace" in body["headers"]
    assert (body["ref_id"], body["ref_id_source"]) == (DLT_REF_ID, "contract")
    assert body["event_id"] == DLT_REF_ID
    assert body["payload"] == to_message_payload(DLT)


def test_the_dlt_lane_reads_the_stage_and_status_off_the_envelope(tmp_path):
    payload = DltAdapter().parse(kafka_message(DLT, DLT_HEADERS)).body["payload"]

    assert rejection_contract(payload).packetExecutionSummary.packetStatus == "ON_HOLD"
    resolution = sr.resolve_dlt(payload, docs_root=tmp_path)
    assert (resolution.detail["stage"], resolution.detail["sub_stage"]) == ("QC", "SMART_QC")

    summary = summarise_payload(payload)
    assert "Stage / sub-stage: QC / SMART_QC" in summary
    assert "Packet status: ON_HOLD" in summary
    assert "Error reason codes: UNHANDLED_EXCEPTION" in summary
    assert f"refId = {DLT_REF_ID}" in summary


def test_the_dlt_key_matching_the_ref_id_is_not_a_mismatch():
    message = kafka_message(DLT, DLT_HEADERS)
    message.key = DLT_REF_ID.encode("utf-8")
    body = DltAdapter().parse(message).body

    assert body["ref_id_mismatch"] is False
