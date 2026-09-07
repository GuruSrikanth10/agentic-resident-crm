"""Serving several original topics from one set of prompts.

The DLT was built around a single payload structure, and two places quietly
assumed it: the Investigator prompt named `EnrolmentEventResponse` fields
directly, and the fingerprint had no payload dimension at all.

Neither survives a second topic. A prompt is one file shared by every
structure, so a field name written into it is asserted against payloads that
do not have it; and a fingerprint blind to the payload type lets two topics
failing in a shared library collapse into one group, where the first topic's
payload description is re-served against the second's records.

The fix keeps the prompts stating invariants and pushes the per-type facts
into the payload summary, which is already rendered per `__TypeId__`. These
tests hold that seam in place.
"""
import json
from pathlib import Path

import pytest

from src.api.dlt_routes import build_failure
from src.dlt import stacktrace as S
from src.dlt.headers import parse_headers
from src.dlt.orchestrator import _evidence_block
from src.models.dlt_payload_schemas import (
    IDENTIFIER_HEADING,
    ROLE_CORRELATION,
    ROLE_FOREIGN,
    TYPE_ENROLMENT_EVENT_RESPONSE,
    summarise_payload,
)

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]

PROMPTS = Path(__file__).parent.parent / "src" / "prompts"

TYPE_A = "com.uidai.enu.common.model.EventMessage"
TYPE_B = "in.gov.uidai.uidabismiddlewaresb.kafka.model.EnrolmentEventResponse"


@pytest.fixture(autouse=True)
def _no_type_flag(monkeypatch):
    monkeypatch.delenv("DLT_FINGERPRINT_TYPE_ID", raising=False)


# ======================================================================
# The fingerprint's payload-type dimension
# ======================================================================

FRAMES = ("com.uidai.shared.Consumer.onMessage", "com.uidai.shared.Util.read")


def _fp(type_id=None):
    return S.compute_fingerprint("java.lang.NullPointerException", FRAMES,
                                 "CODE", type_id=type_id)


def test_the_type_is_not_a_dimension_by_default():
    """Off by default because turning it on fragments every live group once."""
    assert _fp(TYPE_A) == _fp(TYPE_B) == _fp(None)


def test_the_type_separates_groups_once_enabled(monkeypatch):
    """Two topics failing in the same shared frames stop sharing a group."""
    monkeypatch.setenv("DLT_FINGERPRINT_TYPE_ID", "true")

    assert _fp(TYPE_A) != _fp(TYPE_B)


def test_enabling_the_flag_leaves_a_typeless_fingerprint_alone(monkeypatch):
    """The type is appended, never interleaved: a record with no `__TypeId__`
    header keeps the identity it had before the flag existed."""
    before = _fp(None)
    monkeypatch.setenv("DLT_FINGERPRINT_TYPE_ID", "true")

    assert _fp(None) == before
    assert _fp("") == before


def test_the_reference_fingerprint_survives_the_new_parameter():
    """The literal from test_dlt_stacktrace.py, recomputed through the path
    `build_failure` now takes. A change here means live groups fragmented on
    deploy rather than on the operator's decision to set the flag."""
    from tests.test_dlt_stacktrace import REFERENCE_FINGERPRINT

    parsed = S.parse_stacktrace(REFERENCE["kafka_exception-stacktrace"])
    frames = S.normalise_frames(parsed.root_frames)

    assert S.compute_fingerprint(
        parsed.root.fqcn, frames, "UID_ORIGIN_TRACKER_DATA_NOT_FOUND",
        type_id=REFERENCE.get("__TypeId__")) == REFERENCE_FINGERPRINT


def test_build_failure_carries_the_origin_topic_and_type(monkeypatch):
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    from src.dlt import registry
    registry.clear_cache()

    headers = parse_headers(REFERENCE)
    failure = build_failure(headers, headers.exception_message)

    assert failure["origin_topic"] == headers.original_topic
    assert failure["type_id"] == headers.type_id


# ======================================================================
# The identifier labelling contract
# ======================================================================

def test_a_registered_type_labels_every_identifier_it_shows():
    from tests.test_dlt_abis_payload import PAYLOAD

    summary = summarise_payload(PAYLOAD, TYPE_ENROLMENT_EVENT_RESPONSE)

    assert IDENTIFIER_HEADING in summary
    assert ROLE_CORRELATION in summary
    assert ROLE_FOREIGN in summary


def test_an_unregistered_type_says_so_instead_of_staying_silent():
    """A key listing full of id-shaped names with no labelling is how the
    `event_id` mistake gets made. The section is emitted either way."""
    summary = summarise_payload({"eventId": "e-1", "packetId": "p-1"},
                                "com.uidai.some.NewTopicPayload")

    assert IDENTIFIER_HEADING in summary
    assert "NONE LABELLED" in summary
    assert "Do not treat any value shown above as this packet's identifier" in summary


def test_a_non_object_payload_is_labelled_too():
    summary = summarise_payload("just a string", "com.uidai.some.NewTopicPayload")

    assert "NONE LABELLED" in summary


# ======================================================================
# What the prompts may and may not contain
# ======================================================================

def test_the_dlt_prompts_name_no_payload_specific_field():
    """The regression this whole change exists to prevent. A field name here
    is asserted against every structure on the DLT, including the ones that
    do not have it -- so per-type facts belong in the summary, not the prompt."""
    for name in ("DltInvestigatorAgent.md", "DltReviewerAgent.md",
                 "DltSynthesisAgent.md"):
        text = (PROMPTS / name).read_text(encoding="utf-8")
        for field in ("candidateRefId", "event_id", "abisMWResponseNewSeda",
                      "EnrolmentEventResponse", "ABIS"):
            assert field not in text, f"{name} names {field}"


def test_the_investigator_binds_to_the_labelling_instead():
    text = (PROMPTS / "DltInvestigatorAgent.md").read_text(encoding="utf-8")

    assert "Identifiers in this payload" in text
    assert "NONE LABELLED" in text
    assert "more than one original topic" in text


# ======================================================================
# The evidence block
# ======================================================================

def _state(**failure):
    return {
        "case_id": "dlt-topic-0-1",
        "failure": {"chain": [], "frames": [], **failure},
        "corroboration": {"verdict": "UNVERIFIABLE", "reason": "no logs"},
        "logs": "",
        "payload_summary": "Payload type: X",
    }


def test_the_evidence_block_names_the_topic_and_the_structure():
    block = _evidence_block(_state(origin_topic="ENU.MWARE.DEDUPE.V1",
                                   type_id=TYPE_B))

    assert "Original topic: ENU.MWARE.DEDUPE.V1" in block
    assert f"Payload type (__TypeId__): {TYPE_B}" in block
    assert f"### Message payload (structure: {TYPE_B})" in block


def test_a_missing_type_header_is_named_as_missing_not_omitted():
    """Silence would read as "the usual structure" to the model."""
    block = _evidence_block(_state())

    assert "Original topic: (unknown)" in block
    assert "(no __TypeId__ header)" in block
