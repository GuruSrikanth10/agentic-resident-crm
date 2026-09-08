"""
Phase E regression tests (ENHANCEMENT_PLAN.md section 5).

4.3 -- the Synthesis output contract is enforced, with one repair attempt.
4.4 -- confidence is capped on an incomplete trace, and low confidence abstains.
4.2 -- runbooks serve only for reason codes explicitly cleared.
4.6 -- the payload fields the system keys on are declared, not Dict[str, Any].
"""
import json


from src.models.synthesis import (
    SynthesisResult,
    apply_confidence_policy,
    extract_json_block,
    parse_synthesis,
)


# ======================================================================
# 4.3 -- the Synthesis contract
# ======================================================================

_VALID = {
    "rejection_description": "Manual dedup rejected the packet.",
    "synthesis": "Biometrics matched an existing record.",
    "action": "REPLAY",
    "resident_action": "NEW_PACKET",
}


def test_a_valid_response_parses():
    result, error = parse_synthesis(json.dumps(_VALID))
    assert error is None
    assert result.action == "REPLAY"
    assert result.resident_action == "NEW_PACKET"


def test_a_fenced_response_parses():
    result, error = parse_synthesis(f"Here you go:\n```json\n{json.dumps(_VALID)}\n```")
    assert error is None
    assert result.action == "REPLAY"


def test_a_near_miss_action_is_rejected():
    """An LLM returning "REPLAY_PACKET" used to be accepted verbatim and
    persisted into the casebook as if it were a real action."""
    payload = dict(_VALID, action="REPLAY_PACKET")
    result, error = parse_synthesis(json.dumps(payload))
    assert result is None
    assert "action" in error


def test_a_near_miss_resident_action_is_rejected():
    payload = dict(_VALID, resident_action="NEW PACKET")
    result, error = parse_synthesis(json.dumps(payload))
    assert result is None
    assert "resident_action" in error


def test_a_missing_action_is_rejected():
    payload = {k: v for k, v in _VALID.items() if k != "action"}
    result, error = parse_synthesis(json.dumps(payload))
    assert result is None
    assert "action" in error


def test_non_json_prose_is_rejected_with_a_reason():
    result, error = parse_synthesis("I think this packet should be replayed.")
    assert result is None
    assert error


def test_malformed_json_is_rejected_not_raised():
    result, error = parse_synthesis('{"action": "REPLAY", ')
    assert result is None
    assert "JSON" in error


def test_empty_response_is_rejected():
    for text in ("", None, "   "):
        result, error = parse_synthesis(text)
        assert result is None
        assert error


def test_escalation_shape_is_valid():
    """escalate_node and the unrepairable path both emit this."""
    result, error = parse_synthesis(json.dumps({
        "rejection_description": "ESCALATED",
        "synthesis": "ESCALATED TO HUMAN REVIEW.",
        "action": "MANUAL_REVIEW",
        "resident_action": "PENDING",
    }))
    assert error is None
    assert result.action == "MANUAL_REVIEW"


def test_confidence_out_of_range_is_rejected():
    payload = dict(_VALID, confidence=1.5)
    result, error = parse_synthesis(json.dumps(payload))
    assert result is None
    assert "confidence" in error


def test_absent_confidence_stays_none_rather_than_defaulting_to_certainty():
    """Defaulting to 1.0 would manufacture certainty the model never claimed."""
    result, _ = parse_synthesis(json.dumps(_VALID))
    assert result.confidence is None


def test_extract_json_block_handles_the_three_shapes():
    assert extract_json_block('{"a": 1}') == '{"a": 1}'
    assert extract_json_block('```json\n{"a": 1}\n```') == '{"a": 1}'
    assert extract_json_block('text {"a": 1} more') == '{"a": 1}'
    assert extract_json_block("no json here") is None


# ======================================================================
# 4.4 -- confidence and abstention
# ======================================================================

_BANNER = "--- EVIDENCE GAPS (the trace below is INCOMPLETE) ---"


def _result(confidence):
    return SynthesisResult(**dict(_VALID, confidence=confidence))


def test_confidence_is_capped_when_the_trace_has_gaps(monkeypatch):
    """A confident conclusion drawn from a trace we were told is incomplete is
    unsupported by construction."""
    monkeypatch.setenv("SYNTHESIS_GAP_CONFIDENCE_CEILING", "0.6")
    updated, abstained, reason = apply_confidence_policy(
        _result(0.95), logs=f"{_BANNER}\nsome partial trace"
    )
    assert updated.confidence == 0.6
    assert not abstained
    assert "evidence gaps" in reason


def test_confidence_is_untouched_on_a_complete_trace(monkeypatch):
    monkeypatch.setenv("SYNTHESIS_GAP_CONFIDENCE_CEILING", "0.6")
    updated, abstained, reason = apply_confidence_policy(
        _result(0.95), logs="a complete, gap-free trace"
    )
    assert updated.confidence == 0.95
    assert not abstained
    assert reason is None


def test_low_confidence_abstains_to_manual_review(monkeypatch):
    monkeypatch.setenv("SYNTHESIS_CONFIDENCE_THRESHOLD", "0.7")
    updated, abstained, reason = apply_confidence_policy(_result(0.3), logs="")
    assert abstained
    assert updated.action == "MANUAL_REVIEW"
    assert updated.resident_action == "PENDING"
    assert "below the 0.7 threshold" in reason


def test_high_confidence_acts(monkeypatch):
    monkeypatch.setenv("SYNTHESIS_CONFIDENCE_THRESHOLD", "0.7")
    updated, abstained, _ = apply_confidence_policy(_result(0.9), logs="")
    assert not abstained
    assert updated.action == "REPLAY"


def test_abstention_is_disabled_by_default(monkeypatch):
    """A confidence score nobody has calibrated is worse than none, so the
    threshold ships off until accuracy_report says what it is worth."""
    monkeypatch.delenv("SYNTHESIS_CONFIDENCE_THRESHOLD", raising=False)
    updated, abstained, _ = apply_confidence_policy(_result(0.01), logs="")
    assert not abstained
    assert updated.action == "REPLAY"


def test_missing_confidence_never_abstains(monkeypatch):
    """Absent confidence is unknown, not zero -- abstaining on every packet
    whose model omits the field would be an outage."""
    monkeypatch.setenv("SYNTHESIS_CONFIDENCE_THRESHOLD", "0.7")
    updated, abstained, _ = apply_confidence_policy(_result(None), logs="")
    assert not abstained
    assert updated.action == "REPLAY"


def test_gap_cap_can_push_a_result_below_the_threshold(monkeypatch):
    """The two policies compose: an incomplete trace can turn a confident
    answer into an abstention."""
    monkeypatch.setenv("SYNTHESIS_GAP_CONFIDENCE_CEILING", "0.5")
    monkeypatch.setenv("SYNTHESIS_CONFIDENCE_THRESHOLD", "0.7")
    updated, abstained, _ = apply_confidence_policy(
        _result(0.99), logs=f"{_BANNER}\npartial"
    )
    assert abstained
    assert updated.action == "MANUAL_REVIEW"


def test_the_original_result_is_not_mutated(monkeypatch):
    """Callers keep the original for the audit trail."""
    monkeypatch.setenv("SYNTHESIS_CONFIDENCE_THRESHOLD", "0.7")
    original = _result(0.3)
    apply_confidence_policy(original, logs="")
    assert original.action == "REPLAY"
    assert original.confidence == 0.3


# ======================================================================
# 4.2 -- per-reason-code runbook rollout
# ======================================================================

def test_no_allowlist_means_no_restriction(monkeypatch):
    """Existing deployments must be unaffected."""
    from src.utils import runbook_store
    monkeypatch.delenv("RUNBOOK_SERVE_ALLOWLIST", raising=False)
    assert runbook_store.serve_allowlist() is None
    assert runbook_store.is_serve_allowed("ANY_CODE")


def test_only_listed_codes_may_serve(monkeypatch):
    from src.utils import runbook_store
    monkeypatch.setenv("RUNBOOK_SERVE_ALLOWLIST", "CODE_A, CODE_B")
    assert runbook_store.is_serve_allowed("CODE_A")
    assert runbook_store.is_serve_allowed("CODE_B")
    assert not runbook_store.is_serve_allowed("CODE_C")


def test_a_non_allowlisted_code_shadows_instead_of_serving(monkeypatch):
    """It keeps running the agents and is compared against them -- which is
    how it earns its way onto the allowlist."""
    from src.utils import runbook_store

    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setenv("RUNBOOK_SERVE_ALLOWLIST", "OTHER_CODE")

    runbook = {
        "runbook_id": "CODE_C__U",
        "version": 2,
        "rule_fingerprint": "sha256:abc",
        "resolution": dict(_VALID),
    }

    # Mirror runbook_lookup_node's serve decision.
    reason_code = "CODE_C"
    mode = "serve"
    should_shadow = mode == "shadow" or not runbook_store.is_serve_allowed(reason_code)
    assert should_shadow

    monkeypatch.setenv("RUNBOOK_SERVE_ALLOWLIST", "CODE_C")
    assert runbook_store.is_serve_allowed(reason_code)


# ======================================================================
# 4.6 -- typed payload
# ======================================================================

def test_known_packet_fields_are_declared():
    from src.models.schemas import MessagePayload

    payload = MessagePayload(**{
        "eventId": "evt-1",
        "packetMetaData": {"refId": "ref-9", "srn": "srn-3", "enrolmentType": "U"},
        "flowMetaData": {"stage": "BIO", "subStage": "DEDUP"},
        "packetExecutionSummary": {"packetStatus": "REJECTED"},
    })

    assert payload.packetMetaData.refId == "ref-9"
    assert payload.packetMetaData.enrolmentType == "U"
    assert payload.flowMetaData.stage == "BIO"


def test_unknown_upstream_fields_are_preserved_not_rejected():
    """Rejecting a packet over a new upstream key would turn a schema addition
    into an outage."""
    from src.models.schemas import MessagePayload

    payload = MessagePayload(**{
        "eventId": "evt-1",
        "packetMetaData": {"refId": "r", "brandNewUpstreamField": "value"},
        "packetExecutionSummary": {"packetStatus": "REJECTED"},
    })

    assert payload.model_dump()["packetMetaData"]["brandNewUpstreamField"] == "value"


def test_missing_packet_metadata_is_still_allowed():
    """Declaring fields makes absence visible; it does not make them required."""
    from src.models.schemas import MessagePayload

    payload = MessagePayload(**{
        "eventId": "evt-1",
        "packetExecutionSummary": {"packetStatus": "REJECTED"},
    })
    assert payload.packetMetaData is None


def test_model_dump_still_yields_plain_dicts_for_downstream():
    """routes.py and fetch_logs_node index these with .get()."""
    from src.models.schemas import MessagePayload

    dumped = MessagePayload(**{
        "eventId": "evt-1",
        "packetMetaData": {"refId": "r", "srn": "s"},
        "packetExecutionSummary": {"packetStatus": "REJECTED"},
    }).model_dump()

    assert isinstance(dumped["packetMetaData"], dict)
    assert dumped["packetMetaData"].get("refId") == "r"


def test_identifiers_still_extract_from_a_dumped_payload():
    """F11's plumbing runs on model_dump() output, not the model."""
    from src.log_pipeline.sources.k8s.filtering import identifiers_from_payload
    from src.models.schemas import MessagePayload

    dumped = MessagePayload(**{
        "eventId": "evt-1",
        "packetMetaData": {"refId": "ref-9"},
        "packetExecutionSummary": {"packetStatus": "REJECTED"},
    }).model_dump()

    assert identifiers_from_payload(dumped) == ("evt-1", "ref-9")
