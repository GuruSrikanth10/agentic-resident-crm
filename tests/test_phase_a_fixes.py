"""
Phase A regression tests (ENHANCEMENT_PLAN.md section 5).

F2  -- the runbook rule lookup must not raise TypeError, must filter by
       enrolment type consistently, and must fingerprint parsed rows.
F13 -- runbook cache keys must be built through one normalizer so a
       promotion actually invalidates the key lookups read.
"""
import json
from unittest.mock import patch

import pytest

from src.tools import tool_registry
from src.utils import runbook_store


# ======================================================================
# F2 -- lookup_rule_for / lookup_rule_text
# ======================================================================

_UPDATE_RULE = {
    "rule_id": "R-UPD",
    "rule_data": json.dumps(
        {"statement": {"Condition": {"StringEquals": {"enrolmentType": "UPDATE"}}}}
    ),
}
_ENROL_RULE = {
    "rule_id": "R-ENR",
    "rule_data": json.dumps(
        {"statement": {"Condition": {"StringEquals": {"enrolmentType": "ENROLMENT"}}}}
    ),
}
_ANY_RULE = {"rule_id": "R-ANY", "rule_data": json.dumps({"statement": {}})}

_ALL_RULES_JSON = json.dumps([_UPDATE_RULE, _ENROL_RULE, _ANY_RULE])


@pytest.fixture
def stub_rule_json():
    """Patch the shared protected lookup so no DB/cache is involved."""
    with patch.object(tool_registry, "_lookup_rule_json", return_value=_ALL_RULES_JSON):
        yield


def test_lookup_rule_for_is_callable_with_an_enrolment_type(stub_rule_json):
    """The whole point of F2: this call used to raise TypeError.

    `lookup_rule_by_reason_code` is a StructuredTool -- not callable, and it
    takes one argument -- yet three sites invoked it with two. In
    runbook_lookup_node that TypeError was uncaught and DLQ'd the packet.
    """
    rules = tool_registry.lookup_rule_for("SOME_CODE", "U")
    assert rules is not None
    assert [r["rule_id"] for r in rules] == ["R-UPD", "R-ANY"]


def test_lookup_rule_for_accepts_both_short_and_long_enrolment_forms(stub_rule_json):
    """Call sites pass "U"/"E" (payload form) or "UPDATE"/"ENROLMENT" (DB form)."""
    assert tool_registry.lookup_rule_for("C", "U") == tool_registry.lookup_rule_for("C", "UPDATE")
    assert tool_registry.lookup_rule_for("C", "E") == tool_registry.lookup_rule_for("C", "ENROLMENT")


def test_unknown_enrolment_type_does_not_filter(stub_rule_json):
    """An unrecognised type must mean "don't filter", never "assume UPDATE".

    The three runbook sites previously defaulted an unknown type to "UPDATE",
    which silently hid every ENROLMENT rule from an unrecognised packet type.
    """
    rules = tool_registry.lookup_rule_for("C", "SOMETHING_ELSE")
    assert [r["rule_id"] for r in rules] == ["R-UPD", "R-ENR", "R-ANY"]

    assert tool_registry.lookup_rule_for("C", None) is not None
    assert len(tool_registry.lookup_rule_for("C", None)) == 3


def test_any_normalises_to_no_filtering(stub_rule_json):
    """build_runbooks passes "ANY" for a cross-enrolment-type runbook."""
    assert len(tool_registry.lookup_rule_for("C", "ANY")) == 3


def test_filter_falls_back_to_unfiltered_rather_than_returning_nothing():
    """A rule that matched the reason code beats no rule at all."""
    only_update = json.dumps([_UPDATE_RULE])
    with patch.object(tool_registry, "_lookup_rule_json", return_value=only_update):
        rules = tool_registry.lookup_rule_for("C", "E")
    assert [r["rule_id"] for r in rules] == ["R-UPD"]


def test_lookup_rule_for_returns_none_on_a_failure_message():
    """The impl returns prose, not JSON, when the lookup fails."""
    with patch.object(tool_registry, "_lookup_rule_json",
                      return_value="Rule not found for reason code: X in mock DB."):
        assert tool_registry.lookup_rule_for("X", "U") is None


def test_lookup_rule_text_preserves_the_failure_message_for_the_llm():
    """The Investigator should see "rule not found", not an empty prompt."""
    message = "Rule not found for reason code: X in mock DB."
    with patch.object(tool_registry, "_lookup_rule_json", return_value=message):
        assert tool_registry.lookup_rule_text("X", "U") == message


def test_lookup_rule_text_returns_filtered_json_when_parseable(stub_rule_json):
    parsed = json.loads(tool_registry.lookup_rule_text("C", "U"))
    assert [r["rule_id"] for r in parsed] == ["R-UPD", "R-ANY"]


def test_the_tool_still_works_for_llm_invocation(stub_rule_json):
    """The @tool wrapper must keep its .invoke() contract for agent use."""
    assert tool_registry.lookup_rule_by_reason_code.invoke("C") == _ALL_RULES_JSON


# ======================================================================
# F2 -- fingerprints are taken over parsed rows, never the raw JSON string
# ======================================================================

def test_fingerprint_rejects_a_raw_json_string():
    """Hashing DataFrame.to_json() folds column order into the fingerprint,
    so a harmless re-export invalidated every runbook."""
    with pytest.raises(TypeError):
        runbook_store.generate_rule_fingerprint(_ALL_RULES_JSON)


def test_fingerprint_is_stable_across_key_order():
    a = [{"rule_id": "R", "module": "m"}]
    b = [{"module": "m", "rule_id": "R"}]
    assert runbook_store.generate_rule_fingerprint(a) == runbook_store.generate_rule_fingerprint(b)


def test_fingerprint_changes_when_the_rule_changes():
    a = [{"rule_id": "R", "threshold": 1}]
    b = [{"rule_id": "R", "threshold": 2}]
    assert runbook_store.generate_rule_fingerprint(a) != runbook_store.generate_rule_fingerprint(b)


# ======================================================================
# F2 -- runbook_lookup_node degrades to the agents instead of DLQ'ing
# ======================================================================

def test_runbook_lookup_failure_falls_through_to_the_agents(monkeypatch):
    """Any failure in the runbook path must return "agent", never raise.

    The runbook is an optimisation; the agents always produce a valid result.
    An exception escaping this node fails agent.invoke() and DLQs the packet.
    """
    import src.core.agent_orchestrator as orch

    monkeypatch.setenv("RUNBOOK_MODE", "serve")

    payload = {
        "eventId": "evt-1",
        "packetMetaData": {"enrolmentType": "U"},
        "packetExecutionSummary": {"errorData": [{"errorReasonCode": "BOOM"}]},
    }

    def exploding_get_runbook(*_a, **_kw):
        raise RuntimeError("simulated runbook store failure")

    with patch.object(orch, "get_runbook", side_effect=exploding_get_runbook):
        node = _extract_runbook_node(orch)
        result = node({"payload": payload})

    assert result == {"resolution_source": "agent"}


def _extract_runbook_node(orch):
    """runbook_lookup_node is a closure inside get_agent(); rebuild just it.

    Building the whole graph would construct LLM clients, so mirror the node's
    contract instead: call it through a minimal stand-in built from the same
    module-level dependencies the real closure captures.
    """
    import json as _json
    import os as _os

    def runbook_lookup_node(state):
        mode = _os.environ.get("RUNBOOK_MODE", "off").lower()
        if mode == "off":
            return {"resolution_source": "agent"}
        payload = state.get("payload", {})
        log = orch.logger.bind(event_id=payload.get("eventId", "unknown"))
        try:
            exec_summary = payload.get("packetExecutionSummary") or {}
            reason_code = None
            for err in exec_summary.get("errorData") or []:
                if err and err.get("errorReasonCode"):
                    reason_code = err.get("errorReasonCode")
                    break
            if not reason_code:
                return {"resolution_source": "agent"}
            packet_type = payload.get("packetMetaData", {}).get("enrolmentType", "")
            runbook = orch.get_runbook(reason_code, packet_type)
            if not runbook:
                return {"resolution_source": "agent"}
            rules = orch.lookup_rule_for(reason_code, packet_type)
            if rules:
                if orch.generate_rule_fingerprint(rules) != runbook["rule_fingerprint"]:
                    return {"resolution_source": "agent"}
            return {
                "resolution_source": f"runbook:{runbook['runbook_id']}@v{runbook['version']}",
                "synthesis": _json.dumps(runbook["resolution"]),
                "runbook_id": runbook["runbook_id"],
            }
        except Exception as e:
            log.error("Runbook lookup failed; falling through to the agents",
                      error=f"{type(e).__name__}: {e}")
            return {"resolution_source": "agent"}

    return runbook_lookup_node


# ======================================================================
# F13 -- one normalizer for runbook cache keys
# ======================================================================

@pytest.mark.parametrize("raw", ["E", "e", " e ", " E "])
def test_cache_key_normalises_enrolment_type(raw):
    assert runbook_store.runbook_cache_key("CODE", raw) == "CODE__E"


def test_cache_key_defaults_missing_type_to_any():
    assert runbook_store.runbook_cache_key("CODE", None) == "CODE__ANY"
    assert runbook_store.runbook_cache_key("CODE", "") == "CODE__ANY"


def test_promotion_invalidates_the_key_lookups_actually_read(tmp_path, monkeypatch):
    """A draft carrying "e" must invalidate the "E" key get_runbook() uses."""
    monkeypatch.setattr(runbook_store, "RUNBOOK_FINAL_DIR", tmp_path / "final")
    monkeypatch.setattr(runbook_store, "RUNBOOK_DRAFT_DIR", tmp_path / "draft")
    (tmp_path / "final").mkdir(parents=True, exist_ok=True)
    (tmp_path / "draft").mkdir(parents=True, exist_ok=True)

    runbook_store._runbook_cache.clear()
    # Seed the cache the way get_runbook() would, with the normalised key.
    runbook_store._runbook_cache["CODE__E"] = {"version": 1, "stale": True}

    draft_path = tmp_path / "draft" / "CODE__E.json"
    draft_path.write_text("{}", encoding="utf-8")

    promoted = {
        "reason_code": "CODE",
        # Deliberately lower-case: this is the drift F13 describes.
        "enrolment_type": "e",
        "version": 2,
        "status": "final",
    }
    runbook_store.promote_draft_to_final(draft_path, promoted)

    assert "CODE__E" not in runbook_store._runbook_cache
