"""
Phase 2 regression tests (AUDIT_2026_08.md section 6).

G15 -- packet metrics are recorded on every exit path, not only on success.
G16 -- every runbook lookup outcome is counted, so the hit rate has a
       denominator.
G17 -- a failed LLM call is counted, and breaker state is observable.
G23 -- the prompt fingerprint is stable, sensitive, and reaches the outcome.
"""
from unittest.mock import MagicMock, patch

import pytest

import src.core.agent_orchestrator as orch
from src.utils import metrics


def _label_values(counter, **labels):
    """Read a prometheus counter's current value for a label set.

    Returns None when prometheus_client is not installed, so these tests skip
    rather than fail on an optional dependency.
    """
    if not metrics.METRICS_AVAILABLE:
        return None
    return counter.labels(**labels)._value.get()


requires_prometheus = pytest.mark.skipif(
    not metrics.METRICS_AVAILABLE,
    reason="prometheus_client is an optional dependency",
)


# ======================================================================
# G15 -- every exit path is counted
# ======================================================================

@requires_prometheus
def test_packet_metrics_record_the_status_the_caller_sets():
    from src.api.routes import _packet_metrics

    before = _label_values(metrics.PACKETS_TOTAL,
                           status="FAILED_TIMEOUT", resolution_source="agent")

    with _packet_metrics() as outcome:
        outcome["status"] = "FAILED_TIMEOUT"

    after = _label_values(metrics.PACKETS_TOTAL,
                          status="FAILED_TIMEOUT", resolution_source="agent")
    assert after == before + 1, "the timeout path must be counted (G15)"


@requires_prometheus
def test_packet_metrics_record_even_when_the_body_raises():
    """A crash mid-packet is exactly the case worth measuring."""
    from src.api.routes import _packet_metrics

    before = _label_values(metrics.PACKETS_TOTAL,
                           status="unknown", resolution_source="agent")

    with pytest.raises(RuntimeError):
        with _packet_metrics() as outcome:
            raise RuntimeError("boom")

    after = _label_values(metrics.PACKETS_TOTAL,
                          status="unknown", resolution_source="agent")
    assert after == before + 1


@requires_prometheus
def test_runbook_source_collapses_to_a_bounded_label():
    """A per-runbook label would grow cardinality with the catalog."""
    from src.api.routes import _packet_metrics

    before = _label_values(metrics.PACKETS_TOTAL,
                           status="COMPLETED", resolution_source="runbook")

    with _packet_metrics() as outcome:
        outcome["status"] = "COMPLETED"
        outcome["source"] = "runbook:SOME_CODE__U@v7"

    after = _label_values(metrics.PACKETS_TOTAL,
                          status="COMPLETED", resolution_source="runbook")
    assert after == before + 1


# ======================================================================
# G16 -- the hit rate has a denominator
# ======================================================================

@requires_prometheus
def test_a_runbook_miss_is_counted(monkeypatch):
    """Only "hit" was ever recorded, so no rate could be computed."""
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setattr(orch, "_agent", None)

    before = _label_values(metrics.RUNBOOK_LOOKUPS, outcome="miss")

    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: MagicMock()), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", return_value=None):
        node = _runbook_node()
        result = node({"payload": {
            "eventId": "m1",
            "packetExecutionSummary": {"errorData": [{"errorReasonCode": "RC"}]},
        }})

    assert result["resolution_source"] == "agent"
    assert _label_values(metrics.RUNBOOK_LOOKUPS, outcome="miss") == before + 1


@requires_prometheus
def test_a_fingerprint_mismatch_is_counted_distinctly(monkeypatch):
    """A stale runbook is a different signal from a plain miss."""
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setattr(orch, "_agent", None)

    before = _label_values(metrics.RUNBOOK_LOOKUPS, outcome="fingerprint_mismatch")

    runbook = {"runbook_id": "RC__U", "version": 1,
               "rule_fingerprint": "sha256:stale",
               "resolution": {"action": "REPLAY"}}

    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: MagicMock()), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", return_value=runbook), \
         patch.object(orch, "lookup_rule_for", return_value=[{"rule": "x"}]):
        node = _runbook_node()
        result = node({"payload": {
            "eventId": "m2",
            "packetExecutionSummary": {"errorData": [{"errorReasonCode": "RC"}]},
        }})

    assert result["resolution_source"] == "agent"
    assert _label_values(metrics.RUNBOOK_LOOKUPS,
                         outcome="fingerprint_mismatch") == before + 1


@requires_prometheus
def test_a_runbook_error_is_counted(monkeypatch):
    """F2 was an uncaught exception here. It must degrade AND be visible."""
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setattr(orch, "_agent", None)

    before = _label_values(metrics.RUNBOOK_LOOKUPS, outcome="error")

    def boom(*_a, **_k):
        raise TypeError("'StructuredTool' object is not callable")

    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: MagicMock()), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", side_effect=boom):
        node = _runbook_node()
        result = node({"payload": {
            "eventId": "m3",
            "packetExecutionSummary": {"errorData": [{"errorReasonCode": "RC"}]},
        }})

    assert result["resolution_source"] == "agent"
    assert _label_values(metrics.RUNBOOK_LOOKUPS, outcome="error") == before + 1


def _runbook_node():
    """Build the graph and pull runbook_lookup_node back out of it.

    The node is a closure inside get_agent(), so it is reached through the
    compiled graph's node table rather than imported directly. Fails loudly if
    langgraph's internals move, instead of silently testing nothing.
    """
    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: MagicMock()), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        agent = orch.get_agent()

    node = agent.nodes["runbook_lookup"]
    func = getattr(getattr(node, "bound", None), "func", None)
    assert callable(func), (
        "could not reach runbook_lookup_node through the compiled graph; "
        "langgraph's PregelNode layout has changed"
    )
    return func


# ======================================================================
# G17 -- LLM failures and breaker state are observable
# ======================================================================

@requires_prometheus
def test_a_failed_llm_call_is_counted():
    before = _label_values(metrics.LLM_CALLS, node="investigator", outcome="error")

    def boom():
        raise RuntimeError("model unreachable")

    with pytest.raises(RuntimeError):
        orch._counted("investigator", boom)

    after = _label_values(metrics.LLM_CALLS, node="investigator", outcome="error")
    assert after == before + 1


def test_counted_returns_the_value_on_success():
    assert orch._counted("investigator", lambda: "ok") == "ok"


@requires_prometheus
def test_breaker_state_is_sampled():
    """Breaker trip frequency was named unknowable and stayed that way."""
    metrics.sample_breaker_states()
    value = metrics.BREAKER_STATE.labels(breaker="llm_breaker")._value.get()
    assert value in (0, 1, 2)


# ======================================================================
# G23 -- the prompt fingerprint
# ======================================================================

def test_prompt_fingerprint_is_stable_and_sensitive(tmp_path):
    """Stable across calls, and changed by any prompt edit -- otherwise it
    cannot attribute an accuracy movement to a prompt change."""
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    for name in orch.PROMPT_FILES:
        (prompts / name).write_text("original", encoding="utf-8")
    (tmp_path.parent / "agent_policy_context.md").write_text("policy",
                                                             encoding="utf-8")

    first = orch.compute_prompt_fingerprint(str(tmp_path))
    assert first == orch.compute_prompt_fingerprint(str(tmp_path)), "must be stable"
    assert first.startswith("sha256:")

    (prompts / orch.PROMPT_FILES[0]).write_text("edited", encoding="utf-8")
    assert orch.compute_prompt_fingerprint(str(tmp_path)) != first


def test_prompt_fingerprint_distinguishes_content_moving_between_files(tmp_path):
    """Length-prefixing means content shifting across a file boundary changes
    the digest, rather than hashing to the same concatenation."""
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    names = list(orch.PROMPT_FILES)
    for name in names:
        (prompts / name).write_text("", encoding="utf-8")

    (prompts / names[0]).write_text("ab", encoding="utf-8")
    (prompts / names[1]).write_text("", encoding="utf-8")
    first = orch.compute_prompt_fingerprint(str(tmp_path))

    (prompts / names[0]).write_text("a", encoding="utf-8")
    (prompts / names[1]).write_text("b", encoding="utf-8")
    assert orch.compute_prompt_fingerprint(str(tmp_path)) != first


def test_outcome_denormalises_the_new_grouping_keys(tmp_path, monkeypatch):
    """confidence, shadow, and prompt fingerprint must reach the outcome
    record, or none of them can be reported on."""
    from src.storage.local import LocalFilesystemCasebookStorage
    from src.utils import outcomes

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(outcomes, "get_casebook_storage", lambda: store)

    store.save("evt-x", {
        "packet_metadata": {"eid": "evt-x", "update_type": "U"},
        "packet_status": {"status": "COMPLETED",
                          "rejection_data": {"rejection_code": "RC"}},
        "resolution": {
            "source": "agent",
            "action": "REPLAY",
            "confidence": 0.75,
            "abstained": False,
            "shadow": {"runbook_id": "RC__U", "action": "WHITELISTING",
                       "agreed": False},
            "provenance": {"prompt_fingerprint": "sha256:deadbeef"},
        },
    })

    outcome = outcomes.record_outcome("evt-x", "CORRECT", "operator")

    assert outcome["confidence"] == 0.75
    assert outcome["abstained"] is False
    assert outcome["shadow_action"] == "WHITELISTING"
    assert outcome["shadow_runbook_id"] == "RC__U"
    assert outcome["shadow_agreed"] is False
    assert outcome["prompt_fingerprint"] == "sha256:deadbeef"
