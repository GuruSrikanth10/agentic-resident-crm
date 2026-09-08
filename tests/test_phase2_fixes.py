import os
import time
import asyncio
import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import MessagePayload
from src.storage.factory import get_casebook_storage
from src.api.routes import process_rejection
from src.utils.paths import CHECKPOINT_DB_PATH

from test_phase1_fixes import _payload_with_event_id, _cleanup_casebook


def _clear_checkpoint_db():
    # This test drives a real SqliteSaver-backed graph to completion, which
    # persists MagicMock-valued messages into the shared checkpoint DB. A
    # later invoke() for the same thread_id would resume from that poisoned
    # checkpoint and fail to serialize it -- wipe first so this test's
    # outcome never depends on what a previous run left behind.
    for suffix in ("", "-wal", "-shm"):
        db_file = Path(str(CHECKPOINT_DB_PATH) + suffix)
        if db_file.exists():
            db_file.unlink()


# ======================================================================
# 2.3 -- the Investigator prompt is projected to the fields it needs on the
# first attempt, and sends only the delta (prior investigation + feedback)
# on retries instead of resending the full payload/logs/rule context.
# ======================================================================

def test_investigator_prompt_trims_context_on_retry(monkeypatch):
    import src.core.agent_orchestrator as orch

    _clear_checkpoint_db()

    mock_agent_instance = MagicMock()
    mock_agent_instance.invoke.side_effect = [
        {"messages": [MagicMock(content="Investigation output 1")]},  # Investigator run 1
        {"messages": [MagicMock(content="REJECTED: Needs fix")]},     # Reviewer run 1
        {"messages": [MagicMock(content="Investigation output 2")]},  # Investigator run 2 (retry)
        {"messages": [MagicMock(content="APPROVED")]},                # Reviewer run 2
        {"messages": [MagicMock(content="{}")]},                      # Synthesis
    ]

    event_id = "phase2-retry-trim"
    payload = _payload_with_event_id(event_id)

    try:
        with patch.object(orch, "create_react_agent", return_value=mock_agent_instance), \
             patch.object(orch, "_agent", None):
            asyncio.run(process_rejection(MessagePayload(**payload)))

        calls = mock_agent_instance.invoke.call_args_list
        first_investigator_prompt = calls[0].args[0]["messages"][1].content
        retry_investigator_prompt = calls[2].args[0]["messages"][1].content

        # First attempt: the (projected) payload and rule config are present;
        # there is no "previous analysis" yet.
        assert "Kafka Payload:" in first_investigator_prompt
        assert "Your previous analysis" not in first_investigator_prompt
        # Only the fields the prompt needs -- not the whole raw Kafka message.
        assert "sourceTopic" not in first_investigator_prompt

        # Retry: the delta (prior investigation + reviewer feedback), not a
        # second full copy of the payload.
        assert "Your previous analysis" in retry_investigator_prompt
        assert "Investigation output 1" in retry_investigator_prompt
        assert "REJECTED: Needs fix" in retry_investigator_prompt
        assert "Kafka Payload:" not in retry_investigator_prompt
    finally:
        _cleanup_casebook(event_id)
        _clear_checkpoint_db()


# ======================================================================
# 2.1 / 2.2 -- the Reviewer agent is built once (not per-review) and uses
# the cheaper "simple" LLM tier instead of the unused-but-constructed one.
# ======================================================================

@pytest.mark.xfail(
    strict=True,
    reason=(
        "Deliberate deviation: the Reviewer is bound to the 'complex' tier at "
        "agent_orchestrator.py get_agent(). The 'simple'-tier fix was explicitly "
        "declined by the requester (ENHANCEMENT_PLAN section 7.1, AUDIT_2026_08 "
        "G6). This assertion is kept -- and kept failing -- so the cost signal "
        "survives rather than being deleted. strict=True means that if the fix "
        "is ever applied, this test fails as XPASS and forces the marker off, "
        "so the two can never silently drift apart."
    ),
)
def test_reviewer_built_once_with_simple_llm(monkeypatch):
    import src.core.agent_orchestrator as orch

    monkeypatch.setattr(orch, "_agent", None)

    llm_calls = []

    def fake_get_llm(tier):
        obj = MagicMock(name=f"llm-{tier}")
        llm_calls.append((tier, obj))
        return obj

    create_react_agent_calls = []

    def fake_create_react_agent(llm, tools):
        create_react_agent_calls.append((llm, tools))
        return MagicMock()

    with patch.object(orch, "get_llm", side_effect=fake_get_llm), \
         patch.object(orch, "create_react_agent", side_effect=fake_create_react_agent):
        orch.get_agent()

    tiers_requested = [tier for tier, _ in llm_calls]
    # get_llm("complex")/("simple") each requested exactly once, at
    # graph-construction time -- not once per packet.
    assert tiers_requested.count("complex") == 1
    assert tiers_requested.count("simple") == 1

    llm_by_tier = dict(llm_calls)
    complex_llm_obj = llm_by_tier["complex"]
    simple_llm_obj = llm_by_tier["simple"]

    # investigator + synthesis + reviewer -- three agents, built exactly once.
    assert len(create_react_agent_calls) == 3

    reviewer_calls = [c for c in create_react_agent_calls if c[0] is simple_llm_obj]
    assert len(reviewer_calls) == 1
    _, reviewer_tools = reviewer_calls[0]
    assert [t.name for t in reviewer_tools] == ["add_learning_rule"]

    complex_calls = [c for c in create_react_agent_calls if c[0] is complex_llm_obj]
    assert len(complex_calls) == 2


def test_add_learning_rule_uses_per_packet_contextvars(monkeypatch, tmp_path):
    import src.core.agent_orchestrator as orch
    import json

    monkeypatch.setattr(orch, "_agent", None)

    captured = {}

    def fake_create_react_agent(llm, tools):
        if tools and getattr(tools[0], "name", "") == "add_learning_rule":
            captured["tool"] = tools[0]
        return MagicMock()

    with patch.object(orch, "create_react_agent", side_effect=fake_create_react_agent):
        orch.get_agent()

    add_learning_rule = captured["tool"]

    base_dir = os.path.dirname(os.path.dirname(orch.__file__))
    target_file = os.path.join(base_dir, "prompts", "pending_rules.jsonl")
    if os.path.exists(target_file):
        os.remove(target_file)

    try:
        orch._current_event_id.set("evt-context-test")
        orch._current_investigation.set("some investigation text")

        add_learning_rule.invoke({"rule_text": "always double-check X", "reasoning": "because Y"})

        with open(target_file, "r", encoding="utf-8") as f:
            lines = [json.loads(ln) for ln in f if ln.strip()]

        assert len(lines) == 1
        assert lines[0]["eventId"] == "evt-context-test"
        assert lines[0]["investigator_original_output"] == "some investigation text"
    finally:
        if os.path.exists(target_file):
            os.remove(target_file)


# ======================================================================
# 2.4 -- the mock rules table is indexed once, not linearly rescanned on
# every lookup.
# ======================================================================

def test_rule_index_built_once_and_matches(monkeypatch, tmp_path):
    import src.tools.tool_registry as tr

    csv_path = tmp_path / "mock_rules.csv"
    csv_path.write_text(
        "reject_reason_code,rule_data\n"
        "CODE_A,{\"x\":1}\n"
        "CODE_B,{\"x\":2}\n"
        "CODE_A,{\"x\":3}\n"
    )

    monkeypatch.setenv("MOCK_DB_PATH", str(csv_path))
    monkeypatch.setattr(tr, "_DB_CACHE", None)
    monkeypatch.setattr(tr, "_RULE_INDEX_CACHE", None)

    result_a = tr._lookup_rule_by_reason_code_impl("CODE_A")
    # _RULE_INDEX_CACHE is now populated; capture its identity to prove the
    # next lookup reuses it rather than rebuilding (2.4).
    cache_after_first_lookup = tr._RULE_INDEX_CACHE
    assert cache_after_first_lookup is not None

    result_b = tr._lookup_rule_by_reason_code_impl("CODE_B")
    result_missing = tr._lookup_rule_by_reason_code_impl("NOPE")

    assert tr._RULE_INDEX_CACHE is cache_after_first_lookup

    target_col, index = tr._RULE_INDEX_CACHE
    assert target_col == "reject_reason_code"
    assert sorted(index.keys()) == ["CODE_A", "CODE_B"]

    assert "CODE_A" in result_a and result_a.count("CODE_A") == 2
    assert "CODE_B" in result_b
    assert "Rule not found" in result_missing


# ======================================================================
# 2.5 -- the Drain3 TemplateMiner is a process-wide singleton.
# ======================================================================

def test_template_miner_is_a_singleton(monkeypatch, tmp_path):
    from src.log_pipeline import reducer as reducer_module

    monkeypatch.setattr(reducer_module, "DRAIN3_STATE_DIR", str(tmp_path / "drain3_state"))
    monkeypatch.setattr(reducer_module, "_template_miner_instance", None)

    with reducer_module._drain3_intraprocess_lock:
        miner1 = reducer_module._get_template_miner()
        miner2 = reducer_module._get_template_miner()

    assert miner1 is miner2


# ======================================================================
# 2.6 -- agent.invoke runs on the dedicated bounded executor, and the
# server-side timeout path works correctly through the async endpoint.
# ======================================================================

def test_process_rejection_is_async_and_executor_is_bounded():
    import src.api.routes as routes_module

    assert inspect.iscoroutinefunction(process_rejection)
    assert routes_module._agent_invoke_executor._max_workers == routes_module._MAX_CONCURRENT_INVESTIGATIONS


def test_agent_invoke_timeout_returns_failed_timeout(monkeypatch):
    event_id = "phase2-invoke-timeout"
    monkeypatch.setenv("AGENT_INVOKE_TIMEOUT_SECONDS", "0.05")

    def slow_invoke(*args, **kwargs):
        time.sleep(2)
        return {"synthesis": "{}"}

    mock_agent = MagicMock()
    mock_agent.get_state.return_value = None
    mock_agent.invoke.side_effect = slow_invoke

    try:
        with patch("src.api.routes.get_agent", return_value=mock_agent):
            res = asyncio.run(process_rejection(MessagePayload(**_payload_with_event_id(event_id))))

        assert res["status"] == "failed_timeout"
        storage = get_casebook_storage()
        status_doc = storage.load(event_id, filename="status.json")
        assert status_doc["packet_status"]["status"] == "FAILED_TIMEOUT"
    finally:
        _cleanup_casebook(event_id)


# ======================================================================
# 2.7 -- Kafka producer health is cached, not re-checked on every /ready.
# ======================================================================

def test_producer_health_is_cached(monkeypatch):
    import src.api.routes as routes_module

    monkeypatch.setattr(routes_module, "_producer_health_cache", {"ready": False, "checked_at": 0.0})

    mock_producer = MagicMock()
    call_count = {"n": 0}

    def fake_get_producer():
        call_count["n"] += 1
        return mock_producer

    with patch("src.utils.dlq_publisher.get_producer", side_effect=fake_get_producer):
        first = routes_module._check_kafka_producer_ready()
        second = routes_module._check_kafka_producer_ready()

    assert first is True
    assert second is True
    assert call_count["n"] == 1  # second call served from cache, not a fresh connection attempt

    # Force the cache to look stale -- the next check must hit get_producer again.
    routes_module._producer_health_cache["checked_at"] = 0.0
    with patch("src.utils.dlq_publisher.get_producer", side_effect=fake_get_producer):
        third = routes_module._check_kafka_producer_ready()

    assert third is True
    assert call_count["n"] == 2
