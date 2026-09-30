"""An agent run the API has stopped waiting for stops, and says how it ended.

On 2026-09-29 every timed-out run kept its `agent-invoke` worker to the end of
the graph: the next batch queued behind them and timed out unstarted, and the
abandoned runs went on through Reviewer and Synthesis with their side-effect
tools in reach. See src/core/run_control.py.
"""
import asyncio
import concurrent.futures
import threading
import time
from typing import TypedDict
from unittest.mock import MagicMock, patch

import pybreaker
import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph

from src.core import run_control
from src.core.agent_factory import build_agent
from src.core.run_control import RunCancelled
from src.models.schemas import MessagePayload
from test_agent_factory import PACK, ScriptedModel, calls_tool
from test_phase1_fixes import _cleanup_casebook, _payload_with_event_id


class _State(TypedDict, total=False):
    out: str


def test_an_abandoned_run_makes_no_further_tool_or_model_call():
    """Cancelled while its model call is in flight: the tool that call asked
    for is refused and no second model call is made. Run as the route runs
    it -- a graph node, on an executor thread, under run_in_scope -- so this
    also proves the cancel flag reaches the agent through LangGraph."""
    cancel = threading.Event()
    replayed = []

    @tool
    def queue_for_replay_stand_in(id: str) -> str:
        """Stands in for queue_for_replay."""
        replayed.append(id)
        return "queued"

    class CancelledMidCall(ScriptedModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            cancel.set()  # the budget runs out while this call is in flight
            return super()._generate(messages, stop, run_manager, **kwargs)

    model = CancelledMidCall(replies=[
        calls_tool("queue_for_replay_stand_in", {"id": "P1"}, "c1"),
        AIMessage(content="never reached"),
    ], requests=[])
    agent = build_agent("synthesis", model, "ROLE",
                        tools=[queue_for_replay_stand_in], pack=PACK)

    def node(state: _State):
        agent.invoke({"messages": [HumanMessage(content="go")]})
        return {"out": "done"}

    graph = StateGraph(_State)
    graph.add_node("synthesize", node)
    graph.add_edge(START, "synthesize")
    graph.add_edge("synthesize", END)
    compiled = graph.compile()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        run = pool.submit(run_control.run_in_scope, cancel,
                          lambda: compiled.invoke({}))
        with pytest.raises(RunCancelled):
            run.result(timeout=30)

    assert replayed == []
    assert len(model.requests) == 1


def test_a_run_that_is_not_cancelled_is_untouched():
    model = ScriptedModel(replies=[AIMessage(content="ok")], requests=[])
    agent = build_agent("synthesis", model, "ROLE", pack=PACK)
    result = run_control.run_in_scope(
        threading.Event(), lambda: agent.invoke({"messages": [HumanMessage(content="go")]}))
    assert result["messages"][-1].content == "ok"


def test_outside_a_run_nothing_is_cancelled():
    assert run_control.cancelled() is False
    run_control.check()


def test_a_cancelled_run_does_not_count_towards_opening_the_llm_circuit():
    from src.utils.resilience import llm_breaker

    @llm_breaker
    def abandoned():
        raise RunCancelled("abandoned")

    for _ in range(llm_breaker.fail_max + 2):
        with pytest.raises(RunCancelled):
            abandoned()
    assert llm_breaker.current_state == pybreaker.STATE_CLOSED


def test_the_route_cancels_the_run_it_stops_waiting_for(monkeypatch):
    """The timeout sets the run's cancel flag, and the run's end is logged."""
    import src.api.routes as routes

    event_id = "cancel-on-timeout"
    monkeypatch.setenv("AGENT_INVOKE_TIMEOUT_SECONDS", "0.1")
    seen = {}
    ended = threading.Event()

    def slow_invoke(*args, **kwargs):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if run_control.cancelled():
                seen["cancelled"] = True
                break
            time.sleep(0.01)
        run_control.check()
        return {"synthesis": "{}"}

    endings = []
    real_log = routes._log_abandoned_run

    def recording_log(event, run):
        endings.append(run.exception())
        real_log(event, run)
        ended.set()

    monkeypatch.setattr(routes, "_log_abandoned_run", recording_log)
    mock_agent = MagicMock()
    mock_agent.get_state.return_value = None
    mock_agent.invoke.side_effect = slow_invoke

    try:
        with patch("src.api.routes.get_agent", return_value=mock_agent), \
             patch("src.api.routes.publish_to_dlq"):
            res = asyncio.run(routes.process_rejection(
                MessagePayload(**_payload_with_event_id(event_id))))
        assert res["status"] == "failed_timeout"
        assert ended.wait(5)
        assert seen.get("cancelled") is True
        assert isinstance(endings[0], RunCancelled)
    finally:
        _cleanup_casebook(event_id)


def test_queue_for_replay_refuses_an_abandoned_run(monkeypatch):
    from src.tools import tool_registry

    queued = []
    monkeypatch.setattr(tool_registry, "_queue_pending_replay",
                        lambda packet_id, payload: queued.append(packet_id) or "queued")
    cancel = threading.Event()
    cancel.set()
    args = {"id": "P1", "idType": "EID", "priority": 1, "operatorName": "op",
            "category": "c", "fromSedaStart": False}

    refused = run_control.run_in_scope(
        cancel, lambda: tool_registry.queue_for_replay.invoke(args))

    assert "NOT queued" in refused
    assert queued == []


# ----------------------------------------------------------------------
# Cleanup keeps the local backend's record (case_cleanup.py)
# ----------------------------------------------------------------------

@pytest.fixture
def casesheets(tmp_path, monkeypatch):
    import src.storage.factory as factory
    import src.utils.case_cleanup as case_cleanup
    from src.storage.local import LocalFilesystemCasebookStorage

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(case_cleanup, "LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr(factory, "get_casebook_storage", lambda: store)
    return tmp_path, store


def _record(store, event_id):
    store.save_terminal(event_id, {"packet_metadata": {"eid": event_id},
                                   "packet_status": {"status": "COMPLETED"}})


def test_cleanup_keeps_a_record_the_local_backend_holds(casesheets):
    from src.utils.case_cleanup import cleanup_casebook_dir

    root, store = casesheets
    _record(store, "kept")
    cleanup_casebook_dir("kept")

    assert store.terminal_status("kept") == "COMPLETED"
    assert store.exists("kept", terminal_only=True)


def test_cleanup_still_removes_a_working_directory(casesheets):
    from src.utils.case_cleanup import cleanup_casebook_dir

    root, _store = casesheets
    working = root / "casebook_scratch"
    working.mkdir()
    (working / "context.json").write_text("{}")
    cleanup_casebook_dir("scratch")

    assert not working.exists()


def test_cleanup_removes_the_directory_when_the_record_lives_elsewhere(casesheets, monkeypatch):
    import src.storage.factory as factory
    from src.utils.case_cleanup import cleanup_casebook_dir

    root, store = casesheets
    _record(store, "remote")
    monkeypatch.setattr(factory, "get_casebook_storage", lambda: MagicMock())
    cleanup_casebook_dir("remote")

    assert not (root / "casebook_remote").exists()


def test_the_reaper_keeps_records_and_reaps_leftovers(casesheets):
    import os
    from src.utils.case_cleanup import reap_stale_casebooks

    root, store = casesheets
    _record(store, "old-record")
    leftover = root / "casebook_old-leftover"
    leftover.mkdir()
    (leftover / "filtered_logs.txt").write_text("x")
    old = time.time() - 7200
    for path in (root / "casebook_old-record", leftover):
        os.utime(path, (old, old))

    assert reap_stale_casebooks(max_age_seconds=3600) == 1
    assert store.terminal_status("old-record") == "COMPLETED"
    assert not leftover.exists()
