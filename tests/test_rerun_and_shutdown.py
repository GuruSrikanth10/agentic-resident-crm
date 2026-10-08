"""
Three defects found from traces on 2026-10-08, each pinned through the real
route, graph, storage and checkpointer with only the agents scripted.

- A packet run again inherited the last run's graph state: the route reset
  `retry_count` alone, and the Investigator took the old Reviewer verdict for
  a rejection and "fixed" the previous analysis instead of investigating.
- A packet the API shut down on was marked FAILED_SHUTDOWN, "to be
  redelivered" -- and the redelivery was skipped as already processed, so it
  was never investigated and never dead-lettered.
- The approval check read "APPROVED: NO -- ..." as an approval.
"""
import shutil
import time
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
from src.utils import packet_claims
from test_end_to_end import _run, storage  # noqa: F401  (storage: a fixture)

EVENT = "rerun-1"

SYNTHESIS = ('{"rejection_description": "manual dedup rejected the packet", '
             '"synthesis": "ask the resident to resubmit", '
             '"action": "RESIDENT_PACKET_RESUBMIT", "resident_action": "NEW_PACKET", '
             '"confidence": 0.9}')


def _scripted(replies: dict, seen: dict):
    """build_agent's stand-in: each role's agent answers from `replies[role]`
    and records the request it was sent in `seen[role]`."""
    def build(role, *args, **kwargs):
        agent = MagicMock()

        def invoke(request, config=None):
            seen.setdefault(role, []).append(request["messages"][0].content)
            return {"messages": [AIMessage(content=replies[role].pop(0))]}

        agent.invoke.side_effect = invoke
        return agent
    return build


def _forget_casebook(store, event_id):
    """What an operator does to have a packet investigated again."""
    shutil.rmtree(store._resolve_dir(event_id))


# ======================================================================
# A packet run again starts from nothing
# ======================================================================

def test_a_packet_run_again_is_investigated_afresh(storage, monkeypatch):  # noqa: F811
    monkeypatch.setenv("PACKET_CLAIM_ENABLED", "false")
    replies = {"investigator": ["Finding A", "Finding B", "Finding C"],
               "reviewer": ["REJECTED\nThe finding cites no log line.", "APPROVED",
                            "APPROVED"],
               "synthesis": [SYNTHESIS, SYNTHESIS]}
    seen: dict = {}
    with patch.object(orch, "build_agent", side_effect=_scripted(replies, seen)), \
            patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        assert _run(EVENT)["status"] == "processed"
        _forget_casebook(storage, EVENT)
        assert _run(EVENT)["status"] == "processed"

    # Run 1: a first pass, then the retry the rejection asked for.
    assert "Your previous analysis" not in seen["investigator"][0]
    assert "Your previous analysis" in seen["investigator"][1]
    # Run 2 is a first pass again -- not a "fix" of run 1's last analysis
    # against run 1's last verdict.
    rerun = seen["investigator"][2]
    assert "Your previous analysis" not in rerun
    assert "Reviewer Feedback" not in rerun
    assert "Finding B" not in rerun
    assert storage.load(EVENT)["resolution"]["action"] == "RESIDENT_PACKET_RESUBMIT"


def test_a_fresh_run_discards_a_finished_runs_state():
    agent = MagicMock()
    agent.get_state.return_value.values = {"reviewer_feedback": "APPROVED"}
    config = {"configurable": {"thread_id": "evt-1"}}
    orch.invoke_fresh(agent, {"payload": {}}, config)
    agent.checkpointer.delete_thread.assert_called_once_with("evt-1")
    agent.invoke.assert_called_once_with({"payload": {}}, config=config)


@pytest.mark.parametrize("state", [None, MagicMock(values={})])
def test_a_first_run_deletes_nothing(state):
    agent = MagicMock()
    agent.get_state.return_value = state
    orch.invoke_fresh(agent, {"payload": {}}, {"configurable": {"thread_id": "evt-1"}})
    agent.checkpointer.delete_thread.assert_not_called()
    agent.invoke.assert_called_once()


# ======================================================================
# A packet the API shut down on is investigated when it comes back
# ======================================================================

def _abandon(store, event_id, *, claimed_seconds_before=30.0, stamped=True):
    """The state a shutdown leaves: the abandoned run's claim, then a
    FAILED_SHUTDOWN casebook recording when it was abandoned."""
    abandoned_at = time.time()
    packet_claims.get_claim_storage().create_json(
        event_id, packet_claims.CLAIM_FILENAME,
        {"event_id": event_id, "claimed_at": abandoned_at - claimed_seconds_before})
    metadata = {"eid": event_id}
    if stamped:
        metadata["abandoned_at"] = abandoned_at
    store.save_terminal(event_id, {"packet_metadata": metadata,
                                   "packet_status": {"status": "FAILED_SHUTDOWN"},
                                   "resolution": {"synthesis": "will be redelivered"}})


@pytest.fixture
def factory_storage(storage, monkeypatch):  # noqa: F811
    """The claim's checks read the casebook through the storage factory."""
    import src.storage.factory as factory

    monkeypatch.setattr(factory, "get_casebook_storage", lambda: storage)
    return storage


def test_a_packet_abandoned_at_shutdown_is_investigated_when_redelivered(factory_storage):
    _abandon(factory_storage, EVENT)
    replies = {"investigator": ["Finding"], "reviewer": ["APPROVED"], "synthesis": [SYNTHESIS]}
    seen: dict = {}
    with patch.object(orch, "build_agent", side_effect=_scripted(replies, seen)), \
            patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        response = _run(EVENT)

    assert response["status"] == "processed"
    assert len(seen["investigator"]) == 1
    assert factory_storage.terminal_status(EVENT) != "FAILED_SHUTDOWN"
    assert factory_storage.load(EVENT)["resolution"]["action"] == "RESIDENT_PACKET_RESUBMIT"


def test_the_abandoned_runs_claim_is_taken_over_at_once(factory_storage):
    _abandon(factory_storage, EVENT)
    claim = packet_claims.claim_packet(EVENT)
    assert (claim.won, claim.outcome) == (True, "reclaimed")
    # A duplicate of the redelivery finds the redelivery's newer claim.
    duplicate = packet_claims.claim_packet(EVENT)
    assert (duplicate.won, duplicate.outcome) == (False, "duplicate")


def test_a_claim_taken_after_the_shutdown_is_not_taken_over(factory_storage):
    _abandon(factory_storage, EVENT, claimed_seconds_before=-5.0)
    claim = packet_claims.claim_packet(EVENT)
    assert (claim.won, claim.outcome) == (False, "duplicate")


def test_an_unstamped_shutdown_waits_for_the_ttl_as_before(factory_storage):
    _abandon(factory_storage, EVENT, stamped=False)
    claim = packet_claims.claim_packet(EVENT)
    assert (claim.won, claim.outcome) == (False, "duplicate")


def test_failed_shutdown_is_not_finished_but_other_terminals_are(factory_storage):
    _abandon(factory_storage, EVENT)
    assert packet_claims._is_finished(EVENT) is False
    factory_storage.save_terminal("done", {"packet_metadata": {"eid": "done"},
                                           "packet_status": {"status": "COMPLETED"}})
    assert packet_claims._is_finished("done") is True


def test_the_drain_records_when_it_abandoned_a_run(tmp_path, monkeypatch):
    import concurrent.futures

    from src.api import dlt_routes, routes
    from src.storage.local import LocalFilesystemCasebookStorage

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(routes, "API_SHUTDOWN_DRAIN_SECONDS", 0.05)
    monkeypatch.setattr(routes, "_agent_invoke_executor",
                        concurrent.futures.ThreadPoolExecutor(max_workers=1))
    monkeypatch.setattr(dlt_routes, "_dlt_invoke_executor",
                        concurrent.futures.ThreadPoolExecutor(max_workers=1))
    with routes._in_flight_lock:
        routes._in_flight_events["straggler"] += 1
    before = time.time()
    try:
        routes.drain_and_shutdown()
    finally:
        routes._draining.clear()
        with routes._in_flight_lock:
            routes._in_flight_events.pop("straggler", None)

    casebook = store.load("straggler")
    assert casebook["packet_status"]["status"] == "FAILED_SHUTDOWN"
    assert casebook["packet_metadata"]["abandoned_at"] >= before


# ======================================================================
# Only an approval approves
# ======================================================================

@pytest.mark.parametrize("reply, approved", [
    # Read as approvals by the old prefix check; each one rejects.
    ("APPROVED: NO -- the finding is not grounded", False),
    ("APPROVED? No.", False),
    ("Approved with corrections: the stage is wrong", False),
    ("APPROVED - but the stage is wrong", False),
    ("APPROVED.REJECTED", False),
    # Read as rejections by the old check; each one approves.
    ("# APPROVED", True),
    ("<think>\nThe finding cites the rule.\n</think>\nAPPROVED", True),
    # The verdict ending its sentence is still a verdict.
    ("APPROVED.", True),
    ("APPROVED! The finding is grounded.", True),
    ("**APPROVED.**\nreasoning", True),
    ("<think>\nNot grounded.\n</think>\n\nREJECTED", False),
])
def test_a_qualified_approval_is_a_rejection(reply, approved):
    assert orch.is_reviewer_approved(reply) is approved


@pytest.mark.parametrize("feedback, reviews, expected", [
    ("APPROVED", 1, "synthesis"),
    ("APPROVED", 3, "synthesis"),
    ("REJECTED\nno", 1, "investigator"),
    ("REJECTED\nno", 3, "escalate"),
    ("APPROVED: NO", 3, "escalate"),
])
def test_the_routing_rule(monkeypatch, feedback, reviews, expected):
    monkeypatch.setenv("MAX_INVESTIGATION_RETRIES", "3")
    assert orch.review_decision(feedback, reviews) == expected
