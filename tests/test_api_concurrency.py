"""Phase 2 of REMEDIATION_PLAN_2026_08_21.md -- API concurrency.

Two properties, and the second only became reachable once the first was fixed.

2.1  Nothing in the investigation path blocks the event loop. The dedicated
     executor kept `agent.invoke()` off it, but the eight storage round-trips
     and the S3 upload around that call were issued from the loop directly.

2.2  The agent graph is built exactly once under concurrency. This was masked
     while `get_agent()` ran on the loop, which serialised it; moving it off
     removes that accidental protection.
"""
import asyncio
import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.api.routes import _off_loop


# ======================================================================
# 2.1 -- blocking work runs off the event loop
# ======================================================================

def test_off_loop_keeps_the_event_loop_responsive():
    """The property under test, stated directly: while a slow blocking call is
    in flight via `_off_loop`, the loop must still run other coroutines.

    Before, /health and /ready sat behind every packet's S3 round-trips.
    """
    async def scenario():
        loop_ticks = 0

        async def heartbeat():
            nonlocal loop_ticks
            for _ in range(20):
                await asyncio.sleep(0.01)
                loop_ticks += 1

        async def slow_blocking_work():
            # 200ms of genuinely blocking, GIL-holding-ish I/O sleep.
            await _off_loop(time.sleep, 0.2)

        beat = asyncio.create_task(heartbeat())
        await slow_blocking_work()
        await beat
        return loop_ticks

    ticks = asyncio.run(scenario())
    # If the blocking call had been made on the loop, the heartbeat would have
    # been starved for the whole 200ms and completed far fewer ticks.
    assert ticks == 20


def test_off_loop_propagates_the_return_value():
    async def scenario():
        return await _off_loop(lambda a, b=0: a + b, 2, b=3)

    assert asyncio.run(scenario()) == 5


def test_off_loop_propagates_exceptions():
    """A storage failure must still reach the DLQ handler, not be swallowed."""
    def boom():
        raise RuntimeError("storage exploded")

    async def scenario():
        return await _off_loop(boom)

    with pytest.raises(RuntimeError, match="storage exploded"):
        asyncio.run(scenario())


def test_investigation_never_calls_storage_on_the_event_loop(monkeypatch):
    """End-to-end guard: run a whole packet and assert that no storage call
    was made from the thread running the event loop.

    This is the regression that matters -- a future edit that reintroduces a
    bare `storage.load(...)` in the async path fails here.
    """
    from src.api import routes
    from src.models.schemas import MessagePayload
    from test_phase1_fixes import _cleanup_casebook, _payload_with_event_id

    event_id = "phase2-off-loop"
    loop_thread_calls = []
    loop_thread_name = None

    real_storage = routes.get_casebook_storage()

    class _WatchingStorage:
        """Delegates to the real backend, recording the calling thread."""

        def __getattr__(self, name):
            attr = getattr(real_storage, name)
            if not callable(attr):
                return attr

            def wrapper(*args, **kwargs):
                if threading.current_thread().name == loop_thread_name:
                    loop_thread_calls.append(name)
                return attr(*args, **kwargs)

            return wrapper

    agent = MagicMock()
    agent.get_state.return_value = None
    agent.invoke.return_value = {
        "synthesis": json.dumps({"synthesis": "ok", "action": "REPLAY",
                                 "resident_action": "NEW_PACKET"}),
        "logs": "some logs",
    }

    async def scenario():
        nonlocal loop_thread_name
        loop_thread_name = threading.current_thread().name
        return await routes._investigate_packet(
            MessagePayload(**_payload_with_event_id(event_id)),
            {"status": None, "source": "agent"},
        )

    try:
        with patch("src.api.routes.get_agent", return_value=agent), \
             patch("src.api.routes.get_casebook_storage", return_value=_WatchingStorage()):
            result = asyncio.run(scenario())

        assert result["status"] == "processed"
        assert loop_thread_calls == [], (
            f"storage called on the event loop thread: {loop_thread_calls}")
    finally:
        _cleanup_casebook(event_id)


# ======================================================================
# 2.2 -- the graph is built once, under concurrency
# ======================================================================

def _hammer(target, threads: int = 16):
    """Call `target` from many threads released simultaneously."""
    barrier = threading.Barrier(threads)
    errors = []

    def run():
        try:
            barrier.wait()
            target()
        except Exception as e:  # pragma: no cover - surfaced by the assert
            errors.append(e)

    workers = [threading.Thread(target=run) for _ in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert not errors, errors


def test_rejection_agent_graph_is_built_exactly_once():
    from src.core import agent_orchestrator

    builds = []
    original = agent_orchestrator._build_agent

    def counting_build():
        builds.append(1)
        # A real build is slow; this widens the race window the lock closes.
        time.sleep(0.05)
        agent_orchestrator._agent = MagicMock(name="graph")
        return agent_orchestrator._agent

    saved_agent = agent_orchestrator._agent
    try:
        agent_orchestrator._agent = None
        agent_orchestrator._build_agent = counting_build
        _hammer(agent_orchestrator.get_agent)
        assert len(builds) == 1
    finally:
        agent_orchestrator._build_agent = original
        agent_orchestrator._agent = saved_agent


def test_dlt_agent_graph_is_built_exactly_once():
    from src.dlt import orchestrator

    builds = []
    original = orchestrator._build_dlt_agent

    def counting_build():
        builds.append(1)
        time.sleep(0.05)
        orchestrator._agent = MagicMock(name="dlt-graph")
        return orchestrator._agent

    saved_agent = orchestrator._agent
    try:
        orchestrator._agent = None
        orchestrator._build_dlt_agent = counting_build
        _hammer(orchestrator.get_dlt_agent)
        assert len(builds) == 1
    finally:
        orchestrator._build_dlt_agent = original
        orchestrator._agent = saved_agent


def test_casebook_storage_backend_is_built_exactly_once():
    from src.storage import factory

    builds = []
    real = factory.LocalFilesystemCasebookStorage

    def counting_ctor(*args, **kwargs):
        builds.append(1)
        time.sleep(0.05)
        return real(*args, **kwargs)

    saved = factory._STORAGE_CACHE
    try:
        factory.reset_storage_cache()
        with patch.object(factory, "LocalFilesystemCasebookStorage", counting_ctor):
            _hammer(factory.get_casebook_storage)
        assert len(builds) == 1
    finally:
        factory._STORAGE_CACHE = saved


def test_live_db_engine_is_built_exactly_once(monkeypatch):
    """A racing build leaked a second SQLAlchemy pool of 10 + 20 connections:
    only one engine was ever published, the other was unreachable and unclosed.
    """
    from src.tools import tool_registry

    builds = []

    def counting_create_engine(*args, **kwargs):
        builds.append(1)
        time.sleep(0.05)
        return MagicMock(name="engine")

    saved = tool_registry._LIVE_DB_ENGINE
    try:
        tool_registry._LIVE_DB_ENGINE = None
        monkeypatch.setattr(tool_registry, "create_engine", counting_create_engine)
        _hammer(tool_registry.get_live_db_engine)
        assert len(builds) == 1
    finally:
        tool_registry._LIVE_DB_ENGINE = saved


def test_template_catalog_is_built_exactly_once():
    from src.log_pipeline import pipeline

    builds = []
    real = pipeline.TemplateCatalog

    def counting_ctor(*args, **kwargs):
        builds.append(1)
        time.sleep(0.05)
        return real(*args, **kwargs)

    saved = pipeline._cached_catalog
    try:
        pipeline._cached_catalog = None
        with patch.object(pipeline, "TemplateCatalog", counting_ctor):
            _hammer(pipeline._get_catalog)
        assert len(builds) == 1
    finally:
        pipeline._cached_catalog = saved
