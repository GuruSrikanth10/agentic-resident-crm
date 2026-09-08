"""Phase 4 of REMEDIATION_PLAN_2026_08_21.md -- shutdown and lifecycle.

The drain design was sound; one unbounded wait made it inert under exactly
the load it was written for, and the flag that is meant to stop new work
arriving was set too late for anyone to observe it.
"""
import threading
import time

import pytest


# ======================================================================
# 4.1 -- a SIGTERM is observed even with every worker slot busy
# ======================================================================

@pytest.fixture
def _consumer(monkeypatch):
    from src.utils import kafkaConsumer

    monkeypatch.setattr(kafkaConsumer, "SLOT_ACQUIRE_POLL_SECONDS", 0.05)
    kafkaConsumer._shutdown.clear()
    yield kafkaConsumer
    kafkaConsumer._shutdown.clear()


def test_acquire_slot_returns_true_when_a_slot_is_free(_consumer):
    assert _consumer._acquire_slot() is True
    _consumer._queue_semaphore.release()


def test_acquire_slot_gives_up_when_a_shutdown_begins(_consumer):
    """The bare acquire() had no timeout: a thread already inside it never saw
    _shutdown and waited out a full PACKET_TIMEOUT_SECONDS, so the pod was
    SIGKILLed before the drain ran."""
    # Drain every slot so the acquire cannot succeed.
    taken = 0
    while _consumer._queue_semaphore.acquire(blocking=False):
        taken += 1

    result = {}

    def waiter():
        result["value"] = _consumer._acquire_slot()

    thread = threading.Thread(target=waiter)
    started = time.monotonic()
    thread.start()

    time.sleep(0.15)
    _consumer._shutdown.set()
    thread.join(timeout=3)

    assert not thread.is_alive(), "the poll loop never observed the shutdown"
    assert result["value"] is False
    assert time.monotonic() - started < 2, "shutdown was not observed promptly"

    for _ in range(taken):
        _consumer._queue_semaphore.release()


def test_acquire_slot_does_not_leak_a_permit_when_it_gives_up(_consumer):
    """Returning False must mean no slot was taken -- otherwise the drain
    would wait on a worker that never existed."""
    _consumer._shutdown.set()

    before = _consumer._queue_semaphore._value
    assert _consumer._acquire_slot() is False
    assert _consumer._queue_semaphore._value == before


# ======================================================================
# 4.2 -- draining is observable before the socket closes
# ======================================================================

def test_begin_draining_sets_the_flag_immediately():
    from src.api import routes

    routes._draining.clear()
    try:
        assert routes._draining.is_set() is False
        routes.begin_draining()
        assert routes._draining.is_set() is True
    finally:
        routes._draining.clear()


def test_readiness_fails_while_draining():
    """This is the point of the flag: the orchestrator stops routing new
    packets here while the ones already accepted finish."""
    from fastapi import HTTPException

    from src.api import routes

    routes._draining.clear()
    routes.begin_draining()
    try:
        with pytest.raises(HTTPException) as excinfo:
            routes.readiness_check()
        assert excinfo.value.status_code == 503
        assert excinfo.value.detail == "Draining"
    finally:
        routes._draining.clear()


def test_begin_draining_is_idempotent():
    from src.api import routes

    routes._draining.clear()
    try:
        routes.begin_draining()
        routes.begin_draining()
        assert routes._draining.is_set()
    finally:
        routes._draining.clear()


def test_the_signal_handler_chains_to_the_previous_one():
    """uvicorn needs its own handler to run, or the graceful shutdown it
    drives never starts. Replacing it would hang the pod until SIGKILL."""
    import signal

    import src.main_api as main_api
    from src.api import routes

    called = []
    original = signal.getsignal(signal.SIGTERM)

    def uvicorns_handler(signum, frame):
        called.append(signum)

    signal.signal(signal.SIGTERM, uvicorns_handler)
    routes._draining.clear()
    try:
        main_api._install_draining_signal_handlers()
        installed = signal.getsignal(signal.SIGTERM)
        assert installed is not uvicorns_handler, "handler was not wrapped"

        installed(signal.SIGTERM, None)

        assert routes._draining.is_set(), "draining flag was not set"
        assert called == [signal.SIGTERM], "the previous handler was not called"
    finally:
        signal.signal(signal.SIGTERM, original)
        routes._draining.clear()


# ======================================================================
# 4.3 -- a duplicate in-flight id stays visible to the drain
# ======================================================================

def test_a_duplicate_event_id_is_counted_twice():
    """With a set, the first invocation to finish discarded the id while the
    second was still running, so the drain left an IN_PROGRESS stub behind."""
    from src.api import routes

    before = routes._in_flight_investigations()

    with routes._tracked_in_flight("evt-dup"):
        with routes._tracked_in_flight("evt-dup"):
            assert routes._in_flight_investigations() == before + 2
        # The inner invocation finished; the outer is still running.
        assert routes._in_flight_investigations() == before + 1
        assert "evt-dup" in routes._in_flight_events

    assert routes._in_flight_investigations() == before
    assert "evt-dup" not in routes._in_flight_events


def test_tracking_still_deregisters_on_an_exception():
    from src.api import routes

    before = routes._in_flight_investigations()

    with pytest.raises(RuntimeError):
        with routes._tracked_in_flight("evt-boom"):
            raise RuntimeError("boom")

    assert routes._in_flight_investigations() == before
    assert "evt-boom" not in routes._in_flight_events


def test_the_drain_sees_a_still_running_duplicate(tmp_path, monkeypatch):
    """End-to-end: the id must reach the abandoned-investigation marker."""
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

    store.save("dup", {"packet_metadata": {"eid": "dup"},
                       "packet_status": {"status": "IN_PROGRESS"}},
               filename="status.json")

    with routes._in_flight_lock:
        routes._in_flight_events["dup"] += 2   # two concurrent invocations

    try:
        # One of them finishes; the other is still running.
        with routes._in_flight_lock:
            routes._in_flight_events["dup"] -= 1
        routes.drain_and_shutdown()
    finally:
        routes._draining.clear()
        with routes._in_flight_lock:
            routes._in_flight_events.pop("dup", None)

    assert store.terminal_status("dup") == "FAILED_SHUTDOWN"
