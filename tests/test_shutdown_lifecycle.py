"""Phase 4 of REMEDIATION_PLAN_2026_08_21.md -- shutdown and lifecycle.

The drain design was sound; one unbounded wait made it inert under exactly
the load it was written for, and the flag that is meant to stop new work
arriving was set too late for anyone to observe it.
"""
import threading

import pytest


# ======================================================================
# 4.1 -- the poll loop keeps polling, and sees a SIGTERM, with every slot busy
# ======================================================================

class _FakeConsumer:
    """The slice of KafkaConsumer the poll loop uses."""

    def __init__(self, partitions, batches=()):
        self._assigned = set(partitions)
        self._paused = set()
        self.batches = list(batches)
        self.polls = 0

    def assignment(self):
        return set(self._assigned)

    def paused(self):
        return set(self._paused)

    def pause(self, *partitions):
        self._paused |= set(partitions)

    def resume(self, *partitions):
        self._paused -= set(partitions)

    def poll(self, timeout_ms=0):
        self.polls += 1
        if self.batches and not self._paused:
            return self.batches.pop(0)
        return {}

    def commit(self, offsets=None):
        pass


@pytest.fixture
def _consumer(monkeypatch):
    from src.utils import kafkaConsumer

    handled = []

    def handle(tp, msg):
        handled.append(msg)
        # The real handler submits work; the slot stays taken until it ends.

    monkeypatch.setattr(kafkaConsumer, "SATURATED_POLL_SECONDS", 0.01)
    monkeypatch.setattr(kafkaConsumer, "IDLE_POLL_SECONDS", 0.01)
    monkeypatch.setattr(kafkaConsumer, "_handle_one_message", handle)
    monkeypatch.setattr(kafkaConsumer, "_backlog", kafkaConsumer.deque())
    monkeypatch.setattr(kafkaConsumer, "_queue_semaphore", threading.Semaphore(2))
    kafkaConsumer._shutdown.clear()
    kafkaConsumer.handled = handled
    yield kafkaConsumer
    kafkaConsumer._shutdown.clear()
    del kafkaConsumer.handled


def test_messages_beyond_the_free_slots_wait_and_fetching_pauses(_consumer, monkeypatch):
    fake = _FakeConsumer({"p0"}, batches=[{"p0": ["m1", "m2", "m3"]}])
    monkeypatch.setattr(_consumer, "consumer", fake)

    _consumer._poll_once()
    assert _consumer.handled == ["m1", "m2"]
    assert list(_consumer._backlog) == [("p0", "m3")]

    _consumer._poll_once()
    assert fake.paused() == {"p0"}, "fetching was not paused behind the backlog"
    assert _consumer.handled == ["m1", "m2"]


def test_the_loop_keeps_polling_while_every_slot_is_busy(_consumer, monkeypatch):
    """poll() is where kafka-python completes a rebalance. The loop used to
    stop calling it until a worker slot freed -- 9.5 minutes on 2026-09-29,
    with the whole group stalled mid-rebalance behind it."""
    fake = _FakeConsumer({"p0"}, batches=[{"p0": ["m1", "m2", "m3"]}])
    monkeypatch.setattr(_consumer, "consumer", fake)

    for _ in range(5):
        _consumer._poll_once()

    assert fake.polls == 5
    assert _consumer._backlog, "the third message should still be waiting"


def test_a_freed_slot_takes_the_backlog_and_fetching_resumes(_consumer, monkeypatch):
    fake = _FakeConsumer({"p0"}, batches=[{"p0": ["m1", "m2", "m3"]}])
    monkeypatch.setattr(_consumer, "consumer", fake)
    _consumer._poll_once()
    _consumer._poll_once()

    _consumer._queue_semaphore.release()  # a worker finishes
    _consumer._poll_once()

    assert _consumer.handled == ["m1", "m2", "m3"]
    assert not _consumer._backlog
    assert fake.paused() == set()


def test_revoked_partitions_leave_the_backlog(_consumer, monkeypatch):
    fake = _FakeConsumer({"p0", "p1"},
                         batches=[{"p0": ["a1", "a2"], "p1": ["b1", "b2"]}])
    monkeypatch.setattr(_consumer, "consumer", fake)
    _consumer._poll_once()
    assert len(_consumer._backlog) == 2

    _consumer._RebalanceListener().on_partitions_revoked({"p1"})

    assert all(tp == "p0" for tp, _msg in _consumer._backlog)


def test_a_shutdown_stops_dispatch_with_every_slot_busy(_consumer, monkeypatch):
    """Undispatched messages are not committed, so they are redelivered."""
    fake = _FakeConsumer({"p0"}, batches=[{"p0": ["m1", "m2", "m3"]}])
    monkeypatch.setattr(_consumer, "consumer", fake)
    _consumer._poll_once()
    _consumer._shutdown.set()
    _consumer._queue_semaphore.release()

    _consumer._dispatch_backlog()

    assert _consumer.handled == ["m1", "m2"]


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
