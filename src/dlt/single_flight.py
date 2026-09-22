"""One investigation per fingerprint at a time, within this process.

The reuse cache can only serve an answer that already exists. A burst of
identical fingerprints arrives before any of them has produced one, so every
member of the burst decides "novel fingerprint, no recommendation on file" and
pays for the LLM -- which is precisely the cost the group layer exists to
avoid. On 2026-09-22 all five cases reached the reuse decision within half a
second of each other, and the first recommendation existed thirty minutes
later. No cache, however correct, helps that.

This gate makes the first arrival investigate and the rest wait for it. A
waiter, once through, re-reads the group: if the leader left a recommendation
it is served (REUSE_GROUP, no LLM); if not -- the leader failed, timed out, or
produced nothing cacheable -- the waiter investigates in its turn, still
holding the gate so the next one waits on *it*.

**asyncio, not threading.** `analyze_dlt` is a coroutine on the event loop,
and the gate is held across the `await` on the LLM executor. A
`threading.Lock` acquired there would block the loop thread itself the moment
a second request for the same fingerprint arrived, and the first request --
which needs that same thread to resume -- could then never release it.

**In-process only.** Several API replicas each have their own gate, so a
burst split across N replicas still costs up to N investigations rather than
one. Closing that needs a lease in shared storage; the interface below is
where one would go, and it is not built until more than one replica runs.

**Bounded.** A waiter gives up after `wait_seconds()` and investigates anyway.
A gate that can hang a burst behind one stuck investigation is worse than the
duplicated LLM calls it exists to save.
"""
import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def enabled() -> bool:
    return get_bool_env("DLT_SINGLEFLIGHT_ENABLED", True)


def wait_seconds(budget: float) -> float:
    """How long a waiter may wait before investigating anyway.

    Defaults to the analysis budget: past that, the leader has itself been
    timed out and released the gate, so waiting longer cannot help.
    """
    raw = os.environ.get("DLT_SINGLEFLIGHT_WAIT_SECONDS")
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return max(0.0, budget)


@dataclass
class Flight:
    #: This request holds the gate. False only when the wait timed out.
    acquired: bool
    #: Another request for the same fingerprint was in flight on arrival, so
    #: the group may have changed while this one waited.
    waited: bool
    #: Seconds spent waiting for the gate.
    waited_seconds: float = 0.0


class _Entry:
    __slots__ = ("lock", "refs")

    def __init__(self):
        self.lock = asyncio.Lock()
        self.refs = 0


#: Keyed on (event loop, fingerprint). An asyncio.Lock belongs to the loop it
#: was first used on; keying on the loop keeps a lock from ever being reused
#: under a different one -- which happens under tests, where every call is a
#: fresh `asyncio.run`, and would otherwise raise.
#:
#: Only ever touched from coroutines on the loop, never from executor
#: threads, and never across an `await` -- so no further locking is needed.
_entries: dict = {}


@asynccontextmanager
async def flight(fingerprint: str, timeout: float):
    """Hold the gate for `fingerprint` for the duration of the block."""
    key = (asyncio.get_running_loop(), fingerprint)
    entry = _entries.get(key)
    if entry is None:
        entry = _entries[key] = _Entry()
    entry.refs += 1

    waited = entry.lock.locked()
    started = time.monotonic()
    acquired = False
    try:
        try:
            await asyncio.wait_for(entry.lock.acquire(), timeout=max(0.001, timeout))
            acquired = True
        except asyncio.TimeoutError:
            logger.warning("Gave up waiting for another investigation of this "
                           "fingerprint; investigating anyway",
                           fingerprint=fingerprint[:16],
                           waited_seconds=round(time.monotonic() - started, 3))

        yield Flight(acquired=acquired, waited=waited,
                     waited_seconds=time.monotonic() - started)
    finally:
        if acquired:
            entry.lock.release()
        entry.refs -= 1
        if entry.refs <= 0 and _entries.get(key) is entry:
            del _entries[key]


def in_flight(fingerprint: Optional[str] = None) -> int:
    """How many requests currently hold or await a gate. For tests/metrics."""
    return sum(entry.refs for (_, fp), entry in list(_entries.items())
               if fingerprint is None or fp == fingerprint)
