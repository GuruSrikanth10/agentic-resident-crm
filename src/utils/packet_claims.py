"""One investigation per packet, even when duplicates arrive together.

`_investigate_packet` already guarded against duplicates: it read `status.json`,
checked for `IN_PROGRESS`, and returned early if a run was in flight. That is a
check followed by an act, with a storage round-trip in between, and nothing
made the pair atomic.

It failed in testing on 2026-09-24. Five deliveries of event
`6dbd1aa1-ed55-4894-975c-d68d33b87305` reached `/analyze-rejection` within six
milliseconds of each other. All five read `status.json` before any of them
wrote it, all five saw no `IN_PROGRESS`, and all five invoked the graph. Five
concurrent investigations of one packet then ran for nineteen minutes against
the same `thread_id`, interleaving writes into a single checkpoint row, and
between them saturated `MAX_CONCURRENT_INVESTIGATIONS` so nothing else could
be worked on. The packet also outran `max_poll_interval_ms`, so the consumer
was evicted from its group mid-run and the redelivery that followed added yet
more duplicates.

A claim closes that. `create_json` is create-only -- `O_CREAT | O_EXCL`
locally, `If-None-Match: *` on S3 -- so the check and the act are one
operation and exactly one caller can win it. This mirrors `src/dlt/claims.py`,
which solves the same problem for the DLT lane; the rejection lane is the
simpler case, because the storage key IS the claim key and no alias tracking
is needed.

The claim lives under its own storage root rather than in `status.json`.
`status.json` is legitimately written by `/fetch-logs` before analysis starts,
so a create-only write against it would always lose.

The rules for an existing claim:

  holder finished  the packet is already terminal. Skip -- though the
                   terminal-casebook check ahead of this normally gets there
                   first.
  holder stale     the claim is older than the TTL, so its run died without
                   finishing. Take it over. The TTL is twice the invoke
                   budget, because a run may legitimately use all of it and
                   reclaiming sooner would put two investigations in flight --
                   which is the failure this module exists to prevent.
  otherwise        a genuine concurrent duplicate. Skip.

There is deliberately no release step, as in the DLT lane. Every way a run
can end already resolves the claim: a completed, timed-out or DLQ'd packet is
terminal, so `_is_finished` skips the duplicate; and a run that died without
writing anything leaves a claim the TTL takes over. An explicit release would
need an ownership token to stop a losing duplicate freeing the winner's claim,
and would buy only a faster retry on the rare path where a run crashes before
any terminal write. `PACKET_CLAIM_TTL_SECONDS` shortens that wait when it
matters.

Failing open is deliberate, as in the DLT lane: the claim saves an LLM call,
and a claim store that is unreachable must not stop packets being analysed.
Any error lets the packet through and is counted under `error`.
"""
import os
import time
from dataclasses import dataclass
from typing import Optional

from src.storage.factory import get_scoped_storage
from src.utils import metrics
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

CLAIMS_ROOT_NAME = "packet_claims"
CLAIM_FILENAME = "claim.json"

#: Floor for the claim TTL, so a small invoke budget cannot make a healthy
#: in-flight investigation look abandoned.
MIN_CLAIM_TTL_SECONDS = 1800.0


@dataclass(frozen=True)
class Claim:
    won: bool
    #: "won", "reclaimed", "duplicate", "finished", "disabled", "error"
    outcome: str
    age_seconds: Optional[float] = None


def claims_enabled() -> bool:
    return get_bool_env("PACKET_CLAIM_ENABLED", True)


def claim_ttl_seconds() -> float:
    """How old an unfinished claim must be before its run is presumed dead.

    Twice the agent invoke budget, read at call time so it tracks a
    reconfigured deployment. An investigation can legitimately run for the
    whole budget; taking its claim over before then would put a second
    investigation of the same packet in flight -- the exact thing this exists
    to prevent.
    """
    explicit = os.environ.get("PACKET_CLAIM_TTL_SECONDS")
    if explicit:
        try:
            return max(1.0, float(explicit))
        except ValueError:
            pass
    budget = os.environ.get("AGENT_INVOKE_TIMEOUT_SECONDS") \
        or os.environ.get("PACKET_TIMEOUT_SECONDS") or "3600"
    try:
        return max(MIN_CLAIM_TTL_SECONDS, 2.0 * float(budget))
    except ValueError:
        return MIN_CLAIM_TTL_SECONDS * 2


def get_claim_storage():
    return get_scoped_storage(CLAIMS_ROOT_NAME)


def claim_packet(event_id: str) -> Claim:
    """Take the analysis claim on `event_id`, or report that it is held."""
    if not claims_enabled():
        return Claim(won=True, outcome="disabled")

    try:
        claim = _claim(event_id)
    except Exception as e:
        logger.warning("Packet claim failed; proceeding without dedupe",
                       event_id=event_id, error=f"{type(e).__name__}: {e}")
        claim = Claim(won=True, outcome="error")

    metrics.record_packet_claim(claim.outcome)
    return claim


def _claim(event_id: str) -> Claim:
    storage = get_claim_storage()
    now = time.time()

    if storage.create_json(event_id, CLAIM_FILENAME, {"event_id": event_id,
                                                      "claimed_at": now}):
        return Claim(won=True, outcome="won")

    existing = storage.load(event_id, filename=CLAIM_FILENAME) or {}
    age = now - float(existing.get("claimed_at") or 0.0)

    if _is_finished(event_id):
        return Claim(won=False, outcome="finished", age_seconds=round(age, 1))

    if age > claim_ttl_seconds():
        # Blind overwrite: create-only cannot replace, and a conditional
        # overwrite is what some S3-compatible stores refuse. Two redeliveries
        # racing to take over the same dead claim can both win, which is no
        # worse than having no claim at all and needs a dead holder first.
        storage.save(event_id, {"event_id": event_id, "claimed_at": now,
                                "reclaimed_after_seconds": round(age, 1)},
                     filename=CLAIM_FILENAME)
        logger.warning("Took over an abandoned packet claim", event_id=event_id,
                       claim_age_seconds=round(age, 1))
        return Claim(won=True, outcome="reclaimed", age_seconds=round(age, 1))

    return Claim(won=False, outcome="duplicate", age_seconds=round(age, 1))


def _is_finished(event_id: str) -> bool:
    from src.storage.base import TERMINAL_STATUSES
    from src.storage.factory import get_casebook_storage

    try:
        return get_casebook_storage().terminal_status(event_id) in TERMINAL_STATUSES
    except Exception:
        return False
