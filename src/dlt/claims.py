"""One analysis per DLT record, whatever key it arrives under.

`identity.py` derives `case_id = dlt-{topic}-{partition}-{offset}` and calls
it "the only naturally unique, naturally idempotent key available on a DLT
message". Storage, though, is keyed on the refId -- deliberately, so an
operator who only knows the refId can find the casebook -- and every dedupe
check reads storage. So the idempotent key was computed, written into the
casebook body, and never used to deduplicate anything.

It mattered on 2026-09-22. One DLT record (partition 17, offset 5051943)
arrived under two record keys; the key is what `resolve_ref_id` prefers, so
the two deliveries got two refIds, two storage keys, two casebooks and two
full LLM investigations.

A claim closes that without moving the storage key. The first delivery of a
record creates `dlt_claims/casebook_<case_id>/claim.json` naming its refId;
any later delivery under a *different* refId finds the claim and is skipped.
The claim is create-only, which every S3-compatible store supports -- unlike
the conditional overwrite that `update_json` depends on.

The rules for an existing claim:

  same refId          the same case arriving again (a retry, a redelivery).
                      Let it through; the terminal-status check that follows
                      is what stops a finished case being redone.
  holder finished     a duplicate of a record already analysed. Skip.
  holder stale        the holder never finished and its claim is older than
                      the TTL -- its run died. Take the claim over.
  otherwise           a duplicate of a record in flight. Skip.

Failing open is deliberate. Dedupe saves an LLM call; a claim store that is
down must not stop packets being analysed, so any error lets the case
through and is counted.
"""
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

from src.models.schemas import EVENT_ID_PATTERN
from src.storage.factory import get_scoped_storage
from src.utils import metrics
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

CLAIMS_ROOT_NAME = "dlt_claims"
CLAIM_FILENAME = "claim.json"
ALIASES_SUBDIR = "aliases"

#: Floor for the claim TTL, so a small analysis budget cannot make a healthy
#: in-flight claim look abandoned.
MIN_CLAIM_TTL_SECONDS = 1800.0


@dataclass(frozen=True)
class Claim:
    won: bool
    #: "won", "same_delivery", "reclaimed", "duplicate", "disabled", "error"
    outcome: str
    holder_ref_id: Optional[str] = None


def claims_enabled() -> bool:
    return get_bool_env("DLT_CLAIM_ENABLED", True)


def claim_ttl_seconds() -> float:
    """How old an unfinished claim must be before it is presumed dead.

    Twice the analysis budget, read at call time so it tracks a reconfigured
    consumer. A case can legitimately sit in the analysis queue and then run
    for the whole budget; taking its claim over before then would put a second
    investigation of the same record in flight.
    """
    explicit = os.environ.get("DLT_CLAIM_TTL_SECONDS")
    if explicit:
        try:
            return max(1.0, float(explicit))
        except ValueError:
            pass
    budget = float(os.environ.get("DLT_ANALYSIS_TIMEOUT_SECONDS", "300"))
    return max(MIN_CLAIM_TTL_SECONDS, 2.0 * budget)


def get_claim_storage():
    return get_scoped_storage(CLAIMS_ROOT_NAME)


def claim_case(case_id: str, ref_id: str) -> Claim:
    """Take the claim on `case_id` for `ref_id`, or report who holds it."""
    if not claims_enabled():
        return Claim(won=True, outcome="disabled")

    try:
        claim = _claim(case_id, ref_id)
    except Exception as e:
        logger.warning("Case claim failed; proceeding without dedupe",
                       case_id=case_id, ref_id=ref_id,
                       error=f"{type(e).__name__}: {e}")
        claim = Claim(won=True, outcome="error")

    metrics.record_dlt_claim(claim.outcome)
    return claim


def _claim(case_id: str, ref_id: str) -> Claim:
    storage = get_claim_storage()
    now = time.time()

    if storage.create_json(case_id, CLAIM_FILENAME,
                           {"case_id": case_id, "ref_id": ref_id, "claimed_at": now}):
        _record_alias(storage, case_id, ref_id)
        return Claim(won=True, outcome="won", holder_ref_id=ref_id)

    existing = storage.load(case_id, filename=CLAIM_FILENAME) or {}
    holder = existing.get("ref_id")
    _record_alias(storage, case_id, ref_id)

    if holder == ref_id:
        return Claim(won=True, outcome="same_delivery", holder_ref_id=holder)

    if holder and _is_finished(holder):
        return Claim(won=False, outcome="duplicate", holder_ref_id=holder)

    age = now - float(existing.get("claimed_at") or 0.0)
    if not holder or age > claim_ttl_seconds():
        # Blind overwrite: create-only cannot replace, and a conditional
        # overwrite is exactly what some stores refuse. Two duplicates racing
        # to take over the same dead claim can both win here -- which is no
        # worse than having no claim at all, and needs a dead holder first.
        storage.save(case_id, {"case_id": case_id, "ref_id": ref_id,
                               "claimed_at": now, "reclaimed_from": holder,
                               "reclaimed_after_seconds": round(age, 1)},
                     filename=CLAIM_FILENAME)
        logger.warning("Took over an abandoned case claim",
                       case_id=case_id, ref_id=ref_id, previous_holder=holder,
                       claim_age_seconds=round(age, 1))
        return Claim(won=True, outcome="reclaimed", holder_ref_id=ref_id)

    return Claim(won=False, outcome="duplicate", holder_ref_id=holder)


def holder_of(case_id: str) -> Optional[str]:
    """The refId currently holding `case_id`, or None. Never raises.

    Read by the analysis lane, which must not analyse a record whose claim
    has passed to another delivery since this one was queued.
    """
    if not claims_enabled():
        return None
    try:
        return (get_claim_storage().load(case_id, filename=CLAIM_FILENAME) or {}).get("ref_id")
    except Exception as e:
        logger.warning("Could not read case claim", case_id=case_id,
                       error=f"{type(e).__name__}: {e}")
        return None


def aliases_of(case_id: str) -> list:
    """Every refId this record has been seen under. For operators: a skipped
    duplicate has no casebook of its own, so this is how its refId is found."""
    try:
        return [name[:-len(".json")]
                for name in get_claim_storage().list_json(case_id, ALIASES_SUBDIR)]
    except Exception:
        return []


_SAFE_NAME = re.compile(EVENT_ID_PATTERN)


def _record_alias(storage, case_id: str, ref_id: str) -> None:
    # The refId becomes a filename here. A record-key refId is already
    # shape-checked, but one found by searching the payload is not, and a "/"
    # in it would address a path outside `aliases/`. The alias is only an
    # operator convenience, so an unsafe one is simply not recorded.
    if not ref_id or not _SAFE_NAME.match(ref_id):
        return
    try:
        storage.create_json(case_id, f"{ALIASES_SUBDIR}/{ref_id}.json",
                            {"ref_id": ref_id, "seen_at": time.time()})
    except Exception as e:
        logger.debug("Could not record claim alias", case_id=case_id,
                     ref_id=ref_id, error=f"{type(e).__name__}: {e}")


def _is_finished(ref_id: str) -> bool:
    from src.dlt.case_storage import terminal_status
    from src.storage.base import TERMINAL_STATUSES

    try:
        return terminal_status(ref_id) in TERMINAL_STATUSES
    except Exception:
        return False
