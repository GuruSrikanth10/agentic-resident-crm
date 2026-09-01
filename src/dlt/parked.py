"""Phase C7 of DLT_PLAN.md section 14 -- packets waiting for a deploy.

C6 withholds a replay when the fix is on `release` but not running yet.
Withholding without coming back to it would simply lose the packet, so this is
where it waits: one document per case, released once the pods reach the
version that carries the change.

**This is the feature.** `NO_CHANGE` only saves a replay that would have
failed; `NOT_DEPLOYED` turns "replay it and see" into "replay it after
Thursday's deploy", which is a scheduling decision the system can make and act
on by itself. Reading the *pod's* version rather than a manifest is what makes
that possible without ArgoCD or Gitea in the loop -- if the pod is on the new
version, it synced.

**One document per case, not a shared file.** `_queue_pending_replay` in
`tool_registry.py` learned this the hard way: a `pending_replays.jsonl` lived
on whichever pod wrote it, so under `CASEBOOK_STORAGE_BACKEND=s3` with more
than one replica the queue fragmented and an operator saw only their own pod's
entries. The same shape here would have the same bug, and a parked queue is
worse to lose than a pending one -- nobody is watching it.

**Parking requires everything a replay requires, minus the version.** A packet
is parked only when `auto_replay.decide` would have said yes *without* the
precheck veto. Otherwise the release worker would become a second replay path
that bypasses `DLT_AUTO_REPLAY_ENABLED` entirely, replaying packets the
operator never agreed to replay.

**Bounded on both axes.** A fix that never deploys must not park a packet
forever, and a queue nobody watches must not grow without limit -- so entries
expire and the store is capped, and both are logged and counted rather than
silent.
"""
import os
import re
import time
from typing import Optional

from src.dlt import code_check as code_check_module
from src.dlt import versions
from src.storage.factory import get_scoped_storage
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Storage root, alongside `dlt_cases` and `dlt_groups`.
PARKED_ROOT = "dlt_parked_replays"
PARKED_FILENAME = "parked_replay.json"

STATUS_PARKED = "parked"
STATUS_RELEASED = "released"
STATUS_EXPIRED = "expired"

#: 30 days. Longer than any plausible deploy cadence, short enough that a fix
#: which never ships does not hold a packet indefinitely.
DEFAULT_TTL_SECONDS = 30 * 24 * 3600
DEFAULT_CAP = 500


def get_parked_storage():
    return get_scoped_storage(PARKED_ROOT)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def park_enabled() -> bool:
    from src.utils.env import get_bool_env
    return get_bool_env("DLT_CODE_CHECK_PARK_ENABLED", False)


def ttl_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get("DLT_PARKED_REPLAY_TTL_SECONDS",
                                             str(DEFAULT_TTL_SECONDS))))
    except (ValueError, TypeError):
        return float(DEFAULT_TTL_SECONDS)


def cap() -> int:
    try:
        return max(1, int(os.environ.get("DLT_PARKED_REPLAY_CAP",
                                         str(DEFAULT_CAP))))
    except (ValueError, TypeError):
        return DEFAULT_CAP


# ---------------------------------------------------------------------------
# Parking
# ---------------------------------------------------------------------------

def _usable_key(case_id: str) -> bool:
    """The case id is interpolated into a storage path, so it carries the same
    guard `_queue_pending_replay` applies to a packet id (0.11)."""
    from src.models.schemas import EVENT_ID_PATTERN
    return bool(case_id and re.fullmatch(EVENT_ID_PATTERN, str(case_id).strip()))


def would_replay_but_for_the_version(finding, ref_id, code_check) -> bool:
    """Did only the precheck stand between this packet and a replay?

    Asked by re-running the replay gate *without* the verdict. Anything else
    that declines -- the feature flag, the action, the confidence, a missing
    refId -- means this packet was never going to be replayed, and parking it
    would create a second replay path that bypasses `DLT_AUTO_REPLAY_ENABLED`.
    """
    from src.dlt import auto_replay

    if getattr(code_check, "verdict", None) != code_check_module.NOT_DEPLOYED:
        return False
    if not auto_replay.code_check_gates_replay():
        # The verdict is not withholding anything, so there is nothing to
        # come back to.
        return False
    return auto_replay.decide(finding, ref_id, code_check=None).should_replay


def park(case_id: str, ref_id: str, code_check, finding=None) -> dict:
    """Record one packet as waiting for a version. Never raises."""
    required = getattr(code_check, "required_version", None)
    if not required:
        return {"parked": False,
                "reason": "the verdict named no version to wait for"}

    if not _usable_key(case_id):
        logger.error("Refusing to park a replay under an unusable case id",
                     case_id=case_id)
        return {"parked": False, "reason": "the case id is not a usable storage key"}

    storage = get_parked_storage()

    try:
        waiting = len([e for e in _entries(storage)
                       if e.get("status") == STATUS_PARKED])
    except Exception:
        waiting = 0
    if waiting >= cap():
        logger.warning("Parked-replay queue is at its cap; refusing to park",
                       cap=cap(), waiting=waiting, case_id=case_id)
        return {"parked": False,
                "reason": f"the parked queue is at its cap of {cap()}"}

    entry = {
        "case_id": case_id,
        "ref_id": ref_id,
        "required_version": str(required),
        "baseline_version": getattr(code_check, "baseline_version", None),
        "running_version_when_parked": getattr(code_check, "running_version", None),
        "repo": getattr(code_check, "repo", None),
        "path": getattr(code_check, "path", None),
        "action": getattr(finding, "action", None),
        "parked_at": time.time(),
        "status": STATUS_PARKED,
    }

    try:
        storage.save(case_id, entry, filename=PARKED_FILENAME)
    except Exception as e:
        logger.error("Failed to park a replay", case_id=case_id,
                     error=f"{type(e).__name__}: {e}")
        return {"parked": False, "reason": f"{type(e).__name__}: {e}"}

    logger.info("Parked a replay until the fix deploys", case_id=case_id,
                required_version=required)
    return {"parked": True,
            "reason": f"waiting for the pods to reach {required}",
            "required_version": str(required)}


def maybe_park(case_id: str, ref_id: Optional[str], code_check,
               finding=None) -> dict:
    """The one entry point `/analyze-dlt` calls.

    Always returns a dict -- parked or not, and why either way -- meant to be
    embedded verbatim in the casebook beside the `replay` block.
    """
    if not park_enabled():
        return {"parked": False, "reason": "DLT_CODE_CHECK_PARK_ENABLED is off"}
    if not ref_id:
        return {"parked": False, "reason": "no refId; nothing to replay later"}
    if not would_replay_but_for_the_version(finding, ref_id, code_check):
        return {"parked": False,
                "reason": "this packet was not going to be replayed anyway"}
    return park(case_id, ref_id, code_check, finding)


# ---------------------------------------------------------------------------
# Reading and releasing
# ---------------------------------------------------------------------------

def _entries(storage=None) -> list:
    storage = storage or get_parked_storage()
    out = []
    try:
        identifiers = storage.list_events()
    except Exception as e:
        logger.warning("Could not list parked replays",
                       error=f"{type(e).__name__}: {e}")
        return []
    for case_id in identifiers:
        try:
            entry = storage.load(case_id, filename=PARKED_FILENAME)
        except Exception:
            continue
        if entry:
            out.append(entry)
    return out


def list_parked(include_finished: bool = False) -> list:
    """Every parked entry, oldest first. The operator's view."""
    entries = _entries()
    if not include_finished:
        entries = [e for e in entries if e.get("status") == STATUS_PARKED]
    return sorted(entries, key=lambda e: e.get("parked_at") or 0)


def _mark(storage, case_id: str, status: str, **extra) -> None:
    def mutate(current: Optional[dict]) -> dict:
        entry = dict(current or {})
        entry["status"] = status
        entry.update(extra)
        return entry

    try:
        storage.update_json(case_id, PARKED_FILENAME, mutate)
    except Exception as e:
        logger.error("Could not update a parked replay", case_id=case_id,
                     status=status, error=f"{type(e).__name__}: {e}")


def release_ready(running_versions=(), dry_run: bool = False) -> dict:
    """Release every parked packet the running build now satisfies.

    Called by `src/tools/release_parked_replays.py`, on a schedule or by hand
    after a deploy. Releasing goes through `auto_replay.attempt`, which calls
    `queue_for_replay` -- so `ENABLE_AUTO_REPLAY` still decides whether the
    packet reaches OIS directly or lands in `pending_replays` for a human.
    Three switches deep, and each one means something different.

    An entry whose version cannot be compared is left parked rather than
    released: an unreadable running version is not evidence that a fix shipped.
    """
    from src.dlt import auto_replay

    storage = get_parked_storage()
    running = versions.lowest(running_versions) if running_versions else None
    now = time.time()
    ttl = ttl_seconds()

    summary = {"running_version": str(running) if running else None,
               "examined": 0, "released": 0, "expired": 0,
               "waiting": 0, "unknown": 0, "entries": []}

    for entry in list_parked():
        case_id = entry.get("case_id")
        summary["examined"] += 1

        age = now - float(entry.get("parked_at") or now)
        if ttl > 0 and age > ttl:
            summary["expired"] += 1
            summary["entries"].append({"case_id": case_id, "outcome": STATUS_EXPIRED})
            logger.warning("Parked replay expired before its fix deployed",
                           case_id=case_id, age_days=round(age / 86400, 1),
                           required_version=entry.get("required_version"))
            if not dry_run:
                _mark(storage, case_id, STATUS_EXPIRED, expired_at=now)
            continue

        satisfied = versions.at_least(running, entry.get("required_version"))
        if satisfied is None:
            summary["unknown"] += 1
            summary["entries"].append({"case_id": case_id, "outcome": "unknown"})
            continue
        if not satisfied:
            summary["waiting"] += 1
            summary["entries"].append({"case_id": case_id, "outcome": "waiting"})
            continue

        summary["released"] += 1
        outcome = {"case_id": case_id, "outcome": STATUS_RELEASED}
        if not dry_run:
            result = auto_replay.attempt(case_id, entry.get("ref_id"))
            outcome["queued"] = result.get("queued")
            _mark(storage, case_id, STATUS_RELEASED, released_at=now,
                  released_at_version=str(running) if running else None,
                  release_result=result.get("result"))
        summary["entries"].append(outcome)

    return summary
