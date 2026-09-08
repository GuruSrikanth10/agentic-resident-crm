"""
Resolution outcome recording (ENHANCEMENT_PLAN.md section 4.1).

The gap this closes: nothing in the system measured whether a resolution was
actually right. `eval_harness.py` scores the log pipeline's evidence citation,
not whether the `action` the Synthesis agent chose resolved the packet. So
there was no way to safely auto-promote a runbook, detect agent regression
after a prompt change, justify raising automation levels, or answer "how well
does this work?"

An outcome is deliberately a SEPARATE write from the casebook, appended to the
casebook's own directory as `outcome.json`. It is human-supplied ground truth
about an agent-produced artifact, arriving minutes to weeks later; folding it
into casebook.json would mean rewriting a terminal record long after the fact
and would make "what did the agent conclude" and "was it right" impossible to
tell apart in an audit.
"""
from datetime import datetime, timezone
from typing import Optional

from src.storage.base import OUTCOME_VERDICTS
from src.storage.factory import get_casebook_storage
from src.utils.logging_config import get_logger
from src.utils.paths import casebook_dir

logger = get_logger(__name__)

OUTCOME_FILENAME = "outcome.json"


class UnknownEventError(Exception):
    """No casebook exists for this event, so there is nothing to judge."""


class InvalidVerdictError(Exception):
    """The verdict is not one of OUTCOME_VERDICTS."""


def outcome_path(event_id: str):
    return casebook_dir(event_id) / OUTCOME_FILENAME


def record_outcome(event_id: str, verdict: str, verified_by: str,
                   notes: str = "", corrected_action: Optional[str] = None) -> dict:
    """Attach an operator verdict to a completed investigation.

    Raises UnknownEventError if no casebook exists -- recording an outcome for
    an event that was never investigated would silently create a directory and
    pollute the accuracy denominator.
    """
    verdict = (verdict or "").strip().upper()
    if verdict not in OUTCOME_VERDICTS:
        raise InvalidVerdictError(
            f"verdict must be one of {OUTCOME_VERDICTS}, got {verdict!r}"
        )

    # Through CasebookStorage, not the local filesystem. Reading casebook.json
    # off disk meant that under CASEBOOK_STORAGE_BACKEND=s3 -- the backend
    # Phase F added for scale-out -- no casebook was ever found locally, so
    # POST /outcome/{id} returned 404 for every event and the accuracy dataset
    # was permanently empty while looking merely unused (G2).
    storage = get_casebook_storage()
    casebook = storage.load(event_id, filename="casebook.json")
    if not casebook:
        raise UnknownEventError(f"No casebook found for event {event_id}")

    resolution = casebook.get("resolution") or {}
    rejection_data = (casebook.get("packet_status") or {}).get("rejection_data") or {}

    outcome = {
        "event_id": event_id,
        "verdict": verdict,
        "verified_by": verified_by,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "notes": notes,
        # Denormalised so accuracy_report can group without re-reading and
        # re-parsing every casebook, and so the outcome stays interpretable
        # even if the casebook is later pruned.
        "reason_code": rejection_data.get("rejection_code"),
        "enrolment_type": (casebook.get("packet_metadata") or {}).get("update_type"),
        "resolution_source": resolution.get("source"),
        "agent_action": resolution.get("action"),
        "corrected_action": corrected_action,
        # Confidence, so a reliability curve can be built from recorded
        # correctness and SYNTHESIS_CONFIDENCE_THRESHOLD set from evidence
        # rather than intuition (G5, N2).
        "confidence": resolution.get("confidence"),
        "abstained": resolution.get("abstained", False),
        # What the shadowed runbook would have decided. This is what makes
        # "would the runbook have been right?" answerable, and therefore what
        # makes promotion to serve an evidence-based decision (G18).
        "shadow_action": (resolution.get("shadow") or {}).get("action"),
        "shadow_runbook_id": (resolution.get("shadow") or {}).get("runbook_id"),
        "shadow_agreed": (resolution.get("shadow") or {}).get("agreed"),
        # Which prompts produced the verdict being judged (G23).
        "prompt_fingerprint": (resolution.get("provenance") or {}).get("prompt_fingerprint"),
    }

    # The storage layer already writes atomically under a lock (local) or via
    # an atomic PutObject (S3), so the temp-file dance that used to live here
    # is redundant as well as filesystem-only.
    storage.save(event_id, outcome, filename=OUTCOME_FILENAME)

    logger.info("Resolution outcome recorded", event_id=event_id, verdict=verdict,
                resolution_source=outcome["resolution_source"])
    return outcome


def load_outcome(event_id: str) -> Optional[dict]:
    try:
        return get_casebook_storage().load(event_id, filename=OUTCOME_FILENAME)
    except Exception as e:
        logger.warning("Unusable outcome record; ignoring", event_id=event_id,
                       error=f"{type(e).__name__}: {e}")
        return None


def iter_outcomes():
    """Yield every recorded outcome across all casebooks.

    Enumerates through the storage layer rather than walking
    LOCAL_CASESHEETS_DIR, which on the S3 backend is empty -- so this silently
    yielded nothing and `accuracy_report` reported "No outcomes recorded yet"
    no matter how many verdicts existed (G2).
    """
    storage = get_casebook_storage()
    try:
        event_ids = storage.list_events()
    except Exception as e:
        logger.warning("Could not enumerate casebooks",
                       error=f"{type(e).__name__}: {e}")
        return

    for event_id in event_ids:
        try:
            outcome = storage.load(event_id, filename=OUTCOME_FILENAME)
        except Exception as e:
            logger.warning("Skipping unusable outcome record", event_id=event_id,
                           error=f"{type(e).__name__}: {e}")
            continue
        if outcome:
            yield outcome


def summarise(outcomes) -> dict:
    """Aggregate outcomes into accuracy by reason code, type, and source.

    `resolution_source` is grouped on deliberately: comparing agent-generated
    against runbook-served accuracy on the same reason code is the evidence
    needed before a runbook can be trusted to short-circuit the agents (4.2).
    """
    buckets = {}
    for outcome in outcomes:
        source = outcome.get("resolution_source") or "unknown"
        # Collapse runbook:<id>@v<n> to "runbook" so versions aggregate.
        source_kind = "runbook" if source.startswith("runbook:") else source

        key = (
            outcome.get("reason_code") or "unknown",
            outcome.get("enrolment_type") or "unknown",
            source_kind,
        )
        bucket = buckets.setdefault(key, {v: 0 for v in OUTCOME_VERDICTS})
        verdict = outcome.get("verdict")
        if verdict in bucket:
            bucket[verdict] += 1

    rows = []
    for (reason_code, enrolment_type, source), counts in sorted(buckets.items()):
        total = sum(counts.values())
        correct = counts["CORRECT"]
        rows.append({
            "reason_code": reason_code,
            "enrolment_type": enrolment_type,
            "resolution_source": source,
            "total": total,
            **counts,
            # Partial credit is deliberately excluded: a partially correct
            # resolution still needed a human, so it is not an automation win.
            "accuracy": round(correct / total, 4) if total else 0.0,
        })
    return {"rows": rows, "total_outcomes": sum(r["total"] for r in rows)}


def summarise_shadow(outcomes) -> dict:
    """Score shadowed runbooks against the operator's verdict.

    This is the runbook promotion gate (section 4.2, G18).

    `summarise` above can only compare runbooks that are ALREADY serving,
    because a shadowed packet's `resolution_source` is "agent". A runbook
    cannot be cleared to serve until it has been compared, so that comparison
    had no entry point at all. Shadow mode now records what the runbook WOULD
    have decided, and this reconstructs the counterfactual:

      agreed + CORRECT     -- the runbook would have been right
      agreed + INCORRECT   -- both were wrong; the runbook is no worse
      disagreed + CORRECT  -- the agents were right and the runbook was not:
                              the case against promoting it
      disagreed + INCORRECT -- the agents were wrong, so the runbook MIGHT
                              have been right; not counted as a win, because
                              nobody verified the runbook's own answer

    `would_be_correct` is therefore deliberately conservative: it counts only
    agreements confirmed correct by a human.
    """
    buckets = {}
    for outcome in outcomes:
        runbook_id = outcome.get("shadow_runbook_id")
        if not runbook_id:
            continue

        key = (
            outcome.get("reason_code") or "unknown",
            outcome.get("enrolment_type") or "unknown",
            runbook_id,
        )
        bucket = buckets.setdefault(key, {
            "shadowed": 0, "agreed": 0, "disagreed": 0,
            "would_be_correct": 0, "agent_correct": 0, "verdicts": 0,
        })

        bucket["shadowed"] += 1
        agreed = bool(outcome.get("shadow_agreed"))
        bucket["agreed" if agreed else "disagreed"] += 1

        verdict = outcome.get("verdict")
        if verdict in OUTCOME_VERDICTS:
            bucket["verdicts"] += 1
            if verdict == "CORRECT":
                bucket["agent_correct"] += 1
                if agreed:
                    bucket["would_be_correct"] += 1

    rows = []
    for (reason_code, enrolment_type, runbook_id), counts in sorted(buckets.items()):
        verdicts = counts["verdicts"]
        rows.append({
            "reason_code": reason_code,
            "enrolment_type": enrolment_type,
            "runbook_id": runbook_id,
            **counts,
            "agreement_rate": round(counts["agreed"] / counts["shadowed"], 4)
            if counts["shadowed"] else 0.0,
            # Accuracy the runbook would have achieved, over verified packets.
            "would_be_accuracy": round(counts["would_be_correct"] / verdicts, 4)
            if verdicts else 0.0,
            # The agents' accuracy on the same packets, so the two are
            # compared on identical traffic rather than across periods.
            "agent_accuracy": round(counts["agent_correct"] / verdicts, 4)
            if verdicts else 0.0,
        })

    return {"rows": rows, "total_shadowed": sum(r["shadowed"] for r in rows)}
