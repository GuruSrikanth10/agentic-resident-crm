#!/usr/bin/env python3
"""
Operator CLI: attach a ground-truth verdict to a completed investigation.

    python3 -m src.tools.record_outcome --event-id EVT-1 --verdict CORRECT
    python3 -m src.tools.record_outcome --event-id EVT-1 --verdict INCORRECT \
        --corrected-action REPLAY --notes "resident had already resubmitted"
    python3 -m src.tools.record_outcome --list-pending

Without these verdicts there is no accuracy figure, so runbooks cannot be
safely promoted and agent regressions are invisible (ENHANCEMENT_PLAN 4.1).
"""
import argparse
import getpass
import sys

from dotenv import load_dotenv

# Before the storage layer and utils.paths resolve their configuration --
# otherwise CASEBOOK_STORAGE_BACKEND and LOCAL_CASESHEETS_DIR from .env are
# invisible here and every verdict is written to, or looked for in, the
# default local directory instead of the configured store.
load_dotenv()

from src.storage.base import OUTCOME_VERDICTS, TERMINAL_STATUSES  # noqa: E402
from src.utils.outcomes import (  # noqa: E402
    InvalidVerdictError,
    UnknownEventError,
    load_outcome,
    record_outcome,
)


def _list_pending():
    """Terminal casebooks with no verdict yet -- the operator's work queue.

    Enumerated through CasebookStorage, not by walking the local filesystem.
    Walking it meant that under CASEBOOK_STORAGE_BACKEND=s3 this printed "No
    casebooks found" no matter how many were waiting, so the operator's queue
    looked empty and the accuracy dataset was never fed -- the same failure
    G2 fixed in `outcomes.iter_outcomes`, left in place here.
    """
    from src.storage.factory import get_casebook_storage

    storage = get_casebook_storage()
    try:
        event_ids = storage.list_events()
    except Exception as e:
        print(f"Could not enumerate casebooks: {type(e).__name__}: {e}")
        return

    if not event_ids:
        print("No casebooks found.")
        return

    pending = []
    for event_id in event_ids:
        try:
            casebook = storage.load(event_id, filename="casebook.json")
        except Exception:
            continue
        if not casebook:
            continue

        status = (casebook.get("packet_status") or {}).get("status")
        if status not in TERMINAL_STATUSES:
            continue

        # The STORAGE KEY, not the casebook's `eid` field. Outcomes are saved
        # under the key (`storage.save(event_id, outcome, ...)`), so looking up
        # an existing verdict by `eid` would miss it wherever the two differ --
        # and would then offer an already-judged packet again.
        if load_outcome(event_id):
            continue

        resolution = casebook.get("resolution") or {}
        pending.append((event_id, status, resolution.get("action"),
                        resolution.get("source")))

    if not pending:
        print("No investigations are awaiting a verdict.")
        return

    print(f"{len(pending)} investigation(s) awaiting a verdict:\n")
    print(f"{'EVENT ID':<40} {'STATUS':<20} {'ACTION':<16} SOURCE")
    for event_id, status, action, source in pending:
        print(f"{event_id:<40} {status:<20} {str(action):<16} {source}")


def main():
    parser = argparse.ArgumentParser(
        description="Record whether a resolution was actually correct."
    )
    parser.add_argument("--event-id", help="Event whose resolution is being judged")
    parser.add_argument("--verdict", choices=OUTCOME_VERDICTS,
                        help="Was the agent's resolution correct?")
    parser.add_argument("--notes", default="", help="Free-text context")
    parser.add_argument("--corrected-action",
                        help="What the action should have been, if INCORRECT")
    parser.add_argument("--verified-by", default=None,
                        help="Defaults to the current OS user")
    parser.add_argument("--list-pending", action="store_true",
                        help="List terminal casebooks with no verdict yet")
    args = parser.parse_args()

    if args.list_pending:
        _list_pending()
        return 0

    if not args.event_id or not args.verdict:
        parser.error("--event-id and --verdict are required (or use --list-pending)")

    try:
        outcome = record_outcome(
            event_id=args.event_id,
            verdict=args.verdict,
            verified_by=args.verified_by or getpass.getuser(),
            notes=args.notes,
            corrected_action=args.corrected_action,
        )
    except UnknownEventError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except InvalidVerdictError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    print(f"Recorded {outcome['verdict']} for {outcome['event_id']} "
          f"(source: {outcome['resolution_source']}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
