#!/usr/bin/env python3
"""Phase C7 of DLT_PLAN.md section 14 -- release packets whose fix has landed.

C6 withholds a replay when the fix is on `release` but not running yet, and
C7 parks the packet. This is what comes back for it: read what the pods are
running now, and release every parked packet that version satisfies.

Run it after a deploy, or on a schedule -- it is idempotent, and an entry it
releases is marked so it is not released twice.

    python -m src.tools.release_parked_replays --list
    python -m src.tools.release_parked_replays --dry-run
    python -m src.tools.release_parked_replays

**Releasing does not necessarily replay.** It calls `queue_for_replay`, whose
own `ENABLE_AUTO_REPLAY` switch decides what happens next: `false` -- the
default -- puts the packet in `pending_replays` for a human to approve via
`approve_replays.py`. Turning parking on does not, by itself, cause anything
to be sent to OIS.

**One service.** The running version is read for the default Kubernetes app
(`K8S_DEFAULT_APP`), or for `--app` when given. A parked entry records which
repository its fix is in but nothing yet maps that back to a service, so a
deployment with several DLT-producing services should run this once per app.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.dlt import deployed, parked  # noqa: E402


def _when(value) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _print_entries(entries: list) -> None:
    if not entries:
        print("Nothing is parked.")
        return

    print(f"{'CASE':<28} {'REF':<20} {'WAITING FOR':<22} {'PARKED':<17} STATUS")
    print("-" * 100)
    for entry in entries:
        print(f"{(entry.get('case_id') or '-')[:27]:<28} "
              f"{(entry.get('ref_id') or '-')[:19]:<20} "
              f"{(entry.get('required_version') or '-')[:21]:<22} "
              f"{_when(entry.get('parked_at')):<17} "
              f"{entry.get('status') or '-'}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Release DLT replays parked until their fix deployed.")
    parser.add_argument("--list", action="store_true",
                        help="show what is parked and exit")
    parser.add_argument("--all", action="store_true",
                        help="with --list, include released and expired entries")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be released, and change nothing")
    parser.add_argument("--app", default="",
                        help="Kubernetes app to read the running version for")
    parser.add_argument("--namespace", default="")
    parser.add_argument("--version", default="",
                        help="use this running version instead of reading the "
                             "cluster (for a rehearsal, or when kubeconfig is "
                             "not available here)")
    parser.add_argument("--json", action="store_true",
                        help="emit the summary as JSON")
    args = parser.parse_args()

    if args.list:
        _print_entries(parked.list_parked(include_finished=args.all))
        return 0

    if args.version:
        running = (args.version,)
        source = "supplied on the command line"
    else:
        deployed.reset_cache()
        reading = deployed.running_version(args.app or None, args.namespace or None)
        running = reading.versions
        source = f"read from {reading.pods} pod(s)"
        if not reading.ok:
            print(f"Could not read the running version: {reading.reason}")
            print("Nothing released. Pass --version to override.")
            return 1

    summary = parked.release_ready(running, dry_run=args.dry_run)

    if args.json:
        print(json.dumps(summary, indent=2))
        return 0

    print(f"Running version: {summary['running_version']} ({source})")
    if args.dry_run:
        print("DRY RUN -- nothing was released.")
    print(f"  examined  {summary['examined']}")
    print(f"  released  {summary['released']}")
    print(f"  waiting   {summary['waiting']}")
    print(f"  expired   {summary['expired']}")
    if summary["unknown"]:
        print(f"  unknown   {summary['unknown']}  (version could not be compared)")

    if summary["released"] and not args.dry_run:
        from src.utils.env import get_bool_env
        if get_bool_env("ENABLE_AUTO_REPLAY", False):
            print("\nReleased packets were posted to the OIS replay endpoint.")
        else:
            print("\nReleased packets are in `pending_replays`, awaiting a human.")
            print("Approve them with: python -m src.tools.approve_replays")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
