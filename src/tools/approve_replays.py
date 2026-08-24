#!/usr/bin/env python3
"""Approve replays a human has to sign off on before they fire.

`queue_for_replay` records each nomination through `CasebookStorage` under the
`pending_replays` root, one document per packet id. This walks them.

It used to read a local `pending_replays.jsonl`, which the tool also wrote
locally -- so under CASEBOOK_STORAGE_BACKEND=s3 with more than one replica the
queue was fragmented across pods and an operator saw only whichever slice
their own pod had written. Any such legacy file is still drained here, so
nothing queued before this change is stranded.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.tools.tool_registry import (  # noqa: E402
    PENDING_REPLAY_FILENAME,
    PENDING_REPLAY_ROOT,
)


def _legacy_queue_path() -> Path:
    """The pre-shared-storage local queue, if this pod still has one."""
    return Path(__file__).resolve().parent.parent / "db" / "pending_replays.jsonl"


def _load_legacy(entries: list) -> list:
    """Read any local jsonl queue left over from before shared storage.

    Returned alongside the storage-backed entries so a migration does not
    strand replays an operator already approved queuing.
    """
    path = _legacy_queue_path()
    if not path.exists():
        return entries

    from filelock import FileLock

    print(f"Also draining the legacy local queue at {path}.")
    with FileLock(str(path) + ".lock", timeout=10):
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except ValueError:
                print(f"  Skipping an unparseable legacy line: {stripped[:80]}")
                continue
            record["_legacy_line"] = stripped
            entries.append(record)
    return entries


def _rewrite_legacy(consumed_lines: set) -> None:
    """Drop consumed entries from the legacy file, keeping everything else.

    Re-read fresh rather than reusing the initial read, so anything appended
    by a live investigation during this interactive session survives (1.8).
    """
    path = _legacy_queue_path()
    if not path.exists() or not consumed_lines:
        return

    from filelock import FileLock

    with FileLock(str(path) + ".lock", timeout=10):
        current = path.read_text(encoding="utf-8").splitlines(keepends=True)
        remaining = [ln for ln in current if ln.strip() not in consumed_lines]
        path.write_text("".join(remaining), encoding="utf-8")


def _load_pending() -> list:
    """Every replay awaiting approval, from shared storage and any legacy file."""
    from src.storage.factory import get_scoped_storage

    storage = get_scoped_storage(PENDING_REPLAY_ROOT)
    entries = []

    try:
        keys = storage.list_events()
    except Exception as e:
        print(f"Could not list pending replays: {type(e).__name__}: {e}")
        keys = []

    for key in keys:
        try:
            record = storage.load(key, filename=PENDING_REPLAY_FILENAME)
        except Exception as e:
            print(f"  Skipping {key}: {type(e).__name__}: {e}")
            continue
        if not record or record.get("status") != "pending":
            continue
        record["_key"] = key
        entries.append(record)

    return _load_legacy(entries)


def _resolve(storage, entry: dict, outcome: str, legacy_consumed: set) -> None:
    """Mark one entry done, wherever it came from."""
    if "_legacy_line" in entry:
        legacy_consumed.add(entry["_legacy_line"])
        return

    # Kept rather than deleted, and marked with what happened: an approval
    # that fired a real replay against OIS is an audit record, not scratch.
    record = {k: v for k, v in entry.items() if not k.startswith("_")}
    record["status"] = outcome
    storage.save(entry["_key"], record, filename=PENDING_REPLAY_FILENAME)


def main():
    parser = argparse.ArgumentParser(description="Approve Human-in-the-Loop Replays")
    parser.add_argument("--approve-all", action="store_true",
                        help="Approve all matching replays without prompting")
    parser.add_argument("--filter-category", type=str,
                        help="Only process replays with this category")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    from src.storage.factory import get_scoped_storage

    base_url = os.environ.get("OIS_FEIGN_BASE_URL", "http://10.10.79.62:31261/ois/hold/v1")
    endpoint = f"{base_url}/api/v1/forceReplay"
    ois_api_key = os.environ.get("OIS_API_KEY")
    headers = {"X-API-Key": ois_api_key} if ois_api_key else {}
    if not ois_api_key:
        print("Warning: OIS_API_KEY is not set; replay calls will be sent unauthenticated.")

    replays = _load_pending()
    if not replays:
        print("No pending replays found.")
        return 0

    storage = get_scoped_storage(PENDING_REPLAY_ROOT)
    legacy_consumed = set()

    for index, replay in enumerate(replays):
        payload = replay.get("payload", {})
        packet_id = payload.get("id", "UNKNOWN")
        category = payload.get("category", "") or ""

        if args.filter_category and args.filter_category.lower() != category.lower():
            continue

        print("\n" + "=" * 50)
        print(f"Replay Request {index + 1}/{len(replays)}")
        print(f"Timestamp: {replay.get('timestamp')}")
        print(f"Packet ID: {packet_id}")
        print(f"Category: {category}")
        print(f"Priority: {payload.get('priority')}")
        print("=" * 50)

        if args.approve_all:
            action = "y"
            print("Auto-approving due to --approve-all flag.")
        else:
            action = input("Approve and execute this replay? (y/n/skip): ").strip().lower()

        if action == "y":
            print(f"Firing HTTP POST to {endpoint}...")
            try:
                # A JSON body with an auth header rather than query params --
                # query params land in server access logs (1.8).
                response = requests.post(endpoint, json=payload, headers=headers,
                                         timeout=10)
                response.raise_for_status()
                print(f"Success: {response.text}")
                _resolve(storage, replay, "replayed", legacy_consumed)
            except Exception as e:
                print(f"Failed: {e}")
                print("Keeping in the queue to try again later.")
        elif action == "n":
            print("Discarding request.")
            _resolve(storage, replay, "discarded", legacy_consumed)
        else:
            print("Skipping request.")

    _rewrite_legacy(legacy_consumed)
    print("\nQueue processing complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
