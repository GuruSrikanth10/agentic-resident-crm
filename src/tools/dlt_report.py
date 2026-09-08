#!/usr/bin/env python3
"""Phase 9 of DLT_PLAN.md -- read the DLT analysis output.

The goal is two commands from "what is failing most this week" to a specific
stack trace:

    python -m src.tools.dlt_report --top
    python -m src.tools.dlt_report --group <fingerprint-prefix>

Also:
    --case <case_id>    one case in full, with its trace
    --unreviewed        recommendations awaiting human review -- the queue a
                        person will eventually work, and the reason nothing
                        writes `final` in v1
    --stats             corpus-level counts, including the LLM-call reduction

And the replay precheck (DLT_PLAN.md 14):

    --parked            packets waiting for a deploy, and which version each
                        is waiting for
    --code-check        verdict distribution across every group
    --code-check-accuracy
                        did the verdicts turn out to be right? Joins each
                        verdict to whether the packet dead-lettered AGAIN
                        after its replay. This is the evidence bar for
                        turning DLT_CODE_CHECK_GATES_REPLAY on, and without
                        it there is no way to know whether Trap T9's 5% is
                        really 5%.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.dlt import groups, parked  # noqa: E402
from src.dlt.case_storage import get_dlt_storage  # noqa: E402
from src.dlt.reuse import llm_calls_avoided  # noqa: E402

CLASS_LABELS = {
    "A": "business error",
    "B": "code defect",
    "C": "technical/transient",
    "U": "unclassified",
}


def _when(value) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _resolve(prefix: str):
    """Find a group by fingerprint prefix, so nobody types 64 hex characters."""
    matches = [g for g in groups.list_groups()
               if g.get("fingerprint", "").startswith(prefix)]
    if not matches:
        raise SystemExit(f"No group whose fingerprint starts with '{prefix}'.")
    if len(matches) > 1:
        print(f"'{prefix}' matches {len(matches)} groups:")
        for group in matches:
            print(f"  {group['fingerprint'][:16]}  {group.get('signature')}")
        raise SystemExit("Use a longer prefix.")
    return matches[0]


def cmd_top(limit: int) -> None:
    all_groups = groups.list_groups()
    if not all_groups:
        print("No DLT groups recorded yet.")
        return

    ranked = sorted(all_groups, key=lambda g: -(g.get("occurrence_count") or 0))[:limit]

    print(f"{'COUNT':>6}  {'CLS':<3}  {'STATE':<8}  {'LAST SEEN':<17}  SIGNATURE")
    print("-" * 100)
    for group in ranked:
        print(f"{group.get('occurrence_count', 0):>6}  "
              f"{group.get('failure_class', '?'):<3}  "
              f"{group.get('recommendation_state', 'none'):<8}  "
              f"{_when(group.get('last_seen')):<17}  "
              f"{group.get('signature', '')[:60]}")
    print(f"\n{len(all_groups)} distinct failure signatures. "
          f"Inspect one with --group <fingerprint prefix>.")


def cmd_group(prefix: str) -> None:
    group = _resolve(prefix)

    print(f"Fingerprint : {group['fingerprint']}")
    print(f"Signature   : {group.get('signature')}")
    print(f"Class       : {group.get('failure_class')} "
          f"({CLASS_LABELS.get(group.get('failure_class'), 'unknown')})")
    print(f"Code        : {group.get('business_code') or '-'}")
    print(f"Occurrences : {group.get('occurrence_count')}")
    print(f"First seen  : {_when(group.get('first_seen'))}")
    print(f"Last seen   : {_when(group.get('last_seen'))}")

    history = group.get("corroboration_history") or {}
    if history:
        print("\nCorroboration history:")
        for verdict, count in sorted(history.items(), key=lambda kv: -kv[1]):
            print(f"  {count:5d}  {verdict}")
        if history.get("CONTRADICTED"):
            print("  NOTE: some occurrences contradicted the declared exception.")

    check = group.get("code_check")
    if check:
        print("\nReplay precheck (latest):")
        print(f"  verdict  : {check.get('verdict')}")
        print(f"  reason   : {(check.get('reason') or '')[:300]}")
        if check.get("repo"):
            print(f"  file     : {check.get('repo')}:{check.get('path')} "
                  f"@ {check.get('branch')}")
        if check.get("required_version"):
            print(f"  needs    : {check['required_version']}  "
                  f"(running {check.get('running_version') or '?'})")
        history = group.get("code_check_history") or {}
        if history:
            print("  history  : " + ", ".join(
                f"{verdict} x{count}"
                for verdict, count in sorted(history.items(), key=lambda kv: -kv[1])))

    recommendation = group.get("recommendation")
    print(f"\nRecommendation ({group.get('recommendation_state', 'none')}):")
    if recommendation:
        print(f"  action        : {recommendation.get('action')}")
        print(f"  confidence    : {recommendation.get('confidence')}")
        print(f"  narrative     : {(recommendation.get('narrative') or '')[:400]}")
        print(f"  recommendation: {(recommendation.get('recommendation') or '')[:400]}")
        if recommendation.get("discrepancy"):
            print(f"  DISCREPANCY   : {recommendation['discrepancy']}")
    else:
        print("  (none recorded)")

    members = group.get("members") or []
    print(f"\nMembers ({len(members)} retained of {group.get('occurrence_count')}):")
    for case_id in members[-10:]:
        print(f"  {case_id}")
    if members:
        print(f"\nInspect one with --case {members[-1]}")


def cmd_case(case_id: str) -> None:
    storage = get_dlt_storage()
    casebook = storage.load(case_id)
    if not casebook:
        raise SystemExit(f"No casebook for '{case_id}'.")

    print(json.dumps(casebook, indent=2, ensure_ascii=False))

    trace = storage.load_artifact(case_id, "trace.txt")
    if trace:
        print("\n--- trace.txt ---")
        print(trace[:8000])


def cmd_unreviewed() -> None:
    """The queue a human will work. Nothing writes `final` in v1, so every
    recommendation in use is here."""
    pending = [g for g in groups.list_groups()
               if g.get("recommendation_state") == groups.STATE_DRAFT]
    if not pending:
        print("No draft recommendations awaiting review.")
        return

    pending.sort(key=lambda g: -(g.get("occurrence_count") or 0))
    print(f"{len(pending)} draft recommendation(s) awaiting review, "
          f"most-served first:\n")
    for group in pending:
        served = group.get("occurrence_count", 0)
        print(f"  {group['fingerprint'][:16]}  served to {served:>5} case(s)  "
              f"{group.get('signature', '')[:55]}")
    print("\nEach of these is being reused unreviewed. A wrong one is served "
          "to every subsequent occurrence.")


def cmd_parked() -> None:
    """Packets held back because their fix is on `release` but not running."""
    entries = parked.list_parked()
    if not entries:
        print("Nothing is parked.")
        print("\nPackets appear here when the replay precheck finds a fix that "
              "has\nnot deployed yet, and leave when the pods reach its version.")
        return

    print(f"{'CASE':<28} {'REF':<20} {'WAITING FOR':<22} {'PARKED':<17} REPO")
    print("-" * 110)
    for entry in entries:
        print(f"{(entry.get('case_id') or '-')[:27]:<28} "
              f"{(entry.get('ref_id') or '-')[:19]:<20} "
              f"{(entry.get('required_version') or '-')[:21]:<22} "
              f"{_when(entry.get('parked_at')):<17} "
              f"{(entry.get('repo') or '-')[:30]}")

    waiting_for = {}
    for entry in entries:
        version = entry.get("required_version") or "?"
        waiting_for[version] = waiting_for.get(version, 0) + 1

    print(f"\n{len(entries)} packet(s) parked, waiting on:")
    for version, count in sorted(waiting_for.items(), key=lambda kv: -kv[1]):
        print(f"  {count:>5}  {version}")
    print("\nRelease them with: python -m src.tools.release_parked_replays")


def cmd_code_check() -> None:
    """Verdict distribution. The UNKNOWN share is the real coverage number."""
    all_groups = groups.list_groups()
    checked = [g for g in all_groups if g.get("code_check_history")]
    if not checked:
        print("No code-check verdicts recorded yet.")
        print("\nSet DLT_CODE_CHECK_ENABLED=true to start recording them. "
              "Nothing\nabout replay changes until DLT_CODE_CHECK_GATES_REPLAY "
              "is also on.")
        return

    totals = {}
    for group in checked:
        for verdict, count in (group.get("code_check_history") or {}).items():
            totals[verdict] = totals.get(verdict, 0) + count
    overall = sum(totals.values())

    print("Verdicts across every group:")
    for verdict, count in sorted(totals.items(), key=lambda kv: -kv[1]):
        share = f"{100 * count / overall:.1f}%" if overall else "-"
        print(f"  {verdict:<14} {count:>7}  {share:>7}")

    unknown = totals.get("UNKNOWN", 0)
    if overall:
        print(f"\nCoverage: {100 * (overall - unknown) / overall:.0f}% of checks "
              f"reached a verdict.")
        if unknown > overall * 0.5:
            print("  Most checks establish nothing. Usually an unmapped package "
                  "in\n  DLT_REPO_MAP, or no baseline version captured.")

    print(f"\n{'COUNT':>6}  {'LATEST':<14}  SIGNATURE")
    print("-" * 100)
    ranked = sorted(checked, key=lambda g: -(g.get("occurrence_count") or 0))
    for group in ranked[:20]:
        latest = (group.get("code_check") or {}).get("verdict", "-")
        print(f"{group.get('occurrence_count', 0):>6}  {latest:<14}  "
              f"{group.get('signature', '')[:60]}")


def cmd_code_check_accuracy() -> None:
    """Did the verdicts turn out to be right?

    The only outcome this system can observe by itself is whether a packet
    dead-lettered AGAIN after it was replayed. That is exactly the signal
    that matters: a `FIX_DEPLOYED` verdict followed by a recurrence is a false
    positive, and a `NO_CHANGE` verdict followed by a recurrence is the
    verdict being right.

    Counterfactuals are not measurable and are not guessed at. Once
    DLT_CODE_CHECK_GATES_REPLAY is on, a withheld replay produces no outcome
    at all -- which is why this report is worth running BEFORE turning the
    gate on, while replays still fire regardless of the verdict.
    """
    storage = get_dlt_storage()
    try:
        case_ids = storage.list_events()
    except Exception as e:
        raise SystemExit(f"Could not list DLT cases: {e}")

    cases = []
    for case_id in case_ids:
        casebook = storage.load(case_id)
        if casebook:
            cases.append(casebook)

    if not cases:
        print("No DLT casebooks recorded yet.")
        return

    by_ref = {}
    for casebook in cases:
        ref_id = (casebook.get("packet") or {}).get("ref_id")
        if ref_id:
            by_ref.setdefault(ref_id, []).append(casebook)
    for entries in by_ref.values():
        entries.sort(key=lambda c: c.get("detected_at") or 0)

    #: verdict -> [replayed, recurred]
    tally = {}
    examples = []
    for casebook in cases:
        verdict = (casebook.get("code_check") or {}).get("verdict")
        replay = casebook.get("replay") or {}
        if not verdict or not replay.get("queued"):
            continue

        ref_id = (casebook.get("packet") or {}).get("ref_id")
        detected = casebook.get("detected_at") or 0
        recurred = any(
            later.get("detected_at", 0) > detected
            for later in by_ref.get(ref_id, [])
        )

        row = tally.setdefault(verdict, [0, 0])
        row[0] += 1
        row[1] += 1 if recurred else 0
        if recurred and verdict == "FIX_DEPLOYED" and len(examples) < 10:
            examples.append(casebook.get("case_id"))

    if not tally:
        print("No replayed case carries a code-check verdict yet.")
        print("\nThis report needs replays that actually fired. Run with\n"
              "DLT_CODE_CHECK_ENABLED=true and DLT_CODE_CHECK_GATES_REPLAY=false "
              "so\nverdicts are recorded while replays still happen regardless.")
        return

    print("Of the packets that were replayed, how many dead-lettered again?\n")
    print(f"{'VERDICT':<14} {'REPLAYED':>9} {'RECURRED':>9} {'RATE':>8}")
    print("-" * 45)
    for verdict, (replayed, recurred) in sorted(tally.items()):
        rate = f"{100 * recurred / replayed:.0f}%" if replayed else "-"
        print(f"{verdict:<14} {replayed:>9} {recurred:>9} {rate:>8}")

    fixed = tally.get("FIX_DEPLOYED")
    if fixed and fixed[0]:
        false_positive = 100 * fixed[1] / fixed[0]
        print(f"\nFIX_DEPLOYED false-positive rate: {false_positive:.0f}% "
              f"({fixed[1]} of {fixed[0]}).")
        print("This is the number Trap T9 predicts should be near 5%. "
              "It is also\nthe evidence bar for DLT_CODE_CHECK_GATES_REPLAY.")
        if fixed[0] < 30:
            print(f"\n  Only {fixed[0]} sample(s). The deferred Class B replay "
                  f"path asks for\n  at least 30 before it is considered.")
        if examples:
            print("\n  Replayed on FIX_DEPLOYED and dead-lettered again:")
            for case_id in examples:
                print(f"    {case_id}")

    unchanged = tally.get("NO_CHANGE")
    if unchanged and unchanged[0]:
        print(f"\nNO_CHANGE recurrence rate: "
              f"{100 * unchanged[1] / unchanged[0]:.0f}%. A HIGH number here "
              f"means the\nverdict is right -- those replays were always going "
              f"to fail, and the\ngate would have withheld them.")


def cmd_stats() -> None:
    all_groups = groups.list_groups()
    if not all_groups:
        print("No DLT groups recorded yet.")
        return

    messages = sum(g.get("occurrence_count", 0) for g in all_groups)
    by_class = {}
    verdicts = {}
    for group in all_groups:
        cls = group.get("failure_class", "U")
        by_class[cls] = by_class.get(cls, 0) + group.get("occurrence_count", 0)
        for verdict, count in (group.get("corroboration_history") or {}).items():
            verdicts[verdict] = verdicts.get(verdict, 0) + count

    class_a_groups = [g for g in all_groups if g.get("failure_class") == "A"]
    class_a_messages = sum(g.get("occurrence_count", 0) for g in class_a_groups)

    print(f"Cases analysed        : {messages}")
    print(f"Distinct signatures   : {len(all_groups)}")
    print("\nBy class:")
    for cls in ("A", "B", "C", "U"):
        count = by_class.get(cls, 0)
        share = f"{100 * count / messages:.1f}%" if messages else "-"
        print(f"  {cls} {CLASS_LABELS[cls]:<20} {count:>7}  {share:>7}")

    if verdicts:
        print("\nCorroboration verdicts:")
        for verdict, count in sorted(verdicts.items(), key=lambda kv: -kv[1]):
            print(f"  {verdict:<15} {count:>7}")
        contradicted = verdicts.get("CONTRADICTED", 0) + verdicts.get("PARTIAL", 0)
        if contradicted:
            print(f"\n  {contradicted} case(s) where the logs did not support the "
                  f"declared exception.\n  These are the findings a developer "
                  f"cannot get from Kafka UI.")

    if class_a_messages:
        saving = llm_calls_avoided(class_a_messages, len(class_a_groups))
        print(f"\nCost model: {class_a_messages} Class A cases across "
              f"{len(class_a_groups)} signatures.")
        print(f"  Reuse avoided ~{saving * 100:.0f}% of LLM calls.")


def main():
    parser = argparse.ArgumentParser(description="Inspect DLT analysis output")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--top", action="store_true",
                       help="Failure signatures ranked by volume")
    group.add_argument("--group", metavar="FINGERPRINT",
                       help="One signature in detail (prefix is enough)")
    group.add_argument("--case", metavar="CASE_ID", help="One case in full")
    group.add_argument("--unreviewed", action="store_true",
                       help="Draft recommendations awaiting human review")
    group.add_argument("--stats", action="store_true", help="Corpus-level counts")
    group.add_argument("--parked", action="store_true",
                       help="Packets waiting for their fix to deploy")
    group.add_argument("--code-check", action="store_true",
                       dest="code_check",
                       help="Replay-precheck verdict distribution")
    group.add_argument("--code-check-accuracy", action="store_true",
                       dest="code_check_accuracy",
                       help="Did the verdicts turn out to be right?")
    parser.add_argument("--limit", type=int, default=20, help="Rows for --top")
    args = parser.parse_args()

    if args.top:
        cmd_top(args.limit)
    elif args.group:
        cmd_group(args.group)
    elif args.case:
        cmd_case(args.case)
    elif args.unreviewed:
        cmd_unreviewed()
    elif args.parked:
        cmd_parked()
    elif args.code_check:
        cmd_code_check()
    elif args.code_check_accuracy:
        cmd_code_check_accuracy()
    else:
        cmd_stats()


if __name__ == "__main__":
    main()
