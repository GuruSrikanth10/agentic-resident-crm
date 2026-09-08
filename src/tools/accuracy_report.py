#!/usr/bin/env python3
"""
Operator CLI: resolution accuracy by reason code, enrolment type, and source.

    python3 -m src.tools.accuracy_report
    python3 -m src.tools.accuracy_report --json
    python3 -m src.tools.accuracy_report --min-samples 10

This is the figure that gates everything in Phase E: a runbook should not be
promoted to `serve` for a reason code until its accuracy matches the agents'
on that same code (ENHANCEMENT_PLAN 4.1, 4.2).
"""
import argparse
import json
import sys

from dotenv import load_dotenv

# Before the storage layer resolves its backend: without this,
# CASEBOOK_STORAGE_BACKEND=s3 in .env was never seen, so this read the local
# filesystem and reported "No outcomes recorded yet" against a populated
# bucket -- the same G2 symptom iter_outcomes() was rewritten to fix.
load_dotenv()

from src.utils.outcomes import iter_outcomes, summarise, summarise_shadow  # noqa: E402


def _shadow_report(args) -> int:
    """The runbook promotion gate (section 4.2, G18).

    Answers, per runbook: how often did it agree with the agents, and -- on
    the packets a human actually verified -- what accuracy would it have
    achieved? Promote a reason code to RUNBOOK_SERVE_ALLOWLIST once this shows
    enough verified samples and no accuracy regression against the agents.
    """
    summary = summarise_shadow(iter_outcomes())
    rows = [r for r in summary["rows"] if r["shadowed"] >= args.min_samples]
    if args.reason_code:
        rows = [r for r in rows if r["reason_code"] == args.reason_code]

    if args.json:
        print(json.dumps({"rows": rows,
                          "total_shadowed": summary["total_shadowed"]}, indent=2))
        return 0

    if not rows:
        print("No shadowed runbook results recorded yet.")
        print("Run with RUNBOOK_MODE=shadow, then record verdicts with:")
        print("  python3 -m src.tools.record_outcome --list-pending")
        return 0

    print(f"{'RUNBOOK':<34} {'TYPE':<6} {'SHADOWED':>8} {'AGREE':>7} "
          f"{'VERIFIED':>8} {'RUNBOOK':>8} {'AGENT':>7}")
    print("-" * 88)
    for row in rows:
        print(
            f"{row['runbook_id'][:33]:<34} "
            f"{str(row['enrolment_type'])[:5]:<6} "
            f"{row['shadowed']:>8} "
            f"{row['agreement_rate']:>7.1%} "
            f"{row['verdicts']:>8} "
            f"{row['would_be_accuracy']:>8.1%} "
            f"{row['agent_accuracy']:>7.1%}"
        )
    print("-" * 88)
    print(f"{summary['total_shadowed']} shadowed resolution(s).")

    # The recommendation, stated explicitly rather than left to the reader.
    print("\nPromotion readiness:")
    for row in rows:
        thin = row["verdicts"] < args.min_verdicts
        regressed = (row["verdicts"] > 0
                     and row["would_be_accuracy"] < row["agent_accuracy"])

        # A regression is reported whenever it is observed, even on thin
        # evidence: "not enough samples yet" would understate a runbook that
        # disagreed with the agents every time and was wrong every time.
        if regressed:
            verdict = (f"DO NOT PROMOTE: would be {row['would_be_accuracy']:.1%} "
                       f"against the agents' {row['agent_accuracy']:.1%}")
            if thin:
                verdict += f" (on only {row['verdicts']} verified outcome(s))"
        elif thin:
            verdict = (f"NOT READY: only {row['verdicts']} verified outcome(s), "
                       f"need {args.min_verdicts}")
        else:
            verdict = (f"READY: add {row['reason_code']} to "
                       f"RUNBOOK_SERVE_ALLOWLIST")
        print(f"  {row['runbook_id']}: {verdict}")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Report resolution accuracy.")
    parser.add_argument("--json", action="store_true", help="Emit JSON")
    parser.add_argument("--min-samples", type=int, default=1,
                        help="Hide rows with fewer outcomes than this")
    parser.add_argument("--reason-code", help="Filter to one reason code")
    parser.add_argument("--shadow", action="store_true",
                        help="Report shadowed runbooks: agreement rate and the "
                             "accuracy each would have achieved (promotion gate)")
    parser.add_argument("--min-verdicts", type=int, default=20,
                        help="Verified outcomes required before a runbook is "
                             "called ready to serve (--shadow only)")
    args = parser.parse_args()

    if args.shadow:
        return _shadow_report(args)

    summary = summarise(iter_outcomes())
    rows = [r for r in summary["rows"] if r["total"] >= args.min_samples]
    if args.reason_code:
        rows = [r for r in rows if r["reason_code"] == args.reason_code]

    if args.json:
        print(json.dumps({"rows": rows, "total_outcomes": summary["total_outcomes"]},
                         indent=2))
        return 0

    if not rows:
        print("No outcomes recorded yet.")
        print("Record some with: python3 -m src.tools.record_outcome --list-pending")
        return 0

    print(f"{'REASON CODE':<34} {'TYPE':<8} {'SOURCE':<10} "
          f"{'N':>5} {'OK':>5} {'BAD':>5} {'PART':>5} {'ACC':>7}")
    print("-" * 88)
    for row in rows:
        print(
            f"{row['reason_code'][:33]:<34} "
            f"{str(row['enrolment_type'])[:7]:<8} "
            f"{row['resolution_source'][:9]:<10} "
            f"{row['total']:>5} {row['CORRECT']:>5} {row['INCORRECT']:>5} "
            f"{row['PARTIAL']:>5} {row['accuracy']:>7.1%}"
        )
    print("-" * 88)
    print(f"{summary['total_outcomes']} outcome(s) recorded.")

    # Where the same reason code has both agent and runbook results, show them
    # side by side -- that comparison is the runbook promotion gate.
    by_code = {}
    for row in rows:
        by_code.setdefault((row["reason_code"], row["enrolment_type"]), {})[
            row["resolution_source"]] = row
    comparisons = {k: v for k, v in by_code.items()
                   if "agent" in v and "runbook" in v}
    if comparisons:
        print("\nAgent vs runbook on the same reason code:")
        for (code, etype), sources in sorted(comparisons.items()):
            agent, runbook = sources["agent"], sources["runbook"]
            delta = runbook["accuracy"] - agent["accuracy"]
            verdict = "runbook OK" if delta >= 0 else "runbook WORSE"
            print(f"  {code} ({etype}): agent {agent['accuracy']:.1%} "
                  f"(n={agent['total']}) vs runbook {runbook['accuracy']:.1%} "
                  f"(n={runbook['total']})  -> {verdict}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
