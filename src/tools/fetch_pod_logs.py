#!/usr/bin/env python3
"""
Phase 5 of KUBERNETES_LOGS_PLAN.md -- operator CLI and de-risking gate.

Pulls pod logs for an identifier and reports what was found, what was
missing, and why. Deliberately standalone: it touches nothing in the packet
path, so it can be run against production without risk.

This is where the design's assumptions meet a real cluster for the first time.
It exists to answer, from real output rather than guesswork:

  1. Do the services log `eventId`, `refId`, or something else?
  2. Is the service one deployment, or does it vary by flow stage?
  3. What is the real kubelet retention -- how far back can we actually see?

Usage:
    python3 -m src.tools.fetch_pod_logs --identifier EVT-123
    python3 -m src.tools.fetch_pod_logs --identifier EVT-123 REF-456 --since-hours 6
    python3 -m src.tools.fetch_pod_logs --list-pods --namespace enu
    python3 -m src.tools.fetch_pod_logs --identifier EVT-123 --no-filter --output trace.txt

Exit codes:
    0  logs found
    1  could not look (no cluster config, RBAC denied, namespace missing)
    2  looked successfully, found nothing
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.log_pipeline.sources.k8s import client as k8s_client  # noqa: E402
from src.log_pipeline.sources.k8s import discovery, gaps, retrieval  # noqa: E402
from src.log_pipeline.sources.k8s.filtering import (  # noqa: E402
    ContextWindowSelector,
    KeepAllSelector,
    build_matcher,
    context_lines_after,
    context_lines_before,
    resolve_search_values,
)
from src.log_pipeline.types import TimeWindow  # noqa: E402

SEPARATOR = "=" * 78


def _print_header(title):
    print("\n" + SEPARATOR)
    print(title)
    print(SEPARATOR)


def _describe_connection():
    if k8s_client.is_available():
        return "connected"
    return f"UNAVAILABLE -- {k8s_client.unavailable_reason()}"


def _run_discovery(args):
    result = discovery.discover_targets(namespace=args.namespace, app=args.app)

    _print_header("DISCOVERY")
    namespace, match_spec = discovery.resolve_service(app=args.app, namespace=args.namespace)
    print(f"  namespace       : {namespace}")
    print(f"  pod match       : {match_spec.describe()}")
    print(f"  cluster         : {_describe_connection()}")

    if not result.ok:
        print(f"  FAILED          : {result.reason}")
        return result

    print(f"  pods matched    : {result.pods_seen}")
    print(f"  skipped Pending : {result.pods_skipped_pending}")
    print(f"  targets         : {len(result.targets)}")
    if result.truncated:
        print("  NOTE            : fan-out was capped (K8S_MAX_PODS)")

    if result.targets:
        print("\n  pod / container (restarts, phase, started):")
        for target in result.targets:
            started = target.start_time.isoformat() if target.start_time else "unknown"
            print(f"    {target.pod_name} / {target.container} "
                  f"(restarts={target.restart_count}, {target.phase}, started={started})")
    return result


def _build_selector(args):
    if args.no_filter:
        return KeepAllSelector()
    values = resolve_search_values(
        args.identifier[0] if args.identifier else "",
        args.identifier[1:] if args.identifier else [],
    )
    if not values:
        return KeepAllSelector()
    return ContextWindowSelector(
        build_matcher(values),
        before=context_lines_before(),
        after=context_lines_after(),
    )


def _report_per_pod(targets, window, args):
    """Read each target separately so per-pod match counts are visible.

    Aggregate counts would hide the case where one replica holds every
    matching line and the others hold none -- which is exactly what tells an
    operator whether the label selector is too wide.
    """
    rows, all_records, all_gaps = [], [], []
    combined_stats = retrieval.ParseStats()
    oldest_observed = None
    earliest_pod_start = None

    for target in targets:
        outcome = retrieval.read_pod_logs(target, window, selector=_build_selector(args))
        rows.append((target, outcome))
        all_records.extend(outcome.records)
        all_gaps.extend(outcome.gaps)
        combined_stats.total += outcome.stats.total
        combined_stats.level_parsed += outcome.stats.level_parsed
        combined_stats.json_lines += outcome.stats.json_lines

        oldest_observed = retrieval._older(oldest_observed, outcome.oldest_line_timestamp)
        if target.start_time is not None and (
            earliest_pod_start is None or target.start_time < earliest_pod_start
        ):
            earliest_pod_start = target.start_time

    return rows, all_records, all_gaps, combined_stats, oldest_observed, earliest_pod_start


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and inspect Kubernetes pod logs for an identifier."
    )
    parser.add_argument("--identifier", nargs="+", default=[],
                        help="Identifier(s) to search for, e.g. an eventId and a refId.")
    parser.add_argument("--namespace", type=str, help="Override K8S_DEFAULT_NAMESPACE.")
    parser.add_argument("--app", type=str, help="Override K8S_DEFAULT_APP.")
    parser.add_argument("--since-hours", type=float, default=None,
                        help="Look-back window in hours (default K8S_DEFAULT_SINCE_HOURS).")
    parser.add_argument("--no-filter", action="store_true",
                        help="Do not filter by identifier -- dump everything in the window. "
                             "Use this to discover which identifier the service logs.")
    parser.add_argument("--list-pods", action="store_true",
                        help="Only run discovery; do not read any logs.")
    parser.add_argument("--output", type=str, help="Write the matched trace to this path.")
    parser.add_argument("--json", type=str, help="Write structured results to this JSON path.")
    parser.add_argument("--max-print", type=int, default=50,
                        help="Lines to print to stdout (default 50; 0 for all).")
    args = parser.parse_args()

    if not args.identifier and not args.no_filter and not args.list_pods:
        print("ERROR: provide --identifier, or --no-filter to dump everything.",
              file=sys.stderr)
        parser.print_help()
        return 1

    result = _run_discovery(args)
    if not result.ok:
        print("\nCould not look. Fix the configuration above and retry.", file=sys.stderr)
        return 1

    if args.list_pods:
        return 0 if result.targets else 2

    if not result.targets:
        print("\nNo pods matched. Check the namespace and pod match spec.")
        return 2

    import os
    hours = args.since_hours
    if hours is None:
        hours = float(os.environ.get("K8S_DEFAULT_SINCE_HOURS", "2"))
    window = TimeWindow(hours=hours)

    (rows, records, pod_gaps, stats,
     oldest_observed, earliest_pod_start) = _report_per_pod(result.targets, window, args)

    _print_header("PER-POD RESULTS")
    print(f"  window          : last {hours}h")
    print(f"  filtering       : {'disabled (--no-filter)' if args.no_filter else ', '.join(args.identifier)}")
    print()
    for target, outcome in rows:
        status = "ok" if outcome.ok else f"FAILED ({outcome.error})"
        print(f"    {target.pod_name}/{target.container}: "
              f"{len(outcome.records)} lines, {outcome.bytes_read} bytes [{status}]")

    # Gaps: discovery gaps + per-pod gaps + the window-level detections.
    detected = list(result.gaps) + list(pod_gaps)
    rotation = gaps.detect_rotation_gap(oldest_observed, window, earliest_pod_start)
    if rotation:
        detected.append(rotation)
    detected.extend(gaps.detect_pod_replaced_gaps(result.targets, window))
    degradation = gaps.detect_parse_degradation_gap(stats)
    if degradation:
        detected.append(degradation)
    detected = gaps.dedupe_gaps(detected)

    _print_header("EVIDENCE GAPS")
    if detected:
        print(gaps.render_banner(detected))
    else:
        print("  none -- the requested window appears fully covered.")

    _print_header("PARSE QUALITY")
    print(f"  lines parsed    : {stats.total}")
    print(f"  level recovered : {stats.level_parsed} "
          f"({(1 - stats.level_failure_rate):.0%})")
    print(f"  JSON-formatted  : {stats.json_lines} ({stats.json_ratio:.0%})")
    if stats.total and stats.level_failure_rate > 0.5:
        print("  WARNING: most lines had no recognisable level. ERROR detection "
              "would be unreliable for this service -- extend the parser.")

    _print_header(f"TRACE ({len(records)} lines)")
    limit = len(records) if args.max_print == 0 else args.max_print
    for record in records[:limit]:
        print(f"  [{record.get('timestamp','')}] [{record.get('pod_name','')}] "
              f"[{record.get('level','')}] {record.get('message','')}")
    if len(records) > limit:
        print(f"  ... {len(records) - limit} more (use --max-print 0 or --output)")

    if args.output:
        banner = gaps.render_banner(detected)
        lines = [banner] if banner else []
        lines += [
            f"[{r.get('timestamp','')}] [{r.get('pod_name','')}] "
            f"[{r.get('level','')}] {r.get('message','')}"
            for r in records
        ]
        Path(args.output).write_text("\n".join(lines), encoding="utf-8")
        print(f"\nTrace written to {args.output}")

    if args.json:
        payload = {
            "namespace": result.targets[0].namespace if result.targets else None,
            "identifiers": args.identifier,
            "window_hours": hours,
            "pods": [
                {
                    "pod_name": t.pod_name,
                    "container": t.container,
                    "phase": t.phase,
                    "restart_count": t.restart_count,
                    "matched_lines": len(o.records),
                    "bytes_read": o.bytes_read,
                    "ok": o.ok,
                }
                for t, o in rows
            ],
            "gaps": [{"type": g.gap_type.value, "detail": g.detail} for g in detected],
            "parse_stats": {
                "total": stats.total,
                "level_parsed": stats.level_parsed,
                "json_lines": stats.json_lines,
            },
            "records": records,
        }
        Path(args.json).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Structured results written to {args.json}")

    _print_header("NEXT STEPS")
    print("""
  Record in KUBERNETES_LOGS_PLAN.md section 12:
    - which identifier actually matched (Open Question 1)
    - the namespace/selector that worked (Open Question 2)
    - the oldest line available vs the window requested (Open Question 3)

  If nothing matched but pods were found, re-run with --no-filter and grep the
  output by hand -- the service may log a different identifier entirely.
""")

    return 0 if records else 2


if __name__ == "__main__":
    sys.exit(main())
