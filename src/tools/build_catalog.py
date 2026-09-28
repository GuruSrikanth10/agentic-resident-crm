#!/usr/bin/env python3
"""
Stage 0 -- Offline Template Catalog Builder.

Run this script periodically (or whenever logging changes) to build the
template catalog that powers the pipeline's noise classification.

Usage:
    python -m src.tools.build_catalog --refids-file refids.txt
    python -m src.tools.build_catalog --refids id1 id2 id3
    python -m src.tools.build_catalog --service enu-demographic --refids-file refids.txt

The script fetches logs for each refid, clusters them with Drain3, computes
cross-flow statistics, and outputs the catalog JSON.

With `--service`, the refids must be that service's packets. Their logs are
fetched from that service's apps, clustered in that service's own Drain3
parse tree, classified with its decision vocabulary, and the catalog is
written to `template_catalog.<service>.json`, where the pipeline looks for it
(MULTI_SERVICE_PLAN.md Phase 6). A catalog is read once per process, so the
API picks a new one up on restart. Without `--service`, the catalog is the
one callers with no service use, at `CATALOG_PATH`, as before.
"""
import argparse
import os
import sys

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.log_pipeline import scope as log_scope
from src.log_pipeline.catalog import TemplateCatalog
from src.log_pipeline.config import CATALOG_PATH
from src.log_pipeline.reducer import cluster_logs
from src.log_pipeline.sources import chain as source_chain
from src.log_pipeline.types import FetchContext, TimeWindow
from src.utils import service_registry


def _default_window() -> "TimeWindow":
    """Same look-back the pipeline uses, so the catalog is built from the same
    slice of history the analysis will later see."""
    return TimeWindow(hours=float(os.environ.get("K8S_DEFAULT_SINCE_HOURS", "2")))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build the offline template catalog (Stage 0).")
    parser.add_argument("--refids", nargs="+", help="List of refids to sample.")
    parser.add_argument("--refids-file", type=str, help="Path to a file with one refid per line.")
    parser.add_argument("--service", type=str, default=None,
                        help="Build this registered service's own catalog, from its "
                             "packets' refids.")
    parser.add_argument("--output", type=str, default=None,
                        help="Output catalog JSON path (default: CATALOG_PATH, or "
                             "template_catalog.<service>.json with --service).")
    parser.add_argument("--hours", type=float, default=None,
                        help="Look-back window (defaults to K8S_DEFAULT_SINCE_HOURS).")
    args = parser.parse_args(argv)

    # Collect refids
    refids = []
    if args.refids:
        refids = args.refids
    elif args.refids_file:
        with open(args.refids_file, "r") as f:
            refids = [line.strip() for line in f if line.strip()]
    else:
        print("ERROR: Provide either --refids or --refids-file.")
        parser.print_help()
        sys.exit(1)

    if args.service:
        if not service_registry.load().is_registered(args.service):
            print(f"ERROR: {args.service!r} is not a registered service.")
            sys.exit(1)
        scope = log_scope.for_service(args.service)
        # Always the service's own tree, even where the pipeline still reads
        # the unscoped pair because no catalog of the service's own exists
        # yet: this is the build that creates it.
        state_file = str(log_scope.drain3_state_file_for(args.service))
        output = args.output or str(log_scope.catalog_path_for(args.service))
        print(f"Service: {args.service}; searching {list(scope.apps)}.")
    else:
        scope = log_scope.unscoped()
        state_file = None
        output = args.output or str(CATALOG_PATH)

    print(f"Processing {len(refids)} refids.")

    # Track template presence across flows
    # template_id -> { "template": str, "flows_present": set(), "total_count": int }
    template_stats: dict[str, dict] = {}
    total_flows = len(refids)

    for i, refid in enumerate(refids):
        print(f"\n[{i+1}/{total_flows}] Processing refid: {refid}")
        try:
            # Through the source chain, not the Elasticsearch fetcher: the
            # catalog has to be buildable from whatever source the pipeline
            # will actually read at analysis time. Going straight to
            # `fetcher.fetch_logs` meant K8S_MOCK_LOG_FILE and the Kubernetes
            # source were both invisible here, so an operator with no cluster
            # access -- exactly the person running the mock -- could not build
            # the catalog at all, and without a catalog Stage 4 collapses
            # nothing (see reducer.BOILERPLATE_COUNT_THRESHOLD).
            result = source_chain.fetch_with_fallback(
                refid,
                TimeWindow(hours=args.hours) if args.hours else _default_window(),
                FetchContext(event_id=refid, apps=scope.apps,
                             pod_matches=scope.pod_matches),
            )
            raw_logs = result.records  # No catalog filtering during build
            if not raw_logs:
                print(f"  No logs found for {refid}, skipping.")
                continue

            clusters = cluster_logs(raw_logs, catalog=None, state_file=state_file)

            for cluster in clusters:
                tid = cluster["template_id"]
                template_text = cluster["template"]

                if tid not in template_stats:
                    template_stats[tid] = {
                        "template": template_text,
                        "flows_present": set(),
                        "total_count": 0,
                    }
                template_stats[tid]["flows_present"].add(refid)
                template_stats[tid]["total_count"] += cluster["count"]

        except Exception as e:
            print(f"  ERROR processing {refid}: {e}")
            continue

    # Build catalog with classifications
    catalog = TemplateCatalog(path=output)

    for tid, stats in template_stats.items():
        pct = len(stats["flows_present"]) / total_flows if total_flows > 0 else 0
        template_text = stats["template"]

        # Classification logic
        if scope.decision_vocabulary.search(template_text):
            classification = "decision-marker"
            correlation = "decision"
        elif pct >= 0.90:
            classification = "boilerplate"
            correlation = "none"
        else:
            classification = "informative"
            correlation = "varies"

        catalog.upsert(
            template_id=tid,
            template=template_text,
            classification=classification,
            seen_in_pct_of_flows=pct,
            outcome_correlation=correlation,
        )

    catalog.save()
    print(f"\nDone. Catalog saved to {output} with {catalog.size} templates.")

    # Sanity check: before 0.1's Drain3 cross-flow leak fix, every template
    # ever seen (across every flow, not just the ones sampled here) got fed
    # into this pct calculation, which pushed nearly everything over the
    # boilerplate threshold. A catalog where the vast majority of templates
    # are classified boilerplate is a symptom of that leak recurring (or of
    # a sample that's too homogeneous to be representative), not a healthy
    # catalog -- flag it loudly instead of silently shipping it (1.2).
    if catalog.size > 0:
        boilerplate_count = sum(
            1 for tid in template_stats if catalog.get_classification(tid) == "boilerplate"
        )
        boilerplate_share = boilerplate_count / catalog.size
        print(f"Boilerplate share: {boilerplate_share:.1%} ({boilerplate_count}/{catalog.size})")
        if boilerplate_share > 0.40:
            print(
                f"WARNING: {boilerplate_share:.1%} of templates classified as "
                "boilerplate is implausibly high. Verify the Drain3 state file isn't leaking "
                "templates across flows (see reducer.cluster_logs) before trusting this catalog."
            )


if __name__ == "__main__":
    main()
