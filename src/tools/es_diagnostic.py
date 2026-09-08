#!/usr/bin/env python3
"""
Phase 0 diagnostic for KUBERNETES_LOGS_PLAN.md -- Elasticsearch query analysis.

Answers one question before any Kubernetes work begins: are the "missing" logs
actually missing from Elasticsearch, or is the query in
src/log_pipeline/fetcher.py failing to ask for them?

For each supplied eventId it runs the current production query plus three
variants, and aggregates which application_name values actually logged that id.
If the id appears under services other than the hardcoded "enu-biometric"
filter, the logs were never missing -- they were never requested.

Usage:
    python3 -m src.tools.es_diagnostic --event-ids ID1 ID2 ID3
    python3 -m src.tools.es_diagnostic --event-ids-file ids.txt --json out.json
    python3 -m src.tools.es_diagnostic --event-ids ID1 --dry-run

Exit codes:
    0  diagnostic completed
    1  could not run (no ES_HOST, unreachable, auth failure)
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.env import get_bool_env  # noqa: E402
from src.log_pipeline.fetcher import _app_names  # noqa: E402

# The app names fetcher.py filters on. Read through the same helper the
# fetcher uses so this diagnostic can't drift from production when
# ES_APP_NAMES is set (F19) -- variant A being byte-identical to the real
# query is the entire point of this tool.
PRODUCTION_APP_FILTERS = _app_names()

# The primary app name, as a plain string, for report headings and for
# aggregating which service actually logged an id.
PRODUCTION_APP_FILTER = PRODUCTION_APP_FILTERS[0] if PRODUCTION_APP_FILTERS else ""


# ======================================================================
# Query variants -- variant A must stay byte-identical to fetcher.py
# ======================================================================

def build_variants(event_id: str, boilerplate_phrases: list[str]) -> dict:
    """Return {variant_name: (description, es_query)}.

    Variant A mirrors src/log_pipeline/fetcher.py exactly, including the
    catalog-driven must_not clauses. Every other variant changes exactly one
    thing relative to A, so a difference in hit count isolates one cause.
    """
    must_not = [{"match_phrase": {"message": p}} for p in boilerplate_phrases]

    def _with_must_not(query: dict) -> dict:
        if must_not:
            query["bool"]["must_not"] = must_not
        return query

    variant_a = _with_must_not({
        "bool": {
            "must": [{"query_string": {"query": f'"{event_id}"'}}],
            "filter": [{"terms": {"application_name.keyword": PRODUCTION_APP_FILTERS}}],
        }
    })

    variant_b = _with_must_not({
        "bool": {
            "must": [{"query_string": {"query": f'"{event_id}"'}}],
        }
    })

    variant_c = {
        "bool": {
            "must": [{"multi_match": {"query": event_id, "fields": ["*"]}}],
            "filter": [{"terms": {"application_name.keyword": PRODUCTION_APP_FILTERS}}],
        }
    }

    variant_d = {
        "bool": {
            "must": [{"multi_match": {"query": event_id, "fields": ["*"]}}],
        }
    }

    return {
        "A": ("production query (app filter + quoted query_string + must_not)", variant_a),
        "B": ("A minus the application_name filter", variant_b),
        "C": ("A with multi_match instead of quoted query_string", variant_c),
        "D": ("no app filter AND multi_match (widest)", variant_d),
    }


def build_service_aggregation(event_id: str) -> tuple[dict, dict]:
    """Query + aggs that reveal which services actually logged this event id."""
    query = {
        "bool": {"must": [{"multi_match": {"query": event_id, "fields": ["*"]}}]}
    }
    aggs = {
        "by_service": {
            "terms": {"field": "application_name.keyword", "size": 50}
        }
    }
    return query, aggs


# ======================================================================
# Execution
# ======================================================================

def build_client():
    es_host = os.environ.get("ES_HOST")
    if not es_host:
        print("ERROR: ES_HOST is not set. Cannot run the diagnostic.", file=sys.stderr)
        return None, None

    from elasticsearch import Elasticsearch

    auth_args = {}
    es_user = os.environ.get("ES_USERNAME")
    es_pass = os.environ.get("ES_PASSWORD")
    if es_user and es_pass:
        auth_args["basic_auth"] = (es_user, es_pass)

    client = Elasticsearch(
        es_host,
        verify_certs=get_bool_env("ES_VERIFY_CERTS", True),
        request_timeout=float(os.environ.get("ES_REQUEST_TIMEOUT_SECONDS", "30")),
        **auth_args,
    )
    index_pattern = os.environ.get("ES_INDEX_PATTERN", "logs-*")
    return client, index_pattern


def load_boilerplate_phrases() -> list[str]:
    """Read the catalog the production fetcher would use, if one exists."""
    try:
        from src.log_pipeline.catalog import TemplateCatalog
        return TemplateCatalog().get_boilerplate_phrases()
    except Exception as e:
        print(f"  (could not load template catalog: {e})")
        return []


def diagnose_event(client, index_pattern, event_id, boilerplate) -> dict:
    variants = build_variants(event_id, boilerplate)
    counts = {}

    for name, (_desc, query) in variants.items():
        try:
            resp = client.count(index=index_pattern, query=query)
            counts[name] = int(resp["count"])
        except Exception as e:
            counts[name] = f"ERROR: {type(e).__name__}: {e}"

    services = {}
    try:
        query, aggs = build_service_aggregation(event_id)
        resp = client.search(index=index_pattern, query=query, aggs=aggs, size=0)
        for bucket in resp["aggregations"]["by_service"]["buckets"]:
            services[bucket["key"]] = bucket["doc_count"]
    except Exception as e:
        services = {"ERROR": f"{type(e).__name__}: {e}"}

    return {"event_id": event_id, "counts": counts, "services": services}


# ======================================================================
# Verdict
# ======================================================================

def verdict_for_event(result: dict) -> str:
    counts = result["counts"]
    if any(isinstance(v, str) for v in counts.values()):
        return "INCONCLUSIVE (a query errored)"

    a, b, c, d = counts["A"], counts["B"], counts["C"], counts["D"]
    services = {k: v for k, v in result["services"].items() if k != "ERROR"}
    other_services = [s for s in services if s != PRODUCTION_APP_FILTER]

    if d == 0:
        return "ES HAS NOTHING for this id -- genuine ingestion loss or wrong index"
    if a == 0 and b > 0:
        return "APP FILTER IS THE CULPRIT -- logs exist but are filtered out"
    if a == 0 and c > 0:
        return "QUERY SYNTAX IS THE CULPRIT -- quoted query_string does not match"
    if b > a and other_services:
        return f"APP FILTER DROPS EVIDENCE from: {', '.join(other_services)}"
    if d > a:
        return "WIDER QUERY FINDS MORE -- both filter and syntax contribute"
    return "QUERY IS FINE -- production query returns everything ES has"


def overall_recommendation(results: list[dict]) -> str:
    verdicts = [verdict_for_event(r) for r in results]
    filter_issue = sum(1 for v in verdicts if "APP FILTER" in v)
    syntax_issue = sum(1 for v in verdicts if "QUERY SYNTAX" in v)
    nothing = sum(1 for v in verdicts if "ES HAS NOTHING" in v)
    fine = sum(1 for v in verdicts if "QUERY IS FINE" in v)
    total = len(verdicts)

    lines = [
        f"  app-filter problems : {filter_issue}/{total}",
        f"  query-syntax issues : {syntax_issue}/{total}",
        f"  ES genuinely empty  : {nothing}/{total}",
        f"  query already fine  : {fine}/{total}",
        "",
    ]

    if filter_issue + syntax_issue > total / 2:
        lines.append("  GATE: STOP. Fix the Elasticsearch query first.")
        lines.append("  Most 'missing' logs are recoverable without any Kubernetes work.")
        lines.append("  Re-run this diagnostic after the fix before reconsidering Phase 1.")
    elif nothing > total / 2:
        lines.append("  GATE: PROCEED to Phase 1.")
        lines.append("  ES genuinely lacks these logs; a Kubernetes source is justified.")
        lines.append("  Record kubelet retention vs investigation lag (steps 3-4) to choose")
        lines.append("  the chain order and whether snapshot-first is mandatory.")
    else:
        lines.append("  GATE: MIXED RESULT -- widen the sample before deciding.")
        lines.append("  Re-run with more event ids, ideally spanning several reason codes.")
    return "\n".join(lines)


# ======================================================================
# Rendering
# ======================================================================

def print_report(results: list[dict], variants_desc: dict):
    print("\n" + "=" * 78)
    print("PHASE 0 DIAGNOSTIC -- ELASTICSEARCH QUERY ANALYSIS")
    print("=" * 78)

    print("\nQuery variants:")
    for name, desc in variants_desc.items():
        print(f"  {name}: {desc}")

    print("\n" + "-" * 78)
    print(f"{'eventId':<40} {'A':>7} {'B':>7} {'C':>7} {'D':>7}")
    print("-" * 78)
    for r in results:
        c = r["counts"]
        cells = [str(c[k]) if not isinstance(c[k], str) else "ERR" for k in ("A", "B", "C", "D")]
        print(f"{r['event_id'][:40]:<40} {cells[0]:>7} {cells[1]:>7} {cells[2]:>7} {cells[3]:>7}")
    print("-" * 78)

    print("\nPer-event verdict:")
    for r in results:
        print(f"  {r['event_id'][:40]}")
        print(f"    -> {verdict_for_event(r)}")
        services = {k: v for k, v in r["services"].items() if k != "ERROR"}
        if services:
            rendered = ", ".join(f"{k}={v}" for k, v in sorted(services.items(), key=lambda x: -x[1]))
            print(f"    services logging this id: {rendered}")
            others = [s for s in services if s != PRODUCTION_APP_FILTER]
            if others:
                print(f"    NOTE: {len(others)} service(s) excluded by the hardcoded filter")

    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(overall_recommendation(results))

    print("\n" + "=" * 78)
    print("REMAINING MANUAL STEPS (require cluster access)")
    print("=" * 78)
    print("""
  Step 3 -- compare against pod logs for the same ids:

    kubectl -n <NAMESPACE> get pods -l app=<APP> --show-labels
    kubectl -n <NAMESPACE> logs <POD> --since=2h --timestamps | grep -c '<EVENT_ID>'

  Step 4a -- node log rotation settings (answers Open Question 3):

    kubectl get --raw "/api/v1/nodes/<NODE>/proxy/configz" \\
      | python3 -m json.tool | grep -i containerLog

  Step 4b -- investigation lag (answers Open Question 5):

    Compare packet_metadata.created_at against the casebook file mtime
    across recent casebooks in local_casesheets/.

  Record all findings in KUBERNETES_LOGS_PLAN.md section 12 (Phase 0 findings),
  then resolve the decision gate in section 2.
""")


def main():
    parser = argparse.ArgumentParser(
        description="Phase 0: determine whether missing logs are an ES query or ingestion problem."
    )
    parser.add_argument("--event-ids", nargs="+", help="Event ids to diagnose.")
    parser.add_argument("--event-ids-file", type=str, help="File with one event id per line.")
    parser.add_argument("--index", type=str, help="Override ES_INDEX_PATTERN.")
    parser.add_argument("--json", type=str, help="Write raw results to this JSON path.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the queries without contacting Elasticsearch.")
    args = parser.parse_args()

    event_ids = list(args.event_ids or [])
    if args.event_ids_file:
        with open(args.event_ids_file, "r", encoding="utf-8") as f:
            event_ids.extend(line.strip() for line in f if line.strip())

    if not event_ids:
        print("ERROR: provide --event-ids or --event-ids-file.", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    if len(event_ids) < 5:
        print(f"NOTE: the plan calls for 5 event ids; {len(event_ids)} supplied. "
              "A small sample weakens the decision gate.\n")

    if args.dry_run:
        boilerplate = load_boilerplate_phrases()
        print(f"Catalog boilerplate phrases in must_not: {len(boilerplate)}\n")
        for event_id in event_ids:
            print("=" * 78)
            print(f"eventId: {event_id}")
            print("=" * 78)
            for name, (desc, query) in build_variants(event_id, boilerplate).items():
                print(f"\n--- Variant {name}: {desc}")
                print(json.dumps(query, indent=2))
            q, aggs = build_service_aggregation(event_id)
            print("\n--- Service aggregation")
            print(json.dumps({"query": q, "aggs": aggs}, indent=2))
        sys.exit(0)

    client, index_pattern = build_client()
    if client is None:
        sys.exit(1)
    if args.index:
        index_pattern = args.index

    try:
        if not client.ping():
            print(f"ERROR: could not reach Elasticsearch at {os.environ.get('ES_HOST')}.",
                  file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: could not reach Elasticsearch: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Connected. Index pattern: {index_pattern}")
    boilerplate = load_boilerplate_phrases()
    print(f"Catalog boilerplate phrases in must_not: {len(boilerplate)}")

    results = []
    for event_id in event_ids:
        print(f"  querying {event_id} ...")
        results.append(diagnose_event(client, index_pattern, event_id, boilerplate))

    variants_desc = {k: v[0] for k, v in build_variants("SAMPLE", boilerplate).items()}
    print_report(results, variants_desc)

    if args.json:
        payload = {
            "index_pattern": index_pattern,
            "production_app_filter": PRODUCTION_APP_FILTER,
            "boilerplate_phrase_count": len(boilerplate),
            "results": results,
            "verdicts": {r["event_id"]: verdict_for_event(r) for r in results},
        }
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nRaw results written to {args.json}")


if __name__ == "__main__":
    main()
