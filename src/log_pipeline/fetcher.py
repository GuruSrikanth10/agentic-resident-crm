"""
Stage 1 -- Elasticsearch Fetch Layer.

Improvements over the old fetch_elastic_logs:
  - Source-filtered to only @timestamp, level, message (cuts ES payload).
  - search_after with _seq_no tiebreaker for stable pagination.
  - Optional must_not filter driven by catalog boilerplate phrases.
  - Returns structured dicts, not raw formatted strings.
"""
import os
import json
import threading
from typing import Optional

from src.log_pipeline.catalog import TemplateCatalog
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Hard cap on total documents pulled per event_id -- without this, a noisy
# event can pull an unbounded number of rows into memory (1.10). Kept as a
# module constant for backwards compatibility; `_max_documents()` below is the
# live-reading form callers should use.
LOG_MAX_DOCUMENTS = int(os.environ.get("LOG_MAX_DOCUMENTS", "50000"))


def _max_documents() -> int:
    """Read at call time, like every other tunable in this subsystem.

    The comment above promised this function existed; it did not, and line
    `max_documents = LOG_MAX_DOCUMENTS` read the import-time constant instead.
    So the one knob governing memory footprint on a noisy event was the only
    one that could not be changed without a restart, or monkeypatched in a
    test (G14).
    """
    try:
        return int(os.environ.get("LOG_MAX_DOCUMENTS", str(LOG_MAX_DOCUMENTS)))
    except ValueError:
        return LOG_MAX_DOCUMENTS


def _app_names() -> list:
    """Which application_name values to fetch.

    Was hardcoded to a single value, so only one service's logs were ever
    retrievable regardless of which stage the packet actually failed in
    (F19). An empty value means "don't filter by app at all".
    """
    raw = os.environ.get("ES_APP_NAMES", "enu-biometric").strip()
    return [name.strip() for name in raw.split(",") if name.strip()]


_es_client = None
_es_client_key = None
#: The class the cached client was built from, held so it can be compared by
#: identity. NOT folded into the key tuple: `id()` is reused after GC (which
#: silently returned one test's mock client to another), and `==` on a
#: MagicMock returns a truthy mock rather than a bool.
_es_client_class = None
_es_client_lock = threading.Lock()


def _get_es_client(es_host: str, auth_args: dict):
    """Return a cached Elasticsearch client.

    A fresh client per fetch means a new connection pool and TLS handshake for
    every packet (F16). Keyed on the connection settings so a config change
    still takes effect -- and on the Elasticsearch class object itself, so a
    test that patches the constructor gets a real construction rather than a
    client cached from a previous test.
    """
    global _es_client, _es_client_key, _es_client_class

    import elasticsearch

    es_class = elasticsearch.Elasticsearch
    verify_certs = get_bool_env("ES_VERIFY_CERTS", True)
    request_timeout = float(os.environ.get("ES_REQUEST_TIMEOUT_SECONDS", "30"))
    key = (es_host, verify_certs, request_timeout, tuple(sorted(auth_args)))

    with _es_client_lock:
        if (_es_client is not None
                and _es_client_key == key
                and _es_client_class is es_class):
            return _es_client

        _es_client = es_class(
            es_host,
            verify_certs=verify_certs,
            request_timeout=request_timeout,
            **auth_args,
        )
        _es_client_key = key
        _es_client_class = es_class
        return _es_client


def fetch_logs(event_id: str, catalog: Optional[TemplateCatalog] = None,
               window=None, out_diagnostics: Optional[dict] = None) -> list[dict]:
    """Fetch logs from Elasticsearch for a given event_id.

    Returns a list of dicts with keys: timestamp, level, message, app_name,
    in @timestamp ASC order. The query itself scans DESC so that a fetch
    capped by LOG_MAX_DOCUMENTS keeps the most RECENT lines; the list is
    reversed before it is returned.

    `window` is accepted for LogSource protocol conformance but is NOT used to
    bound the query -- see the ES_SEARCH_WINDOW_DAYS comment below for why a
    kubelet-sized window must not be applied to the system of record.

    `out_diagnostics`, when a caller passes a dict, is populated with facts
    about the fetch that the returned list cannot express -- currently
    `truncated` and `max_documents`. It is populated on every return path,
    including the mock-CSV and unconfigured-host ones. An out-parameter rather
    than a
    changed return type because eight callers (build_catalog.py and the
    fetcher's own tests) want exactly today's list and only
    `ElasticLogSource` needs to build an EvidenceGap from the result.
    Omitting it gives byte-for-byte the previous behaviour.
    """
    log = logger.bind(event_id=event_id)
    log.info("Elasticsearch fetch started")

    # Seed the out-parameter immediately. Three branches below return before
    # the paginated scan -- the mock-CSV path (twice) and the unconfigured-host
    # path -- and leaving the dict untouched on those would make "not
    # truncated" indistinguishable from "never reported".
    if out_diagnostics is not None:
        out_diagnostics["truncated"] = False
        out_diagnostics["max_documents"] = _max_documents()

    # --- Testing/Mock Mode: Load logs from a local CSV file ---
    mock_file = os.environ.get("ES_MOCK_FILE")
    if mock_file:
        log.info("ES_MOCK_FILE is set; loading logs from file", mock_file=mock_file)
        logs = []
        try:
            with open(mock_file, 'r', encoding='utf-8', errors='replace') as f:
                # Skip the header line
                next(f, None)
                for line in f:
                    # Extract the JSON object from the line
                    start_idx = line.find('{')
                    end_idx = line.rfind('}')
                    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                        log_json_str = line[start_idx:end_idx+1]
                        
                        # Kibana CSV exports often escape quotes as ""
                        if '""' in log_json_str:
                            log_json_str = log_json_str.replace('""', '"')
                            
                        # Filter by event_id if provided (skips logs not matching the event)
                        if event_id and event_id not in log_json_str:
                            continue
                            
                        try:
                            source = json.loads(log_json_str)
                            logs.append({
                                "timestamp": source.get("@timestamp", "UNKNOWN_TIME"),
                                "level": source.get("level", "INFO"),
                                "message": source.get("message", str(source)),
                                "app_name": source.get("application_name", "unknown-service"),
                            })
                        except json.JSONDecodeError as e:
                            log.warning("Skipped a mock log line that was not valid JSON",
                                        error=str(e), sample=log_json_str[:150])
                            continue
            # Sort by timestamp to mimic Elasticsearch's ascending order
            logs.sort(key=lambda x: x["timestamp"])
            log.info("Mock logs loaded", record_count=len(logs), mock_file=mock_file)
            return logs
        except Exception as e:
            log.error("Failed to load the mock log file", mock_file=mock_file, error=f"{type(e).__name__}: {e}")
            return []

    es_host = os.environ.get("ES_HOST")
    es_user = os.environ.get("ES_USERNAME")
    es_pass = os.environ.get("ES_PASSWORD")
    index_pattern = os.environ.get("ES_INDEX_PATTERN", "logs-*")

    if not es_host:
        log.info("ES_HOST is not set; returning mock logs")
        return [
            {"timestamp": "MOCK", "level": "ERROR", "message": f"[MOCK] connection timeout for {event_id}", "app_name": "mock-service"}
        ]

    auth_args = {}
    if es_user and es_pass:
        auth_args["basic_auth"] = (es_user, es_pass)

    # Certificate verification defaults ON -- it was previously disabled
    # unconditionally with no override, and no request timeout was set (1.9).
    # The client itself is cached across packets (F16).
    es_client = _get_es_client(es_host, auth_args)

    # Build query --------------------------------------------------------
    must_clauses = [
        {"query_string": {"query": f'"{event_id}"'}}
    ]

    filter_clauses = []

    # Restrict to the configured apps. `terms` (not `term`) so more than one
    # service is reachable; empty means no app restriction at all (F19).
    app_names = _app_names()
    if app_names:
        filter_clauses.append({"terms": {"application_name.keyword": app_names}})

    # Bound the query by time so it stops scanning the whole `logs-*` pattern
    # for every packet (F16).
    #
    # Deliberately NOT derived from the `window` argument. That window defaults
    # to K8S_DEFAULT_SINCE_HOURS=2, sized for kubelet retention -- and
    # investigations routinely run much later than the event: consumer lag, DLQ
    # replays, MAX_IN_PROGRESS_AGE_SECONDS resumption, checkpoint resumes and
    # the Investigator retry loop all re-enter this path hours or days after
    # the fact. Applying a 2h bound to Elasticsearch, the system of record,
    # would silently drop exactly the evidence those paths exist to recover.
    #
    # So this is its own much wider knob, and unset means unbounded -- i.e.
    # today's behaviour, unchanged, until an operator opts in.
    search_days = os.environ.get("ES_SEARCH_WINDOW_DAYS", "").strip()
    if search_days:
        try:
            filter_clauses.append(
                {"range": {"@timestamp": {"gte": f"now-{int(float(search_days))}d"}}}
            )
        except ValueError:
            log.warning("Ignoring invalid ES_SEARCH_WINDOW_DAYS", value=search_days)

    # Stage 1 enhancement: must_not from catalog boilerplate
    must_not_clauses = []
    if catalog:
        for phrase in catalog.get_boilerplate_phrases():
            must_not_clauses.append({"match_phrase": {"message": phrase}})

    query = {"bool": {"must": must_clauses}}
    if filter_clauses:
        query["bool"]["filter"] = filter_clauses
    if must_not_clauses:
        query["bool"]["must_not"] = must_not_clauses

    # Source-filter: only pull the fields we need
    source_fields = ["@timestamp", "level", "message", "application_name"]

    # Stable sort with _id tiebreaker (broadly compatible across ES versions).
    #
    # DESCENDING, and reversed to ascending before returning. This used to sort
    # ascending and, on hitting LOG_MAX_DOCUMENTS, keep `logs[:max_documents]`
    # -- the OLDEST lines -- and discard the newest. The end of the trace is
    # where the failure is, so a noisy event lost exactly the ERROR lines that
    # `reducer.branch_on_error` keys off, and a stuck packet was classified as
    # a clean rejection at full confidence.
    #
    # Slicing the tail of an ascending scan would not have fixed it: the loop
    # stops as soon as the cap is reached, so the newest pages are never
    # requested at all. The scan itself has to start from the recent end.
    sort_criteria = [
        {"@timestamp": {"order": "desc"}},
        {"_id": {"order": "desc"}},
    ]

    # Paginate with search_after ---------------------------------------
    logs = []
    search_after_values = None
    page_size = 500
    max_documents = _max_documents()
    truncated = False

    while True:
        # No seq_no_primary_term: the sort tiebreaker is _id, not _seq_no, so
        # requesting it added per-hit payload that nothing read (F17).
        search_kwargs = {
            "index": index_pattern,
            "size": page_size,
            "sort": sort_criteria,
            "query": query,
            "_source": source_fields,
        }
        if search_after_values:
            search_kwargs["search_after"] = search_after_values

        response = es_client.search(**search_kwargs)
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            source = hit["_source"]
            logs.append({
                "timestamp": source.get("@timestamp", "UNKNOWN_TIME"),
                "level": source.get("level", "INFO"),
                "message": source.get("message", str(source)),
                "app_name": source.get("application_name", "unknown-service"),
            })
            search_after_values = hit["sort"]

        if len(logs) >= max_documents:
            # We are scanning newest-first, so the surplus at the end of this
            # page is the oldest of what we read -- the right lines to drop.
            # How many MORE exist beyond the cap is unknown and unknowable
            # without a count query, hence `at least`.
            log.warning("Hit the LOG_MAX_DOCUMENTS cap; keeping the most recent results",
                        max_documents=max_documents)
            logs = logs[:max_documents]
            truncated = True
            break

        # A short page means this was the last one -- no point issuing one
        # more search_after request just to receive an empty page (1.10).
        if len(hits) < page_size:
            break

    # Back to ascending. Every downstream stage -- branch_on_error's context
    # window, cluster_logs' first_seen/last_seen, the flow-boundary guardrail
    # -- reads these in flow order, so the descending scan is an implementation
    # detail that must not escape this function.
    logs.reverse()

    log.info("Elasticsearch fetch completed", record_count=len(logs),
             index_pattern=index_pattern, truncated=truncated)
    if out_diagnostics is not None:
        out_diagnostics["truncated"] = truncated
        out_diagnostics["max_documents"] = max_documents
    return logs
