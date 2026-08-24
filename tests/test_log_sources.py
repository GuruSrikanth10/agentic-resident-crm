"""
Phase 1 of KUBERNETES_LOGS_PLAN.md -- the LogSource seam.

Phase 1's exit criterion is *zero behaviour change*: the Elasticsearch path
must behave exactly as it did when `reduce_logs` called `fetch_logs` directly.
These tests pin that down.
"""
from unittest.mock import MagicMock, patch

import pytest
from elasticsearch import ConnectionError as ESConnectionError

from src.log_pipeline.sources.base import LogSource
from src.log_pipeline.sources.elastic import ElasticLogSource
from src.log_pipeline.types import (
    REQUIRED_RECORD_KEYS,
    EvidenceGap,
    FetchContext,
    FetchResult,
    GapType,
    TimeWindow,
)


def _ctx(event_id="evt-1", catalog=None):
    return FetchContext(event_id=event_id, catalog=catalog)


def _es_hit(i, level="INFO"):
    return {
        "_source": {
            "@timestamp": f"2026-01-01T00:00:{i:02d}",
            "level": level,
            "message": f"message-{i}",
            "application_name": "enu-biometric",
        },
        "sort": [i],
    }


# ======================================================================
# Protocol conformance and canonical shape
# ======================================================================

def test_elastic_source_satisfies_the_protocol():
    assert isinstance(ElasticLogSource(), LogSource)
    assert ElasticLogSource.name == "elastic"


def test_fetch_returns_canonical_record_shape(monkeypatch):
    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")

    mock_client = MagicMock()
    mock_client.search.return_value = {"hits": {"hits": [_es_hit(1), _es_hit(2, "ERROR")]}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_client):
        result = ElasticLogSource().fetch("evt-1", TimeWindow.default(), _ctx())

    assert result.ok is True
    assert result.is_empty is False
    assert len(result.records) == 2
    for record in result.records:
        for key in REQUIRED_RECORD_KEYS:
            assert key in record, f"missing required key {key}"
        assert record["source"] == "elastic"


def test_diagnostics_are_populated(monkeypatch):
    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")

    mock_client = MagicMock()
    mock_client.search.return_value = {"hits": {"hits": [_es_hit(1)]}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_client):
        result = ElasticLogSource().fetch("evt-1", TimeWindow.default(), _ctx())

    assert result.diagnostics.source == "elastic"
    assert result.diagnostics.records_returned == 1
    assert result.diagnostics.latency_ms >= 0


def test_catalog_is_passed_through_to_the_fetcher():
    """The catalog drives the must_not boilerplate filter; dropping it would
    silently change which documents Elasticsearch returns."""
    catalog = MagicMock()

    with patch("src.log_pipeline.sources.elastic.fetch_logs", return_value=[]) as mock_fetch:
        ElasticLogSource().fetch("evt-1", TimeWindow.default(), _ctx(catalog=catalog))

    assert mock_fetch.call_args.kwargs["catalog"] is catalog


# ======================================================================
# The behaviour-preservation guard
# ======================================================================

def test_exceptions_propagate_and_are_not_swallowed():
    """`fetch_elastic_logs` carries @es_breaker and @retry_transient, both of
    which dispatch on exception TYPE. If this adapter caught the exception and
    returned ok=False -- or wrapped it -- retries and the circuit breaker
    would silently stop working, because neither would ever see a type it
    recognises. The original exception must escape untouched."""
    with patch("src.log_pipeline.sources.elastic.fetch_logs",
               side_effect=ESConnectionError("cluster down")):
        with pytest.raises(ESConnectionError):
            ElasticLogSource().fetch("evt-1", TimeWindow.default(), _ctx())


def test_transient_exception_type_is_still_recognised_by_retry():
    """Companion to the test above: prove the escaping type is one
    `retry_transient` actually retries on."""
    from src.utils.resilience import TRANSIENT_EXCEPTIONS
    assert issubclass(ESConnectionError, TRANSIENT_EXCEPTIONS)


def test_empty_fetch_is_ok_not_a_failure():
    """Looked-and-found-nothing must stay distinguishable from
    could-not-look (design principle 3)."""
    with patch("src.log_pipeline.sources.elastic.fetch_logs", return_value=[]):
        result = ElasticLogSource().fetch("evt-1", TimeWindow.default(), _ctx())

    assert result.ok is True
    assert result.is_empty is True
    assert result.records == []


def test_window_is_ignored_by_the_elastic_source():
    """Phase 1 explicitly does not add a time bound to the ES query; doing so
    would be a behaviour change. Two different windows must produce identical
    calls."""
    with patch("src.log_pipeline.sources.elastic.fetch_logs", return_value=[]) as mock_fetch:
        ElasticLogSource().fetch("evt-1", TimeWindow(hours=2), _ctx())
        ElasticLogSource().fetch("evt-1", TimeWindow(hours=99), _ctx())

    first, second = mock_fetch.call_args_list
    assert first == second


# ======================================================================
# ES_MOCK_FILE CSV path -- must keep working byte for byte
# ======================================================================

def _write_kibana_csv(path, event_id):
    """Kibana CSV export shape: header row, then rows embedding a JSON doc
    with doubled quotes."""
    def row(ts, level, msg):
        doc = (
            '{""@timestamp"":""%s"",""level"":""%s"",'
            '""message"":""%s"",""application_name"":""enu-biometric""}' % (ts, level, msg)
        )
        return f'"{ts}","{doc}"\n'

    path.write_text(
        '"time","_source"\n'
        + row("2026-01-01T00:00:01Z", "INFO", f"starting {event_id}")
        + row("2026-01-01T00:00:02Z", "ERROR", f"boom for {event_id}")
        + row("2026-01-01T00:00:03Z", "INFO", "unrelated line without the id")
    )


def test_es_mock_file_csv_still_works_through_the_adapter(monkeypatch, tmp_path):
    csv_path = tmp_path / "sample_logs.csv"
    _write_kibana_csv(csv_path, "evt-csv-1")
    monkeypatch.setenv("ES_MOCK_FILE", str(csv_path))

    result = ElasticLogSource().fetch("evt-csv-1", TimeWindow.default(), _ctx("evt-csv-1"))

    assert result.ok is True
    # Only the two lines containing the id; the unrelated line is filtered out.
    assert len(result.records) == 2
    assert {r["level"] for r in result.records} == {"INFO", "ERROR"}
    assert all(r["source"] == "elastic" for r in result.records)


def test_adapter_output_matches_calling_the_fetcher_directly(monkeypatch, tmp_path):
    """The strongest Phase 1 guarantee: going through the seam yields exactly
    what the old direct call yielded, apart from the added `source` tag."""
    import src.log_pipeline.fetcher as fetcher_module

    csv_path = tmp_path / "sample_logs.csv"
    _write_kibana_csv(csv_path, "evt-csv-2")
    monkeypatch.setenv("ES_MOCK_FILE", str(csv_path))

    direct = fetcher_module.fetch_logs("evt-csv-2", catalog=None)
    through_seam = ElasticLogSource().fetch(
        "evt-csv-2", TimeWindow.default(), _ctx("evt-csv-2")
    ).records

    stripped = [{k: v for k, v in r.items() if k != "source"} for r in through_seam]
    assert stripped == direct


# ======================================================================
# reduce_logs still behaves identically
# ======================================================================

def test_reduce_logs_reports_no_logs_found_when_empty(monkeypatch):
    """Pinned to the elastic-only leg specifically (LOG_SOURCE default is now
    kubernetes,elastic -- see chain.py -- and an unconfigured Kubernetes leg
    would otherwise add a SOURCE_FALLBACK gap this test isn't about)."""
    from src.log_pipeline import pipeline as pipeline_module

    monkeypatch.setenv("LOG_SOURCE", "elastic")
    with patch("src.log_pipeline.sources.elastic.fetch_logs", return_value=[]):
        out = pipeline_module.reduce_logs("evt-empty")

    assert out == "No logs found for ID: evt-empty"


def test_reduce_logs_end_to_end_through_the_csv_mock(monkeypatch, tmp_path):
    from src.log_pipeline import pipeline as pipeline_module

    # Pinned to the elastic-only leg, for the same reason as the test above:
    # ES_MOCK_FILE is an Elasticsearch-path feature, and the default chain
    # tries Kubernetes first. src/utils/env.py calls load_dotenv() at import,
    # so a developer .env that narrows LOG_SOURCE -- as one running against
    # K8S_FIXTURE_DIR does -- would otherwise make this test fail on their
    # machine and pass in CI.
    monkeypatch.setenv("LOG_SOURCE", "elastic")

    csv_path = tmp_path / "sample_logs.csv"
    _write_kibana_csv(csv_path, "evt-csv-3")
    monkeypatch.setenv("ES_MOCK_FILE", str(csv_path))

    out = pipeline_module.reduce_logs("evt-csv-3")

    # Small-trace path (<50 records) renders the raw lines.
    assert "evt-csv-3" in out
    assert "boom for evt-csv-3" in out


def test_reduce_logs_propagates_source_exceptions(monkeypatch):
    """reduce_logs must not convert a fetch failure into 'No logs found' --
    that would turn could-not-look into confirmed-absent."""
    from src.log_pipeline import pipeline as pipeline_module

    # Elastic-only: the exception under test is raised by the Elasticsearch
    # source, and it only propagates from the LAST source in the chain (see
    # chain.fetch_with_fallback) -- so the chain must end there.
    monkeypatch.setenv("LOG_SOURCE", "elastic")

    with patch("src.log_pipeline.sources.elastic.fetch_logs",
               side_effect=ESConnectionError("cluster down")):
        with pytest.raises(ESConnectionError):
            pipeline_module.reduce_logs("evt-boom")


# ======================================================================
# Value types
# ======================================================================

def test_time_window_arithmetic():
    from datetime import datetime, timezone

    window = TimeWindow(hours=2.5)
    assert window.seconds == 9000

    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert window.start_time(now).hour == 9
    assert window.start_time(now).minute == 30

    assert TimeWindow.default().hours == 2.0


def test_fetch_result_failure_helper():
    result = FetchResult.failure("kubernetes", "cluster unreachable")
    assert result.ok is False
    assert result.is_empty is True
    assert result.diagnostics.source == "kubernetes"


def test_evidence_gap_describes_itself():
    gap = EvidenceGap(GapType.LOG_ROTATION, "oldest line is 09:14:22Z")
    assert gap.describe() == "LOG_ROTATION: oldest line is 09:14:22Z"
    # str-Enum so it serialises cleanly into structured logs and JSON.
    assert gap.gap_type == "LOG_ROTATION"
