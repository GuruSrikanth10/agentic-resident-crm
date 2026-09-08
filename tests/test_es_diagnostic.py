"""
Guards the Phase 0 diagnostic (KUBERNETES_LOGS_PLAN.md section 2).

The diagnostic's whole value rests on variant A being byte-identical to the
query production actually sends. If they drift, every hit-count comparison --
and therefore the Phase 0 decision gate -- is measuring the wrong thing.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.tools.es_diagnostic import (
    PRODUCTION_APP_FILTER,
    build_variants,
    build_service_aggregation,
    verdict_for_event,
)


def _capture_production_query(monkeypatch, event_id="evt-abc-123", catalog=None):
    """Run the real fetcher against a mocked ES client and capture its query."""
    import src.log_pipeline.fetcher as fetcher_module

    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")

    mock_client = MagicMock()
    mock_client.search.return_value = {"hits": {"hits": []}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_client):
        fetcher_module.fetch_logs(event_id, catalog=catalog)

    assert mock_client.search.called, "fetcher did not issue a search"
    return mock_client.search.call_args.kwargs["query"]


def test_variant_a_matches_production_query_exactly(monkeypatch):
    event_id = "evt-abc-123"
    production_query = _capture_production_query(monkeypatch, event_id)
    variant_a = build_variants(event_id, boilerplate_phrases=[])["A"][1]

    assert variant_a == production_query, (
        "Variant A has drifted from fetcher.py's query. The Phase 0 diagnostic "
        "is invalid until they match again."
    )


def test_variant_a_includes_catalog_must_not(monkeypatch):
    """With a catalog present, production adds must_not -- variant A must too."""
    event_id = "evt-abc-123"
    phrases = ["Starting biometric stage", "Heartbeat ping received"]

    catalog = MagicMock()
    catalog.get_boilerplate_phrases.return_value = phrases

    production_query = _capture_production_query(monkeypatch, event_id, catalog=catalog)
    variant_a = build_variants(event_id, boilerplate_phrases=phrases)["A"][1]

    assert "must_not" in production_query["bool"], "test premise broken: production omitted must_not"
    assert variant_a == production_query


def test_variants_isolate_one_change_each():
    variants = build_variants("evt-1", boilerplate_phrases=[])
    a, b, c, d = (variants[k][1] for k in ("A", "B", "C", "D"))

    # B differs from A only by dropping the application_name filter.
    assert "filter" in a["bool"] and "filter" not in b["bool"]
    assert a["bool"]["must"] == b["bool"]["must"]

    # C differs from A only by the match style.
    assert "filter" in c["bool"]
    assert c["bool"]["filter"] == a["bool"]["filter"]
    assert a["bool"]["must"] != c["bool"]["must"]

    # D drops both.
    assert "filter" not in d["bool"]
    assert d["bool"]["must"] == c["bool"]["must"]


def test_service_aggregation_is_unfiltered_by_app():
    """The aggregation must not carry the app filter, or it cannot reveal
    the services the filter is hiding -- which is its entire purpose."""
    query, aggs = build_service_aggregation("evt-1")
    assert "filter" not in query["bool"]
    assert aggs["by_service"]["terms"]["field"] == "application_name.keyword"


@pytest.mark.parametrize("counts,services,expected_fragment", [
    ({"A": 0, "B": 120, "C": 0, "D": 120}, {"other-svc": 120}, "APP FILTER IS THE CULPRIT"),
    ({"A": 0, "B": 0, "C": 45, "D": 45}, {PRODUCTION_APP_FILTER: 45}, "QUERY SYNTAX IS THE CULPRIT"),
    ({"A": 0, "B": 0, "C": 0, "D": 0}, {}, "ES HAS NOTHING"),
    ({"A": 50, "B": 50, "C": 50, "D": 50}, {PRODUCTION_APP_FILTER: 50}, "QUERY IS FINE"),
    ({"A": 10, "B": 90, "C": 10, "D": 90}, {PRODUCTION_APP_FILTER: 10, "gateway": 80}, "APP FILTER DROPS EVIDENCE"),
])
def test_verdict_logic(counts, services, expected_fragment):
    result = {"event_id": "evt-1", "counts": counts, "services": services}
    assert expected_fragment in verdict_for_event(result)


def test_verdict_is_inconclusive_when_a_query_errored():
    result = {
        "event_id": "evt-1",
        "counts": {"A": 5, "B": "ERROR: ApiError: boom", "C": 5, "D": 5},
        "services": {},
    }
    assert "INCONCLUSIVE" in verdict_for_event(result)
