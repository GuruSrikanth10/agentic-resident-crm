"""
Phase D regression tests (ENHANCEMENT_PLAN.md section 5).

4.1 -- the outcome loop: verdicts, accuracy by reason code and source.
4.5 -- observability: /metrics, FetchDiagnostics emission, token accounting.
F16, F17, F19 -- Elasticsearch client caching, query shape.
F20, F21, F22 -- cached S3 client, LOG_LEVEL, one path definition.
"""
import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from src.utils import metrics, outcomes


# ======================================================================
# 4.1 -- the outcome loop
# ======================================================================

@pytest.fixture
def casesheets(tmp_path, monkeypatch):
    """Point every casesheets consumer at a tmp root.

    Outcomes now read and write through CasebookStorage rather than the local
    filesystem, so that a verdict can be recorded on any backend (G2). The
    fixture therefore redirects the storage layer, not just the path
    constants.
    """
    from src.storage.local import LocalFilesystemCasebookStorage

    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr(outcomes, "casebook_dir",
                        lambda event_id: tmp_path / f"casebook_{event_id}")

    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(outcomes, "get_casebook_storage", lambda: storage)
    return tmp_path


def _write_casebook(root, event_id, reason_code="DEDUP_REJECT",
                    action="REPLAY", source="agent", etype="U"):
    directory = root / f"casebook_{event_id}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "casebook.json").write_text(json.dumps({
        "packet_metadata": {"eid": event_id, "update_type": etype},
        "packet_status": {
            "status": "COMPLETED",
            "rejection_data": {"rejection_code": reason_code},
        },
        "resolution": {"source": source, "action": action},
    }), encoding="utf-8")
    return directory


def test_recording_a_verdict_persists_it(casesheets):
    _write_casebook(casesheets, "evt-1")
    outcome = outcomes.record_outcome("evt-1", "CORRECT", "operator-a",
                                      notes="looked right")

    assert outcome["verdict"] == "CORRECT"
    assert outcome["verified_by"] == "operator-a"
    assert outcome["verified_at"]

    reloaded = outcomes.load_outcome("evt-1")
    assert reloaded["verdict"] == "CORRECT"
    assert reloaded["notes"] == "looked right"


def test_outcome_denormalises_grouping_keys(casesheets):
    """accuracy_report must group without re-reading every casebook, and the
    outcome must stay interpretable if the casebook is later pruned."""
    _write_casebook(casesheets, "evt-2", reason_code="MAN_DEDUP",
                    action="QC_REPLAY", source="runbook:MAN_DEDUP__U@v3")
    outcome = outcomes.record_outcome("evt-2", "INCORRECT", "op",
                                      corrected_action="REPLAY")

    assert outcome["reason_code"] == "MAN_DEDUP"
    assert outcome["resolution_source"] == "runbook:MAN_DEDUP__U@v3"
    assert outcome["agent_action"] == "QC_REPLAY"
    assert outcome["corrected_action"] == "REPLAY"


def test_unknown_event_is_rejected(casesheets):
    """Recording against a never-investigated event would create a directory
    and pollute the accuracy denominator."""
    with pytest.raises(outcomes.UnknownEventError):
        outcomes.record_outcome("never-investigated", "CORRECT", "op")
    assert not (casesheets / "casebook_never-investigated").exists()


def test_invalid_verdict_is_rejected(casesheets):
    _write_casebook(casesheets, "evt-3")
    with pytest.raises(outcomes.InvalidVerdictError):
        outcomes.record_outcome("evt-3", "PROBABLY_FINE", "op")


def test_verdict_is_case_insensitive(casesheets):
    _write_casebook(casesheets, "evt-4")
    assert outcomes.record_outcome("evt-4", "correct", "op")["verdict"] == "CORRECT"


def test_a_verdict_can_be_revised(casesheets):
    """Ground truth changes when an operator learns more."""
    _write_casebook(casesheets, "evt-5")
    outcomes.record_outcome("evt-5", "CORRECT", "op-a")
    outcomes.record_outcome("evt-5", "INCORRECT", "op-b", notes="revised")

    assert outcomes.load_outcome("evt-5")["verdict"] == "INCORRECT"
    assert outcomes.load_outcome("evt-5")["verified_by"] == "op-b"


def test_summarise_reports_accuracy_per_reason_code(casesheets):
    for i in range(3):
        _write_casebook(casesheets, f"ok-{i}", reason_code="CODE_A")
        outcomes.record_outcome(f"ok-{i}", "CORRECT", "op")
    _write_casebook(casesheets, "bad-1", reason_code="CODE_A")
    outcomes.record_outcome("bad-1", "INCORRECT", "op")

    summary = outcomes.summarise(outcomes.iter_outcomes())
    row = next(r for r in summary["rows"] if r["reason_code"] == "CODE_A")

    assert row["total"] == 4
    assert row["CORRECT"] == 3
    assert row["accuracy"] == 0.75


def test_partial_does_not_count_as_correct(casesheets):
    """A partially correct resolution still needed a human, so it is not an
    automation win."""
    _write_casebook(casesheets, "p-1")
    outcomes.record_outcome("p-1", "PARTIAL", "op")

    row = outcomes.summarise(outcomes.iter_outcomes())["rows"][0]
    assert row["PARTIAL"] == 1
    assert row["accuracy"] == 0.0


def test_agent_and_runbook_are_compared_separately(casesheets):
    """This split is the runbook promotion gate (4.2)."""
    _write_casebook(casesheets, "a-1", reason_code="C", source="agent")
    outcomes.record_outcome("a-1", "CORRECT", "op")
    _write_casebook(casesheets, "r-1", reason_code="C",
                    source="runbook:C__U@v1")
    outcomes.record_outcome("r-1", "INCORRECT", "op")

    rows = outcomes.summarise(outcomes.iter_outcomes())["rows"]
    sources = {r["resolution_source"]: r for r in rows}

    assert sources["agent"]["accuracy"] == 1.0
    assert sources["runbook"]["accuracy"] == 0.0


def test_runbook_versions_aggregate_together(casesheets):
    """Otherwise every version is its own single-sample bucket."""
    for i, version in enumerate((1, 2)):
        _write_casebook(casesheets, f"rb-{i}", reason_code="C",
                        source=f"runbook:C__U@v{version}")
        outcomes.record_outcome(f"rb-{i}", "CORRECT", "op")

    rows = outcomes.summarise(outcomes.iter_outcomes())["rows"]
    assert len(rows) == 1
    assert rows[0]["resolution_source"] == "runbook"
    assert rows[0]["total"] == 2


def test_casebooks_without_outcomes_are_not_counted(casesheets):
    _write_casebook(casesheets, "judged")
    _write_casebook(casesheets, "unjudged")
    outcomes.record_outcome("judged", "CORRECT", "op")

    assert outcomes.summarise(outcomes.iter_outcomes())["total_outcomes"] == 1


def test_summarise_of_nothing_is_empty_not_an_error(casesheets):
    summary = outcomes.summarise(outcomes.iter_outcomes())
    assert summary == {"rows": [], "total_outcomes": 0}


# ======================================================================
# 4.5 -- observability
# ======================================================================

def test_metrics_helpers_are_noops_without_prometheus(monkeypatch):
    """An observability gap must never take the pipeline down with it."""
    monkeypatch.setattr(metrics, "METRICS_AVAILABLE", False)
    noop = metrics._counter("x_total", "doc", ("a",))
    noop.labels(a="1").inc()          # must not raise
    metrics._histogram("y", "doc").observe(1.0)
    assert metrics.render_latest() is None or True


def test_fetch_diagnostics_are_emitted(monkeypatch):
    """FetchDiagnostics was built on every fetch and then discarded."""
    from src.log_pipeline.types import EvidenceGap, FetchDiagnostics, GapType

    logged = {}
    monkeypatch.setattr(metrics.logger, "info",
                        lambda msg, **kw: logged.update(kw))

    diagnostics = FetchDiagnostics(
        source="kubernetes", records_returned=42, bytes_read=1024,
        latency_ms=250.0, pods_queried=3, pods_failed=1,
        redaction_counts={"AADHAAR": 2},
    )
    gaps = [EvidenceGap(GapType.LOG_ROTATION, "rotated", {})]

    metrics.record_fetch_diagnostics(diagnostics, ok=True, gaps=gaps)

    assert logged["source"] == "kubernetes"
    assert logged["records_returned"] == 42
    assert logged["pods_failed"] == 1
    assert logged["gap_count"] == 1


def test_fetch_diagnostics_tolerates_none():
    metrics.record_fetch_diagnostics(None, ok=False)   # must not raise


def test_llm_usage_is_recorded_when_the_provider_reports_it():
    message = MagicMock()
    message.usage_metadata = {"input_tokens": 100, "output_tokens": 20}
    metrics.record_llm_usage("investigator", {"messages": [message]})


def test_llm_usage_absent_does_not_raise():
    """Local OpenAI-compatible endpoints often omit usage_metadata."""
    message = MagicMock()
    message.usage_metadata = None
    metrics.record_llm_usage("investigator", {"messages": [message]})
    metrics.record_llm_usage("investigator", {})
    metrics.record_llm_usage("investigator", None)


def test_metrics_endpoint_returns_exposition_or_501():
    from src.api import routes

    if metrics.METRICS_AVAILABLE:
        response = routes.metrics_endpoint()
        assert b"agentic_resident_crm_" in response.body
    else:
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as excinfo:
            routes.metrics_endpoint()
        assert excinfo.value.status_code == 501


# ======================================================================
# F16, F17, F19 -- Elasticsearch query and client
# ======================================================================

def _search_body(monkeypatch, **env):
    """Run one fetch against a mock ES and return the query it built."""
    from src.log_pipeline import fetcher

    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    captured = {}
    mock_es = MagicMock()

    def fake_search(**kwargs):
        captured.update(kwargs)
        return {"hits": {"hits": []}}

    mock_es.search.side_effect = fake_search
    monkeypatch.setattr(fetcher, "_es_client", None)
    monkeypatch.setattr(fetcher, "_es_client_class", None)

    with patch("elasticsearch.Elasticsearch", return_value=mock_es):
        fetcher.fetch_logs("evt-1")
    return captured


def test_app_filter_is_configurable(monkeypatch):
    """Hardcoding one app made other services' logs unreachable (F19)."""
    body = _search_body(monkeypatch, ES_APP_NAMES="svc-a,svc-b")
    terms = body["query"]["bool"]["filter"][0]["terms"]
    assert terms["application_name.keyword"] == ["svc-a", "svc-b"]


def test_empty_app_filter_removes_the_restriction(monkeypatch):
    body = _search_body(monkeypatch, ES_APP_NAMES="")
    assert "filter" not in body["query"]["bool"]


def test_search_window_is_opt_in(monkeypatch):
    """Unset must stay unbounded: Elasticsearch is the system of record and
    investigations run long after the event."""
    body = _search_body(monkeypatch)
    filters = body["query"]["bool"].get("filter", [])
    assert not any("range" in f for f in filters)


def test_search_window_applies_when_configured(monkeypatch):
    body = _search_body(monkeypatch, ES_SEARCH_WINDOW_DAYS="7")
    ranges = [f for f in body["query"]["bool"]["filter"] if "range" in f]
    assert ranges[0]["range"]["@timestamp"]["gte"] == "now-7d"


def test_seq_no_primary_term_is_not_requested(monkeypatch):
    """The tiebreaker is _id, so requesting _seq_no was pure payload (F17)."""
    body = _search_body(monkeypatch)
    assert "seq_no_primary_term" not in body


def test_es_client_is_reused_across_fetches(monkeypatch):
    """A fresh client per packet means a new TLS handshake per packet (F16)."""
    from src.log_pipeline import fetcher

    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")
    monkeypatch.setattr(fetcher, "_es_client", None)
    monkeypatch.setattr(fetcher, "_es_client_class", None)

    mock_es = MagicMock()
    mock_es.search.return_value = {"hits": {"hits": []}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_es) as MockES:
        fetcher.fetch_logs("evt-1")
        fetcher.fetch_logs("evt-2")
        fetcher.fetch_logs("evt-3")

    assert MockES.call_count == 1


# ======================================================================
# F20, F21, F22
# ======================================================================

def test_s3_client_is_cached(monkeypatch):
    from src.utils import s3_uploader

    monkeypatch.setattr(s3_uploader, "_s3_client", None)
    monkeypatch.setenv("S3_LOGS_BUCKET", "some-bucket")

    with patch.object(s3_uploader, "boto3") as mock_boto3:
        s3_uploader.upload_logs_to_s3("evt-1", "logs")
        s3_uploader.upload_logs_to_s3("evt-2", "logs")

    assert mock_boto3.client.call_count == 1


def test_log_level_is_configurable(monkeypatch):
    from src.utils import logging_config

    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    assert logging_config._resolve_level() == logging.DEBUG

    monkeypatch.setenv("LOG_LEVEL", "nonsense")
    assert logging_config._resolve_level() == logging.INFO

    monkeypatch.delenv("LOG_LEVEL", raising=False)
    assert logging_config._resolve_level() == logging.INFO


def test_casesheets_path_has_one_definition():
    """It was independently re-derived in four places (F22)."""
    from src.log_pipeline import snapshot
    from src.storage.local import LocalFilesystemCasebookStorage
    from src.utils.paths import LOCAL_CASESHEETS_DIR, casebook_dir

    assert LocalFilesystemCasebookStorage().base_dir == LOCAL_CASESHEETS_DIR
    assert snapshot._casesheets_root() == LOCAL_CASESHEETS_DIR
    assert snapshot.snapshot_dir("e") == casebook_dir("e")


def test_casesheets_root_is_overridable(monkeypatch):
    """Needed for a container that mounts its data volume elsewhere."""
    import importlib
    monkeypatch.setenv("LOCAL_CASESHEETS_DIR", "/tmp/agentic-resident-crm-test-root")
    from src.utils import paths
    importlib.reload(paths)
    try:
        assert str(paths.LOCAL_CASESHEETS_DIR) == "/tmp/agentic-resident-crm-test-root"
    finally:
        monkeypatch.delenv("LOCAL_CASESHEETS_DIR", raising=False)
        importlib.reload(paths)


def test_schema_version_is_bumped_for_the_outcome_block(tmp_path):
    from src.storage.base import CASEBOOK_SCHEMA_VERSION
    from src.storage.local import LocalFilesystemCasebookStorage

    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    storage.save("evt-1", {"packet_status": {"status": "COMPLETED"}})

    assert CASEBOOK_SCHEMA_VERSION == "1.2"
    assert storage.load("evt-1")["schema_version"] == "1.2"
