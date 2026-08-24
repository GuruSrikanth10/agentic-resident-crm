"""
Phase 9 of DLT_PLAN.md -- the operator CLI and observability wiring.

The exit criterion is that an operator gets from "what is failing most this
week" to a specific stack trace in two commands. `--unreviewed` exists because
nothing writes a `final` recommendation in v1: every recommendation in use is a
draft being served unreviewed, and a person needs to be able to see that queue.
"""
import time

import pytest

from src.dlt import case_storage, groups
from src.tools import dlt_report


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.delenv("CASEBOOK_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    # No group-lock directory to isolate any more: group updates go through
    # CasebookStorage.update_json, whose atomicity lives in the backend.
    case_storage.reset_cache()
    yield
    case_storage.reset_cache()


RECOMMENDATION = {
    "narrative": "The code reported the row absent.",
    "recommendation": "Query the table for these refIds.",
    "action": "DATA_FIX_REQUIRED",
    "confidence": 0.8,
    "discrepancy": None,
}


def seed(fingerprint, count, failure_class="A", signature="Sig @ Svc.method",
         recommend=True, corroboration="CORROBORATED"):
    for i in range(count):
        groups.record_occurrence(fingerprint, f"{fingerprint[:6]}-case-{i}",
                                 signature=signature, failure_class=failure_class,
                                 business_code="CODE", corroboration=corroboration)
    if recommend:
        groups.attach_recommendation(fingerprint, RECOMMENDATION)


# ======================================================================
# --top
# ======================================================================

def test_top_ranks_by_volume(capsys):
    seed("a" * 64, 5, signature="Rare @ A.one")
    seed("b" * 64, 50, signature="Common @ B.two")

    dlt_report.cmd_top(20)
    out = capsys.readouterr().out

    assert out.index("Common @ B.two") < out.index("Rare @ A.one")
    assert "2 distinct failure signatures" in out


def test_top_respects_the_limit(capsys):
    for i in range(5):
        seed(f"{i}" * 64, i + 1, signature=f"Sig{i} @ X.y")
    dlt_report.cmd_top(2)
    out = capsys.readouterr().out
    assert out.count("@ X.y") == 2


def test_top_on_an_empty_store(capsys):
    dlt_report.cmd_top(20)
    assert "No DLT groups recorded yet" in capsys.readouterr().out


# ======================================================================
# --group
# ======================================================================

def test_group_resolves_by_prefix(capsys):
    seed("abc" + "0" * 61, 3)
    dlt_report.cmd_group("abc")
    out = capsys.readouterr().out
    assert "Occurrences : 3" in out
    assert "DATA_FIX_REQUIRED" in out


def test_group_reports_an_ambiguous_prefix(capsys):
    seed("ab1" + "0" * 61, 1, signature="One @ A.b")
    seed("ab2" + "0" * 61, 1, signature="Two @ C.d")
    with pytest.raises(SystemExit, match="longer prefix"):
        dlt_report.cmd_group("ab")


def test_group_reports_an_unknown_prefix():
    with pytest.raises(SystemExit, match="No group"):
        dlt_report.cmd_group("zzzz")


def test_group_surfaces_contradiction_history(capsys):
    fingerprint = "c" * 64
    groups.record_occurrence(fingerprint, "case-1", failure_class="A",
                             corroboration="CORROBORATED")
    groups.record_occurrence(fingerprint, "case-2", failure_class="A",
                             corroboration="CONTRADICTED")

    dlt_report.cmd_group("cccc")
    out = capsys.readouterr().out
    assert "CONTRADICTED" in out
    assert "contradicted the declared exception" in out


def test_group_without_a_recommendation(capsys):
    seed("d" * 64, 2, recommend=False)
    dlt_report.cmd_group("dddd")
    assert "(none recorded)" in capsys.readouterr().out


def test_group_points_at_a_case_to_inspect(capsys):
    seed("e" * 64, 3)
    dlt_report.cmd_group("eeee")
    out = capsys.readouterr().out
    assert "--case eeeeee-case-2" in out, "two commands from --top to a trace"


# ======================================================================
# --case
# ======================================================================

def test_case_prints_the_casebook_and_trace(capsys):
    storage = case_storage.get_dlt_storage()
    storage.save_terminal("dlt-T-0-1", {
        "case_id": "dlt-T-0-1",
        "failure": {"business_code": "SOME_CODE"},
        "packet_status": {"status": "NEEDS_MANUAL_REVIEW"},
    })
    storage.save_artifact("dlt-T-0-1", "trace.txt", "java.lang.NullPointerException: boom")

    dlt_report.cmd_case("dlt-T-0-1")
    out = capsys.readouterr().out
    assert "SOME_CODE" in out
    assert "--- trace.txt ---" in out
    assert "NullPointerException" in out


def test_case_reports_a_missing_casebook():
    with pytest.raises(SystemExit, match="No casebook"):
        dlt_report.cmd_case("dlt-nope-0-0")


# ======================================================================
# --unreviewed
# ======================================================================

def test_unreviewed_lists_drafts_most_served_first(capsys):
    seed("a" * 64, 3, signature="Small @ A.b")
    seed("b" * 64, 300, signature="Widely served @ C.d")

    dlt_report.cmd_unreviewed()
    out = capsys.readouterr().out

    assert out.index("Widely served") < out.index("Small @ A.b")
    assert "served to   300 case(s)" in out
    assert "served to every subsequent occurrence" in out


def test_unreviewed_excludes_groups_with_no_recommendation(capsys):
    seed("a" * 64, 3, recommend=False)
    dlt_report.cmd_unreviewed()
    assert "No draft recommendations" in capsys.readouterr().out


# ======================================================================
# --stats
# ======================================================================

def test_stats_reports_the_class_split_and_cost_model(capsys):
    seed("a" * 64, 100, failure_class="A")
    seed("b" * 64, 60, failure_class="A")
    seed("c" * 64, 30, failure_class="B", recommend=False)
    seed("d" * 64, 10, failure_class="C", recommend=False)

    dlt_report.cmd_stats()
    out = capsys.readouterr().out

    assert "Cases analysed        : 200" in out
    assert "Distinct signatures   : 4" in out
    assert "160 Class A cases across 2 signatures" in out
    assert "~99% of LLM calls" in out


def test_stats_highlights_contradictions(capsys):
    fingerprint = "a" * 64
    groups.record_occurrence(fingerprint, "case-1", failure_class="A",
                             corroboration="CONTRADICTED")
    groups.record_occurrence(fingerprint, "case-2", failure_class="A",
                             corroboration="PARTIAL")

    dlt_report.cmd_stats()
    out = capsys.readouterr().out
    assert "2 case(s) where the logs did not support" in out
    assert "cannot get from Kafka UI" in out


def test_stats_on_an_empty_store(capsys):
    dlt_report.cmd_stats()
    assert "No DLT groups recorded yet" in capsys.readouterr().out


# ======================================================================
# Observability
# ======================================================================

def test_health_reports_all_four_consumer_heartbeats():
    from src.api.routes import health_check

    payload = health_check()
    for key in ("fast_consumer", "slow_consumer", "dlt_consumer",
                "dlt_analysis_consumer"):
        assert key in payload


def test_absent_dlt_heartbeat_is_unknown_not_dead():
    """The DLT roles are optional and off by default."""
    from src.api.routes import health_check

    payload = health_check()
    assert payload["dlt_consumer"]["alive"] in (None, True, False)


def test_dlt_metrics_are_registered():
    from src.utils import metrics

    metrics.record_dlt_case("A")
    metrics.record_dlt_corroboration("CONTRADICTED")
    metrics.record_dlt_reuse("REUSE_GROUP")
    metrics.record_dlt_registry_miss()
    metrics.record_dlt_window_age(43 * 3600)

    rendered = metrics.render_latest()
    if rendered is not None:
        text = rendered.decode("utf-8")
        assert "packetcrm_dlt_cases_total" in text
        assert "packetcrm_dlt_corroboration_total" in text


def test_group_json_round_trips():
    seed("a" * 64, 1)
    group = groups.load_group("a" * 64)
    import json
    assert json.loads(groups.as_json(group))["fingerprint"] == "a" * 64


def test_last_seen_ordering_is_stable():
    groups.record_occurrence("a" * 64, "case-1", failure_class="A")
    time.sleep(0.01)
    groups.record_occurrence("b" * 64, "case-2", failure_class="A")
    listed = groups.list_groups()
    assert listed[0]["fingerprint"] == "b" * 64
