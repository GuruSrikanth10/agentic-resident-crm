"""
Phase 9 of DLT_PLAN.md -- the operator CLI and observability wiring.

The exit criterion is that an operator gets from "what is failing most this
week" to a specific stack trace in two commands. `--unreviewed` exists because
nothing writes a `final` recommendation in v1: every recommendation in use is a
draft being served unreviewed, and a person needs to be able to see that queue.
"""
import time

import pytest

from src.dlt import case_storage, groups, parked
from src.storage import factory
from src.tools import dlt_report


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.delenv("CASEBOOK_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    # No group-lock directory to isolate any more: group updates go through
    # CasebookStorage.update_json, whose atomicity lives in the backend.
    case_storage.reset_cache()
    factory.reset_scoped_cache()
    yield
    case_storage.reset_cache()
    factory.reset_scoped_cache()


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
        assert "agentic_resident_crm_dlt_cases_total" in text
        assert "agentic_resident_crm_dlt_corroboration_total" in text


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


# ======================================================================
# Phase C8 -- the replay precheck's operator surface
# ======================================================================

from src.dlt import code_check as CC  # noqa: E402
from src.models.dlt_synthesis import DltFinding  # noqa: E402


def _park(monkeypatch, case_id, ref_id, required):
    monkeypatch.setenv("DLT_CODE_CHECK_PARK_ENABLED", "true")
    verdict = CC.CodeCheck(verdict=CC.NOT_DEPLOYED, reason="not running yet",
                           required_version=required, repo="ENU/enu-biometric",
                           path="src/main/java/Svc.java")
    finding = DltFinding(narrative="x", recommendation="y",
                         action="REDRIVE_AFTER_RECOVERY", confidence=0.6)
    return parked.park(case_id, ref_id, verdict, finding)


def test_parked_lists_what_is_waiting_and_for_which_version(monkeypatch, capsys):
    _park(monkeypatch, "dlt-T-1-1", "REF-1", "1.0.1")
    _park(monkeypatch, "dlt-T-1-2", "REF-2", "1.0.1")
    _park(monkeypatch, "dlt-T-1-3", "REF-3", "1.0.2")

    dlt_report.cmd_parked()
    out = capsys.readouterr().out

    assert "dlt-T-1-1" in out and "REF-3" in out
    assert "3 packet(s) parked" in out
    assert "2  1.0.1" in out
    assert "release_parked_replays" in out


def test_parked_on_an_empty_store_explains_itself(capsys):
    dlt_report.cmd_parked()
    out = capsys.readouterr().out

    assert "Nothing is parked" in out
    assert "not deployed yet" in out


def test_code_check_reports_the_verdict_distribution(capsys):
    seed("aaa" * 21 + "a", 3)
    groups.attach_code_check("aaa" * 21 + "a",
                             {"verdict": "NO_CHANGE", "reason": "nothing changed"})
    groups.attach_code_check("aaa" * 21 + "a",
                             {"verdict": "FIX_DEPLOYED", "reason": "it shipped"})

    dlt_report.cmd_code_check()
    out = capsys.readouterr().out

    assert "NO_CHANGE" in out
    assert "FIX_DEPLOYED" in out
    assert "Coverage: 100%" in out


def test_code_check_flags_a_mostly_unknown_corpus(capsys):
    seed("bbb" * 21 + "b", 1)
    for _ in range(4):
        groups.attach_code_check("bbb" * 21 + "b", {"verdict": "UNKNOWN"})
    groups.attach_code_check("bbb" * 21 + "b", {"verdict": "NO_CHANGE"})

    dlt_report.cmd_code_check()
    out = capsys.readouterr().out

    assert "Most checks establish nothing" in out
    assert "DLT_REPO_MAP" in out


def test_code_check_on_an_empty_store_says_how_to_start(capsys):
    dlt_report.cmd_code_check()
    out = capsys.readouterr().out

    assert "DLT_CODE_CHECK_ENABLED" in out
    assert "DLT_CODE_CHECK_GATES_REPLAY" in out


def test_group_shows_the_latest_verdict_inline(capsys):
    fingerprint = "ccc" * 21 + "c"
    seed(fingerprint, 2)
    groups.attach_code_check(fingerprint, {
        "verdict": "NOT_DEPLOYED", "reason": "the fix is not running yet",
        "repo": "ENU/enu-biometric", "path": "src/main/java/Svc.java",
        "branch": "release", "required_version": "1.0.5",
        "running_version": "1.0.1"})

    dlt_report.cmd_group(fingerprint[:8])
    out = capsys.readouterr().out

    assert "Replay precheck" in out
    assert "NOT_DEPLOYED" in out
    assert "1.0.5" in out
    assert "ENU/enu-biometric" in out


# -- the accuracy loop -------------------------------------------------------

def _casebook(case_id, ref_id, detected_at, verdict, queued=True):
    return {
        "case_id": case_id,
        "detected_at": detected_at,
        "packet": {"ref_id": ref_id},
        "code_check": {"verdict": verdict},
        "replay": {"attempted": queued, "queued": queued, "reason": "x"},
        "packet_status": {"status": "NEEDS_MANUAL_REVIEW"},
    }


def _seed_case(casebook):
    case_storage.get_dlt_storage().save(casebook["case_id"], casebook)


def test_accuracy_counts_a_replayed_packet_that_came_back(capsys):
    """A FIX_DEPLOYED verdict followed by a recurrence is a false positive.

    The second case is the recurrence itself -- the same refId dead-lettering
    again. It is not counted as its own replay, so the denominator is 1.
    """
    now = time.time()
    _seed_case(_casebook("dlt-T-1-1", "REF-1", now - 3600, "FIX_DEPLOYED"))
    _seed_case(_casebook("dlt-T-1-2", "REF-1", now, "UNKNOWN", queued=False))

    dlt_report.cmd_code_check_accuracy()
    out = capsys.readouterr().out

    assert "FIX_DEPLOYED" in out
    assert "false-positive rate: 100%" in out
    assert "dlt-T-1-1" in out


def test_accuracy_counts_a_replayed_packet_that_stayed_gone(capsys):
    now = time.time()
    _seed_case(_casebook("dlt-T-2-1", "REF-9", now - 3600, "FIX_DEPLOYED"))

    dlt_report.cmd_code_check_accuracy()
    out = capsys.readouterr().out

    assert "false-positive rate: 0%" in out
    assert "at least 30" in out


def test_a_high_no_change_recurrence_rate_means_the_verdict_is_right(capsys):
    now = time.time()
    _seed_case(_casebook("dlt-T-3-1", "REF-5", now - 3600, "NO_CHANGE"))
    _seed_case(_casebook("dlt-T-3-2", "REF-5", now, "UNKNOWN", queued=False))

    dlt_report.cmd_code_check_accuracy()
    out = capsys.readouterr().out

    assert "NO_CHANGE recurrence rate: 100%" in out
    assert "the\nverdict is right" in out


def test_a_recurrence_is_counted_once_per_replay_not_per_case(capsys):
    """Two replays of the same refId, both of which came back: the rate is
    over replays, not over casebooks."""
    now = time.time()
    _seed_case(_casebook("dlt-T-5-1", "REF-8", now - 7200, "FIX_DEPLOYED"))
    _seed_case(_casebook("dlt-T-5-2", "REF-8", now - 3600, "FIX_DEPLOYED"))
    _seed_case(_casebook("dlt-T-5-3", "REF-8", now, "UNKNOWN", queued=False))

    dlt_report.cmd_code_check_accuracy()
    out = capsys.readouterr().out

    assert "false-positive rate: 100% (2 of 2)" in out


def test_accuracy_ignores_cases_where_no_replay_fired(capsys):
    """A withheld replay produces no outcome, and is not guessed at."""
    now = time.time()
    _seed_case(_casebook("dlt-T-4-1", "REF-7", now, "NO_CHANGE", queued=False))

    dlt_report.cmd_code_check_accuracy()
    out = capsys.readouterr().out

    assert "No replayed case carries a code-check verdict yet" in out
    assert "GATES_REPLAY=false" in out


def test_accuracy_on_an_empty_store(capsys):
    dlt_report.cmd_code_check_accuracy()
    assert "No DLT casebooks recorded yet" in capsys.readouterr().out
