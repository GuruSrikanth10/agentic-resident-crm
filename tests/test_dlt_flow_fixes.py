"""The DLT flow fixes of 2026-09-22, end to end through the routes.

Each block pins one defect seen in that run:

  claims          one DLT record analysed twice under two record keys
  single-flight   a burst of one fingerprint paying for the LLM once per case
  honesty         casebooks asserting a recommendation was filed when the
                  write had failed
  per-code        packet-specific text cached against a fingerprint
  confidence      a documented root cause capped at 0.5 because logs were silent
  replay          ...without letting an uncorroborated finding auto-replay
  prompts         the DLT agent reading rejection-only rules
"""
import asyncio
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.api import dlt_routes
from src.api.dlt_routes import analyze_dlt, fetch_dlt_logs
from src.dlt import (auto_replay, case_storage, claims, code_check, groups,
                     parked, per_code, registry)
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy
from src.storage import factory

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]

FINDING = DltFinding(
    narrative="The origin-tracker lookup treats an absent row as fatal.",
    recommendation="Query the origin-tracker table for each matched candidate.",
    action="DATA_FIX_REQUIRED", confidence=0.9)

ENV = ("CASEBOOK_STORAGE_BACKEND", "DLT_GROUP_STORE", "DLT_CLAIM_ENABLED",
       "DLT_CLAIM_TTL_SECONDS", "DLT_ANALYSIS_TIMEOUT_SECONDS",
       "DLT_ANALYZE_TIMEOUT_SECONDS", "DLT_SINGLEFLIGHT_ENABLED",
       "DLT_SINGLEFLIGHT_WAIT_SECONDS", "DLT_REUSE_ENABLED",
       "DLT_PER_CODE_WITHHOLD", "DLT_UNVERIFIED_CONFIDENCE_CEILING",
       "DLT_LOGS_UNAVAILABLE_CEILING", "DLT_LOGS_SILENT_CEILING",
       "DLT_REPLAY_ALLOW_UNVERIFIED", "DLT_AUTO_REPLAY_ENABLED",
       "DLT_REPLAY_CONFIDENCE_THRESHOLD", "DLT_CODE_CHECK_ENABLED",
       "DLT_CODE_CHECK_GATES_REPLAY", "DLT_MAX_LOG_AGE_SECONDS")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ENV:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    monkeypatch.setattr(dlt_routes, "publish_to_dlt_analysis_queue", lambda m: True)
    # Offline and instant: the real call probes for a Kubernetes config.
    snapshot = SimpleNamespace(ok=False, mixed=False, versions=(),
                               as_dict=lambda: {"ok": False, "versions": []})
    monkeypatch.setattr(dlt_routes.deployed, "running_version", lambda: snapshot)
    factory.reset_storage_cache()
    case_storage.reset_cache()
    registry.clear_cache()
    yield
    factory.reset_storage_cache()
    case_storage.reset_cache()


def message(case_id="dlt-T-63-1", ref_id="REF-FIX-1"):
    return DltMessage(case_id=case_id, headers=dict(REFERENCE),
                      payload={"packetMetaData": {"refId": ref_id}}, ref_id=ref_id)


def stub_llm(monkeypatch, finding=FINDING, delay=0.0):
    calls = []
    lock = threading.Lock()

    def fake(ref_id, failure, corroboration, logs, payload_summary=None):
        with lock:
            calls.append(ref_id)
        if delay:
            time.sleep(delay)
        return finding, None

    monkeypatch.setattr(dlt_routes.orchestrator, "investigate", fake)
    return calls


def analyse(*messages):
    async def run():
        return await asyncio.gather(*(analyze_dlt(m) for m in messages))
    return asyncio.run(run())


def casebook(ref_id):
    return case_storage.get_dlt_storage().load(ref_id)


def fingerprint():
    return dlt_routes.build_failure(
        dlt_routes.parse_headers(REFERENCE),
        dlt_routes.parse_headers(REFERENCE).exception_message)["fingerprint"]


# ======================================================================
# Claims -- one analysis per DLT record, whatever its key
# ======================================================================

def test_the_first_delivery_wins_the_claim():
    claim = claims.claim_case("dlt-T-17-5051943", "1c120f1e")
    assert claim.won and claim.outcome == "won"


def test_the_same_record_under_another_key_is_a_duplicate():
    """1c120f1e and f8973dfc: partition 17, offset 5051943, two keys."""
    claims.claim_case("dlt-T-17-5051943", "1c120f1e")
    claim = claims.claim_case("dlt-T-17-5051943", "f8973dfc")
    assert not claim.won
    assert claim.outcome == "duplicate"
    assert claim.holder_ref_id == "1c120f1e"


def test_the_same_delivery_again_is_let_through():
    """A retry of the same case is not a duplicate; the terminal-status check
    after the claim is what stops a finished case being redone."""
    claims.claim_case("dlt-T-1-1", "REF-A")
    claim = claims.claim_case("dlt-T-1-1", "REF-A")
    assert claim.won and claim.outcome == "same_delivery"


def test_a_duplicate_of_a_finished_record_is_skipped_even_when_old(monkeypatch):
    monkeypatch.setenv("DLT_CLAIM_TTL_SECONDS", "1")
    claims.claim_case("dlt-T-1-1", "REF-A")
    case_storage.get_dlt_storage().save_terminal(
        "REF-A", {"packet_status": {"status": "NEEDS_MANUAL_REVIEW"}})
    _age_claim("dlt-T-1-1", seconds=60)

    claim = claims.claim_case("dlt-T-1-1", "REF-B")
    assert not claim.won and claim.outcome == "duplicate"


def test_an_abandoned_claim_is_taken_over(monkeypatch):
    monkeypatch.setenv("DLT_CLAIM_TTL_SECONDS", "1")
    claims.claim_case("dlt-T-1-1", "REF-A")
    _age_claim("dlt-T-1-1", seconds=60)

    claim = claims.claim_case("dlt-T-1-1", "REF-B")
    assert claim.won and claim.outcome == "reclaimed"
    assert claims.holder_of("dlt-T-1-1") == "REF-B"


def test_a_live_claim_is_not_taken_over(monkeypatch):
    monkeypatch.setenv("DLT_CLAIM_TTL_SECONDS", "3600")
    claims.claim_case("dlt-T-1-1", "REF-A")
    assert not claims.claim_case("dlt-T-1-1", "REF-B").won


def test_every_ref_id_a_record_arrived_under_is_findable():
    claims.claim_case("dlt-T-1-1", "REF-A")
    claims.claim_case("dlt-T-1-1", "REF-B")
    assert sorted(claims.aliases_of("dlt-T-1-1")) == ["REF-A", "REF-B"]


def test_an_unsafe_ref_id_is_not_used_as_a_filename(tmp_path):
    claims.claim_case("dlt-T-1-1", "../../escape")
    assert claims.aliases_of("dlt-T-1-1") == []
    assert not any(p.name.startswith("escape") for p in tmp_path.rglob("*"))


def test_a_broken_claim_store_fails_open(monkeypatch):
    monkeypatch.setattr(claims, "get_claim_storage",
                        lambda: (_ for _ in ()).throw(RuntimeError("down")))
    claim = claims.claim_case("dlt-T-1-1", "REF-A")
    assert claim.won and claim.outcome == "error"


def test_claims_can_be_disabled(monkeypatch):
    monkeypatch.setenv("DLT_CLAIM_ENABLED", "false")
    claims.claim_case("dlt-T-1-1", "REF-A")
    assert claims.claim_case("dlt-T-1-1", "REF-B").won


def test_the_ttl_follows_the_analysis_budget(monkeypatch):
    monkeypatch.setenv("DLT_ANALYSIS_TIMEOUT_SECONDS", "3600")
    assert claims.claim_ttl_seconds() == 7200
    monkeypatch.setenv("DLT_ANALYSIS_TIMEOUT_SECONDS", "60")
    assert claims.claim_ttl_seconds() == claims.MIN_CLAIM_TTL_SECONDS


def _age_claim(case_id, seconds):
    store = claims.get_claim_storage()
    record = store.load(case_id, filename=claims.CLAIM_FILENAME)
    record["claimed_at"] -= seconds
    store.save(case_id, record, filename=claims.CLAIM_FILENAME)


def test_the_fetch_lane_skips_a_second_key_for_the_same_record(monkeypatch):
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "1")      # no log fetch
    first = fetch_dlt_logs(message(case_id="dlt-T-17-5051943", ref_id="REF-KEY-1"))
    second = fetch_dlt_logs(message(case_id="dlt-T-17-5051943", ref_id="REF-KEY-2"))

    assert first["status"] == "queued_for_analysis"
    assert second == {"status": "already_processed",
                      "case_id": "dlt-T-17-5051943", "claimed_by": "REF-KEY-1"}


def test_the_analysis_lane_skips_a_record_claimed_by_another_key(monkeypatch):
    calls = stub_llm(monkeypatch)
    claims.claim_case("dlt-T-17-5051943", "REF-KEY-1")

    (result,) = analyse(message(case_id="dlt-T-17-5051943", ref_id="REF-KEY-2"))

    assert result["status"] == "already_processed"
    assert result["claimed_by"] == "REF-KEY-1"
    assert calls == []


def test_the_analysis_lane_proceeds_with_no_claim_on_file(monkeypatch):
    """Messages queued before claims existed must still be analysed."""
    calls = stub_llm(monkeypatch)
    (result,) = analyse(message())
    assert result["status"] == "processed"
    assert calls == ["REF-FIX-1"]


# ======================================================================
# Single-flight -- one investigation per fingerprint per burst
# ======================================================================

def test_a_burst_of_one_fingerprint_calls_the_llm_once(monkeypatch):
    """All five cases on 2026-09-22 reached the reuse decision within half a
    second; three shared a fingerprint and all three paid for the LLM."""
    calls = stub_llm(monkeypatch, delay=0.4)
    msgs = [message(case_id=f"dlt-T-63-{n}", ref_id=f"REF-SF-{n}") for n in range(3)]

    results = analyse(*msgs)

    assert len(calls) == 1
    assert sorted(r["decision"] for r in results) == \
        ["LLM_REQUIRED", "REUSE_GROUP", "REUSE_GROUP"]
    outcomes = sorted(casebook(m.ref_id)["provenance"]["single_flight"] for m in msgs)
    assert outcomes == ["leader", "reused", "reused"]
    assert groups.load_group(fingerprint())["occurrence_count"] == 3


def test_a_late_arrival_after_the_leader_finished_still_reuses(monkeypatch):
    """Decided "novel" before the gate, arrived after it was released: it
    never waits, so it must re-read anyway."""
    calls = stub_llm(monkeypatch)
    analyse(message(case_id="dlt-T-63-1", ref_id="REF-LATE-1"))

    real_decide = dlt_routes.reuse.decide
    first = {"done": False}

    def stale_first_decision(failure_class, verdict, group):
        if not first["done"]:
            first["done"] = True
            return real_decide(failure_class, verdict, None)   # the stale read
        return real_decide(failure_class, verdict, group)

    monkeypatch.setattr(dlt_routes.reuse, "decide", stale_first_decision)
    (result,) = analyse(message(case_id="dlt-T-63-2", ref_id="REF-LATE-2"))

    assert result["decision"] == "REUSE_GROUP"
    assert len(calls) == 1


def test_different_fingerprints_never_wait_on_each_other(monkeypatch):
    """Measured as overlap rather than wall-clock time, so a slow machine
    cannot make it flaky."""
    state = {"now": 0, "peak": 0}
    lock = threading.Lock()

    def fake(ref_id, failure, corroboration, logs, payload_summary=None):
        with lock:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
        time.sleep(0.3)
        with lock:
            state["now"] -= 1
        return FINDING, None

    monkeypatch.setattr(dlt_routes.orchestrator, "investigate", fake)
    other = dict(REFERENCE)
    other["kafka_exception-stacktrace"] = other["kafka_exception-stacktrace"].replace(
        "UID_ORIGIN_TRACKER_DATA_NOT_FOUND", "INDEX_MASTER_DATA_NOT_FOUND")
    a = message(case_id="dlt-T-63-1", ref_id="REF-FP-A")
    b = DltMessage(case_id="dlt-T-63-2", headers=other,
                   payload={"packetMetaData": {"refId": "REF-FP-B"}}, ref_id="REF-FP-B")

    analyse(a, b)
    assert state["peak"] == 2, "both investigations were in flight at once"


def test_single_flight_can_be_disabled(monkeypatch):
    monkeypatch.setenv("DLT_SINGLEFLIGHT_ENABLED", "false")
    calls = stub_llm(monkeypatch, delay=0.2)
    analyse(*[message(case_id=f"dlt-T-63-{n}", ref_id=f"REF-OFF-{n}") for n in range(2)])
    assert len(calls) == 2


def test_a_waiter_that_times_out_investigates_anyway(monkeypatch):
    monkeypatch.setenv("DLT_SINGLEFLIGHT_WAIT_SECONDS", "0.05")
    calls = stub_llm(monkeypatch, delay=0.4)
    msgs = [message(case_id=f"dlt-T-63-{n}", ref_id=f"REF-TO-{n}") for n in range(2)]

    analyse(*msgs)

    assert len(calls) == 2
    outcomes = sorted(casebook(m.ref_id)["provenance"]["single_flight"] for m in msgs)
    assert outcomes == ["leader", "timeout"]


# ======================================================================
# Casebook honesty
# ======================================================================

def test_a_cached_recommendation_is_reported_as_draft(monkeypatch):
    stub_llm(monkeypatch)
    analyse(message())
    provenance = casebook("REF-FIX-1")["provenance"]
    assert provenance["recommendation_state"] == groups.STATE_DRAFT
    assert provenance["group_state"] == "ok"
    assert provenance["group_occurrences"] == 1
    assert groups.load_group(fingerprint())["recommendation_by_case_id"] == "dlt-T-63-1"


def test_a_failed_cache_write_is_reported_as_unpersisted(monkeypatch):
    """Every casebook on 2026-09-22 said "draft"; every group said "none"."""
    stub_llm(monkeypatch)
    monkeypatch.setattr(dlt_routes.groups, "attach_recommendation",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("412")))
    analyse(message())
    assert casebook("REF-FIX-1")["provenance"]["recommendation_state"] == \
        groups.STATE_UNPERSISTED


def test_a_failed_occurrence_write_is_distinguishable_from_a_first_one(monkeypatch):
    stub_llm(monkeypatch)
    monkeypatch.setattr(dlt_routes.groups, "record_occurrence",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("412")))
    analyse(message())
    provenance = casebook("REF-FIX-1")["provenance"]
    assert provenance["group_state"] in ("occurrence_not_recorded", "unavailable")
    assert provenance["group_state"] != "ok"


def test_occurrences_are_recorded_under_the_case_id_not_the_ref_id(monkeypatch):
    stub_llm(monkeypatch)
    analyse(message(case_id="dlt-T-63-9", ref_id="REF-KEYED"))
    assert groups.load_group(fingerprint())["members"] == ["dlt-T-63-9"]


def test_a_canned_finding_is_not_reported_as_a_cached_recommendation(monkeypatch):
    npe = dict(REFERENCE)
    npe["kafka_exception-stacktrace"] = (
        "org.springframework.X: outer\n\tat org.springframework.A.b(A.java:1)"
        "\nCaused by: java.lang.NullPointerException: boom"
        "\n\tat com.uidai.enu.biometric.Svc.doWork(Svc.java:88)\n\t... 3 more\n")
    npe.pop("kafka_exception-message", None)
    msg = DltMessage(case_id="dlt-B-0-1", headers=npe,
                     payload={"packetMetaData": {"refId": "REF-NPE"}}, ref_id="REF-NPE")
    analyse(msg)
    assert casebook("REF-NPE")["provenance"]["recommendation_state"] == groups.STATE_NONE


# ======================================================================
# Per-code guard
# ======================================================================

def test_a_finding_with_an_identifier_is_withheld_from_the_cache(monkeypatch):
    leaky = FINDING.model_copy(update={
        "narrative": "Candidate 6183f7cc-1234-4abc-9def-0123456789ab had no row."})
    calls = stub_llm(monkeypatch, finding=leaky)

    analyse(message(case_id="dlt-T-63-1", ref_id="REF-LEAK-1"))
    book = casebook("REF-LEAK-1")
    assert book["provenance"]["recommendation_state"] == groups.STATE_WITHHELD
    assert book["finding"]["per_code_violations"][0]["pattern"] == "uuid"
    assert groups.load_group(fingerprint())["recommendation"] is None

    analyse(message(case_id="dlt-T-63-2", ref_id="REF-LEAK-2"))
    assert len(calls) == 2, "nothing was cached, so the next case investigates"


def test_a_phrasing_violation_is_cached_but_recorded(monkeypatch):
    wordy = FINDING.model_copy(update={
        "narrative": "BioDataBaseHelperServiceImpl.java:185 threw for this packet."})
    stub_llm(monkeypatch, finding=wordy)
    analyse(message())
    book = casebook("REF-FIX-1")
    assert book["provenance"]["recommendation_state"] == groups.STATE_DRAFT
    assert {v["pattern"] for v in book["finding"]["per_code_violations"]} == \
        {"source_line", "this_packet"}


def test_withholding_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("DLT_PER_CODE_WITHHOLD", "false")
    leaky = SimpleNamespace(narrative="uid 123456789012", recommendation="",
                            discrepancy=None)
    check = per_code.check(leaky)
    assert check.violations and not check.withhold


# ======================================================================
# Confidence: documentation-primary ceilings
# ======================================================================

def _policy(could_not_look, confidence=0.99):
    return apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="DATA_FIX_REQUIRED",
                   confidence=confidence),
        failure_class="A", corroboration="UNVERIFIABLE", registry_hit=True,
        could_not_look=could_not_look)


def test_logs_that_could_not_be_checked_cap_at_the_unavailable_ceiling():
    finding = _policy(True)
    assert finding.confidence == 0.75
    assert {"unverifiable", "logs_unavailable"} <= set(finding.ceilings_applied)


def test_logs_that_were_checked_and_silent_cap_lower():
    finding = _policy(False)
    assert finding.confidence == 0.6
    assert {"unverifiable", "logs_silent"} <= set(finding.ceilings_applied)


def test_a_caller_that_does_not_say_keeps_the_original_ceiling():
    assert _policy(None).confidence == 0.5


def test_an_explicit_legacy_ceiling_is_still_honoured(monkeypatch):
    """Silently raising a cap an operator deliberately lowered is the worst
    direction to surprise anyone in."""
    monkeypatch.setenv("DLT_UNVERIFIED_CONFIDENCE_CEILING", "0.4")
    assert _policy(True).confidence == 0.4
    assert _policy(False).confidence == 0.4


def test_each_new_ceiling_can_be_set_on_its_own(monkeypatch):
    monkeypatch.setenv("DLT_LOGS_UNAVAILABLE_CEILING", "0.8")
    assert _policy(True).confidence == 0.8


def test_contradiction_is_still_capped_as_before():
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="NEEDS_MANUAL_REVIEW",
                   confidence=0.99),
        failure_class="A", corroboration="CONTRADICTED", registry_hit=True,
        could_not_look=False)
    assert finding.confidence == 0.6


# ======================================================================
# Replay: an uncorroborated finding never auto-replays
# ======================================================================

def _redrive(ceilings, confidence=0.9):
    return DltFinding(narrative="x", recommendation="y",
                      action="REDRIVE_AFTER_RECOVERY", confidence=confidence,
                      ceilings_applied=ceilings)


def test_an_uncorroborated_finding_that_clears_the_threshold_is_not_replayed(monkeypatch):
    """0.75 clears the 0.55 default. Under the old 0.5 cap this could not
    happen, which is the guarantee this rule now states outright."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(_redrive(["unverifiable", "logs_unavailable"], 0.75),
                                  "REF-1")
    assert not decision.should_replay
    assert "no runtime evidence corroborates" in decision.reason


def test_a_corroborated_finding_is_unaffected(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    assert auto_replay.decide(_redrive(["contradicted"], 0.58), "REF-1").should_replay


def test_the_rule_can_be_lifted_deliberately(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_REPLAY_ALLOW_UNVERIFIED", "true")
    assert auto_replay.decide(_redrive(["unverifiable"], 0.75), "REF-1").should_replay


def test_a_below_threshold_finding_keeps_its_original_reason(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(_redrive(["unverifiable"], 0.3), "REF-1")
    assert "below" in decision.reason


def test_parking_cannot_bypass_the_rule(monkeypatch):
    """Parked entries are released later without coming back through
    `decide` -- so the check must hold at park time."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")
    verdict = code_check.CodeCheck(verdict=code_check.NOT_DEPLOYED, reason="r")
    assert not parked.would_replay_but_for_the_version(
        _redrive(["unverifiable"], 0.75), "REF-1", verdict)
    assert parked.would_replay_but_for_the_version(
        _redrive(["contradicted"], 0.58), "REF-1", verdict)


def test_an_uncorroborated_redrive_is_not_replayed_end_to_end(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    stub_llm(monkeypatch, finding=DltFinding(
        narrative="x", recommendation="redrive", action="REDRIVE_AFTER_RECOVERY",
        confidence=0.95))
    (result,) = analyse(message())
    assert result["replay_attempted"] is False
    assert "no runtime evidence" in casebook("REF-FIX-1")["replay"]["reason"]


# ======================================================================
# Prompts: each flow gets its own rules
# ======================================================================

def test_the_dlt_agent_is_no_longer_told_it_is_the_rejection_agent():
    from src.utils.prompt_loader import render
    rendered = render("DltInvestigator", ref_id="R", output_path="/o")
    agents = Path("AGENTS.md").read_text(encoding="utf-8")
    for text in (rendered, agents):
        assert "Rejection Investigator Agent" not in text
    assert "Do NOT read `reason_codes.csv`" not in rendered
    assert "Enrolment type rules" not in rendered
    assert "Flow rules: DLT" in rendered


def test_the_rejection_agent_keeps_its_rules():
    import re
    from src.utils.prompt_loader import _load_text, render
    needed = set(re.findall(r"\{\{(\w+)\}\}", _load_text("RejectionInvestigator")))
    rendered = render("RejectionInvestigator", **{k: "x" for k in needed})
    assert "Enrolment type rules" in rendered
    assert "Do NOT read `reason_codes.csv`" in rendered
    assert "Flow rules: DLT" not in rendered


def test_the_cached_prompts_no_longer_instruct_per_packet_wording():
    for path in ("src/prompts/harness/DltInvestigator.md",
                 "src/prompts/DltSynthesisAgent.md"):
        text = Path(path).read_text(encoding="utf-8")
        assert "not available for this packet" not in text, path
        assert "UNVERIFIABLE: no higher than 0.5" not in text, path


@pytest.mark.parametrize("name", ["../AGENTS", "/etc/passwd", "rules/../../x"])
def test_an_include_cannot_leave_the_harness_directory(name, monkeypatch):
    from src.utils import prompt_loader
    monkeypatch.setattr(prompt_loader, "_load_text", lambda n: f"{{{{> {name}}}}}")
    with pytest.raises((ValueError, FileNotFoundError, OSError)):
        prompt_loader.render("anything")


def test_a_mistyped_include_fails_loudly(monkeypatch):
    """Left as literal text, it would silently drop the flow's rules."""
    from src.utils import prompt_loader
    monkeypatch.setattr(prompt_loader, "_load_text", lambda n: "{{> rules/dlt.md}}")
    with pytest.raises(ValueError):
        prompt_loader.render("anything")


def test_the_legacy_ceiling_copied_from_the_example_does_not_disable_the_split(monkeypatch):
    """.env.example has always shipped this line at 0.5, so nearly every .env
    carries it verbatim -- that is not an operator's choice."""
    monkeypatch.setenv("DLT_UNVERIFIED_CONFIDENCE_CEILING", "0.5")
    assert _policy(True).confidence == 0.75
    assert _policy(False).confidence == 0.6
    assert _policy(None).confidence == 0.5
