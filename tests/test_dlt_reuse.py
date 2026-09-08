"""
Phase 7 of DLT_PLAN.md -- group records and the reuse policy.

Two properties carry the phase:

* `test_contradiction_overrides_a_cached_recommendation` -- the whole reason
  reuse is gated on corroboration rather than served blind. A fingerprint whose
  usual cause is a genuine missing record but which occasionally wraps a
  timeout must not be mislabelled on exactly the occurrences that matter.

* `test_replaying_a_corpus_collapses_to_a_few_groups` -- the measurement the
  cost model rests on. If a bug's occurrences do not collapse into one group,
  the LLM runs per message and 2,000/day is unaffordable.
"""
import pytest

from src.dlt import case_storage, groups
from src.dlt.corroborate import Verdict
from src.dlt.reuse import Decision, decide, llm_calls_avoided


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_GROUP_MEMBER_CAP", "DLT_REUSE_ENABLED", "CASEBOOK_STORAGE_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    # No group-lock directory to isolate any more: group updates go through
    # CasebookStorage.update_json, whose atomicity lives in the backend.
    case_storage.reset_cache()
    yield
    case_storage.reset_cache()


FP = "a" * 64
RECOMMENDATION = {"narrative": "the row is absent", "action": "DATA_FIX_REQUIRED"}


def seed_group(fingerprint=FP, with_recommendation=True, state=groups.STATE_DRAFT):
    groups.record_occurrence(fingerprint, "dlt-T-0-1", signature="sig",
                             failure_class="A", business_code="CODE",
                             corroboration="CORROBORATED")
    if with_recommendation:
        groups.attach_recommendation(fingerprint, RECOMMENDATION, state=state)
    return groups.load_group(fingerprint)


# ======================================================================
# Group records
# ======================================================================

def test_novel_fingerprint_has_no_group():
    assert groups.load_group(FP) is None


def test_first_occurrence_creates_the_group():
    group = groups.record_occurrence(FP, "dlt-T-0-1", signature="sig",
                                     failure_class="A", business_code="CODE")
    assert group["occurrence_count"] == 1
    assert group["members"] == ["dlt-T-0-1"]
    assert group["signature"] == "sig"
    assert group["recommendation_state"] == groups.STATE_NONE
    assert group["first_seen"] and group["last_seen"]


def test_occurrences_accumulate():
    for i in range(5):
        group = groups.record_occurrence(FP, f"dlt-T-0-{i}", failure_class="A")
    assert group["occurrence_count"] == 5
    assert len(group["members"]) == 5


def test_the_same_case_does_not_double_count():
    """A redrive would otherwise inflate every count and make the cost model
    look better than it is."""
    groups.record_occurrence(FP, "dlt-T-0-1", failure_class="A")
    group = groups.record_occurrence(FP, "dlt-T-0-1", failure_class="A")
    assert group["occurrence_count"] == 1
    assert group["members"] == ["dlt-T-0-1"]


def test_members_are_capped_but_the_count_keeps_going(monkeypatch):
    monkeypatch.setenv("DLT_GROUP_MEMBER_CAP", "3")
    for i in range(10):
        group = groups.record_occurrence(FP, f"dlt-T-0-{i}", failure_class="A")

    assert group["occurrence_count"] == 10
    assert len(group["members"]) == 3
    assert group["members"] == ["dlt-T-0-7", "dlt-T-0-8", "dlt-T-0-9"], "newest kept"


def test_first_seen_is_stable_and_last_seen_advances():
    first = groups.record_occurrence(FP, "dlt-T-0-1", failure_class="A")
    later = groups.record_occurrence(FP, "dlt-T-0-2", failure_class="A")
    assert later["first_seen"] == first["first_seen"]
    assert later["last_seen"] >= first["last_seen"]


def test_corroboration_history_accumulates():
    groups.record_occurrence(FP, "dlt-T-0-1", failure_class="A", corroboration="CORROBORATED")
    groups.record_occurrence(FP, "dlt-T-0-2", failure_class="A", corroboration="CORROBORATED")
    group = groups.record_occurrence(FP, "dlt-T-0-3", failure_class="A",
                                     corroboration="CONTRADICTED")
    assert group["corroboration_history"] == {"CORROBORATED": 2, "CONTRADICTED": 1}


def test_attach_recommendation_marks_it_draft():
    seed_group()
    group = groups.load_group(FP)
    assert group["recommendation"] == RECOMMENDATION
    assert group["recommendation_state"] == groups.STATE_DRAFT


def test_nothing_writes_final_in_v1():
    """There is no review mechanism yet, so every reused recommendation stays
    explicitly marked unreviewed."""
    seed_group()
    assert groups.load_group(FP)["recommendation_state"] != groups.STATE_FINAL


def test_list_groups_orders_by_recent_activity():
    groups.record_occurrence("a" * 64, "dlt-T-0-1", failure_class="A")
    groups.record_occurrence("b" * 64, "dlt-T-0-2", failure_class="A")
    listed = groups.list_groups()
    assert len(listed) == 2
    assert listed[0]["last_seen"] >= listed[1]["last_seen"]


def test_unreadable_group_is_treated_as_novel(monkeypatch):
    monkeypatch.setattr("src.dlt.groups.get_group_storage",
                        lambda: (_ for _ in ()).throw(RuntimeError("storage down")))
    assert groups.load_group(FP) is None


# ======================================================================
# Reuse policy
# ======================================================================

def test_novel_fingerprint_requires_the_llm():
    result = decide("A", Verdict.CORROBORATED.value, None)
    assert result.decision is Decision.LLM_REQUIRED
    assert result.calls_llm


def test_known_corroborated_fingerprint_reuses():
    result = decide("A", Verdict.CORROBORATED.value, seed_group())
    assert result.decision is Decision.REUSE_GROUP
    assert not result.calls_llm


def test_contradiction_overrides_a_cached_recommendation():
    """Blind cache reuse would disable the mis-cast detector on exactly the
    occurrences worth catching."""
    result = decide("A", Verdict.CONTRADICTED.value, seed_group())
    assert result.decision is Decision.LLM_REQUIRED
    assert "CONTRADICTED" in result.reason


def test_partial_corroboration_also_requires_the_llm():
    assert decide("A", Verdict.PARTIAL.value, seed_group()).decision is Decision.LLM_REQUIRED


def test_unverifiable_with_a_cached_recommendation_still_reuses():
    """Re-running the model on the same evidence minus the logs would cost
    tokens to reach a worse conclusion. Phase 8 caps the confidence."""
    result = decide("A", Verdict.UNVERIFIABLE.value, seed_group())
    assert result.decision is Decision.REUSE_GROUP
    assert "capped" in result.reason


def test_unverifiable_without_a_recommendation_requires_the_llm():
    assert decide("A", Verdict.UNVERIFIABLE.value, None).decision is Decision.LLM_REQUIRED


def test_group_without_a_recommendation_requires_the_llm():
    group = seed_group(with_recommendation=False)
    assert decide("A", Verdict.CORROBORATED.value, group).decision is Decision.LLM_REQUIRED


@pytest.mark.parametrize("failure_class", ["B", "C", "U"])
@pytest.mark.parametrize("verdict", [v.value for v in Verdict])
def test_non_business_classes_are_always_canned(failure_class, verdict):
    """No source to reason about, a fixed answer, or nothing parseable --
    at any occurrence count, and regardless of corroboration."""
    result = decide(failure_class, verdict, seed_group())
    assert result.decision is Decision.CANNED
    assert not result.calls_llm


def test_reuse_can_be_disabled(monkeypatch):
    monkeypatch.setenv("DLT_REUSE_ENABLED", "false")
    result = decide("A", Verdict.CORROBORATED.value, seed_group())
    assert result.decision is Decision.LLM_REQUIRED
    assert "disabled" in result.reason


def test_disabling_reuse_does_not_make_class_b_call_the_llm(monkeypatch):
    monkeypatch.setenv("DLT_REUSE_ENABLED", "false")
    assert decide("B", Verdict.CORROBORATED.value, None).decision is Decision.CANNED


# ======================================================================
# The cost model -- Phase 7's exit criteria
# ======================================================================

def test_replaying_a_corpus_collapses_to_a_few_groups():
    """400 messages across 3 real bugs must become 3 groups, not 400."""
    fingerprints = ["a" * 64, "b" * 64, "c" * 64]
    for i in range(400):
        groups.record_occurrence(fingerprints[i % 3], f"dlt-T-0-{i}",
                                 failure_class="A", corroboration="CORROBORATED")

    listed = groups.list_groups()
    assert len(listed) == 3
    assert sum(g["occurrence_count"] for g in listed) == 400


def test_only_the_first_occurrence_of_each_group_calls_the_llm():
    """The measurement that proves the cost model."""
    fingerprints = ["a" * 64, "b" * 64, "c" * 64]
    llm_calls = 0

    for i in range(400):
        fingerprint = fingerprints[i % 3]
        group = groups.load_group(fingerprint)
        result = decide("A", Verdict.CORROBORATED.value, group)
        if result.calls_llm:
            llm_calls += 1
            groups.attach_recommendation(fingerprint, RECOMMENDATION)
        groups.record_occurrence(fingerprint, f"dlt-T-0-{i}", failure_class="A")

    assert llm_calls == 3, "one investigation per distinct bug, not per message"
    assert llm_calls_avoided(400, llm_calls) > 0.99


def test_llm_calls_avoided_edge_cases():
    assert llm_calls_avoided(0, 0) == 0.0
    assert llm_calls_avoided(100, 100) == 0.0
    assert llm_calls_avoided(100, 200) == 0.0
