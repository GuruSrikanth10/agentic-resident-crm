"""The decomposed group store (src/dlt/group_store.py).

`test_the_2026_09_22_run_against_a_store_that_refuses_if_match` is the one
that matters. It replays the run that exposed the defect -- three cases on one
fingerprint, each attaching a code-check verdict and a recommendation --
against an S3 fake that behaves like the production endpoint: creates work,
conditional overwrites are refused. Under the single-document store that
produced `occurrence_count: 1`, a null recommendation and a null code check.
"""
import threading

import pytest

from s3_fakes import FakeS3, install
from src.dlt import case_storage, group_store, groups
from src.storage import factory

FP = "cfa44b11" + "0" * 56
REC = {"narrative": "per-code narrative", "recommendation": "do x",
       "action": "DATA_FIX_REQUIRED", "confidence": 0.7}


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_GROUP_MEMBER_CAP", "CASEBOOK_STORAGE_BACKEND", "DLT_GROUP_STORE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    factory.reset_storage_cache()
    case_storage.reset_cache()
    yield
    factory.reset_storage_cache()
    case_storage.reset_cache()


def case(n):
    return f"dlt-ENU.MWARE.DEDUPE.PROCESS.COMPLETION.V1-17-{5051940 + n}"


# ======================================================================
# The regression
# ======================================================================

def test_the_2026_09_22_run_against_a_store_that_refuses_if_match(monkeypatch):
    install(monkeypatch, FakeS3(if_match="refuse"))

    for n, ref in enumerate(("1c120f1e", "64a0bb91", "f8973dfc")):
        groups.record_occurrence(FP, case(n), ref_id=ref,
                                 signature="BusinessException[UID_ORIGIN...]",
                                 failure_class="A",
                                 business_code="UID_ORIGIN_TRACKER_DATA_NOT_FOUND",
                                 corroboration="UNVERIFIABLE")
        groups.attach_code_check(FP, {"verdict": "NO_CHANGE", "reason": "r"},
                                 by_case_id=case(n))
        groups.attach_recommendation(FP, REC, by_case_id=case(n))

    group_store.reset_cache()                 # read it back cold, from "S3"
    group = groups.load_group(FP)

    assert group["occurrence_count"] == 3
    assert group["members"] == [case(0), case(1), case(2)]
    assert group["recommendation"] == REC
    assert group["recommendation_state"] == groups.STATE_DRAFT
    assert group["code_check"]["verdict"] == "NO_CHANGE"
    assert group["code_check_history"] == {"NO_CHANGE": 3}
    assert group["corroboration_history"] == {"UNVERIFIABLE": 3}
    assert group["business_code"] == "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"


def test_the_v2_store_never_sends_if_match(monkeypatch):
    fake = install(monkeypatch, FakeS3(if_match="refuse"))
    sent = []
    real_put = fake.put_object

    def spy(**kwargs):
        sent.append(kwargs.get("IfMatch"))
        return real_put(**kwargs)

    monkeypatch.setattr(fake, "put_object", spy)
    groups.record_occurrence(FP, case(0), failure_class="A")
    groups.attach_code_check(FP, {"verdict": "NO_CHANGE"}, by_case_id=case(0))
    groups.attach_recommendation(FP, REC, by_case_id=case(0))

    assert sent and all(v is None for v in sent)


def test_the_v1_store_does_fail_against_that_endpoint(monkeypatch):
    """Pins the diagnosis: the same run through the single-document store is
    what left every group record stuck at its creation state."""
    monkeypatch.setenv("DLT_GROUP_STORE", "v1")
    install(monkeypatch, FakeS3(if_match="refuse"))

    groups.record_occurrence(FP, case(0), failure_class="A")
    with pytest.raises(Exception):
        groups.record_occurrence(FP, case(1), failure_class="A")
    with pytest.raises(Exception):
        groups.attach_recommendation(FP, REC)


# ======================================================================
# Idempotency by construction
# ======================================================================

def test_the_same_case_is_counted_once():
    groups.record_occurrence(FP, case(0), failure_class="A", corroboration="UNVERIFIABLE")
    group = groups.record_occurrence(FP, case(0), failure_class="A",
                                     corroboration="CORROBORATED")
    assert group["occurrence_count"] == 1
    assert group["corroboration_history"] == {"UNVERIFIABLE": 1}, "first verdict stands"


def test_one_record_under_two_record_keys_is_one_occurrence():
    """1c120f1e and f8973dfc were the same DLT record -- partition 17, offset
    5051943 -- consumed under two record keys. Keyed on the refId they
    counted twice; keyed on the case id they are one occurrence."""
    groups.record_occurrence(FP, case(3), ref_id="1c120f1e", failure_class="A")
    group = groups.record_occurrence(FP, case(3), ref_id="f8973dfc", failure_class="A")
    assert group["occurrence_count"] == 1


def test_concurrent_occurrences_are_all_counted():
    barrier = threading.Barrier(8)
    errors = []

    def worker(n):
        try:
            barrier.wait()
            groups.record_occurrence(FP, case(n), failure_class="A",
                                     corroboration="UNVERIFIABLE")
        except Exception as e:                     # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    group_store.reset_cache()
    group = groups.load_group(FP)
    assert not errors
    assert group["occurrence_count"] == 8
    assert group["corroboration_history"] == {"UNVERIFIABLE": 8}


def test_a_code_check_per_case_is_counted_once_but_the_latest_wins():
    groups.attach_code_check(FP, {"verdict": "NOT_DEPLOYED"}, by_case_id=case(0))
    group = groups.attach_code_check(FP, {"verdict": "FIX_DEPLOYED"}, by_case_id=case(0))
    assert group["code_check"]["verdict"] == "FIX_DEPLOYED"
    assert group["code_check_history"] == {"NOT_DEPLOYED": 1}


def test_anonymous_code_checks_are_each_counted():
    for verdict in ("UNKNOWN", "UNKNOWN", "NO_CHANGE"):
        group = groups.attach_code_check(FP, {"verdict": verdict})
    assert group["code_check_history"] == {"UNKNOWN": 2, "NO_CHANGE": 1}


# ======================================================================
# Shape and ordering
# ======================================================================

def test_a_recommendation_alone_is_a_group():
    """The reuse loop can attach before the first occurrence lands."""
    groups.attach_recommendation(FP, REC)
    group = groups.load_group(FP)
    assert group is not None
    assert group["recommendation"] == REC
    assert group["occurrence_count"] == 0


def test_members_keep_arrival_order_past_ten():
    """Lexical order would put ...-10 before ...-9."""
    for n in range(12):
        group = groups.record_occurrence(FP, f"dlt-T-0-{n}", failure_class="A")
    assert group["members"][-3:] == ["dlt-T-0-9", "dlt-T-0-10", "dlt-T-0-11"]


def test_the_recommendation_records_which_case_wrote_it():
    group = groups.attach_recommendation(FP, REC, by_case_id=case(0))
    assert group["recommendation_by_case_id"] == case(0)


# ======================================================================
# Caching
# ======================================================================

def test_occurrences_are_fetched_once_per_process(monkeypatch):
    fake = install(monkeypatch, FakeS3())
    for n in range(20):
        groups.record_occurrence(FP, case(n), failure_class="A")

    group_store.reset_cache()
    gets = {"n": 0}
    real_get = fake.get_object

    def counting_get(**kwargs):
        if "/occurrences/" in kwargs["Key"]:
            gets["n"] += 1
        return real_get(**kwargs)

    monkeypatch.setattr(fake, "get_object", counting_get)

    groups.load_group(FP)
    assert gets["n"] == 20, "cold: each read once"

    group_store.invalidate(FP)
    groups.record_occurrence(FP, case(99), failure_class="A")
    assert gets["n"] == 20, "warm: the new one is known from its own write"


def test_caches_do_not_leak_between_storage_roots(monkeypatch, tmp_path):
    groups.record_occurrence(FP, case(0), failure_class="A")
    assert groups.load_group(FP)["occurrence_count"] == 1

    other = tmp_path / "elsewhere"
    other.mkdir()
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", other)
    factory.reset_scoped_cache()                    # new root, caches NOT cleared

    assert groups.load_group(FP) is None


def test_another_processes_write_is_visible_after_the_ttl(monkeypatch):
    monkeypatch.setattr(group_store, "CACHE_TTL_SECONDS", 0.0)
    groups.record_occurrence(FP, case(0), failure_class="A")
    assert groups.load_group(FP)["recommendation"] is None

    # Written behind this process's back, as another pod would.
    case_storage.get_group_storage().save(
        FP, {"recommendation": REC, "recommendation_state": "draft"},
        filename=group_store.RECOMMENDATION_FILENAME)

    assert groups.load_group(FP)["recommendation"] == REC


# ======================================================================
# Degradation and cutover
# ======================================================================

def test_an_unreadable_store_is_treated_as_novel(monkeypatch):
    def down():
        raise RuntimeError("storage down")

    monkeypatch.setattr(group_store, "get_group_storage", down)
    assert groups.load_group(FP) is None


def test_a_failed_listing_is_novel_not_empty(monkeypatch):
    fake = install(monkeypatch, FakeS3())
    groups.record_occurrence(FP, case(0), failure_class="A")
    groups.attach_recommendation(FP, REC)
    group_store.reset_cache()

    monkeypatch.setattr(fake, "get_paginator",
                        lambda name: (_ for _ in ()).throw(RuntimeError("503")))
    assert groups.load_group(FP) is None


def test_a_legacy_group_is_still_served():
    case_storage.get_group_storage().save(FP, {
        "fingerprint": FP, "occurrence_count": 1, "members": ["1c120f1e"],
        "recommendation": REC, "recommendation_state": "draft",
    }, filename="group.json")

    group = groups.load_group(FP)
    assert group["recommendation"] == REC


def test_a_legacy_recommendation_survives_the_first_new_occurrence():
    case_storage.get_group_storage().save(FP, {
        "fingerprint": FP, "recommendation": REC, "recommendation_state": "draft",
        "code_check": {"verdict": "NO_CHANGE"},
    }, filename="group.json")

    group = groups.record_occurrence(FP, case(0), failure_class="A")
    assert group["occurrence_count"] == 1
    assert group["recommendation"] == REC
    assert group["code_check"] == {"verdict": "NO_CHANGE"}


def test_v1_remains_selectable(monkeypatch):
    monkeypatch.setenv("DLT_GROUP_STORE", "v1")
    groups.record_occurrence(FP, case(0), failure_class="A")
    stored = case_storage.get_group_storage().load(FP, filename="group.json")
    assert stored["occurrence_count"] == 1


# ======================================================================
# The v1 record is folded in, never migrated
# ======================================================================

def _legacy(**fields):
    case_storage.get_group_storage().save(FP, {"fingerprint": FP, **fields},
                                          filename="group.json")


def test_the_v1_count_carries_across_the_cutover():
    """Local-backend deployments have real v1 counts; they must not reset."""
    _legacy(occurrence_count=5, members=["r1", "r2", "r3", "r4", "r5"],
            corroboration_history={"CORROBORATED": 4, "UNVERIFIABLE": 1},
            code_check_history={"NO_CHANGE": 2}, first_seen=100.0, last_seen=200.0,
            signature="sig", failure_class="A", business_code="CODE")

    group = groups.record_occurrence(FP, case(0), ref_id="r6", failure_class="A",
                                     corroboration="UNVERIFIABLE")

    assert group["occurrence_count"] == 6
    assert group["members"][-1] == case(0)
    assert group["corroboration_history"] == {"CORROBORATED": 4, "UNVERIFIABLE": 2}
    assert group["code_check_history"] == {"NO_CHANGE": 2}
    assert group["first_seen"] == 100.0
    assert group["signature"] == "sig" and group["business_code"] == "CODE"


def test_the_count_beyond_the_v1_member_cap_is_kept():
    _legacy(occurrence_count=500, members=["r498", "r499", "r500"])
    assert groups.load_group(FP)["occurrence_count"] == 500


def test_a_v1_member_redelivered_is_not_counted_twice():
    """v1 members were refIds; the same packet arriving again under v2 is
    matched on its refId."""
    _legacy(occurrence_count=2, members=["r1", "r2"])
    group = groups.record_occurrence(FP, case(0), ref_id="r1", failure_class="A")
    assert group["occurrence_count"] == 2


def test_rolling_back_and_forward_again_is_consistent(monkeypatch):
    groups.record_occurrence(FP, case(0), ref_id="r0", failure_class="A")   # v2

    monkeypatch.setenv("DLT_GROUP_STORE", "v1")                              # roll back
    groups.record_occurrence(FP, case(1), ref_id="r1", failure_class="A")

    monkeypatch.setenv("DLT_GROUP_STORE", "v2")                              # and forward
    group_store.reset_cache()
    group = groups.record_occurrence(FP, case(2), ref_id="r2", failure_class="A")

    assert group["occurrence_count"] == 3
    assert set(group["members"]) == {case(0), case(1), case(2)}


def test_a_malformed_v1_record_does_not_break_the_read():
    _legacy(occurrence_count="many", corroboration_history={"X": "lots"},
            members=None)
    group = groups.record_occurrence(FP, case(0), failure_class="A")
    assert group["occurrence_count"] == 1, "the corrupt count reads as zero"
    assert group["members"] == [case(0)]
    assert group["corroboration_history"] == {}
