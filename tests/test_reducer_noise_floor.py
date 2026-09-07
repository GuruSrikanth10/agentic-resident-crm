"""
Stage 2.5 (noise floor) and the Stage 4 collapse fix.

The bug these pin down: `apply_evidence_guardrails` could only collapse a
template via a catalog classification, so a deployment that never ran
`build_catalog` -- where every template classifies as "unknown" -- collapsed
nothing at all. On the reference trace 107 of 139 templates were kept in full
with example lines and the "reduced" output came out ~1.8x LARGER than the
input it was meant to compress.
"""
import pytest

from src.log_pipeline import reducer
from src.log_pipeline.reducer import (
    apply_evidence_guardrails,
    apply_noise_floor,
    collapse_sql,
)

SELECT = ("select a.one,a.two,a.three,a.four,a.five from uidprocessv3_d.bio_stage_tracker a "
          "where a.refid=? and a.sub_stage=?")
INSERT = ("insert into SCHEMA.bio_stage_tracker (created_by,creation_date,event_message,"
          "integrity,refid) values (?,?,?,?,?)")
UPDATE = ("update SCHEMA.bio_helper_cache_store set created_by=?,creation_date=?,"
          "enrl_type=?,helper_record=? where helper_record_key=?")


def _rec(level, message, ts="2026-09-07T15:31:08.7+05:30"):
    return {"timestamp": ts, "level": level, "message": message,
            "app_name": "enu-biometric"}


# ----------------------------------------------------------------- noise floor

def test_floor_drops_debug_and_keeps_the_rest(monkeypatch):
    monkeypatch.setattr(reducer, "MIN_LEVEL", "INFO")
    logs = [_rec("DEBUG", "setting the shard hint"), _rec("INFO", "Packet status"),
            _rec("WARN", "Integrity verification failed."), _rec("ERROR", "boom")]
    kept, report = apply_noise_floor(logs)
    assert [r["level"] for r in kept] == ["INFO", "WARN", "ERROR"]
    assert report["dropped_below_level"] == 1


def test_floor_never_discards_error_or_warn(monkeypatch):
    """The floor is clamped at WARN, so no LOG_MIN_LEVEL can delete a warning.

    "Integrity verification failed." is a WARN in the reference trace and is
    exactly the kind of evidence this pipeline exists to surface.
    """
    monkeypatch.setattr(reducer, "MIN_LEVEL", "ERROR")
    kept, _ = apply_noise_floor([_rec("WARN", "w"), _rec("ERROR", "e")])
    assert len(kept) == 2


def test_unknown_level_is_kept_not_dropped(monkeypatch):
    # A source emitting a level outside LEVEL_ORDER must not have its whole
    # trace deleted by a floor it was never measured against.
    monkeypatch.setattr(reducer, "MIN_LEVEL", "INFO")
    kept, _ = apply_noise_floor([_rec("NOTICE", "unusual but real")])
    assert len(kept) == 1


def test_floor_does_not_mutate_the_caller_s_records(monkeypatch):
    """raw_logs.txt is written from the same list; collapsing in place would
    retroactively edit the audit copy."""
    monkeypatch.setattr(reducer, "MIN_LEVEL", "INFO")
    original = _rec("INFO", SELECT)
    logs = [original]
    kept, _ = apply_noise_floor(logs)
    assert original["message"] == SELECT
    assert kept[0]["message"] != SELECT


# ------------------------------------------------------------------ SQL collapse

@pytest.mark.parametrize("statement,marker", [
    (SELECT, "columns elided"), (INSERT, "columns elided"), (UPDATE, "assignments elided"),
])
def test_sql_column_lists_collapse(statement, marker):
    out = collapse_sql(statement)
    assert marker in out and len(out) < len(statement)


def test_sql_collapse_keeps_table_and_predicate():
    out = collapse_sql(SELECT)
    # The diagnostic content is the table and the where clause, never the columns.
    assert "uidprocessv3_d.bio_stage_tracker" in out
    assert "where a.refid=? and a.sub_stage=?" in out


def test_non_sql_is_left_alone():
    msg = "Fetched Record From BioStageTracker for refId: db35bfda and subStage:ABIS_DEDUP"
    assert collapse_sql(msg) == msg


def test_short_column_list_is_not_worth_collapsing():
    # The marker would be longer than the text it replaces.
    assert collapse_sql("select count(*) from t where x=?") == "select count(*) from t where x=?"


def test_collapse_can_be_disabled(monkeypatch):
    monkeypatch.setattr(reducer, "COLLAPSE_SQL", False)
    assert collapse_sql(SELECT) == SELECT


# --------------------------------------------------------- Stage 4 collapse fix

def _cluster(cid, count, classification="unknown", has_error=False):
    return {"template_id": cid, "template": f"tpl-{cid}", "count": count,
            "first_seen": "t0", "last_seen": "t1", "classification": classification,
            "examples": [f"ex-{cid}-{i}" for i in range(3)], "has_error": has_error}


def test_repetitive_templates_collapse_without_a_catalog(monkeypatch):
    """The regression that made the reducer inflate instead of reduce."""
    monkeypatch.setattr(reducer, "BOILERPLATE_COUNT_THRESHOLD", 5)
    out = apply_evidence_guardrails([_cluster("a", count=40)], [])
    collapsed = out["clusters"][0]
    assert collapsed["examples"] == []
    assert collapsed["classification"] == "repetitive"


def test_rare_templates_still_keep_their_examples(monkeypatch):
    monkeypatch.setattr(reducer, "BOILERPLATE_COUNT_THRESHOLD", 5)
    out = apply_evidence_guardrails([_cluster("a", count=1)], [])
    assert out["clusters"][0]["examples"]
    assert out["clusters"][0]["classification"] == "rare"


def test_a_template_that_ever_errored_is_never_collapsed(monkeypatch):
    """Losing an ERROR to a frequency heuristic is the expensive direction."""
    monkeypatch.setattr(reducer, "BOILERPLATE_COUNT_THRESHOLD", 5)
    out = apply_evidence_guardrails([_cluster("a", count=500, has_error=True)], [])
    assert out["clusters"][0]["examples"], "an erroring template kept its evidence"


def test_catalog_boilerplate_still_collapses(monkeypatch):
    monkeypatch.setattr(reducer, "BOILERPLATE_COUNT_THRESHOLD", 999999)
    out = apply_evidence_guardrails([_cluster("a", count=40, classification="boilerplate")], [])
    assert out["clusters"][0]["examples"] == []
