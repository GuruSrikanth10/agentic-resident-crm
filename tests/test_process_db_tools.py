"""
The enu-biometric process DB tools (src/tools/agent_tools/enu_biometric/:
stage_tracker.py, parking_queue.py, helper_cache.py and their _process_db.py,
over the shared read-only database layer, agent_tools/_database.py).

Run against SQLite in memory with the three tables' DDL columns. The
MySQL-only parts -- the READ ONLY session hook and URL escaping -- are tested
on their own.
"""
import json
from unittest.mock import MagicMock

import pybreaker
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.tools import agent_tools
from src.tools.agent_tools import _database
from src.tools.agent_tools.enu_biometric import _process_db
from src.utils.resilience import process_db_breaker

REFID = "3f2b9c1e-0d4a-4a8e-9b1f-6c7d8e9f0a1b"
CANDIDATE = "c0ffee00-1111-2222-3333-444455556666"
OTHER = "aaaaaaaa-0000-0000-0000-000000000000"
SID = "1234567890123456789012345678"


@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE bio_stage_tracker (
                stage_tracker_key INTEGER PRIMARY KEY, uid TEXT, refid TEXT,
                sid TEXT, sid_date TEXT, srn_num TEXT, priority INTEGER,
                enrl_type TEXT, stage TEXT, sub_stage TEXT,
                sub_stage_status TEXT, sub_stage_status_date TEXT,
                sub_stage_reason_code TEXT, sub_stage_reject_reason_code TEXT,
                markfor_stage_resubmission INTEGER,
                stage_resubmission_count INTEGER, event_message TEXT,
                created_by TEXT, creation_date TEXT, last_updated_by TEXT,
                last_updated_date TEXT, integrity TEXT)"""))
        conn.execute(text("""
            CREATE TABLE bio_helper_cache_store (
                helper_record_key INTEGER PRIMARY KEY, refid TEXT, sid TEXT,
                enrl_type TEXT, record_type TEXT, helper_record TEXT,
                created_by TEXT, creation_date TEXT, last_updated_by TEXT,
                last_updated_date TEXT, integrity TEXT)"""))
        conn.execute(text("""
            CREATE TABLE bio_parking_queue_store (
                parking_queue_key INTEGER PRIMARY KEY, refid TEXT,
                record_category TEXT, event_json TEXT, created_by TEXT,
                creation_date TEXT, last_updated_by TEXT,
                last_updated_date TEXT, is_parked INTEGER)"""))
    monkeypatch.setattr(_process_db.PROCESS, "get_engine", lambda: engine)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    for variable in ("PROCESS_DB_MAX_ROWS", "PROCESS_DB_MAX_OUTPUT_CHARS"):
        monkeypatch.delenv(variable, raising=False)
    process_db_breaker.close()
    yield engine
    process_db_breaker.close()


def call(name, **arguments):
    """Invoke a registered tool as an agent would; parse JSON results."""
    output = agent_tools.get_tool(name).invoke(arguments)
    try:
        return json.loads(output)
    except ValueError:
        return output


_stage_key = iter(range(1, 10_000))


def stage(conn, refid, sub_stage, status, at, attempt=0, reject=None,
          writer="Biometric", retry=0):
    conn.execute(text("""
        INSERT INTO bio_stage_tracker (stage_tracker_key, uid, refid, sid,
            sid_date, srn_num, priority, enrl_type, stage, sub_stage,
            sub_stage_status, sub_stage_reject_reason_code,
            markfor_stage_resubmission, stage_resubmission_count,
            event_message, created_by, creation_date, last_updated_date,
            integrity)
        VALUES (:key, 'enc-uid', :refid, :sid, '2026-09-01 09:00:00', 'SRN1', 9,
            'U', 'Biometric', :sub_stage, :status, :reject, :retry, :attempt,
            '{"big": "event"}', :writer, :at, :at, 'checksum')"""),
        {"key": next(_stage_key), "refid": refid, "sid": SID, "sub_stage": sub_stage,
         "status": status, "reject": reject, "retry": retry, "attempt": attempt,
         "writer": writer, "at": at})


def helper(conn, refid, record_type, record, key=None):
    conn.execute(text("""
        INSERT INTO bio_helper_cache_store (helper_record_key, refid, sid,
            enrl_type, record_type, helper_record, creation_date,
            last_updated_date)
        VALUES (:key, :refid, :sid, 'UPDATE', :record_type, :record,
            '2026-09-01 10:00:00', '2026-09-01 10:05:00')"""),
        {"key": key, "refid": refid, "sid": SID, "record_type": record_type,
         "record": json.dumps(record)})


def parking(conn, refid, category, status_map, bms_error=False, parked=1):
    conn.execute(text("""
        INSERT INTO bio_parking_queue_store (refid, record_category, event_json,
            created_by, creation_date, last_updated_date, is_parked)
        VALUES (:refid, :category, :event, 'enu-biometric',
            '2026-09-01 10:00:00', '2026-09-01 10:10:00', :parked)"""),
        {"refid": refid, "category": category, "parked": parked,
         "event": json.dumps({"parkingStatusMap": status_map,
                              "sendToPortalDueToBMSError": bms_error})})


# ======================================================================
# bio_stage_tracker
# ======================================================================

def test_summary_finds_an_open_substage_and_ignores_request_copies(db):
    with db.begin() as conn:
        stage(conn, REFID, "ABIS_DEDUP", "IN PROGRESS", "2026-09-01 10:00:00")
        stage(conn, REFID, "ABIS_DEDUP_RESUBMISSION", "IN PROGRESS", "2026-09-01 10:00:00")
        stage(conn, REFID, "ABIS_DEDUP", "COMPLETED", "2026-09-01 10:02:30")
        stage(conn, REFID, "SDK_CROSS_MATCH", "IN PROGRESS", "2026-09-01 10:03:00")
        stage(conn, REFID, "SDK_CROSS_MATCH_RESUBMISSION", "IN PROGRESS", "2026-09-01 10:03:00")
        stage(conn, OTHER, "MFC_PORTAL", "IN PROGRESS", "2026-09-01 10:03:00")

    result = call("bio_get_packet_stage_summary", refid=REFID)

    assert result["found"] is True
    assert [o["sub_stage"] for o in result["open_substages"]] == ["SDK_CROSS_MATCH"]
    assert {c["sub_stage"] for c in result["request_copies"]} == {
        "ABIS_DEDUP_RESUBMISSION", "SDK_CROSS_MATCH_RESUBMISSION"}
    dedup = next(s for s in result["substages"] if s["sub_stage"] == "ABIS_DEDUP")
    assert dedup["latest_state"] == "completed"
    assert dedup["attempts"][0]["duration_seconds"] == 150.0
    assert result["last_row"]["sub_stage"] in ("SDK_CROSS_MATCH", "SDK_CROSS_MATCH_RESUBMISSION")


def test_summary_marks_a_replayed_attempt_superseded_not_stuck(db):
    with db.begin() as conn:
        stage(conn, REFID, "UPDATE_CHECKER", "IN PROGRESS", "2026-09-01 10:00:00", attempt=0)
        stage(conn, REFID, "UPDATE_CHECKER", "IN PROGRESS", "2026-09-01 11:00:00",
              attempt=1, retry=1)
        stage(conn, REFID, "UPDATE_CHECKER", "COMPLETED", "2026-09-01 11:00:40", attempt=1)

    result = call("bio_get_packet_stage_summary", refid=REFID)

    assert result["open_substages"] == []
    checker = result["substages"][0]
    assert checker["latest_attempt"] == 1
    assert [a["state"] for a in checker["attempts"]] == ["superseded_by_replay", "completed"]
    assert checker["attempts"][1]["markfor_stage_resubmission"] == 1


def test_summary_reports_reject_codes_from_completed_rows(db):
    with db.begin() as conn:
        stage(conn, REFID, "MDD_POLICY_BATCH_1", "IN PROGRESS", "2026-09-01 10:00:00")
        stage(conn, REFID, "MDD_POLICY_BATCH_1", "COMPLETED", "2026-09-01 10:00:05",
              reject="RESIDENT_MAN_DEDUP_REJECT_TD")

    result = call("bio_get_packet_stage_summary", refid=REFID)

    assert result["reject_reason_codes"] == [{
        "sub_stage": "MDD_POLICY_BATCH_1", "created_by": "Biometric", "attempt": 0,
        "sub_stage_reject_reason_code": "RESIDENT_MAN_DEDUP_REJECT_TD",
        "at": "2026-09-01 10:00:05"}]


def test_summary_keeps_canary_rows_apart(db):
    with db.begin() as conn:
        stage(conn, REFID, "ABIS_DEDUP", "IN PROGRESS", "2026-09-01 10:00:00")
        stage(conn, REFID, "ABIS_DEDUP", "COMPLETED", "2026-09-01 10:01:00",
              writer="Biometric_Canary_Mode")

    result = call("bio_get_packet_stage_summary", refid=REFID)

    assert {(s["created_by"], s["latest_state"]) for s in result["substages"]} == {
        ("Biometric", "in_progress"), ("Biometric_Canary_Mode", "completed")}
    assert [o["created_by"] for o in result["open_substages"]] == ["Biometric"]


def test_status_spelt_with_an_underscore_is_still_in_progress(db):
    with db.begin() as conn:
        stage(conn, REFID, "ABIS_UPDATE", "IN_PROGRESS", "2026-09-01 10:00:00")
    assert call("bio_get_packet_stage_summary", refid=REFID)["open_substages"][0]["sub_stage"] \
        == "ABIS_UPDATE"


def test_summary_for_an_unknown_packet_says_not_found(db):
    assert call("bio_get_packet_stage_summary", refid=REFID) == {
        "refid": REFID, "found": False, "substages": []}


def test_timeline_is_oldest_first_without_event_uid_or_integrity(db):
    with db.begin() as conn:
        stage(conn, REFID, "ABIS_DEDUP", "IN PROGRESS", "2026-09-01 10:00:00")
        stage(conn, REFID, "ABIS_DEDUP", "COMPLETED", "2026-09-01 10:01:00",
              reject="SOME_CODE")

    result = call("bio_get_packet_stage_timeline", refid=REFID)

    assert [row["sub_stage_status"] for row in result["rows"]] == ["IN PROGRESS", "COMPLETED"]
    assert result["packet"] == {"sid": SID, "sid_date": "2026-09-01 09:00:00",
                                "srn_num": "SRN1", "priority": 9, "enrl_type": "U"}
    flat = json.dumps(result)
    assert "event" not in flat and "enc-uid" not in flat and "checksum" not in flat
    assert "sub_stage_reject_reason_code" not in result["rows"][0]
    assert result["rows"][1]["sub_stage_reject_reason_code"] == "SOME_CODE"


def test_timeline_filters_by_substage_and_keeps_the_newest_when_capped(db, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_MAX_ROWS", "2")
    with db.begin() as conn:
        for minute in range(4):
            stage(conn, REFID, "ABIS_DEDUP", "IN PROGRESS", f"2026-09-01 10:0{minute}:00",
                  attempt=minute)
        stage(conn, REFID, "SDK_CONSISTENCY", "IN PROGRESS", "2026-09-01 10:09:00")

    result = call("bio_get_packet_stage_timeline", refid=REFID, sub_stage="ABIS_DEDUP")

    assert result["truncated"] is True
    assert [row["stage_resubmission_count"] for row in result["rows"]] == [2, 3]
    assert {row["sub_stage"] for row in result["rows"]} == {"ABIS_DEDUP"}


def test_timeline_shrinks_to_fit_the_output_cap_keeping_valid_json(db, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_MAX_OUTPUT_CHARS", "900")
    with db.begin() as conn:
        for second in range(20):
            stage(conn, REFID, "ABIS_DEDUP", "IN PROGRESS", f"2026-09-01 10:00:{second:02d}")

    raw = agent_tools.get_tool("bio_get_packet_stage_timeline").invoke({"refid": REFID})
    result = json.loads(raw)

    assert len(raw) <= 900
    assert result["rows_left_out_to_fit"] > 0
    assert result["rows"][-1]["creation_date"] == "2026-09-01 10:00:19"


# ======================================================================
# bio_parking_queue_store
# ======================================================================

def test_applicant_waiting_on_an_in_process_candidate_is_parked(db):
    with db.begin() as conn:
        parking(conn, REFID, "APPLICANT", {CANDIDATE: "InProcess", OTHER: "Completed"})
        stage(conn, REFID, "BIO_CANDIDATE_PARKING", "COMPLETED", "2026-09-01 10:00:00")

    result = call("bio_get_parking_status", refid=REFID)

    assert result["parked_now"] is True
    assert result["as_applicant"]["counts"] == {"InProcess": 1, "Completed": 1}
    assert result["as_applicant"]["waiting_on"]["InProcess"] == [CANDIDATE]
    assert result["as_candidate"] == {"found": False}
    assert result["parking_stage_rows"][0]["sub_stage"] == "BIO_CANDIDATE_PARKING"


def test_a_departing_completed_row_means_not_parked_even_with_a_stale_map(db):
    with db.begin() as conn:
        parking(conn, REFID, "Applicant", {CANDIDATE: "InProcess"})
        stage(conn, REFID, "BIO_CANDIDATE_DEPARTING", "COMPLETED", "2026-09-01 11:00:00")

    result = call("bio_get_parking_status", refid=REFID)

    assert result["parked_now"] is False
    assert "BIO_CANDIDATE_DEPARTING" in result["parked_now_basis"]
    assert result["as_applicant"]["record_category"] == "Applicant"


def test_no_in_process_entry_left_means_not_parked(db):
    with db.begin() as conn:
        parking(conn, REFID, "APPLICANT", {CANDIDATE: "Completed"})
    result = call("bio_get_parking_status", refid=REFID)
    assert result["parked_now"] is False
    assert "no InProcess" in result["parked_now_basis"]


def test_candidate_row_lists_the_applicants_waiting_on_it(db):
    with db.begin() as conn:
        parking(conn, CANDIDATE, "CANDIDATE", {REFID: "InProcess"}, bms_error=True)

    result = call("bio_get_parking_status", refid=CANDIDATE)

    assert result["parked_now"] is False
    assert result["as_candidate"]["waiting_on_this"] == {"InProcess": [REFID]}
    assert result["as_candidate"]["send_to_portal_due_to_bms_error"] is True
    assert "never parked as an applicant" in result["parked_now_basis"]


def test_never_parked_packet(db):
    result = call("bio_get_parking_status", refid=REFID)
    assert result["found"] is False
    assert result["parked_now"] is False


# ======================================================================
# bio_helper_cache_store
# ======================================================================

ABIS_RECORD = {"abisMWResponseNewSeda": {
    "requestType": "BIOUPDATE", "responseStatus": "SUCCESS", "referenceId": REFID,
    "abisResponses": {"abisResponse": [
        {"abisId": "ABIS1", "candidates": {"matchedCandidate": [
            {"candidateRefId": CANDIDATE, "scaledScore": 7000, "faceScore": 60},
            {"candidateRefId": OTHER, "scaledScore": 9000}]},
         "diagnostics": {"note": "ok"}},
        {"abisId": "ABIS2", "candidates": {"matchedCandidate":
            {"candidateRefId": CANDIDATE, "scaledScore": 9500, "leftIrisScore": 80}}},
        {"abisId": "ABIS3", "candidates": {}},
    ]}}}


def test_list_helper_records_names_the_missing_steps(db):
    with db.begin() as conn:
        helper(conn, REFID, "AbisMwCandidateRecord", ABIS_RECORD)

    result = call("bio_list_helper_records", refid=REFID)

    assert [r["record_type"] for r in result["records"]] == ["AbisMwCandidateRecord"]
    assert result["records"][0]["helper_record_bytes"] > 0
    assert set(result["missing_record_types"]) == {
        "ApplicantCandidateHelperRecord", "ParkingHelperRecord", "UpdateCheckerHelperRecord"}


def test_abis_candidates_take_the_highest_score_across_engines(db):
    with db.begin() as conn:
        helper(conn, REFID, "AbisMwCandidateRecord", ABIS_RECORD)

    result = call("bio_get_abis_candidates", refid=REFID)

    assert result["requestType"] == "BIOUPDATE"
    assert [c["candidateRefId"] for c in result["candidates"]] == [CANDIDATE, OTHER]
    top = result["candidates"][0]
    assert top["highest_scaledScore"] == 9500
    assert [e["abisId"] for e in top["engines"]] == ["ABIS1", "ABIS2"]
    assert top["engines"][1]["leftIrisScore"] == 80
    assert [e["matched_candidates"] for e in result["engines"]] == [2, 1, 0]


def test_abis_record_without_the_wrapper_is_read_too(db):
    with db.begin() as conn:
        helper(conn, REFID, "AbisMwCandidateRecord", ABIS_RECORD["abisMWResponseNewSeda"])
    assert call("bio_get_abis_candidates", refid=REFID)["candidate_count"] == 2


def test_an_unexpected_shape_is_reported_not_read_as_no_candidates(db):
    with db.begin() as conn:
        helper(conn, REFID, "AbisMwCandidateRecord", {"somethingElse": {"x": 1}})
    result = call("bio_get_abis_candidates", refid=REFID)
    assert result["shape_recognized"] is False
    assert result["top_level_keys"] == ["somethingElse"]
    assert "candidates" not in result


def test_missing_record_means_the_step_did_not_happen(db):
    result = call("bio_get_abis_candidates", refid=REFID)
    assert result["found"] is False
    assert "No ABIS middleware response" in result["meaning"]


FACTS_RECORD = {
    "applicantRefId": REFID, "isWhitelistedApplicant": False,
    "isFirstTimeBioUpdate": True, "sendToPortalDueToBMSError": False,
    "candidatesWithAadhaar": [CANDIDATE], "candidatesWithOutAadhaar": [OTHER],
    "candidateHelperRecordMap": {
        CANDIDATE: {"uid": "234567890123", "eid": "E1", "maxAbisScore": 9500,
                    "isCandidateTD": True, "packetStatus": "COMPLETED"},
        OTHER: {"uid": None, "eid": "E2", "maxAbisScore": 9000,
                "isCandidateTD": False, "remark": "mobile 9876543210"},
    }}


def test_candidate_facts_mask_uids_and_rank_by_score(db):
    with db.begin() as conn:
        helper(conn, REFID, "ApplicantCandidateHelperRecord", FACTS_RECORD)

    result = call("bio_get_candidate_facts", refid=REFID)

    assert result["applicant"]["isFirstTimeBioUpdate"] is True
    assert [c["candidateRefId"] for c in result["candidates"]] == [CANDIDATE, OTHER]
    assert result["candidates"][0]["uid"] == "[REDACTED:UID]"
    assert result["candidates"][1]["uid"] is None
    assert "9876543210" not in json.dumps(result)
    assert result["candidatesWithAadhaar"] == {"count": 1, "items": [CANDIDATE]}


def test_candidate_facts_for_one_candidate(db):
    with db.begin() as conn:
        helper(conn, REFID, "ApplicantCandidateHelperRecord", FACTS_RECORD)

    one = call("bio_get_candidate_facts", refid=REFID, candidate_ref_id=CANDIDATE.upper())
    assert one["candidate_found"] is True
    assert [c["eid"] for c in one["candidates"]] == ["E1"]

    none = call("bio_get_candidate_facts", refid=REFID, candidate_ref_id="not-a-candidate")
    assert none["candidate_found"] is False
    assert none["candidates"] == []
    assert none["candidate_count"] == 2


def test_parking_verdicts_are_counted_and_unwrapped(db):
    verdicts = {CANDIDATE: {"FACE": "MATCH", "LEFT_IRIS": "NO_MATCH", "RIGHT_IRIS": "MATCH"},
                OTHER: {"FACE": "CANNOT_DETERMINE"}}
    with db.begin() as conn:
        helper(conn, REFID, "ParkingHelperRecord",
               {"allCandidatesPerModalityOverallMatchResult": verdicts})

    result = call("bio_get_parking_match_verdicts", refid=REFID)

    assert result["verdict_counts"] == {"MATCH": 2, "NO_MATCH": 1, "CANNOT_DETERMINE": 1}
    first = result["candidates"][0]
    assert first["candidateRefId"] == CANDIDATE and first["counts"] == {"MATCH": 2, "NO_MATCH": 1}


def test_update_checker_result_counts_matches(db):
    record = {"applicantRefid": REFID, "masterRefid": OTHER, "latestBioUpdateRefid": None,
              "isFlagged": True, "consistencyMasterResult": [
                  {"candidateRefid": CANDIDATE, "matchResults": [
                      {"modality": "FACE", "matched": True, "matchScore": 91},
                      {"modality": "LEFT_IRIS", "matched": False, "errorCode": "E42",
                       "errorMessage": "low quality", "unexpected": "dropped"}]}]}
    with db.begin() as conn:
        helper(conn, REFID, "UpdateCheckerHelperRecord", record)

    result = call("bio_get_update_checker_result", refid=REFID)

    assert result["isFlagged"] is True
    candidate = result["candidates"][0]
    assert candidate["counts"] == {"matched": 1, "not_matched": 1, "with_error": 1, "results": 2}
    assert "unexpected" not in candidate["matchResults"][1]


def test_helper_record_fields_uses_json_extract(db):
    with db.begin() as conn:
        helper(conn, REFID, "ApplicantCandidateHelperRecord", FACTS_RECORD)

    result = call("bio_get_helper_record_fields", refid=REFID,
                  record_type="ApplicantCandidateHelperRecord",
                  json_paths=["$.isFirstTimeBioUpdate",
                              f'$.candidateHelperRecordMap."{CANDIDATE}".maxAbisScore',
                              "$.noSuchField"])

    assert result["fields"]["$.noSuchField"] is None
    assert result["fields"][f'$.candidateHelperRecordMap."{CANDIDATE}".maxAbisScore'] == 9500
    assert result["fields"]["$.isFirstTimeBioUpdate"] in (True, 1)


@pytest.mark.parametrize("path", ["isFlagged", "$.a; DROP TABLE x", "$.a**", "$..a",
                                  "$.a[-1]", "$" + ".a" * 200])
def test_helper_record_fields_refuses_unsupported_paths(db, path):
    result = call("bio_get_helper_record_fields", refid=REFID,
                  record_type="ParkingHelperRecord", json_paths=[path])
    assert isinstance(result, str) and result.startswith("Invalid argument")


def test_helper_record_fields_needs_paths(db):
    result = call("bio_get_helper_record_fields", refid=REFID,
                  record_type="ParkingHelperRecord", json_paths=[])
    assert result.startswith("Invalid argument")


# ======================================================================
# Failures never read like an empty result
# ======================================================================

@pytest.mark.parametrize("name", ["bio_get_packet_stage_summary", "bio_get_parking_status",
                                  "bio_list_helper_records", "bio_get_abis_candidates"])
def test_disabled_lookup_is_an_evidence_gap(db, monkeypatch, name):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    result = call(name, refid=REFID)
    assert isinstance(result, str)
    assert "switched off" in result and "evidence gap" in result


@pytest.mark.parametrize("refid", ["", "   ", "x" * 37])
def test_an_unusable_refid_is_refused_before_any_query(db, refid):
    result = call("bio_get_packet_stage_summary", refid=refid)
    assert isinstance(result, str) and result.startswith("Invalid argument")


def test_a_failing_database_is_an_evidence_gap_not_an_exception(monkeypatch):
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("boom")
    monkeypatch.setattr(_process_db.PROCESS, "get_engine", lambda: broken)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    process_db_breaker.close()
    try:
        result = call("bio_get_parking_status", refid=REFID)
    finally:
        process_db_breaker.close()
    assert result.startswith("The process DB lookup failed (RuntimeError)")
    assert "evidence gap" in result


def test_an_open_breaker_fails_fast_with_a_message(db, monkeypatch):
    def refuse(*_args, **_kwargs):
        raise pybreaker.CircuitBreakerError("open")

    monkeypatch.setattr(_process_db, "query", refuse)
    # The tool modules bound `query` at import; patch where it is used.
    from src.tools.agent_tools.enu_biometric import stage_tracker
    monkeypatch.setattr(stage_tracker, "query", refuse)
    result = call("bio_get_packet_stage_summary", refid=REFID)
    assert "circuit breaker is open" in result


# ======================================================================
# Connection guarantees
# ======================================================================

def test_connections_are_forced_read_only_first():
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    _process_db.PROCESS.on_connect(connection, None)

    statements = [c.args[0] for c in cursor.execute.call_args_list]
    assert statements[0] == "SET SESSION TRANSACTION READ ONLY"
    assert any("max_execution_time" in s for s in statements)


def test_a_missing_statement_timeout_does_not_block_the_connection():
    cursor = MagicMock()
    cursor.execute.side_effect = [None, Exception("unknown variable")]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    _process_db.PROCESS.on_connect(connection, None)


def test_a_read_only_failure_refuses_the_connection():
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("denied")
    connection = MagicMock()
    connection.cursor.return_value = cursor
    with pytest.raises(Exception, match="denied"):
        _process_db.PROCESS.on_connect(connection, None)


def test_engine_escapes_the_password(monkeypatch):
    _process_db.PROCESS.reset_engine()
    monkeypatch.setenv("PROCESS_DB_HOST", "db.internal")
    monkeypatch.setenv("PROCESS_DB_USERNAME", "reader")
    monkeypatch.setenv("PROCESS_DB_PASSWORD", "p@ss:w/rd")
    monkeypatch.delenv("PROCESS_DB_PORT", raising=False)
    monkeypatch.delenv("PROCESS_DB_NAME", raising=False)
    try:
        engine = _process_db.get_engine()
        assert engine.url.host == "db.internal"
        assert engine.url.password == "p@ss:w/rd"
        assert engine.url.port == 6446
        assert engine.url.database == "uidprocessv2_2"
    finally:
        _process_db.PROCESS.reset_engine()


def test_redaction_spares_refids_but_not_pii_beside_them():
    value = {"note": f"candidate {CANDIDATE} matched uid 2345 6789 0123",
             "ids": [CANDIDATE], "uid": 234567890123, "score": 9500}
    redacted = _process_db.redact(value, [REFID])
    assert CANDIDATE in redacted["note"]
    assert "2345 6789 0123" not in redacted["note"]
    assert redacted["ids"] == [CANDIDATE]
    assert redacted["uid"] == "[REDACTED:UID]"
    assert redacted["score"] == 9500


# ======================================================================
# The shared database layer (agent_tools/_database.py, D9)
# ======================================================================

@pytest.fixture
def other_database():
    """A second database key, forgotten afterwards."""
    database = _database.declare("probe_other", label="probe DB")
    yield database
    database.breaker.close()
    _database._databases.pop("probe_other", None)


def test_the_process_database_keeps_its_settings_and_breaker():
    assert _process_db.PROCESS.setting("HOST") == "PROCESS_DB_HOST"
    assert _process_db.PROCESS.breaker is process_db_breaker
    assert _process_db.PROCESS.breaker_name == "process_db_breaker"


def test_another_database_has_its_own_settings_and_breaker(other_database):
    assert other_database.setting("ENABLED") == "AGENT_DB_PROBE_OTHER_ENABLED"
    assert other_database.breaker is not process_db_breaker
    assert _database.breakers()["agent_db_probe_other_breaker"] is other_database.breaker


def test_one_database_is_shared_and_cannot_be_declared_two_ways(other_database):
    assert _database.declare("probe_other", label="probe DB") is other_database
    with pytest.raises(ValueError, match="declared twice"):
        _database.declare("probe_other", label="another label")
    with pytest.raises(ValueError, match="must match"):
        _database.declare("Bad-Key", label="x")


def test_one_databases_outage_opens_only_its_own_breaker(db, other_database, monkeypatch):
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("down")
    monkeypatch.setattr(other_database, "get_engine", lambda: broken)
    monkeypatch.setenv("AGENT_DB_PROBE_OTHER_ENABLED", "true")

    for _ in range(4):
        result = other_database.run_lookup(lambda: {"rows": other_database.query("SELECT 1", {})})
        assert "evidence gap" in result
    assert other_database.breaker.current_state == "open"
    assert "circuit breaker is open" in other_database.run_lookup(
        lambda: {"rows": other_database.query("SELECT 1", {})})

    # The process database still answers.
    assert call("bio_get_packet_stage_summary", refid=REFID)["found"] is False
    assert process_db_breaker.current_state == "closed"


def test_a_database_switched_on_without_its_settings_fails_the_boot(other_database, monkeypatch):
    monkeypatch.setenv("AGENT_DB_PROBE_OTHER_ENABLED", "true")
    monkeypatch.setenv("AGENT_DB_PROBE_OTHER_HOST", "db.internal")
    for name in ("USERNAME", "PASSWORD"):
        monkeypatch.delenv(f"AGENT_DB_PROBE_OTHER_{name}", raising=False)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    for name in ("HOST", "USERNAME", "PASSWORD"):
        monkeypatch.delenv(f"PROCESS_DB_{name}", raising=False)

    errors = _database.validate()

    assert "AGENT_DB_PROBE_OTHER_ENABLED=true requires AGENT_DB_PROBE_OTHER_USERNAME to be set." \
        in errors
    assert "PROCESS_DB_ENABLED=true requires PROCESS_DB_HOST to be set." in errors
    assert not any("AGENT_DB_PROBE_OTHER_HOST" in error for error in errors)


def test_a_database_switched_off_needs_no_settings(other_database, monkeypatch):
    monkeypatch.delenv("AGENT_DB_PROBE_OTHER_ENABLED", raising=False)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert _database.validate() == []


def test_every_databases_breaker_is_sampled(other_database):
    from src.utils import metrics

    if not metrics.METRICS_AVAILABLE:
        pytest.skip("prometheus_client is not installed")
    other_database.breaker.open()
    metrics.sample_breaker_states()
    assert metrics.BREAKER_STATE.labels(breaker="agent_db_probe_other_breaker")._value.get() == 2
    assert metrics.BREAKER_STATE.labels(breaker="process_db_breaker")._value.get() in (0, 1, 2)
