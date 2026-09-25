"""
Read-only process DB lookups (src/tools/process_db.py).

Exercised against SQLite in memory with the three tables' columns; the
MySQL-only parts (the READ ONLY session hook, URL escaping) are tested on
their own.
"""
import json
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.tools import process_db
from src.utils.resilience import process_db_breaker

REFID = "3f2b9c1e-0d4a-4a8e-9b1f-6c7d8e9f0a1b"
OTHER_REFID = "aaaaaaaa-0000-0000-0000-000000000000"
SID = "1234567890123456789012345678"


@pytest.fixture
def engine(monkeypatch):
    eng = create_engine("sqlite://", poolclass=StaticPool,
                        connect_args={"check_same_thread": False})
    with eng.begin() as conn:
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

    monkeypatch.setattr(process_db, "get_engine", lambda: eng)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    monkeypatch.delenv("PROCESS_DB_MAX_ROWS", raising=False)
    monkeypatch.delenv("PROCESS_DB_MAX_FIELD_CHARS", raising=False)
    process_db_breaker.close()
    yield eng
    process_db_breaker.close()


def _stage_row(conn, key, refid, sub_stage, status, uid=None, message=None):
    conn.execute(text("""
        INSERT INTO bio_stage_tracker (stage_tracker_key, uid, refid, sid,
            sid_date, priority, enrl_type, stage, sub_stage, sub_stage_status,
            event_message, creation_date, last_updated_date, integrity)
        VALUES (:key, :uid, :refid, :sid, '2026-09-01 10:00:00', 9, 'U',
            'BIO_DEDUP', :sub_stage, :status, :message,
            '2026-09-01 10:00:00', '2026-09-01 10:05:00', 'checksum')"""),
        {"key": key, "uid": uid, "refid": refid, "sid": SID,
         "sub_stage": sub_stage, "status": status, "message": message})


def test_stage_tracker_rows_are_oldest_first_without_uid_or_integrity(engine):
    with engine.begin() as conn:
        _stage_row(conn, 1, REFID, "ABIS_INSERT", "COMPLETED", uid="enc-uid")
        _stage_row(conn, 2, REFID, "ABIS_DEDUP", "IN_PROGRESS")
        _stage_row(conn, 3, OTHER_REFID, "ABIS_DEDUP", "FAILED")

    result = process_db.fetch_stage_tracker(REFID)

    assert result["table"] == "bio_stage_tracker"
    assert result["row_count"] == 2
    assert result["truncated"] is False
    assert [r["sub_stage"] for r in result["rows"]] == ["ABIS_INSERT", "ABIS_DEDUP"]
    first = result["rows"][0]
    assert "uid" not in first and "integrity" not in first
    assert first["uid_present"] is True
    assert result["rows"][1]["uid_present"] is False


def test_row_cap_keeps_the_newest_and_says_so(engine, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_MAX_ROWS", "2")
    with engine.begin() as conn:
        for key, sub in ((1, "A"), (2, "B"), (3, "C")):
            _stage_row(conn, key, REFID, sub, "COMPLETED")

    result = process_db.fetch_stage_tracker(REFID)

    assert result["truncated"] is True
    assert [r["sub_stage"] for r in result["rows"]] == ["B", "C"]


def test_helper_record_is_parsed_redacted_and_filtered(engine):
    record = {"refId": REFID, "sid": SID, "residentUid": "234567890123",
              "decision": "MATCH"}
    with engine.begin() as conn:
        for key, record_type in ((1, "UPDATE_CHECKER"), (2, "PARKING_HELPER")):
            conn.execute(text("""
                INSERT INTO bio_helper_cache_store (helper_record_key, refid,
                    sid, record_type, helper_record, creation_date,
                    last_updated_date)
                VALUES (:key, :refid, :sid, :rt, :hr, '2026-09-01', '2026-09-01')"""),
                {"key": key, "refid": REFID, "sid": SID, "rt": record_type,
                 "hr": json.dumps(record)})

    result = process_db.fetch_helper_cache(REFID, "UPDATE_CHECKER")

    assert result["record_type"] == "UPDATE_CHECKER"
    assert result["row_count"] == 1
    helper = result["rows"][0]["helper_record"]
    assert isinstance(helper, dict)
    assert helper["residentUid"] == "[REDACTED:AADHAAR]"
    # Correlation ids survive redaction.
    assert helper["refId"] == REFID and helper["sid"] == SID

    assert process_db.fetch_helper_cache(REFID)["row_count"] == 2


def test_oversized_blob_is_cut_and_left_as_text(engine, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_MAX_FIELD_CHARS", "20")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bio_parking_queue_store (parking_queue_key, refid,
                record_category, event_json, creation_date, last_updated_date,
                is_parked)
            VALUES (1, :refid, 'APPLICANT', :ej, '2026-09-01', '2026-09-01', 1)"""),
            {"refid": REFID, "ej": json.dumps({"payload": "x" * 100})})

    row = process_db.fetch_parking_queue(REFID)["rows"][0]

    assert isinstance(row["event_json"], str)
    assert row["event_json"].endswith("chars]")
    assert "[truncated" in row["event_json"]
    assert row["is_parked"] == 1


def test_tool_reports_an_empty_table_as_zero_rows(engine):
    result = json.loads(process_db.lookup_bio_parking_queue.invoke({"refid": REFID}))
    assert result["row_count"] == 0
    assert result["rows"] == []


def test_disabled_lookup_does_not_read_like_an_empty_result(engine, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")

    with pytest.raises(process_db.ProcessDbDisabled):
        process_db.fetch_stage_tracker(REFID)
    message = process_db.lookup_bio_stage_tracker.invoke({"refid": REFID})
    assert "disabled" in message and "row_count" not in message


@pytest.mark.parametrize("refid", ["", "   ", "x" * 37])
def test_unusable_refid_is_refused_before_the_query(engine, refid):
    with pytest.raises(ValueError):
        process_db.fetch_stage_tracker(refid)
    assert "Invalid argument" in process_db.lookup_bio_stage_tracker.invoke({"refid": refid})


def test_query_failure_becomes_a_message_not_an_exception(monkeypatch):
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("boom")
    monkeypatch.setattr(process_db, "get_engine", lambda: broken)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    process_db_breaker.close()
    try:
        message = process_db.lookup_bio_helper_cache.invoke({"refid": REFID})
    finally:
        process_db_breaker.close()

    assert message.startswith("Failed to query bio_helper_cache_store")
    assert "No rows were read" in message


def test_connections_are_forced_read_only_first():
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    process_db._on_connect(connection, None)

    statements = [c.args[0] for c in cursor.execute.call_args_list]
    assert statements[0] == "SET SESSION TRANSACTION READ ONLY"
    assert any("max_execution_time" in s for s in statements)


def test_missing_statement_timeout_does_not_block_the_connection():
    cursor = MagicMock()
    cursor.execute.side_effect = [None, Exception("unknown variable")]
    connection = MagicMock()
    connection.cursor.return_value = cursor

    process_db._on_connect(connection, None)  # does not raise


def test_read_only_failure_refuses_the_connection():
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("denied")
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with pytest.raises(Exception, match="denied"):
        process_db._on_connect(connection, None)


def test_engine_escapes_the_password(monkeypatch):
    monkeypatch.setattr(process_db, "_ENGINE", None)
    monkeypatch.setenv("PROCESS_DB_HOST", "db.internal")
    monkeypatch.setenv("PROCESS_DB_USERNAME", "reader")
    monkeypatch.setenv("PROCESS_DB_PASSWORD", "p@ss:w/rd")
    monkeypatch.delenv("PROCESS_DB_PORT", raising=False)
    monkeypatch.delenv("PROCESS_DB_NAME", raising=False)
    try:
        eng = process_db.get_engine()
        assert eng.url.host == "db.internal"
        assert eng.url.password == "p@ss:w/rd"
        assert eng.url.port == 6446
        assert eng.url.database == "uidprocessv2_2"
        eng.dispose()
    finally:
        monkeypatch.setattr(process_db, "_ENGINE", None)


def test_cli_exit_codes(engine, monkeypatch, capsys):
    assert process_db.main(["--refid", REFID]) == 2
    with engine.begin() as conn:
        _stage_row(conn, 1, REFID, "ABIS_DEDUP", "IN_PROGRESS")
    assert process_db.main(["--refid", REFID, "--table", "stage"]) == 0
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert process_db.main(["--refid", REFID]) == 1


def test_tools_are_registered():
    from src.tools.tool_registry import get_tool_by_name

    for name in ("lookup_bio_stage_tracker", "lookup_bio_helper_cache",
                 "lookup_bio_parking_queue"):
        assert get_tool_by_name(name).name == name
