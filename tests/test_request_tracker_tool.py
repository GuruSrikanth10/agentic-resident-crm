"""
The request_tracker tool (src/tools/agent_tools/common/request_tracker.py):
RSMS's pipeline-wide progress row, read from the process database.

Run against SQLite in memory with the table's DDL columns. The database
layer's own guarantees are tested in test_process_db_tools.py.
"""
import json

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.tools import agent_tools
from src.tools.agent_tools.common import request_tracker
from src.tools.agent_tools.enu_biometric import _process_db
from src.utils.resilience import process_db_breaker

REFID = "3f2b9c1e-0d4a-4a8e-9b1f-6c7d8e9f0a1b"
OTHER = "aaaaaaaa-0000-0000-0000-000000000000"
SID = "0000002120694020260926203812"

TOOL = "get_request_tracker_status"

#: Every column of the DDL, so a statement naming one that does not exist fails.
COLUMNS = """
    tracker_key INTEGER PRIMARY KEY, uid TEXT, refid TEXT UNIQUE,
    sid TEXT NOT NULL UNIQUE, sid_date TEXT NOT NULL, srn_num TEXT,
    grievance_id TEXT, priority INTEGER NOT NULL DEFAULT 999, type TEXT,
    sub_type TEXT, print_type TEXT, print_sub_type TEXT, mode TEXT, stage TEXT,
    stage_status TEXT, stage_status_date TEXT, stage_reason_code TEXT,
    stage_reject_reason_code TEXT, print_status TEXT, dispatch_date TEXT,
    awb_number TEXT, payment_reference_number TEXT, payment_status TEXT,
    payment_date TEXT, payment_invoice_id TEXT, submission_count INTEGER,
    operator_id TEXT, supervisor_id TEXT, upload_time TEXT, update_ind TEXT,
    reject_ind TEXT, auth_extraction_status TEXT,
    auth_extraction_status_date TEXT, is_child TEXT, source TEXT,
    created_by TEXT, creation_date TEXT, last_updated_by TEXT,
    last_updated_date TEXT, appointment_id TEXT, appointment_date TEXT,
    appointment_time_slot TEXT, enrolment_centre TEXT, is_resumable INTEGER,
    can_cancel INTEGER, color_code TEXT, mobile_number TEXT,
    consent_reference_id TEXT, email_id TEXT, gps_longitude REAL,
    gps_latitude REAL, machine_code TEXT, station_id TEXT, agency_code TEXT,
    registrar_id TEXT, milestone TEXT, client_version TEXT,
    resident_name TEXT, hof_status TEXT, date_of_approval_introducer TEXT,
    date_of_approval_hof TEXT, hof_uid TEXT, introducer_uid TEXT,
    pin_number TEXT, disposal_date_of_hof TEXT,
    disposal_date_of_introducer TEXT, packet_url TEXT, hof_relation_type TEXT,
    is_hof INTEGER DEFAULT 0, is_introducer INTEGER DEFAULT 0,
    is_assisted INTEGER DEFAULT 0, paid_amount REAL DEFAULT 0
"""

#: The resident's details: none may appear in a result.
PERSONAL = {
    "uid": "234567890123", "resident_name": "Asha Kumari",
    "mobile_number": "9876543210", "email_id": "asha@example.com",
    "pin_number": "560001", "hof_uid": "345678901234",
    "introducer_uid": "456789012345", "gps_latitude": 12.9716,
    "gps_longitude": 77.5946, "operator_id": "OPERATOR-7",
    "payment_reference_number": "PAYREF-42", "awb_number": "AWB-99",
}


@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE request_tracker ({COLUMNS})"))
    monkeypatch.setattr(_process_db.PROCESS, "get_engine", lambda: engine)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    process_db_breaker.close()
    yield engine
    process_db_breaker.close()


def call(**arguments):
    output = agent_tools.get_tool(TOOL).invoke(arguments)
    try:
        return json.loads(output)
    except ValueError:
        return output


def tracker(conn, stage, status, *, refid=REFID, sid=SID, reason=None, reject=None,
            at="2026-09-01 10:00:00", **columns):
    values = {"refid": refid, "sid": sid, "sid_date": "2026-09-01 09:00:00",
              "srn_num": "SRN1", "priority": 9, "type": "U", "sub_type": "BIO",
              "mode": "ONLINE", "stage": stage, "stage_status": status,
              "stage_status_date": at, "stage_reason_code": reason,
              "stage_reject_reason_code": reject, "submission_count": 1,
              "created_by": "rsms", "creation_date": "2026-09-01 09:00:00",
              "last_updated_by": "enu-biometric", "last_updated_date": at,
              **PERSONAL, **columns}
    names = ", ".join(values)
    conn.execute(text(f"INSERT INTO request_tracker ({names}) VALUES "
                      f"({', '.join(':' + name for name in values)})"), values)


# ======================================================================
# Scope and declaration
# ======================================================================

def test_it_reads_the_process_database_enu_biometric_reads():
    """One database, one pool, one breaker, one switch."""
    assert request_tracker.PROCESS is _process_db.PROCESS
    assert request_tracker.PROCESS.breaker is process_db_breaker


def test_it_is_for_every_services_investigator_only():
    toolset = request_tracker.REQUEST_TRACKER
    assert toolset.services == ("*",)
    assert toolset.agents == ("investigator",)
    assert toolset.read_only is True


# ======================================================================
# Reading the row
# ======================================================================

def test_a_packet_in_process_at_a_stage(db):
    with db.begin() as conn:
        tracker(conn, "Biometric", "InProcess")
        tracker(conn, "QC", "Completed", refid=OTHER, sid="S-OTHER",
                reject="SOMEONE_ELSES_CODE")

    result = call(refid=REFID)

    assert result["found"] is True
    assert result["stage"] == "Biometric"
    assert result["reading"]["state"] == "in_process"
    assert result["reading"]["reason_code_recorded"] is False
    assert "stuck" in result["reading"]["summary"]
    position = result["stage_position"]
    assert (position["path"], position["index"], position["of"]) == ("main", 10, 13)
    assert position["earlier_stages"][0] == "EnS"
    assert position["earlier_stages"][-1] == "Stateportal"
    assert result["seconds_since_stage_status_date"] > 0
    assert result["packet"]["sid"] == SID and result["packet"]["type"] == "U"


def test_completed_with_a_reject_code_is_a_rejection_at_that_stage(db):
    with db.begin() as conn:
        tracker(conn, "Biometric", "Completed",
                reject="RESIDENT_MAN_DEDUPE_REJECT_DEMO__E")

    result = call(refid=REFID)

    assert result["stage_reject_reason_code"] == "RESIDENT_MAN_DEDUPE_REJECT_DEMO__E"
    assert result["reading"]["state"] == "completed"
    assert result["reading"]["reason_code_recorded"] is True
    assert "rejected at Biometric" in result["reading"]["summary"]


def test_a_code_in_stage_reason_code_counts_too(db):
    """Which column RSMS stores stageReasonCode in is not documented."""
    with db.begin() as conn:
        tracker(conn, "Demographic", "Completed", reason="DEMO_REJECT_CODE")
    result = call(refid=REFID)
    assert result["stage_reason_code"] == "DEMO_REJECT_CODE"
    assert result["reading"]["reason_code_recorded"] is True
    assert "rejected at Demographic" in result["reading"]["summary"]


def test_completed_without_a_code_passed_the_stage(db):
    with db.begin() as conn:
        tracker(conn, "QC", "Completed")
    reading = call(refid=REFID)["reading"]
    assert reading["state"] == "completed"
    assert reading["reason_code_recorded"] is False
    assert "passed QC" in reading["summary"]


def test_reject_interceptor_means_an_earlier_stage_rejected_it(db):
    with db.begin() as conn:
        tracker(conn, "RejectInterceptor", "InProcess")
    result = call(refid=REFID)
    assert result["reading"]["on_reject_path"] is True
    assert "reject path" in result["reading"]["summary"]
    assert result["stage_position"]["path"] == "reject"
    assert "earlier_stages" not in result["stage_position"]


def test_post_enrolment_alone_does_not_say_approved(db):
    with db.begin() as conn:
        tracker(conn, "PostEnrolment", "Completed")
    summary = call(refid=REFID)["reading"]["summary"]
    assert "does not say whether the packet was approved" in summary


@pytest.mark.parametrize("stored", ["IN_PROCESS", "in process", "INPROCESS"])
def test_status_constants_are_matched_whatever_their_spelling(db, stored):
    with db.begin() as conn:
        tracker(conn, "MetaData", stored)
    result = call(refid=REFID)
    assert result["stage_status"] == stored
    assert result["reading"]["state"] == "in_process"


def test_stage_names_are_matched_whatever_their_casing(db):
    with db.begin() as conn:
        tracker(conn, "METADATA", "Completed")
    result = call(refid=REFID)
    assert result["stage"] == "METADATA"
    assert result["stage_position"]["stage"] == "MetaData"


def test_an_unknown_status_or_stage_is_reported_not_guessed(db):
    with db.begin() as conn:
        tracker(conn, "SomeNewStage", "Parked")
    result = call(refid=REFID)
    assert result["reading"]["state"] == "unrecognised_status"
    assert result["stage_position"] == {"known_stage": False}


def test_the_redrive_stage_is_known(db):
    with db.begin() as conn:
        tracker(conn, "Ois", "InProcess")
    assert call(refid=REFID)["stage_position"]["path"] == "redrive"


# ======================================================================
# Lookup
# ======================================================================

def test_lookup_by_sid(db):
    with db.begin() as conn:
        tracker(conn, "EnS", "InProcess")
    result = call(sid=SID)
    assert result["lookup"] == {"sid": SID}
    assert result["packet"]["refid"] == REFID


def test_refid_wins_and_a_given_sid_is_checked_against_the_row(db):
    with db.begin() as conn:
        tracker(conn, "EnS", "InProcess")
    assert call(refid=REFID, sid=SID)["sid_matches"] is True
    result = call(refid=REFID, sid="NOT-THIS-SID")
    assert result["lookup"] == {"refid": REFID}
    assert result["sid_matches"] is False


def test_an_unknown_packet_says_not_found(db):
    assert call(refid=OTHER) == {"lookup": {"refid": OTHER}, "found": False}


@pytest.mark.parametrize("arguments", [{}, {"refid": "  ", "sid": ""},
                                       {"refid": "x" * 37}, {"sid": "s" * 41}])
def test_an_unusable_key_is_refused_before_any_query(db, arguments):
    result = call(**arguments)
    assert isinstance(result, str) and result.startswith("Invalid argument")


def test_disabled_lookup_is_an_evidence_gap(db, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    result = call(refid=REFID)
    assert isinstance(result, str)
    assert "switched off" in result and "evidence gap" in result


# ======================================================================
# Privacy
# ======================================================================

def test_the_residents_details_are_never_returned(db):
    with db.begin() as conn:
        tracker(conn, "Biometric", "InProcess")

    result = call(refid=REFID)
    textual = json.dumps(result)

    for column, value in PERSONAL.items():
        assert f'"{column}"' not in textual, column
        assert str(value) not in textual, column
    assert result["uid_assigned"] is True


def test_no_uid_reads_as_not_assigned(db):
    with db.begin() as conn:
        tracker(conn, "Enrolment", "InProcess", uid=None)
    assert call(refid=REFID)["uid_assigned"] is False
