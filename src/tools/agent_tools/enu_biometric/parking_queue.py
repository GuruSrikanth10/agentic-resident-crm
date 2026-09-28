"""
bio_parking_queue_store: biometric dedup's waiting room.

When an applicant's ABIS-matched candidates are themselves still being
enrolled (no UID master record yet), the applicant is parked until they
finish. One row per (refid, record_category): APPLICANT means "this applicant
waits on these candidates", CANDIDATE means "these applicants wait on this
candidate". event_json holds {"parkingStatusMap": {refId: InProcess |
Completed | Rejected}, "sendToPortalDueToBMSError": bool}.

Redis holds the working copy and every change is also written here, but only
the Redis copy is deleted on cleanup: a row, or is_parked = 1, does not mean
the packet is parked now. The service's own rule is that it is parked only
while its map still has InProcess entries and bio_stage_tracker has no
BIO_CANDIDATE_DEPARTING COMPLETED row, which is what `parked_now` applies.
The docs disagree on record_category's casing, so it is matched
case-insensitively.
"""
from src.tools.agent_tools import agent_tool
from src.tools.agent_tools.enu_biometric._process_db import (
    COMPLETED,
    PROCESS_DB,
    field,
    normalize_status,
    parse_json,
    plain,
    query,
    redact,
    refid_arg,
    run_lookup,
)

#: Refs listed per status before the rest are only counted.
MAX_REFS_PER_STATUS = 25


@agent_tool(PROCESS_DB)
def bio_get_parking_status(refid: str) -> str:
    """Whether a packet is parked in biometric dedup, what it waits on, and who waits on it.

    An applicant is parked when ABIS matched candidates that are still being
    enrolled. `as_applicant` gives the candidates this packet waits on, grouped
    by status (InProcess, Completed, Rejected); `as_candidate` gives the
    applicants waiting on this packet. `parked_now` applies the service's rule:
    parked only while the APPLICANT map still has InProcess entries and
    bio_stage_tracker has no BIO_CANDIDATE_DEPARTING COMPLETED row;
    `parked_now_basis` says which fact decided it. Rows are not deleted after
    unparking and is_parked is never cleared, so a row alone does not mean the
    packet is parked now, and only the current map is kept (no history).
    send_to_portal_due_to_bms_error true means the matching service (BMS)
    failed, so the applicant goes to the manual portal instead of the rules.

    refid: the refId to look up -- an applicant's, or a candidate's to see who
    waits on it.
    """
    return run_lookup(lambda: _parking_status(refid))


def _parking_status(refid) -> dict:
    refid = refid_arg(refid)
    rows = query(
        "SELECT parking_queue_key, record_category, event_json, is_parked, "
        "creation_date, last_updated_date "
        "FROM bio_parking_queue_store WHERE refid = :refid "
        "ORDER BY parking_queue_key LIMIT 10",
        {"refid": refid})
    stage_rows = query(
        "SELECT sub_stage, sub_stage_status, stage_resubmission_count, creation_date "
        "FROM bio_stage_tracker WHERE refid = :refid "
        "AND sub_stage IN ('BIO_CANDIDATE_PARKING', 'BIO_CANDIDATE_DEPARTING') "
        "ORDER BY creation_date, stage_tracker_key LIMIT 50",
        {"refid": refid})

    by_category: dict = {}
    for row in rows:
        category = str(row.get("record_category") or "").strip().upper() or "UNKNOWN"
        by_category.setdefault(category, []).append(row)

    allowlist = [refid]
    applicant = _role_view(by_category.get("APPLICANT"), "waiting_on", allowlist)
    candidate = _role_view(by_category.get("CANDIDATE"), "waiting_on_this", allowlist)
    others = sorted(category for category in by_category
                    if category not in ("APPLICANT", "CANDIDATE"))

    departing_completed = any(
        row.get("sub_stage") == "BIO_CANDIDATE_DEPARTING"
        and normalize_status(row.get("sub_stage_status")) == COMPLETED
        for row in stage_rows)
    in_process = applicant["in_process_count"] if applicant["found"] else 0

    if not applicant["found"]:
        parked_now = False
        basis = "No APPLICANT row: this packet was never parked as an applicant."
    elif departing_completed:
        parked_now = False
        basis = ("bio_stage_tracker has a BIO_CANDIDATE_DEPARTING COMPLETED row: "
                 "the applicant has left parking.")
    elif in_process == 0:
        parked_now = False
        basis = "The APPLICANT map has no InProcess candidate left."
    else:
        parked_now = True
        basis = (f"The APPLICANT map still has {in_process} InProcess candidate(s) "
                 f"and bio_stage_tracker has no BIO_CANDIDATE_DEPARTING COMPLETED row.")

    result: dict = {
        "refid": refid,
        "found": bool(rows),
        "parked_now": parked_now,
        "parked_now_basis": basis,
        "as_applicant": applicant,
        "as_candidate": candidate,
        "parking_stage_rows": [
            {"sub_stage": row.get("sub_stage"),
             "sub_stage_status": row.get("sub_stage_status"),
             "stage_resubmission_count": plain(row.get("stage_resubmission_count")),
             "creation_date": plain(row.get("creation_date"))}
            for row in stage_rows],
    }
    if others:
        result["other_record_categories"] = others
    return result


def _role_view(rows, list_name: str, allowlist: list) -> dict:
    """One record_category's row, summarised. The newest row wins if the
    category somehow has more than one."""
    if not rows:
        return {"found": False}
    row = rows[-1]
    document = parse_json(row.get("event_json"))
    status_map = field(document, "parkingStatusMap")

    view: dict = {
        "found": True,
        "record_category": row.get("record_category"),
        "is_parked": plain(row.get("is_parked")),
        "send_to_portal_due_to_bms_error": field(document, "sendToPortalDueToBMSError"),
        "creation_date": plain(row.get("creation_date")),
        "last_updated_date": plain(row.get("last_updated_date")),
    }
    if len(rows) > 1:
        view["rows_for_category"] = len(rows)
    if not isinstance(status_map, dict):
        view["shape_recognized"] = False
        view["event_json_keys"] = sorted(document) if isinstance(document, dict) else None
        view["in_process_count"] = 0
        return view

    grouped: dict = {}
    for ref, status in status_map.items():
        grouped.setdefault(str(status), []).append(str(ref))
    view["counts"] = {status: len(refs) for status, refs in grouped.items()}
    view[list_name] = redact(
        {status: sorted(refs)[:MAX_REFS_PER_STATUS] for status, refs in grouped.items()},
        allowlist)
    # Counted case- and separator-insensitively; the doc spells it InProcess.
    view["in_process_count"] = sum(
        len(refs) for status, refs in grouped.items()
        if status.replace("_", "").replace(" ", "").lower() == "inprocess")
    return view
