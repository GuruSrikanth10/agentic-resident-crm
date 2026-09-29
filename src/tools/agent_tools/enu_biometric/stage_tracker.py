"""
bio_stage_tracker: where a packet is in the enu-biometric pipeline.

The table is the service's state machine stored as rows, append-only. When
the service sends work out for substage X it inserts an X row IN PROGRESS
(event: the whole packet EventMessage) and an X_RESUBMISSION row IN PROGRESS
(event: the exact request sent, Snappy-compressed and Base64-encoded -- what
an SLA-breach replay re-sends). The response inserts an X row COMPLETED.
After a replay the same set appears again with stage_resubmission_count one
higher. A rejection is a COMPLETED row carrying sub_stage_reject_reason_code,
not a status of its own. `stage` is 'Biometric' on every row, so it is never
used as a filter, and the `event_message` payload is never returned.
"""
from src.tools.agent_tools import agent_tool
from src.tools.agent_tools.enu_biometric._process_db import (
    COMPLETED,
    IN_PROGRESS,
    PROCESS_DB,
    max_rows,
    name_arg,
    normalize_status,
    plain,
    query,
    refid_arg,
    run_lookup,
    seconds_between,
)

#: Retry copies of an outbound substage. They stay IN PROGRESS by design, so
#: they are never reported as stuck.
REQUEST_COPY_SUFFIX = "_RESUBMISSION"

#: Rows the summary reads. Far above any packet's real history; a packet past
#: it is reported as truncated.
SUMMARY_MAX_ROWS = 1000

#: Optional columns left out of a timeline row when null, to keep a long
#: timeline inside one tool result.
_OMIT_WHEN_NULL = ("sub_stage_status_date", "sub_stage_reason_code",
                   "sub_stage_reject_reason_code")


@agent_tool(PROCESS_DB)
def bio_get_packet_stage_summary(refid: str) -> str:
    """Summarise where a packet is in the enu-biometric pipeline, from bio_stage_tracker.

    Use this first for: which substage the packet reached, whether a substage
    is stuck, how long a step took, whether it was retried, and at which
    substage a reject reason code was recorded.

    Per substage and attempt (stage_resubmission_count, 0 = first attempt) it
    gives when the substage went IN PROGRESS and COMPLETED and the seconds
    between them. `open_substages` lists substages whose latest attempt is IN
    PROGRESS with no COMPLETED row: in flight or stuck -- compare the time with
    the failure. An earlier attempt with no COMPLETED row was superseded by an
    SLA-breach replay. `<NAME>_RESUBMISSION` substages are stored request copies
    that stay IN PROGRESS by design; they are listed apart and never counted as
    open. A rejection shows as a COMPLETED row with a reject reason code, not
    as a status. Rows are grouped by writer (created_by), because the canary
    pipeline writes as Biometric_Canary_Mode. The final approve/reject verdict
    is not in this table.

    refid: the packet's refId (packetMetaData.refId).
    """
    # The open substages and reject codes are listed on their own, so when a
    # long history has to shrink it is the oldest substages that go.
    return run_lookup(lambda: _summary(refid),
                      list_key="substages", keep_from_end=True)


@agent_tool(PROCESS_DB)
def bio_get_packet_stage_timeline(refid: str, sub_stage: str = "") -> str:
    """List a packet's bio_stage_tracker rows in time order, oldest first.

    Use it when the summary is not enough and you need the rows themselves.
    Each row is one event: a substage going IN PROGRESS or COMPLETED, with
    stage_resubmission_count (0 = first attempt), markfor_stage_resubmission
    (1 = a retry), sub_stage_reject_reason_code and created_by
    (Biometric_Canary_Mode = written by the canary pipeline). Null reason codes
    and status dates are left out of a row. The stored event payload is not
    returned. When there are more rows than the limit, the newest are returned
    and `truncated` is true.

    refid: the packet's refId. sub_stage: optional exact substage name to
    return only its rows, e.g. ABIS_DEDUP, ABIS_UPDATE, SDK_CROSS_MATCH,
    UPDATE_CHECKER, MDD_POLICY_BATCH_1, BIO_CANDIDATE_PARKING,
    BIO_CANDIDATE_DEPARTING; leave empty for every substage.
    """
    return run_lookup(lambda: _timeline(refid, sub_stage),
                      list_key="rows", keep_from_end=True)


def _timeline(refid, sub_stage) -> dict:
    refid = refid_arg(refid)
    sub_stage = name_arg("sub_stage", sub_stage)
    limit = max_rows()

    sql = ("SELECT stage_tracker_key, sid, sid_date, srn_num, priority, enrl_type, "
           "sub_stage, sub_stage_status, sub_stage_status_date, "
           "sub_stage_reason_code, sub_stage_reject_reason_code, "
           "markfor_stage_resubmission, stage_resubmission_count, "
           "created_by, creation_date "
           "FROM bio_stage_tracker WHERE refid = :refid")
    params = {"refid": refid, "limit": limit + 1}
    if sub_stage is not None:
        sql += " AND sub_stage = :sub_stage"
        params["sub_stage"] = sub_stage
    # Newest first, so the limit keeps the recent rows; one extra row tells a
    # complete result from a cut one.
    sql += " ORDER BY creation_date DESC, stage_tracker_key DESC LIMIT :limit"

    rows = query(sql, params)
    truncated = len(rows) > limit
    rows = rows[:limit]
    result: dict = {"refid": refid, "found": bool(rows)}
    if sub_stage is not None:
        result["sub_stage"] = sub_stage
    if not rows:
        result["rows"] = []
        return result

    newest = rows[0]
    result["packet"] = {column: plain(newest.get(column))
                        for column in ("sid", "sid_date", "srn_num", "priority", "enrl_type")}
    result["row_count"] = len(rows)
    result["truncated"] = truncated
    result["rows"] = [_timeline_row(row) for row in reversed(rows)]
    return result


def _timeline_row(row: dict) -> dict:
    shaped = {}
    for column in ("stage_tracker_key", "sub_stage", "sub_stage_status",
                   "sub_stage_status_date", "sub_stage_reject_reason_code",
                   "sub_stage_reason_code", "stage_resubmission_count",
                   "markfor_stage_resubmission", "created_by", "creation_date"):
        value = plain(row.get(column))
        if value is None and column in _OMIT_WHEN_NULL:
            continue
        shaped[column] = value
    return shaped


def _summary(refid) -> dict:
    refid = refid_arg(refid)
    rows = query(
        "SELECT stage_tracker_key, sub_stage, sub_stage_status, "
        "sub_stage_reject_reason_code, markfor_stage_resubmission, "
        "stage_resubmission_count, created_by, creation_date "
        "FROM bio_stage_tracker WHERE refid = :refid "
        "ORDER BY creation_date DESC, stage_tracker_key DESC LIMIT :limit",
        {"refid": refid, "limit": SUMMARY_MAX_ROWS + 1})
    truncated = len(rows) > SUMMARY_MAX_ROWS
    rows = list(reversed(rows[:SUMMARY_MAX_ROWS]))
    if not rows:
        return {"refid": refid, "found": False, "substages": []}

    # (writer, substage) -> attempt -> what happened, in first-seen order.
    groups: dict = {}
    for row in rows:
        key = (row.get("created_by") or "unknown", row.get("sub_stage") or "unknown")
        attempt = _attempt_number(row.get("stage_resubmission_count"))
        slot = groups.setdefault(key, {}).setdefault(attempt, {
            "in_progress": [], "completed": [], "other": [],
            "reject_reason_code": None, "retry_flag": 0,
        })
        created = plain(row.get("creation_date"))
        status = normalize_status(row.get("sub_stage_status"))
        if status == IN_PROGRESS:
            slot["in_progress"].append(created)
        elif status == COMPLETED:
            slot["completed"].append(created)
        else:
            slot["other"].append({"sub_stage_status": row.get("sub_stage_status"),
                                  "creation_date": created})
        reject = row.get("sub_stage_reject_reason_code")
        if reject and (slot["reject_reason_code"] is None or status == COMPLETED):
            slot["reject_reason_code"] = reject
        if row.get("markfor_stage_resubmission"):
            slot["retry_flag"] = 1

    substages, request_copies, open_substages, rejections = [], [], [], []
    for (writer, sub_stage), attempts in groups.items():
        latest = max(attempts)
        if sub_stage.endswith(REQUEST_COPY_SUFFIX):
            request_copies.append({"sub_stage": sub_stage, "created_by": writer,
                                   "attempts": sorted(attempts)})
            continue

        described = []
        for number in sorted(attempts):
            slot = attempts[number]
            state = _attempt_state(slot)
            if state == "in_progress" and number < latest:
                state = "superseded_by_replay"
            entry = {"attempt": number, "state": state}
            if slot["in_progress"]:
                entry["in_progress_at"] = slot["in_progress"][0]
            if slot["completed"]:
                entry["completed_at"] = slot["completed"][0]
            if slot["in_progress"] and slot["completed"]:
                entry["duration_seconds"] = seconds_between(
                    slot["in_progress"][0], slot["completed"][0])
            if len(slot["in_progress"]) > 1 or len(slot["completed"]) > 1:
                entry["row_counts"] = {"in_progress": len(slot["in_progress"]),
                                       "completed": len(slot["completed"])}
            if slot["other"]:
                entry["other_statuses"] = slot["other"]
            if slot["retry_flag"]:
                entry["markfor_stage_resubmission"] = 1
            if slot["reject_reason_code"]:
                entry["sub_stage_reject_reason_code"] = slot["reject_reason_code"]
                rejections.append({"sub_stage": sub_stage, "created_by": writer,
                                   "attempt": number,
                                   "sub_stage_reject_reason_code": slot["reject_reason_code"],
                                   "at": slot["completed"][0] if slot["completed"]
                                   else (slot["in_progress"] or [None])[0]})
            described.append(entry)

        latest_state = described[-1]["state"]
        if latest_state == "in_progress":
            open_substages.append({"sub_stage": sub_stage, "created_by": writer,
                                   "attempt": latest,
                                   "in_progress_since": described[-1].get("in_progress_at")})
        substages.append({"sub_stage": sub_stage, "created_by": writer,
                          "latest_attempt": latest, "latest_state": latest_state,
                          "attempts": described})

    newest = rows[-1]
    return {
        "refid": refid,
        "found": True,
        "rows_read": len(rows),
        "truncated": truncated,
        "last_row": {"sub_stage": newest.get("sub_stage"),
                     "sub_stage_status": newest.get("sub_stage_status"),
                     "created_by": newest.get("created_by"),
                     "creation_date": plain(newest.get("creation_date"))},
        "open_substages": open_substages,
        "reject_reason_codes": rejections,
        "request_copies": request_copies,
        "substages": substages,
    }


def _attempt_number(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _attempt_state(slot: dict) -> str:
    if slot["completed"]:
        return "completed"
    if slot["in_progress"]:
        return "in_progress"
    return "other"
