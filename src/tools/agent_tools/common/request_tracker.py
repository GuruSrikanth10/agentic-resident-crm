"""
request_tracker: where a packet is in the ENU pipeline, across every service.

RSMS (Request State Management Service) keeps it. Every ENU stage service
reports to RSMS when a packet enters its stage (InProcess) and when it leaves
(Completed, with a reason code when the stage rejected the packet), and RSMS
refuses an out-of-order transition (error 20001), so the row follows a
checked state machine. `sid` and `refid` are each unique: the table keeps one
row per packet, overwritten as the packet moves on, so it says where the
packet is now, not where it has been.

The table is pipeline-wide, so the tool is for every service's packets
(`services=("*",)`) and its name carries no service prefix. It lives in
uidprocessv2_2, the database enu-biometric's tools read: `declare` returns
that same `process` database -- one pool, one breaker, one PROCESS_DB_ENABLED
switch -- and refuses a declaration that disagrees with theirs.

Most of the row is the resident's (name, mobile, email, PIN, GPS, HOF and
introducer UIDs, payment and appointment details). None of it is selected:
the statement names only stage, status and packet-identity columns, and the
UID comes back as whether one is assigned.
"""
from src.tools.agent_tools import Toolset, agent_tool
from src.tools.agent_tools._database import (
    REFID_MAX,
    InvalidArgument,
    declare,
    name_arg,
    plain,
    seconds_between,
)

#: The same database, with the same settings, as enu_biometric/_process_db.py.
PROCESS = declare("process", label="process DB", default_port=6446,
                  default_name="uidprocessv2_2")

SID_MAX = 40

IN_PROCESS = "InProcess"
COMPLETED = "Completed"

#: The success path, in enu-esb's routing order (stageworkflow.yaml).
MAIN_PATH = ("EnS", "Structural", "FlowControl", "BioFraud", "MetaData",
             "Demographic", "ExtIntService", "QC", "Stateportal", "Biometric",
             "Enrolment", "SuccessInterceptor", "PostEnrolment")

#: Where a rejected packet goes instead.
REJECT_PATH = ("RejectInterceptor", "PostEnrolment")

#: Written by enu-esb when it re-drives a packet from the dead-letter topic.
REDRIVE_STAGE = "Ois"

#: Canonical stage names by their lower case: writers' casing is matched
#: loosely, and reported as stored.
_CANONICAL = {name.lower(): name
              for name in (*MAIN_PATH, *REJECT_PATH, REDRIVE_STAGE)}

GUIDANCE = f"""\
get_request_tracker_status reads RSMS's request_tracker table in uidprocessv2_2,
read-only: the pipeline-wide progress row of a packet, which every ENU stage
service updates when it picks the packet up (InProcess) and when it finishes
with it (Completed, plus a reason code if it rejected the packet). Look it
up by the packet's refId (packetMetaData.refId), or by its SID.

Use it for: which stage the packet is at now, whether a stage recorded a
reject reason code, whether the packet is still InProcess at a stage long
after it entered it (stuck), and how many times it was submitted.

- One row per packet, overwritten as the packet moves on: it shows where the
  packet is now, not where it has been, and it may already be past the
  stage that failed (a rejected packet goes on to RejectInterceptor, then
  PostEnrolment).
- Completed does not mean approved: a rejection is Completed plus a reason
  code.
- Stage order: {", ".join(MAIN_PATH)}; a rejected packet goes to
  {REJECT_PATH[0]}, then {REJECT_PATH[1]}. A stage can be skipped for a
  packet type, and some stages report only under conditions, so a stage
  missing from a packet's path is not an error, and an earlier stage is not
  proven to have run.
- Stage-level only: what happened inside a stage is in that stage's own
  tables, which this tool does not read.
- The row is live: compare stage_status_date and last_updated_date with the
  time of the failure before reading the row as the state at the failure."""


def is_enabled() -> bool:
    return PROCESS.is_enabled()


REQUEST_TRACKER = Toolset(
    name="request_tracker",
    # The per-packet investigation only, as with the process DB tools: the
    # Reviewer gets these results as evidence, and the DLT lane's narrative
    # is per error code, which one packet's row must not leak into.
    agents=("investigator",),
    guidance=GUIDANCE,
    enabled=is_enabled,
    # One SELECT on a READ ONLY session.
    read_only=True,
    # Every stage service reports here, so it means the same for any packet.
    services=("*",),
)


@agent_tool(REQUEST_TRACKER)
def get_request_tracker_status(refid: str = "", sid: str = "") -> str:
    """Where a packet is in the ENU pipeline now, from RSMS's request_tracker table.

    Use this for: which stage the packet is at, whether that stage has
    finished with it, whether a reject reason code is recorded, and whether it
    has sat InProcess at a stage for a long time (stuck). The table keeps one
    row per packet, overwritten at every stage report, so there is no history:
    `stage_position.earlier_stages` are the stages before this one in the
    pipeline's order, which the packet passed unless they were skipped for its
    type.

    `reading` applies the table's rules: `state` is in_process (the stage has
    picked it up and not finished), completed (the stage has finished with it,
    passed or rejected) or unrecognised_status; `reason_code_recorded` is
    whether stage_reason_code or stage_reject_reason_code is set -- with
    Completed, that is a rejection; `on_reject_path` is whether the packet is
    at RejectInterceptor; `summary` says what the row means in words.
    `seconds_since_stage_status_date` is measured against the database
    server's clock (`read_at`). Also returned: the packet's type, sub_type,
    mode, priority, submission_count, update_ind, reject_ind and whether a UID
    is assigned. The resident's personal details are never returned.

    refid: the packet's refId (packetMetaData.refId). sid: the packet's SID,
    used when no refid is given.
    """
    return PROCESS.run_lookup(lambda: _status(refid, sid))


def _status(refid, sid) -> dict:
    refid = name_arg("refid", refid, limit=REFID_MAX)
    sid = name_arg("sid", sid, limit=SID_MAX)
    if refid is None and sid is None:
        raise InvalidArgument("refid or sid is required.")
    # Both are uniquely indexed. refid wins when both are given, and the SID
    # is then checked against the row rather than queried. `column` is one of
    # these two names, never an argument.
    column, value = ("refid", refid) if refid is not None else ("sid", sid)

    rows = PROCESS.query(
        "SELECT tracker_key, refid, sid, sid_date, srn_num, priority, type, "
        "sub_type, mode, stage, stage_status, stage_status_date, "
        "stage_reason_code, stage_reject_reason_code, submission_count, "
        "upload_time, update_ind, reject_ind, is_child, source, milestone, "
        "client_version, "
        "CASE WHEN uid IS NULL OR uid = '' THEN 0 ELSE 1 END AS uid_assigned, "
        "created_by, creation_date, last_updated_by, last_updated_date, "
        "CURRENT_TIMESTAMP AS read_at "
        f"FROM request_tracker WHERE {column} = :value LIMIT 1",
        {"value": value})

    result: dict = {"lookup": {column: value}, "found": bool(rows)}
    if not rows:
        return result
    row = rows[0]

    stage = row.get("stage")
    status = _status_name(row.get("stage_status"))
    reason_code = row.get("stage_reject_reason_code") or row.get("stage_reason_code")
    if column == "refid" and sid is not None:
        result["sid_matches"] = row.get("sid") == sid

    result.update({
        "stage": stage,
        "stage_status": row.get("stage_status"),
        "stage_status_date": plain(row.get("stage_status_date")),
        "stage_reason_code": row.get("stage_reason_code"),
        "stage_reject_reason_code": row.get("stage_reject_reason_code"),
        "reading": _reading(stage, status, reason_code, row.get("stage_status")),
        "stage_position": _position(stage),
        "seconds_since_stage_status_date": seconds_between(
            row.get("stage_status_date"), row.get("read_at")),
        "read_at": plain(row.get("read_at")),
        "packet": {
            name: plain(row.get(name))
            for name in ("refid", "sid", "sid_date", "srn_num", "type", "sub_type",
                           "mode", "priority", "submission_count", "upload_time",
                           "update_ind", "reject_ind", "is_child", "source",
                           "milestone", "client_version")
        },
        "uid_assigned": bool(row.get("uid_assigned")),
        "row": {
            name: plain(row.get(name))
            for name in ("tracker_key", "created_by", "creation_date",
                           "last_updated_by", "last_updated_date")
        },
    })
    return result


def _status_name(value):
    """InProcess / Completed whatever the casing or separators, else None.

    enu-biometric, enu-rejectinterceptor and enu-ext-integration send
    InProcess/Completed; enu-metadata and enu-biofraud-check send constants
    (IN_PROCESS/COMPLETED) whose stored spelling the docs do not show.
    """
    folded = str(value or "").replace("_", "").replace(" ", "").lower()
    return {"inprocess": IN_PROCESS, "completed": COMPLETED}.get(folded)


def _canonical(stage):
    return _CANONICAL.get(str(stage or "").strip().lower())


def _position(stage) -> dict:
    name = _canonical(stage)
    if name is None:
        return {"known_stage": False}
    if name == REDRIVE_STAGE:
        return {"known_stage": True, "stage": name, "path": "redrive"}
    if name == REJECT_PATH[0]:
        return {"known_stage": True, "stage": name, "path": "reject",
                "index": 1, "of": len(REJECT_PATH)}
    index = MAIN_PATH.index(name)
    return {"known_stage": True, "stage": name, "path": "main",
            "index": index + 1, "of": len(MAIN_PATH),
            "earlier_stages": list(MAIN_PATH[:index])}


def _reading(stage, status, reason_code, raw_status) -> dict:
    name = _canonical(stage) or stage or "The stage"
    on_reject_path = name == REJECT_PATH[0]
    reading = {"state": "unrecognised_status",
               "reason_code_recorded": bool(reason_code),
               "on_reject_path": on_reject_path}

    if status is None:
        reading["summary"] = (f"stage_status {raw_status!r} is neither InProcess nor "
                              f"Completed, so the row's state cannot be read.")
        return reading

    if status == IN_PROCESS:
        reading["state"] = "in_process"
        summary = (f"{name} has picked the packet up and has not reported it finished. "
                   f"If that was long ago, the packet may be stuck there.")
        if reason_code:
            summary += (" A reason code is set although the stage has not finished: "
                        "the row keeps only the latest values, so it is probably left "
                        "from an earlier stage's rejection.")
    else:
        reading["state"] = "completed"
        if reason_code and name in REJECT_PATH:
            summary = (f"{name} has finished with the packet. The reason code is most "
                       f"likely the one recorded by the stage that rejected it, which "
                       f"this row no longer names.")
        elif reason_code:
            summary = (f"{name} has finished with the packet and recorded a reason "
                       f"code: the packet was rejected at {name}.")
        elif on_reject_path:
            summary = ("The packet is on the reject path: an earlier stage rejected it, "
                       "and this row no longer names that stage or its reason code.")
        elif name == MAIN_PATH[-1]:
            summary = ("PostEnrolment has finished with the packet, the end of its "
                       "journey. PostEnrolment follows both SuccessInterceptor and "
                       "RejectInterceptor, and no reason code is recorded, so this row "
                       "alone does not say whether the packet was approved.")
        else:
            summary = (f"{name} has finished with the packet without a reject reason "
                       f"code, so it passed {name}, and no later stage has reported it "
                       f"yet.")

    if on_reject_path and status == IN_PROCESS:
        summary += " The packet is on the reject path: an earlier stage rejected it."
    reading["summary"] = summary
    return reading
