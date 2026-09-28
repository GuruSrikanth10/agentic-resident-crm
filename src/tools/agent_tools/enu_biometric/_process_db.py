"""
enu-biometric's process database (uidprocessv2_2) and the toolset that reads it.

A helper, not a tool module -- the leading underscore keeps discovery from
scanning it. `stage_tracker.py`, `parking_queue.py` and `helper_cache.py`
import it. The database is the `process` key of the shared read-only layer
(`agent_tools/_database.py`), which gives every tool here its guarantees:
read-only sessions, a statement timeout, capped rows and output, redaction,
and its own engine and circuit breaker (process_db_breaker), so an outage
here cannot trip the rules database's breaker, or the reverse. Every
statement filters on `refid`, which all three tables index; bio_stage_tracker
is past a billion rows, so an unindexed predicate on it would be a full scan
of a table the pipeline is writing to.

The toolset is scoped to enu-biometric alone (MULTI_SERVICE_PLAN.md D7), so
its tools carry that service's prefix, `bio_` (D8), and no other service's
packet is ever offered them. Off unless PROCESS_DB_ENABLED=true; while off,
the toolset is not offered to any agent at all.
"""
from src.tools.agent_tools import Toolset
from src.tools.agent_tools._database import (  # noqa: F401 -- re-exported for the tool modules
    InvalidArgument,
    as_datetime,
    as_list,
    as_number,
    bounded_json,
    declare,
    field,
    name_arg,
    parse_json,
    plain,
    redact,
    refid_arg,
    seconds_between,
)

#: The database these tools read. Its settings are PROCESS_DB_*.
PROCESS = declare("process", label="process DB", default_port=6446,
                  default_name="uidprocessv2_2")

ENV_ENABLED = PROCESS.setting("ENABLED")

IN_PROGRESS = "IN PROGRESS"
COMPLETED = "COMPLETED"

GUIDANCE = """\
These tools read enu-biometric's own working tables in uidprocessv2_2,
read-only, by the packet's refId (`packetMetaData.refId` in the payload).
Pick the tool by the question:

- Where is the packet in the biometric pipeline, is a substage stuck, how
  long did a step take, was it retried: bio_get_packet_stage_summary, then
  bio_get_packet_stage_timeline for the rows behind it.
- Is the applicant parked, which candidates is it waiting for, which
  applicants are waiting on a candidate: bio_get_parking_status.
- Which candidates ABIS returned and with what scores: bio_get_abis_candidates.
  What the service knew about each candidate when it decided:
  bio_get_candidate_facts. Cross-match verdicts of a parked update packet:
  bio_get_parking_match_verdicts. What the Update Checker said:
  bio_get_update_checker_result. Which of these records exist:
  bio_list_helper_records. Any other field of a record:
  bio_get_helper_record_fields.

None of these tables holds the final approve/reject verdict; the reason code
and the rule do. The tables are live, so rows written after the failure can
be present: compare their timestamps with the time of the failure. The
service also keeps working copies in Redis, which these tools do not read.
For a *_DATA_NOT_FOUND reason code (for example BIO_STAGE_TRACKER_DATA_NOT_FOUND,
BIO_PARKING_QUEUE_STORE_DATA_NOT_FOUND or
UPDATE_CHECKER_BIO_HELPER_CACHE_STORE_DATA_NOT_FOUND), a missing row agrees
with what the service reported. A row that is present now shows the record
exists; say so with its timestamps instead of guessing why the service did
not find it."""


def is_enabled() -> bool:
    return PROCESS.is_enabled()


PROCESS_DB = Toolset(
    name="process_db",
    # The per-packet investigation only. The Reviewer is given these results
    # as evidence rather than querying again, and the DLT lane's narrative is
    # per error code, which packet rows must not leak into.
    agents=("investigator",),
    guidance=GUIDANCE,
    enabled=is_enabled,
    # Every statement is a SELECT on a READ ONLY session.
    read_only=True,
    # enu-biometric's own tables: meaningless for any other service's packet.
    services=("enu-biometric",),
)


def max_rows() -> int:
    return PROCESS.max_rows()


def max_output_chars() -> int:
    return PROCESS.max_output_chars()


def get_engine():
    return PROCESS.get_engine()


def query(sql: str, params: dict) -> list:
    """Run one SELECT on the process database; see `Database.query`."""
    return PROCESS.query(sql, params)


def run_lookup(build, *, list_key=None, keep_from_end: bool = False) -> str:
    """Run one lookup on the process database; see `Database.run_lookup`."""
    return PROCESS.run_lookup(build, list_key=list_key, keep_from_end=keep_from_end)


def finish(result: dict, *, list_key=None, keep_from_end: bool = False) -> str:
    return PROCESS.finish(result, list_key=list_key, keep_from_end=keep_from_end)


def normalize_status(value) -> str:
    """IN PROGRESS / COMPLETED whatever the spacing, else the raw value.

    The service writes 'IN PROGRESS' with a space; matching 'IN_PROGRESS' as
    well costs nothing and survives a writer that spells it the other way.
    """
    raw = "" if value is None else str(value)
    folded = raw.strip().upper().replace("_", " ")
    if folded == IN_PROGRESS:
        return IN_PROGRESS
    if folded == COMPLETED:
        return COMPLETED
    return raw
