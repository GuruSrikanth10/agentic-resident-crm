"""
Shared plumbing for the enu-biometric process database tools (uidprocessv2_2).

A helper, not a tool module -- the leading underscore keeps discovery from
scanning it. `stage_tracker.py`, `parking_queue.py` and `helper_cache.py`
import it, and every tool they define inherits these guarantees:

- Read only. The account in the service's own configuration can write, so
  every pooled connection runs SET SESSION TRANSACTION READ ONLY before it is
  handed out, and a connection on which that fails is refused.
- Indexed. Every statement filters on `refid`, which all three tables index.
  bio_stage_tracker is past a billion rows; an unindexed predicate on it is a
  full scan of a table the pipeline is writing to. No tool takes SQL.
- Bounded. Row counts are capped in SQL, list outputs shrink to fit
  PROCESS_DB_MAX_OUTPUT_CHARS, and MySQL's max_execution_time caps each SELECT.
- Redacted. Values from the JSON blobs pass through the log pipeline's PII
  redaction, and a candidate's `uid` is masked outright.
- Isolated. Its own engine and its own circuit breaker (process_db_breaker),
  so an outage here cannot trip the rules database's breaker, or the reverse.

Off unless PROCESS_DB_ENABLED=true; while off, the toolset is not offered to
any agent at all.
"""
import json
import os
import re
import threading
from datetime import date, datetime
from decimal import Decimal
from typing import Callable, Optional

import pybreaker
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL
# Imported for its side effect: without it a missing PyMySQL surfaces on the
# first lookup rather than at import. Same reasoning as tool_registry.
import pymysql  # noqa: F401

from src.log_pipeline.redaction import redact_text
from src.tools.agent_tools import Toolset
from src.utils.env import get_bool_env, get_required_env
from src.utils.logging_config import get_logger
from src.utils.resilience import process_db_breaker, retry_transient

logger = get_logger(__name__)

ENV_ENABLED = "PROCESS_DB_ENABLED"

#: Rows one lookup reads, where a packet can have many (stage rows,
#: candidates). The newest or highest-ranked are kept, and the output says
#: when more existed.
DEFAULT_MAX_ROWS = 50

#: Characters in one tool result. About 4000 tokens: enough for a full stage
#: summary, small enough that several lookups fit beside the logs.
DEFAULT_MAX_OUTPUT_CHARS = 16000

#: Longest value each argument can match, from the DDL. A longer argument
#: cannot match anything, so it is refused before it reaches the database.
REFID_MAX = 36
NAME_MAX = 100

IN_PROGRESS = "IN PROGRESS"
COMPLETED = "COMPLETED"

GUIDANCE = """\
These tools read enu-biometric's own working tables in uidprocessv2_2,
read-only, by the packet's refId (`packetMetaData.refId` in the payload).
Pick the tool by the question:

- Where is the packet in the biometric pipeline, is a substage stuck, how
  long did a step take, was it retried: get_packet_stage_summary, then
  get_packet_stage_timeline for the rows behind it.
- Is the applicant parked, which candidates is it waiting for, which
  applicants are waiting on a candidate: get_parking_status.
- Which candidates ABIS returned and with what scores: get_abis_candidates.
  What the service knew about each candidate when it decided:
  get_candidate_facts. Cross-match verdicts of a parked update packet:
  get_parking_match_verdicts. What the Update Checker said:
  get_update_checker_result. Which of these records exist:
  list_helper_records. Any other field of a record: get_helper_record_fields.

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
    return get_bool_env(ENV_ENABLED, False)


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
)


class InvalidArgument(ValueError):
    """An argument no row could match; refused before any query."""


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, "").strip() or default)
    except ValueError:
        logger.warning("Ignoring a non-integer setting", variable=name)
        return default
    return value if value > 0 else default


def max_rows() -> int:
    return _int_env("PROCESS_DB_MAX_ROWS", DEFAULT_MAX_ROWS)


def max_output_chars() -> int:
    return _int_env("PROCESS_DB_MAX_OUTPUT_CHARS", DEFAULT_MAX_OUTPUT_CHARS)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_ENGINE = None
_engine_lock = threading.Lock()


def _on_connect(dbapi_connection, _record):
    """Make every pooled connection read-only before it is handed out.

    READ ONLY must succeed: a connection that could write is closed rather
    than used. The statement timeout is best effort -- max_execution_time is
    MySQL 5.7.8+, and a server without it should still serve lookups.
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SET SESSION TRANSACTION READ ONLY")
        try:
            timeout_ms = _int_env("PROCESS_DB_QUERY_TIMEOUT_MS", 10000)
            cursor.execute(f"SET SESSION max_execution_time = {timeout_ms}")
        except Exception as e:
            logger.warning("Could not set a statement timeout on the process DB",
                           error=f"{type(e).__name__}: {e}")
    finally:
        cursor.close()


def get_engine():
    """The process DB engine, built once.

    URL.create rather than an f-string, so a password containing '@', ':' or
    '/' is escaped instead of silently changing the host.
    """
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    with _engine_lock:
        if _ENGINE is not None:
            return _ENGINE
        url = URL.create(
            "mysql+pymysql",
            username=get_required_env("PROCESS_DB_USERNAME"),
            password=get_required_env("PROCESS_DB_PASSWORD"),
            host=get_required_env("PROCESS_DB_HOST"),
            port=int(get_required_env("PROCESS_DB_PORT", "6446")),
            database=get_required_env("PROCESS_DB_NAME", "uidprocessv2_2"),
            query={"charset": "utf8mb4"},
        )
        engine = create_engine(
            url,
            pool_size=_int_env("PROCESS_DB_POOL_SIZE", 5),
            max_overflow=_int_env("PROCESS_DB_MAX_OVERFLOW", 5),
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"connect_timeout": 10, "read_timeout": 30},
        )
        event.listen(engine, "connect", _on_connect)
        _ENGINE = engine
        return _ENGINE


@process_db_breaker
@retry_transient
def query(sql: str, params: dict) -> list:
    """Run one SELECT and return its rows as dicts.

    Callers pass fixed SQL; only values are bound. Retried on transient
    errors and guarded by this database's own breaker.
    """
    with get_engine().connect() as conn:
        return [dict(row) for row in conn.execute(text(sql), params).mappings()]


# ---------------------------------------------------------------------------
# Running a lookup
# ---------------------------------------------------------------------------

DISABLED = ("The process DB tools are switched off (PROCESS_DB_ENABLED is not "
            "true), so nothing was read. This is an evidence gap, not a finding.")


def run_lookup(build: Callable[[], dict], *, list_key: Optional[str] = None,
               keep_from_end: bool = False) -> str:
    """Run `build` and return its result as JSON text; failures as one line.

    The failure lines must never read like an empty result: "the table has no
    row for this refid" is evidence for a *_DATA_NOT_FOUND code, and "the
    lookup did not run" is not.
    """
    if not is_enabled():
        return DISABLED
    try:
        result = build()
    except InvalidArgument as e:
        return f"Invalid argument: {e} Nothing was read."
    except pybreaker.CircuitBreakerError:
        logger.error("Process DB circuit breaker is open; failing fast")
        return ("The process DB is failing and its circuit breaker is open, so "
                "nothing was read. This is an evidence gap, not a finding.")
    except Exception as e:
        logger.error("Process DB lookup failed", error=f"{type(e).__name__}: {e}")
        return (f"The process DB lookup failed ({type(e).__name__}), so nothing "
                f"was read. This is an evidence gap, not a finding.")
    return finish(result, list_key=list_key, keep_from_end=keep_from_end)


def _dumps(value) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


def finish(result: dict, *, list_key: Optional[str] = None,
           keep_from_end: bool = False) -> str:
    """Serialise, shrinking `result[list_key]` until it fits the output cap.

    Entries are dropped whole, so the JSON stays valid and the output says how
    many were left out. `keep_from_end` keeps the last entries (the newest,
    for a chronological list); otherwise the first (the highest-ranked). A
    result still over the cap after that is cut as text, as a last resort.
    """
    limit = max_output_chars()
    textual = _dumps(result)
    items = result.get(list_key) if list_key else None
    if len(textual) > limit and isinstance(items, list) and items:
        def build(keep: int) -> dict:
            kept = items[len(items) - keep:] if keep_from_end else items[:keep]
            shrunk = dict(result)
            shrunk[list_key] = kept
            shrunk[f"{list_key}_left_out_to_fit"] = len(items) - keep
            return shrunk

        low, high = 0, len(items) - 1
        while low < high:
            middle = (low + high + 1) // 2
            if len(_dumps(build(middle))) <= limit:
                low = middle
            else:
                high = middle - 1
        textual = _dumps(build(low))
    if len(textual) > limit:
        textual = (textual[:limit]
                   + f" ... [cut at PROCESS_DB_MAX_OUTPUT_CHARS={limit}]")
    return textual


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

def refid_arg(value) -> str:
    value = str(value or "").strip()
    if not value:
        raise InvalidArgument("refid is required.")
    if len(value) > REFID_MAX:
        raise InvalidArgument(f"refid is longer than the column ({REFID_MAX} characters).")
    return value


def name_arg(label: str, value, *, required: bool = False,
             limit: int = NAME_MAX) -> Optional[str]:
    value = str(value or "").strip()
    if not value:
        if required:
            raise InvalidArgument(f"{label} is required.")
        return None
    if len(value) > limit:
        raise InvalidArgument(f"{label} is longer than the column ({limit} characters).")
    return value


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------

def plain(value):
    """A value JSON can carry."""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


def as_datetime(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def seconds_between(start, end) -> Optional[float]:
    start, end = as_datetime(start), as_datetime(end)
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


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


def parse_json(value):
    """A JSON column's value, parsed. None when absent or not valid JSON."""
    value = plain(value)
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def field(obj, name: str):
    """obj[name], matching the key case-insensitively when not exact."""
    if not isinstance(obj, dict):
        return None
    if name in obj:
        return obj[name]
    lowered = name.lower()
    for key, value in obj.items():
        if isinstance(key, str) and key.lower() == lowered:
            return value
    return None


def as_list(value) -> list:
    """A JSON array; a lone object (a one-element list serialised as an
    object) becomes a one-element list; anything else, empty."""
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return []


def as_number(value) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


#: A refId. The Aadhaar pattern (four digits, separator, four digits,
#: separator, four digits) matches inside a UUID whose middle groups happen to
#: be all decimal -- about one in three hundred -- so refIds are shielded from
#: redaction like any other correlation id.
_UUID = re.compile(r"\b[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}"
                   r"-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\b")


def redact(value, allowlist: list):
    """PII-redact a parsed JSON value; `uid` keys are masked outright.

    Numbers are checked too: a UID serialised from a Java long is a bare
    12-digit number, which a string-only pass would let through. refIds
    (UUIDs) and the `allowlist` are left intact.
    """
    if isinstance(value, dict):
        return {key: (_mask(item) if isinstance(key, str) and key.lower() == "uid"
                      else redact(item, allowlist))
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item, allowlist) for item in value]
    if isinstance(value, str):
        return redact_text(value, allowlist=[*allowlist, *_UUID.findall(value)]).text
    if isinstance(value, int) and not isinstance(value, bool):
        textual = str(value)
        redacted = redact_text(textual, allowlist=allowlist).text
        return value if redacted == textual else redacted
    return value


def _mask(value):
    return value if value in (None, "") else "[REDACTED:UID]"


def bounded_json(value, limit: int):
    """`value` unchanged when its JSON fits `limit`, else its JSON cut to it."""
    textual = _dumps(value)
    if len(textual) <= limit:
        return value
    return textual[:limit] + f" ... [{len(textual)} characters in total]"
