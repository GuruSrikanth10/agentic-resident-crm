#!/usr/bin/env python3
"""
Read-only lookups against the ENU biometric process database (uidprocessv2_2).

enu-biometric keeps its per-packet working state in three tables, and several
reason codes are about a row in one of them being absent when it was needed:

  bio_stage_tracker        one row per (refid, stage, sub_stage) the packet
                           entered, with its status and reason codes.
                           BIO_STAGE_TRACKER_DATA_NOT_FOUND is thrown when no
                           IN_PROGRESS row exists for the refid + sub_stage.
  bio_helper_cache_store   JSON helper records (update-checker responses,
                           applicant-candidate and parking helper records),
                           keyed by (refid, record_type). The
                           *_HELPER_CACHE_STORE_DATA_NOT_FOUND codes are thrown
                           when both Redis and this table miss.
  bio_parking_queue_store  the applicant's parked event, keyed by
                           (refid, record_category).
                           BIO_PARKING_QUEUE_STORE_DATA_NOT_FOUND is thrown
                           when the applicant was never parked or its row was
                           purged.

So the rows -- or their absence -- are direct evidence for those codes.

Every lookup is by `refid`, which is indexed on all three tables. There is no
free-form SQL and no lookup by any other column: bio_stage_tracker is past a
billion rows, and an unindexed predicate on it is a full scan of a production
table the enrolment pipeline is writing to.

The account in the service's own configuration is a WRITE account. Sessions
are therefore forced READ ONLY on connect, so a mistake here cannot become a
write to the pipeline's state, whatever grant the account holds.

Off unless PROCESS_DB_ENABLED=true. The datasource is separate from the rules
database in tool_registry (DB_*), with its own engine and its own circuit
breaker, so an outage of one does not trip the other.

Usage:
    python3 -m src.tools.process_db --refid <refid>
    python3 -m src.tools.process_db --refid <refid> --table stage
    python3 -m src.tools.process_db --refid <refid> --table helper --record-type <type>

Exit codes:
    0  rows found
    1  could not look (disabled, misconfigured, DB unreachable)
    2  looked successfully, found nothing
"""
import argparse
import json
import os
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pybreaker
from langchain_core.tools import tool
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL
# Imported for its side effect: without it a missing PyMySQL surfaces on the
# first query rather than at import. Same reasoning as tool_registry.
import pymysql  # noqa: F401

from src.log_pipeline.redaction import redact_text
from src.utils.env import get_bool_env, get_required_env
from src.utils.logging_config import get_logger
from src.utils.resilience import process_db_breaker, retry_transient

logger = get_logger(__name__)

ENV_ENABLED = "PROCESS_DB_ENABLED"

#: Rows returned per lookup. A packet that was resubmitted many times can
#: carry many stage rows; the newest are the ones that explain the failure,
#: so the cut drops the oldest and says it did.
DEFAULT_MAX_ROWS = 50

#: Characters kept of each free-text or JSON column. event_message alone is
#: varchar(10000), and helper_record / event_json are unbounded JSON.
DEFAULT_MAX_FIELD_CHARS = 4000

#: Longest value each filter column holds, from the DDL. A longer argument
#: cannot match anything, so it is refused before it reaches the database.
_REFID_MAX = 36
_RECORD_TYPE_MAX = 100
_RECORD_CATEGORY_MAX = 50


@dataclass(frozen=True)
class _TableSpec:
    table: str
    #: Auto-increment primary key, used as insertion order.
    key: str
    #: Columns selected as-is. `uid` and `integrity` are never selected: the
    #: first is a resident identifier, the second a row checksum with no
    #: diagnostic value.
    columns: tuple
    #: Free text / JSON columns: PII-redacted and bounded.
    blob_columns: tuple = ()
    #: Of the blob columns, the ones parsed back into JSON when they fit.
    json_columns: tuple = ()
    #: Extra SELECT expressions, already aliased.
    derived: tuple = ()
    #: Optional second filter, and only ever the second column of an index
    #: that leads with refid.
    filter_column: Optional[str] = None


STAGE_TRACKER = _TableSpec(
    table="bio_stage_tracker",
    key="stage_tracker_key",
    columns=(
        "stage_tracker_key", "refid", "sid", "sid_date", "srn_num", "priority",
        "enrl_type", "stage", "sub_stage", "sub_stage_status",
        "sub_stage_status_date", "sub_stage_reason_code",
        "sub_stage_reject_reason_code", "markfor_stage_resubmission",
        "stage_resubmission_count", "event_message", "created_by",
        "creation_date", "last_updated_by", "last_updated_date",
    ),
    blob_columns=("event_message",),
    # Whether a UID has been attached is useful; the UID itself is not.
    derived=("(uid IS NOT NULL) AS uid_present",),
)

HELPER_CACHE = _TableSpec(
    table="bio_helper_cache_store",
    key="helper_record_key",
    columns=(
        "helper_record_key", "refid", "sid", "enrl_type", "record_type",
        "helper_record", "created_by", "creation_date", "last_updated_by",
        "last_updated_date",
    ),
    blob_columns=("helper_record",),
    json_columns=("helper_record",),
    filter_column="record_type",  # idx_sst_multiple (refid, record_type)
)

PARKING_QUEUE = _TableSpec(
    table="bio_parking_queue_store",
    key="parking_queue_key",
    columns=(
        "parking_queue_key", "refid", "record_category", "event_json",
        "is_parked", "created_by", "creation_date", "last_updated_by",
        "last_updated_date",
    ),
    blob_columns=("event_json",),
    json_columns=("event_json",),
    filter_column="record_category",  # uq_refid_category (refid, record_category)
)

#: CLI --table names.
TABLES = {"stage": STAGE_TRACKER, "helper": HELPER_CACHE, "parking": PARKING_QUEUE}


class ProcessDbDisabled(RuntimeError):
    """PROCESS_DB_ENABLED is not true."""


_ENGINE = None
_engine_lock = threading.Lock()


def is_enabled() -> bool:
    return get_bool_env(ENV_ENABLED, False)


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, "").strip() or default)
    except ValueError:
        logger.warning("Ignoring a non-integer setting", variable=name)
        return default
    return value if value > 0 else default


def max_rows() -> int:
    return _int_env("PROCESS_DB_MAX_ROWS", DEFAULT_MAX_ROWS)


def max_field_chars() -> int:
    return _int_env("PROCESS_DB_MAX_FIELD_CHARS", DEFAULT_MAX_FIELD_CHARS)


def _on_connect(dbapi_connection, _record):
    """Make every pooled connection read-only before it is handed out.

    READ ONLY must succeed: a connection that could write is closed rather
    than used. The statement timeout is best effort -- max_execution_time is
    MySQL 5.7.8+ and a server without it should still serve lookups.
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

    URL.create rather than an f-string, so a password carrying '@', ':' or
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


def _checked(name: str, value: Optional[str], limit: int,
             required: bool) -> Optional[str]:
    value = (value or "").strip()
    if not value:
        if required:
            raise ValueError(f"{name} is required.")
        return None
    if len(value) > limit:
        raise ValueError(f"{name} is longer than the column ({limit} chars).")
    return value


def _plain(value):
    """A value json.dumps can write."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


def _bounded_blob(value, as_json: bool, allowlist: list, limit: int):
    """Redact, then bound. Parsed back into JSON only when it was not cut.

    Redaction runs first so the cut can never fall inside an identifier and
    leave a fragment the patterns no longer recognise.
    """
    if value is None:
        return None
    value = _plain(value)
    body = value if isinstance(value, str) else json.dumps(value, default=str)
    body = redact_text(body, allowlist=allowlist).text
    if len(body) > limit:
        return f"{body[:limit]}... [truncated {len(body) - limit} chars]"
    if as_json:
        try:
            return json.loads(body)
        except ValueError:
            pass
    return body


def _shape_row(spec: _TableSpec, row: dict, allowlist: list, limit: int) -> dict:
    shaped = {}
    for column, value in row.items():
        if column in spec.blob_columns:
            shaped[column] = _bounded_blob(
                value, column in spec.json_columns, allowlist, limit)
        elif column == "uid_present":
            shaped[column] = bool(value)
        else:
            shaped[column] = _plain(value)
    return shaped


@process_db_breaker
@retry_transient
def _select(spec: _TableSpec, refid: str, filter_value: Optional[str],
            limit: int) -> list:
    """The one query path: retried, breaker-guarded, refid-indexed.

    Newest first so LIMIT keeps the recent rows; one extra row is fetched to
    tell a full result from a cut one.
    """
    # Table and column names come from the module-level specs, never from
    # the caller; only values are bound.
    select_list = ", ".join(spec.columns + spec.derived)
    sql = f"SELECT {select_list} FROM {spec.table} WHERE refid = :refid"
    params = {"refid": refid, "limit": limit + 1}
    if filter_value is not None:
        sql += f" AND {spec.filter_column} = :filter_value"
        params["filter_value"] = filter_value
    sql += f" ORDER BY {spec.key} DESC LIMIT :limit"

    with get_engine().connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params).mappings()]


def fetch(spec: _TableSpec, refid: str, filter_value: Optional[str] = None) -> dict:
    """Rows for `refid` from one table, oldest first.

    Raises ProcessDbDisabled, ValueError on a bad argument, and the database
    or breaker exception on a failed lookup. Python callers that pre-fetch
    evidence should use this; the LLM-facing tools below turn every failure
    into a message instead.
    """
    if not is_enabled():
        raise ProcessDbDisabled(f"{ENV_ENABLED} is not true.")

    refid = _checked("refid", refid, _REFID_MAX, required=True)
    if spec.filter_column is None:
        filter_value = None
    else:
        limit = (_RECORD_TYPE_MAX if spec.filter_column == "record_type"
                 else _RECORD_CATEGORY_MAX)
        filter_value = _checked(spec.filter_column, filter_value, limit,
                                required=False)

    row_limit = max_rows()
    rows = _select(spec, refid, filter_value, row_limit)
    truncated = len(rows) > row_limit
    rows = rows[:row_limit]
    rows.reverse()

    # refid and sid are correlation ids, not resident PII; a numeric one must
    # not be mistaken for an Aadhaar number inside a JSON blob.
    allowlist = [refid] + sorted({str(r["sid"]) for r in rows if r.get("sid")})
    field_limit = max_field_chars()
    result = {
        "table": spec.table,
        "refid": refid,
        "row_count": len(rows),
        "truncated": truncated,
        "rows": [_shape_row(spec, r, allowlist, field_limit) for r in rows],
    }
    if filter_value is not None:
        result[spec.filter_column] = filter_value
    logger.info("Process DB lookup finished", table=spec.table, refid=refid,
                row_count=len(rows), truncated=truncated)
    return result


def fetch_stage_tracker(refid: str) -> dict:
    return fetch(STAGE_TRACKER, refid)


def fetch_helper_cache(refid: str, record_type: Optional[str] = None) -> dict:
    return fetch(HELPER_CACHE, refid, record_type)


def fetch_parking_queue(refid: str, record_category: Optional[str] = None) -> dict:
    return fetch(PARKING_QUEUE, refid, record_category)


def _tool_result(spec: _TableSpec, refid: str,
                 filter_value: Optional[str] = None) -> str:
    """JSON on success; otherwise one line saying why nothing was read.

    The failure messages must not read like an empty result: "the table has
    no row for this refid" is evidence for a *_DATA_NOT_FOUND code, and
    "the lookup did not run" is not.
    """
    try:
        return json.dumps(fetch(spec, refid, filter_value), ensure_ascii=False)
    except ProcessDbDisabled:
        return "Process DB lookups are disabled; no rows were read."
    except ValueError as e:
        return f"Invalid argument for {spec.table} lookup: {e}"
    except pybreaker.CircuitBreakerError:
        logger.error("Process DB circuit breaker is open; failing fast",
                     table=spec.table, refid=refid)
        return (f"Failed to query {spec.table}: the process DB circuit breaker "
                f"is open. No rows were read.")
    except Exception as e:
        logger.error("Process DB lookup failed", table=spec.table, refid=refid,
                     error=f"{type(e).__name__}: {e}")
        return f"Failed to query {spec.table}: {type(e).__name__}. No rows were read."


@tool
def lookup_bio_stage_tracker(refid: str) -> str:
    """Read the stage history of a packet from bio_stage_tracker, by refId.

    One row per stage / sub_stage the packet entered, oldest first, with
    sub_stage_status, sub_stage_reason_code, sub_stage_reject_reason_code,
    resubmission marks and the stored event_message. Use it to see which
    stage the packet reached and what status each sub-stage was left in --
    for BIO_STAGE_TRACKER_DATA_NOT_FOUND, whether an IN_PROGRESS row existed
    for the sub-stage being processed. A row_count of 0 means the table holds
    no rows for this refId.
    """
    return _tool_result(STAGE_TRACKER, refid)


@tool
def lookup_bio_helper_cache(refid: str, record_type: str = "") -> str:
    """Read helper records for a packet from bio_helper_cache_store, by refId.

    Helper records are JSON the dedup flow persists between steps
    (update-checker responses, applicant-candidate and parking helper
    records), labelled by record_type. Pass record_type to read one kind, or
    leave it empty for all. Use it for the *_HELPER_CACHE_STORE_DATA_NOT_FOUND
    codes: a row_count of 0 means the table holds no such record for this
    refId, though the service also checks Redis, which this does not read.
    """
    return _tool_result(HELPER_CACHE, refid, record_type)


@tool
def lookup_bio_parking_queue(refid: str, record_category: str = "") -> str:
    """Read an applicant's parking record from bio_parking_queue_store, by refId.

    Holds the parked event (event_json), its record_category and is_parked.
    Pass record_category to read one category, or leave it empty for all.
    Use it for BIO_PARKING_QUEUE_STORE_DATA_NOT_FOUND: a row_count of 0 means
    the table holds no parking record for this refId, though the service also
    checks Redis, which this does not read.
    """
    return _tool_result(PARKING_QUEUE, refid, record_category)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only lookup of a packet's rows in the enu-biometric process DB.")
    parser.add_argument("--refid", required=True)
    parser.add_argument("--table", choices=sorted(TABLES) + ["all"], default="all")
    parser.add_argument("--record-type", help="bio_helper_cache_store filter")
    parser.add_argument("--record-category", help="bio_parking_queue_store filter")
    args = parser.parse_args(argv)

    filters = {"helper": args.record_type, "parking": args.record_category}
    names = sorted(TABLES) if args.table == "all" else [args.table]

    found = 0
    for name in names:
        try:
            result = fetch(TABLES[name], args.refid, filters.get(name))
        except Exception as e:
            print(f"{TABLES[name].table}: {type(e).__name__}: {e}", file=sys.stderr)
            return 1
        found += result["row_count"]
        print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if found else 2


if __name__ == "__main__":
    sys.exit(main())
