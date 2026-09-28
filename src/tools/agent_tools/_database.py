"""
The read-only database layer every database-backed toolset shares.

A helper, not a tool module -- the leading underscore keeps discovery from
scanning it. A toolset module declares the database it reads with `declare`
and runs its lookups through the returned `Database`; every tool built on it
inherits these guarantees:

- Read only. The account in a service's own configuration can often write, so
  every pooled connection runs SET SESSION TRANSACTION READ ONLY before it is
  handed out, and a connection on which that fails is refused.
- Indexed. Tools take no SQL: every statement is fixed in the tool module and
  filters on an indexed key, with only values bound.
- Bounded. Row counts are capped in SQL (`<PREFIX>_MAX_ROWS`), list outputs
  shrink to fit `<PREFIX>_MAX_OUTPUT_CHARS`, and MySQL's max_execution_time
  (`<PREFIX>_QUERY_TIMEOUT_MS`) caps each SELECT.
- Redacted. `redact` passes values through the log pipeline's PII redaction,
  with refIds shielded and a `uid` masked outright.
- Isolated per database. Connections are shared per database, not per
  toolset (MULTI_SERVICE_PLAN.md D9): one engine and one circuit breaker per
  database key, each with its own switch and settings, so one database's
  outage opens only that database's breaker, and two toolsets reading one
  database share its pool.

A database is keyed by a short name. Its settings are `AGENT_DB_<KEY>_*`
(ENABLED, HOST, PORT, NAME, USERNAME, PASSWORD, POOL_SIZE, MAX_OVERFLOW,
QUERY_TIMEOUT_MS, MAX_ROWS, MAX_OUTPUT_CHARS), except the `process` key,
whose settings stay the `PROCESS_DB_*` they always were. A database is off
unless its `..._ENABLED` is true; a toolset built on it passes
`enabled=database.is_enabled`, so while it is off its tools are not served.

A common database does not make a common tool: a toolset for one service can
read a shared database, and says which database it reads by the key it
declares.
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
from src.utils.env import get_bool_env, get_required_env
from src.utils.logging_config import get_logger
from src.utils.resilience import process_db_breaker, retry_transient

logger = get_logger(__name__)

#: A database key: part of every setting's name and of its breaker's name.
_KEY = re.compile(r"^[a-z][a-z0-9_]{0,31}$")

#: Keys whose settings predate this layer and keep their names.
LEGACY_PREFIXES = {"process": "PROCESS_DB"}

#: Breakers that predate this layer, kept so everything that already refers
#: to them -- the resilience module, tests -- goes on meaning the same one.
_LEGACY_BREAKERS = {"process": process_db_breaker}

#: Rows one lookup reads, where a packet can have many. The newest or
#: highest-ranked are kept, and the output says when more existed.
DEFAULT_MAX_ROWS = 50

#: Characters in one tool result. About 4000 tokens: enough for a full
#: summary, small enough that several lookups fit beside the logs.
DEFAULT_MAX_OUTPUT_CHARS = 16000

DEFAULT_QUERY_TIMEOUT_MS = 10000

#: Longest value each argument can match. A longer argument cannot match
#: anything, so it is refused before it reaches the database.
REFID_MAX = 36
NAME_MAX = 100

#: Settings a database cannot connect without, checked at boot while it is on.
REQUIRED_SETTINGS = ("HOST", "USERNAME", "PASSWORD")


class InvalidArgument(ValueError):
    """An argument no row could match; refused before any query."""


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, "").strip() or default)
    except ValueError:
        logger.warning("Ignoring a non-integer setting", variable=name)
        return default
    return value if value > 0 else default


def _dumps(value) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


class Database:
    """One database the tools read: its settings, its engine, its breaker.

    Made by `declare`, never directly, so that every toolset reading one
    database key gets the same object -- and with it the same pool and the
    same breaker.
    """

    def __init__(self, key: str, *, label: str, default_port: int,
                 default_name: Optional[str] = None):
        self.key = key
        #: How messages name it, e.g. "process DB".
        self.label = label
        self.default_port = default_port
        self.default_name = default_name
        self.env_prefix = LEGACY_PREFIXES.get(key, f"AGENT_DB_{key.upper()}")
        self.breaker = _LEGACY_BREAKERS.get(key) or pybreaker.CircuitBreaker(
            fail_max=3, reset_timeout=60, name=self.breaker_name)
        self._engine = None
        self._engine_lock = threading.Lock()
        # Retried inside the breaker, as the decorators always stacked: a
        # transient error is retried, and only a lookup that still fails
        # counts against the breaker.
        self._retrying_query = retry_transient(self._run_query)

    # -- settings ----------------------------------------------------------

    def setting(self, name: str) -> str:
        """The name of one of this database's settings, e.g. PROCESS_DB_HOST."""
        return f"{self.env_prefix}_{name}"

    @property
    def breaker_name(self) -> str:
        """The breaker's label on the breaker-state metric."""
        return f"{self.env_prefix.lower()}_breaker"

    def is_enabled(self) -> bool:
        return get_bool_env(self.setting("ENABLED"), False)

    def max_rows(self) -> int:
        return _int_env(self.setting("MAX_ROWS"), DEFAULT_MAX_ROWS)

    def max_output_chars(self) -> int:
        return _int_env(self.setting("MAX_OUTPUT_CHARS"), DEFAULT_MAX_OUTPUT_CHARS)

    def missing_settings(self) -> list:
        """Boot errors for a database that is on but cannot connect."""
        if not self.is_enabled():
            return []
        return [f"{self.setting('ENABLED')}=true requires {self.setting(name)} to be set."
                for name in REQUIRED_SETTINGS
                if not os.environ.get(self.setting(name), "").strip()]

    # -- connection --------------------------------------------------------

    def on_connect(self, dbapi_connection, _record) -> None:
        """Make every pooled connection read-only before it is handed out.

        READ ONLY must succeed: a connection that could write is closed rather
        than used. The statement timeout is best effort -- max_execution_time is
        MySQL 5.7.8+, and a server without it should still serve lookups.
        """
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
            try:
                timeout_ms = _int_env(self.setting("QUERY_TIMEOUT_MS"),
                                      DEFAULT_QUERY_TIMEOUT_MS)
                cursor.execute(f"SET SESSION max_execution_time = {timeout_ms}")
            except Exception as e:
                logger.warning("Could not set a statement timeout on a tool database",
                               database=self.key, error=f"{type(e).__name__}: {e}")
        finally:
            cursor.close()

    def get_engine(self):
        """This database's engine, built once.

        URL.create rather than an f-string, so a password containing '@', ':'
        or '/' is escaped instead of silently changing the host.
        """
        if self._engine is not None:
            return self._engine
        with self._engine_lock:
            if self._engine is not None:
                return self._engine
            url = URL.create(
                "mysql+pymysql",
                username=get_required_env(self.setting("USERNAME")),
                password=get_required_env(self.setting("PASSWORD")),
                host=get_required_env(self.setting("HOST")),
                port=int(get_required_env(self.setting("PORT"), str(self.default_port))),
                # Required when the declaration names no default.
                database=get_required_env(self.setting("NAME"), self.default_name),
                query={"charset": "utf8mb4"},
            )
            engine = create_engine(
                url,
                pool_size=_int_env(self.setting("POOL_SIZE"), 5),
                max_overflow=_int_env(self.setting("MAX_OVERFLOW"), 5),
                pool_timeout=30,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={"connect_timeout": 10, "read_timeout": 30},
            )
            event.listen(engine, "connect", self.on_connect)
            self._engine = engine
            return self._engine

    def reset_engine(self) -> None:
        """Forget the engine, disposing of its pool. For tests."""
        with self._engine_lock:
            engine, self._engine = self._engine, None
        if engine is not None:
            engine.dispose()

    def _run_query(self, sql: str, params: dict) -> list:
        with self.get_engine().connect() as conn:
            return [dict(row) for row in conn.execute(text(sql), params).mappings()]

    def query(self, sql: str, params: dict) -> list:
        """Run one SELECT and return its rows as dicts.

        Callers pass fixed SQL; only values are bound. Retried on transient
        errors and guarded by this database's own breaker.
        """
        return self.breaker.call(self._retrying_query, sql, params)

    # -- running a lookup --------------------------------------------------

    @property
    def disabled_message(self) -> str:
        return (f"The {self.label} tools are switched off ({self.setting('ENABLED')} "
                f"is not true), so nothing was read. This is an evidence gap, not a "
                f"finding.")

    def run_lookup(self, build: Callable[[], dict], *, list_key: Optional[str] = None,
                   keep_from_end: bool = False) -> str:
        """Run `build` and return its result as JSON text; failures as one line.

        The failure lines must never read like an empty result: "the table has
        no row for this refid" can be evidence in its own right, and "the
        lookup did not run" is not.
        """
        if not self.is_enabled():
            return self.disabled_message
        try:
            result = build()
        except InvalidArgument as e:
            return f"Invalid argument: {e} Nothing was read."
        except pybreaker.CircuitBreakerError:
            logger.error("A tool database's circuit breaker is open; failing fast",
                         database=self.key)
            return (f"The {self.label} is failing and its circuit breaker is open, so "
                    f"nothing was read. This is an evidence gap, not a finding.")
        except Exception as e:
            logger.error("A tool database lookup failed", database=self.key,
                         error=f"{type(e).__name__}: {e}")
            return (f"The {self.label} lookup failed ({type(e).__name__}), so nothing "
                    f"was read. This is an evidence gap, not a finding.")
        return self.finish(result, list_key=list_key, keep_from_end=keep_from_end)

    def finish(self, result: dict, *, list_key: Optional[str] = None,
               keep_from_end: bool = False) -> str:
        """Serialise, shrinking `result[list_key]` until it fits the output cap.

        Entries are dropped whole, so the JSON stays valid and the output says
        how many were left out. `keep_from_end` keeps the last entries (the
        newest, for a chronological list); otherwise the first (the
        highest-ranked). A result still over the cap after that is cut as text,
        as a last resort.
        """
        limit = self.max_output_chars()
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
                       + f" ... [cut at {self.setting('MAX_OUTPUT_CHARS')}={limit}]")
        return textual


# ---------------------------------------------------------------------------
# The databases this process knows
# ---------------------------------------------------------------------------

_databases: dict = {}
_databases_lock = threading.Lock()


def declare(key: str, *, label: str, default_port: int = 3306,
            default_name: Optional[str] = None) -> Database:
    """The database `key`, made on first declaration and shared after it.

    A second declaration with the same settings -- another toolset reading the
    same database, or a module imported again -- returns the same object. One
    that disagrees is two toolsets describing one database differently, which
    is refused rather than letting whichever imported first win.
    """
    if not isinstance(key, str) or not _KEY.match(key):
        raise ValueError(f"Database key {key!r} must match {_KEY.pattern}.")
    with _databases_lock:
        existing = _databases.get(key)
        if existing is not None:
            if (existing.label, existing.default_port, existing.default_name) != \
                    (label, default_port, default_name):
                raise ValueError(f"Database {key!r} is declared twice with different "
                                 f"settings.")
            return existing
        database = Database(key, label=label, default_port=default_port,
                            default_name=default_name)
        _databases[key] = database
        return database


def databases() -> list:
    """Every declared database, by key."""
    with _databases_lock:
        return [_databases[key] for key in sorted(_databases)]


def breakers() -> dict:
    """{breaker name: breaker} for every declared database."""
    return {database.breaker_name: database.breaker for database in databases()}


def validate() -> list:
    """Boot errors: every database that is on has what it needs to connect."""
    errors = []
    for database in databases():
        errors.extend(database.missing_settings())
    return errors


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
