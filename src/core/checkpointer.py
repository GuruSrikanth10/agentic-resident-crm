"""
Checkpointer selection (ENHANCEMENT_PLAN.md 4.7).

`SqliteSaver` on a local file is the second of the three things pinning this
system to one node (S3CasebookStorage removed the first). Two API replicas
cannot share a SQLite file across pods, so a packet checkpointed by one
replica cannot be resumed by another -- which defeats the resume-from-
checkpoint path that MAX_IN_PROGRESS_AGE_SECONDS and DLQ replay rely on.

CHECKPOINT_BACKEND selects:
  sqlite    -- local file, the default, unchanged behaviour
  postgres  -- shared, suitable for multiple replicas

Postgres is imported lazily and its driver is an optional dependency: a
single-node deployment must not be forced to install it.
"""
import os
import sqlite3
import threading

from src.utils.logging_config import get_logger
from src.utils.paths import CHECKPOINT_DB_PATH

logger = get_logger(__name__)

# The built checkpointer, cached for the process lifetime.
#
# This used to be rebuilt on every call, and /ready called it on every probe
# (G4). Each postgres build opened a connection, ran setup()'s DDL, and
# returned a saver that was never closed -- an orchestrator probing every 5s
# exhausted max_connections within the hour and took down every replica
# sharing the database, so the readiness probe became the outage. The SQLite
# path leaked file handles on the same curve.
#
# Keyed on the resolved configuration so a config change still rebuilds.
_checkpointer = None
_checkpointer_key = None
_checkpointer_lock = threading.Lock()
#: Held so health_check() can probe the live connection instead of opening one.
_sqlite_conn = None


def backend_name() -> str:
    return os.environ.get("CHECKPOINT_BACKEND", "sqlite").strip().lower()


def _resolve_config() -> tuple:
    """Return (backend, cache_key), raising on a misconfiguration.

    Validation happens here, ahead of the cache, so a bad config fails loudly
    on every call rather than only on the first one.
    """
    backend = backend_name()

    if backend == "postgres":
        uri = os.environ.get("CHECKPOINT_POSTGRES_URI", "").strip()
        if not uri:
            raise ValueError(
                "CHECKPOINT_BACKEND=postgres requires CHECKPOINT_POSTGRES_URI."
            )
        return backend, ("postgres", uri)

    if backend != "sqlite":
        raise ValueError(
            f"Unknown CHECKPOINT_BACKEND {backend!r}; expected 'sqlite' or 'postgres'."
        )

    return backend, ("sqlite", str(CHECKPOINT_DB_PATH))


def _build_sqlite():
    global _sqlite_conn
    from langgraph.checkpoint.sqlite import SqliteSaver

    os.makedirs(CHECKPOINT_DB_PATH.parent, exist_ok=True)
    conn = sqlite3.connect(str(CHECKPOINT_DB_PATH), check_same_thread=False)
    # WAL lets readers proceed during a write, which matters because several
    # packet threads share this one connection.
    conn.execute("PRAGMA journal_mode=WAL;")
    _sqlite_conn = conn
    logger.info("Checkpointer ready", backend="sqlite", path=str(CHECKPOINT_DB_PATH))
    return SqliteSaver(conn)


def _build_postgres():
    uri = os.environ.get("CHECKPOINT_POSTGRES_URI", "").strip()
    if not uri:
        raise ValueError(
            "CHECKPOINT_BACKEND=postgres requires CHECKPOINT_POSTGRES_URI."
        )

    try:
        from langgraph.checkpoint.postgres import PostgresSaver
    except ImportError as e:
        raise ImportError(
            "CHECKPOINT_BACKEND=postgres requires langgraph-checkpoint-postgres: "
            "pip install langgraph-checkpoint-postgres"
        ) from e

    checkpointer = PostgresSaver.from_conn_string(uri)
    # Enter the context manager's setup explicitly: from_conn_string returns a
    # context manager, but the graph outlives any `with` block here.
    if hasattr(checkpointer, "__enter__"):
        checkpointer = checkpointer.__enter__()

    # Idempotent; creates the checkpoint tables on first run.
    checkpointer.setup()
    logger.info("Checkpointer ready", backend="postgres")
    return checkpointer


def get_checkpointer():
    """Return the process-wide checkpointer, building it at most once.

    A misconfigured postgres backend fails loudly rather than silently falling
    back to SQLite: a multi-replica deployment that quietly used per-pod local
    checkpoints would look healthy while losing every cross-replica resume.
    That check runs before the cache, so it is not skipped on later calls.
    """
    global _checkpointer, _checkpointer_key

    backend, key = _resolve_config()

    with _checkpointer_lock:
        if _checkpointer is not None and _checkpointer_key == key:
            return _checkpointer

        _checkpointer = _build_postgres() if backend == "postgres" else _build_sqlite()
        _checkpointer_key = key
        return _checkpointer


def health_check() -> bool:
    """Is the checkpoint store reachable right now?

    Exists so /ready can answer that question without constructing a
    checkpointer, which is what made the readiness probe a connection leak
    (G4). Probes the live connection the graph is already using, so it also
    detects a connection that has since dropped -- which rebuilding a fresh
    saver every probe never did.

    Deliberate behaviour change: the old probe required CHECKPOINT_DB_PATH to
    already exist and 503'd otherwise. On a fresh SQLite deployment nothing
    creates that file until the first packet builds the graph, and no packet
    can arrive while the pod is unready -- so the probe never recovered. This
    builds (once) through the cache instead, which makes "ready" mean the
    store is reachable AND initialised.
    """
    checkpointer = get_checkpointer()

    if backend_name() == "sqlite":
        if _sqlite_conn is None:
            return False
        _sqlite_conn.execute("SELECT 1")
        return True

    # psycopg connections and pools both expose execute(); if this saver
    # exposes neither, a successfully built saver is the best signal available.
    conn = getattr(checkpointer, "conn", None)
    if conn is None or not hasattr(conn, "execute"):
        return checkpointer is not None
    conn.execute("SELECT 1")
    return True


def reset_checkpointer():
    """Drop the cached checkpointer. For tests and config reloads."""
    global _checkpointer, _checkpointer_key, _sqlite_conn
    with _checkpointer_lock:
        _checkpointer = None
        _checkpointer_key = None
        _sqlite_conn = None
