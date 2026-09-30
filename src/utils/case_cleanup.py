"""Local casebook directory cleanup.

Two layers:

1. **Immediate cleanup** -- ``cleanup_casebook_dir(event_id)`` removes the
   local ``casebook_{event_id}/`` directory right after ``save_terminal()``
   persists the terminal casebook to S3 (or the configured backend).  Called
   at every completion path in routes.py, dlt_routes.py, and
   message_adapters.py.

2. **Background reaper** -- ``reap_stale_casebooks()`` scans
   ``LOCAL_CASESHEETS_DIR`` on a timer and removes any ``casebook_*``
   directory older than ``CASEBOOK_LOCAL_TTL_SECONDS`` (default 1 hour).
   This catches directories left behind by crashes, timeouts, and any path
   where the immediate cleanup did not run.

The dedupe check (``storage.exists(..., terminal_only=True)``) reads from
the persistent backend (S3), not from local disk, so deleting a local
directory after the terminal casebook is persisted does not break
idempotency.  A redelivered packet will hit the S3 check and short-circuit.

That holds only when the persistent backend is somewhere else. With
``CASEBOOK_STORAGE_BACKEND=local`` the backend's root IS
``LOCAL_CASESHEETS_DIR``, and ``casebook_{event_id}/`` holds the only copy of
``casebook.json`` and ``status.json``. Deleting it erased every finished
casebook the moment it was saved, and a redelivery then passed the
terminal-casebook check and was analysed again (seen on 2026-09-29: two
FAILED_TIMEOUT packets redelivered ten minutes later were not recognised as
terminal). Neither layer touches a directory that holds a local record.
"""
import os
import shutil
import time
from pathlib import Path

from src.utils.logging_config import get_logger
from src.utils.paths import LOCAL_CASESHEETS_DIR

logger = get_logger(__name__)

#: The files LocalFilesystemCasebookStorage keeps a case's record in.
_RECORD_FILES = ("casebook.json", "status.json")


def _is_local_record(case_dir: Path) -> bool:
    """Whether `case_dir` is the local casebook backend's copy of a record.

    True when the casebook backend is the local filesystem, rooted at the
    directory `case_dir` sits in, and `case_dir` holds a record file. When
    the backend cannot be built, the directory is treated as a record: a
    working directory left behind costs disk, a deleted record costs a
    casebook.
    """
    if not any((case_dir / name).exists() for name in _RECORD_FILES):
        return False
    try:
        from src.storage.factory import get_casebook_storage
        from src.storage.local import LocalFilesystemCasebookStorage

        storage = get_casebook_storage()
    except Exception:
        return True
    if not isinstance(storage, LocalFilesystemCasebookStorage):
        return False
    return Path(storage.base_dir).resolve() == case_dir.parent.resolve()


def cleanup_casebook_dir(event_id: str) -> None:
    """Remove the local working directory for a completed case.

    Safe to call after ``storage.save_terminal()`` has persisted the
    terminal casebook.  The directory may contain harness working files
    (supported_logs.txt, context.json, investigation.json, review.json,
    dlt_evidence.txt, etc.), which are no longer needed once the case is
    terminal in the persistent backend. When the local backend is that
    persistent backend the directory is the record itself, and is kept.

    Never raises: a cleanup failure must not turn a successful case into
    a failure.
    """
    case_dir = LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"
    try:
        if _is_local_record(case_dir):
            logger.debug("Keeping local casebook directory; it holds the record",
                         event_id=event_id)
            return
        if case_dir.exists():
            shutil.rmtree(case_dir)
            logger.info("Cleaned up local casebook directory",
                        event_id=event_id)
    except Exception as e:
        logger.warning("Failed to clean up local casebook directory",
                       event_id=event_id,
                       error=f"{type(e).__name__}: {e}")


def reap_stale_casebooks(max_age_seconds: int = None) -> int:
    """Scan ``LOCAL_CASESHEETS_DIR`` and remove stale directories.

    A directory is stale when its mtime is older than *max_age_seconds*.
    Default is ``CASEBOOK_LOCAL_TTL_SECONDS`` env var (3600s = 1 hour).

    Returns the number of directories removed.
    """
    if max_age_seconds is None:
        max_age_seconds = int(
            os.environ.get("CASEBOOK_LOCAL_TTL_SECONDS", "3600")
        )

    if not LOCAL_CASESHEETS_DIR.is_dir():
        return 0

    cutoff = time.time() - max_age_seconds
    removed = 0

    for entry in sorted(LOCAL_CASESHEETS_DIR.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("casebook_"):
            continue
        try:
            if entry.stat().st_mtime < cutoff and not _is_local_record(entry):
                shutil.rmtree(entry)
                removed += 1
        except Exception as e:
            logger.warning("Reaper: failed to remove stale directory",
                           directory=entry.name,
                           error=f"{type(e).__name__}: {e}")

    if removed:
        logger.info("Reaper removed stale casebook directories",
                    count=removed, max_age_seconds=max_age_seconds)

    return removed


def _reaper_loop(interval_seconds: int = None) -> None:
    """Daemon loop that calls ``reap_stale_casebooks`` on a timer."""
    if interval_seconds is None:
        interval_seconds = int(
            os.environ.get("CASEBOOK_REAPER_INTERVAL_SECONDS", "300")
        )

    logger.info("Casebook reaper started",
                interval_seconds=interval_seconds)

    while True:
        try:
            time.sleep(interval_seconds)
            reap_stale_casebooks()
        except Exception as e:
            logger.warning("Reaper loop error",
                           error=f"{type(e).__name__}: {e}")


def start_reaper() -> None:
    """Launch the reaper as a daemon thread. Call once at startup."""
    import threading
    t = threading.Thread(target=_reaper_loop, daemon=True, name="casebook-reaper")
    t.start()
