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
"""
import os
import shutil
import time

from src.utils.logging_config import get_logger
from src.utils.paths import LOCAL_CASESHEETS_DIR

logger = get_logger(__name__)


def cleanup_casebook_dir(event_id: str) -> None:
    """Remove the local working directory for a completed case.

    Safe to call after ``storage.save_terminal()`` has persisted the
    terminal casebook.  The directory may contain harness working files
    (supported_logs.txt, context.json, investigation.json, review.json,
    dlt_evidence.txt, etc.) and possibly casebook.json/status.json from a
    local storage backend -- all of which are no longer needed once the
    case is terminal in the persistent backend.

    Never raises: a cleanup failure must not turn a successful case into
    a failure.
    """
    case_dir = LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"
    try:
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
            if entry.stat().st_mtime < cutoff:
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
