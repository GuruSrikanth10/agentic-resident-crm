"""
Atomic file replacement that survives a Windows file lock.

Every durable write in this project follows the same discipline: write a
`.tmp`, then `os.replace` it over the destination, so a reader never sees a
half-written file and a crash mid-write leaves the previous version intact.

That discipline is only unconditionally safe on POSIX. `rename(2)` succeeds
regardless of who holds the destination open -- readers keep their handle to
the old inode. On Windows the same call is `MoveFileEx(REPLACE_EXISTING)`,
which fails with `ERROR_ACCESS_DENIED` (WinError 5) for as long as any other
process holds either path open without `FILE_SHARE_DELETE`. A checkout under
a synced or indexed directory guarantees such holders: OneDrive uploading,
the search indexer, an AV real-time scan sweeping a directory that was just
written into.

Those holders release in milliseconds, so the failure is transient -- but
until this existed, each site raised on the first denial. In
`LocalFilesystemCasebookStorage.save` that surfaced as an unhandled 500 out
of `POST /fetch-logs`, which the fast consumer turned into a DLQ entry: a
legitimate packet dead-lettered by an antivirus scan.

Retrying, and only then re-raising, is the whole fix. Callers that must not
lose data (the casebook writes, the runbook writes) let the final failure
propagate exactly as before, so a genuinely unwritable disk still reaches the
DLQ. Callers that can drop a write (the consumer heartbeat) catch it
themselves.
"""
import os
import time
from typing import Optional

# Five attempts at a 100ms base gives ~1s of total grace (0.1+0.2+0.3+0.4),
# which comfortably outlasts an AV or indexer handle while staying well
# inside an HTTP request's budget.
DEFAULT_ATTEMPTS = max(1, int(os.environ.get("ATOMIC_REPLACE_ATTEMPTS", "5")))
DEFAULT_BACKOFF_SECONDS = float(os.environ.get("ATOMIC_REPLACE_BACKOFF_SECONDS", "0.1"))


def replace_with_retry(src, dst, *, attempts: Optional[int] = None,
                       backoff: Optional[float] = None, abort=None) -> None:
    """`os.replace(src, dst)`, retried through a transient lock.

    Raises the last error once the attempts are exhausted, so a caller that
    treats a failed write as fatal keeps doing exactly that -- this widens the
    window, it does not swallow the failure.

    `abort`, when given, is a `threading.Event` that cuts the backoff short:
    callers on a shutdown path stop retrying the moment a drain begins rather
    than spending their termination budget here. Aborting still raises, since
    the write did not happen.

    On POSIX this retries a path that effectively never fails, which costs
    nothing and keeps one implementation for both platforms.
    """
    attempts = DEFAULT_ATTEMPTS if attempts is None else max(1, attempts)
    backoff = DEFAULT_BACKOFF_SECONDS if backoff is None else backoff

    last_error = None
    for attempt in range(attempts):
        try:
            os.replace(src, dst)
            return
        except OSError as e:
            last_error = e

        if attempt + 1 == attempts:
            break

        delay = backoff * (attempt + 1)
        if abort is not None:
            if abort.wait(delay):
                break
        else:
            time.sleep(delay)

    raise last_error
