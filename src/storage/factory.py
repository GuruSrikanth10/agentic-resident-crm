import os
import threading

from src.storage.base import CasebookStorage
from src.storage.local import LocalFilesystemCasebookStorage
from src.storage.s3 import S3CasebookStorage

_STORAGE_CACHE = None
# Racing callers each built a backend. Harmless for the local one, but the S3
# constructor resolves credentials and the duplicate is simply discarded --
# and this is now reached from several threads at once, since the API's
# storage calls moved off the event loop.
_STORAGE_LOCK = threading.Lock()


def get_casebook_storage() -> CasebookStorage:
    global _STORAGE_CACHE
    if _STORAGE_CACHE is not None:
        return _STORAGE_CACHE

    with _STORAGE_LOCK:
        if _STORAGE_CACHE is not None:
            return _STORAGE_CACHE

        backend = os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").lower()
        if backend == "s3":
            _STORAGE_CACHE = S3CasebookStorage()
        else:
            _STORAGE_CACHE = LocalFilesystemCasebookStorage()

        return _STORAGE_CACHE


def reset_storage_cache() -> None:
    """Drop the cached backend. For tests, which swap backends per case."""
    global _STORAGE_CACHE
    with _STORAGE_LOCK:
        _STORAGE_CACHE = None
    reset_scoped_cache()


# ---------------------------------------------------------------------------
# Scoped roots
# ---------------------------------------------------------------------------
# A second, third, ... namespace under the same backend: `dlt_cases`,
# `dlt_groups`, `pending_replays`. Each is a subdirectory locally and a key
# prefix on S3, so the protocol, the locking and the artifact methods are all
# reused rather than reimplemented.
#
# Generalised out of src/dlt/case_storage.py, which had exactly this and was
# the only place that could create one -- so the replay queue, which needs the
# same thing and is not DLT-specific, wrote to local disk instead.

_SCOPED_CACHE: dict = {}
_SCOPED_LOCK = threading.Lock()


def _build_scoped(root_name: str) -> CasebookStorage:
    backend = os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").lower()
    if backend == "s3":
        base = (os.environ.get("CASEBOOK_S3_PREFIX") or "").strip("/")
        prefix = f"{base}/{root_name}" if base else root_name
        return S3CasebookStorage(prefix=prefix)

    from src.utils.paths import LOCAL_CASESHEETS_DIR

    return LocalFilesystemCasebookStorage(
        base_dir=str(LOCAL_CASESHEETS_DIR / root_name))


def get_scoped_storage(root_name: str) -> CasebookStorage:
    """Storage rooted at its own subdirectory / key prefix."""
    with _SCOPED_LOCK:
        existing = _SCOPED_CACHE.get(root_name)
        if existing is not None:
            return existing

    built = _build_scoped(root_name)
    with _SCOPED_LOCK:
        return _SCOPED_CACHE.setdefault(root_name, built)


def reset_scoped_cache() -> None:
    """Drop cached scoped handles. For tests, which swap backends per case."""
    with _SCOPED_LOCK:
        _SCOPED_CACHE.clear()
