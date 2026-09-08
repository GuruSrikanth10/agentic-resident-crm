"""DLT case storage -- a separate root over the shared `CasebookStorage`.

DLT cases live under `dlt_cases/` (local) or the `dlt_cases` key prefix (S3),
not beside the rejection casebooks. They have a different schema, a different
lifecycle and a different audience, and mixing them would put DLT cases in
front of `accuracy_report`, `prune_casesheets` and every other tool that walks
`list_events()` expecting rejection casebooks.

Both backends already take a root, so this is configuration rather than a new
storage implementation: the protocol, the file-locking, the terminal-status
handling and the artifact methods are all reused as-is.

The root-scoping machinery itself now lives in `storage.factory`
(`get_scoped_storage`), because it is not DLT-specific -- the replay queue
needs the same thing.
"""
from typing import Optional

from src.storage.base import CasebookStorage
from src.storage.factory import get_scoped_storage, reset_scoped_cache

#: Subdirectory (local) or key prefix (S3) holding DLT cases.
DLT_ROOT_NAME = "dlt_cases"

#: Group records, one per fingerprint. Phase 7 writes these.
DLT_GROUPS_ROOT_NAME = "dlt_groups"

def get_dlt_storage() -> CasebookStorage:
    """Storage for individual DLT cases, keyed by `case_id`."""
    return get_scoped_storage(DLT_ROOT_NAME)


def get_group_storage() -> CasebookStorage:
    """Storage for per-fingerprint group records. Phase 7."""
    return get_scoped_storage(DLT_GROUPS_ROOT_NAME)


def reset_cache() -> None:
    """Drop cached storage handles. For tests, which swap backends per case."""
    reset_scoped_cache()


def terminal_status(case_id: str) -> Optional[str]:
    """Recorded terminal status for a case, or None. Never raises on a miss."""
    return get_dlt_storage().terminal_status(case_id)
