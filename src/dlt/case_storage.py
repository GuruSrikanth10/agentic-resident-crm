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
import time
from typing import Optional

from src.storage.base import CasebookStorage
from src.storage.factory import get_scoped_storage, reset_scoped_cache

#: Subdirectory (local) or key prefix (S3) holding DLT cases.
DLT_ROOT_NAME = "dlt_cases"

#: Group records, one per fingerprint. Phase 7 writes these.
DLT_GROUPS_ROOT_NAME = "dlt_groups"

#: Bumped when the DLT casebook shape changes. Independent of the rejection
#: casebook's CASEBOOK_SCHEMA_VERSION -- different schema, different lifecycle.
#: Defined here rather than in `api/dlt_routes.py` (which re-exports it) so the
#: consumer, which writes a casebook of its own on a timeout, does not import
#: the API to stamp it.
# 1.1 adds the `code_check` block (DLT_PLAN.md 14, phase C5).
# 1.2 adds provenance.group_state and provenance.single_flight, and
#     finding.per_code_violations; recommendation_state now reports what
#     happened ("unpersisted", "withheld", "none") instead of always "draft".
#     Additive: every 1.1 field is unchanged.
# 1.3 adds packet.event_id, packet.service, packet.service_resolution,
#     failure.fingerprint_service and provenance.service_pack
#     (MULTI_SERVICE_PLAN.md Phase 8). Additive.
# 1.4 adds finding.reviewer_approved and recommendation_state
#     "per_occurrence"; confidence.ceilings_applied may name "unreviewed";
#     finding.discrepancy is null unless the corroboration verdict was
#     CONTRADICTED or PARTIAL. Both timeout casebooks -- the API's and the
#     consumer's -- are now this schema, with `case_id` the DLT case id and
#     the refId under `packet`; the consumer's was a rejection-shaped record
#     stamped with the rejection schema version.
# The shape itself is src/models/dlt_casebook.py, published to readers as
# casebook_structures/dlt_casebook.schema.json: a bump changes all three.
DLT_CASEBOOK_SCHEMA_VERSION = "1.4"


def timeout_casebook(case_id: Optional[str], ref_id: Optional[str],
                     narrative: str) -> dict:
    """The terminal casebook for a case that ran out of time before it
    produced a finding: the API's own budget, or the consumer's.

    One builder, so the two read alike: `dlt_report` finds both by case id
    or by refId, as it finds a finished case.
    """
    return {
        "schema_version": DLT_CASEBOOK_SCHEMA_VERSION,
        "case_id": case_id,
        "detected_at": time.time(),
        "packet": {"ref_id": ref_id},
        "finding": {
            "narrative": narrative,
            "recommendation": "A human should read the attached evidence.",
            "action": "NEEDS_MANUAL_REVIEW",
        },
        "packet_status": {"status": "FAILED_TIMEOUT"},
    }


def get_dlt_storage() -> CasebookStorage:
    """Storage for individual DLT cases, one per record, keyed by
    `identity.storage_key` (`<refId>__<digest of case_id>`; before
    MULTI_SERVICE_PLAN.md Phase 8, the bare refId)."""
    return get_scoped_storage(DLT_ROOT_NAME)


def keys_for_ref_id(ref_id: str) -> list:
    """Every stored case of the packet `ref_id`, sorted: each record's own key
    and a case stored under the bare refId before Phase 8. How an operator
    who knows only the refId finds its cases."""
    from src.dlt.identity import STORAGE_KEY_SEPARATOR

    prefix = f"{ref_id}{STORAGE_KEY_SEPARATOR}"
    return sorted(key for key in get_dlt_storage().list_events()
                  if key == ref_id or key.startswith(prefix))


def get_group_storage() -> CasebookStorage:
    """Storage for per-fingerprint group records. Phase 7."""
    return get_scoped_storage(DLT_GROUPS_ROOT_NAME)


def reset_cache() -> None:
    """Drop cached storage handles. For tests, which swap backends per case.

    Also drops the group store's in-memory compositions. They are keyed on the
    storage root and so cannot leak between roots, but a test that reuses one
    root and expects a clean slate should get one.
    """
    reset_scoped_cache()
    # Imported here: group_store imports this module.
    from src.dlt import group_store
    group_store.reset_cache()


def terminal_status(ref_id: str) -> Optional[str]:
    """Recorded terminal status for a case, or None. Never raises on a miss."""
    return get_dlt_storage().terminal_status(ref_id)
