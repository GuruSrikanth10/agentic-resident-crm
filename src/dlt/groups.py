"""Phase 7 of DLT_PLAN.md -- per-fingerprint group records.

A *group* is the durable record for one failure mode: how often it has been
seen, when it started, which cases belong to it, and the recommendation (if
any) an earlier investigation produced for it.

At ~2,000 messages/day with tens of distinct fingerprints, the group is what
turns a per-message cost into a per-*bug* cost. It is also the unit an operator
actually wants to read: "this bug has hit 431 packets since Tuesday" is a more
useful sentence than 431 individual casebooks.

`members` is capped. An uncapped list on a fingerprint seeing 400 hits/day
would grow without bound; `occurrence_count` keeps counting past the cap.

**Updates go through `CasebookStorage.update_json`, not load-then-save.** Both
mutations here are read-modify-write on a counter, and the DLT analysis role
is meant to scale out. This used to be guarded by a `filelock` under
`LOCAL_CHECKPOINTS_DIR` -- which coordinates processes on a shared filesystem
and does nothing at all for two pods on different nodes, while the group
records themselves were in S3. So on precisely the deployment the lock was
written for, every increment was a lost-update race and two pods analysing one
novel fingerprint could each write a different `recommendation`,
last-writer-wins. `update_json` puts the atomicity in the backend that can
actually provide it: a held lock locally, a conditional write on S3.

**That conditional write turned out not to be portable.** An S3-compatible
store that accepts `If-None-Match: *` but refuses `If-Match` creates every
group record and then refuses every update to it -- `occurrence_count` stuck
at 1, `recommendation` and `code_check` permanently null, reuse never firing,
with no contending writer anywhere. `group_store.py` is the replacement: the
same records, built only from create-only and blind writes, which every store
supports. It is the default (`DLT_GROUP_STORE=v2`). The single-document store
below is kept, selectable with `DLT_GROUP_STORE=v1`, as a rollback path only.
"""
import json
import os
import time
from typing import Optional

from src.dlt import group_store
from src.dlt.case_storage import get_group_storage
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_MEMBER_CAP = group_store.DEFAULT_MEMBER_CAP

#: Recommendation lifecycle. Nothing writes `final` in v1 -- there is no
#: review mechanism yet, so every reused recommendation stays explicitly
#: marked unreviewed (DLT_PLAN.md section 2).
STATE_NONE = "none"
STATE_DRAFT = "draft"
STATE_FINAL = "final"

#: Casebook-only states, never stored on a group -- they describe what
#: happened to *this* finding, so a reader of the casebook is never told a
#: recommendation was filed when it was not. The casebook used to assert
#: "draft" unconditionally, including on every case whose write had failed.
#:   unpersisted  the finding should have been cached and the write failed
#:   withheld     the finding carried packet-specific identifiers and was
#:                deliberately kept out of the cache (src/dlt/per_code.py)
STATE_UNPERSISTED = "unpersisted"
STATE_WITHHELD = "withheld"



def member_cap() -> int:
    return group_store.member_cap()


def store_version() -> str:
    """Which group store is live: "v2" (default) or "v1".

    Read on every call rather than at import, so a test -- or an operator
    rolling back -- can switch it without a restart.
    """
    raw = os.environ.get("DLT_GROUP_STORE", "v2").strip().lower()
    return "v1" if raw == "v1" else "v2"


def _v2() -> bool:
    return store_version() == "v2"


def _blank(fingerprint: str) -> dict:
    return {
        "fingerprint": fingerprint,
        "signature": "",
        "failure_class": "U",
        "business_code": None,
        "first_seen": None,
        "last_seen": None,
        "occurrence_count": 0,
        "members": [],
        "recommendation": None,
        "recommendation_state": STATE_NONE,
        "corroboration_history": {},
        # The most recent code-check verdict for this failure mode (phase C5).
        # A record, not a cache: cost control lives in `bitbucket.py`, whose
        # reads are already keyed on things that repeat within a group. This
        # is what `dlt_report --code-check` reads and what the accuracy loop
        # joins against, so it must reflect the last verdict, not the first.
        "code_check": None,
        "code_check_history": {},
    }


def load_group(fingerprint: str) -> Optional[dict]:
    """Read a group record, or None when the fingerprint is novel.

    An unreadable store also yields None: the reuse decision then runs the
    LLM, which is the safe default. It must never raise into the caller.
    """
    if not fingerprint:
        return None
    try:
        if _v2():
            return group_store.load_group(fingerprint)
        return get_group_storage().load(fingerprint, filename="group.json")
    except Exception as e:
        logger.warning("Could not load DLT group; treating as novel",
                       fingerprint=fingerprint[:16], error=f"{type(e).__name__}: {e}")
        return None


# `save_group` deliberately no longer exists. It was a plain, non-atomic
# `storage.save()` of a whole group record -- exactly the read-modify-write
# pattern that made concurrent occurrence counts lose increments. Leaving it
# beside `update_json` would have left a working, obvious, wrong way to write
# a group for the next caller to reach for. Use `record_occurrence` or
# `attach_recommendation`; both go through the atomic path.


def record_occurrence(fingerprint: str,
                      case_id: str,
                      signature: str = "",
                      failure_class: str = "U",
                      business_code: Optional[str] = None,
                      corroboration: Optional[str] = None,
                      ref_id: Optional[str] = None) -> dict:
    """Register one case against its fingerprint and return the group.

    Idempotent per case: a redelivered case that is already a member does not
    double-count. Without that, a redrive would inflate every occurrence count
    and make the cost model look better than it is.

    `case_id` must be the DLT case id -- `dlt-{topic}-{partition}-{offset}` --
    not the refId. Idempotency is keyed on it, and the same record redelivered
    under a different record key has a different refId but the same case id.
    `ref_id` is recorded beside it so an operator can still look the packet up.
    """
    if _v2():
        return group_store.record_occurrence(
            fingerprint, case_id, ref_id=ref_id, signature=signature,
            failure_class=failure_class, business_code=business_code,
            corroboration=corroboration)

    now = time.time()

    def mutate(current: Optional[dict]) -> dict:
        # Called under the backend's own atomicity guarantee, and possibly
        # more than once if a conditional write loses a race -- so everything
        # here is derived from `current`, never from a value read earlier.
        group = dict(current or _blank(fingerprint))

        already_member = case_id in group.get("members", [])
        if not already_member:
            group["occurrence_count"] = int(group.get("occurrence_count", 0)) + 1
            members = list(group.get("members", []))
            members.append(case_id)
            group["members"] = members[-member_cap():]

        group["signature"] = signature or group.get("signature") or ""
        group["failure_class"] = failure_class or group.get("failure_class") or "U"
        group["business_code"] = business_code or group.get("business_code")
        group["first_seen"] = group.get("first_seen") or now
        group["last_seen"] = now

        if corroboration and not already_member:
            history = dict(group.get("corroboration_history") or {})
            history[corroboration] = int(history.get(corroboration, 0)) + 1
            group["corroboration_history"] = history

        return group

    return get_group_storage().update_json(fingerprint, "group.json", mutate)


def attach_recommendation(fingerprint: str, recommendation: dict,
                          state: str = STATE_DRAFT,
                          by_case_id: Optional[str] = None) -> dict:
    """Record the recommendation an investigation produced for this group."""
    if _v2():
        return group_store.attach_recommendation(
            fingerprint, recommendation, state, by_case_id=by_case_id)

    def mutate(current: Optional[dict]) -> dict:
        group = dict(current or _blank(fingerprint))
        group["recommendation"] = recommendation
        group["recommendation_state"] = state
        return group

    return get_group_storage().update_json(fingerprint, "group.json", mutate)


def attach_code_check(fingerprint: str, record: dict,
                      by_case_id: Optional[str] = None) -> dict:
    """Record this group's latest code-check verdict, and count the verdicts
    it has seen. Phase C5.

    Goes through `update_json` like the other two mutators. A plain
    load-then-save would lose increments exactly as `record_occurrence` used
    to, and the DLT analysis role is meant to scale out.
    """
    if _v2():
        return group_store.attach_code_check(fingerprint, record,
                                             by_case_id=by_case_id)

    def mutate(current: Optional[dict]) -> dict:
        group = dict(current or _blank(fingerprint))
        group["code_check"] = record
        verdict = (record or {}).get("verdict")
        if verdict:
            history = dict(group.get("code_check_history") or {})
            history[verdict] = int(history.get(verdict, 0)) + 1
            group["code_check_history"] = history
        return group

    return get_group_storage().update_json(fingerprint, "group.json", mutate)


def has_usable_recommendation(group: Optional[dict]) -> bool:
    return bool(group
                and group.get("recommendation")
                and group.get("recommendation_state") in (STATE_DRAFT, STATE_FINAL))


def list_groups() -> list:
    """Every group record, newest activity first. For the operator CLI."""
    if _v2():
        return group_store.list_groups()

    storage = get_group_storage()
    groups = []
    try:
        identifiers = storage.list_events()
    except Exception as e:
        logger.warning("Could not list DLT groups", error=f"{type(e).__name__}: {e}")
        return []

    for fingerprint in identifiers:
        try:
            group = storage.load(fingerprint, filename="group.json")
        except Exception:
            continue
        if group:
            groups.append(group)
    return sorted(groups, key=lambda g: g.get("last_seen") or 0, reverse=True)


def as_json(group: dict) -> str:
    return json.dumps(group, indent=2, ensure_ascii=False, sort_keys=True)
