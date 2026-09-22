"""Per-fingerprint group state, stored so it needs no conditional overwrite.

`groups.py` originally kept one `group.json` per fingerprint and mutated it
through `CasebookStorage.update_json` -- a compare-and-swap on S3. That is
correct against AWS. It is not correct against every S3-compatible store: one
that accepts `If-None-Match: *` but refuses `If-Match` will create each group
record and then silently refuse every update to it, forever, with no
contending writer anywhere. The failure looks exactly like contention, which
is what makes it expensive to diagnose -- eight retries, a warning, and a
dropped write, per case.

This module removes the requirement. Every write here is either **create-only**
or a **blind PUT to a name no other writer uses**, and both are universally
supported:

    meta.json                    blind PUT   signature, class, code, first_seen
    occurrences/<case_id>.json   create-only one object per case, ever
    recommendation.json          blind PUT   the cached finding
    code_check.json              blind PUT   the latest verdict
    code_checks/<key>.json       blind PUT   one per check, for the histogram

`occurrence_count` is the number of objects under `occurrences/`, not a
counter. That is the substantive change: a counter can lose an increment, and
recovers wrong when it does. A set of named objects cannot -- the same
`case_id` written twice is one object, so redelivery, replay and a retried
write all converge on the same count without anyone having to reason about
ordering. `corroboration_history` and `code_check_history` are derived the
same way, by reading objects rather than accumulating in place.

Last-writer-wins on `recommendation.json` is correct here rather than merely
tolerated. The finding is cached against a *fingerprint* and re-served to
every later packet with the same stack trace, so it must already be free of
packet-specific detail -- which means any two concurrent investigations of one
fingerprint produce equally valid answers, and keeping either is fine. The
record carries `by_case_id` and `written_at` so an operator can still see
which case produced the stored text.

Cost: every object under `occurrences/` and `code_checks/` is written
create-only and never modified, so each is fetched at most once per process
and then served from memory. Reading a group in steady state is therefore two
LISTs and three small GETs, plus one GET per object created since this
process last looked -- not one GET per occurrence, which would make a
fingerprint seeing hundreds of hits a day cost hundreds of requests per case.

`load_group` returns exactly the dict shape `groups.load_group` always did, so
no reader -- the reuse decision, the canned builder, dlt_report -- changes.
"""
import os
import threading
import time
import uuid
from typing import Optional

from cachetools import LRUCache

from src.dlt.case_storage import get_group_storage
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

META_FILENAME = "meta.json"
RECOMMENDATION_FILENAME = "recommendation.json"
CODE_CHECK_FILENAME = "code_check.json"
OCCURRENCES_SUBDIR = "occurrences"
CODE_CHECKS_SUBDIR = "code_checks"

#: The pre-v2 single-document record. Read for groups that predate this
#: layout, never written. See `_load_legacy`.
LEGACY_FILENAME = "group.json"

SCHEMA_VERSION = "2.0"

DEFAULT_MEMBER_CAP = 200

#: How long a composed group may be served from memory. Short, because the
#: reuse decision reads it and a stale "no recommendation on file" costs an
#: LLM call -- but non-zero, because `list_groups` and the report would
#: otherwise re-walk every fingerprint on every call. Every write in this
#: process invalidates its own entry, so the TTL only governs how quickly a
#: *different* process's write becomes visible here.
CACHE_TTL_SECONDS = float(os.environ.get("DLT_GROUP_CACHE_TTL_SECONDS", "5"))


def member_cap() -> int:
    try:
        return max(1, int(os.environ.get("DLT_GROUP_MEMBER_CAP",
                                         str(DEFAULT_MEMBER_CAP))))
    except (ValueError, TypeError):
        return DEFAULT_MEMBER_CAP


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

_cache: dict = {}
_cache_lock = threading.Lock()

#: Returned by `_cache_get` on a miss. Distinct from None, which is a cached
#: answer meaning "this fingerprint is novel".
_MISS = object()


def _scope(storage) -> str:
    """Identify which store a cached value came from.

    The caches below are keyed on this as well as the fingerprint. Keyed on
    the fingerprint alone, a value read from one store would be served for
    another -- which is exactly what happens between tests, each of which
    gets a fresh root but reuses the same handful of fingerprints.
    """
    base_dir = getattr(storage, "base_dir", None)
    if base_dir is not None:
        return f"file://{base_dir}"
    bucket = getattr(storage, "bucket", None)
    if bucket is not None:
        return f"s3://{bucket}/{getattr(storage, 'prefix', '')}"
    return f"obj:{id(storage)}"


def _cache_get(scope: str, fingerprint: str):
    with _cache_lock:
        entry = _cache.get((scope, fingerprint))
    if entry and (time.time() - entry[0]) < CACHE_TTL_SECONDS:
        return entry[1]
    return _MISS


def _cache_put(scope: str, fingerprint: str, group: Optional[dict]) -> None:
    with _cache_lock:
        _cache[(scope, fingerprint)] = (time.time(), group)


def invalidate(fingerprint: str, storage=None) -> None:
    """Drop the cached composition, so the next read sees our own write."""
    scope = _scope(storage or get_group_storage())
    with _cache_lock:
        _cache.pop((scope, fingerprint), None)


#: Immutable per-item records, keyed (fingerprint, subdir, name). Safe to hold
#: indefinitely because nothing in this module ever rewrites one -- both
#: subdirs are written create-only. Bounded so a long-lived process does not
#: grow without limit; an eviction costs one GET, not correctness.
_records = LRUCache(maxsize=int(os.environ.get("DLT_GROUP_RECORD_CACHE_SIZE", "50000")))
_records_lock = threading.Lock()


def _remember(scope: str, fingerprint: str, subdir: str, name: str,
              record: dict) -> None:
    with _records_lock:
        _records[(scope, fingerprint, subdir, name)] = {**record, "_name": name}


def reset_cache() -> None:
    with _cache_lock:
        _cache.clear()
    with _records_lock:
        _records.clear()


def _stamp() -> dict:
    """Wall-clock time plus a tiebreaker.

    `time.time()` ties within a fast loop (and routinely on Windows, whose
    clock is coarse), which would leave arrival order -- and so which members
    survive the cap -- up to the filesystem's listing order. `perf_counter_ns`
    is monotonic within a process and breaks those ties. It is meaningless
    across processes, where it only ever decides between two genuinely
    simultaneous arrivals whose order is arbitrary anyway.
    """
    return {"at": time.time(), "seq": time.perf_counter_ns()}


def _order_key(record: dict):
    return (record.get("at") or 0.0, record.get("seq") or 0)


def _reload(fingerprint: str) -> dict:
    """The group after a write, or {} if it cannot be read back.

    A failure here is a failed *read*, not a failed write -- the write above
    it has already landed. Letting it raise would report a persisted
    recommendation as lost, and the casebook would say so.
    """
    try:
        return load_group(fingerprint) or {}
    except Exception as e:
        logger.warning("Wrote the group but could not read it back",
                       fingerprint=fingerprint[:16], error=f"{type(e).__name__}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def record_occurrence(fingerprint: str,
                      case_id: str,
                      ref_id: Optional[str] = None,
                      signature: str = "",
                      failure_class: str = "U",
                      business_code: Optional[str] = None,
                      corroboration: Optional[str] = None) -> dict:
    """Register one case against its fingerprint. Returns the composed group.

    Idempotent per `case_id` by construction: the occurrence is an object
    named after the case and written create-only, so writing it twice is
    writing it once. There is no counter to double-count and no member list
    to append to twice.
    """
    storage = get_group_storage()
    record = {"case_id": case_id, "ref_id": ref_id,
              "corroboration": corroboration, **_stamp()}

    created = storage.create_json(
        fingerprint, f"{OCCURRENCES_SUBDIR}/{case_id}.json", dict(record))
    if created:
        # We know exactly what we just wrote; no need to read it back.
        _remember(_scope(storage), fingerprint, OCCURRENCES_SUBDIR, case_id, record)

    # Descriptive rather than accumulated: every case with this fingerprint
    # agrees on these fields by definition, so a blind PUT loses nothing.
    # `first_seen` is also recomputed from the occurrences at read time, so a
    # race here can only ever make this copy of it slightly late, never wrong
    # in what `load_group` reports.
    existing = storage.load(fingerprint, filename=META_FILENAME) or {}
    storage.save(fingerprint, {
        "fingerprint": fingerprint,
        "signature": signature or existing.get("signature") or "",
        "failure_class": failure_class or existing.get("failure_class") or "U",
        "business_code": business_code or existing.get("business_code"),
        "first_seen": existing.get("first_seen") or record["at"],
    }, filename=META_FILENAME)

    invalidate(fingerprint, storage)
    if not created:
        logger.debug("Occurrence already recorded for this case",
                     fingerprint=fingerprint[:16], case_id=case_id)
    return _reload(fingerprint)


def attach_recommendation(fingerprint: str, recommendation: dict,
                          state: str, by_case_id: Optional[str] = None) -> dict:
    """Cache the finding this fingerprint's investigation produced."""
    storage = get_group_storage()
    storage.save(fingerprint, {
        "recommendation": recommendation,
        "recommendation_state": state,
        "by_case_id": by_case_id,
        "written_at": time.time(),
    }, filename=RECOMMENDATION_FILENAME)
    invalidate(fingerprint, storage)
    return _reload(fingerprint)


def attach_code_check(fingerprint: str, record: dict,
                      by_case_id: Optional[str] = None) -> dict:
    """Record a code-check verdict: as the group's latest, and in its history.

    `code_check.json` always takes the newest verdict, so the report shows
    the current state of the fix. The history is one create-only object per
    check: keyed on the case when there is one, so a redelivered case is
    counted once (as its occurrence and corroboration verdict are); keyed
    uniquely otherwise, so every call is counted -- which is what the
    single-document store did and what `dlt_report --code-check` expects.
    """
    storage = get_group_storage()
    stamp = _stamp()

    storage.save(fingerprint, {
        "code_check": record,
        "by_case_id": by_case_id,
        "written_at": stamp["at"],
    }, filename=CODE_CHECK_FILENAME)

    verdict = (record or {}).get("verdict")
    if verdict:
        key = by_case_id or f"anon-{uuid.uuid4().hex}"
        entry = {"verdict": verdict, "by_case_id": by_case_id, **stamp}
        if storage.create_json(fingerprint, f"{CODE_CHECKS_SUBDIR}/{key}.json",
                               dict(entry)):
            _remember(_scope(storage), fingerprint, CODE_CHECKS_SUBDIR, key, entry)

    invalidate(fingerprint, storage)
    return _reload(fingerprint)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def _read_all(storage, fingerprint: str, subdir: str) -> list:
    """Every record under `subdir`, fetching only the ones not yet seen."""
    scope = _scope(storage)
    records = []
    for filename in storage.list_json(fingerprint, subdir):
        name = filename[:-len(".json")]
        key = (scope, fingerprint, subdir, name)
        with _records_lock:
            record = _records.get(key)
        if record is None:
            loaded = storage.load(fingerprint, filename=f"{subdir}/{filename}")
            if not loaded:
                continue
            record = {**loaded, "_name": name}
            with _records_lock:
                _records[key] = record
        records.append(record)
    return records


def load_group(fingerprint: str) -> Optional[dict]:
    """Compose the group record from its parts, or None when novel.

    Raises if the store cannot be read at all -- callers decide how to
    degrade. `groups.load_group` turns that into "treat as novel", which is
    the documented contract.
    """
    if not fingerprint:
        return None

    storage = get_group_storage()
    scope = _scope(storage)

    cached = _cache_get(scope, fingerprint)
    if cached is not _MISS:
        return cached

    occurrences = _read_all(storage, fingerprint, OCCURRENCES_SUBDIR)
    checks = _read_all(storage, fingerprint, CODE_CHECKS_SUBDIR)
    meta = storage.load(fingerprint, filename=META_FILENAME) or {}
    recommendation = storage.load(fingerprint, filename=RECOMMENDATION_FILENAME) or {}
    code_check = storage.load(fingerprint, filename=CODE_CHECK_FILENAME) or {}
    legacy = _load_legacy(storage, fingerprint) or {}

    # A recommendation can be attached before the first occurrence lands --
    # any one of these parts on its own means the group exists.
    if not (occurrences or checks or meta or recommendation or code_check or legacy):
        _cache_put(scope, fingerprint, None)
        return None

    # --- Fold in the frozen v1 record (see `_load_legacy`). ---------------
    raw_members = legacy.get("members")
    legacy_members = ([m for m in raw_members if isinstance(m, str) and m]
                      if isinstance(raw_members, list) else [])
    legacy_ids = set(legacy_members)
    occurrences.sort(key=_order_key)
    # An occurrence the v1 record already lists is not counted again. v1
    # members were refIds (the old call site passed the wrong identifier) or,
    # after that fix, case ids -- so either may match.
    fresh = [r for r in occurrences
             if not ({r.get("case_id"), r.get("ref_id")} & legacy_ids)]

    corroboration_history = _counts(legacy.get("corroboration_history"))
    for record in fresh:
        verdict = record.get("corroboration")
        if verdict:
            corroboration_history[verdict] = corroboration_history.get(verdict, 0) + 1

    code_check_history = _counts(legacy.get("code_check_history"))
    for record in checks:
        verdict = record.get("verdict")
        if verdict:
            code_check_history[verdict] = code_check_history.get(verdict, 0) + 1

    times = [r["at"] for r in fresh if r.get("at") is not None]
    starts = [t for t in (meta.get("first_seen"), legacy.get("first_seen"), *times)
              if t is not None]
    ends = [t for t in (legacy.get("last_seen"), *times) if t is not None]

    if not recommendation and legacy.get("recommendation"):
        recommendation = {
            "recommendation": legacy["recommendation"],
            "recommendation_state": legacy.get("recommendation_state", "none"),
        }
    if not code_check and legacy.get("code_check"):
        code_check = {"code_check": legacy["code_check"]}

    members = legacy_members + [r.get("case_id") or r["_name"] for r in fresh]

    group = {
        "fingerprint": fingerprint,
        "signature": meta.get("signature") or legacy.get("signature") or "",
        "failure_class": (meta.get("failure_class") or legacy.get("failure_class")
                          or "U"),
        "business_code": meta.get("business_code") or legacy.get("business_code"),
        "first_seen": min(starts, default=None),
        "last_seen": max(ends, default=min(starts, default=None)),
        # The v1 count, not len(v1 members): v1 capped the list and kept
        # counting past the cap.
        "occurrence_count": _int(legacy.get("occurrence_count")) + len(fresh),
        # Newest kept, in arrival order -- the same contract as the member
        # cap in the single-document store. The count keeps going past it.
        "members": members[-member_cap():],
        "recommendation": recommendation.get("recommendation"),
        "recommendation_state": recommendation.get("recommendation_state", "none"),
        "recommendation_by_case_id": recommendation.get("by_case_id"),
        "corroboration_history": corroboration_history,
        "code_check": code_check.get("code_check"),
        "code_check_history": code_check_history,
        "schema_version": SCHEMA_VERSION,
    }
    _cache_put(scope, fingerprint, group)
    return group


def _int(value) -> int:
    """A non-negative int, or 0. A corrupt v1 field must not make every read
    of the group fail -- that would treat the fingerprint as novel forever."""
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _counts(raw) -> dict:
    """A copy of a verdict histogram, tolerating a malformed one."""
    out: dict = {}
    for key, value in (raw or {}).items() if isinstance(raw, dict) else ():
        try:
            out[key] = int(value)
        except (TypeError, ValueError):
            continue
    return out


def _load_legacy(storage, fingerprint: str) -> Optional[dict]:
    """The pre-v2 `group.json`, read and never written.

    Nothing in this module writes `group.json`, so under v2 it is a frozen
    snapshot of everything the single-document store accumulated. Rather
    than migrate it -- a one-shot job that would race live writers, and one
    more step to forget -- `load_group` folds it in on every read: its count,
    members, histories and cached recommendation all carry across the
    cutover, and occurrences it already lists are not counted twice.

    Rolling back to v1 (`DLT_GROUP_STORE=v1`) and forward again stays
    consistent too: v1 appends to this file, and anything it records there
    is simply part of the baseline next time v2 reads it.

    One ambiguity is accepted. Before the call-site fix, v1 members were
    refIds, and one packet can in principle dead-letter twice with the same
    stack trace under two case ids; a post-cutover occurrence sharing a v1
    member's refId is treated as the same occurrence. That errs toward an
    undercount of genuine repeats rather than an overcount of redeliveries.
    """
    try:
        legacy = storage.load(fingerprint, filename=LEGACY_FILENAME)
    except Exception:
        return None
    return legacy if isinstance(legacy, dict) else None


def list_groups() -> list:
    """Every group record, newest activity first."""
    storage = get_group_storage()
    try:
        identifiers = storage.list_events()
    except Exception as e:
        logger.warning("Could not list DLT groups", error=f"{type(e).__name__}: {e}")
        return []

    groups = []
    for fingerprint in identifiers:
        try:
            group = load_group(fingerprint)
        except Exception as e:
            logger.warning("Could not load DLT group", fingerprint=fingerprint[:16],
                           error=f"{type(e).__name__}: {e}")
            continue
        if group:
            groups.append(group)
    return sorted(groups, key=lambda g: g.get("last_seen") or 0, reverse=True)
