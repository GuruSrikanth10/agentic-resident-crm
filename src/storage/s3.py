"""
S3 casebook storage (ENHANCEMENT_PLAN.md 4.7).

The local backend pins the system to one node: `filelock` coordinates
processes on a shared filesystem, which two pods on different nodes do not
have. This backend removes that ceiling.

Atomicity differs from the local backend, and simplifies:
`LocalFilesystemCasebookStorage` needs `.tmp` + `os.replace` because a
partially written local file is readable. An S3 `PutObject` is atomic at the
object level -- a reader sees either the previous object or the complete new
one, never a partial write -- so no temp-key dance is needed.

What S3 does NOT give us is mutual exclusion. Two processes writing the same
key concurrently produce last-writer-wins rather than a merge. That is
acceptable here for the same reason it is acceptable locally: the idempotency
guard in routes.py, the terminal-status check, and the Kafka dedupe check all
run before a write, so concurrent writes to one event are already the
exceptional path rather than the norm.
"""
import json
import os
import random
import time
import threading
from typing import Optional

from src.storage.base import (
    CASEBOOK_SCHEMA_VERSION,
    CasebookStorage,
    TERMINAL_STATUSES,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Reused across calls, like the uploader's client (F20).
_s3_client = None
_s3_client_lock = threading.Lock()

#: How many times `update_json` re-reads and retries after losing a
#: conditional write. Contention is between the handful of pods analysing the
#: same fingerprint at once, so this only has to outlast a short burst.
UPDATE_MAX_ATTEMPTS = int(os.environ.get("S3_UPDATE_MAX_ATTEMPTS", "8"))

#: Backoff bounds for that retry. The previous form was
#: `random.uniform(0, min(attempt * 0.05, 0.3))` over a zero-based `attempt`,
#: which slept exactly zero on the first retry and capped at 0.3s -- so the
#: first two rounds of a real contention burst were a guaranteed tie and the
#: whole budget was spent inside ~1.2s.
UPDATE_BASE_BACKOFF = float(os.environ.get("S3_UPDATE_BASE_BACKOFF", "0.05"))
UPDATE_MAX_BACKOFF = float(os.environ.get("S3_UPDATE_MAX_BACKOFF", "2.0"))

#: How this endpoint wants an ETag presented back on `If-Match`.
#:
#: `get_object` returns the ETag quoted (`"d41d8cd9..."`), and that quoted form
#: is what AWS compares against. Some S3-compatible stores compare the header
#: against the *bare* hex instead and therefore refuse every conditional
#: overwrite, while still accepting `If-None-Match: *` creates -- which
#: presents as "every group record is created and never updated again".
#:
#: auto    try the form `get_object` returned, then the bare form, and latch
#:         whichever the server accepts for the life of the process
#: quoted  pin to the form `get_object` returned
#: bare    pin to the unquoted form
ETAG_STYLE = os.environ.get("S3_ETAG_STYLE", "auto").strip().lower()

#: Set to "true" to skip the startup conditional-write probe. For tests, and
#: for deployments that have already made the determination.
SKIP_CAS_PROBE = os.environ.get("S3_SKIP_CAS_PROBE", "false").strip().lower() in ("true", "1", "yes")

#: Latched result of the `auto` ETag negotiation. None until a conditional
#: overwrite has actually succeeded.
_etag_style_resolved = None if ETAG_STYLE == "auto" else ETAG_STYLE
_etag_style_lock = threading.Lock()

#: Returned by `_load_with_etag` in place of the document when the read itself
#: failed. Distinct from None, which means the key is genuinely absent --
#: conflating the two is how a transient read error turns into an
#: `If-None-Match: *` against an existing key, which can never succeed and
#: burns the entire retry budget before dropping the write.
_READ_FAILED = object()


class ConditionalWriteUnsupported(RuntimeError):
    """The endpoint refused a precondition it should have accepted.

    Distinct from losing a race. Retrying cannot help: nothing changed
    between our read and our write, so the server is refusing an `If-Match`
    against the very ETag it just handed us. Raised instead of spending the
    remaining attempts and then reporting "another writer won every round",
    which is the wrong diagnosis and sends the reader hunting for contention
    that does not exist.
    """


class StorageReadError(RuntimeError):
    """A read failed for some reason other than the key being absent."""


def _is_precondition_failure(error) -> bool:
    """Did a conditional write lose the race?

    S3 answers a failed `If-Match` with 412 PreconditionFailed and a failed
    `If-None-Match: *` with 409 ConditionalRequestConflict. Both mean the same
    thing here: re-read and try again.
    """
    response = getattr(error, "response", None) or {}
    code = (response.get("Error") or {}).get("Code")
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return code in ("PreconditionFailed", "ConditionalRequestConflict") or status in (409, 412)


def _is_not_found(error) -> bool:
    response = getattr(error, "response", None) or {}
    code = (response.get("Error") or {}).get("Code")
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return code in ("NoSuchKey", "NoSuchBucket", "404") or status == 404


def _backoff_seconds(attempt: int) -> float:
    """Jittered backoff for retry number `attempt` (zero-based).

    Full jitter over an exponentially growing ceiling: every writer draws from
    a different range each round, which is what actually separates a burst.
    """
    ceiling = min(UPDATE_MAX_BACKOFF, UPDATE_BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, max(0.0, ceiling))


def _etag_forms(etag: str) -> list:
    """The forms of this ETag worth presenting on `If-Match`, best first."""
    quoted = etag if etag.startswith('"') else f'"{etag}"'
    bare = etag.strip('"')

    with _etag_style_lock:
        resolved = _etag_style_resolved

    if resolved == "bare":
        return [bare]
    if resolved == "quoted":
        return [quoted]

    # Unresolved `auto`: what the server returned first, then the other form.
    forms = [etag]
    for alternative in (quoted, bare):
        if alternative not in forms:
            forms.append(alternative)
    return forms


def _latch_etag_style(accepted: str, original: str) -> None:
    """Remember which ETag form this endpoint accepted.

    Only meaningful while the style is `auto`. Without it, an endpoint that
    wants the bare form pays a wasted round trip on every single conditional
    overwrite for the life of the process.
    """
    global _etag_style_resolved
    with _etag_style_lock:
        if _etag_style_resolved is not None:
            return
        _etag_style_resolved = "quoted" if accepted.startswith('"') else "bare"
        style = _etag_style_resolved
    if accepted != original:
        logger.info("Latched S3 ETag style for conditional writes",
                    etag_style=style,
                    note="this endpoint refused the form get_object returned")


def _get_client():
    global _s3_client
    with _s3_client_lock:
        if _s3_client is None:
            import boto3
            from botocore.config import Config

            endpoint = os.environ.get("S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")
            kwargs = {}
            if endpoint:
                kwargs["endpoint_url"] = endpoint
                # Self-hosted S3-compatible stores (MinIO, Ceph, etc.) need
                # path-style addressing: they do not resolve
                # <bucket>.<host> virtual-host style.
                kwargs["config"] = Config(s3={"addressing_style": "path"})

                if os.environ.get("S3_VERIFY_SSL", "true").lower() == "false":
                    kwargs["verify"] = False

            _s3_client = boto3.client("s3", **kwargs)
        return _s3_client


class S3CasebookStorage(CasebookStorage):
    """CasebookStorage backed by an S3 bucket."""

    def __init__(self, bucket: Optional[str] = None, prefix: Optional[str] = None):
        self.bucket = bucket or os.environ.get("CASEBOOK_S3_BUCKET") or os.environ.get("S3_LOGS_BUCKET")
        if not self.bucket:
            raise ValueError(
                "CASEBOOK_STORAGE_BACKEND=s3 requires CASEBOOK_S3_BUCKET "
                "(or S3_LOGS_BUCKET) to be set."
            )
        self.prefix = (prefix if prefix is not None
                       else os.environ.get("CASEBOOK_S3_PREFIX", "casebooks")).strip("/")

    def _key(self, event_id: str, filename: str) -> str:
        # eventId is constrained by EVENT_ID_PATTERN before it reaches here, so
        # it cannot contain "/" or ".." and cannot escape the prefix (0.11).
        parts = [p for p in (self.prefix, f"casebook_{event_id}", filename) if p]
        return "/".join(parts)

    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        if "schema_version" not in casebook:
            casebook["schema_version"] = CASEBOOK_SCHEMA_VERSION

        # Stamp the last_updated timestamp on every save
        if isinstance(casebook, dict):
            meta = casebook.setdefault("casebook_metadata", {})
            meta["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        _get_client().put_object(
            Bucket=self.bucket,
            Key=self._key(event_id, filename),
            Body=json.dumps(casebook, indent=4, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

    def save_terminal(self, event_id: str, casebook: dict) -> None:
        """Write casebook.json then status.json, mirroring the local backend.

        casebook.json goes first for the same reason: a terminal status.json
        with no casebook behind it would suppress reprocessing forever,
        whereas a stale IN_PROGRESS ages out on its own.
        """
        self.save(event_id, casebook)

        status = (casebook.get("packet_status") or {}).get("status")
        self.save(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": status},
            "resolution": {
                "synthesis": (casebook.get("resolution") or {}).get("synthesis")
            },
        }, filename="status.json")

    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        try:
            response = _get_client().get_object(
                Bucket=self.bucket, Key=self._key(event_id, filename)
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception as e:
            # A missing key is the common, expected case -- every new packet
            # probes for one. Only log something that is NOT a 404.
            if getattr(e, "response", {}).get("Error", {}).get("Code") not in ("NoSuchKey", "404"):
                logger.warning("Failed to load casebook from S3", event_id=event_id,
                               filename=filename, error=f"{type(e).__name__}: {e}")
            return None

    def exists(self, event_id: str, terminal_only: bool = False,
               filename: str = "casebook.json") -> bool:
        data = self.load(event_id, filename=filename)
        if not data:
            return False
        if terminal_only:
            return (data.get("packet_status") or {}).get("status") in TERMINAL_STATUSES
        return True

    def terminal_status(self, event_id: str,
                        filenames: tuple = ("status.json", "casebook.json")) -> Optional[str]:
        for filename in filenames:
            data = self.load(event_id, filename=filename)
            if not data:
                continue
            status = (data.get("packet_status") or {}).get("status")
            if status in TERMINAL_STATUSES:
                return status
        return None

    # ------------------------------------------------------------------
    # Blob artifacts (G3)
    # ------------------------------------------------------------------

    def save_artifact(self, event_id: str, filename: str, content: str) -> str:
        """PutObject is atomic at the object level, so no temp-key dance --
        the same reasoning as save() above."""
        key = self._key(event_id, filename)
        _get_client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )
        return f"s3://{self.bucket}/{key}"

    def load_artifact(self, event_id: str, filename: str) -> Optional[str]:
        try:
            response = _get_client().get_object(
                Bucket=self.bucket, Key=self._key(event_id, filename)
            )
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            # A missing artifact is the common case (every fresh packet probes
            # for a snapshot), so only a non-404 is worth reporting.
            if getattr(e, "response", {}).get("Error", {}).get("Code") not in ("NoSuchKey", "404"):
                logger.warning("Failed to load artifact from S3", event_id=event_id,
                               filename=filename, error=f"{type(e).__name__}: {e}")
            return None

    def artifact_exists(self, event_id: str, filename: str) -> bool:
        try:
            _get_client().head_object(
                Bucket=self.bucket, Key=self._key(event_id, filename)
            )
            return True
        except Exception:
            return False

    def update_json(self, event_id: str, filename: str, mutate) -> dict:
        """Read-modify-write via an S3 conditional write.

        S3 gives no mutual exclusion, so a load-then-save pair is a lost-update
        race: two analysis pods incrementing the same DLT group counter both
        read N and both write N+1. `filelock` cannot help -- it coordinates
        processes on a shared filesystem, and two pods have none.

        The fix is compare-and-swap, which S3 does support: `If-None-Match: *`
        creates only when absent, `If-Match: <etag>` overwrites only when the
        object is still the one we read. Either returns 412 when another writer
        got there first, and we retry from the fresh state.

        Not every S3-compatible store implements that faithfully. Two failure
        modes are separated here rather than both surfacing as "another writer
        won every round":

        - the ETag is compared in a different form (see `_etag_forms`), and
        - the store refuses `If-Match` outright.

        The second raises `ConditionalWriteUnsupported` on the second refusal
        rather than burning the whole budget, because nothing changed between
        the two reads -- that is a broken endpoint, not contention, and the
        caller needs to know which.
        """
        key = self._key(event_id, filename)
        refused_etag = None

        for attempt in range(UPDATE_MAX_ATTEMPTS):
            current, etag = self._load_with_etag(key)
            if current is _READ_FAILED:
                # Falling through here would send `If-None-Match: *` against a
                # key that probably exists, which can never succeed.
                raise StorageReadError(
                    f"Could not read {key} for update; refusing to treat an "
                    f"unreadable object as an absent one.")

            updated = mutate(current)
            if "schema_version" not in updated:
                updated["schema_version"] = CASEBOOK_SCHEMA_VERSION

            body = json.dumps(updated, indent=4, ensure_ascii=False).encode("utf-8")
            if self._put_conditional(key, body, etag):
                return updated

            # Two refusals against the same ETag means nothing changed between
            # the reads while our write was refused -- so the server declined
            # an `If-Match` against the ETag it had just returned. No number of
            # retries fixes that.
            if etag is not None and etag == refused_etag:
                raise ConditionalWriteUnsupported(
                    f"{key}: the endpoint refused If-Match against the ETag it "
                    f"returned ({etag!r}) on two consecutive reads. This store "
                    f"does not support conditional overwrite; group state "
                    f"cannot be maintained through update_json against it.")
            refused_etag = etag

            logger.info("Conditional write lost a race; retrying",
                        event_id=event_id, filename=filename,
                        attempt=attempt + 1)
            time.sleep(_backoff_seconds(attempt))

        raise RuntimeError(
            f"Could not update {key} after {UPDATE_MAX_ATTEMPTS} attempts: "
            f"another writer won every round."
        )

    def _put_conditional(self, key: str, body: bytes, etag) -> bool:
        """Store `body` under a precondition. True when it landed.

        `etag` None means create-only (`If-None-Match: *`); otherwise the write
        is conditioned on the object still being the one we read. Returns False
        when the precondition was refused, which the caller reads as "re-read
        and try again".
        """
        client = _get_client()

        if etag is None:
            try:
                client.put_object(Bucket=self.bucket, Key=key, Body=body,
                                  ContentType="application/json",
                                  IfNoneMatch="*")
                return True
            except Exception as e:
                if not _is_precondition_failure(e):
                    raise
                return False

        for form in _etag_forms(etag):
            try:
                client.put_object(Bucket=self.bucket, Key=key, Body=body,
                                  ContentType="application/json",
                                  IfMatch=form)
                _latch_etag_style(form, etag)
                return True
            except Exception as e:
                if not _is_precondition_failure(e):
                    raise
        return False

    def _load_with_etag(self, key: str) -> tuple:
        """Return (document, etag).

        (None, None) means the key is absent. (`_READ_FAILED`, None) means the
        read failed for some other reason -- a distinction the caller must
        keep, because treating an unreadable object as an absent one produces
        an `If-None-Match: *` that can never succeed.
        """
        try:
            response = _get_client().get_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            if _is_not_found(e):
                return None, None
            logger.warning("Could not read for update", key=key,
                           error=f"{type(e).__name__}: {e}")
            return _READ_FAILED, None

        etag = response.get("ETag")
        try:
            return json.loads(response["Body"].read().decode("utf-8")), etag
        except Exception as e:
            # The object is present but will not parse. Rebuild it from
            # scratch -- as the local backend does under its lock -- while
            # still conditioning the write on the ETag we just read, so
            # recovering from corruption cannot clobber a concurrent writer.
            logger.warning("Corrupt document; rebuilding it under its ETag",
                           key=key, error=f"{type(e).__name__}: {e}")
            return None, etag

    def create_json(self, event_id: str, filename: str, document: dict) -> bool:
        """Create-only write. See `CasebookStorage.create_json`.

        `If-None-Match: *` is the one precondition every S3-compatible store
        implements, including those that refuse conditional *overwrite*. That
        is why the group store is built out of this rather than update_json.
        """
        if "schema_version" not in document:
            document["schema_version"] = CASEBOOK_SCHEMA_VERSION

        body = json.dumps(document, indent=4, ensure_ascii=False).encode("utf-8")
        try:
            _get_client().put_object(
                Bucket=self.bucket, Key=self._key(event_id, filename),
                Body=body, ContentType="application/json", IfNoneMatch="*",
            )
            return True
        except Exception as e:
            if _is_precondition_failure(e):
                return False
            raise

    def list_json(self, event_id: str, subdir: str) -> list:
        prefix = self._key(event_id, subdir.strip("/")) + "/"
        names = []
        try:
            paginator = _get_client().get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix,
                                           Delimiter="/"):
                for entry in page.get("Contents", []):
                    name = entry["Key"][len(prefix):]
                    if name.endswith(".json") and "/" not in name:
                        names.append(name)
        except Exception as e:
            # An empty listing and a failed listing must not read alike: the
            # caller counts these, and a swallowed error would report a
            # fingerprint as unseen rather than unknown.
            logger.warning("Could not list JSON objects", prefix=prefix,
                           error=f"{type(e).__name__}: {e}")
            raise
        return sorted(names)

    def list_events(self) -> list:
        """List casebook prefixes, paginated.

        Delimiter="/" makes S3 return the directory-like CommonPrefixes rather
        than every object under them, so this stays one round-trip per 1000
        events instead of one per artifact.
        """
        prefix = f"{self.prefix}/casebook_" if self.prefix else "casebook_"
        events = []
        try:
            paginator = _get_client().get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix,
                                           Delimiter="/"):
                for entry in page.get("CommonPrefixes", []):
                    name = entry["Prefix"][len(prefix):].rstrip("/")
                    if name:
                        events.append(name)
        except Exception as e:
            logger.warning("Failed to list casebooks from S3",
                           error=f"{type(e).__name__}: {e}")
            return []
        return sorted(events)


# ---------------------------------------------------------------------------
# Conditional-write capability probe (phase 1C)
# ---------------------------------------------------------------------------
# `update_json` is only atomic if the endpoint honours the preconditions it
# sends. An endpoint that accepts `If-None-Match: *` but refuses every
# `If-Match` presents as "every record is created once and never updated
# again" -- which is silent, survives every unit test written against a
# faithful fake, and is only visible as a warning line per dropped write.
#
# So we ask the endpoint directly, once, at startup.

#: Result of the startup probe, or None when it has not run.
_capability: Optional[dict] = None
_capability_lock = threading.Lock()


def probe_conditional_write(bucket: str, prefix: str = "") -> dict:
    """Ask this endpoint what it actually supports.

    Returns a dict with:
      create_only   `If-None-Match: *` refuses an overwrite of a present key
      overwrite     `If-Match: <etag>` is accepted against a current ETag
      etag_style    which ETag form it accepted ("quoted" / "bare" / None)
      error         the exception text, when the probe could not complete

    Writes and deletes one sentinel object under `<prefix>/_probe/`.
    """
    client = _get_client()
    key = "/".join(p for p in (prefix.strip("/"), "_probe", "conditional_write.json") if p)
    result = {"create_only": None, "overwrite": None, "etag_style": None, "error": None}

    try:
        client.put_object(Bucket=bucket, Key=key, Body=b'{"probe":0}',
                          ContentType="application/json")
        etag = client.get_object(Bucket=bucket, Key=key).get("ETag") or ""

        for style, form in (("quoted", etag if etag.startswith('"') else f'"{etag}"'),
                            ("bare", etag.strip('"'))):
            try:
                client.put_object(Bucket=bucket, Key=key, Body=b'{"probe":1}',
                                  ContentType="application/json", IfMatch=form)
                result["overwrite"] = True
                result["etag_style"] = style
                break
            except Exception as e:
                if not _is_precondition_failure(e):
                    raise
        else:
            result["overwrite"] = False

        # A store that accepts `If-None-Match: *` over a key that already
        # exists gives no create-only guarantee either, which is what the DLT
        # case claim in src/dlt/claims.py relies on.
        try:
            client.put_object(Bucket=bucket, Key=key, Body=b'{"probe":2}',
                              ContentType="application/json", IfNoneMatch="*")
            result["create_only"] = False
        except Exception as e:
            if not _is_precondition_failure(e):
                raise
            result["create_only"] = True
    except Exception as e:
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass

    return result


def conditional_write_capability(bucket: str, prefix: str = "",
                                 force: bool = False) -> dict:
    """Probe once per process and cache the answer.

    Logs at ERROR when conditional overwrite is unsupported, naming the
    consequence rather than the mechanism -- an operator reading this needs to
    know that DLT group state will not accumulate, not that a 412 came back.
    """
    global _capability, _etag_style_resolved
    if SKIP_CAS_PROBE and not force:
        return {"create_only": None, "overwrite": None,
                "etag_style": None, "error": "probe skipped"}

    with _capability_lock:
        if _capability is not None and not force:
            return _capability

    result = probe_conditional_write(bucket, prefix)

    if result["error"]:
        logger.warning("Could not probe S3 conditional-write support",
                       error=result["error"])
    elif result["overwrite"] is False:
        logger.error(
            "This S3 endpoint refuses conditional overwrite (If-Match). "
            "Read-modify-write through update_json cannot work against it: "
            "records will be created and then never updated. DLT group "
            "records must use the decomposed store (src/dlt/group_store.py), "
            "and any other update_json caller against this backend will drop "
            "writes.",
            create_only=result["create_only"])
    else:
        logger.info("S3 conditional-write support confirmed",
                    etag_style=result["etag_style"],
                    create_only=result["create_only"])
        # The probe already learned which form this endpoint wants, so the
        # first real conditional overwrite need not rediscover it.
        if result["etag_style"] and ETAG_STYLE == "auto":
            with _etag_style_lock:
                if _etag_style_resolved is None:
                    _etag_style_resolved = result["etag_style"]

    with _capability_lock:
        _capability = result
    return result


def probe_configured_endpoint() -> Optional[dict]:
    """Run the capability probe against the configured bucket, if any.

    The startup entry point. Uses the base casebook prefix rather than a
    scoped root, so the sentinel never lands among the DLT group or case
    records. Never raises: a probe that cannot run is logged and ignored.
    """
    bucket = os.environ.get("CASEBOOK_S3_BUCKET") or os.environ.get("S3_LOGS_BUCKET")
    if not bucket:
        return None
    prefix = (os.environ.get("CASEBOOK_S3_PREFIX") or "casebooks").strip("/")
    try:
        return conditional_write_capability(bucket, prefix)
    except Exception as e:
        logger.warning("S3 conditional-write probe crashed",
                       error=f"{type(e).__name__}: {e}")
        return None

