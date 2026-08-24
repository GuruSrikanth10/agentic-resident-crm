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


def _get_client():
    global _s3_client
    with _s3_client_lock:
        if _s3_client is None:
            import boto3
            _s3_client = boto3.client("s3")
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
        """
        key = self._key(event_id, filename)

        for attempt in range(UPDATE_MAX_ATTEMPTS):
            current, etag = self._load_with_etag(key)
            updated = mutate(current)
            if "schema_version" not in updated:
                updated["schema_version"] = CASEBOOK_SCHEMA_VERSION

            body = json.dumps(updated, indent=4, ensure_ascii=False).encode("utf-8")
            condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}

            try:
                _get_client().put_object(
                    Bucket=self.bucket, Key=key, Body=body,
                    ContentType="application/json", **condition,
                )
                return updated
            except Exception as e:
                if not _is_precondition_failure(e):
                    raise
                logger.info("Conditional write lost a race; retrying",
                            event_id=event_id, filename=filename,
                            attempt=attempt + 1)

        raise RuntimeError(
            f"Could not update {key} after {UPDATE_MAX_ATTEMPTS} attempts: "
            f"another writer won every round."
        )

    def _load_with_etag(self, key: str) -> tuple:
        """Return (document, etag), or (None, None) when the key is absent."""
        try:
            response = _get_client().get_object(Bucket=self.bucket, Key=key)
            body = json.loads(response["Body"].read().decode("utf-8"))
            return body, response.get("ETag")
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return None, None
            # A corrupt document must not be silently replaced: without an
            # etag we would fall through to IfNoneMatch and fail, which is the
            # honest outcome.
            logger.warning("Could not read for update", key=key,
                           error=f"{type(e).__name__}: {e}")
            return None, None

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
