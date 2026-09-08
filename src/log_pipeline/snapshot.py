"""
Log snapshot persistence (KUBERNETES_LOGS_PLAN.md 4.3).

The mechanism that makes a short-retention source viable at all.

Kubelet retention is roughly 10MB x 5 files per container, so a busy service
can lose a two-hour window in minutes. Meanwhile investigations do NOT
reliably run promptly: consumer lag, DLQ replays, MAX_IN_PROGRESS_AGE_SECONDS
staleness resumption, checkpoint resumes, and the Investigator retry loop all
re-enter fetch_logs well after the event. On every one of those paths a naive
fetch-at-analysis-time design returns nothing.

So the first successful fetch is persisted as structured JSONL, and every
later fetch for the same event reuses it and skips the API entirely. Retries
become deterministic and free, the cluster is not re-hit once per retry-loop
iteration, and evidence captured while it existed survives long after the
kubelet dropped it.

JSONL rather than the formatted `raw_logs.txt` so Stages 2-4 can be re-run
later with different tuning.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.log_pipeline.types import EvidenceGap, GapType
from src.storage.factory import get_casebook_storage
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger
from src.utils.paths import LOCAL_CASESHEETS_DIR

logger = get_logger(__name__)

RECORDS_FILENAME = "raw_logs_k8s.jsonl"
META_FILENAME = "log_snapshot_meta.json"


def reuse_enabled() -> bool:
    return get_bool_env("LOG_SNAPSHOT_REUSE", True)


def _casesheets_root() -> Path:
    """Root holding every casebook directory.

    Delegates to utils.paths rather than re-deriving the path (F22), but stays
    a function so tests can redirect it at a tmp_path.
    """
    return LOCAL_CASESHEETS_DIR


def snapshot_dir(event_id: str) -> Path:
    return _casesheets_root() / f"casebook_{event_id}"


def _records_path(event_id: str) -> Path:
    return snapshot_dir(event_id) / RECORDS_FILENAME


def _meta_path(event_id: str) -> Path:
    return snapshot_dir(event_id) / META_FILENAME


def exists(event_id: str) -> bool:
    """Through the storage seam, like save() and load().

    Leaving this on the filesystem while the writes moved to storage would
    reintroduce exactly the split G3 describes: a snapshot saved to S3 would
    report as absent.
    """
    return get_casebook_storage().artifact_exists(event_id, RECORDS_FILENAME)


def save(event_id: str, records: list, gaps: Optional[list] = None,
         source: str = "kubernetes") -> bool:
    """Persist a snapshot. Returns True when written.

    Goes through CasebookStorage, which writes atomically on both backends
    (`.tmp` + `os.replace` under a lock locally, an atomic PutObject on S3).
    Writing straight to the local filesystem meant that under
    CASEBOOK_STORAGE_BACKEND=s3 a snapshot lived only on the pod that captured
    it -- so a retry, DLQ replay, or checkpoint resume landing on a different
    pod found nothing and re-hit the cluster, or got nothing at all because
    the kubelet had since rotated the window (G3). Snapshot reuse is the
    mechanism that makes this short-retention source viable, and it was
    silently off for every multi-replica deployment.
    """
    try:
        storage = get_casebook_storage()

        body = "".join(
            json.dumps(record, ensure_ascii=False) + "\n" for record in records
        )
        storage.save_artifact(event_id, RECORDS_FILENAME, body)

        meta = {
            "event_id": event_id,
            "source": source,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(records),
            "gaps": [
                {"type": g.gap_type.value, "detail": g.detail, "context": g.context}
                for g in (gaps or [])
            ],
        }
        storage.save(event_id, meta, filename=META_FILENAME)

        logger.info("Log snapshot saved", event_id=event_id,
                    record_count=len(records), source=source)
        return True
    except Exception as e:
        # A snapshot is an optimisation, never a correctness requirement:
        # failing to write one must not fail the packet.
        logger.warning("Failed to save log snapshot", event_id=event_id,
                       error=f"{type(e).__name__}: {e}")
        return False


def load(event_id: str) -> Optional[tuple]:
    """Return (records, gaps) from a snapshot, or None when unusable."""
    try:
        storage = get_casebook_storage()

        body = storage.load_artifact(event_id, RECORDS_FILENAME)
        if body is None:
            return None

        records = [json.loads(line) for line in body.splitlines() if line.strip()]

        gaps = []
        captured_at = None
        meta = storage.load(event_id, filename=META_FILENAME)
        if meta:
            captured_at = meta.get("captured_at")
            for entry in meta.get("gaps", []):
                try:
                    gaps.append(EvidenceGap(
                        GapType(entry["type"]),
                        entry.get("detail", ""),
                        entry.get("context", {}) or {},
                    ))
                except (KeyError, ValueError):
                    continue

        logger.info("Log snapshot reused", event_id=event_id,
                    record_count=len(records), captured_at=captured_at)
        return records, gaps
    except Exception as e:
        # A corrupt snapshot must not poison the packet: fall through to a
        # live fetch instead.
        logger.warning("Unusable log snapshot; ignoring", event_id=event_id,
                       error=f"{type(e).__name__}: {e}")
        return None
