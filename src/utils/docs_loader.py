"""
S3 corpus downloader for the opencode harness.

Downloads the DROA-generated service documentation from S3 at startup so
the opencode agent can read it via Glob/Grep/Read. The corpus is a set of
markdown and JSON files organised as:

    <prefix>/enu-biometric/docs/architecture/...
    <prefix>/enu-biometric/docs/modules/...
    <prefix>/enu-biometric/docs/ontology/...
    <prefix>/enu-biometric/MANIFEST.json
    <prefix>/abis-middleware/...
    ...

Downloaded to DOCS_CACHE_DIR (default: docs_cache/ at the repo root).

Graceful degradation: if S3 is unavailable, continues with whatever is on
disk. The corpus changes on deploy, not per message, so a stale copy is
better than no copy.
"""
import os
import shutil
from pathlib import Path
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_DOCS_DIR = "docs_cache"
DEFAULT_S3_PREFIX = "nalanda/corpus"

# Set to True while a download is in progress, False otherwise.
# /ready checks this so it can return 503 "Downloading" even when
# docs_cache/ has files from a previous run.
_download_in_progress = False
_download_complete = False


def docs_cache_dir() -> Path:
    raw = os.environ.get("DOCS_CACHE_DIR", "").strip()
    if raw:
        return Path(raw)
    return Path(DEFAULT_DOCS_DIR)


def s3_prefix() -> str:
    return os.environ.get("DOCS_S3_PREFIX", DEFAULT_S3_PREFIX).strip("/")


def _get_s3_client():
    from src.storage.s3 import _get_client
    return _get_client()


def _bucket() -> Optional[str]:
    return (
        os.environ.get("CASEBOOK_S3_BUCKET")
        or os.environ.get("S3_LOGS_BUCKET")
    )


def download_corpus() -> bool:
    """Download the corpus from S3 to the local docs_cache directory.

    Returns True on success, False on failure (logs and continues).
    """
    global _download_in_progress, _download_complete

    bucket = _bucket()
    if not bucket:
        logger.info("S3 bucket not configured; skipping corpus download")
        _download_complete = True
        return False

    _download_in_progress = True
    target = docs_cache_dir()
    prefix = s3_prefix()
    logger.info("Downloading documentation corpus from S3",
                bucket=bucket, prefix=prefix, target=str(target))

    try:
        client = _get_s3_client()
        paginator = client.get_paginator("list_objects_v2")

        file_count = 0
        temp_dir = target.with_suffix(".tmp")

        if temp_dir.exists():
            shutil.rmtree(temp_dir)

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rel = key[len(prefix):].lstrip("/")
                if not rel or rel.endswith("/"):
                    continue

                local_path = temp_dir / rel
                local_path.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(bucket, key, str(local_path))
                file_count += 1

        if file_count == 0:
            logger.warning("Corpus download found no files",
                           bucket=bucket, prefix=prefix)
            _download_in_progress = False
            _download_complete = True
            return False

        if target.exists():
            shutil.rmtree(target)
        temp_dir.rename(target)

        logger.info("Corpus download complete",
                    files=file_count, target=str(target))
        _download_in_progress = False
        _download_complete = True
        return True

    except Exception as e:
        logger.warning("Corpus download failed; using existing docs if available",
                       error=f"{type(e).__name__}: {e}")
        _download_in_progress = False
        _download_complete = True
        return False


def corpus_available() -> bool:
    """True when the corpus is ready for the agent to read.

    Returns False while a download is in progress (even if old files exist
    on disk), and True once the download completes (or if no download was
    ever started and files are on disk from a previous run).
    """
    if _download_in_progress:
        return False
    if _download_complete:
        target = docs_cache_dir()
        return target.is_dir() and any(target.iterdir())
    # No download was initiated — check disk directly (e.g. manual copy)
    target = docs_cache_dir()
    return target.is_dir() and any(target.iterdir())


def list_services() -> list:
    target = docs_cache_dir()
    if not target.is_dir():
        return []
    return sorted(
        p.name for p in target.iterdir()
        if p.is_dir() and (p / "docs").is_dir()
    )
