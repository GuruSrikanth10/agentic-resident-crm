import os
import threading
from typing import Optional
import boto3
from botocore.exceptions import ClientError

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Cached across uploads: constructing a boto3 client resolves credentials and
# builds a signer every time, which is wasted work per packet (F20).
_s3_client = None
_s3_client_lock = threading.Lock()


def _get_s3_client():
    global _s3_client
    with _s3_client_lock:
        if _s3_client is None:
            _s3_client = boto3.client("s3")
        return _s3_client


def upload_logs_to_s3(event_id: str, logs: str) -> Optional[str]:
    """
    Uploads raw logs to AWS S3 and returns the S3 URL, or None if the
    upload could not happen (bucket not configured, or the upload failed).

    Callers must check for None rather than storing the return value
    directly -- this previously returned a fake s3://mock-bucket/... URL (or
    an "S3 Upload Failed: ..." string) that got persisted in the casebook as
    if it were a real, resolvable link while the actual log text was
    discarded (1.12).
    """
    log = logger.bind(event_id=event_id)
    bucket_name = os.environ.get("S3_LOGS_BUCKET")

    if not bucket_name:
        log.info("S3_LOGS_BUCKET not configured; cannot upload logs.")
        return None

    try:
        file_key = f"logs/{event_id}/raw_elastic_logs.txt"

        _get_s3_client().put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=logs.encode('utf-8'),
            ContentType="text/plain"
        )

        s3_url = f"s3://{bucket_name}/{file_key}"
        log.info("Logs uploaded to S3", s3_url=s3_url)
        return s3_url

    except ClientError as e:
        log.error("Failed to upload logs to S3", error=str(e))
        return None
    except Exception as e:
        log.error("Unexpected S3 upload error", error=str(e))
        return None
