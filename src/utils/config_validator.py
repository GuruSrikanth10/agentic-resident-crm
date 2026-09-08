import os
import sys
from pathlib import Path

from src.utils.env import get_bool_env
from src.utils.paths import LOCAL_CHECKPOINTS_DIR
from src.utils.logging_config import get_logger

# Every other module logs through structlog; this one used the stdlib logger,
# so its startup errors arrived as unparseable plain text interleaved with the
# JSON stream an aggregator expects (F21).
logger = get_logger(__name__)

def _collect_llm_key_errors() -> list:
    """Return validation errors for whichever LLM provider get_llm() will
    actually select. get_llm() picks a provider via USE_HF /
    MOCK_LLM_WITH_MISTRAL and never reads OPENAI_API_KEY, so hard-failing on
    that unconditionally blocked startup on every provider except a literal
    OpenAI endpoint (1.3). Split out as a pure function so it's testable
    without the pytest-skip in validate_config() below getting in the way.
    """
    errors = []
    use_hf = get_bool_env("USE_HF", False)
    use_mistral = get_bool_env("MOCK_LLM_WITH_MISTRAL", False)
    if use_hf:
        if not os.environ.get("HF_TOKEN"):
            errors.append("USE_HF=true requires HF_TOKEN to be set.")
    elif use_mistral:
        if not os.environ.get("LLM_API_KEY_COMPLEX"):
            errors.append("MOCK_LLM_WITH_MISTRAL=true requires LLM_API_KEY_COMPLEX to be set.")
    else:
        if not os.environ.get("LLM_API_KEY_COMPLEX"):
            errors.append("LLM_API_KEY_COMPLEX must be set for the OpenAI-compatible provider.")
    return errors


def validate_config():
    """Fail-fast startup check to ensure required configurations and permissions are valid."""
    errors = []

    # 1. The API key the *selected* LLM provider actually reads is set.
    if "pytest" not in sys.modules:
        errors.extend(_collect_llm_key_errors())

    # 2. KAFKA_CONSUMER_BROKERS list is non-empty
    kafka_brokers_env = os.environ.get("KAFKA_CONSUMER_BROKERS", "localhost:9092")
    brokers = [b.strip() for b in kafka_brokers_env.split(",") if b.strip()]
    if not brokers:
        errors.append("KAFKA_CONSUMER_BROKERS must contain at least one valid broker.")

    # 2b. The analysis-queue topic POST /fetch-logs publishes to, and
    # slow_consumer.py reads from by default, must not be blanked out.
    if not os.environ.get("PACKET_ANALYSIS_TOPIC_NAME", "packet-analysis-queue").strip():
        errors.append("PACKET_ANALYSIS_TOPIC_NAME must not be empty.")

    # 3. AGENTIC_RESIDENT_CRM_API_KEY is explicitly set in prod
    api_key = os.environ.get("AGENTIC_RESIDENT_CRM_API_KEY")
    env = os.environ.get("ENV", "dev").lower()
    if env == "prod":
        if not api_key or api_key == "dev-secret-key":
            errors.append("AGENTIC_RESIDENT_CRM_API_KEY must be explicitly set to a secure value in production (cannot be empty or 'dev-secret-key').")

    # 4. Scale-out backends are fully configured when selected (4.7). A
    # half-configured backend must fail at boot, not on the first packet.
    backend = os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").strip().lower()
    if backend == "s3":
        if not (os.environ.get("CASEBOOK_S3_BUCKET") or os.environ.get("S3_LOGS_BUCKET")):
            errors.append(
                "CASEBOOK_STORAGE_BACKEND=s3 requires CASEBOOK_S3_BUCKET "
                "(or S3_LOGS_BUCKET) to be set."
            )
    elif backend != "local":
        errors.append(
            f"Unknown CASEBOOK_STORAGE_BACKEND '{backend}'; expected 'local' or 's3'."
        )

    checkpoint_backend = os.environ.get("CHECKPOINT_BACKEND", "sqlite").strip().lower()
    if checkpoint_backend == "postgres":
        if not os.environ.get("CHECKPOINT_POSTGRES_URI"):
            errors.append(
                "CHECKPOINT_BACKEND=postgres requires CHECKPOINT_POSTGRES_URI to be set."
            )
    elif checkpoint_backend != "sqlite":
        errors.append(
            f"Unknown CHECKPOINT_BACKEND '{checkpoint_backend}'; expected 'sqlite' or 'postgres'."
        )

    # Local filelock does not coordinate across pods, so a multi-replica
    # deployment on local storage silently loses the idempotency guard.
    if backend == "local" and checkpoint_backend == "sqlite":
        replicas = os.environ.get("API_REPLICA_COUNT", "1")
        try:
            if int(replicas) > 1:
                errors.append(
                    "API_REPLICA_COUNT > 1 requires CASEBOOK_STORAGE_BACKEND=s3 and "
                    "CHECKPOINT_BACKEND=postgres: local filelock and a local SQLite "
                    "file do not coordinate across pods."
                )
        except ValueError:
            pass

    # 5. Storage and SQLite checkpointer paths are writable
    try:
        from src.storage.factory import get_casebook_storage
        storage = get_casebook_storage()
        if hasattr(storage, "base_dir"):
            storage_dir = Path(storage.base_dir)
            os.makedirs(storage_dir, exist_ok=True)
            if not os.access(storage_dir, os.W_OK):
                errors.append(f"CasebookStorage base_dir ({storage_dir}) is not writable.")
    except Exception as e:
        errors.append(f"Failed to validate CasebookStorage path: {e}")

    try:
        checkpoints_dir = LOCAL_CHECKPOINTS_DIR
        os.makedirs(checkpoints_dir, exist_ok=True)
        if not os.access(checkpoints_dir, os.W_OK):
            errors.append(f"SQLite checkpoints directory ({checkpoints_dir}) is not writable.")
    except Exception as e:
        errors.append(f"Failed to validate checkpointer path: {e}")

    if errors:
        for err in errors:
            logger.error("Startup validation error", detail=err)
            # Also to stderr: a boot failure must be visible even when the log
            # stream is being shipped elsewhere or the level is raised.
            print(f"Startup validation error: {err}", file=sys.stderr)
        sys.exit(1)

    logger.info("Configuration validation passed")