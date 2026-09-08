"""
Phase F regression tests (ENHANCEMENT_PLAN.md section 5).

4.7 -- S3CasebookStorage implements the full protocol; the checkpointer
       backend is selectable so two replicas can share state.

Scope note: these prove the seams are correct and the contracts match. They
do NOT prove two live replicas coordinate -- that needs a real S3 bucket,
Postgres, and Kafka broker. See the Phase F notes in ENHANCEMENT_PLAN.md.
"""
from unittest.mock import MagicMock

import pytest

from src.storage.base import CasebookStorage
from src.storage.local import LocalFilesystemCasebookStorage
from src.storage.s3 import S3CasebookStorage


class _FakeS3:
    """In-memory stand-in with boto3's put/get semantics."""

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            error = Exception("NoSuchKey")
            error.response = {"Error": {"Code": "NoSuchKey"}}
            raise error
        return {"Body": MagicMock(read=lambda: self.objects[(Bucket, Key)])}


@pytest.fixture
def s3_storage(monkeypatch):
    fake = _FakeS3()
    monkeypatch.setattr("src.storage.s3._get_client", lambda: fake)
    storage = S3CasebookStorage(bucket="test-bucket", prefix="casebooks")
    return storage, fake


# ======================================================================
# 4.7 -- S3 storage
# ======================================================================

def test_s3_backend_requires_a_bucket(monkeypatch):
    """Failing at construction beats failing on the first packet."""
    monkeypatch.delenv("CASEBOOK_S3_BUCKET", raising=False)
    monkeypatch.delenv("S3_LOGS_BUCKET", raising=False)
    with pytest.raises(ValueError, match="CASEBOOK_S3_BUCKET"):
        S3CasebookStorage()


def test_save_and_load_round_trip(s3_storage):
    storage, _ = s3_storage
    storage.save("evt-1", {"packet_status": {"status": "COMPLETED"}})
    assert storage.load("evt-1")["packet_status"]["status"] == "COMPLETED"


def test_load_of_a_missing_object_is_none_not_an_error(s3_storage):
    """Every new packet probes for one, so this is the common path."""
    storage, _ = s3_storage
    assert storage.load("never-seen") is None


def test_schema_version_is_stamped(s3_storage):
    from src.storage.base import CASEBOOK_SCHEMA_VERSION
    storage, _ = s3_storage
    storage.save("evt-1", {"packet_status": {"status": "COMPLETED"}})
    assert storage.load("evt-1")["schema_version"] == CASEBOOK_SCHEMA_VERSION


def test_save_terminal_writes_both_objects(s3_storage):
    storage, fake = s3_storage
    storage.save_terminal("evt-1", {
        "packet_status": {"status": "FAILED_TIMEOUT"},
        "resolution": {"synthesis": "timed out"},
    })

    keys = {key for _, key in fake.objects}
    assert "casebooks/casebook_evt-1/casebook.json" in keys
    assert "casebooks/casebook_evt-1/status.json" in keys


def test_terminal_status_reads_either_object(s3_storage):
    """The F4 guarantee must hold on this backend too."""
    storage, _ = s3_storage
    storage.save("evt-1", {"packet_status": {"status": "DLQ"}})
    storage.save("evt-1", {"packet_status": {"status": "IN_PROGRESS"}},
                 filename="status.json")
    assert storage.terminal_status("evt-1") == "DLQ"


def test_terminal_status_is_none_while_in_progress(s3_storage):
    storage, _ = s3_storage
    storage.save("evt-1", {"packet_status": {"status": "IN_PROGRESS"}},
                 filename="status.json")
    assert storage.terminal_status("evt-1") is None


def test_exists_respects_terminal_only(s3_storage):
    storage, _ = s3_storage
    storage.save("evt-1", {"packet_status": {"status": "IN_PROGRESS"}})
    assert storage.exists("evt-1") is True
    assert storage.exists("evt-1", terminal_only=True) is False

    storage.save("evt-1", {"packet_status": {"status": "COMPLETED"}})
    assert storage.exists("evt-1", terminal_only=True) is True


def test_keys_are_namespaced_by_prefix(s3_storage):
    storage, fake = s3_storage
    storage.save("evt-1", {"packet_status": {}})
    assert ("test-bucket", "casebooks/casebook_evt-1/casebook.json") in fake.objects


def test_both_backends_expose_the_same_protocol():
    """A backend missing a method fails at the call site in production, long
    after the config change that selected it."""
    required = [
        name for name in dir(CasebookStorage)
        if not name.startswith("_")
    ]
    for backend in (LocalFilesystemCasebookStorage, S3CasebookStorage):
        for name in required:
            assert callable(getattr(backend, name, None)), (
                f"{backend.__name__} is missing {name}()"
            )


def test_s3_and_local_agree_on_terminal_semantics(tmp_path, s3_storage):
    """The two backends must not disagree about what counts as terminal."""
    s3, _ = s3_storage
    local = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))

    for status, expected in (("COMPLETED", True), ("IN_PROGRESS", False),
                             ("DLQ", True), ("FAILED_TIMEOUT", True)):
        doc = {"packet_status": {"status": status}}
        s3.save(f"evt-{status}", doc)
        local.save(f"evt-{status}", doc)
        assert s3.exists(f"evt-{status}", terminal_only=True) is expected
        assert local.exists(f"evt-{status}", terminal_only=True) is expected


# ======================================================================
# 4.7 -- checkpointer selection
# ======================================================================

def test_default_backend_is_sqlite(monkeypatch):
    from src.core import checkpointer
    monkeypatch.delenv("CHECKPOINT_BACKEND", raising=False)
    assert checkpointer.backend_name() == "sqlite"


def test_unknown_backend_fails_loudly(monkeypatch):
    from src.core import checkpointer
    monkeypatch.setenv("CHECKPOINT_BACKEND", "mongodb")
    with pytest.raises(ValueError, match="Unknown CHECKPOINT_BACKEND"):
        checkpointer.get_checkpointer()


def test_postgres_without_a_uri_fails_loudly(monkeypatch):
    """Silently falling back to SQLite would give a multi-replica deployment
    per-pod checkpoints while looking healthy."""
    from src.core import checkpointer
    monkeypatch.setenv("CHECKPOINT_BACKEND", "postgres")
    monkeypatch.delenv("CHECKPOINT_POSTGRES_URI", raising=False)
    with pytest.raises(ValueError, match="CHECKPOINT_POSTGRES_URI"):
        checkpointer.get_checkpointer()


def test_sqlite_checkpointer_builds(monkeypatch, tmp_path):
    from src.core import checkpointer
    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH",
                        tmp_path / "checkpoints.db")
    assert checkpointer.get_checkpointer() is not None


# ======================================================================
# Boot-time validation of the scale-out config
# ======================================================================

def _validation_errors(monkeypatch, **env):
    """Run validate_config and capture whether it would exit."""
    from src.utils import config_validator

    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)

    with pytest.raises(SystemExit):
        config_validator.validate_config()


def test_s3_backend_without_a_bucket_fails_at_boot(monkeypatch):
    _validation_errors(monkeypatch, CASEBOOK_STORAGE_BACKEND="s3",
                       CASEBOOK_S3_BUCKET=None, S3_LOGS_BUCKET=None)


def test_unknown_storage_backend_fails_at_boot(monkeypatch):
    _validation_errors(monkeypatch, CASEBOOK_STORAGE_BACKEND="gcs")


def test_postgres_without_uri_fails_at_boot(monkeypatch):
    _validation_errors(monkeypatch, CASEBOOK_STORAGE_BACKEND="local",
                       CHECKPOINT_BACKEND="postgres",
                       CHECKPOINT_POSTGRES_URI=None)


def test_multiple_replicas_on_local_storage_fails_at_boot(monkeypatch):
    """filelock and a local SQLite file do not coordinate across pods, so this
    combination silently loses the idempotency guard."""
    _validation_errors(monkeypatch, CASEBOOK_STORAGE_BACKEND="local",
                       CHECKPOINT_BACKEND="sqlite", API_REPLICA_COUNT="3")


def test_single_replica_on_local_storage_is_fine(monkeypatch):
    from src.utils import config_validator

    monkeypatch.setenv("CASEBOOK_STORAGE_BACKEND", "local")
    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setenv("API_REPLICA_COUNT", "1")
    monkeypatch.setenv("KAFKA_CONSUMER_BROKERS", "localhost:9092")
    config_validator.validate_config()   # must not raise
