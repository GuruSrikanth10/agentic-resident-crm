"""Phase 5 of REMEDIATION_PLAN_2026_08_21.md -- multi-pod correctness.

Two mechanisms coordinated through the local filesystem while the data they
guard lived in shared storage. Both were silent under
CASEBOOK_STORAGE_BACKEND=s3 with more than one replica -- the configuration
the deployment plan exists to enable.
"""
import io
import json
import threading

import pytest

from src.dlt import case_storage, groups
from src.storage.factory import get_scoped_storage, reset_scoped_cache
from src.storage.local import LocalFilesystemCasebookStorage
from src.tools import tool_registry


@pytest.fixture(autouse=True)
def _isolated_roots(tmp_path, monkeypatch):
    monkeypatch.delenv("CASEBOOK_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    reset_scoped_cache()
    case_storage.reset_cache()
    yield
    reset_scoped_cache()
    case_storage.reset_cache()


# ======================================================================
# 5.1 -- group updates are atomic read-modify-write
# ======================================================================

def test_concurrent_occurrences_do_not_lose_increments():
    """The whole point: N pods recording the same fingerprint must produce N,
    not "however many happened not to collide"."""
    threads = 24
    barrier = threading.Barrier(threads)

    def record(index):
        barrier.wait()
        groups.record_occurrence("fp-concurrent", f"case-{index}",
                                 signature="sig", failure_class="A")

    workers = [threading.Thread(target=record, args=(i,)) for i in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    group = groups.load_group("fp-concurrent")
    assert group["occurrence_count"] == threads
    assert len(set(group["members"])) == threads


def test_a_redelivered_case_still_does_not_double_count():
    """Idempotency per case survived the rewrite."""
    for _ in range(5):
        groups.record_occurrence("fp-dupe", "case-1", signature="sig",
                                 failure_class="A")

    assert groups.load_group("fp-dupe")["occurrence_count"] == 1


def test_update_json_is_a_read_modify_write(tmp_path):
    """The primitive itself: the mutate function sees the stored document."""
    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "root"))

    seen = []

    def bump(current):
        seen.append(current)
        return {"n": (current or {}).get("n", 0) + 1}

    assert store.update_json("k", "doc.json", bump)["n"] == 1
    assert store.update_json("k", "doc.json", bump)["n"] == 2

    assert seen[0] is None                 # absent on the first call
    assert seen[1]["n"] == 1               # the stored value on the second
    assert store.load("k", filename="doc.json")["n"] == 2


def test_update_json_survives_a_corrupt_document(tmp_path):
    """A half-written or hand-edited file must not wedge the counter."""
    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "root"))
    store.save_artifact("k", "doc.json", "{ not json")

    result = store.update_json("k", "doc.json", lambda cur: {"n": (cur or {}).get("n", 0) + 1})
    assert result["n"] == 1


def test_concurrent_update_json_is_serialised(tmp_path):
    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "root"))
    threads = 20
    barrier = threading.Barrier(threads)

    def bump():
        barrier.wait()
        store.update_json("k", "doc.json",
                          lambda cur: {"n": (cur or {}).get("n", 0) + 1})

    workers = [threading.Thread(target=bump) for _ in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    assert store.load("k", filename="doc.json")["n"] == threads


def test_groups_no_longer_uses_a_local_lock_directory():
    """The lock coordinated processes on one filesystem while the records
    lived in S3 -- so on the deployment it was written for, it guarded
    nothing."""
    assert not hasattr(groups, "_file_lock")
    assert not hasattr(groups, "LOCAL_CHECKPOINTS_DIR")


# ======================================================================
# 5.2 -- the replay queue lives in shared storage
# ======================================================================

def test_a_queued_replay_is_visible_through_shared_storage(monkeypatch):
    """It used to land in a local jsonl on whichever pod ran the packet."""
    monkeypatch.setenv("ENABLE_AUTO_REPLAY", "false")

    result = tool_registry.queue_for_replay.invoke({
        "id": "evt-123", "idType": "EID", "priority": 5,
        "operatorName": "op", "category": "TEST", "fromSedaStart": False,
    })
    assert "Successfully queued" in result

    store = get_scoped_storage(tool_registry.PENDING_REPLAY_ROOT)
    assert "evt-123" in store.list_events()

    record = store.load("evt-123", filename=tool_registry.PENDING_REPLAY_FILENAME)
    assert record["status"] == "pending"
    assert record["payload"]["id"] == "evt-123"
    assert record["payload"]["category"] == "TEST"


def test_a_replay_id_that_is_not_a_valid_storage_key_is_refused(monkeypatch):
    """The id reaches here from an LLM tool call, and is interpolated into a
    storage path (0.11)."""
    monkeypatch.setenv("ENABLE_AUTO_REPLAY", "false")

    result = tool_registry.queue_for_replay.invoke({
        "id": "../../etc/passwd", "idType": "EID", "priority": 5,
        "operatorName": "op", "category": "TEST", "fromSedaStart": False,
    })

    assert "Failed to queue" in result
    assert get_scoped_storage(tool_registry.PENDING_REPLAY_ROOT).list_events() == []


def test_notification_fields_are_still_never_accepted(monkeypatch):
    """G20: the LLM must not be able to choose who gets notified."""
    monkeypatch.setenv("ENABLE_AUTO_REPLAY", "false")

    tool_registry.queue_for_replay.invoke({
        "id": "evt-456", "idType": "EID", "priority": 5,
        "operatorName": "op", "category": "TEST", "fromSedaStart": False,
    })

    store = get_scoped_storage(tool_registry.PENDING_REPLAY_ROOT)
    payload = store.load("evt-456",
                         filename=tool_registry.PENDING_REPLAY_FILENAME)["payload"]
    assert "notificationEmail" not in payload
    assert "notificationMobile" not in payload


def test_approve_replays_reads_the_shared_queue(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTO_REPLAY", "false")
    from src.tools import approve_replays

    for packet in ("evt-a", "evt-b"):
        tool_registry.queue_for_replay.invoke({
            "id": packet, "idType": "EID", "priority": 5,
            "operatorName": "op", "category": "TEST", "fromSedaStart": False,
        })

    pending = approve_replays._load_pending()
    assert sorted(r["payload"]["id"] for r in pending) == ["evt-a", "evt-b"]


def test_a_resolved_replay_is_not_offered_again(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTO_REPLAY", "false")
    from src.tools import approve_replays

    tool_registry.queue_for_replay.invoke({
        "id": "evt-done", "idType": "EID", "priority": 5,
        "operatorName": "op", "category": "TEST", "fromSedaStart": False,
    })

    store = get_scoped_storage(tool_registry.PENDING_REPLAY_ROOT)
    entry = approve_replays._load_pending()[0]
    approve_replays._resolve(store, entry, "replayed", set())

    assert approve_replays._load_pending() == []
    # Kept, not deleted: a fired replay is an audit record.
    assert store.load("evt-done",
                      filename=tool_registry.PENDING_REPLAY_FILENAME)["status"] == "replayed"


def test_a_legacy_local_queue_is_still_drained(monkeypatch, tmp_path):
    """Nothing queued before this change may be stranded."""
    from src.tools import approve_replays

    legacy = tmp_path / "pending_replays.jsonl"
    legacy.write_text(json.dumps({
        "timestamp": "2026-08-01T00:00:00",
        "payload": {"id": "evt-legacy", "category": "OLD"},
    }) + "\n", encoding="utf-8")
    monkeypatch.setattr(approve_replays, "_legacy_queue_path", lambda: legacy)

    pending = approve_replays._load_pending()
    assert [r["payload"]["id"] for r in pending] == ["evt-legacy"]

    # Consuming it rewrites the file rather than deleting the whole queue.
    store = get_scoped_storage(tool_registry.PENDING_REPLAY_ROOT)
    consumed = set()
    approve_replays._resolve(store, pending[0], "replayed", consumed)
    approve_replays._rewrite_legacy(consumed)

    assert legacy.read_text(encoding="utf-8").strip() == ""
    assert approve_replays._load_pending() == []


# ======================================================================
# 5.1 -- the S3 backend's conditional write is where the real race lives
# ======================================================================

class _FakeS3:
    """Enough of S3 to exercise compare-and-swap.

    `contended` makes the first N conditional writes lose, the way a
    competing pod's write would.
    """

    class _Error(Exception):
        def __init__(self, code, status):
            super().__init__(code)
            self.response = {"Error": {"Code": code},
                             "ResponseMetadata": {"HTTPStatusCode": status}}

    def __init__(self, contended: int = 0):
        self.objects = {}
        self.etags = {}
        self.contended = contended
        self.puts = 0
        self._version = 0

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self._Error("NoSuchKey", 404)
        body = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": self.etags[Key]}

    def put_object(self, Bucket, Key, Body, ContentType=None,
                   IfMatch=None, IfNoneMatch=None):
        self.puts += 1

        if self.contended > 0:
            self.contended -= 1
            # Simulate another writer landing first: bump the stored version so
            # our precondition no longer holds.
            self._version += 1
            self.objects[Key] = json.dumps({"n": 0}).encode("utf-8")
            self.etags[Key] = f'"v{self._version}"'
            raise self._Error("PreconditionFailed", 412)

        if IfNoneMatch == "*" and Key in self.objects:
            raise self._Error("PreconditionFailed", 412)
        if IfMatch is not None and self.etags.get(Key) != IfMatch:
            raise self._Error("PreconditionFailed", 412)

        self._version += 1
        self.objects[Key] = Body
        self.etags[Key] = f'"v{self._version}"'


def _s3_store(fake, monkeypatch):
    from src.storage import s3 as s3_module

    monkeypatch.setattr(s3_module, "_get_client", lambda: fake)
    return s3_module.S3CasebookStorage(bucket="b", prefix="p")


def test_s3_update_json_creates_when_absent(monkeypatch):
    fake = _FakeS3()
    store = _s3_store(fake, monkeypatch)

    result = store.update_json("k", "doc.json", lambda cur: {"n": (cur or {}).get("n", 0) + 1})

    assert result["n"] == 1
    assert fake.puts == 1


def test_s3_update_json_retries_after_losing_a_race(monkeypatch):
    """A competing pod wrote between our read and our write. We must re-read
    and re-apply, not overwrite their value."""
    fake = _FakeS3(contended=3)
    store = _s3_store(fake, monkeypatch)

    result = store.update_json("k", "doc.json", lambda cur: {"n": (cur or {}).get("n", 0) + 1})

    assert result["n"] == 1          # applied on top of the winner's state
    assert fake.puts == 4            # three lost rounds, then success


def test_s3_update_json_gives_up_rather_than_clobbering(monkeypatch):
    """Losing every round must raise, not fall back to an unconditional put --
    that would silently discard whoever kept winning."""
    from src.storage import s3 as s3_module

    fake = _FakeS3(contended=999)
    store = _s3_store(fake, monkeypatch)

    with pytest.raises(RuntimeError, match="another writer won every round"):
        store.update_json("k", "doc.json", lambda cur: {"n": 1})

    assert fake.puts == s3_module.UPDATE_MAX_ATTEMPTS


def test_s3_update_json_uses_a_conditional_write_every_time(monkeypatch):
    """An unconditional put_object here is the bug, so assert the precondition
    is always present."""
    seen = []

    class _Recording(_FakeS3):
        def put_object(self, **kwargs):
            seen.append(("IfMatch" in kwargs, "IfNoneMatch" in kwargs))
            return super().put_object(**kwargs)

    fake = _Recording()
    store = _s3_store(fake, monkeypatch)

    store.update_json("k", "doc.json", lambda cur: {"n": 1})   # create
    store.update_json("k", "doc.json", lambda cur: {"n": 2})   # overwrite

    assert seen == [(False, True), (True, False)]


@pytest.mark.parametrize("code,status", [
    ("PreconditionFailed", 412),
    ("ConditionalRequestConflict", 409),
])
def test_precondition_failures_are_recognised(code, status):
    from src.storage.s3 import _is_precondition_failure

    error = _FakeS3._Error(code, status)
    assert _is_precondition_failure(error) is True


def test_an_unrelated_s3_error_is_not_mistaken_for_contention():
    """AccessDenied must propagate, not spin through eight retries."""
    from src.storage.s3 import _is_precondition_failure

    assert _is_precondition_failure(_FakeS3._Error("AccessDenied", 403)) is False


# ======================================================================
# The two backends must stay interchangeable
# ======================================================================

def test_both_backends_implement_the_protocol_with_identical_signatures():
    """Signature drift between the backends is how this codebase's worst bugs
    started. F4 was a terminal status written to one file and read from
    another; G2 and G3 were methods that worked locally and silently returned
    nothing on S3. A caller is not supposed to know which backend it holds, so
    pin that they are actually substitutable.
    """
    import inspect

    from src.storage.base import CasebookStorage
    from src.storage.s3 import S3CasebookStorage

    protocol = {name for name, _ in inspect.getmembers(CasebookStorage, inspect.isfunction)
                if not name.startswith("_")}

    drift = []
    for name in sorted(protocol):
        local = getattr(LocalFilesystemCasebookStorage, name, None)
        s3 = getattr(S3CasebookStorage, name, None)
        assert local is not None, f"local backend is missing {name}"
        assert s3 is not None, f"S3 backend is missing {name}"
        if str(inspect.signature(local)) != str(inspect.signature(s3)):
            drift.append(f"{name}: local{inspect.signature(local)} "
                         f"vs s3{inspect.signature(s3)}")

    assert not drift, "backend signatures have drifted:\n  " + "\n  ".join(drift)
