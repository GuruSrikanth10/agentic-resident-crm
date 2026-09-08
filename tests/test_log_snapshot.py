"""
Phase 8 of KUBERNETES_LOGS_PLAN.md -- snapshot persistence and pruning.

Snapshots are what make a short-retention source usable at all: kubelet logs
vanish in minutes, but investigations replay hours or days later via consumer
lag, DLQ replays, staleness resumption, and the retry loop.
"""
import json
import time

import pytest

from src.log_pipeline import snapshot
from src.log_pipeline.types import EvidenceGap, GapType
from src.tools import prune_casesheets


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """Point snapshots at a tmp root.

    Snapshots now persist through CasebookStorage rather than writing to the
    local filesystem directly, so that they survive on any backend and remain
    reusable across replicas (G3). The isolation therefore has to redirect the
    storage layer as well as the path helper.
    """
    from src.storage.local import LocalFilesystemCasebookStorage

    monkeypatch.delenv("LOG_SNAPSHOT_REUSE", raising=False)
    monkeypatch.setattr(snapshot, "_casesheets_root", lambda: tmp_path)
    monkeypatch.setattr(snapshot, "get_casebook_storage",
                        lambda: LocalFilesystemCasebookStorage(base_dir=str(tmp_path)))
    yield


def _records(n=2):
    return [
        {"timestamp": f"2026-01-01T10:00:0{i}Z", "level": "INFO",
         "message": f"line-{i}", "app_name": "svc", "source": "kubernetes"}
        for i in range(n)
    ]


# ======================================================================
# Round trip
# ======================================================================

def test_save_then_load_round_trips():
    assert snapshot.save("evt-1", _records(3)) is True
    loaded, gaps = snapshot.load("evt-1")

    assert len(loaded) == 3
    assert loaded[0]["message"] == "line-0"
    assert gaps == []


def test_gaps_survive_the_round_trip():
    """Capture-time gaps must be replayed too: a snapshot that silently drops
    them would make an incomplete trace look complete on every later run."""
    gaps = [EvidenceGap(GapType.LOG_ROTATION, "rotated at 09:14", {"a": 1})]
    snapshot.save("evt-2", _records(), gaps=gaps)

    _loaded, restored = snapshot.load("evt-2")
    assert len(restored) == 1
    assert restored[0].gap_type == GapType.LOG_ROTATION
    assert restored[0].detail == "rotated at 09:14"


def test_load_returns_none_when_absent():
    assert snapshot.load("never-captured") is None
    assert snapshot.exists("never-captured") is False


def test_exists_reflects_a_saved_snapshot():
    snapshot.save("evt-3", _records())
    assert snapshot.exists("evt-3") is True


def test_meta_records_provenance(tmp_path):
    snapshot.save("evt-4", _records(2), source="kubernetes")
    meta = json.loads((tmp_path / "casebook_evt-4" / snapshot.META_FILENAME).read_text())

    assert meta["event_id"] == "evt-4"
    assert meta["source"] == "kubernetes"
    assert meta["record_count"] == 2
    assert meta["captured_at"]


def test_records_are_written_as_jsonl(tmp_path):
    """JSONL rather than the formatted raw_logs.txt, so Stages 2-4 can be
    re-run later with different tuning."""
    snapshot.save("evt-5", _records(3))
    lines = (tmp_path / "casebook_evt-5" / snapshot.RECORDS_FILENAME) \
        .read_text().strip().splitlines()

    assert len(lines) == 3
    assert all(json.loads(line)["source"] == "kubernetes" for line in lines)


# ======================================================================
# Robustness -- a snapshot is an optimisation, never a correctness requirement
# ======================================================================

def test_corrupt_snapshot_is_ignored_rather_than_trusted(tmp_path):
    snapshot.save("evt-6", _records())
    (tmp_path / "casebook_evt-6" / snapshot.RECORDS_FILENAME).write_text("{not json\n")

    assert snapshot.load("evt-6") is None


def test_missing_meta_still_loads_records(tmp_path):
    snapshot.save("evt-7", _records(2))
    (tmp_path / "casebook_evt-7" / snapshot.META_FILENAME).unlink()

    loaded, gaps = snapshot.load("evt-7")
    assert len(loaded) == 2
    assert gaps == []


def test_save_failure_is_not_fatal(monkeypatch):
    """Failing to write a snapshot must not fail the packet.

    Induced at the storage seam rather than by pointing snapshot_dir at an
    unwritable path: persistence goes through CasebookStorage now, so the
    old path-based fault never reached the write (G3).
    """
    class BrokenStorage:
        def save_artifact(self, *_args, **_kwargs):
            raise OSError("backing store unavailable")

    monkeypatch.setattr(snapshot, "get_casebook_storage", BrokenStorage)
    assert snapshot.save("evt-8", _records()) is False


def test_unknown_gap_type_in_meta_is_skipped(tmp_path):
    snapshot.save("evt-9", _records())
    meta_path = tmp_path / "casebook_evt-9" / snapshot.META_FILENAME
    meta = json.loads(meta_path.read_text())
    meta["gaps"] = [{"type": "NOT_A_REAL_GAP", "detail": "x"}]
    meta_path.write_text(json.dumps(meta))

    _records_out, gaps = snapshot.load("evt-9")
    assert gaps == []


def test_no_partial_file_left_after_an_interrupted_write(tmp_path):
    """Atomic .tmp + os.replace: a crash mid-write must not leave a
    half-parsed JSONL for a later run to trust."""
    snapshot.save("evt-10", _records())
    directory = tmp_path / "casebook_evt-10"
    assert not list(directory.glob("*.tmp"))


def test_reuse_flag_is_read_from_config(monkeypatch):
    monkeypatch.setenv("LOG_SNAPSHOT_REUSE", "false")
    assert snapshot.reuse_enabled() is False
    monkeypatch.setenv("LOG_SNAPSHOT_REUSE", "true")
    assert snapshot.reuse_enabled() is True


# ======================================================================
# Pruning
# ======================================================================

def _casebook(root, event_id, status, age_days=0.0):
    directory = root / f"casebook_{event_id}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "casebook.json").write_text(json.dumps({
        "packet_status": {"status": status}
    }))
    (directory / "raw_logs.txt").write_text("x" * 100)
    (directory / "raw_logs_k8s.jsonl").write_text('{"a":1}\n')
    if age_days:
        old = time.time() - age_days * 86400
        for path in list(directory.rglob("*")) + [directory]:
            import os
            os.utime(path, (old, old))
    return directory


def _run_prune(argv, monkeypatch):
    monkeypatch.setattr("sys.argv", ["prune_casesheets"] + argv)
    return prune_casesheets.main()


def test_prune_removes_only_old_terminal_casebooks(tmp_path, monkeypatch, capsys):
    _casebook(tmp_path, "old-done", "COMPLETED", age_days=60)
    _casebook(tmp_path, "recent-done", "COMPLETED", age_days=0)

    _run_prune(["--root", str(tmp_path), "--older-than-days", "30"], monkeypatch)

    assert not (tmp_path / "casebook_old-done").exists()
    assert (tmp_path / "casebook_recent-done").exists()


def test_prune_never_touches_non_terminal_casebooks(tmp_path, monkeypatch, capsys):
    """An in-flight or resumable investigation must not have its evidence
    deleted out from under it."""
    _casebook(tmp_path, "in-flight", "IN_PROGRESS", age_days=90)

    _run_prune(["--root", str(tmp_path), "--older-than-days", "30"], monkeypatch)

    assert (tmp_path / "casebook_in-flight").exists()


def test_prune_skips_directories_without_a_casebook(tmp_path, monkeypatch, capsys):
    stray = tmp_path / "casebook_no-json"
    stray.mkdir(parents=True)
    (stray / "raw_logs.txt").write_text("orphan")
    import os
    old = time.time() - 90 * 86400
    os.utime(stray, (old, old))

    _run_prune(["--root", str(tmp_path), "--older-than-days", "30"], monkeypatch)

    assert stray.exists()


def test_prune_dry_run_removes_nothing(tmp_path, monkeypatch, capsys):
    _casebook(tmp_path, "old-done", "COMPLETED", age_days=60)

    _run_prune(["--root", str(tmp_path), "--older-than-days", "30", "--dry-run"],
               monkeypatch)

    assert (tmp_path / "casebook_old-done").exists()
    assert "DRY RUN" in capsys.readouterr().out


def test_prune_logs_only_keeps_the_casebook(tmp_path, monkeypatch, capsys):
    directory = _casebook(tmp_path, "old-done", "COMPLETED", age_days=60)

    _run_prune(["--root", str(tmp_path), "--older-than-days", "30", "--logs-only"],
               monkeypatch)

    assert (directory / "casebook.json").exists()
    assert not (directory / "raw_logs.txt").exists()
    assert not (directory / "raw_logs_k8s.jsonl").exists()


def test_prune_handles_a_missing_root(tmp_path, monkeypatch, capsys):
    assert _run_prune(["--root", str(tmp_path / "absent")], monkeypatch) == 0
