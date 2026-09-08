"""
Windows-lock regression tests for the atomic-replace helper.

The bug these exist for: `os.replace` is denied on Windows while any other
process holds either path open (OneDrive, the search indexer, an AV scan
sweeping a directory that was just written into). Every durable write in this
project ends in that call, so a single antivirus scan raised an unhandled
PermissionError out of `LocalFilesystemCasebookStorage.save`, which became a
500 from `POST /fetch-logs`, which the fast consumer turned into a DLQ entry.
A legitimate packet was dead-lettered by a transient file lock.
"""
import os
import threading
import time

import pytest

from src.storage.local import LocalFilesystemCasebookStorage
from src.utils.atomic import replace_with_retry


def _denying_replace(target_name, denials):
    """os.replace that denies the first `denials` swaps onto `target_name`.

    Scoped to one filename so every other replace in the process -- including
    any the test framework itself performs -- keeps working.
    """
    real = os.replace
    state = {"remaining": denials}

    def _fake(src, dst, *args, **kwargs):
        if str(dst).endswith(target_name) and state["remaining"] > 0:
            state["remaining"] -= 1
            raise PermissionError(5, "Access is denied")
        return real(src, dst, *args, **kwargs)

    return _fake


def test_replace_survives_a_transient_denial(tmp_path, monkeypatch):
    src = tmp_path / "payload.tmp"
    dst = tmp_path / "payload.json"
    src.write_text("content", encoding="utf-8")

    monkeypatch.setattr(os, "replace", _denying_replace("payload.json", 3))
    replace_with_retry(src, dst, attempts=5, backoff=0.001)

    assert dst.read_text(encoding="utf-8") == "content"


def test_replace_reraises_once_attempts_are_exhausted(tmp_path, monkeypatch):
    """The contract that keeps this from becoming silent data loss: a disk
    that genuinely cannot be written must still reach the caller, which is
    what routes an unwritable casebook to the DLQ."""
    src = tmp_path / "payload.tmp"
    dst = tmp_path / "payload.json"
    src.write_text("content", encoding="utf-8")

    monkeypatch.setattr(os, "replace", _denying_replace("payload.json", 99))

    with pytest.raises(PermissionError):
        replace_with_retry(src, dst, attempts=3, backoff=0.001)


def test_abort_event_cuts_the_backoff_short(tmp_path, monkeypatch):
    """A consumer draining on SIGTERM must not spend its termination budget
    waiting out retries."""
    src = tmp_path / "payload.tmp"
    dst = tmp_path / "payload.json"
    src.write_text("content", encoding="utf-8")

    monkeypatch.setattr(os, "replace", _denying_replace("payload.json", 99))
    abort = threading.Event()
    abort.set()

    started = time.monotonic()
    with pytest.raises(PermissionError):
        replace_with_retry(src, dst, attempts=50, backoff=5.0, abort=abort)

    assert time.monotonic() - started < 1.0, "an aborted retry must not sleep"


def test_casebook_save_survives_a_transient_denial(tmp_path, monkeypatch):
    """The exact path that 500'd POST /fetch-logs and dead-lettered a packet:
    status.json written into a freshly created casebook directory, while an
    AV scan of that new directory still held the file open."""
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(os, "replace", _denying_replace("status.json", 2))

    storage.save("evt-locked", {
        "packet_metadata": {"eid": "evt-locked"},
        "packet_status": {"status": "LOGS_FETCHED"},
    }, filename="status.json")

    reloaded = storage.load("evt-locked", filename="status.json")
    assert reloaded["packet_status"]["status"] == "LOGS_FETCHED"


def test_artifact_save_survives_a_transient_denial(tmp_path, monkeypatch):
    """Log artifacts land in the same new directory and face the same scan."""
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(os, "replace", _denying_replace("fetched_logs.txt", 2))

    storage.save_artifact("evt-locked", "fetched_logs.txt", "--- trace ---")

    assert storage.load_artifact("evt-locked", "fetched_logs.txt") == "--- trace ---"
