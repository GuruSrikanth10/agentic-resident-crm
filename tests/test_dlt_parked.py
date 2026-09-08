"""Phase C7 -- packets parked until their fix deploys.

Three properties, and every test is one of them:

* **Parking never becomes a second replay path.** A packet is parked only
  when the ordinary replay gate would have said yes but for the version. If
  `DLT_AUTO_REPLAY_ENABLED` is off, nothing parks -- otherwise the release
  worker would replay packets the operator never agreed to replay.
* **Release is version-driven and idempotent.** An entry releases when the
  pods reach its version, not before, and not twice.
* **Both axes are bounded.** A fix that never deploys must not park a packet
  forever, and a queue nobody watches must not grow without limit.
"""
import time

import pytest

from src.dlt import code_check as CC
from src.dlt import parked
from src.models.dlt_synthesis import DltFinding
from src.storage import factory


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_CODE_CHECK_PARK_ENABLED", "DLT_PARKED_REPLAY_TTL_SECONDS",
                "DLT_PARKED_REPLAY_CAP", "DLT_AUTO_REPLAY_ENABLED",
                "DLT_CODE_CHECK_GATES_REPLAY", "DLT_REPLAY_CONFIDENCE_THRESHOLD",
                "CASEBOOK_STORAGE_BACKEND", "ENABLE_AUTO_REPLAY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    factory.reset_scoped_cache()
    monkeypatch.setenv("DLT_CODE_CHECK_PARK_ENABLED", "true")
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")
    yield
    factory.reset_scoped_cache()


def finding(action="REDRIVE_AFTER_RECOVERY", confidence=0.6):
    return DltFinding(narrative="x", recommendation="y", action=action,
                      confidence=confidence)


def verdict(name=CC.NOT_DEPLOYED, required="1.0.1", **kwargs):
    return CC.CodeCheck(verdict=name, reason="the fix is not running yet",
                        required_version=required, repo="ENU/enu-biometric",
                        path="src/main/java/Svc.java", **kwargs)


CASE = "dlt-T-63-3352"


# ---------------------------------------------------------------------------
# What may be parked
# ---------------------------------------------------------------------------

def test_a_withheld_replay_is_parked_with_the_version_to_wait_for():
    result = parked.maybe_park(CASE, "REF-1", verdict(), finding())

    assert result["parked"] is True
    assert result["required_version"] == "1.0.1"

    entries = parked.list_parked()
    assert len(entries) == 1
    assert entries[0]["case_id"] == CASE
    assert entries[0]["ref_id"] == "REF-1"
    assert entries[0]["status"] == parked.STATUS_PARKED


def test_nothing_parks_when_the_flag_is_off(monkeypatch):
    monkeypatch.setenv("DLT_CODE_CHECK_PARK_ENABLED", "false")

    result = parked.maybe_park(CASE, "REF-1", verdict(), finding())

    assert result["parked"] is False
    assert parked.list_parked() == []


def test_parking_never_becomes_a_second_replay_path(monkeypatch):
    """With auto-replay off, this packet was never going to be replayed.
    Parking it would let the release worker replay it anyway."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "false")

    result = parked.maybe_park(CASE, "REF-1", verdict(), finding())

    assert result["parked"] is False
    assert "not going to be replayed anyway" in result["reason"]
    assert parked.list_parked() == []


@pytest.mark.parametrize("declined", [
    DltFinding(narrative="x", recommendation="y", action="DATA_FIX_REQUIRED",
               confidence=0.9),
    DltFinding(narrative="x", recommendation="y",
               action="REDRIVE_AFTER_RECOVERY", confidence=None),
    DltFinding(narrative="x", recommendation="y",
               action="REDRIVE_AFTER_RECOVERY", confidence=0.1),
])
def test_a_finding_the_gate_declines_on_its_own_is_not_parked(declined):
    assert parked.maybe_park(CASE, "REF-1", verdict(), declined)["parked"] is False


@pytest.mark.parametrize("name", [CC.NO_CHANGE, CC.FIX_DEPLOYED, CC.UNKNOWN])
def test_only_not_deployed_parks(name):
    """NO_CHANGE means a replay never works; FIX_DEPLOYED means it already
    fired; UNKNOWN means we established nothing."""
    assert parked.maybe_park(CASE, "REF-1", verdict(name), finding())["parked"] is False


def test_nothing_parks_when_the_veto_is_not_even_on(monkeypatch):
    """The verdict is withholding nothing, so there is nothing to come back
    to -- the replay already fired."""
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "false")

    assert parked.maybe_park(CASE, "REF-1", verdict(), finding())["parked"] is False


def test_no_ref_id_cannot_be_parked():
    assert parked.maybe_park(CASE, None, verdict(), finding())["parked"] is False


def test_a_verdict_with_no_required_version_cannot_be_parked():
    result = parked.maybe_park(CASE, "REF-1", verdict(required=None), finding())
    assert result["parked"] is False


def test_a_case_id_that_is_not_a_usable_storage_key_is_refused():
    """The id is interpolated into a storage path -- the same guard
    `_queue_pending_replay` applies."""
    result = parked.park("../../etc/passwd", "REF-1", verdict(), finding())

    assert result["parked"] is False
    assert parked.list_parked() == []


# ---------------------------------------------------------------------------
# Bounds
# ---------------------------------------------------------------------------

def test_the_queue_is_capped(monkeypatch):
    monkeypatch.setenv("DLT_PARKED_REPLAY_CAP", "2")

    for i in range(4):
        parked.park(f"dlt-T-63-{i}", f"REF-{i}", verdict(), finding())

    assert len(parked.list_parked()) == 2


def test_parking_the_same_case_twice_does_not_double_up():
    parked.park(CASE, "REF-1", verdict(), finding())
    parked.park(CASE, "REF-1", verdict(), finding())

    assert len(parked.list_parked()) == 1


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------

def _attempts(monkeypatch):
    """Capture what `release_ready` would send to queue_for_replay."""
    calls = []
    from src.dlt import auto_replay

    def fake(case_id, ref_id):
        calls.append((case_id, ref_id))
        return {"queued": True, "result": "ok", "args": {}}

    monkeypatch.setattr(auto_replay, "attempt", fake)
    return calls


def test_a_packet_releases_once_the_pods_reach_its_version(monkeypatch):
    calls = _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    summary = parked.release_ready(["1.0.2"])

    assert summary["released"] == 1
    assert calls == [(CASE, "REF-1")]
    assert parked.list_parked() == []


def test_a_packet_waits_while_the_pods_are_behind(monkeypatch):
    calls = _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    summary = parked.release_ready(["1.0.0"])

    assert summary["waiting"] == 1
    assert summary["released"] == 0
    assert calls == []
    assert len(parked.list_parked()) == 1


def test_release_is_numeric_not_lexical(monkeypatch):
    """Trap T7 reaching the release worker: `1.0.10` is ahead of `1.0.9`."""
    _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.9"), finding())

    assert parked.release_ready(["1.0.10"])["released"] == 1


def test_an_unreadable_running_version_leaves_everything_parked(monkeypatch):
    """Not evidence that a fix shipped."""
    calls = _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    summary = parked.release_ready(["latest"])

    assert summary["unknown"] == 1
    assert summary["released"] == 0
    assert calls == []
    assert len(parked.list_parked()) == 1


def test_releasing_twice_does_not_queue_twice(monkeypatch):
    calls = _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    parked.release_ready(["1.0.2"])
    parked.release_ready(["1.0.2"])

    assert calls == [(CASE, "REF-1")]


def test_a_dry_run_changes_nothing(monkeypatch):
    calls = _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    summary = parked.release_ready(["1.0.2"], dry_run=True)

    assert summary["released"] == 1
    assert calls == []
    assert len(parked.list_parked()) == 1


def test_a_released_entry_records_what_happened(monkeypatch):
    _attempts(monkeypatch)
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())
    parked.release_ready(["1.0.2"])

    entry = parked.list_parked(include_finished=True)[0]
    assert entry["status"] == parked.STATUS_RELEASED
    assert entry["released_at_version"] == "1.0.2"
    assert entry["released_at"] > 0


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------

def test_an_entry_whose_fix_never_deployed_expires(monkeypatch):
    calls = _attempts(monkeypatch)
    monkeypatch.setenv("DLT_PARKED_REPLAY_TTL_SECONDS", "60")
    parked.park(CASE, "REF-1", verdict(required="9.9.9"), finding())

    storage = parked.get_parked_storage()
    entry = storage.load(CASE, filename=parked.PARKED_FILENAME)
    entry["parked_at"] = time.time() - 3600
    storage.save(CASE, entry, filename=parked.PARKED_FILENAME)

    summary = parked.release_ready(["1.0.0"])

    assert summary["expired"] == 1
    assert calls == []
    assert parked.list_parked() == []
    assert parked.list_parked(include_finished=True)[0]["status"] == parked.STATUS_EXPIRED


def test_expiry_beats_release_so_a_stale_packet_is_never_replayed(monkeypatch):
    """A month-old packet is not obviously safe to replay just because the
    version finally moved."""
    calls = _attempts(monkeypatch)
    monkeypatch.setenv("DLT_PARKED_REPLAY_TTL_SECONDS", "60")
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    storage = parked.get_parked_storage()
    entry = storage.load(CASE, filename=parked.PARKED_FILENAME)
    entry["parked_at"] = time.time() - 3600
    storage.save(CASE, entry, filename=parked.PARKED_FILENAME)

    summary = parked.release_ready(["9.9.9"])

    assert summary["expired"] == 1
    assert summary["released"] == 0
    assert calls == []


def test_a_zero_ttl_disables_expiry(monkeypatch):
    _attempts(monkeypatch)
    monkeypatch.setenv("DLT_PARKED_REPLAY_TTL_SECONDS", "0")
    parked.park(CASE, "REF-1", verdict(required="1.0.1"), finding())

    storage = parked.get_parked_storage()
    entry = storage.load(CASE, filename=parked.PARKED_FILENAME)
    entry["parked_at"] = time.time() - 10_000_000
    storage.save(CASE, entry, filename=parked.PARKED_FILENAME)

    summary = parked.release_ready(["1.0.2"])

    assert summary["expired"] == 0
    assert summary["released"] == 1


def test_a_malformed_ttl_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("DLT_PARKED_REPLAY_TTL_SECONDS", "soon")
    assert parked.ttl_seconds() == parked.DEFAULT_TTL_SECONDS


def test_an_empty_store_releases_nothing():
    assert parked.release_ready(["1.0.0"])["examined"] == 0
