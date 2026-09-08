"""
Phase B regression tests (ENHANCEMENT_PLAN.md section 5).

F3  -- offsets commit at the low-water mark, never past in-flight work.
F4  -- terminal status reaches both files, and the late-result guard sees it.
F5  -- the consumer isn't rate-limited; API keys compare in constant time.
F9  -- the heartbeat ticks independently of the poll loop.
F10 -- redaction covers every log source, not just Kubernetes.
F12 -- SIGTERM stops the poll loop and drains.
"""
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kafka.structs import OffsetAndMetadata

import src.utils.kafkaConsumer as kc


# ======================================================================
# F3 -- low-water-mark offset commits
# ======================================================================

def test_out_of_order_completion_does_not_commit_past_inflight_work():
    """The core F3 bug: 12 finishing first must not commit past 10 and 11.

    Committing 13 here and then crashing resumes the group at 13, and offsets
    10 and 11 are never redelivered -- silent message loss.
    """
    tracker = kc.OffsetTracker()
    tp = "partition-0"

    for offset in (10, 11, 12):
        tracker.dispatched(tp, offset)

    tracker.completed(tp, 12)

    # 10 is still the lowest in-flight offset, so nothing is committable.
    assert tracker.take_committable() == {}

    tracker.completed(tp, 10)
    # 11 is still in flight, so only 10 clears -- commit position 11.
    assert tracker.take_committable() == {tp: OffsetAndMetadata(11, None, -1)}

    tracker.completed(tp, 11)
    # Now everything has finished, so the watermark jumps to 13.
    assert tracker.take_committable() == {tp: OffsetAndMetadata(13, None, -1)}


def test_committed_offsets_are_retired_so_the_tracker_does_not_grow():
    tracker = kc.OffsetTracker()
    tp = "p"
    for offset in range(100):
        tracker.dispatched(tp, offset)
        tracker.completed(tp, offset)

    assert tracker.take_committable() == {tp: OffsetAndMetadata(100, None, -1)}
    assert tracker.take_committable() == {}
    assert tracker.in_flight() == 0


def test_in_flight_counts_only_uncompleted_work():
    tracker = kc.OffsetTracker()
    tp = "p"
    tracker.dispatched(tp, 1)
    tracker.dispatched(tp, 2)
    assert tracker.in_flight() == 2
    tracker.completed(tp, 1)
    assert tracker.in_flight() == 1


def test_partitions_are_tracked_independently():
    tracker = kc.OffsetTracker()
    tracker.dispatched("a", 5)
    tracker.dispatched("b", 9)
    tracker.completed("b", 9)

    commits = tracker.take_committable()
    assert commits == {"b": OffsetAndMetadata(10, None, -1)}


def test_a_failed_investigation_blocks_the_watermark():
    """An uncommitted failure must hold the floor so Kafka redelivers it."""
    tracker = kc.OffsetTracker()
    tp = "p"
    tracker.dispatched(tp, 1)   # will fail -- never completed
    tracker.dispatched(tp, 2)
    tracker.completed(tp, 2)

    assert tracker.take_committable() == {}


# ======================================================================
# F4 -- terminal status reaches both files
# ======================================================================

@pytest.fixture
def storage(tmp_path):
    from src.storage.local import LocalFilesystemCasebookStorage
    return LocalFilesystemCasebookStorage(base_dir=str(tmp_path))


def test_save_terminal_writes_both_files(storage):
    storage.save_terminal("evt-1", {
        "packet_metadata": {"eid": "evt-1"},
        "packet_status": {"status": "FAILED_TIMEOUT"},
        "resolution": {"synthesis": "timed out"},
    })

    casebook = storage.load("evt-1", filename="casebook.json")
    status = storage.load("evt-1", filename="status.json")

    assert casebook["packet_status"]["status"] == "FAILED_TIMEOUT"
    assert status["packet_status"]["status"] == "FAILED_TIMEOUT"


def test_terminal_status_sees_a_verdict_in_either_file(storage):
    """The exact F4 hole: the consumer wrote casebook.json, the guard read
    status.json, so the guard never fired."""
    # Consumer-style write: casebook.json only, status.json left IN_PROGRESS.
    storage.save("evt-2", {
        "packet_metadata": {"eid": "evt-2"},
        "packet_status": {"status": "FAILED_TIMEOUT"},
    })
    storage.save("evt-2", {
        "packet_metadata": {"eid": "evt-2"},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    assert storage.terminal_status("evt-2") == "FAILED_TIMEOUT"


def test_terminal_status_is_none_while_in_progress(storage):
    storage.save("evt-3", {
        "packet_metadata": {"eid": "evt-3"},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    assert storage.terminal_status("evt-3") is None


def test_terminal_status_is_none_for_an_unknown_event(storage):
    assert storage.terminal_status("never-seen") is None


# ======================================================================
# F5 -- the consumer is not rate-limited; keys compare in constant time
# ======================================================================

def _request(host, headers=None):
    return SimpleNamespace(
        client=SimpleNamespace(host=host),
        headers=headers or {},
    )


def test_loopback_caller_is_exempt_from_rate_limiting():
    """The consumer forwards every packet from one IP. Rate-limiting it made
    the 11th packet in any minute a 429, stalling the whole pipeline (F5)."""
    from src.api import routes

    # Far more than any configured limit.
    for _ in range(500):
        routes.rate_limiter(_request("127.0.0.1"))


def test_external_caller_is_still_rate_limited():
    from src.api import routes
    from fastapi import HTTPException

    routes._rate_limits.clear()
    with pytest.raises(HTTPException) as excinfo:
        for _ in range(routes.RATE_LIMIT + 5):
            routes.rate_limiter(_request("203.0.113.9"))
    assert excinfo.value.status_code == 429


def test_forwarded_for_is_ignored_from_an_untrusted_peer():
    """Otherwise any caller could forge an exempt source address."""
    from src.api import routes

    resolved = routes.client_ip_for(
        _request("203.0.113.9", {"x-forwarded-for": "127.0.0.1"})
    )
    assert resolved == "203.0.113.9"


def test_forwarded_for_is_honoured_from_a_trusted_proxy(monkeypatch):
    from src.api import routes
    import ipaddress

    monkeypatch.setattr(routes, "_TRUSTED_PROXY_CIDRS",
                        [ipaddress.ip_network("10.0.0.0/8")])
    resolved = routes.client_ip_for(
        _request("10.1.2.3", {"x-forwarded-for": "198.51.100.7, 10.1.2.3"})
    )
    assert resolved == "198.51.100.7"


def test_api_key_check_accepts_the_valid_key_and_rejects_others():
    from src.api import routes
    from fastapi import HTTPException

    with patch.object(routes, "API_KEYS", ["correct-key"]):
        assert routes.get_api_key("correct-key") == "correct-key"
        for bad in ("wrong-key", "", None, "correct-ke"):
            with pytest.raises(HTTPException):
                routes.get_api_key(bad)


# ======================================================================
# F9 -- heartbeat independent of the poll loop
# ======================================================================

def test_heartbeat_is_written_atomically(tmp_path):
    """/health reads this concurrently; a truncate-and-write can be read
    empty mid-write."""
    heartbeat = tmp_path / "consumer_heartbeat.txt"
    kc._write_heartbeat(heartbeat)

    assert float(heartbeat.read_text()) > 0
    assert not (tmp_path / "consumer_heartbeat.tmp").exists()


def test_heartbeat_thread_ticks_without_the_poll_loop(tmp_path, monkeypatch):
    """The whole point of F9: the poll loop blocks on the semaphore for
    minutes under load, and the heartbeat must not block with it."""
    heartbeat = tmp_path / "consumer_heartbeat.txt"
    monkeypatch.setattr(kc, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
    kc._shutdown.clear()
    try:
        thread = kc._start_heartbeat_thread(heartbeat)
        time.sleep(0.1)
        first = float(heartbeat.read_text())
        time.sleep(0.1)
        second = float(heartbeat.read_text())
        assert second > first
    finally:
        kc._shutdown.set()
        thread.join(timeout=2)
        kc._shutdown.clear()


def _replace_failing_n_times(n, failures):
    """Stand-in for os.replace that denies the first `n` heartbeat swaps.

    Only heartbeat swaps: every other replace in the process (temp-file
    writes elsewhere, logging) must keep working while this is installed.
    """
    import os as _os

    real = _os.replace

    def _fake(src, dst, *args, **kwargs):
        if str(dst).endswith("consumer_heartbeat.txt") and len(failures) < n:
            failures.append(str(dst))
            raise PermissionError(5, "Access is denied")
        return real(src, dst, *args, **kwargs)

    return _fake


def test_heartbeat_write_retries_a_transient_lock(tmp_path, monkeypatch):
    """Windows denies os.replace while another process holds the destination
    open (OneDrive, the indexer, an AV scan). Those holders release in
    milliseconds, so the tick must not be lost to one."""
    import os

    heartbeat = tmp_path / "consumer_heartbeat.txt"
    failures = []
    monkeypatch.setattr(os, "replace", _replace_failing_n_times(2, failures))
    monkeypatch.setattr(kc, "HEARTBEAT_WRITE_BACKOFF_SECONDS", 0.001)

    kc._write_heartbeat(heartbeat)

    assert len(failures) == 2, "both transient denials should have been hit"
    assert float(heartbeat.read_text()) > 0, "the third attempt must land"
    assert not (tmp_path / "consumer_heartbeat.tmp").exists()


def test_heartbeat_write_never_raises_when_every_attempt_fails(tmp_path, monkeypatch):
    """The ticker calls this in a bare loop: an exception escaping here kills
    the heartbeat thread for the life of the process."""
    import os

    heartbeat = tmp_path / "consumer_heartbeat.txt"
    monkeypatch.setattr(os, "replace", _replace_failing_n_times(99, []))
    monkeypatch.setattr(kc, "HEARTBEAT_WRITE_BACKOFF_SECONDS", 0.001)

    kc._write_heartbeat(heartbeat)  # must return, not raise

    assert not heartbeat.exists()


def test_liveness_survives_a_heartbeat_file_that_cannot_be_written(tmp_path, monkeypatch):
    """The reason the stamp moved into memory: a locked, full, or read-only
    filesystem must not make a running consumer report itself dead and earn a
    liveness-probe restart that kills every in-flight packet."""
    import os

    heartbeat = tmp_path / "consumer_heartbeat.txt"
    monkeypatch.setattr(os, "replace", _replace_failing_n_times(99, []))
    monkeypatch.setattr(kc, "HEARTBEAT_WRITE_BACKOFF_SECONDS", 0.001)
    monkeypatch.setattr(kc, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(kc, "_heartbeat_path", lambda: heartbeat)
    kc._shutdown.clear()

    try:
        thread = kc._start_heartbeat_thread(heartbeat)
        time.sleep(0.1)

        assert not heartbeat.exists(), "the write is genuinely failing"
        assert kc.is_healthy() is True, "liveness must not depend on the disk"
    finally:
        kc._shutdown.set()
        thread.join(timeout=2)
        kc._shutdown.clear()

    # The ticker is gone, so memory stops being authoritative and the file --
    # which was never written -- is all that is left.
    assert kc._last_heartbeat is None
    assert kc.is_healthy() is False


# ======================================================================
# F12 -- shutdown request stops the loop
# ======================================================================

def test_request_shutdown_sets_the_flag():
    kc._shutdown.clear()
    try:
        kc.request_shutdown(15, None)
        assert kc._shutdown.is_set()
    finally:
        kc._shutdown.clear()


def test_drain_commits_what_completed_and_leaves_the_rest(monkeypatch):
    """A packet still mid-LLM-call simply isn't committed -- Kafka redelivers
    it, which is the correct at-least-once outcome."""
    fake_consumer = MagicMock()
    tracker = kc.OffsetTracker()
    tp = "p"
    tracker.dispatched(tp, 1)
    tracker.completed(tp, 1)
    tracker.dispatched(tp, 2)   # never completes

    monkeypatch.setattr(kc, "consumer", fake_consumer)
    monkeypatch.setattr(kc, "_offset_tracker", tracker)
    monkeypatch.setattr(kc, "SHUTDOWN_DRAIN_SECONDS", 0.05)
    monkeypatch.setattr(kc, "_worker_pool", MagicMock())

    kc._drain_and_commit()

    fake_consumer.commit.assert_called_once_with({tp: OffsetAndMetadata(2, None, -1)})
    fake_consumer.close.assert_called_once()


# ======================================================================
# F10 -- redaction covers every source
# ======================================================================

def test_elasticsearch_path_records_are_redacted_before_persistence(tmp_path, monkeypatch):
    """Elasticsearch is the path most deployments actually use, and it never
    redacted -- so resident PII reached raw_logs.txt, the casebook and S3."""
    from src.log_pipeline import pipeline
    from src.log_pipeline.types import FetchDiagnostics, FetchResult

    aadhaar = "123456789012"
    mobile = "9876543210"
    records = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "level": "INFO",
            "message": f"resident uid={aadhaar} mobile={mobile} eventId=evt-42",
            "app_name": "enu-biometric",
            "source": "elastic",
        }
    ]

    monkeypatch.setattr(pipeline, "_save_reduced_logs", lambda *_a, **_kw: "reduced")
    monkeypatch.setattr(
        pipeline.source_chain, "fetch_with_fallback",
        lambda *_a, **_kw: FetchResult(
            records=records, diagnostics=FetchDiagnostics(source="elastic")
        ),
    )

    written = {}

    # **_kw absorbs the `storage=` argument the artifact writers gained in
    # Phase 5 of DLT_PLAN.md, matching the other stubs in this file.
    def capture_raw(event_id, logs, **_kw):
        written["logs"] = [dict(entry) for entry in logs]
        return "raw_logs.txt"

    monkeypatch.setattr(pipeline, "_save_raw_logs", capture_raw)

    output = pipeline.reduce_logs("evt-42")

    persisted = written["logs"][0]["message"]
    assert aadhaar not in persisted
    assert mobile not in persisted
    assert "[REDACTED:AADHAAR]" in persisted
    assert "[REDACTED:MOBILE]" in persisted

    # ...and nothing unredacted reaches the LLM-facing text either.
    assert aadhaar not in output
    assert mobile not in output

    # The correlation id is operational, not resident PII -- it must survive,
    # or the investigation loses its anchor.
    assert "evt-42" in persisted


def test_extra_identifiers_are_allowlisted_from_redaction(monkeypatch):
    """A 12-digit refId would otherwise be scrubbed as an Aadhaar number."""
    from src.log_pipeline import pipeline
    from src.log_pipeline.types import FetchDiagnostics, FetchResult

    ref_id = "998877665544"   # 12 digits: matches the Aadhaar pattern
    records = [{
        "timestamp": "2026-01-01T00:00:00Z",
        "level": "INFO",
        "message": f"processing refId={ref_id}",
        "app_name": "svc",
    }]

    monkeypatch.setattr(pipeline, "_save_reduced_logs", lambda *_a, **_kw: "reduced")
    monkeypatch.setattr(pipeline, "_save_raw_logs", lambda *_a, **_kw: "raw")
    monkeypatch.setattr(
        pipeline.source_chain, "fetch_with_fallback",
        lambda *_a, **_kw: FetchResult(
            records=records, diagnostics=FetchDiagnostics(source="elastic")
        ),
    )

    output = pipeline.reduce_logs("evt-1", extra_identifiers=(ref_id,))
    assert ref_id in output


def test_redaction_is_idempotent():
    """The k8s source redacts before its snapshot write, then the pipeline
    redacts again. The second pass must be a no-op, not a corruption."""
    from src.log_pipeline import redaction

    once = redaction.redact_text("uid=123456789012").text
    twice = redaction.redact_text(once).text
    assert once == twice


# ======================================================================
# F18 (pulled forward) -- extra patterns are compiled once, not per record
# ======================================================================

def test_extra_patterns_are_cached(monkeypatch):
    from src.log_pipeline import redaction

    redaction._extra_patterns_cache.clear()
    monkeypatch.setenv("K8S_REDACT_EXTRA_PATTERNS", r"SECRET-\d+")

    first = redaction._extra_patterns()
    second = redaction._extra_patterns()
    assert first is second   # same tuple object -> no recompilation

    result = redaction.redact_text("token SECRET-42 here")
    assert "[REDACTED:CUSTOM]" in result.text


def test_changing_extra_patterns_is_picked_up(monkeypatch):
    from src.log_pipeline import redaction

    redaction._extra_patterns_cache.clear()
    monkeypatch.setenv("K8S_REDACT_EXTRA_PATTERNS", r"AAA")
    assert "[REDACTED:CUSTOM]" in redaction.redact_text("AAA").text

    monkeypatch.setenv("K8S_REDACT_EXTRA_PATTERNS", r"BBB")
    assert "[REDACTED:CUSTOM]" in redaction.redact_text("BBB").text


# ======================================================================
# CONSUMER_ROLE -- the fetch/analyze split's constant resolution
# (ARCHITECTURE.md 3.11)
# ======================================================================

def test_consumer_role_slow_resolves_the_slow_defaults():
    """kafkaConsumer.py's CONSUMER_ROLE branch is read once, at import time.

    This spawns a fresh interpreter with CONSUMER_ROLE=slow rather than
    reloading the already-imported `kc` module in-process: every other test
    in this file (and ~40 call sites across test_audit_phase1.py,
    test_audit_phase3.py, test_phase0_fixes.py, test_resilience.py) imports
    the SAME cached module object via `import src.utils.kafkaConsumer as kc`
    and relies on its default (fast) constants -- an in-process reload would
    mutate that shared object out from under all of them for the rest of the
    test session.

    The default (fast) role is already exhaustively exercised by every one
    of those ~40 call sites, none of which ever set CONSUMER_ROLE, so this
    only needs to prove the slow branch resolves independently and
    differently -- not re-prove the fast defaults.
    """
    import json
    import os
    import subprocess
    import sys

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = (
        "import json, src.utils.kafkaConsumer as kc\n"
        "print(json.dumps({\n"
        "    'topic': kc.kafkaConsumerTopicName,\n"
        "    'group': kc.kafkaConsumerGroupId,\n"
        "    'endpoint': kc.kafkaConsumerInternalEndpoint,\n"
        "    'timeout': kc.kafkaConsumerInternalTimeoutSec,\n"
        "    'heartbeat_path': str(kc._heartbeat_path()),\n"
        "    'health_port_env': kc._HEALTH_PORT_ENV,\n"
        "}))\n"
    )

    env = dict(os.environ)
    env["CONSUMER_ROLE"] = "slow"
    env["PYTHONPATH"] = repo_root
    # Pinned explicitly so the assertions below don't depend on whatever the
    # developer's local .env happens to contain -- load_dotenv() never
    # overrides an already-set variable, so this wins regardless.
    env["PACKET_TIMEOUT_SECONDS"] = "300"
    for var in ("SLOW_CONSUMER_TOPIC_NAME", "SLOW_CONSUMER_GROUP_ID",
                "SLOW_CONSUMER_ENDPOINT", "SLOW_CONSUMER_HEALTH_PORT"):
        env.pop(var, None)

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root, env=env, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    resolved = json.loads(result.stdout.strip().splitlines()[-1])

    assert resolved["topic"] == "packet-analysis-queue"
    assert resolved["group"] == "packet-analysis-group"
    assert resolved["endpoint"] == "http://localhost:8000/analyze-rejection"
    assert resolved["timeout"] == 300.0
    assert resolved["health_port_env"] == "SLOW_CONSUMER_HEALTH_PORT"

    from src.utils import paths
    assert resolved["heartbeat_path"] == str(paths.SLOW_CONSUMER_HEARTBEAT_PATH)
    assert resolved["heartbeat_path"] != str(paths.CONSUMER_HEARTBEAT_PATH)
