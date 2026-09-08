"""
Phase 1 regression tests (AUDIT_2026_08.md section 6).

G1 -- a failed message retires its offset instead of freezing the commit floor.
G2 -- the outcome loop goes through CasebookStorage, so it works on any backend.
G3 -- log artifacts and snapshots go through the storage layer too.
G4 -- the checkpointer is built once, not once per readiness probe.
G5 -- routes.py enforces the synthesis contract and persists confidence.
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kafka.structs import OffsetAndMetadata

import src.utils.kafkaConsumer as kc


# ======================================================================
# G1 -- an abandoned offset must not pin the commit floor
# ======================================================================

def test_abandoned_offset_releases_the_commit_floor():
    """The core G1 bug.

    Offsets 10, 11, 12 dispatched; 11 fails. Before the fix, 11 stayed in
    `_dispatched` forever, `min(still_running)` was permanently 11, and no
    offset on this partition could ever be committed again -- the consumer
    kept working and silently stopped committing.
    """
    tracker = kc.OffsetTracker()
    tp = "partition-0"

    for offset in (10, 11, 12):
        tracker.dispatched(tp, offset)

    tracker.completed(tp, 10)
    tracker.completed(tp, 12)

    # 11 is still in flight, so only 10 is safe so far.
    commits = tracker.take_committable()
    assert commits[tp].offset == 11

    # 11 now fails and is routed to the DLQ.
    tracker.abandoned(tp, 11)

    # 12 becomes committable: the floor moved, which is the whole point.
    commits = tracker.take_committable()
    assert commits[tp].offset == 13
    assert tracker.in_flight() == 0


def test_abandoned_offset_is_not_reported_as_in_flight():
    """`_drain_and_commit` loops until in_flight() == 0.

    A stranded offset made that condition unreachable, so every shutdown
    burned the full SHUTDOWN_DRAIN_SECONDS waiting for work that had already
    failed.
    """
    tracker = kc.OffsetTracker()
    tp = "partition-0"

    tracker.dispatched(tp, 5)
    assert tracker.in_flight() == 1

    tracker.abandoned(tp, 5)
    assert tracker.in_flight() == 0


def test_processing_failure_publishes_to_dlq_and_frees_the_floor(monkeypatch):
    """A storage error inside _handle_one_message must DLQ, not stall."""
    tp = "partition-0"
    monkeypatch.setattr(kc, "_offset_tracker", kc.OffsetTracker())

    published = []
    monkeypatch.setattr(
        "src.utils.dlq_publisher.publish_to_dlq",
        lambda payload, error: published.append((payload, error)),
    )

    # Blow up at the dedupe check, after the offset is already dispatched.
    def exploding_storage():
        raise RuntimeError("S3 is having a moment")

    # Phase 4 moved the dedupe check into RejectionAdapter.should_skip, which
    # imports the factory at call time. Patching the factory itself injects the
    # same failure at the same point and survives further refactors.
    monkeypatch.setattr("src.storage.factory.get_casebook_storage", exploding_storage)
    monkeypatch.setattr(kc._queue_semaphore, "release", lambda: None)

    msg = SimpleNamespace(
        offset=7,
        value=json.dumps({
            "eventId": "EVT-G1",
            "packetExecutionSummary": {"packetStatus": "REJECTED"},
        }).encode("utf-8"),
    )

    kc._handle_one_message(tp, msg)

    assert len(published) == 1, "the failed message must reach the DLQ"
    assert "S3 is having a moment" in published[0][1]
    assert kc._offset_tracker.in_flight() == 0, "the floor must be released"


def test_dlq_failure_holds_the_offset_rather_than_losing_the_message(monkeypatch):
    """If the DLQ is unreachable too, stalling is the lesser evil.

    Advancing the floor past a message that exists nowhere would be silent
    loss; holding it is recoverable on restart.
    """
    tp = "partition-0"
    monkeypatch.setattr(kc, "_offset_tracker", kc.OffsetTracker())

    def dead_dlq(payload, error):
        raise ConnectionError("broker unreachable")

    monkeypatch.setattr("src.utils.dlq_publisher.publish_to_dlq", dead_dlq)
    monkeypatch.setattr("src.storage.factory.get_casebook_storage",
                        lambda: (_ for _ in ()).throw(RuntimeError("storage down")))
    monkeypatch.setattr(kc._queue_semaphore, "release", lambda: None)

    msg = SimpleNamespace(
        offset=7,
        value=json.dumps({
            "eventId": "EVT-G1B",
            "packetExecutionSummary": {"packetStatus": "REJECTED"},
        }).encode("utf-8"),
    )

    kc._handle_one_message(tp, msg)

    assert kc._offset_tracker.in_flight() == 1, "offset must stay held"


def test_forwarding_failure_publishes_to_dlq_and_frees_the_floor(monkeypatch):
    """Same guarantee on the worker-thread path (_process_and_commit)."""
    tp = "partition-0"
    monkeypatch.setattr(kc, "_offset_tracker", kc.OffsetTracker())
    kc._offset_tracker.dispatched(tp, 3)

    published = []
    monkeypatch.setattr(
        "src.utils.dlq_publisher.publish_to_dlq",
        lambda payload, error: published.append((payload, error)),
    )
    monkeypatch.setattr(kc, "forward_signal_to_internal_endpoint",
                        lambda payload: (_ for _ in ()).throw(RuntimeError("API down")))
    monkeypatch.setattr(kc._queue_semaphore, "release", lambda: None)

    message_offsets = {tp: OffsetAndMetadata(4, None, -1)}
    kc._process_and_commit({"eventId": "EVT-G1C"}, message_offsets)

    assert len(published) == 1
    assert "API down" in published[0][1]
    assert kc._offset_tracker.in_flight() == 0


# ======================================================================
# G4 -- the checkpointer is built once, not once per readiness probe
# ======================================================================

def test_checkpointer_is_memoised(monkeypatch, tmp_path):
    """Every /ready probe used to build a fresh saver.

    On postgres that opened a connection and ran setup()'s DDL each time and
    closed none of them, so a 5s probe interval exhausted max_connections
    within the hour.
    """
    from src.core import checkpointer

    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "cp.db")
    checkpointer.reset_checkpointer()

    builds = []
    real_build = checkpointer._build_sqlite

    def counting_build():
        builds.append(1)
        return real_build()

    monkeypatch.setattr(checkpointer, "_build_sqlite", counting_build)

    first = checkpointer.get_checkpointer()
    for _ in range(20):
        checkpointer.get_checkpointer()

    assert len(builds) == 1, "21 calls must build exactly one checkpointer"
    assert checkpointer.get_checkpointer() is first
    checkpointer.reset_checkpointer()


def test_checkpointer_rebuilds_when_the_config_changes(monkeypatch, tmp_path):
    """Memoising must not pin a stale configuration."""
    from src.core import checkpointer

    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "one.db")
    checkpointer.reset_checkpointer()
    first = checkpointer.get_checkpointer()

    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "two.db")
    second = checkpointer.get_checkpointer()

    assert first is not second
    checkpointer.reset_checkpointer()


def test_misconfiguration_still_raises_after_the_cache_is_warm(monkeypatch, tmp_path):
    """Validation runs ahead of the cache, so it is not skipped on later calls."""
    from src.core import checkpointer

    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "cp.db")
    checkpointer.reset_checkpointer()
    checkpointer.get_checkpointer()

    monkeypatch.setenv("CHECKPOINT_BACKEND", "mongodb")
    with pytest.raises(ValueError, match="Unknown CHECKPOINT_BACKEND"):
        checkpointer.get_checkpointer()
    checkpointer.reset_checkpointer()


def test_health_check_probes_the_live_connection(monkeypatch, tmp_path):
    from src.core import checkpointer

    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "cp.db")
    checkpointer.reset_checkpointer()

    assert checkpointer.health_check() is True
    checkpointer.reset_checkpointer()


# ======================================================================
# G5 -- routes.py enforces the contract and persists confidence
# ======================================================================

def _run_packet(event_id, synthesis, logs=None):
    """Drive process_rejection with a stubbed agent and return the casebook."""
    import asyncio
    from src.api.routes import process_rejection
    from src.models.schemas import MessagePayload
    from src.storage.factory import get_casebook_storage
    from test_phase1_fixes import _payload_with_event_id

    agent = MagicMock()
    agent.get_state.return_value = None
    agent.invoke.return_value = {"synthesis": synthesis, "logs": logs}

    with patch("src.api.routes.get_agent", return_value=agent):
        asyncio.run(process_rejection(
            MessagePayload(**_payload_with_event_id(event_id))
        ))
    return get_casebook_storage().load(event_id)


def test_confidence_reaches_the_casebook():
    """Confidence was computed by the policy and then dropped on the floor.

    Nothing downstream could correlate stated confidence against recorded
    correctness, so SYNTHESIS_CONFIDENCE_THRESHOLD could never be calibrated
    and the abstention feature could never be responsibly switched on.
    """
    from test_phase1_fixes import _cleanup_casebook

    event_id = "g5-confidence"
    try:
        casebook = _run_packet(event_id, json.dumps({
            "rejection_description": "biometric mismatch",
            "synthesis": "replay the packet",
            "action": "REPLAY",
            "resident_action": "NEW_PACKET",
            "confidence": 0.42,
        }))
        assert casebook["resolution"]["confidence"] == 0.42
        assert casebook["resolution"]["abstained"] is False
        assert casebook["resolution"]["action"] == "REPLAY"
    finally:
        _cleanup_casebook(event_id)


def test_invalid_action_is_not_persisted_as_null():
    """An out-of-enum action used to be written through verbatim.

    The casebook then carried action: "REPLAY_PACKET" (or action: null after
    the str() fallback), which downstream could not distinguish from a packet
    the agents genuinely could not classify.
    """
    from test_phase1_fixes import _cleanup_casebook

    event_id = "g5-bad-action"
    try:
        casebook = _run_packet(event_id, json.dumps({
            "synthesis": "ok",
            "action": "REPLAY_PACKET",          # not in ACTIONS
            "resident_action": "NEW_PACKET",
        }))
        assert casebook["packet_status"]["status"] == "FAILED_SYNTHESIS_PARSE"
        assert casebook["resolution"]["action"] == "MANUAL_REVIEW"
        assert "REPLAY_PACKET" in casebook["resolution"]["raw_output"]
        assert casebook["resolution"]["parse_error"]
    finally:
        _cleanup_casebook(event_id)


def test_unparseable_output_preserves_the_evidence():
    """A contract breach must not also cost us the trace that explains it."""
    from test_phase1_fixes import _cleanup_casebook

    event_id = "g5-garbage"
    try:
        casebook = _run_packet(
            event_id,
            "the model rambled and never produced JSON",
            logs="ERROR something went wrong",
        )
        assert casebook["packet_status"]["status"] == "FAILED_SYNTHESIS_PARSE"
        rejection = casebook["packet_status"]["rejection_data"]
        # `rejection_logs` is a {"path", "gaps"} locator now, not the log text:
        # the text goes to S3 or to the raw_logs.txt artifact, and the casebook
        # records where. What this test cares about is that the record is still
        # written on the failure path -- a contract breach must not also erase
        # the pointer to the trace.
        assert rejection["rejection_logs"]["path"]
        assert rejection["rejection_code"] is not None
        # The trace that explains the breach: the model's verbatim output and
        # the validator's complaint about it, both persisted alongside.
        assert "rambled" in casebook["resolution"]["raw_output"]
        assert casebook["resolution"]["parse_error"]
        assert casebook["packet_metadata"]["eid"] == event_id
    finally:
        _cleanup_casebook(event_id)


def test_abstention_is_recorded_distinctly_from_a_genuine_manual_review():
    """apply_confidence_policy sets abstained on the model, so it survives
    serialisation and reaches the casebook."""
    from src.models.synthesis import SynthesisResult, apply_confidence_policy

    result = SynthesisResult(
        synthesis="replay it", action="REPLAY",
        resident_action="NEW_PACKET", confidence=0.1,
    )
    updated, abstained, _reason = apply_confidence_policy(result, logs="")

    assert abstained is False, "threshold defaults to 0 (disabled)"
    assert updated.abstained is False

    import os
    os.environ["SYNTHESIS_CONFIDENCE_THRESHOLD"] = "0.5"
    try:
        updated, abstained, _reason = apply_confidence_policy(result, logs="")
        assert abstained is True
        assert updated.abstained is True
        assert updated.action == "MANUAL_REVIEW"
        assert "abstained" in updated.model_dump_json()
    finally:
        del os.environ["SYNTHESIS_CONFIDENCE_THRESHOLD"]


# ======================================================================
# G2 / G3 -- outcomes and artifacts are backend-agnostic
# ======================================================================

class _InMemoryStorage:
    """A third backend, implementing only the protocol.

    Standing in for S3 without needing one: if the outcome loop or the
    snapshot layer still reaches for the local filesystem anywhere, these
    tests fail. That is precisely what G2 and G3 were -- code that worked
    only because the local backend happened to also be a real directory.
    """

    def __init__(self):
        self.docs = {}
        self.artifacts = {}

    def save(self, event_id, casebook, filename="casebook.json"):
        self.docs[(event_id, filename)] = casebook

    def load(self, event_id, filename="casebook.json"):
        return self.docs.get((event_id, filename))

    def save_artifact(self, event_id, filename, content):
        self.artifacts[(event_id, filename)] = content
        return f"memory://{event_id}/{filename}"

    def load_artifact(self, event_id, filename):
        return self.artifacts.get((event_id, filename))

    def artifact_exists(self, event_id, filename):
        return (event_id, filename) in self.artifacts

    def list_events(self):
        return sorted({event_id for event_id, _ in self.docs})


def test_outcomes_work_on_a_non_filesystem_backend(monkeypatch):
    """POST /outcome returned 404 for every event under the S3 backend, and
    accuracy_report reported an empty dataset no matter how many verdicts
    had been recorded."""
    from src.utils import outcomes

    storage = _InMemoryStorage()
    monkeypatch.setattr(outcomes, "get_casebook_storage", lambda: storage)

    storage.save("evt-s3", {
        "packet_metadata": {"eid": "evt-s3", "update_type": "U"},
        "packet_status": {"status": "COMPLETED",
                          "rejection_data": {"rejection_code": "DEDUP"}},
        "resolution": {"source": "agent", "action": "REPLAY"},
    })

    outcome = outcomes.record_outcome("evt-s3", "CORRECT", "operator")
    assert outcome["verdict"] == "CORRECT"
    assert outcomes.load_outcome("evt-s3")["verdict"] == "CORRECT"

    summary = outcomes.summarise(outcomes.iter_outcomes())
    assert summary["total_outcomes"] == 1
    assert summary["rows"][0]["reason_code"] == "DEDUP"


def test_unknown_event_still_rejected_on_a_non_filesystem_backend(monkeypatch):
    from src.utils import outcomes

    monkeypatch.setattr(outcomes, "get_casebook_storage", _InMemoryStorage)
    with pytest.raises(outcomes.UnknownEventError):
        outcomes.record_outcome("never-seen", "CORRECT", "operator")


def test_snapshot_round_trips_on_a_non_filesystem_backend(monkeypatch):
    """Snapshot reuse is what makes the Kubernetes source viable across
    retries and replays; it was silently off for every S3 deployment."""
    from src.log_pipeline import snapshot

    storage = _InMemoryStorage()
    monkeypatch.setattr(snapshot, "get_casebook_storage", lambda: storage)
    monkeypatch.delenv("LOG_SNAPSHOT_REUSE", raising=False)

    records = [{"timestamp": "2026-01-01T00:00:00Z", "level": "INFO",
                "message": "hello", "app_name": "svc"}]
    assert snapshot.save("evt-mem", records) is True

    loaded, gaps = snapshot.load("evt-mem")
    assert loaded == records
    assert gaps == []


def test_log_artifacts_go_through_storage(monkeypatch):
    """raw_logs.txt / reduced_logs.txt lived on pod-local disk even when the
    casebook itself was in S3, so the evidence died with the pod."""
    from src.log_pipeline import pipeline

    storage = _InMemoryStorage()
    monkeypatch.setattr(pipeline, "get_casebook_storage", lambda: storage)

    locator = pipeline._save_raw_logs("evt-art", [
        {"timestamp": "T0", "level": "ERROR", "message": "boom",
         "app_name": "svc"},
    ])

    assert locator == "memory://evt-art/raw_logs.txt"
    assert "boom" in storage.artifacts[("evt-art", "raw_logs.txt")]


def test_pruning_preserves_recorded_outcomes(tmp_path, monkeypatch):
    """Ground truth must survive routine disk hygiene."""
    from src.tools import prune_casesheets

    directory = tmp_path / "casebook_evt-old"
    directory.mkdir(parents=True)
    (directory / "casebook.json").write_text(
        json.dumps({"packet_status": {"status": "COMPLETED"}}), encoding="utf-8")
    (directory / "raw_logs.txt").write_text("bulky", encoding="utf-8")
    (directory / "outcome.json").write_text(
        json.dumps({"verdict": "CORRECT"}), encoding="utf-8")

    import os
    old = 1
    os.utime(directory, (old, old))

    monkeypatch.setattr("sys.argv", [
        "prune_casesheets", "--root", str(tmp_path), "--older-than-days", "1",
    ])
    prune_casesheets.main()

    assert (directory / "outcome.json").exists(), "the verdict must survive"
    assert not (directory / "raw_logs.txt").exists()
    assert not (directory / "casebook.json").exists()
