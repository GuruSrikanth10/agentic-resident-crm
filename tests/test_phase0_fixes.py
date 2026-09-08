import json
import time
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import MessagePayload
from src.core.agent_orchestrator import is_reviewer_approved
from src.storage.factory import get_casebook_storage
from src.api.routes import process_rejection

from test_resilience import DUMMY_PAYLOAD  # reuse the shared dummy payload


# ======================================================================
# 0.1 -- Drain3 must not leak one flow's templates into another flow's
# evidence, since template_miner.drain.clusters holds every cluster ever
# seen across every packet that shares the persisted parse tree.
# ======================================================================

@pytest.fixture
def isolated_drain3_state(tmp_path, monkeypatch):
    from src.log_pipeline import reducer as reducer_module
    monkeypatch.setattr(reducer_module, "DRAIN3_STATE_DIR", str(tmp_path / "drain3_state"))
    yield


def _log(msg, ts):
    return {"message": msg, "timestamp": ts, "level": "INFO"}


def test_cluster_logs_does_not_leak_templates_across_flows(isolated_drain3_state):
    from src.log_pipeline.reducer import cluster_logs

    # Deliberately different token counts so Drain3 can never bucket them
    # into the same cluster.
    flow_a_logs = [_log("AAA", "2026-01-01T00:00:00")]
    flow_b_logs = [_log("BBB CCC DDD EEE FFF GGG", "2026-01-01T00:01:00")]

    clusters_a = cluster_logs(flow_a_logs)
    clusters_b = cluster_logs(flow_b_logs)

    template_ids_a = {c["template_id"] for c in clusters_a}
    template_ids_b = {c["template_id"] for c in clusters_b}

    # Flow B's output must never contain a cluster only ever matched by flow A.
    assert template_ids_a.isdisjoint(template_ids_b)
    # And flow B must still see its own template.
    assert len(clusters_b) == 1
    assert clusters_b[0]["count"] == 1


# ======================================================================
# 0.4 -- "APPROVED" substring match must not approve rejections.
# ======================================================================

@pytest.mark.parametrize("feedback,expected", [
    ("APPROVED", True),
    ("approved", True),
    ("  APPROVED  ", True),
    ("**APPROVED**", True),
    ("APPROVED. Nice work.", True),
    ("NOT APPROVED", False),
    ("DISAPPROVED", False),
    ("this is not approved because the rule id is wrong", False),
    ("REJECTED: needs fix", False),
])
def test_is_reviewer_approved(feedback, expected):
    assert is_reviewer_approved(feedback) is expected


# ======================================================================
# 0.6 -- idempotency guard must be a total function over
# (is_stale, has_active_checkpoint), not fall through to a reprocess.
# ======================================================================

def test_idempotency_in_progress_not_stale_no_checkpoint_short_circuits():
    """Not stale + no active checkpoint (a run in flight between checkpoint
    writes) must short-circuit, not silently reprocess the whole packet."""
    storage = get_casebook_storage()
    storage.save("test-1234", {
        "packet_metadata": {"eid": "test-1234", "started_at": time.time()},
        "packet_status": {"status": "IN_PROGRESS"}
    }, filename="status.json")

    mock_agent = MagicMock()
    mock_state = MagicMock()
    mock_state.next = None  # no active checkpoint
    mock_agent.get_state.return_value = mock_state

    with patch("src.api.routes.get_agent", return_value=mock_agent):
        res = asyncio.run(process_rejection(MessagePayload(**DUMMY_PAYLOAD)))

    assert res["status"] == "already_processing"
    mock_agent.invoke.assert_not_called()


# ======================================================================
# 0.9 -- consumer.commit() raising after a successful submit() must not
# double-release the queue semaphore.
# ======================================================================

def test_semaphore_not_double_released_when_commit_raises_after_submit():
    import src.utils.kafkaConsumer as kc

    fake_consumer = MagicMock()
    fake_consumer.commit.side_effect = RuntimeError("broker unreachable")

    real_signal = dict(DUMMY_PAYLOAD)
    real_signal["packetExecutionSummary"] = {
        "packetStatus": "REJECTED",
        "errorData": [{"errorReasonCode": "X"}],
    }
    msg = SimpleNamespace(value=json.dumps(real_signal).encode("utf-8"), offset=41)
    tp = MagicMock()

    with patch.object(kc, "consumer", fake_consumer), \
         patch.object(kc, "_worker_pool") as fake_pool, \
         patch("src.storage.factory.get_casebook_storage") as fake_storage_factory:
        fake_storage_factory.return_value.exists.return_value = False
        semaphore_release = MagicMock()
        with patch.object(kc._queue_semaphore, "release", semaphore_release):
            kc._handle_one_message(tp, msg)

        # submit() happened (so _process_and_commit owns a release), then
        # commit() blew up -- the outer handler must NOT also release.
        fake_pool.submit.assert_called_once()
        semaphore_release.assert_not_called()


def test_semaphore_released_once_when_submit_never_happens():
    """Sanity check for the other side of the 0.9 fix: paths that never
    submit (e.g. dedupe skip) must still release exactly once."""
    import src.utils.kafkaConsumer as kc

    fake_consumer = MagicMock()

    real_signal = dict(DUMMY_PAYLOAD)
    real_signal["packetExecutionSummary"] = {
        "packetStatus": "REJECTED",
        "errorData": [{"errorReasonCode": "X"}],
    }
    msg = SimpleNamespace(value=json.dumps(real_signal).encode("utf-8"), offset=7)
    tp = MagicMock()

    with patch.object(kc, "consumer", fake_consumer), \
         patch.object(kc, "_worker_pool") as fake_pool, \
         patch("src.storage.factory.get_casebook_storage") as fake_storage_factory:
        fake_storage_factory.return_value.exists.return_value = True  # already terminal -> dedupe skip
        semaphore_release = MagicMock()
        with patch.object(kc._queue_semaphore, "release", semaphore_release):
            kc._handle_one_message(tp, msg)

        fake_pool.submit.assert_not_called()
        semaphore_release.assert_called_once()


# ======================================================================
# 0.10 -- commits must target only the message just handled, not the whole
# polled batch, so a crash mid-batch can't silently lose later messages.
# ======================================================================

def test_skip_path_records_only_this_message_offset():
    """A message skipped without dispatch becomes committable immediately --
    and contributes only its own offset, never the whole polled batch.

    Skips go through the same OffsetTracker as completions rather than
    committing inline. Committing a skip inline had the identical ordering
    hazard as an out-of-order completion: skipping offset 100 while offset 99
    was still in flight would commit 101 and strand 99 (F3).
    """
    import src.utils.kafkaConsumer as kc
    from kafka.structs import OffsetAndMetadata

    fake_consumer = MagicMock()

    # packetStatus != REJECTED -> skipped without dispatch.
    skipped_signal = dict(DUMMY_PAYLOAD)
    skipped_signal["packetExecutionSummary"] = {
        "packetStatus": "PROCESSED",
        "errorData": [],
    }
    msg = SimpleNamespace(value=json.dumps(skipped_signal).encode("utf-8"), offset=99)
    tp = MagicMock()

    with patch.object(kc, "consumer", fake_consumer), \
         patch.object(kc, "_worker_pool") as fake_pool, \
         patch.object(kc, "_offset_tracker", kc.OffsetTracker()) as tracker, \
         patch("src.storage.factory.get_casebook_storage"):
        kc._handle_one_message(tp, msg)

        fake_pool.submit.assert_not_called()
        # Nothing is committed inline...
        fake_consumer.commit.assert_not_called()
        # ...but this offset alone is now safe to commit.
        assert tracker.take_committable() == {tp: OffsetAndMetadata(100, None, -1)}


def test_dispatched_message_defers_its_commit_to_completion():
    """A message handed to the worker pool must NOT be committed on dispatch.

    Committing at enqueue time is commit-before-work: a crash between dispatch
    and completion advances the group past a packet that was never
    investigated. `_handle_one_message` therefore passes the offset down to
    `_process_and_commit`, which queues it on `_offsets_to_commit` only after
    the work actually succeeded (commit 5cf2dbd).

    The previous version of this test asserted the opposite and was never
    updated when that behaviour changed, so it sat red in the suite (F6).
    """
    import src.utils.kafkaConsumer as kc
    from kafka.structs import OffsetAndMetadata

    fake_consumer = MagicMock()

    real_signal = dict(DUMMY_PAYLOAD)
    real_signal["packetExecutionSummary"] = {
        "packetStatus": "REJECTED",
        "errorData": [{"errorReasonCode": "X"}],
    }
    msg = SimpleNamespace(value=json.dumps(real_signal).encode("utf-8"), offset=99)
    tp = MagicMock()

    with patch.object(kc, "consumer", fake_consumer), \
         patch.object(kc, "_worker_pool") as fake_pool, \
         patch("src.storage.factory.get_casebook_storage") as fake_storage_factory:
        fake_storage_factory.return_value.exists.return_value = False
        kc._handle_one_message(tp, msg)

    # Dispatched, and NOT committed on the way in.
    fake_pool.submit.assert_called_once()
    fake_consumer.commit.assert_not_called()

    # The worker is handed this message's offset alone, so completion commits
    # exactly what was processed.
    _fn, _signal, message_offsets = fake_pool.submit.call_args[0]
    assert message_offsets == {tp: OffsetAndMetadata(100, None, -1)}


# ======================================================================
# 0.11 -- eventId must be constrained so it can't be used for path traversal,
# and the local storage backend must refuse to resolve outside its root even
# if a bad value slips through.
# ======================================================================

def test_event_id_rejects_path_traversal_characters():
    bad_payload = dict(DUMMY_PAYLOAD)
    bad_payload["eventId"] = "../../etc/passwd"
    with pytest.raises(Exception):
        MessagePayload(**bad_payload)


def test_event_id_accepts_normal_values():
    ok_payload = dict(DUMMY_PAYLOAD)
    ok_payload["eventId"] = "abc-123_XYZ.456:789"
    MessagePayload(**ok_payload)  # should not raise


def test_local_storage_rejects_path_escaping_event_id(tmp_path):
    from src.storage.local import LocalFilesystemCasebookStorage

    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    # "casebook_" + event_id contributes one real path segment before the
    # first "..", so three ".." levels are needed to actually climb above
    # base_dir rather than just cancel back down into it.
    with pytest.raises(ValueError):
        storage._get_dir("../../../escaped")
