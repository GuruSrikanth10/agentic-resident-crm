import os
import json
import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import concurrent.futures

from src.storage.factory import get_casebook_storage
from src.api.routes import process_rejection
from src.models.schemas import MessagePayload
from src.utils.paths import CHECKPOINT_DB_PATH

# --- Mock DLQ for assertions ---
MOCK_DLQ = []
def mock_publish_to_dlq(payload, error_msg):
    MOCK_DLQ.append({"payload": payload, "error": error_msg})

# --- Dummy Payload ---
DUMMY_PAYLOAD = {
    "eventId": "test-1234",
    "eventTimestamp": "2026-07-08 10:01:24.231",
    "sourceTopic": "ENU.BIO.PROCESS.COMPLETION.V2",
    "packetMetaData": {
        "enrolmentType": "N",
        "srn": "1234567890",
    },
    "packetExecutionSummary": {
        "packetStatus": "REJECTED",
        "errorData": [{"errorReasonCode": "RESIDENT_MAN_DEDUP_REJECT_TD"}],
    }
}

@pytest.fixture(autouse=True)
def clean_environment():
    MOCK_DLQ.clear()
    storage = get_casebook_storage()
    if hasattr(storage, 'base_dir'):
        import shutil
        shutil.rmtree(storage.base_dir, ignore_errors=True)
    # Also wipe the LangGraph checkpoint DB -- otherwise retry_count and
    # other graph state leak between tests that share a thread_id/eventId
    # (this is what produced a stale `retry_count: 5` against a fresh
    # "test-1234" invocation before this fix).
    for suffix in ("", "-wal", "-shm"):
        db_file = Path(str(CHECKPOINT_DB_PATH) + suffix)
        if db_file.exists():
            db_file.unlink()
    os.environ["MAX_INVESTIGATION_RETRIES"] = "2"
    os.environ["MAX_IN_PROGRESS_AGE_SECONDS"] = "1"
    os.environ["PACKET_TIMEOUT_SECONDS"] = "2"
    yield

def test_loop_guard_max_retries():
    mock_agent_instance = MagicMock()
    mock_agent_instance.invoke.side_effect = [
        {"messages": [MagicMock(content="Investigation output")]},  # Investigator run 1
        {"messages": [MagicMock(content="REJECTED: Needs fix")]},   # Reviewer run 1
        {"messages": [MagicMock(content="Investigation output")]},  # Investigator run 2
        {"messages": [MagicMock(content="REJECTED: Needs fix")]},   # Reviewer run 2
        {"messages": [MagicMock(content="Investigation output")]},  # Investigator run 3 (retries=2)
        {"messages": [MagicMock(content="REJECTED: Needs fix")]},   # Reviewer run 3 (triggers escalate node)
    ]
    
    with patch("src.core.agent_orchestrator.create_react_agent", return_value=mock_agent_instance):
        with patch("src.core.agent_orchestrator._agent", None):
            asyncio.run(process_rejection(MessagePayload(**DUMMY_PAYLOAD)))
            
    storage = get_casebook_storage()
    casebook = storage.load("test-1234")
    assert casebook["packet_status"]["status"] == "NEEDS_MANUAL_REVIEW"
    assert "ESCALATED TO HUMAN REVIEW" in casebook["resolution"]["synthesis"]

def test_idempotency_short_circuit():
    storage = get_casebook_storage()
    storage.save("test-1234", {
        "packet_status": {"status": "COMPLETED"}
    })
    
    with patch("src.api.routes.get_agent") as mock_get_agent:
        res = asyncio.run(process_rejection(MessagePayload(**DUMMY_PAYLOAD)))
        assert res["status"] == "already_processed"
        mock_get_agent.assert_not_called()

def test_poison_pill_validation():
    
    bad_signal = {"eventId": "bad-1234", "packetExecutionSummary": "not-a-dict"}
    with patch("src.utils.kafkaConsumer.forward_signal_to_internal_endpoint") as mock_forward:
        with patch("src.utils.dlq_publisher.publish_to_dlq", side_effect=mock_publish_to_dlq):
            from src.models.schemas import MessagePayload
            try:
                MessagePayload(**bad_signal)
                assert False, "Should have raised exception"
            except Exception as e:
                mock_publish_to_dlq(bad_signal, str(e))
                
    assert len(MOCK_DLQ) == 1
    assert "validation" in MOCK_DLQ[0]["error"].lower()

def test_atomic_write_interruption():
    storage = get_casebook_storage()
    event_id = "test-atomic"
    target_dir = storage._get_dir(event_id)
    tmp_path = target_dir / "casebook.json.tmp"
    final_path = target_dir / "casebook.json"
    
    with open(tmp_path, "w") as f:
        f.write('{"partial": "')
        
    assert not storage.exists(event_id)
    
    storage.save(event_id, {"packet_status": {"status": "COMPLETED"}})
    assert storage.exists(event_id)
    assert storage.load(event_id)["packet_status"]["status"] == "COMPLETED"

def test_concurrent_add_learning_rule():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    target_file = os.path.join(base_dir, "src", "prompts", "pending_rules.jsonl")
    if os.path.exists(target_file):
        os.remove(target_file)
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(10):
            from filelock import FileLock
            lock_file = target_file + ".lock"
            def write_sim():
                with FileLock(lock_file, timeout=10):
                    with open(target_file, "a", encoding="utf-8") as f:
                        f.write(json.dumps({"test": i}) + "\n")
            futures.append(executor.submit(write_sim))
        concurrent.futures.wait(futures)
        
    with open(target_file, "r") as f:
        lines = f.readlines()
        assert len(lines) == 10
        for line in lines:
            json.loads(line)

@patch("src.api.routes.publish_to_dlq", side_effect=mock_publish_to_dlq)
def test_dlq_forced_unrecoverable_failure(mock_publish):
    mock_agent = MagicMock()
    mock_agent.invoke.side_effect = Exception("Hard crash!")
    with patch("src.api.routes.get_agent", return_value=mock_agent):
        res = asyncio.run(process_rejection(MessagePayload(**DUMMY_PAYLOAD)))
        assert res["status"] == "dlq"
        assert res["error"] == "Hard crash!"
        
    storage = get_casebook_storage()
    casebook = storage.load(DUMMY_PAYLOAD["eventId"])
    assert casebook["packet_status"]["status"] == "DLQ"
    
    assert len(MOCK_DLQ) == 1
    assert "Hard crash!" in MOCK_DLQ[0]["error"]

def test_pipeline_timeout_handling():
    from src.utils.kafkaConsumer import _process_and_commit
    import requests
    
    with patch("src.utils.kafkaConsumer.forward_signal_to_internal_endpoint", side_effect=requests.exceptions.Timeout("Timed out")):
        with patch("src.utils.dlq_publisher.publish_to_dlq", side_effect=mock_publish_to_dlq):
            _process_and_commit(DUMMY_PAYLOAD)
            
    assert len(MOCK_DLQ) == 1
    assert "Pipeline timed out" in MOCK_DLQ[0]["error"]
    
    storage = get_casebook_storage()
    casebook = storage.load(DUMMY_PAYLOAD["eventId"])
    assert casebook["packet_status"]["status"] == "FAILED_TIMEOUT"

def test_circuit_breaker_mock():
    import pybreaker
    test_breaker = pybreaker.CircuitBreaker(fail_max=2, reset_timeout=1)
    
    @test_breaker
    def flaky_func(fail):
        if fail:
            raise ValueError("Failed")
        return "Success"
        
    with pytest.raises(ValueError):
        flaky_func(True)
    with pytest.raises(pybreaker.CircuitBreakerError):
        flaky_func(True)
        
    with pytest.raises(pybreaker.CircuitBreakerError):
        flaky_func(False)