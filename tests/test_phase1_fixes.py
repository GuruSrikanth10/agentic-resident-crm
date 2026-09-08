import json
import shutil
import asyncio
from unittest.mock import MagicMock, patch

import pytest

from src.models.schemas import MessagePayload
from src.storage.factory import get_casebook_storage
from src.api.routes import process_rejection

from test_resilience import DUMMY_PAYLOAD  # reuse the shared dummy payload


def _payload_with_event_id(event_id: str) -> dict:
    payload = dict(DUMMY_PAYLOAD)
    payload["eventId"] = event_id
    return payload


def _cleanup_casebook(event_id: str):
    storage = get_casebook_storage()
    if hasattr(storage, "base_dir"):
        shutil.rmtree(storage.base_dir / f"casebook_{event_id}", ignore_errors=True)


# ======================================================================
# 1.1 -- rule lookup cache must never cache a failure result.
# ======================================================================

def test_rule_lookup_cache_skips_failures_but_caches_success():
    from src.tools import tool_registry as tr

    tr._rule_cache.clear()
    call_count = {"n": 0}

    def fake_impl(reason_code):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return "Failed to query live DB: connection refused"
        return f"OK-{call_count['n']}"

    with patch.object(tr, "_lookup_rule_by_reason_code_impl", side_effect=fake_impl):
        result1 = tr.lookup_rule_by_reason_code.invoke("CODE_X")
        assert result1.startswith("Failed to query live DB")
        assert call_count["n"] == 1

        # Second call succeeds -- this result should be cached.
        result2 = tr.lookup_rule_by_reason_code.invoke("CODE_X")
        assert result2 == "OK-2"
        assert call_count["n"] == 2

        # Third call is served from cache -- impl must not run again.
        result3 = tr.lookup_rule_by_reason_code.invoke("CODE_X")
        assert result3 == "OK-2"
        assert call_count["n"] == 2


# ======================================================================
# 1.3 -- config validation checks the key the *selected* provider needs.
# ======================================================================

def test_config_validator_requires_mistral_key_when_mistral_selected(monkeypatch):
    from src.utils.config_validator import _collect_llm_key_errors

    monkeypatch.setenv("MOCK_LLM_WITH_MISTRAL", "true")
    monkeypatch.delenv("USE_HF", raising=False)
    monkeypatch.delenv("LLM_API_KEY_COMPLEX", raising=False)

    errors = _collect_llm_key_errors()
    assert any("LLM_API_KEY_COMPLEX" in e for e in errors)


def test_config_validator_passes_without_openai_key(monkeypatch):
    from src.utils.config_validator import _collect_llm_key_errors

    monkeypatch.delenv("USE_HF", raising=False)
    monkeypatch.delenv("MOCK_LLM_WITH_MISTRAL", raising=False)
    monkeypatch.setenv("LLM_API_KEY_COMPLEX", "some-real-key")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    assert _collect_llm_key_errors() == []


# ======================================================================
# 1.4 -- unknown LLM tiers must fail fast, not silently fall back.
# ======================================================================

def test_get_llm_rejects_unknown_tier():
    from src.utils.llm_utils import get_llm

    with pytest.raises(ValueError):
        get_llm("medium")


# ======================================================================
# 1.5 -- the DLQ path must move status.json to a terminal status too.
# ======================================================================

def test_dlq_path_updates_status_json():
    event_id = "phase1-dlq-status"
    mock_agent = MagicMock()
    mock_agent.get_state.return_value = None
    mock_agent.invoke.side_effect = Exception("boom")

    try:
        with patch("src.api.routes.get_agent", return_value=mock_agent), \
             patch("src.api.routes.publish_to_dlq"):
            res = asyncio.run(process_rejection(MessagePayload(**_payload_with_event_id(event_id))))

        assert res["status"] == "dlq"
        storage = get_casebook_storage()
        status_doc = storage.load(event_id, filename="status.json")
        assert status_doc is not None
        assert status_doc["packet_status"]["status"] == "DLQ"
    finally:
        _cleanup_casebook(event_id)


# ======================================================================
# 1.6 -- fetch_elastic_logs must return None (not an error-prefixed
# string) on failure.
# ======================================================================

def test_fetch_elastic_logs_returns_none_on_failure():
    from src.tools import tool_registry as tr

    def boom(event_id):
        raise RuntimeError("pipeline exploded")

    with patch("src.log_pipeline.pipeline.reduce_logs", side_effect=boom):
        result = tr.fetch_elastic_logs.invoke("evt-boom")

    assert result is None


# ======================================================================
# 1.9 / 1.10 -- ES client defaults, request cap, and short-page exit.
# ======================================================================

def _make_hit(i):
    return {
        "_source": {
            "@timestamp": f"2026-01-01T00:00:{i:02d}",
            "level": "INFO",
            "message": f"msg-{i}",
            "application_name": "svc",
        },
        "sort": [i],
    }


def test_fetch_logs_verify_certs_and_timeout_defaults(monkeypatch):
    import src.log_pipeline.fetcher as fetcher_module

    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.delenv("ES_VERIFY_CERTS", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")

    mock_es_instance = MagicMock()
    mock_es_instance.search.return_value = {"hits": {"hits": []}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_es_instance) as MockES:
        fetcher_module.fetch_logs("evt-certs")

    _, kwargs = MockES.call_args
    assert kwargs["verify_certs"] is True
    assert "request_timeout" in kwargs


def test_fetch_logs_caps_at_log_max_documents(monkeypatch):
    import src.log_pipeline.fetcher as fetcher_module

    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")
    monkeypatch.setattr(fetcher_module, "LOG_MAX_DOCUMENTS", 1000)

    full_page = [_make_hit(i) for i in range(500)]
    mock_es_instance = MagicMock()
    mock_es_instance.search.return_value = {"hits": {"hits": full_page}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_es_instance):
        logs = fetcher_module.fetch_logs("evt-cap")

    assert len(logs) == 1000
    assert mock_es_instance.search.call_count == 2


def test_fetch_logs_stops_on_short_page(monkeypatch):
    import src.log_pipeline.fetcher as fetcher_module

    monkeypatch.delenv("ES_MOCK_FILE", raising=False)
    monkeypatch.setenv("ES_HOST", "https://fake-es:9200")

    short_page = [_make_hit(i) for i in range(10)]
    mock_es_instance = MagicMock()
    mock_es_instance.search.return_value = {"hits": {"hits": short_page}}

    with patch("elasticsearch.Elasticsearch", return_value=mock_es_instance):
        logs = fetcher_module.fetch_logs("evt-short")

    assert len(logs) == 10
    # No wasted round-trip requesting a follow-up (empty) page.
    assert mock_es_instance.search.call_count == 1


# ======================================================================
# 1.11 -- branch_on_error must cap the trailing window too.
# ======================================================================

def test_branch_on_error_caps_trailing_window(monkeypatch):
    from src.log_pipeline import reducer as reducer_module

    monkeypatch.setattr(reducer_module, "ERROR_CONTEXT_LINES", 2)
    monkeypatch.setattr(reducer_module, "ERROR_TRAILING_LINES", 3)

    logs = [{"level": "INFO", "message": f"info-{i}"} for i in range(5)]
    logs.append({"level": "ERROR", "message": "boom"})
    logs += [{"level": "INFO", "message": f"after-{i}"} for i in range(50)]

    result = reducer_module.branch_on_error(logs)

    assert result["has_error"] is True
    # context_start = max(0, 5-2) = 3; context_end = min(len, 5+1+3) = 9
    assert len(result["payload"]) == 6


# ======================================================================
# 1.12 -- S3 upload failures must not silently discard the evidence.
# ======================================================================

def test_upload_logs_to_s3_returns_none_when_unconfigured(monkeypatch):
    monkeypatch.delenv("S3_LOGS_BUCKET", raising=False)
    from src.utils.s3_uploader import upload_logs_to_s3

    assert upload_logs_to_s3("evt", "some logs") is None


def test_routes_falls_back_to_truncated_logs_when_s3_unset(monkeypatch):
    monkeypatch.delenv("S3_LOGS_BUCKET", raising=False)
    event_id = "phase1-s3-fallback"
    big_logs = "X" * 6000

    mock_agent = MagicMock()
    mock_agent.get_state.return_value = None
    mock_agent.invoke.return_value = {
        # Valid enum values: this test is about log truncation, and an invalid
        # action here would route the packet down the FAILED_SYNTHESIS_PARSE
        # path instead, testing something else entirely (G5).
        "synthesis": json.dumps({"synthesis": "ok", "action": "REPLAY",
                                 "resident_action": "NEW_PACKET"}),
        "logs": big_logs,
    }

    try:
        with patch("src.api.routes.get_agent", return_value=mock_agent):
            res = asyncio.run(process_rejection(MessagePayload(**_payload_with_event_id(event_id))))

        assert res["status"] == "processed"
        storage = get_casebook_storage()
        casebook = storage.load(event_id)
        # `rejection_logs` is a {"path", "gaps"} record, not a bare string:
        # the log text itself lives in the raw_logs.txt artifact, and only its
        # locator is embedded here. The old assertions read it as a string and
        # raised AttributeError once that shape changed.
        logs_field = casebook["packet_status"]["rejection_data"]["rejection_logs"]
        assert logs_field is not None
        path = logs_field["path"]
        # With no bucket configured there is no resolvable URL to record, so
        # the casebook says so plainly rather than persisting a fake one (1.12).
        assert path == "Logs persisted to local storage (S3 unavailable)."
        assert not path.startswith("s3://")
        # No evidence gaps were injected, so none are claimed.
        assert logs_field["gaps"] is None
    finally:
        _cleanup_casebook(event_id)


# ======================================================================
# 1.13 -- dlq_publisher must handle a non-dict payload without logging a
# false "publish failed" after send()/flush() actually succeeded.
# ======================================================================

def test_publish_to_dlq_handles_string_payload():
    from src.utils import dlq_publisher as dlq_module

    mock_producer = MagicMock()
    mock_logger = MagicMock()

    with patch.object(dlq_module, "get_producer", return_value=mock_producer), \
         patch.object(dlq_module, "logger", mock_logger):
        dlq_module.publish_to_dlq("not-a-dict-payload", "Structural validation failed: xyz")

    mock_producer.send.assert_called_once()
    mock_producer.flush.assert_called_once()
    mock_logger.error.assert_not_called()
    mock_logger.info.assert_called_once()


# ======================================================================
# 1.17 -- load()/exists() must never create a casebook directory as a
# side effect of checking whether it exists.
# ======================================================================

def test_storage_load_does_not_create_directory(tmp_path):
    from src.storage.local import LocalFilesystemCasebookStorage

    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))

    assert storage.load("never-touched") is None
    assert not (tmp_path / "casebook_never-touched").exists()

    assert storage.exists("never-touched") is False
    assert not (tmp_path / "casebook_never-touched").exists()
