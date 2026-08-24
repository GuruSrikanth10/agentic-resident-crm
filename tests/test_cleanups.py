"""Phase 6 of REMEDIATION_PLAN_2026_08_21.md -- correctness and observability
cleanups. Small, independent, and each one previously invisible.
"""
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.log_pipeline.types import TimeWindow


# ======================================================================
# 6.1 -- the escalation path reaches the retry histogram
# ======================================================================

def test_escalation_records_the_retry_count(monkeypatch):
    """escalate_node is reached exactly when retry_count hits the maximum --
    the packets with the MOST Reviewer rejections. Observing only in
    synthesis_node gave the histogram a hard ceiling at max_retries - 1 and
    hid the tail it exists to measure.

    Driven end to end: a Reviewer that never approves, run until the graph
    gives up, with the histogram watched throughout.
    """
    from src.core import agent_orchestrator

    monkeypatch.setenv("MAX_INVESTIGATION_RETRIES", "2")
    monkeypatch.setenv("RUNBOOK_MODE", "off")
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "false")

    observed = []
    histogram = MagicMock()
    histogram.observe.side_effect = observed.append

    rejecting_agent = MagicMock()
    rejecting_agent.invoke.return_value = {
        "messages": [MagicMock(content="REJECTED: not grounded in the logs")]
    }

    saved = agent_orchestrator._agent
    try:
        agent_orchestrator._agent = None
        with patch.object(agent_orchestrator.metrics, "INVESTIGATOR_RETRIES", histogram), \
             patch.object(agent_orchestrator, "create_react_agent",
                          return_value=rejecting_agent), \
             patch.object(agent_orchestrator, "get_checkpointer", return_value=None), \
             patch.object(agent_orchestrator, "fetch_and_persist_logs",
                          return_value="Log fetching disabled."):
            graph = agent_orchestrator.get_agent()
            result = graph.invoke(
                {"payload": {"eventId": "evt-escalate"}, "retry_count": 0})
    finally:
        agent_orchestrator._agent = saved

    # It escalated rather than synthesising...
    assert "ESCALATED" in result["synthesis"]
    # ...and the retry count still reached the histogram.
    assert observed == [2], f"escalation did not observe the retry count: {observed}"


def test_escalate_node_observes_before_returning():
    """The observation must happen on the escalation path itself, not only
    where a successful synthesis records it."""
    import inspect

    from src.core import agent_orchestrator

    source = inspect.getsource(agent_orchestrator.get_agent.__globals__["_build_agent"])
    escalate = source[source.index("def escalate_node"):source.index("def synthesis_node")]
    assert "INVESTIGATOR_RETRIES.observe" in escalate


# ======================================================================
# 6.2 -- ceilings_applied is order-independent
# ======================================================================

def test_every_triggered_ceiling_is_named_regardless_of_order():
    """Naming only the tightest ceiling would make this list depend on
    evaluation order: Class B (0.3) is checked before UNVERIFIABLE (0.5), so
    an unverifiable Class B case would stop reporting it was unverifiable."""
    from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy

    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y",
                   action="NEEDS_MANUAL_REVIEW", confidence=0.99),
        failure_class="B", corroboration="UNVERIFIABLE", registry_hit=False)

    assert finding.confidence <= 0.3, "the tightest ceiling still wins the number"
    assert {"class_b", "unverifiable"} <= set(finding.ceilings_applied)


def test_a_ceiling_that_was_not_triggered_is_not_named():
    from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy

    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y",
                   action="DATA_FIX_REQUIRED", confidence=0.9),
        failure_class="A", corroboration="CORROBORATED", registry_hit=True)

    assert "class_b" not in finding.ceilings_applied
    assert "unverifiable" not in finding.ceilings_applied


# ======================================================================
# 6.3 -- the DLT window's trailing bound is actually applied
# ======================================================================

def test_time_window_excludes_records_past_the_bound():
    until = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)
    window = TimeWindow(hours=2, until=until)

    assert window.excludes("2026-08-21T12:00:01Z") is True
    assert window.excludes("2026-08-21T11:59:59Z") is False


def test_a_window_with_no_bound_excludes_nothing():
    """Every pre-existing caller passes no bound and must be unaffected."""
    window = TimeWindow(hours=2)
    assert window.excludes("2099-01-01T00:00:00Z") is False


@pytest.mark.parametrize("timestamp", ["", None, "not-a-timestamp", "garbage"])
def test_an_unreadable_timestamp_is_never_excluded(timestamp):
    """Dropping a line because we could not read its clock would be discarding
    evidence on a parse failure -- the opposite of this pipeline's purpose."""
    window = TimeWindow(hours=2, until=datetime(2020, 1, 1, tzinfo=timezone.utc))
    assert window.excludes(timestamp) is False


def test_a_naive_timestamp_is_treated_as_utc():
    window = TimeWindow(hours=2,
                        until=datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc))
    assert window.excludes("2026-08-21T13:00:00") is True


def test_log_window_carries_its_trailing_bound_into_the_time_window():
    """`end_ms` was computed, documented as "applied during filtering", and
    then used nowhere at all."""
    from src.dlt.window import LogWindow

    anchor = 1_755_000_000_000
    lw = LogWindow(anchor_ms=anchor, start_ms=anchor - 300_000,
                   end_ms=anchor + 120_000, too_old=False,
                   anchor_is_fallback=False, age_seconds=100.0)

    tw = lw.to_time_window(now_ms=anchor + 3_600_000)

    assert tw.until is not None
    assert int(tw.until.timestamp() * 1000) == lw.end_ms
    assert tw.excludes(
        datetime.fromtimestamp((anchor + 200_000) / 1000, tz=timezone.utc).isoformat()
    ) is True


def test_read_all_trims_records_past_the_bound(monkeypatch):
    from src.log_pipeline.sources.k8s import retrieval
    from src.log_pipeline.sources.k8s.discovery import PodTarget
    from src.log_pipeline.sources.k8s.filtering import KeepAllSelector

    base = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)

    def fake_read_instance(target, window, previous, selector):
        records = [
            {"timestamp": (base + timedelta(seconds=offset)).isoformat(),
             "level": "INFO", "message": f"line {offset}", "app_name": "c"}
            for offset in (-60, 0, 60, 600)
        ]
        return records, retrieval.ParseStats(total=4, level_parsed=4), 10, False, None

    monkeypatch.setattr(retrieval, "_read_instance", fake_read_instance)

    outcome = retrieval.read_all(
        [PodTarget(namespace="ns", pod_name="p", container="c")],
        TimeWindow(hours=2, until=base + timedelta(seconds=120)),
        selector_factory=lambda: KeepAllSelector(),
    )

    kept = [r["message"] for r in outcome.records]
    assert kept == ["line -60", "line 0", "line 60"]


# ======================================================================
# 6.4 -- call_with_retry's policy kwargs cannot collide with API kwargs
# ======================================================================

def test_api_kwargs_named_like_the_policy_reach_the_function():
    """Call sites splat a caller-built dict of Kubernetes API kwargs in here.
    A plain `max_attempts` among them would silently rebind the retry policy."""
    from src.log_pipeline.sources.k8s.retry import call_with_retry

    received = {}

    def api_call(**kwargs):
        received.update(kwargs)
        return "ok"

    assert call_with_retry(api_call, max_attempts="not-a-policy",
                           sleep="also-not") == "ok"
    assert received == {"max_attempts": "not-a-policy", "sleep": "also-not"}


def test_the_policy_is_still_configurable_under_its_own_names():
    from src.log_pipeline.sources.k8s.retry import call_with_retry

    attempts = []

    def always_503():
        attempts.append(1)
        error = Exception("busy")
        error.status = 503
        raise error

    with pytest.raises(Exception):
        call_with_retry(always_503, _max_attempts=4, _sleep=lambda _s: None)

    assert len(attempts) == 4


# ======================================================================
# 6.6 -- one Kafka producer per process, not three
# ======================================================================

def test_all_three_publishers_share_one_producer():
    """Each built its own with byte-for-byte identical settings, so every
    process held three connection pools to one cluster."""
    from src.utils import analysis_queue_publisher, dlq_publisher, kafka_producer

    sentinel = object()
    with patch.object(kafka_producer, "_producer", sentinel):
        assert dlq_publisher.get_producer() is sentinel
        assert analysis_queue_publisher.get_producer() is sentinel
        assert analysis_queue_publisher.get_dlt_producer() is sentinel


def test_the_producer_is_built_once_under_concurrency():
    from src.utils import kafka_producer

    builds = []

    def counting_producer(**kwargs):
        builds.append(1)
        return MagicMock(name="producer")

    kafka_producer.reset_producer()
    barrier = threading.Barrier(12)

    def run():
        barrier.wait()
        kafka_producer.get_producer()

    try:
        with patch("kafka.KafkaProducer", counting_producer):
            workers = [threading.Thread(target=run) for _ in range(12)]
            for w in workers:
                w.start()
            for w in workers:
                w.join()
        assert len(builds) == 1
    finally:
        kafka_producer.reset_producer()


def test_the_topics_stay_separate():
    """Sharing the client must not have merged the queues."""
    from src.utils import analysis_queue_publisher, dlq_publisher

    assert dlq_publisher.dlq_topic != analysis_queue_publisher.analysis_topic


# ======================================================================
# 6.7 -- a poison pill is not published to the DLQ twice
# ======================================================================

def test_a_failed_poison_pill_publish_is_not_retried_in_the_outer_handler():
    """The first publish is what raised; republishing only fails again against
    the same broker and logs two failures for one message."""
    from src.utils import kafkaConsumer

    publishes = []

    def exploding_publish(payload, error_message):
        publishes.append(payload)
        raise RuntimeError("broker unreachable")

    message = MagicMock()
    message.offset = 7
    message.value = b"{not json"

    adapter = MagicMock()
    adapter.parse.return_value = kafkaConsumer.message_adapters.ParseResult(
        raw_text="{not json", error="Structural validation failed")

    with patch.object(kafkaConsumer, "_adapter", adapter), \
         patch("src.utils.dlq_publisher.publish_to_dlq", exploding_publish):
        kafkaConsumer._handle_one_message("tp", message)

    assert len(publishes) == 1, "the DLQ was published to twice for one message"


def test_a_failed_poison_pill_publish_holds_the_offset():
    """Never advance past a message that now exists nowhere: the offset stays
    dispatched so the commit floor holds, which is the trade the module
    docstring states."""
    from src.utils import kafkaConsumer

    def exploding_publish(payload, error_message):
        raise RuntimeError("broker unreachable")

    message = MagicMock()
    message.offset = 11
    message.value = b"{not json"

    adapter = MagicMock()
    adapter.parse.return_value = kafkaConsumer.message_adapters.ParseResult(
        raw_text="{not json", error="Structural validation failed")

    tracker = kafkaConsumer.OffsetTracker()
    with patch.object(kafkaConsumer, "_adapter", adapter), \
         patch.object(kafkaConsumer, "_offset_tracker", tracker), \
         patch("src.utils.dlq_publisher.publish_to_dlq", exploding_publish):
        kafkaConsumer._handle_one_message("tp", message)

    # Neither completed nor abandoned: nothing is committable.
    assert tracker.take_committable() == {}
    assert tracker.in_flight() == 1


# ======================================================================
# 6.5 -- token accounting was flagged for confirmation. It is correct.
# ======================================================================

def test_no_node_feeds_graph_state_messages_back_into_an_invoke():
    """The plan flagged `record_llm_usage` as possibly double-counting, since
    it sums usage across every message in a response and `synthesis_node`
    returns `res["messages"]` into graph state.

    Confirmed not to be a defect: every node builds a fresh two-message list
    for its own invoke, and nothing ever reads `messages` back out of state.
    The stored copy is inert. Asserting it so a future edit that DOES feed it
    back has to notice this.
    """
    import inspect

    from src.core import agent_orchestrator
    from src.dlt import orchestrator as dlt_orchestrator

    for module in (agent_orchestrator, dlt_orchestrator):
        source = inspect.getsource(module)
        assert 'state.get("messages")' not in source
        assert 'state["messages"]' not in source


def test_record_llm_usage_sums_one_invocations_messages():
    """Summing across the messages of ONE response is correct: a react loop
    can make several model calls before it returns."""
    from src.utils import metrics

    counter = MagicMock()
    labelled = MagicMock()
    counter.labels.return_value = labelled

    def message(inp, out):
        m = MagicMock()
        m.usage_metadata = {"input_tokens": inp, "output_tokens": out}
        return m

    with patch.object(metrics, "LLM_TOKENS", counter):
        metrics.record_llm_usage("investigator",
                                 {"messages": [message(10, 5), message(20, 7)]})

    assert sorted(c.args[0] for c in labelled.inc.call_args_list) == [5, 7, 10, 20]
