"""
Producer for the analysis queue (the fetch/analyze consumer split).

POST /fetch-logs publishes the original payload here once its logs are
fetched and persisted; the slow consumer subscribes to this topic and
forwards each message to POST /analyze-rejection. Mirrors dlq_publisher.py's
shape (cached producer, `acks="all"` so the last line of defence for handing
work to the slow consumer isn't a leader-only ack), but publishes the payload
unwrapped -- there is no error to envelope here, unlike a DLQ message.
"""
from src.utils.env import get_required_env
from src.utils.kafka_producer import brokers as _brokers
from src.utils.kafka_producer import get_producer
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

analysis_topic = get_required_env("PACKET_ANALYSIS_TOPIC_NAME", "packet-analysis-queue")
brokers = _brokers()

def publish_to_analysis_queue(payload: dict) -> bool:
    """Publish a fetched packet's payload for the slow consumer to pick up.

    Returns True on success. Raises on failure rather than swallowing it --
    unlike the DLQ publisher (which is itself the last line of defence and has
    nowhere further to escalate to), a failed publish here must surface as a
    non-2xx response from POST /fetch-logs so the fast consumer does not
    commit the original topic's offset and Kafka redelivers the message (see
    forward_signal_to_internal_endpoint / _process_and_commit in
    kafkaConsumer.py, which already handle that on any internal-endpoint
    failure).
    """
    producer = get_producer()
    if not producer:
        raise RuntimeError("Analysis-queue producer is not available")

    event_id = payload.get("eventId", "unknown") if isinstance(payload, dict) else "unknown"

    producer.send(analysis_topic, payload)
    producer.flush()
    logger.info("Published to analysis queue", event_id=event_id, topic=analysis_topic)
    return True


# ---------------------------------------------------------------------------
# DLT analysis queue (DLT_PLAN.md Phase 5)
# ---------------------------------------------------------------------------
# A second TOPIC, but not a second producer. The two queues carry different
# message shapes, are consumed by different roles, and a backlog on one must
# not stall the other -- all of which is true of the topics and none of which
# is a property of the client object. A KafkaProducer multiplexes topics, so
# the separate one this used to build bought nothing and cost a third
# connection pool, metadata fetcher and sender thread per process.


def get_dlt_producer():
    """Retained as a name so existing callers and tests keep working."""
    return get_producer()


def publish_to_dlt_analysis_queue(message: dict) -> bool:
    """Hand a fetched DLT case to the analysis consumer.

    Raises on failure for the same reason as `publish_to_analysis_queue`: the
    non-2xx response stops the DLT consumer committing its offset, so Kafka
    redelivers rather than the case being silently dropped between stages.
    """
    import os

    topic = os.environ.get("DLT_ANALYSIS_TOPIC_NAME", "dlt-analysis-queue")
    producer = get_dlt_producer()
    if not producer:
        raise RuntimeError("DLT analysis-queue producer is not available")

    case_id = message.get("case_id", "unknown") if isinstance(message, dict) else "unknown"
    producer.send(topic, message)
    producer.flush()
    logger.info("Published to DLT analysis queue", case_id=case_id, topic=topic)
    return True
