from src.utils.env import get_required_env
from src.utils.kafka_producer import brokers as _brokers
from src.utils.kafka_producer import get_producer
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

dlq_topic = get_required_env("KAFKA_DLQ_TOPIC", "rejected-packets-dlq")
# Kept as a module attribute: /ready and several tests read it.
brokers = _brokers()

#: How long to wait for the broker's answer once `flush()` has returned. The
#: record has been sent by then, so this only reads the outcome.
DLQ_CONFIRM_TIMEOUT_SECONDS = 10

# `get_producer` is re-exported rather than redefined. This module used to
# build its own KafkaProducer with settings identical to the two queue
# publishers', so every process held three connection pools to one cluster
# (see src/utils/kafka_producer.py).

def publish_to_dlq(payload, error_message: str):
    """Publish one message to the DLQ, raising if it did not get there.

    It used to log and return either way. Every consumer path that calls it
    is written to hold the message's offset when the publish raises -- a
    commit stall, recoverable on restart, rather than advancing past a
    message that then exists nowhere (kafkaConsumer.py, G1) -- and the tests
    of those paths raise from a stand-in publisher to prove it. The real one
    never raised, so a DLQ the broker refused, or a producer that could not
    be built, retired the offset anyway and the message was lost. The API,
    which must answer whether or not the DLQ is reachable, catches the error
    itself (routes._publish_to_dlq_best_effort).
    """
    producer = get_producer()
    if not producer:
        raise RuntimeError("DLQ producer is not available")

    dlq_message = {
        "original_payload": payload,
        "dlq_error": error_message
    }

    # The poison-pill path passes the raw (undecoded-JSON) string payload,
    # not a dict -- payload.get(...) would raise AttributeError here and get
    # logged as a publish failure even after producer.send()/flush()
    # succeeded (1.13). A DLT case has a case_id rather than an eventId.
    if isinstance(payload, dict):
        event_id = payload.get("eventId") or payload.get("case_id") or "unknown"
    else:
        event_id = "unknown"

    try:
        future = producer.send(dlq_topic, dlq_message)
        producer.flush()
        # `flush` waits for the send but does not report how it ended; a
        # record the broker refused only says so through its future.
        future.get(timeout=DLQ_CONFIRM_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error("Failed to publish to DLQ", event_id=event_id, error=str(e))
        raise
    logger.info("Published to DLQ", event_id=event_id, topic=dlq_topic)
