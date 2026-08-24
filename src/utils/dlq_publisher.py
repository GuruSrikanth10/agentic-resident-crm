from src.utils.env import get_required_env
from src.utils.kafka_producer import brokers as _brokers
from src.utils.kafka_producer import get_producer
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

dlq_topic = get_required_env("KAFKA_DLQ_TOPIC", "rejected-packets-dlq")
# Kept as a module attribute: /ready and several tests read it.
brokers = _brokers()

# `get_producer` is re-exported rather than redefined. This module used to
# build its own KafkaProducer with settings identical to the two queue
# publishers', so every process held three connection pools to one cluster
# (see src/utils/kafka_producer.py).

def publish_to_dlq(payload, error_message: str):
    producer = get_producer()
    if not producer:
        logger.error("DLQ producer not available; dropping DLQ message.")
        return

    dlq_message = {
        "original_payload": payload,
        "dlq_error": error_message
    }

    # The poison-pill path passes the raw (undecoded-JSON) string payload,
    # not a dict -- payload.get(...) would raise AttributeError here and get
    # logged as a publish failure even after producer.send()/flush()
    # succeeded (1.13).
    event_id = payload.get("eventId", "unknown") if isinstance(payload, dict) else "unknown"

    try:
        producer.send(dlq_topic, dlq_message)
        producer.flush()
        logger.info("Published to DLQ", event_id=event_id, topic=dlq_topic)
    except Exception as e:
        logger.error("Failed to publish to DLQ", event_id=event_id, error=str(e))
