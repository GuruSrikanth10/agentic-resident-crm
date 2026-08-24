"""One Kafka producer for the whole process.

The DLQ, the analysis queue and the DLT analysis queue each built their own
`KafkaProducer` with byte-for-byte the same configuration, so every API and
consumer process held three connection pools, three metadata fetchers and
three sender threads to reach one cluster.

Separate *topics* are justified -- they carry different shapes, are consumed
by different roles, and a backlog on one must not stall the other. Separate
*producers* are not: a producer multiplexes topics, and per-topic isolation is
a property of the broker's partitions, not of the client object.

`acks="all"` and `retries=3` are kept from the DLQ producer, which had the
strictest settings of the three (they were in fact identical): the DLQ is the
last line of defence for a packet that failed everywhere else, and losing the
record because only the leader acked would defeat the point.
"""
import json
import threading

from src.utils.env import get_required_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

_producer = None
_producer_lock = threading.Lock()


def brokers() -> list:
    return [b.strip() for b in
            get_required_env("KAFKA_CONSUMER_BROKERS", "localhost:9092").split(",")
            if b.strip()]


def get_producer():
    """The process-wide producer, or None when it could not be built.

    Returning None rather than raising is the DLQ publisher's existing
    contract: it is the last escalation point and has nowhere further to go,
    so it logs and drops. The queue publishers turn None into a raise
    themselves, because for them a failed publish must surface as a non-2xx
    and stop the consumer committing its offset.
    """
    global _producer
    if _producer is not None:
        return _producer

    with _producer_lock:
        if _producer is not None:
            return _producer
        try:
            from kafka import KafkaProducer

            _producer = KafkaProducer(
                bootstrap_servers=brokers(),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
        except Exception as e:
            logger.error("Failed to initialize the Kafka producer", error=str(e))
        return _producer


def reset_producer() -> None:
    """Drop the cached producer. For tests and for config reloads."""
    global _producer
    with _producer_lock:
        _producer = None
