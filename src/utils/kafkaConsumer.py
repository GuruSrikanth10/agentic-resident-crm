import os
import json
import requests
import signal
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from kafka import ConsumerRebalanceListener, KafkaConsumer
from kafka.structs import OffsetAndMetadata
from pathlib import Path
from typing import Optional
from .env import get_required_env
from src.utils import message_adapters
from src.utils.atomic import replace_with_retry
from src.utils.logging_config import get_logger
from src.utils.paths import (
    CONSUMER_HEARTBEAT_PATH,
    DLT_ANALYSIS_HEARTBEAT_PATH,
    DLT_CONSUMER_HEARTBEAT_PATH,
    SLOW_CONSUMER_HEARTBEAT_PATH,
)

logger = get_logger(__name__)

kafkaConsumerBrokers = [
    b.strip() for b in get_required_env("KAFKA_CONSUMER_BROKERS", "localhost:9092").split(",") if b.strip()
]

# Which role this process plays -- set by the entrypoint script
# (src/fast_consumer.py or src/slow_consumer.py) before this module is
# imported, since each role runs as its own OS process/pod. Defaults to
# "fast", so every existing caller/test that never sets this env var (which
# is all of them -- CONSUMER_ROLE is new) gets today's exact behaviour,
# unchanged. Everything below this block -- OffsetTracker, the rebalance
# listener, heartbeat/health-server plumbing, _handle_one_message,
# _process_and_commit, consume_forever, the shutdown drain -- is identical
# for both roles and reused as-is: the analysis queue carries the exact same
# payload shape as the original topic (still packetStatus == "REJECTED",
# still validated by the same MessagePayload schema), so the poison-pill
# check and the terminal-casebook dedupe are exactly the right guards for
# both, unmodified.
CONSUMER_ROLE = os.environ.get("CONSUMER_ROLE", "fast").strip().lower()

if CONSUMER_ROLE == "dlt":
    # Reads the dead-letter topic and forwards to /fetch-dlt-logs. Bounded
    # I/O only, exactly like the fast consumer -- see DLT_PLAN.md 6.1.
    kafkaConsumerTopicName = get_required_env("DLT_CONSUMER_TOPIC_NAME", "packet-dlt")
    kafkaConsumerGroupId = get_required_env("DLT_CONSUMER_GROUP_ID", "dlt-analysis-group")
    kafkaConsumerInternalEndpoint = get_required_env(
        "DLT_CONSUMER_ENDPOINT", "http://localhost:8000/fetch-dlt-logs")
    kafkaConsumerInternalTimeoutSec = float(
        os.environ.get("DLT_CONSUMER_TIMEOUT_SECONDS", "90"))
    _HEARTBEAT_PATH = DLT_CONSUMER_HEARTBEAT_PATH
    _HEALTH_PORT_ENV = "DLT_HEALTH_PORT"
elif CONSUMER_ROLE == "dlt_analysis":
    # Reads the DLT analysis queue and forwards to /analyze-dlt. LLM-bound,
    # so it carries the long timeout and is the role to scale out.
    kafkaConsumerTopicName = get_required_env("DLT_ANALYSIS_TOPIC_NAME", "dlt-analysis-queue")
    kafkaConsumerGroupId = get_required_env("DLT_ANALYSIS_GROUP_ID", "dlt-analysis-slow-group")
    kafkaConsumerInternalEndpoint = get_required_env(
        "DLT_ANALYSIS_ENDPOINT", "http://localhost:8000/analyze-dlt")
    kafkaConsumerInternalTimeoutSec = float(
        os.environ.get("DLT_ANALYSIS_TIMEOUT_SECONDS", "300"))
    _HEARTBEAT_PATH = DLT_ANALYSIS_HEARTBEAT_PATH
    _HEALTH_PORT_ENV = "DLT_ANALYSIS_HEALTH_PORT"
elif CONSUMER_ROLE == "slow":
    # Reads the analysis queue that POST /fetch-logs publishes to, and
    # forwards each message to /analyze-rejection for the LLM-driven
    # investigation. This is the process meant to scale out to many pods to
    # absorb LLM latency, independently of the fast consumer.
    kafkaConsumerTopicName = get_required_env("SLOW_CONSUMER_TOPIC_NAME", "packet-analysis-queue")
    kafkaConsumerGroupId = get_required_env("SLOW_CONSUMER_GROUP_ID", "packet-analysis-group")
    kafkaConsumerInternalEndpoint = get_required_env("SLOW_CONSUMER_ENDPOINT", "http://localhost:8000/analyze-rejection")
    # Same env var and meaning as before the split: PACKET_TIMEOUT_SECONDS
    # has always been "how long the consumer's HTTP client waits for the
    # internal call to finish", and it's the slow (LLM-bound) consumer whose
    # call now actually needs that budget.
    kafkaConsumerInternalTimeoutSec = float(os.environ.get("PACKET_TIMEOUT_SECONDS", "300"))
    _HEARTBEAT_PATH = SLOW_CONSUMER_HEARTBEAT_PATH
    _HEALTH_PORT_ENV = "SLOW_CONSUMER_HEALTH_PORT"
else:
    # Reads the original rejections topic and forwards to /fetch-logs.
    # Bounded I/O only (no LLM call), so it gets its own, much shorter,
    # per-message timeout instead of the LLM-scaled PACKET_TIMEOUT_SECONDS.
    kafkaConsumerTopicName = get_required_env("KAFKA_CONSUMER_TOPIC_NAME", "rejections")
    kafkaConsumerGroupId = get_required_env("KAFKA_CONSUMER_GROUP_ID", "rejection-agents-group")
    # Same env var as before the split; only the default changed, from
    # /process-rejection to /fetch-logs.
    kafkaConsumerInternalEndpoint = get_required_env("KAFKA_CONSUMER_INTERNAL_ENDPOINT", "http://localhost:8000/fetch-logs")
    kafkaConsumerInternalTimeoutSec = float(os.environ.get("FAST_CONSUMER_TIMEOUT_SECONDS", "90"))
    _HEARTBEAT_PATH = CONSUMER_HEARTBEAT_PATH
    _HEALTH_PORT_ENV = "CONSUMER_HEALTH_PORT"

# Consumer will be instantiated lazily in consume_forever
consumer = None
MAX_CONCURRENT_INVESTIGATIONS = int(get_required_env("MAX_CONCURRENT_INVESTIGATIONS", "5"))
_worker_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_INVESTIGATIONS)
# Create a bounded queue by acquiring semaphore BEFORE reading from Kafka
_queue_semaphore = threading.Semaphore(MAX_CONCURRENT_INVESTIGATIONS)

# Set by SIGTERM/SIGINT so the poll loop can stop between messages rather
# than being killed mid-investigation (F12).
_shutdown = threading.Event()

# How long to wait for in-flight investigations to finish on shutdown. Kept
# under a typical Kubernetes terminationGracePeriodSeconds (30s) so the drain
# completes before SIGKILL rather than being cut off by it.
SHUTDOWN_DRAIN_SECONDS = float(os.environ.get("SHUTDOWN_DRAIN_SECONDS", "25"))

HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "5"))

# How old a heartbeat may be before this consumer calls itself dead. Six
# intervals of grace, so a single slow tick does not fail a liveness probe.
HEARTBEAT_STALE_SECONDS = float(
    os.environ.get("HEARTBEAT_STALE_SECONDS", str(HEARTBEAT_INTERVAL_SECONDS * 6))
)

# How many times to attempt the atomic heartbeat swap before giving up on a
# tick. See src/utils/atomic.py for why the swap needs retrying at all. The
# heartbeat keeps its own budget rather than the shared default: a tick comes
# round again in HEARTBEAT_INTERVAL_SECONDS anyway, so it should spend less
# time waiting than a write nobody repeats.
HEARTBEAT_WRITE_ATTEMPTS = max(1, int(os.environ.get("HEARTBEAT_WRITE_ATTEMPTS", "3")))
HEARTBEAT_WRITE_BACKOFF_SECONDS = float(
    os.environ.get("HEARTBEAT_WRITE_BACKOFF_SECONDS", "0.1")
)

# The stamp this process answers `is_healthy()` from while it owns the
# heartbeat ticker. Liveness used to be read back off the disk the ticker had
# just written, which made a locked, full, or read-only filesystem able to
# fail a perfectly healthy consumer's own probe -- the F9 restart-under-load
# failure arriving by a different route. None means no ticker runs here (the
# API, an operator CLI, a test), and those still read the file, which is the
# only signal available across processes.
_last_heartbeat = None


def _heartbeat_path() -> Path:
    return _HEARTBEAT_PATH


class OffsetTracker:
    """Per-partition offset bookkeeping that only ever commits a safe floor.

    Workers finish out of order. Committing each completed offset as it
    arrives means that when offsets 10, 11, 12 are dispatched together and 12
    finishes first, `commit(13)` runs while 10 and 11 are still in flight -- a
    crash then resumes the group at 13 and those two messages are never
    redelivered (F3).

    So completion is recorded, not committed. What gets committed is the
    low-water mark: the highest offset below which every dispatched message
    has completed. Anything at or above the lowest still-running offset waits.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._dispatched = defaultdict(set)
        self._completed = defaultdict(set)

    def dispatched(self, tp, offset: int):
        with self._lock:
            self._dispatched[tp].add(offset)

    def completed(self, tp, offset: int):
        with self._lock:
            self._completed[tp].add(offset)

    def committable_for(self, partitions) -> dict:
        """Take the safe floor for `partitions` only.

        Used on partition revocation, where whatever is safe must be committed
        before the partition moves to another member.
        """
        with self._lock:
            wanted = set(partitions)
            commits = {}
            for tp in list(self._completed):
                if tp not in wanted:
                    continue
                completed = self._completed[tp]
                if not completed:
                    continue

                still_running = self._dispatched[tp] - completed
                if still_running:
                    floor = min(still_running)
                    committable = {o for o in completed if o < floor}
                else:
                    committable = set(completed)

                if not committable:
                    continue

                commits[tp] = OffsetAndMetadata(max(committable) + 1, None, -1)
                self._dispatched[tp] -= committable
                self._completed[tp] -= committable

            return commits

    def forget(self, partitions):
        """Drop all state for partitions this consumer no longer owns.

        Without this, revoked partitions kept their entries forever and
        `_commit_ready_offsets` would happily commit offsets for a partition
        now owned by a different group member -- moving that member's
        committed position past messages it is still working on (G7).
        """
        with self._lock:
            for tp in partitions:
                self._dispatched.pop(tp, None)
                self._completed.pop(tp, None)

    def tracked_partitions(self) -> set:
        with self._lock:
            return set(self._dispatched) | set(self._completed)

    def abandoned(self, tp, offset: int):
        """Retire an offset that will never complete, without committing it.

        Registering an offset as dispatched and then never completing it used
        to strand it in `_dispatched` forever. `take_committable` takes
        `min(still_running)` as its floor, so that one offset became a
        permanent floor and NOTHING on the partition could ever be committed
        again -- the consumer kept processing packets correctly and silently
        stopped committing until it was restarted (G1). One `FileLock` timeout
        or S3 blip inside `_handle_one_message` was enough to trigger it.

        Retiring it here advances the floor past a message this process did
        not finish, so callers MUST hand the message to the DLQ in the same
        branch. That is the trade: a bounded, visible DLQ entry instead of an
        unbounded, invisible commit stall.
        """
        with self._lock:
            self._dispatched[tp].discard(offset)
            self._completed[tp].discard(offset)

    def in_flight(self) -> int:
        with self._lock:
            return sum(len(self._dispatched[tp] - self._completed[tp])
                       for tp in self._dispatched)

    def take_committable(self) -> dict:
        """Return {tp: OffsetAndMetadata} for the safe floor, and retire it.

        Offsets are only retired once they are being committed, so a failure
        to commit leaves them to be retried on the next drain.
        """
        with self._lock:
            commits = {}
            for tp, completed in list(self._completed.items()):
                if not completed:
                    continue

                still_running = self._dispatched[tp] - completed
                if still_running:
                    # Everything strictly below the lowest in-flight offset is
                    # safe; that offset itself is not.
                    floor = min(still_running)
                    committable = {o for o in completed if o < floor}
                else:
                    committable = set(completed)

                if not committable:
                    continue

                commits[tp] = OffsetAndMetadata(max(committable) + 1, None, -1)
                self._dispatched[tp] -= committable
                self._completed[tp] -= committable

            return commits


_offset_tracker = OffsetTracker()

# Per-role validation, skip rules, identity and request body (Phase 4). The
# rejection roles get today's logic moved verbatim; only `dlt`/`dlt_analysis`
# behave differently.
_adapter = message_adapters.for_role(CONSUMER_ROLE)


def forward_signal_to_internal_endpoint(signal_payload: dict):
    api_key = os.environ.get("PACKET_CRM_API_KEY", "dev-secret-key")
    headers = {"X-API-Key": api_key}
    response = requests.post(
        kafkaConsumerInternalEndpoint,
        json=signal_payload,
        headers=headers,
        proxies={"http": None, "https": None},
        timeout=kafkaConsumerInternalTimeoutSec,
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error forwarding signal to the internal endpoint",
                     event_id=signal_payload.get("eventId"),
                     status_code=getattr(e.response, "status_code", None))
        raise
    return response


def _record_completion(message_offsets):
    """Mark every offset in `message_offsets` complete on the tracker.

    `message_offsets` carries the *next* offset to commit (offset + 1), which
    is the shape consumer.commit() wants; the tracker works in message
    offsets, hence the -1.
    """
    if not message_offsets:
        return
    for tp, offset_and_metadata in message_offsets.items():
        _offset_tracker.completed(tp, offset_and_metadata.offset - 1)


def _record_abandonment(message_offsets):
    """Retire offsets this process will not finish, so the floor can advance.

    Always paired with a DLQ publish by the caller -- see
    `OffsetTracker.abandoned` for why the pairing is mandatory (G1).
    """
    if not message_offsets:
        return
    for tp, offset_and_metadata in message_offsets.items():
        _offset_tracker.abandoned(tp, offset_and_metadata.offset - 1)


def _dlq_and_abandon(payload, error_message: str, message_offsets: dict):
    """Hand a message to the DLQ and release its hold on the commit floor.

    Order matters: publish first, retire second. If the DLQ publish itself
    fails, the offset stays dispatched and the floor stays put -- a commit
    stall is bad, but it is better than advancing past a message that now
    exists nowhere at all.
    """
    from src.utils.dlq_publisher import publish_to_dlq

    publish_to_dlq(payload, error_message)
    _record_abandonment(message_offsets)


def _process_and_commit(signal_payload: dict, message_offsets: Optional[dict] = None):
    # Storage key and log identifier -- eventId for the rejection roles,
    # case_id for the DLT roles (Phase 4).
    event_id = _adapter.identity_of(signal_payload)
    success = False
    try:
        logger.info("Forwarding event to internal endpoint", event_id=event_id)
        response = forward_signal_to_internal_endpoint(signal_payload)
        logger.info("Forwarded event", event_id=event_id, status_code=response.status_code)
        success = True
    except requests.exceptions.Timeout:
        logger.error("Timeout forwarding Kafka message", event_id=event_id)
        # Every call in this handler can fail -- a FileLock timeout or S3 blip
        # inside save_terminal, an unreachable broker inside publish_to_dlq.
        # An exception raised HERE is not caught by the sibling `except
        # Exception` below (Python does not offer a raising except clause to
        # its siblings); it escapes _process_and_commit entirely, leaving
        # `success` False. The offset is then neither completed nor abandoned,
        # so it stays in `_dispatched` forever and pins the partition's commit
        # floor permanently -- the exact G1 stall the abandon/DLQ pairing
        # exists to prevent. Both steps are therefore guarded.
        #
        # Both casebook.json and status.json move to FAILED_TIMEOUT together.
        # Writing only casebook.json left the API's late-result guard -- which
        # reads status.json -- unable to see this verdict, so a slow agent run
        # could overwrite it with COMPLETED while the DLQ message stayed
        # queued (F4).
        try:
            # Routed through the adapter so a DLT timeout writes into
            # dlt_cases/, not beside the rejection casebooks.
            if event_id:
                _adapter.save_terminal(event_id, _adapter.timeout_casebook(signal_payload))
        except Exception as storage_error:
            # Best-effort: the DLQ publish below is what actually authorises the
            # commit, so a failed terminal write must not skip it.
            logger.error("Could not record FAILED_TIMEOUT; continuing to the DLQ",
                         event_id=event_id,
                         error=f"{type(storage_error).__name__}: {storage_error}")

        try:
            from src.utils.dlq_publisher import publish_to_dlq
            publish_to_dlq(signal_payload, "Pipeline timed out (PACKET_TIMEOUT_SECONDS exceeded)")
            success = True  # Successfully handled via DLQ, so we should commit
        except Exception as dlq_error:
            # Same trade as the generic branch below: the offset stays
            # dispatched and commits stall, rather than advancing past a
            # message that would then exist nowhere.
            logger.error("DLQ publish failed after a timeout; holding the offset uncommitted",
                         event_id=event_id,
                         error=f"{type(dlq_error).__name__}: {dlq_error}")
    except Exception as e:
        # Not committed -- but not left dangling either. Leaving the offset
        # dispatched-and-never-completed pinned the partition's commit floor
        # forever, so a single failed forward silently froze commits for every
        # later message on that partition (G1). Route it to the DLQ, where it
        # is visible and replayable, and release the floor.
        logger.exception("Error forwarding Kafka message; routing to the DLQ",
                         event_id=event_id)
        try:
            _dlq_and_abandon(
                signal_payload,
                f"Forwarding to the internal endpoint failed: {type(e).__name__}: {e}",
                message_offsets,
            )
        except Exception as dlq_error:
            # DLQ unreachable too. The offset intentionally stays dispatched:
            # stalling commits is the lesser evil against advancing past a
            # message that would then exist nowhere.
            logger.error("DLQ publish failed; holding the offset uncommitted",
                         event_id=event_id,
                         error=f"{type(dlq_error).__name__}: {dlq_error}")
    finally:
        if success:
            _record_completion(message_offsets)
            logger.info("Event completed; offset eligible for commit", event_id=event_id)
        _queue_semaphore.release()


def _handle_one_message(tp, msg):
    """Process a single polled Kafka message: validate, dedupe-check, enqueue.

    Caller must have already acquired `_queue_semaphore` for this message.

    No path commits directly any more. Every outcome -- dispatched, skipped,
    or poison-pilled -- is recorded on `_offset_tracker`, and the poll loop
    commits the resulting low-water mark (F3). A skip that committed
    immediately had the same batch-ordering hazard as an out-of-order
    completion: skipping offset 11 while offset 10 was still in flight would
    commit 12 and strand 10.
    """
    submitted = False
    signal_payload = None
    payload = None
    # Set once the poison-pill branch has handed this message to the DLQ, so
    # the outer handler does not publish it a second time when the FIRST
    # publish is what raised. The duplicate was harmless -- the first attempt
    # had failed -- but it logged two failures for one message and doubled the
    # work against a broker already known to be unreachable.
    dlq_attempted = False

    # Track only this message's offset, never the whole polled batch -- a bare
    # consumer.commit() advances past every message in the batch, including
    # ones not yet enqueued (0.10).
    message_offsets = {tp: OffsetAndMetadata(msg.offset + 1, None, -1)}
    _offset_tracker.dispatched(tp, msg.offset)

    try:
        # Validation, the skip rules, the identity and the request body are the
        # only per-role parts; everything above and below is shared (Phase 4).
        parsed = _adapter.parse(msg)
        payload = parsed.raw_text
        if parsed.is_poison:
            from src.utils.dlq_publisher import publish_to_dlq
            dlq_attempted = True
            publish_to_dlq(parsed.raw_text, parsed.error or "Message failed validation")
            _record_completion(message_offsets)
            return

        signal_payload = parsed.body or {}
        event_id = _adapter.identity_of(signal_payload)

        # May raise (FileLock timeout, S3 blip) -- the outer handler DLQs the
        # *parsed* payload and abandons the offset, which is why parsing and
        # skipping are separate calls.
        skip_reason = _adapter.should_skip(signal_payload)
        if skip_reason:
            logger.info("Skipping message", event_id=event_id, reason=skip_reason)
            _record_completion(message_offsets)
            return

        logger.info("Received message; enqueueing for processing",
                    event_id=event_id, role=CONSUMER_ROLE)

        _worker_pool.submit(_process_and_commit, signal_payload, message_offsets)
        submitted = True
    except Exception as e:
        payload_sample = payload[:500] if payload else 'Decode failed'
        logger.exception("Error processing Kafka message; routing to the DLQ",
                         payload_sample=payload_sample)
        # This branch is reached by anything the happy path can raise: a
        # FileLock timeout or S3 error inside storage.exists(), a
        # get_casebook_storage() failure, a decode fault. Every one of them
        # used to leave this offset dispatched forever and freeze the
        # partition's commit floor (G1).
        if dlq_attempted:
            # The poison-pill publish above is what raised. Republishing would
            # only fail the same way against the same broker.
            #
            # The offset is deliberately left dispatched: it is neither
            # completed nor abandoned, so the partition's commit floor stays
            # put. That is the same trade `_dlq_and_abandon` makes and the
            # module docstring states -- a commit stall is bad, but advancing
            # past a message that now exists nowhere at all is worse.
            logger.error("DLQ publish failed for a poison pill; "
                         "holding the offset uncommitted",
                         error=f"{type(e).__name__}: {e}")
        else:
            try:
                _dlq_and_abandon(
                    signal_payload if signal_payload is not None else payload_sample,
                    f"Consumer-side processing failed: {type(e).__name__}: {e}",
                    message_offsets,
                )
            except Exception as dlq_error:
                logger.error("DLQ publish failed; holding the offset uncommitted",
                             error=f"{type(dlq_error).__name__}: {dlq_error}")
    finally:
        if not submitted:
            _queue_semaphore.release()


class _RebalanceListener(ConsumerRebalanceListener):
    """Keep offset bookkeeping honest across group rebalances (G7).

    A rebalance happens whenever a member joins, leaves, or is evicted on
    max_poll_interval_ms -- so running the two consumers that Phase F's exit
    criteria call for makes it certain, and none of this had ever run.

    On revoke: commit what is safe for those partitions while we still own
    them, then drop their state. On assign: clear any stale state, because a
    partition coming back must not inherit offsets from a previous ownership
    period that another member has since advanced past.
    """

    def on_partitions_revoked(self, revoked):
        if not revoked:
            return
        logger.info("Partitions revoked; committing and dropping their offsets",
                    partitions=[str(tp) for tp in revoked])
        commits = _offset_tracker.committable_for(revoked)
        if commits and consumer is not None:
            try:
                consumer.commit(commits)
            except Exception as e:
                # The rebalance is already under way; the new owner will
                # redeliver from the last committed offset, which is correct.
                logger.warning("Could not commit on revoke; messages will redeliver",
                               error=f"{type(e).__name__}: {e}")
        _offset_tracker.forget(revoked)

    def on_partitions_assigned(self, assigned):
        if not assigned:
            return
        stale = _offset_tracker.tracked_partitions() & set(assigned)
        if stale:
            logger.warning("Dropping stale offset state for reassigned partitions",
                           partitions=[str(tp) for tp in stale])
            _offset_tracker.forget(stale)
        logger.info("Partitions assigned",
                    partitions=[str(tp) for tp in assigned])


def _commit_ready_offsets():
    """Commit whatever the tracker says is safe. Never raises."""
    commits = _offset_tracker.take_committable()
    if not commits:
        return
    try:
        consumer.commit(commits)
        logger.debug("Committed offsets", commits=str(commits))
    except Exception as e:
        logger.error("Error committing offset", error=str(e))


def _write_heartbeat(heartbeat_file: Path):
    """Atomically stamp the heartbeat file, retrying a transient lock.

    Atomic because /health reads this concurrently; a plain truncate-and-write
    can be read as an empty file mid-write. Retried because that atomicity is
    exactly what Windows refuses while another process holds the destination
    open -- see src/utils/atomic.py. `_shutdown` is passed as the abort so a
    drain does not wait out the backoff.

    The one caller of `replace_with_retry` that swallows the final failure,
    and deliberately: the ticker calls this in a bare loop, so an exception
    escaping here would kill the heartbeat thread for the life of the
    process. Dropping a tick costs nothing anyway -- it is only a stale
    reading for cross-process readers, since `is_healthy()` answers from
    memory in the process that ticks.
    """
    tmp_path = heartbeat_file.with_suffix(".tmp")
    try:
        tmp_path.write_text(str(time.time()), encoding="utf-8")
        replace_with_retry(tmp_path, heartbeat_file,
                           attempts=HEARTBEAT_WRITE_ATTEMPTS,
                           backoff=HEARTBEAT_WRITE_BACKOFF_SECONDS,
                           abort=_shutdown)
    except Exception as e:
        logger.warning("Failed to write heartbeat",
                       error=str(e), attempts=HEARTBEAT_WRITE_ATTEMPTS)


def _start_heartbeat_thread(heartbeat_file: Path) -> threading.Thread:
    """Stamp the heartbeat on its own ticker, independent of the poll loop.

    The heartbeat used to be written once per poll() cycle, but the poll loop
    blocks on `_queue_semaphore.acquire()` whenever all worker slots are busy
    -- for as long as an investigation takes, i.e. minutes. /health treats a
    heartbeat older than 30s as a dead consumer, so under exactly the
    sustained load the semaphore exists to manage, a liveness probe would
    restart a perfectly healthy pod and kill every in-flight packet (F9).
    """
    def _tick():
        global _last_heartbeat
        try:
            while not _shutdown.is_set():
                # In memory first, disk second. The stamp is what liveness is
                # actually answered from, so it must not be contingent on the
                # write below succeeding.
                _last_heartbeat = time.time()
                _write_heartbeat(heartbeat_file)
                _shutdown.wait(HEARTBEAT_INTERVAL_SECONDS)
        finally:
            # The ticker is gone, so memory is no longer authoritative for
            # this process and must stop being trusted as though it were.
            _last_heartbeat = None

    thread = threading.Thread(target=_tick, name="consumer-heartbeat", daemon=True)
    thread.start()
    return thread


def is_healthy() -> bool:
    """Has the heartbeat ticked recently enough to call this consumer alive?

    Read by the consumer's own /health listener. The API used to answer this
    by reading the heartbeat file off its local disk, which works only when
    both processes share a filesystem -- in the split-pod deployment Phase F
    exists to enable, `consumer_alive` was permanently false (G8).

    Answered from `_last_heartbeat` whenever this process runs the ticker, so
    the answer no longer depends on reading back a file this same process
    wrote seconds ago. Only callers with no ticker of their own fall through
    to the file.
    """
    if _shutdown.is_set():
        return False

    stamp = _last_heartbeat
    if stamp is None:
        # No ticker in this process, so the file is the only signal there is.
        try:
            stamp = float(_heartbeat_path().read_text(encoding="utf-8").strip())
        except Exception:
            return False

    return (time.time() - stamp) < HEARTBEAT_STALE_SECONDS


def _start_health_server():
    """Serve GET /health for this process, if a port is configured.

    Deliberately stdlib-only and tiny: the consumer is not an HTTP service and
    should not grow a web framework to answer one boolean. Disabled by default
    so single-host runs via start.py are unaffected.

    Reads CONSUMER_HEALTH_PORT (fast role) or SLOW_CONSUMER_HEALTH_PORT
    (slow role) via `_HEALTH_PORT_ENV`, so the two roles never collide on
    the same port when co-located under start.py.
    """
    port = os.environ.get(_HEALTH_PORT_ENV, "").strip()
    if not port:
        return None

    from http.server import BaseHTTPRequestHandler, HTTPServer

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.rstrip("/") not in ("/health", ""):
                self.send_response(404)
                self.end_headers()
                return

            healthy = is_healthy()
            body = json.dumps({
                "status": "up" if healthy else "down",
                "in_flight": _offset_tracker.in_flight(),
                "draining": _shutdown.is_set(),
            }).encode("utf-8")

            self.send_response(200 if healthy else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            # Probe traffic through structlog would drown the packet stream.
            pass

    try:
        server = HTTPServer(("0.0.0.0", int(port)), _Handler)
    except Exception as e:
        logger.error("Could not start the consumer health listener",
                     port=port, error=f"{type(e).__name__}: {e}")
        return None

    thread = threading.Thread(target=server.serve_forever,
                              name="consumer-health", daemon=True)
    thread.start()
    logger.info("Consumer health listener started", port=int(port))
    return server


def request_shutdown(signum=None, frame=None):
    """Signal handler: stop polling and let the drain run (F12)."""
    logger.info("Shutdown requested; draining in-flight work", signal=signum)
    _shutdown.set()


def _install_signal_handlers():
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, request_shutdown)
        except (ValueError, OSError):
            # Not the main thread (e.g. under a test runner) -- non-fatal.
            logger.debug("Could not install signal handler", signal=str(sig))


def _drain_and_commit():
    """Wait briefly for in-flight work, then commit whatever completed.

    Bounded: a packet mid-LLM-call may simply not finish in time. Its offset
    stays uncommitted, so Kafka redelivers it -- which is the correct
    at-least-once outcome, not a loss.
    """
    deadline = time.monotonic() + SHUTDOWN_DRAIN_SECONDS
    while time.monotonic() < deadline:
        remaining = _offset_tracker.in_flight()
        if remaining == 0:
            break
        logger.info("Draining in-flight investigations", remaining=remaining)
        time.sleep(0.5)

    _worker_pool.shutdown(wait=False, cancel_futures=True)

    if consumer is not None:
        _commit_ready_offsets()
        try:
            consumer.close()
        except Exception as e:
            logger.warning("Error closing consumer", error=str(e))

    logger.info("Consumer shutdown complete")


#: How long a single `_queue_semaphore.acquire()` attempt waits before the
#: loop re-checks `_shutdown`. Short enough that a SIGTERM is observed
#: promptly, long enough that a busy pool is not a spin.
SLOT_ACQUIRE_POLL_SECONDS = float(os.environ.get("SLOT_ACQUIRE_POLL_SECONDS", "1"))


def _acquire_slot() -> bool:
    """Take a worker slot, or return False because a shutdown began.

    The bare `_queue_semaphore.acquire()` this replaces took no timeout. With
    every worker busy -- exactly the sustained load the semaphore exists to
    manage -- the poll loop parked here indefinitely, and a thread already
    inside `acquire()` never observes `_shutdown`. It waited for a worker to
    finish, which for the slow consumer is up to PACKET_TIMEOUT_SECONDS (300s).

    SHUTDOWN_DRAIN_SECONDS is 25s and a typical terminationGracePeriodSeconds
    is 30s, so the pod was SIGKILLed long before `_drain_and_commit` ran: the
    F12 drain was inert under precisely the conditions it was written for.

    Declining to dispatch is safe. The offset is never committed, so Kafka
    redelivers the message to whoever takes the partition next.
    """
    while not _shutdown.is_set():
        if _queue_semaphore.acquire(timeout=SLOT_ACQUIRE_POLL_SECONDS):
            return True
    return False


def consume_forever():
    global consumer
    if consumer is None:
        # The topic is NOT passed to the constructor: subscribing separately
        # is what lets a ConsumerRebalanceListener be attached, which is how
        # revoked partitions get committed and dropped instead of lingering in
        # the tracker (G7).
        consumer = KafkaConsumer(
            group_id=kafkaConsumerGroupId,
            bootstrap_servers=kafkaConsumerBrokers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=int(os.environ.get("KAFKA_MAX_POLL_RECORDS", MAX_CONCURRENT_INVESTIGATIONS)),
            max_poll_interval_ms=int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", "900000")),
        )
        consumer.subscribe([kafkaConsumerTopicName],
                           listener=_RebalanceListener())

    _install_signal_handlers()

    logger.info("Kafka consumer started", topic=kafkaConsumerTopicName, brokers=kafkaConsumerBrokers)

    heartbeat_file = _heartbeat_path()
    os.makedirs(heartbeat_file.parent, exist_ok=True)
    _start_heartbeat_thread(heartbeat_file)
    _start_health_server()

    try:
        while not _shutdown.is_set():
            # Commit whatever became safe since the last cycle.
            _commit_ready_offsets()

            records = consumer.poll(timeout_ms=5000)

            if not records:
                continue

            for tp, messages in records.items():
                if _shutdown.is_set():
                    break
                for msg in messages:
                    if _shutdown.is_set():
                        # Stop taking new work; anything not dispatched simply
                        # isn't committed and will be redelivered.
                        break
                    # Block if the pool is full BEFORE parsing (backpressure).
                    if not _acquire_slot():
                        break
                    _handle_one_message(tp, msg)
    finally:
        _drain_and_commit()
