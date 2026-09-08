from typing import Protocol, Optional

#: Casebook schema version written by the storage layer.
#: 1.1 -> 1.2 adds the optional `resolution_outcome` block (section 4.1), which
#: records whether a resolution was actually correct. It is additive: a 1.1
#: casebook is a valid 1.2 casebook with no outcome recorded yet.
CASEBOOK_SCHEMA_VERSION = "1.2"

#: Verdicts an operator can attach to a resolution.
OUTCOME_VERDICTS = ("CORRECT", "INCORRECT", "PARTIAL")

#: Statuses that end a packet's lifecycle. Defined here rather than duplicated
#: across routes.py, kafkaConsumer.py and local.py, which each carried their
#: own copy and had already drifted (F4).
TERMINAL_STATUSES = (
    "COMPLETED",
    "REJECTED",
    "NEEDS_MANUAL_REVIEW",
    "FAILED_PERMANENT",
    "DLQ",
    "FAILED_TIMEOUT",
    # The agents produced output that does not satisfy the Synthesis contract,
    # even after a repair attempt. Named distinctly so it is never confused
    # with a packet the agents genuinely could not classify -- the two used to
    # be indistinguishable, both surfacing as action: null (G5).
    "FAILED_SYNTHESIS_PARSE",
    # The API shut down mid-investigation. Terminal so the packet is not
    # stranded at IN_PROGRESS for MAX_IN_PROGRESS_AGE_SECONDS; its Kafka
    # offset was never committed, so it is redelivered regardless (G9).
    "FAILED_SHUTDOWN",
)

#: Terminal statuses recorded by an actor *other* than the API's own success
#: path -- a late-finishing agent run must never overwrite one of these with a
#: "successful" result.
PROTECTED_TERMINAL_STATUSES = ("FAILED_TIMEOUT", "DLQ")

#: Non-terminal status written by POST /fetch-logs once it has persisted the
#: `fetched_logs.txt` artifact and published the payload onto the analysis
#: queue. Deliberately absent from TERMINAL_STATUSES: it must never satisfy a
#: terminal_only dedupe check, and it must fall through the `status ==
#: "IN_PROGRESS"` branch in routes.py's investigation dedupe so
#: /analyze-rejection still writes its own fresh IN_PROGRESS stub and invokes
#: the agent normally. It exists purely for observability -- distinguishing
#: "never seen" from "logs fetched, awaiting analysis" from "analysis running".
LOGS_FETCHED_STATUS = "LOGS_FETCHED"


class CasebookStorage(Protocol):
    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        """Save the casebook persistently."""
        ...

    def save_terminal(self, event_id: str, casebook: dict) -> None:
        """Record a terminal outcome in casebook.json AND status.json together.

        The two files must reach a terminal status as one operation. The
        consumer's timeout handler used to write only casebook.json while the
        API's late-result guard read only status.json, so a slow agent run
        could overwrite a FAILED_TIMEOUT casebook with COMPLETED while its DLQ
        message stayed queued (F4).
        """
        ...

    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        """Load the casebook for the given event ID."""
        ...

    def exists(self, event_id: str, terminal_only: bool = False, filename: str = "casebook.json") -> bool:
        """Check if a casebook exists. If terminal_only is True, returns True only if status is a terminal state."""
        ...

    def terminal_status(self, event_id: str,
                        filenames: tuple = ("status.json", "casebook.json")) -> Optional[str]:
        """Return the terminal status recorded in EITHER file, or None.

        Checking both is what closes F4: whichever actor got there first, its
        verdict is visible to every other actor.

        `filenames` narrows that search. Every actor that records a terminal
        status does so through `save_terminal`, which writes casebook.json and
        then status.json -- so a caller that only needs to know whether SOME
        actor has finished can read status.json alone and halve the round
        trips. On the S3 backend each of these is a network GET, and the
        late-result guard in routes.py runs on every packet.
        """
        ...

    # ------------------------------------------------------------------
    # Blob artifacts (G3)
    # ------------------------------------------------------------------
    # The JSON methods above cover casebook.json / status.json / outcome.json.
    # Everything else beside a casebook -- raw_logs.txt, reduced_logs.txt, the
    # Kubernetes snapshot JSONL -- was written straight to the local
    # filesystem, bypassing this abstraction entirely. Under
    # CASEBOOK_STORAGE_BACKEND=s3 those artifacts lived on whichever pod ran
    # the packet and died with it, which silently disabled snapshot reuse --
    # the mechanism that makes the short-retention Kubernetes source viable
    # across retries, DLQ replays, and checkpoint resumes.

    def save_artifact(self, event_id: str, filename: str, content: str) -> str:
        """Persist a text artifact beside the casebook. Returns its locator."""
        ...

    def load_artifact(self, event_id: str, filename: str) -> Optional[str]:
        """Read a text artifact back, or None when it is absent/unreadable."""
        ...

    def artifact_exists(self, event_id: str, filename: str) -> bool:
        """Is this artifact present?"""
        ...

    def update_json(self, event_id: str, filename: str, mutate) -> dict:
        """Atomic read-modify-write of one JSON document.

        `mutate` receives the current document (or None when absent) and
        returns the document to store. It may be called more than once and
        must therefore be free of side effects.

        This exists because a read-modify-write built out of `load()` and
        `save()` is only atomic where the backend can make it so, and the two
        backends make it so by completely different means -- a lock locally, a
        conditional write on S3. Callers that need the guarantee (the DLT
        group counters, which are incremented by several analysis pods at
        once) must not have to know which backend they are talking to.

        Returns the stored document.
        """
        ...

    def list_events(self) -> list:
        """Every event id this store holds a casebook directory for.

        Needed so `iter_outcomes()` can aggregate accuracy without walking the
        local filesystem, which returned nothing at all on the S3 backend and
        made `accuracy_report` print "No outcomes recorded yet" regardless of
        how many verdicts had been recorded (G2).
        """
        ...
