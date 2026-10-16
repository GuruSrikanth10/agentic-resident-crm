"""
The DLT casebook contract: casebook_structures/dlt_casebook.schema.json in
code. What the contract is, and what a violation does, is in
`src/models/casebook.py`; this module is the DLT lane's half of it.

The vocabularies below are written out rather than imported from the modules
that produce them, so the contract is readable on its own and loads nothing
heavier than pydantic. tests/test_casebook_contract.py holds each one to its
producer's constants and to the published schema.
"""
from typing import Any, ClassVar, Dict, List, Literal, Optional

from pydantic import ConfigDict, Field, model_validator

from src.dlt.case_storage import DLT_CASEBOOK_SCHEMA_VERSION
from src.models.casebook import CasebookBlock, StampedMetadata, checked
from src.models.dlt_synthesis import DLT_ACTIONS

#: `classify.FailureClass`.
FAILURE_CLASSES = ("A", "B", "C", "U")
#: `corroborate.Verdict`.
CORROBORATION_VERDICTS = ("CORROBORATED", "PARTIAL", "CONTRADICTED", "UNVERIFIABLE")
#: Where packet.ref_id was read from (`payload.resolve_ref_id`).
REF_ID_SOURCES = ("contract", "record_key", "configured_path", "type_path", "search", "none")
#: How packet.service was decided (`service_registry.resolve_dlt`).
SERVICE_SOURCES = ("consumer_group", "flow_stage", "original_topic", "java_package",
                   "source_topic", "reason_code_docs", "none")
#: Why the log evidence is weaker than usual (`/fetch-dlt-logs`).
EVIDENCE_GAPS = ("REFID_KEY_PAYLOAD_MISMATCH", "NO_CORRELATION_ID", "NO_TIMESTAMP",
                 "LOGS_TOO_OLD", "LOG_FETCH_FAILED")
#: `apply_dlt_confidence_policy` labels.
CEILINGS = ("class_b", "class_u", "unverifiable", "logs_unavailable", "logs_silent",
            "contradicted", "registry_miss", "unreviewed", "evidence_gap", "reuse_decay")
#: `per_code` patterns, severities and the finding fields it scans.
PER_CODE_PATTERNS = ("uuid", "long_number", "this_packet", "source_line")
PER_CODE_SEVERITIES = ("identifier", "phrasing")
PER_CODE_FIELDS = ("narrative", "recommendation", "discrepancy")
#: How the finding was produced.
FINDING_SOURCES = ("agent", "canned", "group_reuse", "failed_synthesis")
#: `reuse.Decision`.
REUSE_DECISIONS = ("LLM_REQUIRED", "REUSE_GROUP", "CANNED")
#: Whether the record was counted in its group.
GROUP_STATES = ("ok", "occurrence_not_recorded", "unavailable")
#: `groups.STATE_*`.
RECOMMENDATION_STATES = ("none", "draft", "final", "unpersisted", "withheld", "per_occurrence")
#: `single_flight` outcomes.
SINGLE_FLIGHT_OUTCOMES = ("leader", "reused", "waited_then_ran", "timeout")
#: `code_check` verdicts.
CODE_CHECK_VERDICTS = ("NO_CHANGE", "NOT_DEPLOYED", "FIX_DEPLOYED", "UNKNOWN")

#: An analysed record: COMPLETED for NO_ACTION, NEEDS_MANUAL_REVIEW otherwise.
ANALYSED_STATUSES = ("COMPLETED", "NEEDS_MANUAL_REVIEW")
#: A record whose analysis ran out of time, recorded as a short record.
TIMEOUT_STATUS = "FAILED_TIMEOUT"

FINGERPRINT = r"^[0-9a-f]{64}$"


class DltSource(CasebookBlock):
    original_topic: Optional[str]
    partition: Optional[int]
    offset: Optional[int]
    consumer_group: Optional[str]
    attempts: Optional[int]
    original_timestamp: Optional[int]
    last_attempt_timestamp: Optional[int]
    anchor_is_fallback: bool
    type_id: Optional[str]


class DltServiceResolutionDetail(CasebookBlock):
    """The values a resolution looked at. Open, like the published schema's."""
    model_config = ConfigDict(extra="allow", strict=True)

    consumer_group: Optional[str] = None
    original_topic: Optional[str] = None
    stage: Optional[str] = None
    sub_stage: Optional[str] = None
    source_topic: Optional[str] = None
    java_frame: Optional[str] = None
    reason_code: Optional[str] = None


class DltServiceResolution(CasebookBlock):
    """How packet.service was decided. Read back from the fetch stage's
    stored copy, so it is filled in and kept open as the rejection lane's is
    (`casebook.ServiceResolution`)."""
    model_config = ConfigDict(extra="allow", strict=True)

    service: str
    source: Literal[SERVICE_SOURCES]  # type: ignore[valid-type]
    matched: Optional[str] = None
    conflict: Optional[Dict[str, str]] = None
    detail: DltServiceResolutionDetail = Field(default_factory=DltServiceResolutionDetail)
    registry_sha256: Optional[str] = None
    reason_code_map_sha256: Optional[str] = None


class DltPacket(CasebookBlock):
    ref_id: Optional[str]
    ref_id_source: Literal[REF_ID_SOURCES]  # type: ignore[valid-type]
    record_key: Optional[str]
    payload_ref_id_conflict: Optional[str]
    event_id: Optional[str]
    service: Optional[str]
    service_resolution: Optional[DltServiceResolution]


class DltFailure(CasebookBlock):
    # `class` in the document; a Python keyword, so named otherwise here.
    failure_class: Literal[FAILURE_CLASSES] = Field(alias="class")  # type: ignore[valid-type]
    class_reason: str
    root_fqcn: Optional[str]
    business_code: Optional[str]
    registry_description: Optional[str]
    registry_category: Optional[str]
    registry_category_source: Optional[str]
    registry_stage: Optional[str]
    signature: str
    fingerprint: str = Field(pattern=FINGERPRINT)
    fingerprint_service: Optional[str]
    frames: List[str]
    truncated: bool


class DltEvidence(CasebookBlock):
    corroboration: Literal[CORROBORATION_VERDICTS]  # type: ignore[valid-type]
    corroboration_reason: str
    citations: List[str] = Field(max_length=20)
    unexplained_exceptions: List[str]
    could_not_look: bool
    log_window: Optional[str]
    gaps: List[Literal[EVIDENCE_GAPS]]  # type: ignore[valid-type]


class DltPerCodeViolation(CasebookBlock):
    pattern: Literal[PER_CODE_PATTERNS]  # type: ignore[valid-type]
    severity: Literal[PER_CODE_SEVERITIES]  # type: ignore[valid-type]
    field: Literal[PER_CODE_FIELDS]  # type: ignore[valid-type]
    text: str


class DltCasebookFinding(CasebookBlock):
    """The casebook's `finding` block -- not `dlt_synthesis.DltFinding`, the
    analysis contract it is written from."""
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"per_code_violations", "parse_error"})

    narrative: str
    discrepancy: Optional[str]
    recommendation: str
    action: Literal[DLT_ACTIONS]  # type: ignore[valid-type]
    reviewer_approved: Optional[bool]
    per_code_violations: Optional[List[DltPerCodeViolation]] = None
    parse_error: Optional[str] = None


class DltConfidence(CasebookBlock):
    score: Optional[float] = Field(ge=0, le=1)
    ceilings_applied: List[Literal[CEILINGS]]  # type: ignore[valid-type]
    abstained: bool


class DltServicePack(CasebookBlock):
    service: Optional[str]
    sha256: Optional[str]


class DltProvenance(CasebookBlock):
    source: Literal[FINDING_SOURCES]  # type: ignore[valid-type]
    reuse_decision: Literal[REUSE_DECISIONS]  # type: ignore[valid-type]
    reuse_reason: str
    group_fingerprint: str
    group_occurrences: Optional[int]
    group_state: Literal[GROUP_STATES]  # type: ignore[valid-type]
    recommendation_state: Literal[RECOMMENDATION_STATES]  # type: ignore[valid-type]
    single_flight: Optional[Literal[SINGLE_FLIGHT_OUTCOMES]]  # type: ignore[valid-type]
    service_pack: DltServicePack


class DltCodeCheckCandidate(CasebookBlock):
    commit: str
    subject: str
    path: str
    timestamp_ms: Optional[int]
    first_containing_version: Optional[str]
    resolved_by: str


class DltCodeCheck(CasebookBlock):
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"details"})

    verdict: Literal[CODE_CHECK_VERDICTS]  # type: ignore[valid-type]
    reason: str
    repo: Optional[str]
    path: Optional[str]
    paths_checked: List[str]
    frames_resolved: int
    frames_unmapped: int
    branch: Optional[str]
    baseline_version: Optional[str]
    running_version: Optional[str]
    required_version: Optional[str]
    candidates: List[DltCodeCheckCandidate]
    frames_examined: int
    checked_at: float
    details: Optional[Dict[str, Any]] = None


class DltReplayArgs(CasebookBlock):
    """The request sent to the replay service (`auto_replay.attempt`)."""
    id: str
    idType: str
    priority: int
    operatorName: str
    category: str
    fromSedaStart: bool


class DltReplay(CasebookBlock):
    """`args` is there exactly when a replay was attempted, null when there
    was no refId to send."""
    attempted: bool
    reason: str
    queued: bool
    result: Optional[str]
    args: Optional[DltReplayArgs] = None

    @model_validator(mode="after")
    def _args_exactly_when_attempted(self):
        if self.attempted != ("args" in self.model_fields_set):
            raise ValueError("replay.args must be present exactly when attempted is true")
        return self

    def _absent_fields(self) -> set:
        return set() if self.attempted else {"args"}


class DltParked(CasebookBlock):
    """`required_version` is there exactly when the replay was parked."""
    parked: bool
    reason: str
    required_version: Optional[str] = None

    @model_validator(mode="after")
    def _version_exactly_when_parked(self):
        if self.parked != ("required_version" in self.model_fields_set):
            raise ValueError("parked.required_version must be present exactly when "
                             "parked is true")
        return self

    def _absent_fields(self) -> set:
        return set() if self.parked else {"required_version"}


class DltPacketStatus(CasebookBlock):
    status: Literal[ANALYSED_STATUSES]  # type: ignore[valid-type]


class DltCasebook(CasebookBlock):
    """casebook.json for an analysed dead-lettered record."""
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"casebook_metadata"})

    schema_version: Literal[DLT_CASEBOOK_SCHEMA_VERSION]  # type: ignore[valid-type]
    case_id: Optional[str]
    detected_at: float
    source: DltSource
    packet: DltPacket
    failure: DltFailure
    evidence: DltEvidence
    finding: DltCasebookFinding
    confidence: DltConfidence
    provenance: DltProvenance
    code_check: DltCodeCheck
    replay: DltReplay
    parked: DltParked
    packet_status: DltPacketStatus
    casebook_metadata: Optional[StampedMetadata] = None

    @model_validator(mode="after")
    def _the_finding_carries_what_its_source_records(self):
        source = self.provenance.source
        if source == "agent" and self.finding.per_code_violations is None:
            raise ValueError("an agent finding must carry finding.per_code_violations")
        if source == "failed_synthesis" and self.finding.parse_error is None:
            raise ValueError("a failed_synthesis finding must carry finding.parse_error")
        return self


class DltTimeoutPacket(CasebookBlock):
    ref_id: Optional[str]


class DltTimeoutFinding(CasebookBlock):
    narrative: str
    recommendation: str
    action: Literal["NEEDS_MANUAL_REVIEW"]


class DltTimeoutStatus(CasebookBlock):
    status: Literal["FAILED_TIMEOUT"]


class DltTimeoutRecord(CasebookBlock):
    """casebook.json for a record whose analysis ran out of time
    (`case_storage.timeout_casebook`)."""
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"casebook_metadata"})

    schema_version: Literal[DLT_CASEBOOK_SCHEMA_VERSION]  # type: ignore[valid-type]
    case_id: Optional[str]
    detected_at: float
    packet: DltTimeoutPacket
    finding: DltTimeoutFinding
    packet_status: DltTimeoutStatus
    casebook_metadata: Optional[StampedMetadata] = None


def check_dlt_casebook(document: dict, key=None) -> dict:
    """`document` checked against the DLT contract: the timeout record's when
    its status is FAILED_TIMEOUT, the full casebook's otherwise. See
    `casebook.checked` for what a violation does."""
    status = ((document or {}).get("packet_status") or {}).get("status")
    model = DltTimeoutRecord if status == TIMEOUT_STATUS else DltCasebook
    return checked(model, document, lane="dlt", key=key)


def save_dlt_casebook(storage, key: str, document: dict) -> None:
    """Save `document` as the record's terminal casebook, once it has passed
    the contract (`CasebookStorage.save_terminal`). The one way a DLT
    casebook reaches storage."""
    storage.save_terminal(key, check_dlt_casebook(document, key=key))
