"""
The casebook contract: the shape of every casebook.json this system saves.

The shape is published to the services that read casebooks as JSON Schema --
casebook_structures/rejection_casebook.schema.json and dlt_casebook.schema.json
-- and these models are the same contract in code: this module for the
rejection lane, `src/models/dlt_casebook.py` for the DLT lane. Readers build
their own objects from those files, so a writer that drifts from them -- a key
renamed, a field dropped, a value of the wrong type, an action the contract
does not list -- breaks a service that is not this one.

Every casebook therefore passes through `save_rejection_casebook` or
`save_dlt_casebook` on its way to storage, and is checked against its model
there. Nothing else in src/ calls `save_terminal`;
tests/test_casebook_contract.py holds every writer to that, and holds these
models and the published schemas to each other.

What a violation does is CASEBOOK_CONTRACT_MODE:

  enforce  (the default) the casebook is not saved and CasebookContractError
           is raised. The request fails, the consumer dead-letters the
           packet, and it is replayed once the writer is fixed.
  record   the casebook is saved as it is, and the violation is logged.

Both count it in agentic_resident_crm_casebook_contract_violations_total.

A document is checked before it is saved, so neither of the two fields the
storage layer stamps on every save is required of it: `schema_version`, and
`casebook_metadata.last_updated` on the records whose writer does not set it.
"""
import os
from typing import Annotated, Any, ClassVar, Dict, List, Literal, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_serializer,
    model_validator,
)

from src.models.synthesis import ACTIONS, RESIDENT_ACTIONS
from src.storage.base import CASEBOOK_SCHEMA_VERSION
from src.utils import metrics
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

ENV_MODE = "CASEBOOK_CONTRACT_MODE"
MODE_ENFORCE = "enforce"
MODE_RECORD = "record"
MODES = (MODE_ENFORCE, MODE_RECORD)

#: This system's own timestamps: UTC, to the second (`timestamps.utc_now`).
UTC_SECOND = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

#: A digest as this system writes one.
SHA256 = r"^sha256:[0-9a-f]{64}$"

#: How packet_metadata.service was decided (`service_registry.SOURCE_*`).
SERVICE_SOURCES = ("reason_code_map", "flow_stage", "source_topic",
                   "reason_code_docs", "none")

#: An investigation that finished. REJECTED is the packet's own packetStatus.
INVESTIGATED_STATUSES = ("REJECTED", "NEEDS_MANUAL_REVIEW", "FAILED_SYNTHESIS_PARSE")

#: A run that never finished, recorded as a short record.
FAILURE_RECORD_STATUSES = ("FAILED_TIMEOUT", "DLQ", "FAILED_SHUTDOWN")

#: `reason_code_docs.lookup` outcomes, and "disabled" when lookups are off.
DOC_OUTCOMES = ("hit", "miss", "no_reason_code", "error", "disabled")
#: `reason_code_docs.SCOPE_*`.
DOC_SCOPES = ("own", "other_service", "all")
DOC_REF_KINDS = ("code", "rule")

#: How the Investigator and the Reviewer ran.
AGENT_PATHS = ("direct", "harness")


def contract_mode() -> str:
    """CASEBOOK_CONTRACT_MODE, read at call time like every other switch.
    Anything unrecognised is enforce: a typo must not quietly turn the check
    off. Boot validation reports one (config_validator)."""
    raw = (os.environ.get(ENV_MODE) or "").strip().lower()
    return raw if raw in MODES else MODE_ENFORCE


class CasebookContractError(ValueError):
    """A casebook did not match its contract, and was not saved."""


class CasebookBlock(BaseModel):
    """One object in a casebook.

    Unknown keys are refused and values are not coerced, so a document is
    checked as its writer built it. A field named in `_OMIT_WHEN_NONE` is one
    the contract leaves out rather than writing as null; every other field is
    written, null included.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset()

    @model_serializer(mode="wrap")
    def _leave_out_absent(self, handler):
        document = handler(self)
        for name in self._absent_fields():
            document.pop(name, None)
        return document

    def _absent_fields(self) -> set:
        """The fields to leave out of this block's document."""
        return {name for name in self._OMIT_WHEN_NONE if getattr(self, name) is None}


def describe(error: ValidationError) -> str:
    """Every violation, one per field, as `path: message`."""
    parts = []
    for item in error.errors():
        field = ".".join(str(part) for part in item["loc"]) or "(root)"
        parts.append(f"{field}: {item['msg']}")
    return "; ".join(parts)


def checked(model: type, document: dict, *, lane: str, key=None) -> dict:
    """`document` as `model` writes it -- or, when it breaks the contract,
    what CASEBOOK_CONTRACT_MODE says to do about that: raise
    CasebookContractError (enforce), or return `document` unchanged (record).
    """
    try:
        return model.model_validate(document).model_dump(mode="json", by_alias=True)
    except ValidationError as error:
        problems = describe(error)
        refused = contract_mode() == MODE_ENFORCE
        metrics.record_casebook_contract_violation(lane, "refused" if refused else "saved")
        logger.error("Casebook does not match its contract",
                     lane=lane, key=key, contract=model.__name__, problems=problems,
                     outcome="refused" if refused else "saved anyway")
        if refused:
            raise CasebookContractError(
                f"The {lane} casebook {key} does not match its contract "
                f"({model.__name__}) and was not saved: {problems}") from error
        return document


# ======================================================================
# The rejection casebook
# ======================================================================

class CasebookMetadata(CasebookBlock):
    created_at: str = Field(pattern=UTC_SECOND)
    last_updated: str = Field(pattern=UTC_SECOND)


class StampedMetadata(CasebookBlock):
    """The metadata of a record whose writer sets none: the storage layer's
    save stamp alone."""
    last_updated: str = Field(pattern=UTC_SECOND)


class ServiceResolutionDetail(CasebookBlock):
    """The payload values a resolution looked at. Open, like the published
    schema's: a key beyond these is kept."""
    model_config = ConfigDict(extra="allow", strict=True)

    stage: Optional[str] = None
    sub_stage: Optional[str] = None
    source_topic: Optional[str] = None
    reason_code: Optional[str] = None


class ServiceResolution(CasebookBlock):
    """How packet_metadata.service was decided (`service_registry.resolve`).

    As often read back from the fetch stage's stored copy as computed afresh,
    so a key an older copy lacks is written as null rather than refused, and
    a key beyond these is kept, as the published schema allows.
    """
    model_config = ConfigDict(extra="allow", strict=True)

    service: str
    source: Literal[SERVICE_SOURCES]  # type: ignore[valid-type]
    matched: Optional[str] = None
    conflict: Optional[Dict[str, str]] = None
    detail: ServiceResolutionDetail = Field(default_factory=ServiceResolutionDetail)
    registry_sha256: Optional[str] = None
    reason_code_map_sha256: Optional[str] = None


class PacketMetadata(CasebookBlock):
    srn: Optional[str]
    sid: Optional[str]
    ref_id: Optional[str]
    source: Optional[str]
    packet_type: Optional[str]
    is_mbu: Optional[bool]
    update_type: Optional[str]
    is_child: Optional[bool]
    created_at: Optional[str]
    rejected_at: Optional[str]
    service: str
    service_resolution: ServiceResolution

    @field_validator("is_mbu", mode="before")
    @classmethod
    def _flag(cls, value):
        """isMBU is passed through from the payload, and only the AUDIT
        contract types it. Read the way that contract reads it: a boolean, or
        "true"/"false" in any case; anything else says nothing, so null --
        rather than a packet refused over a field nothing here uses."""
        if value is None or isinstance(value, bool):
            return value
        return {"true": True, "false": False}.get(str(value).strip().lower())


class RejectionLogs(CasebookBlock):
    path: str
    gaps: Optional[str]
    coverage: Optional[str]


class RejectionData(CasebookBlock):
    rejection_code: Optional[str]
    rejection_description: str
    rejection_logs: RejectionLogs


class PacketStatus(CasebookBlock):
    status: Literal[INVESTIGATED_STATUSES]  # type: ignore[valid-type]
    service: Optional[str]
    sub_service: Optional[str]
    last_updated: str = Field(pattern=UTC_SECOND)
    is_in_process: Optional[bool]
    rejection_data: RejectionData


class ConfidencePolicy(CasebookBlock):
    model_confidence: float = Field(ge=0, le=1)
    detail: str


class ShadowComparison(CasebookBlock):
    runbook_id: str
    action: Optional[str]
    resident_action: Optional[str]
    synthesis: Optional[str]
    agreed: bool


class ServicePack(CasebookBlock):
    service: str
    sha256: Optional[str]


class ReasonCodeDocRef(CasebookBlock):
    source: str
    kind: Literal[DOC_REF_KINDS]  # type: ignore[valid-type]
    ref: str


class ReasonCodeDoc(CasebookBlock):
    outcome: Literal[DOC_OUTCOMES]  # type: ignore[valid-type]
    reason_code: Optional[str]
    requested_type: Optional[str]
    matched_type: Optional[str]
    refs: List[ReasonCodeDocRef]
    sha256: Optional[str]
    truncated: bool
    detail: Optional[str]
    scope: Optional[Literal[DOC_SCOPES]]  # type: ignore[valid-type]


class ToolCall(CasebookBlock):
    tool: str
    args: Dict[str, Any]


#: The prompts' Langfuse versions, {prompt name: version}, under
#: PROMPT_SOURCE=langfuse; a digest of the prompt files under files.
PromptFingerprint = Union[Annotated[str, Field(pattern=SHA256)], Dict[str, int]]


class Provenance(CasebookBlock):
    prompt_fingerprint: PromptFingerprint
    service_pack: ServicePack
    reason_code_doc: Optional[ReasonCodeDoc]
    investigator_path: Optional[Literal[AGENT_PATHS]]  # type: ignore[valid-type]
    reviewer_path: Optional[Literal[AGENT_PATHS]]  # type: ignore[valid-type]
    tool_calls: List[ToolCall]


class Resolution(CasebookBlock):
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset(
        {"confidence_policy", "parse_error", "raw_output", "shadow"})

    source: str = Field(pattern=r"^(agent|runbook:.+)$")
    synthesis: str
    action: Literal[ACTIONS]  # type: ignore[valid-type]
    resident_action: Literal[RESIDENT_ACTIONS]  # type: ignore[valid-type]
    confidence: Optional[float] = Field(ge=0, le=1)
    abstained: bool
    confidence_policy: Optional[ConfidencePolicy] = None
    parse_error: Optional[str] = None
    raw_output: Optional[str] = Field(default=None, max_length=2000)
    shadow: Optional[ShadowComparison] = None
    provenance: Provenance


class RejectionCasebook(CasebookBlock):
    """casebook.json for an investigation that finished: REJECTED,
    NEEDS_MANUAL_REVIEW, or FAILED_SYNTHESIS_PARSE."""
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"pilot", "schema_version"})

    casebook_metadata: CasebookMetadata
    packet_metadata: PacketMetadata
    packet_status: PacketStatus
    resolution: Resolution
    pilot: Optional[Literal[True]] = None
    schema_version: Optional[Literal[CASEBOOK_SCHEMA_VERSION]] = None  # type: ignore[valid-type]

    @model_validator(mode="after")
    def _a_parse_failure_carries_its_evidence(self):
        if self.packet_status.status != "FAILED_SYNTHESIS_PARSE":
            return self
        resolution = self.resolution
        missing = [name for name in ("parse_error", "raw_output")
                   if getattr(resolution, name) is None]
        if missing:
            raise ValueError("a FAILED_SYNTHESIS_PARSE casebook must carry "
                             + " and ".join(f"resolution.{name}" for name in missing))
        if (resolution.action, resolution.resident_action) != ("MANUAL_REVIEW", "PENDING"):
            raise ValueError("a FAILED_SYNTHESIS_PARSE casebook's action must be "
                             "MANUAL_REVIEW and its resident_action PENDING")
        return self


class FailureRecordPacketMetadata(CasebookBlock):
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"abandoned_at"})

    eid: str
    abandoned_at: Optional[float] = None


class FailureRecordStatus(CasebookBlock):
    status: Literal[FAILURE_RECORD_STATUSES]  # type: ignore[valid-type]


class FailureRecordResolution(CasebookBlock):
    synthesis: str


class RejectionFailureRecord(CasebookBlock):
    """casebook.json for a run that never finished: FAILED_TIMEOUT, DLQ, or
    FAILED_SHUTDOWN."""
    _OMIT_WHEN_NONE: ClassVar[frozenset] = frozenset({"schema_version", "casebook_metadata"})

    packet_metadata: FailureRecordPacketMetadata
    packet_status: FailureRecordStatus
    resolution: FailureRecordResolution
    schema_version: Optional[Literal[CASEBOOK_SCHEMA_VERSION]] = None  # type: ignore[valid-type]
    casebook_metadata: Optional[StampedMetadata] = None


def check_rejection_casebook(document: dict, key=None) -> dict:
    """`document` checked against the rejection contract: the short record's
    when its status says the run never finished, the full casebook's
    otherwise. See `checked` for what a violation does."""
    status = ((document or {}).get("packet_status") or {}).get("status")
    model = RejectionFailureRecord if status in FAILURE_RECORD_STATUSES else RejectionCasebook
    return checked(model, document, lane="rejection", key=key)


def save_rejection_casebook(storage, event_id: str, document: dict) -> None:
    """Save `document` as the packet's terminal casebook, once it has passed
    the contract (`CasebookStorage.save_terminal`). The one way a rejection
    casebook reaches storage."""
    storage.save_terminal(event_id, check_rejection_casebook(document, key=event_id))
