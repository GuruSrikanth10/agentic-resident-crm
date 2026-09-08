from typing import Optional, List, Any, Dict
from pydantic import BaseModel, ConfigDict, Field

# eventId is interpolated directly into filesystem paths (local casebook
# storage) and S3 keys. Left unconstrained, a value like "../../something"
# escapes the storage root even though this arrives API-key-protected and
# straight off a Kafka topic (0.11).
EVENT_ID_PATTERN = r"^[A-Za-z0-9_.:-]{1,128}$"

class ErrorData(BaseModel):
    type: Optional[str] = None
    errorReasonCode: Optional[str] = None

class PacketExecutionSummary(BaseModel):
    hasExecutionErrors: Optional[bool] = None
    hasValidationErrors: Optional[bool] = None
    packetStatus: Optional[str] = None
    errorData: Optional[List[ErrorData]] = None
    isExecutionSuccess: Optional[bool] = None
    isValidationSuccess: Optional[bool] = None

class PacketMetaData(BaseModel):
    """The packet fields this system actually keys on (4.6).

    These were `Dict[str, Any]`, so an upstream rename silently produced None
    in the casebook and, since F11, silently dropped an identifier the
    Kubernetes source needs to match log lines. Declaring them does not make
    them required -- it makes their absence visible.

    `extra="allow"` is deliberate: upstream adds fields regularly and
    rejecting a packet over an unknown key would turn a schema addition into
    an outage.
    """

    model_config = ConfigDict(extra="allow")

    refId: Optional[str] = None
    srn: Optional[str] = None
    enrolmentType: Optional[str] = None
    pktSource: Optional[str] = None
    uid: Optional[str] = None


class FlowMetaData(BaseModel):
    """Where in the flow the packet was when it failed."""

    model_config = ConfigDict(extra="allow")

    stage: Optional[str] = None
    subStage: Optional[str] = None


class MessagePayload(BaseModel):
    eventId: str = Field(pattern=EVENT_ID_PATTERN)
    category: Optional[str] = None
    eventType: Optional[str] = None
    eventTimestamp: Optional[str] = None
    eventVersion: Optional[str] = None
    sid: Optional[str] = None
    sidDate: Optional[str] = None
    version: Optional[str] = None
    sourceTopic: Optional[str] = None
    callbackTopic: Optional[str] = None
    flowMetaData: Optional[FlowMetaData] = None
    taskMetaData: Optional[Any] = None
    packetMetaData: Optional[PacketMetaData] = None
    packetExecutionSummary: PacketExecutionSummary
    rejectBits: Optional[Any] = None
    requestStage: Optional[str] = None
    requestStageStatus: Optional[str] = None
    resubmissionSummary: Optional[Dict[str, Any]] = None
    uidV2DataArray: Optional[Any] = None
