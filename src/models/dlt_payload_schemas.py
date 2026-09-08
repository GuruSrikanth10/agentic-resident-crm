"""The DLT payload family -- typed. See DLT_PLAN.md 5.3.

These models exist to **locate and describe**, never to gate. The payload is
upstream's contract, not ours: it is deserialised from a Java model we do not
own, on a topic whose schema can gain a field without telling us. So every
field is optional, every model allows extras, and `parse_payload` returns None
instead of raising. A payload we failed to anticipate must cost us the `refId`
at worst -- never the message, whose real evidence is in the headers.

That is also why validation is not wired into `DltMessage.payload`, which stays
`Any`. This module is applied *on top of* the raw dict by the code that wants
structure (identifier extraction, the payload summary shown to the analyst),
and the raw dict is what gets persisted and republished.

Two traps live in this shape, both from the `EnrolmentEventResponse` sample:

* **`event_id` is not `refId`.** The payload carries its own `event_id`
  (`b733ab61-...`) which is a *different* UUID from the `refId`
  (`c5d21184-...`) that the service writes into its pod logs. This project's
  own vocabulary calls refId "the event id" (DLT_PLAN.md 3), so reaching for
  the field literally named `event_id` is the natural mistake -- and it fails
  silently, as a correlation id that matches no log line anywhere. Nothing in
  this module or in `src/dlt/payload.py` may treat it as an identifier.

* **`eventTimestamp` is local time, not UTC.** The sample reads
  `2026-08-17 19:07:47.552` where `kafka_original-timestamp` is
  `1786973867552` = `13:37:47.552Z` -- the same instant, expressed +05:30, with
  no offset written down. Never anchor a log window on it; the headers carry
  real epoch millis (`src/dlt/window.py`).
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

#: Shared by every model here: unknown fields are kept, not rejected. An
#: upstream schema addition is a routine event and must never dead-letter our
#: own analysis of a dead-lettered record.
_LENIENT = ConfigDict(extra="allow", populate_by_name=True)


class MatchedCandidate(BaseModel):
    """One biometric match returned by an ABIS instance.

    `candidateRefId` is the refId of a *different* enrolment -- the record the
    matcher thinks looks like this one. It is emphatically not this packet's
    correlation id, and searching pod logs for it would pull in an unrelated
    packet's lines.
    """

    model_config = _LENIENT

    candidateRefId: Optional[str] = None
    scaledScore: Optional[int] = None
    faceScore: Optional[int] = None
    fingerLeftSlapScore: Optional[int] = None
    fingerRightSlapScore: Optional[int] = None
    fingerBothThumbsScore: Optional[int] = None
    leftIrisScore: Optional[int] = None
    rightIrisScore: Optional[int] = None


class Candidates(BaseModel):
    model_config = _LENIENT

    matchedCandidate: List[MatchedCandidate] = Field(default_factory=list)


class AbisResponse(BaseModel):
    """One ABIS instance's verdict. `abisId` is ABIS1/ABIS2/ABIS3 in the sample."""

    model_config = _LENIENT

    abisId: Optional[str] = None
    candidates: Optional[Candidates] = None
    diagnostics: Optional[Any] = None

    @property
    def candidate_count(self) -> int:
        return len(self.candidates.matchedCandidate) if self.candidates else 0


class AbisResponses(BaseModel):
    model_config = _LENIENT

    abisResponse: List[AbisResponse] = Field(default_factory=list)

    #: Duplicates the enclosing `refId` in the sample. Kept as a free fallback
    #: path for identifier extraction, not as an independent identifier.
    referenceId: Optional[str] = None


class AbisMwResponse(BaseModel):
    """The ABIS middleware's response block -- where the real `refId` lives."""

    model_config = _LENIENT

    requestType: Optional[str] = None
    responseStatus: Optional[str] = None

    #: The middleware's own request id. Correlates ABIS-side, not packet-side.
    requestId: Optional[str] = None

    #: **The log-correlation identifier.** The same value the DLT record is
    #: keyed on, and the only one the service writes into its pod logs.
    refId: Optional[str] = None

    abisResponses: Optional[AbisResponses] = None


class EnrolmentEventResponse(BaseModel):
    """`__TypeId__: in.gov.uidai.uidabismiddlewaresb.kafka.model.EnrolmentEventResponse`

    The payload on `ENU.MWARE.DEDUPE.PROCESS.COMPLETION.V1`, consumed by
    `AbisMwResponseConsumer`. Captured from a real dead-lettered record on
    2026-08-20.
    """

    model_config = _LENIENT

    #: The payload's own id. **Not** the log-correlation id -- see the module
    #: docstring. Named `event_id` while the rest of the envelope is camelCase,
    #: which is exactly why it is easy to grab by mistake.
    event_id: Optional[str] = None

    category: Optional[str] = None
    event_type: Optional[str] = None

    #: Local time (+05:30 in the sample), no offset written down. Not an anchor.
    eventTimestamp: Optional[str] = None

    sid: Optional[str] = None
    sidDate: Optional[str] = None
    version: Optional[str] = None
    sourceTopic: Optional[str] = None
    callbackTopic: Optional[str] = None
    flowMetaData: Optional[Any] = None

    abisMWResponseNewSeda: Optional[AbisMwResponse] = None

    @property
    def ref_id(self) -> Optional[str]:
        """The log-correlation id, or None. Never falls back to `event_id`."""
        block = self.abisMWResponseNewSeda
        if block is None:
            return None
        if block.refId:
            return block.refId
        if block.abisResponses and block.abisResponses.referenceId:
            return block.abisResponses.referenceId
        return None


# ---------------------------------------------------------------------------
# The type registry
# ---------------------------------------------------------------------------
# DLT_PLAN.md 5.3 assumed one payload schema, named by `__TypeId__`. Two are
# now confirmed on the DLT (`EventMessage` and `EnrolmentEventResponse`), from
# two different original topics, so the "single schema" assumption is retired.
# Adding a third stays config, not code: register a path here or set
# DLT_REFID_PATHS_BY_TYPE.
#
# A type is fully supported when three things exist for it: refId paths here,
# a model in `MODELS_BY_TYPE`, and a summariser in `SUMMARISERS_BY_TYPE` that
# ends with `identifier_block`. None of the three is required -- an
# unregistered type still yields a case, a bounded key listing and an
# explicitly unlabelled identifier section -- but each one it has makes the
# evidence stronger, and none of them is a prompt change.

TYPE_ENROLMENT_EVENT_RESPONSE = (
    "in.gov.uidai.uidabismiddlewaresb.kafka.model.EnrolmentEventResponse")
TYPE_EVENT_MESSAGE = "com.uidai.enu.common.model.EventMessage"

#: `__TypeId__` -> ordered dotted paths at which the refId is known to sit.
#: Tried in order; the first scalar hit wins. An unregistered type falls
#: through to the bounded search in `src/dlt/payload.py`, which is why an
#: unknown payload still works -- just with weaker provenance.
REFID_PATHS_BY_TYPE: Dict[str, tuple] = {
    TYPE_ENROLMENT_EVENT_RESPONSE: (
        "abisMWResponseNewSeda.refId",
        "abisMWResponseNewSeda.abisResponses.referenceId",
    ),
    # No payload has been captured for EventMessage yet -- the reference
    # sample carried headers only. Left unregistered on purpose: a guessed
    # path that silently misses is worse than the search, which at least
    # reports `search` as its provenance.
}

#: `__TypeId__` -> model. Only used for the payload summary; extraction does
#: not need the model and must keep working for unmodelled types.
MODELS_BY_TYPE = {
    TYPE_ENROLMENT_EVENT_RESPONSE: EnrolmentEventResponse,
}


def model_for_type(type_id: Optional[str]):
    return MODELS_BY_TYPE.get(type_id or "")


def parse_payload(payload: Any, type_id: Optional[str] = None):
    """Apply the registered model to a raw payload. None if it does not fit.

    Never raises. A payload that fails to validate against the model we
    expected is a finding about our config, not a reason to lose the case.
    """
    model = model_for_type(type_id)
    if model is None or not isinstance(payload, dict):
        return None
    try:
        return model.model_validate(payload)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# The payload summary
# ---------------------------------------------------------------------------
# Until now the payload was read for one identifier and then dropped, never
# reaching the analyst. This sample shows why that was a loss: the trace fails
# inside `filterCandidatesAndBuildRefIdUidMap -> getIndexMasterData`, and the
# candidate list that method iterates is sitting in the payload. The missing
# index-master row belongs to one of those candidates. Summarising the payload
# turns "a row was absent" into "a row was absent while resolving these two
# candidate refIds", which is the difference between a per-code answer and
# something an operator can act on.
#
# Bounded on purpose. A 3-ABIS response with a long candidate list must not be
# able to push the stacktrace or the logs out of the evidence block.

#: Candidate refIds listed before the summary elides the rest.
MAX_SUMMARY_CANDIDATES = 10

#: Top-level keys listed for a payload with no registered model.
MAX_SUMMARY_KEYS = 40


# ---------------------------------------------------------------------------
# The identifier labelling contract
# ---------------------------------------------------------------------------
# Every summary ends with this section, and it is the seam that lets one set
# of prompts serve many payload structures.
#
# The Investigator prompt used to name `refId`, `event_id` and
# `candidateRefId` directly. That worked while the DLT carried one structure
# and becomes wrong the moment it carries two: the prompt is a single file
# shared by every topic, so it would be asserting an identifier contract that
# does not hold for the payload actually in front of the model -- fields that
# are absent, or worse, present with a different meaning.
#
# So the prompt now states the invariant ("use only the id this section labels
# as the packet's correlation id, and treat every other id as belonging to
# something else"), and the per-type facts travel here, in text rendered by
# the code that already knows the type. Adding a topic is then a summariser,
# not a prompt edit.
#
# A summariser that omits this section leaves the model with unlabelled
# identifiers and an invariant it cannot apply, which is why the unregistered
# fallback below emits the section too -- saying plainly that nothing is
# labelled, rather than saying nothing at all.

IDENTIFIER_HEADING = "Identifiers in this payload:"

#: The three roles an identifier can hold. Exhaustive on purpose: an id that
#: fits none of them is one whose role has not been established, and it must
#: be labelled unlabelled rather than guessed into a bucket.
ROLE_CORRELATION = ("THIS packet's log-correlation id -- the value the pod "
                    "logs are keyed on")
ROLE_ENVELOPE = ("NOT the log-correlation id -- envelope-local, correlating to "
                 "nothing you have been shown")
ROLE_FOREIGN = "A DIFFERENT record's id -- never this packet's"


def identifier_block(correlation=(), envelope=(), foreign=(),
                     unlabelled_reason: Optional[str] = None) -> str:
    """Render the identifier section, from `(id_text, note)` pairs per role.

    `unlabelled_reason` renders the section for a payload whose identifiers
    have not been established, which is a real and useful thing to say: an
    unlabelled section stops the model reaching for an id-shaped field on the
    reasoning that nothing forbade it.
    """
    lines = [IDENTIFIER_HEADING]

    if unlabelled_reason:
        lines.append(f"  NONE LABELLED -- {unlabelled_reason}")
        lines.append("  Do not treat any value shown above as this packet's "
                     "identifier.")
        return "\n".join(lines)

    for role, entries in ((ROLE_CORRELATION, correlation),
                          (ROLE_ENVELOPE, envelope),
                          (ROLE_FOREIGN, foreign)):
        for id_text, note in entries:
            lines.append(f"  - {id_text}")
            lines.append(f"      {role}. {note}.")

    if len(lines) == 1:
        lines.append("  (none found on this payload)")
    return "\n".join(lines)


def _summarise_enrolment_event_response(model: "EnrolmentEventResponse") -> str:
    lines = [
        "Payload type: EnrolmentEventResponse "
        "(ABIS middleware dedupe response)",
        f"Category / type / version: {model.category} / {model.event_type} / {model.version}",
        f"Payload eventTimestamp: {model.eventTimestamp} "
        f"(local time, no offset recorded -- not a log anchor)",
    ]

    envelope = [(f"event_id = {model.event_id}",
                 "Named event_id while the rest of the envelope is camelCase, "
                 "which is exactly why it is the easy one to grab by mistake")]
    correlation: list = []
    foreign: list = []

    block = model.abisMWResponseNewSeda
    if block is None:
        lines.append("No abisMWResponseNewSeda block on this payload.")
        lines.append(identifier_block(correlation=correlation, envelope=envelope,
                                      foreign=foreign))
        return "\n".join(lines)

    lines.append(
        f"ABIS middleware: requestType={block.requestType} "
        f"responseStatus={block.responseStatus} requestId={block.requestId}")

    correlation.append((f"refId = {block.refId}",
                        "At abisMWResponseNewSeda.refId -- the value the "
                        "service writes into its pod lines"))
    envelope.append((f"requestId = {block.requestId}",
                     "The middleware's own request id -- correlates ABIS-side, "
                     "not packet-side"))

    responses = block.abisResponses.abisResponse if block.abisResponses else []
    if responses:
        breakdown = ", ".join(
            f"{r.abisId}: {r.candidate_count} candidate(s)" for r in responses)
        lines.append(f"ABIS results ({len(responses)} instance(s)): {breakdown}")

    candidates = [
        c for r in responses
        for c in (r.candidates.matchedCandidate if r.candidates else [])
    ]
    if candidates:
        shown = candidates[:MAX_SUMMARY_CANDIDATES]
        lines.append(
            f"Matched candidate refIds ({len(candidates)} total"
            + (f", first {len(shown)} shown" if len(candidates) > len(shown) else "")
            + "):")
        for c in shown:
            lines.append(
                f"  - {c.candidateRefId} (scaledScore={c.scaledScore})")
        foreign.append(
            (f"the {len(candidates)} matched candidate refIds listed above",
             "These are OTHER enrolments' refIds -- the keys the failing "
             "lookup iterates, not this packet's correlation id"))
    else:
        lines.append("No matched candidates were returned by any ABIS instance.")

    lines.append(identifier_block(correlation=correlation, envelope=envelope,
                                  foreign=foreign))
    return "\n".join(lines)


#: `__TypeId__` -> the function that describes that payload. The third and
#: last per-type registry in this module, alongside `REFID_PATHS_BY_TYPE` and
#: `MODELS_BY_TYPE`: between them they are everything a new DLT topic needs.
#: Nothing outside this file should have to change to support one -- if a new
#: structure ever forces a prompt edit, the labelling contract above has a gap
#: and that is the thing to fix, not the prompt.
SUMMARISERS_BY_TYPE = {
    TYPE_ENROLMENT_EVENT_RESPONSE: _summarise_enrolment_event_response,
}


def summarise_payload(payload: Any, type_id: Optional[str] = None) -> Optional[str]:
    """A bounded, human-readable description of the payload, or None.

    Falls back to a key listing for an unmodelled type rather than dumping the
    payload verbatim: an unbounded dump is both a context-budget problem and a
    redaction surface we have not reasoned about.

    The fallback still emits the identifier section. A summary that simply
    stopped after the key listing would leave the Investigator holding a list
    of field names, several of them id-shaped, with nothing saying which (if
    any) is this packet's -- and an unlabelled id next to a silent prompt is
    exactly the shape of the `event_id` mistake this module was written to
    prevent.
    """
    model = parse_payload(payload, type_id)
    summariser = SUMMARISERS_BY_TYPE.get(type_id or "")
    if model is not None and summariser is not None:
        return summariser(model)

    if isinstance(payload, dict):
        keys = list(payload.keys())[:MAX_SUMMARY_KEYS]
        elided = "" if len(payload) <= len(keys) else f" (+{len(payload) - len(keys)} more)"
        unlabelled = identifier_block(unlabelled_reason=(
            f"no model is registered for {type_id or 'this type'}, so no field "
            f"here has been confirmed to be the log-correlation id"))
        return (f"Payload type: {type_id or 'unregistered'} (no model registered; "
                f"top-level keys only)\nKeys: {', '.join(map(str, keys))}{elided}\n"
                f"{unlabelled}")

    if payload is None:
        return None
    return (f"Payload type: {type_id or 'unregistered'} (not a JSON object)\n"
            + identifier_block(unlabelled_reason=(
                "the payload is not a JSON object, so nothing in it has been "
                "read as an identifier")))
