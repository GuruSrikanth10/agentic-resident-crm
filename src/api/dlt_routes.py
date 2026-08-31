"""Phase 5 of DLT_PLAN.md -- the DLT fetch endpoint.

`POST /fetch-dlt-logs` is the DLT flow's equivalent of `/fetch-logs`: bounded
I/O, no LLM. It parses the record, persists the evidence, fetches whatever pod
logs cover the failing attempt, and hands the case to the analysis queue.

Two ordering rules matter here:

* **Evidence is persisted before analysis, always.** `headers.json` and
  `trace.txt` are written verbatim first, so a parser bug is recoverable
  without re-consuming Kafka -- by which time the record may have aged out of
  the topic's retention.

* **Redaction runs before any persistence.** A stacktrace message can carry a
  UID, and this text lands on disk and possibly in S3. The `refId` is
  allowlisted so it survives -- it is an operational correlation id, and
  scrubbing it would destroy the investigation.
"""
import asyncio
import concurrent.futures
import functools
import json
import os
import time
from typing import Optional

from fastapi import APIRouter, Depends

from src.api.routes import _off_loop, get_api_key, rate_limiter, register_executor
from src.dlt import (
    auto_replay,
    canned,
    code_check,
    deployed,
    groups,
    orchestrator,
    registry,
    reuse,
)
from src.dlt.case_storage import get_dlt_storage
from src.dlt.corroborate import corroborate
from src.dlt.classify import classify
from src.dlt.headers import parse_headers
from src.dlt.stacktrace import (
    build_signature,
    compute_fingerprint,
    normalise_frame_locations,
    normalise_frames,
    parse_stacktrace,
)
from src.dlt.window import derive_window
from src.log_pipeline import redaction
from src.log_pipeline.pipeline import reduce_logs
from src.models.dlt_payload_schemas import summarise_payload
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy
from src.storage.base import (
    LOGS_FETCHED_STATUS,
    PROTECTED_TERMINAL_STATUSES,
    TERMINAL_STATUSES,
)
from src.utils import metrics
from src.utils.analysis_queue_publisher import publish_to_dlt_analysis_queue
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

# A dedicated, bounded pool for the DLT analysis graph -- a SIBLING of
# routes._agent_invoke_executor, not the same one, so a DLT backlog cannot
# starve the rejection lane and vice versa.
#
# `/analyze-dlt` used to be a sync `def`, which meant a multi-minute LLM
# investigation occupied one of anyio's 40 default threadpool slots -- the
# same pool /health, /ready, /fetch-logs and /fetch-dlt-logs are dispatched
# on. routes.py documents at length why the rejection lane does not do that;
# this lane simply had not been given the same treatment.
_MAX_CONCURRENT_DLT_ANALYSES = int(
    os.environ.get("MAX_CONCURRENT_DLT_ANALYSES",
                   os.environ.get("MAX_CONCURRENT_INVESTIGATIONS", "5")))
_dlt_invoke_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=_MAX_CONCURRENT_DLT_ANALYSES, thread_name_prefix="dlt-analyze"
)
# So the API's shutdown drain tears this pool down along with the rejection
# lane's, rather than leaving its threads for SIGKILL. A getter, so swapping
# this module's attribute substitutes the pool that actually gets shut down.
register_executor(lambda: _dlt_invoke_executor)


def _dlt_analyze_timeout_seconds() -> float:
    """Server-side budget for one DLT analysis.

    The DLT analysis consumer gives up after DLT_ANALYSIS_TIMEOUT_SECONDS
    (default 300s) and writes FAILED_TIMEOUT itself. Without a budget on this
    side, the API thread kept running an investigation nobody was waiting for
    and could later overwrite that verdict with a "successful" casebook -- bug
    0.8, which the rejection lane fixed and this one inherited unfixed.

    Read at call time, like the rejection lane's equivalent, so it tracks a
    reconfigured consumer timeout.
    """
    consumer_budget = float(os.environ.get("DLT_ANALYSIS_TIMEOUT_SECONDS", "300"))
    default_budget = max(consumer_budget - 30, 30)
    return float(os.environ.get("DLT_ANALYZE_TIMEOUT_SECONDS", default_budget))


def _timeout_casebook(case_id: str, ref_id: Optional[str], budget: float) -> dict:
    """Terminal record for an analysis that outran its server-side budget."""
    return {
        "schema_version": DLT_CASEBOOK_SCHEMA_VERSION,
        "case_id": case_id,
        "detected_at": time.time(),
        "packet": {"ref_id": ref_id},
        "finding": {
            "narrative": (
                f"The DLT analysis exceeded the server-side budget of "
                f"{budget}s and was abandoned. The stack trace, headers and "
                f"any fetched logs are attached; no finding was produced."
            ),
            "recommendation": "A human should read the attached evidence.",
            "action": "NEEDS_MANUAL_REVIEW",
        },
        "packet_status": {"status": "FAILED_TIMEOUT"},
    }

#: Artifact names written per case. Kept here so Phase 8 and the operator CLI
#: read them under one set of names.
HEADERS_ARTIFACT = "headers.json"
TRACE_ARTIFACT = "trace.txt"
PARSED_TRACE_ARTIFACT = "parsed_trace.json"
PAYLOAD_SUMMARY_ARTIFACT = "payload_summary.txt"
FETCHED_LOGS_ARTIFACT = "fetched_logs.txt"
DEPLOYED_ARTIFACT = "deployed.json"

#: Bumped when the DLT casebook shape changes. Independent of the rejection
#: casebook's CASEBOOK_SCHEMA_VERSION -- different schema, different lifecycle.
# 1.1 adds the `code_check` block (DLT_PLAN.md 14, phase C5).
DLT_CASEBOOK_SCHEMA_VERSION = "1.1"


def build_failure(headers, exception_message: Optional[str]) -> dict:
    """Parse, classify and fingerprint one record's failure.

    Shared with `/analyze-dlt` so both stages derive identical values from the
    same bytes -- the parse is pure, so re-deriving it is cheaper and safer
    than trusting a summary carried across a topic.
    """
    trace = parse_stacktrace(headers.stacktrace)
    # `registry.class_for` lets a code declared TECHNICAL_EXCEPTION at source
    # be classified C even though it arrived wrapped in a BusinessException.
    # The lookup is cached on the catalog's mtime, so this costs one `stat`.
    result = classify(trace, exception_message, code_class=registry.class_for)
    frames = normalise_frames(trace.root_frames)
    # Index-parallel with `frames`, under the same filter -- but deliberately
    # NOT an input to the fingerprint. See FrameLocation and Risk R3.
    locations = normalise_frame_locations(trace.root_frames, trace.root_locations)
    root_fqcn = trace.root.fqcn if trace.root else None
    code = result.business_code or ""
    entry = registry.lookup_entry(result.business_code)

    return {
        "failure_class": result.failure_class.value,
        "class_reason": result.reason,
        "root_fqcn": root_fqcn,
        "root_message": trace.root.message if trace.root else "",
        "business_code": result.business_code,
        "registry_description": entry.description if entry else None,
        "registry_category": entry.category if entry else None,
        # "declared" or "inferred". A category the Java source stated and one
        # derived from a numeric id range are not the same evidence, and a
        # casebook that could not tell them apart would hide that.
        "registry_category_source": entry.category_source if entry else None,
        "registry_stage": entry.stage if entry else None,
        "fingerprint": compute_fingerprint(root_fqcn, frames, code),
        "signature": build_signature(root_fqcn, frames, code),
        "frames": list(frames),
        # Parallel to `frames`, carrying the file and line the fingerprint
        # must not see. A source lookup (phase C4) reads these; nothing else
        # in the DLT lane does.
        "locations": [location.as_dict() for location in locations],
        "truncated": trace.truncated,
        "chain": [
            {"fqcn": link.fqcn, "message": link.message, "frames": list(link.frames)}
            for link in trace.chain
        ],
    }


def _persist_evidence(storage, case_id: str, message: DltMessage,
                      failure: dict, allowlist: list) -> None:
    """Write the verbatim record and the parsed failure, redacted."""
    redacted_headers = {
        name: (redaction.redact_text(value, allowlist=allowlist).text
               if isinstance(value, str) else value)
        for name, value in (message.headers or {}).items()
    }
    storage.save_artifact(case_id, HEADERS_ARTIFACT,
                          json.dumps(redacted_headers, indent=2, ensure_ascii=False))

    raw_trace = (message.headers or {}).get("kafka_exception-stacktrace") or ""
    storage.save_artifact(case_id, TRACE_ARTIFACT,
                          redaction.redact_text(raw_trace, allowlist=allowlist).text)

    storage.save_artifact(
        case_id, PARSED_TRACE_ARTIFACT,
        redaction.redact_text(json.dumps(failure, indent=2, ensure_ascii=False),
                              allowlist=allowlist).text)

    # The payload used to be read for one identifier and then dropped. It is
    # evidence: this sample's trace fails inside
    # `filterCandidatesAndBuildRefIdUidMap -> getIndexMasterData`, and the
    # candidates that loop iterates are in the payload. Summarised rather than
    # dumped -- a bounded description is both a context budget and a redaction
    # surface we have actually reasoned about.
    summary = summarise_payload(message.payload,
                                (message.headers or {}).get("__TypeId__"))
    if summary:
        storage.save_artifact(
            case_id, PAYLOAD_SUMMARY_ARTIFACT,
            redaction.redact_text(summary, allowlist=allowlist).text)


@router.post("/fetch-dlt-logs", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
def fetch_dlt_logs(message: DltMessage):
    """Endpoint the DLT consumer forwards dead-lettered records to.

    Deliberately NOT async, for the same reason as `/fetch-logs`: this is
    bounded I/O, not a multi-minute LLM call, so it belongs on Starlette's
    sync-dispatch threadpool.
    """
    case_id = message.case_id
    log = logger.bind(case_id=case_id, ref_id=message.ref_id)
    storage = get_dlt_storage()

    recorded_status = storage.terminal_status(case_id)
    if recorded_status in TERMINAL_STATUSES:
        log.info("Skipping fetch; a terminal DLT case already exists",
                 recorded_status=recorded_status)
        return {"status": "already_processed", "case_id": case_id}

    headers = parse_headers(message.headers)
    failure = build_failure(headers, headers.exception_message)
    allowlist = [v for v in (message.ref_id, case_id) if v]

    _persist_evidence(storage, case_id, message, failure, allowlist)
    metrics.record_dlt_case(failure["failure_class"])

    gaps = []
    window = derive_window(headers)

    # Two identifiers that should have been equal were not. The key still
    # wins (see resolve_ref_id), but the log lane may now be searching for the
    # wrong packet, so the finding must not be read as if it were clean.
    if message.ref_id_mismatch:
        gaps.append("REFID_KEY_PAYLOAD_MISMATCH")
        log.warning("Record key and payload disagreed on the refId",
                    record_key=message.record_key)

    if storage.artifact_exists(case_id, FETCHED_LOGS_ARTIFACT):
        log.info("Logs already fetched; reusing the persisted artifact")
    elif not message.ref_id:
        # Header-only is a valid outcome, not a failure. The stacktrace is
        # still the primary evidence; we simply cannot corroborate it.
        gaps.append("NO_CORRELATION_ID")
        log.warning("No refId on the payload; skipping the log fetch")
        storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                              "No refId available; logs were not fetched.")
    elif window is None:
        gaps.append("NO_TIMESTAMP")
        log.warning("No usable timestamp in the headers; skipping the log fetch")
        storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                              "No usable timestamp; logs were not fetched.")
    elif window.too_old:
        # A fetch certain to return nothing still costs a full Kubernetes
        # fan-out across every pod in the namespace.
        gaps.append("LOGS_TOO_OLD")
        log.warning("Log window is older than DLT_MAX_LOG_AGE_SECONDS; skipping the fetch",
                    window=window.describe())
        storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                              f"Log window too old to fetch: {window.describe()}")
    else:
        log.info("Fetching logs for the DLT case", window=window.describe())
        metrics.record_dlt_window_age(window.age_seconds)
        try:
            formatted = reduce_logs(
                # Search on refId -- the only identifier the service logs --
                # but persist under case_id (DLT_PLAN.md 5.5).
                message.ref_id,
                extra_identifiers=(case_id,),
                storage_key=case_id,
                window=window.to_time_window(),
                storage=storage,
            )
            storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT, formatted)
        except Exception as e:
            # The stacktrace is already persisted, so a log-fetch failure
            # degrades the case rather than losing it.
            gaps.append("LOG_FETCH_FAILED")
            log.error("Log fetch failed; continuing header-only",
                      error=f"{type(e).__name__}: {e}")
            storage.save_artifact(case_id, FETCHED_LOGS_ARTIFACT,
                                  f"Log fetch failed: {type(e).__name__}: {e}")

    # Which build was running when this packet failed. Recorded here, in the
    # fast lane, because it is an observation about *this moment* -- by the
    # time the analysis lane runs, or a parked replay is reconsidered days
    # later, the pods may be on something else entirely (DLT_PLAN.md 14, C1).
    #
    # Never gated on a feature flag: it answers Open Question 3 and mitigates
    # Risk R4 on its own, independently of the code check that consumes it.
    baseline = deployed.running_version()
    metrics.record_dlt_deployed_version_read(baseline.ok, baseline.mixed)
    if baseline.ok:
        log.info("Recorded the running build for this case",
                 versions=list(baseline.versions), mixed=baseline.mixed)
    storage.save_artifact(case_id, DEPLOYED_ARTIFACT,
                          json.dumps(baseline.as_dict(), indent=2, ensure_ascii=False))

    existing = storage.load(case_id, filename="status.json")
    existing_value = (existing or {}).get("packet_status", {}).get("status")
    if existing_value in (None, LOGS_FETCHED_STATUS):
        storage.save(case_id, {
            "packet_metadata": {"eid": case_id, "ref_id": message.ref_id,
                                "started_at": time.time()},
            "packet_status": {"status": LOGS_FETCHED_STATUS},
        }, filename="status.json")

    queued = message.model_dump()
    queued["evidence_gaps"] = gaps
    queued["log_window"] = window.describe() if window else None
    # Carried for visibility on the queue; the analysis lane reads the
    # artifact rather than trusting this copy, on the same reasoning that
    # makes it re-derive the failure from the headers.
    queued["baseline_versions"] = list(baseline.versions)
    publish_to_dlt_analysis_queue(queued)

    log.info("Queued DLT case for analysis", state=LOGS_FETCHED_STATUS,
             failure_class=failure["failure_class"], gaps=gaps)
    return {"status": "queued_for_analysis", "case_id": case_id,
            "failure_class": failure["failure_class"], "gaps": gaps,
            "baseline_versions": list(baseline.versions)}


# ---------------------------------------------------------------------------
# Phase 8 -- the analysis lane
# ---------------------------------------------------------------------------

def _recorded_baseline(storage, case_id: str) -> tuple:
    """The versions the fast lane observed running when this packet failed.

    Read back from `deployed.json` rather than trusted from the queue: this is
    an observation about a moment that has passed, and by the time the
    analysis lane runs the pods may be on something else. An absent or
    unreadable artifact yields an empty tuple, which C5 reports as a missing
    baseline rather than substituting today's version for it.
    """
    try:
        raw = storage.load_artifact(case_id, DEPLOYED_ARTIFACT)
        if not raw:
            return ()
        return tuple(json.loads(raw).get("versions") or ())
    except Exception as e:
        logger.warning("Could not read the recorded baseline version",
                       case_id=case_id, error=f"{type(e).__name__}: {e}")
        return ()


def _casebook(message: DltMessage, headers, failure: dict, corroboration,
              finding, decision, group: Optional[dict], gaps: list,
              window_description: Optional[str], provenance_source: str,
              replay: Optional[dict] = None,
              code_check_result=None) -> dict:
    """Assemble the terminal casebook. See DLT_PLAN.md 7.1."""
    return {
        "schema_version": DLT_CASEBOOK_SCHEMA_VERSION,
        "case_id": message.case_id,
        "detected_at": time.time(),
        "source": {
            "original_topic": headers.original_topic,
            "partition": headers.original_partition,
            "offset": headers.original_offset,
            "consumer_group": headers.consumer_group,
            "attempts": headers.attempts,
            "original_timestamp": headers.original_timestamp_ms,
            "last_attempt_timestamp": headers.last_attempt_ms,
            "anchor_is_fallback": headers.anchor_is_fallback,
            "type_id": headers.type_id,
        },
        "packet": {
            "ref_id": message.ref_id,
            "ref_id_source": message.ref_id_source,
            "record_key": message.record_key,
            # Only set when the two sources disagreed; a null here means they
            # agreed or only one of them spoke, not that the check was skipped.
            "payload_ref_id_conflict": (message.payload_ref_id
                                        if message.ref_id_mismatch else None),
        },
        "failure": {
            "class": failure["failure_class"],
            "class_reason": failure["class_reason"],
            "root_fqcn": failure["root_fqcn"],
            "business_code": failure["business_code"],
            "registry_description": failure["registry_description"],
            "registry_category": failure.get("registry_category"),
            "registry_category_source": failure.get("registry_category_source"),
            "registry_stage": failure.get("registry_stage"),
            "signature": failure["signature"],
            "fingerprint": failure["fingerprint"],
            "frames": failure["frames"],
            "truncated": failure["truncated"],
        },
        "evidence": {
            "corroboration": corroboration.verdict.value,
            "corroboration_reason": corroboration.reason,
            "citations": list(corroboration.citations),
            "unexplained_exceptions": list(corroboration.unexplained),
            "could_not_look": corroboration.could_not_look,
            "log_window": window_description,
            "gaps": gaps,
        },
        "finding": {
            "narrative": finding.narrative,
            "discrepancy": finding.discrepancy,
            "recommendation": finding.recommendation,
            "action": finding.action,
        },
        "confidence": {
            "score": finding.confidence,
            "ceilings_applied": finding.ceilings_applied,
            "abstained": finding.abstained,
        },
        "provenance": {
            "source": provenance_source,
            "reuse_decision": decision.decision.value,
            "reuse_reason": decision.reason,
            "group_fingerprint": failure["fingerprint"],
            "group_occurrences": (group or {}).get("occurrence_count"),
            "recommendation_state": groups.STATE_DRAFT,
        },
        # The replay precheck's verdict (DLT_PLAN.md 14). Always present, and
        # always UNKNOWN when the feature is off -- "we did not look" and "we
        # looked and found nothing" must not read alike here either.
        #
        # Kept separate from `finding` on purpose: this says whether the code
        # at the failure site changed and whether that change is running. It
        # does NOT say the change fixes this bug, and merging it into the
        # narrative would blur a claim the evidence does not support.
        "code_check": (code_check_result or code_check.CodeCheck()).as_dict(),
        # See src/dlt/auto_replay.py for the gate. `replay` is always present
        # once analysis has run -- "not attempted, and here is why" is exactly
        # as much a part of the casebook as "attempted, and here is what
        # happened", so a human reading this later never has to guess whether
        # replay was even considered.
        "replay": replay or {"attempted": False,
                              "reason": "replay gate not evaluated",
                              "queued": False, "result": None},
        "packet_status": {"status": _terminal_status(finding)},
    }


def _terminal_status(finding) -> str:
    """Map an action onto a terminal status the storage layer recognises."""
    if finding.action == "NO_ACTION":
        return "COMPLETED"
    return "NEEDS_MANUAL_REVIEW"


@router.post("/analyze-dlt", dependencies=[Depends(get_api_key), Depends(rate_limiter)])
async def analyze_dlt(message: DltMessage):
    """Endpoint the DLT analysis consumer forwards fetched cases to.

    The reuse policy decides whether this costs an LLM call. Logs and
    corroboration run either way -- never serve a cached recommendation blind
    (DLT_PLAN.md 5.7), because that would disable the mis-cast detector on
    exactly the occurrences worth catching.

    `async def`, on the same reasoning as /process-rejection: the LLM lane is
    minutes long, so it goes to a bounded executor under a server-side budget
    rather than occupying a slot in Starlette's shared sync-dispatch pool for
    its whole duration.
    """
    case_id = message.case_id
    log = logger.bind(case_id=case_id, ref_id=message.ref_id)
    storage = get_dlt_storage()

    recorded_status = await _off_loop(storage.terminal_status, case_id)
    if recorded_status in TERMINAL_STATUSES:
        log.info("Skipping analysis; a terminal DLT case already exists",
                 recorded_status=recorded_status)
        return {"status": "already_processed", "case_id": case_id}

    headers = parse_headers(message.headers)
    failure = build_failure(headers, headers.exception_message)
    fingerprint = failure["fingerprint"]

    logs = await _off_loop(storage.load_artifact, case_id, FETCHED_LOGS_ARTIFACT) or ""
    # Re-derived from the payload on the queue message rather than read back
    # from the artifact, for the same reason `build_failure` re-parses the
    # trace: the derivation is pure, so recomputing it cannot drift, while a
    # stored copy can.
    payload_summary = summarise_payload(
        message.payload, (message.headers or {}).get("__TypeId__"))
    corroboration = corroborate(logs, failure["root_fqcn"],
                                failure["business_code"], failure["frames"])
    metrics.record_dlt_corroboration(corroboration.verdict.value)

    if failure["failure_class"] == "A" and not failure["registry_description"]:
        metrics.record_dlt_registry_miss()

    # Record the occurrence BEFORE deciding, so a canned finding's "N
    # occurrences" count includes the case it is describing. Recording only
    # touches counts and history, never `recommendation`, so the reuse
    # decision below sees exactly the same cache state either way.
    group = await _off_loop(
        groups.record_occurrence,
        fingerprint, case_id,
        signature=failure["signature"],
        failure_class=failure["failure_class"],
        business_code=failure["business_code"],
        corroboration=corroboration.verdict.value,
    )
    decision = reuse.decide(failure["failure_class"],
                            corroboration.verdict.value, group)
    metrics.record_dlt_reuse(decision.decision.value)

    # The replay precheck. Bounded I/O with no LLM, so it belongs beside
    # corroboration rather than in the fast lane -- this is where the replay
    # gate lives and where the group record it feeds is already loaded.
    #
    # `baseline` comes from the artifact rather than the queue message, for
    # the same reason `build_failure` re-parses the trace: the artifact is
    # what the fast lane actually observed, and a queue field can be dropped
    # by a schema change without anyone noticing.
    baseline_versions = await _off_loop(_recorded_baseline, storage, case_id)
    running = await _off_loop(deployed.running_version)
    code_check_result = await _off_loop(
        code_check.evaluate, failure, headers.last_attempt_ms,
        baseline_versions, running.versions)
    metrics.record_dlt_code_check(code_check_result.verdict)
    if code_check_result.verdict != code_check.UNKNOWN:
        log.info("Replay precheck", verdict=code_check_result.verdict,
                 reason=code_check_result.reason)
        group = await _off_loop(groups.attach_code_check, fingerprint,
                                code_check_result.as_dict())

    parse_error = None
    if decision.decision is reuse.Decision.CANNED:
        finding = canned.build(failure["failure_class"], failure, corroboration, group)
        provenance = "canned"
    elif decision.decision is reuse.Decision.REUSE_GROUP:
        finding = DltFinding(**(group or {})["recommendation"])
        provenance = "group_reuse"
    else:
        log.info("Running the DLT analysis lane", reason=decision.reason)
        budget = _dlt_analyze_timeout_seconds()
        invoke = functools.partial(
            orchestrator.investigate, case_id, failure, corroboration, logs,
            payload_summary=payload_summary)
        try:
            finding, parse_error = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    _dlt_invoke_executor, invoke),
                timeout=budget,
            )
        except asyncio.TimeoutError:
            # The consumer's own client-side budget is about to fire (or has
            # already), and it will write FAILED_TIMEOUT and DLQ the message.
            # Recording the same verdict here keeps the two in agreement
            # instead of leaving this side to finish later and overwrite it.
            log.error("DLT analysis exceeded the server-side budget",
                      timeout_seconds=budget, state="FAILED_TIMEOUT")
            await _off_loop(storage.save_terminal, case_id,
                            _timeout_casebook(case_id, message.ref_id, budget))
            return {"status": "failed_timeout", "case_id": case_id}
        provenance = "agent"
        if finding is None:
            finding = DltFinding(
                narrative="The analysis produced output that does not satisfy "
                          "the finding contract, even after a repair attempt. "
                          "The verbatim stack trace and logs are attached.",
                recommendation="A human should read the attached evidence.",
                action="NEEDS_MANUAL_REVIEW",
                confidence=0.0,
            )
            provenance = "failed_synthesis"

    finding = apply_dlt_confidence_policy(
        finding,
        failure_class=failure["failure_class"],
        corroboration=corroboration.verdict.value,
        registry_hit=bool(failure["registry_description"]),
        reused=decision.decision is reuse.Decision.REUSE_GROUP,
        logs=logs,
    )

    # Only an agent run produces a recommendation worth caching. A canned
    # treatment is recomputed identically every time, and re-storing a reused
    # one would just rewrite what is already there.
    if provenance == "agent":
        group = await _off_loop(groups.attach_recommendation, fingerprint,
                                finding.model_dump(), state=groups.STATE_DRAFT)

    # Evaluated on the FINAL finding -- after ceilings, after reuse decay --
    # so a confidence the ceilings already capped is what gets checked, never
    # the model's raw, uncapped number.
    # May POST to the OIS replay endpoint or append to the pending queue --
    # network or filesystem either way.
    replay = await _off_loop(auto_replay.maybe_replay, case_id, message.ref_id,
                             finding)
    metrics.record_dlt_auto_replay(
        "queued" if replay["queued"] else
        "failed" if replay["attempted"] else "not_attempted")
    if replay["attempted"]:
        log.info("DLT auto-replay evaluated", queued=replay["queued"],
                 reason=replay["reason"])

    casebook = _casebook(message, headers, failure, corroboration, finding,
                         decision, group, message.model_dump().get("evidence_gaps") or [],
                         message.model_dump().get("log_window"), provenance,
                         replay=replay, code_check_result=code_check_result)
    if parse_error:
        casebook["finding"]["parse_error"] = parse_error

    # The late-result guard, matching routes.py. The terminal check at the top
    # of this function ran before a multi-minute investigation; by now the DLT
    # analysis consumer's own client-side timeout may have fired and written
    # FAILED_TIMEOUT while DLQ-ing the message. Overwriting that with a
    # "successful" casebook leaves the verdict and the queued DLQ record
    # disagreeing about what happened (0.8 / F4).
    # Both files, for the reason spelled out at the matching guard in
    # routes.py: save_terminal writes casebook.json first, so a writer that
    # died between its two writes is visible only in casebook.json.
    recorded_status = await _off_loop(storage.terminal_status, case_id)
    if recorded_status in PROTECTED_TERMINAL_STATUSES:
        log.warning("Discarding late DLT result; a terminal status was already "
                    "recorded by another actor", recorded_status=recorded_status)
        return {"status": "already_processed", "case_id": case_id}

    await _off_loop(storage.save_terminal, case_id, casebook)

    log.info("DLT case analysed", failure_class=failure["failure_class"],
             corroboration=corroboration.verdict.value,
             decision=decision.decision.value, action=finding.action,
             confidence=finding.confidence)

    return {"status": "processed", "case_id": case_id,
            "action": finding.action, "confidence": finding.confidence,
            "corroboration": corroboration.verdict.value,
            "decision": decision.decision.value,
            "code_check": code_check_result.verdict,
            "replay_attempted": replay["attempted"],
            "replay_queued": replay["queued"]}
