"""Extension to DLT_PLAN.md Phase 8 -- automatic replay on a high-confidence
redrive finding.

DLT_PLAN.md's own non-goals (section 2) rule out remediation for v1: "No
remediation. No replay, no redrive, no writes to any upstream service." This
module is the deliberate, narrow exception to that, added because a real
mechanism exists downstream (a Temporal remediation flow that repairs the DB
inconsistency behind codes like INDEX_MASTER_DATA_NOT_FOUND) that this system
had no way to know about. It stays off by default
(DLT_AUTO_REPLAY_ENABLED=false), and every condition that gates it is named so
a skipped replay is auditable, not silent.

**Two independent switches, not one.** `DLT_AUTO_REPLAY_ENABLED` decides
whether the DLT lane may call `queue_for_replay` at all. Once called, that
tool's own `ENABLE_AUTO_REPLAY` switch (unchanged, shared with the rejection
flow) decides what happens next: `true` posts straight to the OIS replay
endpoint, `false` -- the safer default -- appends to `pending_replays.jsonl`
for a human to approve via `approve_replays.py`. Turning DLT_AUTO_REPLAY_ENABLED
on with ENABLE_AUTO_REPLAY left off means "let the DLT lane nominate packets
for replay, but still make a human press the button" -- the sensible default
posture, and worth reaching for before ever turning both on together.

**Why REDRIVE_AFTER_RECOVERY, and not DATA_FIX_REQUIRED.** DATA_FIX_REQUIRED
is Class A's typical action -- the code says a row is missing, and nothing
about the packet or the environment has changed just because time passed.
Replaying it reproduces the identical dead letter. REDRIVE_AFTER_RECOVERY is
different: DltSynthesisAgent.md instructs the model to choose it specifically
for "a transient or infrastructure fault; replay once the dependency is
healthy" -- which is exactly the shape of a mis-cast finding, where
corroboration showed the declared business exception wasn't the real story and
the logs pointed at something transient instead. That is also the only
finding shape a confidence-gated check can act on in the first place --
canned.py never attaches a confidence to a Class C finding ("no model
produced this"), so a canned redrive recommendation can never clear a
confidence threshold and never triggers this path. Only an LLM-synthesised (or
group-reused) REDRIVE_AFTER_RECOVERY carries a real number to threshold
against.

**Why the default confidence threshold sits at 0.55, not higher.** The only
realistic route to a non-None-confidence REDRIVE_AFTER_RECOVERY is a
CONTRADICTED corroboration verdict (a CORROBORATED one means the business
exception really did happen, which argues against "replay will fix it"), and
CONTRADICTED is capped at DEFAULT_CONTRADICTED_CEILING (0.6) in
dlt_synthesis.py. A threshold at or above that ceiling would make this feature
permanently inert -- no finding could ever clear it. 0.55 sits just under the
ceiling, so a well-evidenced mis-cast conclusion can actually pass while a
weak one still cannot.

**What is still a guess.** `id`/`idType`/`category`/`priority`/`fromSedaStart`
describe the rejection flow's contract with a live OIS endpoint
(OIS_FEIGN_BASE_URL). The rejection flow always passes `id = eventId`; a DLT
case has no eventId, only `ref_id` -- the same correlation id used for log
matching. Whether OIS's replay API accepts a bare refId, and what idType/
category/priority values it expects for a DLT-originated redrive, has not been
confirmed against the real endpoint. Every one of these is therefore
configurable (see .env.example) rather than hardcoded, so a wrong guess is a
config change, not a code change -- and DLT_AUTO_REPLAY_ENABLED stays off
until an operator has actually confirmed them.
"""
import os
from dataclasses import dataclass
from typing import Optional

from src.dlt import code_check as code_check_module
from src.dlt.classify import FailureClass  # noqa: F401  (kept for the module's readers -- see decide())
from src.models.dlt_synthesis import DLT_ACTIONS
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Just under dlt_synthesis.py's DEFAULT_CONTRADICTED_CEILING (0.6) -- see the
#: module docstring for why a higher default would make this feature inert.
DEFAULT_CONFIDENCE_THRESHOLD = 0.55

#: The only action, out of DLT_ACTIONS, whose semantics mean "replay is the
#: fix" today. Extensible via DLT_REPLAY_ACTIONS -- e.g. once a code-specific
#: "known automated remediation" action exists, it belongs on this list too.
DEFAULT_REPLAY_ACTIONS = ("REDRIVE_AFTER_RECOVERY",)

#: All placeholders. See "What is still a guess" above.
DEFAULT_ID_TYPE = "REFID"
DEFAULT_OPERATOR_NAME = "dlt-analysis-auto"
DEFAULT_CATEGORY = "DLT_REDRIVE"
DEFAULT_PRIORITY = 5


def auto_replay_enabled() -> bool:
    return get_bool_env("DLT_AUTO_REPLAY_ENABLED", False)


def confidence_threshold() -> float:
    try:
        return float(os.environ.get("DLT_REPLAY_CONFIDENCE_THRESHOLD",
                                    str(DEFAULT_CONFIDENCE_THRESHOLD)))
    except (ValueError, TypeError):
        return DEFAULT_CONFIDENCE_THRESHOLD


def replay_actions() -> tuple:
    """Action values that qualify as "replay is the fix", extended (not
    replaced) by DLT_REPLAY_ACTIONS -- same override idiom as DLT_CLASS_MAP."""
    raw = os.environ.get("DLT_REPLAY_ACTIONS", "").strip()
    if not raw:
        return DEFAULT_REPLAY_ACTIONS

    configured = tuple(v.strip() for v in raw.split(",") if v.strip())
    unknown = [a for a in configured if a not in DLT_ACTIONS]
    if unknown:
        logger.warning("DLT_REPLAY_ACTIONS names actions the DLT finding "
                       "contract does not have; they can never match",
                       unknown=unknown, valid=DLT_ACTIONS)
    return configured or DEFAULT_REPLAY_ACTIONS


def id_type() -> str:
    return os.environ.get("DLT_REPLAY_ID_TYPE", DEFAULT_ID_TYPE)


def operator_name() -> str:
    return os.environ.get("DLT_REPLAY_OPERATOR_NAME", DEFAULT_OPERATOR_NAME)


def replay_category() -> str:
    return os.environ.get("DLT_REPLAY_CATEGORY", DEFAULT_CATEGORY)


def replay_priority() -> int:
    try:
        return int(os.environ.get("DLT_REPLAY_PRIORITY", str(DEFAULT_PRIORITY)))
    except (ValueError, TypeError):
        return DEFAULT_PRIORITY


def from_seda_start() -> bool:
    return get_bool_env("DLT_REPLAY_FROM_SEDA_START", False)


def code_check_gates_replay() -> bool:
    """Whether a replay-precheck verdict may withhold a replay (phase C6).

    The SECOND of two independent switches, and deliberately not the same one
    as `DLT_CODE_CHECK_ENABLED`. That flag decides whether the lookup happens
    and a verdict is recorded; this one decides whether the verdict is allowed
    to do anything. Running the first alone for a few weeks is what produces
    the evidence for turning this one on.
    """
    return get_bool_env("DLT_CODE_CHECK_GATES_REPLAY", False)


@dataclass(frozen=True)
class ReplayDecision:
    should_replay: bool
    #: Always populated, whether or not should_replay is True -- a skipped
    #: replay must be as auditable as one that fired.
    reason: str


def decide(finding, ref_id: Optional[str], code_check=None) -> ReplayDecision:
    """Whether this finding qualifies for an automatic replay. Pure, no I/O.

    Order matters only for which reason is reported first; every one of these
    is independently sufficient to withhold replay.

    `code_check` is the phase C5 verdict, and it is **a veto only**. It goes
    last, after every existing condition, and it can subtract a replay but
    never add one. That asymmetry is the whole safety argument: a wrong
    verdict can delay a packet, and cannot cause a replay that fails again.
    Letting `FIX_DEPLOYED` *enable* a replay the existing gate declined is a
    real capability -- it is what would finally make Class B replayable -- but
    it is deferred until the verdicts have been checked against reality.
    """
    if not auto_replay_enabled():
        return ReplayDecision(False, "DLT_AUTO_REPLAY_ENABLED is off")

    if finding.action not in replay_actions():
        return ReplayDecision(
            False, f"action {finding.action!r} is not in the replay-worthy "
                   f"set {replay_actions()}")

    if finding.confidence is None:
        return ReplayDecision(
            False, "finding carries no confidence score -- a canned "
                   "treatment produced it, and no model ran to be confident "
                   "about anything")

    threshold = confidence_threshold()
    if finding.confidence < threshold:
        return ReplayDecision(
            False, f"confidence {finding.confidence:.2f} is below "
                   f"DLT_REPLAY_CONFIDENCE_THRESHOLD ({threshold:.2f})")

    if not ref_id:
        return ReplayDecision(
            False, "no refId on this case; nothing to identify the packet "
                   "to OIS with")

    veto = _code_check_veto(code_check)
    if veto:
        return ReplayDecision(False, veto)

    return ReplayDecision(
        True, f"action {finding.action} at confidence {finding.confidence:.2f} "
              f"clears the {threshold:.2f} threshold")


def _code_check_veto(code_check) -> Optional[str]:
    """The reason a verdict withholds this replay, or None.

    Only two verdicts withhold. `FIX_DEPLOYED` says a replay is worth trying,
    which the existing gate had already concluded; `UNKNOWN` says we could not
    establish anything, and must therefore change nothing -- that is what
    keeps a Bitbucket outage, an unmapped package or a disabled flag from
    silently stopping every replay in the system.
    """
    if code_check is None or not code_check_gates_replay():
        return None

    verdict = getattr(code_check, "verdict", None)
    reason = getattr(code_check, "reason", "") or ""

    if verdict == code_check_module.NO_CHANGE:
        return (f"the replay precheck found no change at the failure site: "
                f"{reason}")
    if verdict == code_check_module.NOT_DEPLOYED:
        required = getattr(code_check, "required_version", None)
        return (f"the replay precheck found a change that is not running yet"
                + (f"; replay once the pods reach {required}" if required else "")
                + f": {reason}")
    return None


def attempt(case_id: str, ref_id: str) -> dict:
    """Call queue_for_replay via the same tool the rejection flow's synthesis
    agent uses. Never raises -- a replay-queue failure must not cost the
    casebook that is about to be saved; it must only be visible inside it.
    """
    from src.tools.tool_registry import get_tool_by_name

    args = {
        "id": ref_id,
        "idType": id_type(),
        "priority": replay_priority(),
        "operatorName": operator_name(),
        "category": replay_category(),
        "fromSedaStart": from_seda_start(),
    }

    try:
        tool = get_tool_by_name("queue_for_replay")
        result = tool.invoke(args)
        logger.info("DLT auto-replay queued", case_id=case_id, ref_id=ref_id,
                    result=result)
        return {"queued": True, "result": str(result), "args": args}
    except Exception as e:
        logger.error("DLT auto-replay failed", case_id=case_id, ref_id=ref_id,
                     error=f"{type(e).__name__}: {e}")
        return {"queued": False, "result": f"{type(e).__name__}: {e}",
                "args": args}


def maybe_replay(case_id: str, ref_id: Optional[str], finding,
                 code_check=None) -> dict:
    """The one entry point `/analyze-dlt` calls. Always returns a dict --
    attempted or not, and why either way -- meant to be embedded verbatim in
    the casebook's `replay` block.
    """
    decision = decide(finding, ref_id, code_check)
    if not decision.should_replay:
        return {"attempted": False, "reason": decision.reason,
                "queued": False, "result": None}

    outcome = attempt(case_id, ref_id)
    return {"attempted": True, "reason": decision.reason, **outcome}
