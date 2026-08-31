"""
DLT auto-replay: deciding whether a finding is confident enough to queue a
packet for redrive, and calling the existing `queue_for_replay` tool if so.

Off by default, and every gate is independently sufficient to withhold
replay -- this file's job is to prove each one actually holds, since a wrong
gate here means either a packet that should have redriven sits idle, or one
that should not have is POSTed at a real OIS endpoint.

Four properties carry the module:

* **DLT_AUTO_REPLAY_ENABLED off means nothing else is even evaluated.** The
  master switch, mirroring ENABLE_AUTO_REPLAY's default-off posture.
* **A canned finding can never trigger this**, because canned.py never
  attaches a confidence to a Class B/C/U finding -- `None` fails the
  threshold check by construction, not by a special case.
* **The action must be in the replay-worthy set.** DATA_FIX_REQUIRED (Class
  A's typical action) never qualifies: replaying reproduces a data-not-found
  packet's condition identically, since nothing about the missing row
  changed just because time passed.
* **A queue_for_replay failure never costs the casebook.** `attempt()` is
  exception-shielded and always returns a dict describing what happened.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.dlt import auto_replay
from src.models.dlt_synthesis import DltFinding


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("DLT_AUTO_REPLAY_ENABLED", "DLT_REPLAY_CONFIDENCE_THRESHOLD",
                "DLT_REPLAY_ACTIONS", "DLT_REPLAY_ID_TYPE",
                "DLT_REPLAY_OPERATOR_NAME", "DLT_REPLAY_CATEGORY",
                "DLT_REPLAY_PRIORITY", "DLT_REPLAY_FROM_SEDA_START",
                "DLT_CODE_CHECK_GATES_REPLAY"):
        monkeypatch.delenv(var, raising=False)
    yield


def finding(action="REDRIVE_AFTER_RECOVERY", confidence=0.6):
    return DltFinding(narrative="x", recommendation="y", action=action,
                      confidence=confidence)


# ======================================================================
# The decision gate
# ======================================================================

def test_off_by_default_regardless_of_everything_else():
    decision = auto_replay.decide(finding(), ref_id="REF-1")
    assert decision.should_replay is False
    assert "DLT_AUTO_REPLAY_ENABLED is off" in decision.reason


def test_enabled_and_qualifying_says_yes(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(finding(confidence=0.6), ref_id="REF-1")
    assert decision.should_replay is True


def test_a_non_replay_action_is_declined(monkeypatch):
    """DATA_FIX_REQUIRED is Class A's typical action. Replaying it reproduces
    the same missing-row condition, since nothing about the row changed."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(
        finding(action="DATA_FIX_REQUIRED", confidence=0.9), ref_id="REF-1")
    assert decision.should_replay is False
    assert "not in the replay-worthy set" in decision.reason


@pytest.mark.parametrize("action", ["ROUTE_TO_DEV", "NEEDS_MANUAL_REVIEW", "NO_ACTION"])
def test_every_other_action_is_declined_too(monkeypatch, action):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(finding(action=action, confidence=0.99),
                                  ref_id="REF-1")
    assert decision.should_replay is False


def test_a_canned_findings_none_confidence_is_declined_not_treated_as_zero(monkeypatch):
    """The property that matters: canned.py's Class C REDRIVE_AFTER_RECOVERY
    finding always has confidence=None ("no model produced this"). This must
    read as "cannot evaluate", not as a passing or failing number."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(finding(confidence=None), ref_id="REF-1")
    assert decision.should_replay is False
    assert "no confidence score" in decision.reason


def test_low_confidence_is_declined(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(finding(confidence=0.2), ref_id="REF-1")
    assert decision.should_replay is False
    assert "below" in decision.reason


def test_confidence_exactly_at_the_threshold_passes(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_REPLAY_CONFIDENCE_THRESHOLD", "0.5")
    decision = auto_replay.decide(finding(confidence=0.5), ref_id="REF-1")
    assert decision.should_replay is True


def test_the_default_threshold_sits_under_the_contradicted_ceiling(monkeypatch):
    """0.6 is DEFAULT_CONTRADICTED_CEILING in dlt_synthesis.py -- the only
    realistic path to a non-None REDRIVE_AFTER_RECOVERY. A default threshold
    at or above it would make the whole feature permanently inert."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(finding(confidence=0.6), ref_id="REF-1")
    assert decision.should_replay is True
    assert auto_replay.confidence_threshold() < 0.6


def test_no_ref_id_is_declined(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    decision = auto_replay.decide(finding(confidence=0.9), ref_id=None)
    assert decision.should_replay is False
    assert "refId" in decision.reason


def test_configured_actions_extend_not_replace(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_REPLAY_ACTIONS", "NO_ACTION")
    assert auto_replay.replay_actions() == ("NO_ACTION",)
    # REDRIVE_AFTER_RECOVERY is no longer in the set once configured away --
    # this is override semantics, matching DLT_REFID_KEYS, not additive.
    decision = auto_replay.decide(finding(action="REDRIVE_AFTER_RECOVERY",
                                          confidence=0.9), ref_id="REF-1")
    assert decision.should_replay is False


def test_an_unknown_configured_action_is_warned_but_not_fatal(monkeypatch):
    monkeypatch.setenv("DLT_REPLAY_ACTIONS", "NOT_A_REAL_ACTION")
    assert auto_replay.replay_actions() == ("NOT_A_REAL_ACTION",)


# ======================================================================
# Calling the tool
# ======================================================================

def test_attempt_calls_queue_for_replay_with_ref_id_as_the_id():
    fake_tool = MagicMock()
    fake_tool.invoke.return_value = "Successfully queued packet REF-1"

    with patch("src.tools.tool_registry.get_tool_by_name", return_value=fake_tool):
        result = auto_replay.attempt("dlt-case-1", "REF-1")

    assert result["queued"] is True
    fake_tool.invoke.assert_called_once()
    args = fake_tool.invoke.call_args[0][0]
    assert args["id"] == "REF-1"
    assert set(args.keys()) == {"id", "idType", "priority", "operatorName",
                                "category", "fromSedaStart"}


def test_attempt_never_raises_on_a_tool_failure():
    fake_tool = MagicMock()
    fake_tool.invoke.side_effect = RuntimeError("OIS is down")

    with patch("src.tools.tool_registry.get_tool_by_name", return_value=fake_tool):
        result = auto_replay.attempt("dlt-case-1", "REF-1")

    assert result["queued"] is False
    assert "OIS is down" in result["result"]


def test_attempt_never_raises_when_the_tool_itself_is_missing():
    with patch("src.tools.tool_registry.get_tool_by_name",
              side_effect=ValueError("Tool not found")):
        result = auto_replay.attempt("dlt-case-1", "REF-1")
    assert result["queued"] is False


# ======================================================================
# maybe_replay -- the single entry point
# ======================================================================

def test_maybe_replay_skips_without_calling_the_tool_when_the_gate_declines():
    fake_tool = MagicMock()
    with patch("src.tools.tool_registry.get_tool_by_name", return_value=fake_tool):
        result = auto_replay.maybe_replay("dlt-case-1", "REF-1",
                                          finding(confidence=0.9))
    assert result["attempted"] is False
    assert result["queued"] is False
    assert result["result"] is None
    fake_tool.invoke.assert_not_called()


def test_maybe_replay_attempts_when_the_gate_passes(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    fake_tool = MagicMock()
    fake_tool.invoke.return_value = "queued"

    with patch("src.tools.tool_registry.get_tool_by_name", return_value=fake_tool):
        result = auto_replay.maybe_replay("dlt-case-1", "REF-1",
                                          finding(confidence=0.6))

    assert result["attempted"] is True
    assert result["queued"] is True
    fake_tool.invoke.assert_called_once()


# ======================================================================
# Phase C6 -- the replay precheck as a veto
# ======================================================================

from src.dlt import code_check as CC  # noqa: E402


def verdict(name, **kwargs):
    return CC.CodeCheck(verdict=name, reason="because the repository said so",
                        **kwargs)


def _qualifying(monkeypatch):
    """The state in which `decide` says yes on its own."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")


def test_the_gate_flag_off_leaves_every_verdict_inert(monkeypatch):
    """Two independent switches: recording a verdict must not act on one."""
    _qualifying(monkeypatch)

    for name in CC.VERDICTS:
        assert auto_replay.decide(finding(), "REF-1", verdict(name)).should_replay


def test_no_change_withholds_an_otherwise_qualifying_replay(monkeypatch):
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")

    decision = auto_replay.decide(finding(), "REF-1", verdict(CC.NO_CHANGE))

    assert decision.should_replay is False
    assert "no change at the failure site" in decision.reason
    assert "because the repository said so" in decision.reason


def test_not_deployed_withholds_and_names_the_version_to_wait_for(monkeypatch):
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")

    decision = auto_replay.decide(
        finding(), "REF-1",
        verdict(CC.NOT_DEPLOYED, required_version="1.0.0-release.43"))

    assert decision.should_replay is False
    assert "1.0.0-release.43" in decision.reason


@pytest.mark.parametrize("name", [CC.FIX_DEPLOYED, CC.UNKNOWN])
def test_a_verdict_that_establishes_nothing_changes_nothing(monkeypatch, name):
    """UNKNOWN is the whole safety story: a Bitbucket outage, an unmapped
    package or a disabled flag must not silently stop every replay."""
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")

    assert auto_replay.decide(finding(), "REF-1", verdict(name)).should_replay


def test_no_verdict_at_all_changes_nothing(monkeypatch):
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")

    assert auto_replay.decide(finding(), "REF-1", None).should_replay


def test_the_veto_can_only_subtract_a_replay_never_add_one(monkeypatch):
    """FIX_DEPLOYED must not rescue a finding the existing gate declined.
    Enabling that is what would make Class B replayable, and it is deferred
    until the verdicts have been checked against reality."""
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")

    for declined in (finding(action="DATA_FIX_REQUIRED"),
                     finding(confidence=None),
                     finding(confidence=0.1)):
        decision = auto_replay.decide(declined, "REF-1", verdict(CC.FIX_DEPLOYED))
        assert decision.should_replay is False


def test_the_existing_conditions_still_report_first(monkeypatch):
    """The veto goes last, so a replay declined for its own reasons still
    says why rather than blaming the precheck."""
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")

    decision = auto_replay.decide(finding(action="DATA_FIX_REQUIRED"), "REF-1",
                                  verdict(CC.NO_CHANGE))

    assert "not in the replay-worthy set" in decision.reason
    assert "precheck" not in decision.reason


def test_maybe_replay_withholds_without_calling_the_tool(monkeypatch):
    _qualifying(monkeypatch)
    monkeypatch.setenv("DLT_CODE_CHECK_GATES_REPLAY", "true")
    fake_tool = MagicMock()

    with patch("src.tools.tool_registry.get_tool_by_name", return_value=fake_tool):
        result = auto_replay.maybe_replay("case-1", "REF-1", finding(),
                                          verdict(CC.NO_CHANGE))

    assert result["attempted"] is False
    assert result["queued"] is False
    assert "no change at the failure site" in result["reason"]
    fake_tool.invoke.assert_not_called()
