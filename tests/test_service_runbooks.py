"""
Runbooks and learned rules per service (MULTI_SERVICE_PLAN.md Phase 5, D11).

A runbook is a stored answer for one service's reason code. It is kept under
that service, checked against what it was derived from -- the rules-table
rule, or for a service without a rules table its documentation entries --
and cleared to serve per `service:CODE`. A learned rule goes to the pack it
was learned under unless it is marked generic.
"""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
from src.tools import build_runbooks, promote_rules
from src.utils import paths, reason_code_docs as rcd, runbook_store as rs
from src.utils import service_registry as sr

BIO = "enu-biometric"
NONE_SVC = "svc-none"
CODE = "NONE_SVC_CODE"

_RESOLUTION = {"rejection_description": "d", "synthesis": "s",
               "action": "REPLAY", "resident_action": "NEW_PACKET"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _doc_file(docs, stem, code, description, extra_codes=()):
    """One service's documentation: a code entry and a rule for it."""
    (docs / f"{stem}.json").write_text(json.dumps({
        "schema_version": 1, "service": stem,
        "codes": [{"numeric_code": 7, "reason_code": code,
                   "description": description, "category": "Technical",
                   "is_retryable": True},
                  *extra_codes],
        "rules": {"total_rules": 1, "rules": [{
            "rule_id": "r-1", "module": "Checker", "reject_reason_code": code,
            "condition_description": "the enrolment type is 'UPDATE'",
            "description": "When the enrolment type is 'UPDATE'. The packet is "
                           "rejected."}]},
    }), encoding="utf-8")


@pytest.fixture
def estate(tmp_path, monkeypatch):
    """enu-biometric with a rules table, svc-none without one, each with its
    documentation, and empty runbook directories."""
    packs = tmp_path / "packs"
    documents = (
        ({"schema_version": 1, "service": sr.DEFAULT_PACK, "display_name": "Default",
          "match": {}, "rule_source": {"type": "none"}}, "No service policy applies."),
        ({"schema_version": 1, "service": BIO, "display_name": "Bio",
          "tool_prefix": "bio",
          "match": {"stages": ["Biometric"], "sub_stages": [], "source_topics": []},
          "rule_source": {"type": "rules_db"}}, "The biometric policy."),
        ({"schema_version": 1, "service": NONE_SVC, "display_name": "None",
          "tool_prefix": "none",
          "match": {"stages": ["Nowhere"], "sub_stages": [], "source_topics": []},
          "enrolment_types": {
              "payload": {"U": {"family": "U", "label": "An update"}},
              "family_labels": {"U": "Update (U)"},
              "doc_aliases": {"UPDATE": "U"}},
          "rule_source": {"type": "none"}}, "The policy of svc-none."))
    for document, policy in documents:
        directory = packs / document["service"]
        directory.mkdir(parents=True)
        (directory / "service.json").write_text(json.dumps(document), encoding="utf-8")
        (directory / "policy.md").write_text(policy, encoding="utf-8")

    docs = tmp_path / "docs" / "services"
    docs.mkdir(parents=True)
    _doc_file(docs, BIO, "BIO_CODE", "Raised by the biometric service.")
    _doc_file(docs, NONE_SVC, CODE, "Raised when the packet is unreadable.")

    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", packs)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", tmp_path / "docs")
    monkeypatch.setattr(rs, "RUNBOOK_FINAL_DIR", tmp_path / "runbooks" / "final")
    monkeypatch.setattr(rs, "RUNBOOK_DRAFT_DIR", tmp_path / "runbooks" / "draft")
    monkeypatch.setenv("REJECTION_SERVICES_ENABLED", f"{BIO},{NONE_SVC}")
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    rs._runbook_cache.clear()
    yield tmp_path
    rs._runbook_cache.clear()


def _write(directory: Path, service: str, name: str, data: dict) -> Path:
    path = directory / service / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _final(service, reason_code=CODE, etype="U", binding=None, **overrides):
    data = {"schema_version": rs.SCHEMA_VERSION, "service": service,
            "runbook_id": f"{reason_code}__{etype}", "reason_code": reason_code,
            "enrolment_type": etype, "status": "final", "version": 1,
            "binding": binding or {"type": rs.BINDING_REASON_CODE_DOC,
                                   "fingerprint": "sha256:x"},
            "resolution": dict(_RESOLUTION)}
    data.update(overrides)
    return data


def _doc_binding(service=NONE_SVC, reason_code=CODE, etype="U"):
    state = rcd.lookup(reason_code, etype, **sr.docs_lookup_options(service))
    return {"type": rs.BINDING_REASON_CODE_DOC,
            "fingerprint": rs.doc_binding_fingerprint(state)}


# ---------------------------------------------------------------------------
# The store: one directory per service.
# ---------------------------------------------------------------------------

def test_a_runbook_is_found_under_its_own_service_only(estate):
    _write(rs.RUNBOOK_FINAL_DIR, "svc-a", "CODE__E.json",
           _final("svc-a", "CODE", "E"))

    assert rs.get_runbook("svc-a", "CODE", "E")["service"] == "svc-a"
    assert rs.get_runbook("svc-b", "CODE", "E") is None


def test_the_any_fallback_stays_within_one_service(estate):
    _write(rs.RUNBOOK_FINAL_DIR, "svc-a", "CODE__ANY.json",
           _final("svc-a", "CODE", "ANY"))
    _write(rs.RUNBOOK_FINAL_DIR, "svc-b", "OTHER__ANY.json",
           _final("svc-b", "OTHER", "ANY"))

    assert rs.get_runbook("svc-a", "CODE", "U")["enrolment_type"] == "ANY"
    assert rs.get_runbook("svc-a", "OTHER", "U") is None, \
        "another service's ANY runbook is not a fallback"


def test_a_1_1_runbook_in_the_biometric_directory_still_loads(estate):
    legacy = {"schema_version": "1.1", "runbook_id": "CODE__E", "reason_code": "CODE",
              "enrolment_type": "E", "status": "final", "version": 2,
              "rule_fingerprint": "sha256:rule", "resolution": dict(_RESOLUTION)}
    _write(rs.RUNBOOK_FINAL_DIR, BIO, "CODE__E.json", legacy)
    _write(rs.RUNBOOK_FINAL_DIR, "svc-a", "CODE__E.json", legacy)

    found = rs.get_runbook(BIO, "CODE", "E")
    assert found["version"] == 2
    assert rs.binding_of(found) == {"type": rs.BINDING_DB_RULE,
                                    "fingerprint": "sha256:rule"}
    assert rs.get_runbook("svc-a", "CODE", "E") is None, \
        "a runbook written before services is the pre-registry service's"


@pytest.mark.parametrize("overrides", [
    {"service": "svc-b"},
    {"binding": {"type": "something_else", "fingerprint": "sha256:x"}},
    {"binding": {"type": rs.BINDING_DB_RULE, "fingerprint": ""}},
    {"schema_version": "9.9"},
])
def test_a_malformed_1_2_runbook_is_not_served(estate, overrides):
    data = _final("svc-a", "CODE", "E")
    data.update(overrides)
    _write(rs.RUNBOOK_FINAL_DIR, "svc-a", "CODE__E.json", data)

    assert rs.get_runbook("svc-a", "CODE", "E") is None


def test_the_default_pack_has_no_runbooks(estate):
    assert rs.get_runbook(sr.DEFAULT_PACK, CODE, "U") is None
    with pytest.raises(ValueError):
        rs.write_draft_runbook(sr.DEFAULT_PACK, CODE, "U", {})


def test_drafts_are_written_and_promoted_within_their_service(estate):
    rs.write_draft_runbook(NONE_SVC, CODE, "u", {"x": 1})
    (rs.RUNBOOK_DRAFT_DIR / "STRAY__E.json").write_text("{}", encoding="utf-8")

    drafts = rs.list_draft_runbooks()
    assert drafts == [rs.RUNBOOK_DRAFT_DIR / NONE_SVC / f"{CODE}__U.json"], \
        "a runbook outside any service directory is not listed"

    rs.promote_draft_to_final(drafts[0], _final(NONE_SVC, etype="u"))
    assert rs.list_final_runbooks() == [rs.RUNBOOK_FINAL_DIR / NONE_SVC / f"{CODE}__U.json"]
    assert rs.list_draft_runbooks() == []


def test_a_draft_naming_another_service_is_not_promoted(estate):
    rs.write_draft_runbook(NONE_SVC, CODE, "U", {})
    draft = rs.list_draft_runbooks()[0]

    with pytest.raises(ValueError):
        rs.promote_draft_to_final(draft, _final(BIO))
    assert draft.exists()


def test_the_shipped_drafts_are_the_biometric_services():
    """The 39 drafts moved into draft/enu-biometric/ and name that service."""
    drafts = rs.list_draft_runbooks()

    assert len(drafts) == 39
    assert not list(rs.RUNBOOK_DRAFT_DIR.glob("*.json"))
    for path in drafts:
        assert rs.service_of_path(path) == BIO
        assert json.loads(path.read_text(encoding="utf-8"))["service"] == BIO


# ---------------------------------------------------------------------------
# The serve allowlist names the service.
# ---------------------------------------------------------------------------

def test_an_allowlist_entry_clears_one_service_only(monkeypatch):
    monkeypatch.setenv("RUNBOOK_SERVE_ALLOWLIST", f"{NONE_SVC}:{CODE}")

    assert rs.is_serve_allowed(NONE_SVC, CODE)
    assert not rs.is_serve_allowed(BIO, CODE)


def test_a_bare_allowlist_code_means_the_biometric_service_with_a_warning(monkeypatch):
    warnings = []
    monkeypatch.setattr(rs, "logger", MagicMock(
        warning=lambda message, **kw: warnings.append((message, kw))))
    monkeypatch.setattr(rs, "_warned_bare_codes", set())
    monkeypatch.setenv("RUNBOOK_SERVE_ALLOWLIST", "BARE_CODE, CODE:WITH:COLONS")

    assert rs.serve_allowlist() == {(BIO, "BARE_CODE"), (BIO, "CODE:WITH:COLONS")}
    assert rs.is_serve_allowed(BIO, "BARE_CODE")
    assert not rs.is_serve_allowed(NONE_SVC, "BARE_CODE")
    assert sorted(kw["entry"] for _, kw in warnings) == ["BARE_CODE", "CODE:WITH:COLONS"], \
        "warned once per entry, however often the list is read"


# ---------------------------------------------------------------------------
# The documentation binding.
# ---------------------------------------------------------------------------

def test_a_documentation_binding_ignores_a_label_change(estate):
    with_pack_labels = rcd.lookup(CODE, "U", **sr.docs_lookup_options(NONE_SVC))
    relabelled = rcd.lookup(CODE, "U", **{**sr.docs_lookup_options(NONE_SVC),
                                          "type_labels": {"U": "Renamed update"}})

    assert with_pack_labels["outcome"] == "hit"
    assert with_pack_labels["sha256"] != relabelled["sha256"], "the text did change"
    assert rs.doc_binding_fingerprint(with_pack_labels) == \
        rs.doc_binding_fingerprint(relabelled)


def test_a_documentation_binding_moves_when_the_entries_change(estate):
    before = _doc_binding()
    _doc_file(estate / "docs" / "services", NONE_SVC, CODE,
              "Raised when the packet is unreadable or truncated.")

    assert _doc_binding()["fingerprint"] != before["fingerprint"]


def test_there_is_no_documentation_binding_without_a_hit(estate):
    assert rs.doc_binding_fingerprint(rcd.lookup("UNKNOWN_CODE", "U")) is None
    assert rs.doc_binding_fingerprint({"outcome": "disabled"}) is None
    assert rs.doc_binding_fingerprint(None) is None


# ---------------------------------------------------------------------------
# The runbook lookup node.
# ---------------------------------------------------------------------------

class _Recorder:
    def __init__(self):
        self.prompts = []

    def invoke(self, request):
        self.prompts.append(request["messages"][-1].content)
        return {"messages": [AIMessage(content="the findings")]}


def _node(monkeypatch, name):
    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", lambda *a, **k: _Recorder())
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    return orch._build_agent().builder.nodes[name].runnable.func


def _state(stage="Nowhere", code=CODE, etype="U", **overrides):
    state = {"payload": {
        "eventId": "evt-1", "flowMetaData": {"stage": stage},
        "packetMetaData": {"enrolmentType": etype},
        "packetExecutionSummary": {"packetStatus": "REJECTED",
                                   "errorData": [{"errorReasonCode": code}]}},
        "logs": "Log fetching disabled.", "db_rule": "", "investigation": "",
        "retry_count": 0}
    state.update(overrides)
    return state


@pytest.fixture
def lookups(monkeypatch):
    """(outcome, service) of every RUNBOOK_LOOKUPS increment."""
    seen = []
    monkeypatch.setattr(orch.metrics, "RUNBOOK_LOOKUPS", MagicMock(
        labels=lambda outcome, service: MagicMock(
            inc=lambda: seen.append((outcome, service)))))
    return seen


@pytest.fixture
def no_rules_table(monkeypatch):
    def _refuse(*args, **kwargs):
        raise AssertionError("a service with no rules table queried the rules table")

    monkeypatch.setattr(orch, "lookup_rule_text", _refuse)
    monkeypatch.setattr(orch, "lookup_rule_for", _refuse)


def test_the_documentation_is_resolved_once_whatever_the_mode(estate, monkeypatch):
    """Resolved before the RUNBOOK_MODE check and carried to the Investigator,
    which then does not look it up again."""
    monkeypatch.setenv("RUNBOOK_MODE", "off")
    calls = []
    real = rcd.lookup
    monkeypatch.setattr(rcd, "lookup", lambda *a, **k: calls.append(a) or real(*a, **k))
    lookup = _node(monkeypatch, "runbook_lookup")
    investigate = _node(monkeypatch, "investigate")

    result = lookup(_state())
    assert result["resolution_source"] == "agent"
    assert result["reason_code_doc"]["outcome"] == "hit"

    investigate(_state(reason_code_doc=result["reason_code_doc"]))
    assert len(calls) == 1


def test_a_documentation_bound_runbook_is_served_for_its_service(
        estate, monkeypatch, lookups, no_rules_table):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    _write(rs.RUNBOOK_FINAL_DIR, NONE_SVC, f"{CODE}__U.json",
           _final(NONE_SVC, binding=_doc_binding()))
    lookup = _node(monkeypatch, "runbook_lookup")

    result = lookup(_state())

    assert result["resolution_source"] == f"runbook:{CODE}__U@v1"
    assert result["reason_code_doc"]["outcome"] == "hit", \
        "the casebook records the documentation the runbook was checked against"
    assert lookups == [("hit", NONE_SVC)]


def test_a_documentation_bound_runbook_goes_stale_with_its_entries(
        estate, monkeypatch, lookups, no_rules_table):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    _write(rs.RUNBOOK_FINAL_DIR, NONE_SVC, f"{CODE}__U.json",
           _final(NONE_SVC, binding=_doc_binding()))
    _doc_file(estate / "docs" / "services", NONE_SVC, CODE, "A rewritten description.")
    lookup = _node(monkeypatch, "runbook_lookup")

    assert lookup(_state())["resolution_source"] == "agent"
    assert lookups == [("fingerprint_mismatch", NONE_SVC)]


def test_without_the_documentation_a_documentation_bound_runbook_is_not_served(
        estate, monkeypatch, lookups, no_rules_table):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    _write(rs.RUNBOOK_FINAL_DIR, NONE_SVC, "OTHER_CODE__U.json",
           _final(NONE_SVC, reason_code="OTHER_CODE", binding=_doc_binding()))
    lookup = _node(monkeypatch, "runbook_lookup")

    assert lookup(_state(code="OTHER_CODE"))["resolution_source"] == "agent"
    assert lookups == [("binding_unavailable", NONE_SVC)]


@pytest.mark.parametrize("stage, binding", [
    # A rules-table binding for a service with no rules table ...
    ("Nowhere", {"type": rs.BINDING_DB_RULE, "fingerprint": "sha256:rule"}),
    # ... and a documentation binding for one with a rules table.
    ("Biometric", {"type": rs.BINDING_REASON_CODE_DOC, "fingerprint": "sha256:doc"}),
])
def test_a_runbook_bound_to_the_wrong_rule_source_is_stale(
        estate, monkeypatch, lookups, stage, binding):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setattr(orch, "get_runbook",
                        lambda *a: _final("any", binding=binding))
    monkeypatch.setattr(orch, "lookup_rule_for", lambda *a, **k: [{"rule": 1}])
    lookup = _node(monkeypatch, "runbook_lookup")

    assert lookup(_state(stage=stage))["resolution_source"] == "agent"
    assert [outcome for outcome, _ in lookups] == ["fingerprint_mismatch"]


def test_the_biometric_lookup_reads_its_own_directory_and_rule(estate, monkeypatch, lookups):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    rules = [{"rule": "the rows"}]
    monkeypatch.setattr(orch, "lookup_rule_for", lambda *a, **k: rules)
    _write(rs.RUNBOOK_FINAL_DIR, BIO, "BIO_CODE__U.json",
           _final(BIO, reason_code="BIO_CODE",
                  binding={"type": rs.BINDING_DB_RULE,
                           "fingerprint": rs.generate_rule_fingerprint(rules)}))
    lookup = _node(monkeypatch, "runbook_lookup")

    assert lookup(_state(stage="Biometric", code="BIO_CODE"))["resolution_source"] \
        == "runbook:BIO_CODE__U@v1"
    assert lookups == [("hit", BIO)]


def test_the_allowlist_is_read_per_service(estate, monkeypatch, lookups, no_rules_table):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setenv("RUNBOOK_SERVE_ALLOWLIST", f"{BIO}:{CODE}")
    _write(rs.RUNBOOK_FINAL_DIR, NONE_SVC, f"{CODE}__U.json",
           _final(NONE_SVC, binding=_doc_binding()))
    lookup = _node(monkeypatch, "runbook_lookup")

    result = lookup(_state())

    assert result["resolution_source"] == "agent"
    assert result["shadow_runbook_resolution"]
    assert lookups == [("shadow", NONE_SVC)]


def test_an_unresolved_packet_uses_no_runbook(estate, monkeypatch, lookups):
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    monkeypatch.setattr(orch, "get_runbook",
                        lambda *a: pytest.fail("an unresolved packet read a runbook"))
    lookup = _node(monkeypatch, "runbook_lookup")

    lookup(_state(service_pack=sr.DEFAULT_PACK))
    assert [outcome for outcome, _ in lookups] == ["no_service"]


# ---------------------------------------------------------------------------
# Drafting: casebooks are grouped by service.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("casebook, service", [
    ({"packet_metadata": {"service": NONE_SVC}}, NONE_SVC),
    ({"packet_metadata": {"service": "svc-unregistered"}}, None),
    ({"packet_metadata": {"service": sr.UNRESOLVED}}, None),
    # Before Phase 1: placed by the stage, and only in enu-biometric.
    ({"packet_metadata": {}, "packet_status": {"service": " biometric "}}, BIO),
    ({"packet_metadata": {}, "packet_status": {"service": "Nowhere"}}, None),
    ({"packet_metadata": {}, "packet_status": {}}, None),
    # Analysed with another service's pack: the gate was only recording.
    ({"packet_metadata": {"service": NONE_SVC},
      "resolution": {"provenance": {"service_pack": {"service": BIO}}}}, None),
    ({"packet_metadata": {"service": NONE_SVC},
      "resolution": {"provenance": {"service_pack": {"service": NONE_SVC}}}}, NONE_SVC),
])
def test_a_casebook_teaches_only_its_own_services_runbook(estate, casebook, service):
    assert build_runbooks.casebook_service(casebook) == service


def test_a_draft_binds_to_its_services_rule_source(estate, monkeypatch):
    rules = [{"rule": "rows"}]
    asked = []
    monkeypatch.setattr(build_runbooks, "lookup_rule_for",
                        lambda code, etype, type_filter=None: asked.append(code) or rules)

    assert build_runbooks.binding_for(BIO, "BIO_CODE", "U") == {
        "type": rs.BINDING_DB_RULE, "fingerprint": rs.generate_rule_fingerprint(rules)}
    assert build_runbooks.binding_for(NONE_SVC, CODE, "U") == _doc_binding()
    assert asked == ["BIO_CODE"], "a service with no rules table is not looked up there"
    # A code only another service documents gives nothing to bind to.
    assert build_runbooks.binding_for(NONE_SVC, "BIO_CODE", "U") is None


def test_coverage_checks_each_runbook_against_its_own_services_documentation(estate):
    for service, code in ((NONE_SVC, CODE), (NONE_SVC, "BIO_CODE"), (BIO, "BIO_CODE")):
        _write(rs.RUNBOOK_DRAFT_DIR, service, f"{code}__U.json", {})

    _, warnings = rcd.validate(coverage=True)
    covered = [w for w in warnings if "has a runbook" in w]

    assert covered == [f"BIO_CODE has a runbook (draft/{NONE_SVC}/BIO_CODE__U.json) "
                       f"but no documentation in services/{NONE_SVC}.json."]


# ---------------------------------------------------------------------------
# Learned rules carry a scope.
# ---------------------------------------------------------------------------

@pytest.fixture
def queued(tmp_path, monkeypatch):
    """The direct Reviewer's add_learning_rule tool, queueing to a temporary
    file; returns (tool, read the queue)."""
    pending = tmp_path / "pending_rules.jsonl"
    monkeypatch.setattr(orch, "PENDING_RULES_FILE", str(pending))
    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    captured = {}

    def fake_build_agent(role, llm, system_prompt, tools=(), pack=None):
        for tool in tools:
            if getattr(tool, "name", "") == "add_learning_rule":
                captured["tool"] = tool
        return MagicMock()

    monkeypatch.setattr(orch, "build_agent", fake_build_agent)
    orch._build_agent()

    def read():
        return [json.loads(line) for line in pending.read_text().splitlines()]
    return captured["tool"], read


@pytest.mark.parametrize("args, scope", [
    ({}, "service"),
    ({"scope": "generic"}, "generic"),
    ({"scope": " Generic "}, "generic"),
    ({"scope": "everywhere"}, "service"),
])
def test_a_proposed_rule_records_its_scope(queued, args, scope):
    tool, read = queued
    orch._current_service.set(NONE_SVC)
    orch._current_pack.set(NONE_SVC)

    outcome = tool.invoke({"rule_text": "Always cite the log line relied on.",
                           "reasoning": "a claim had no citation", **args})

    assert outcome.startswith("Successfully queued")
    [entry] = read()
    assert (entry["scope"], entry["service_pack"]) == (scope, NONE_SVC)


@pytest.mark.parametrize("entry, scope, expected", [
    ({"service_pack": NONE_SVC}, None, f"packs/{NONE_SVC}/learned_rules.md"),
    ({"service_pack": NONE_SVC, "scope": "generic"}, None, "repo/src/prompts/learned_rules.md"),
    # The operator's choice overrides the proposal, in both directions.
    ({"service_pack": NONE_SVC, "scope": "generic"}, "service",
     f"packs/{NONE_SVC}/learned_rules.md"),
    ({"service_pack": NONE_SVC}, "generic", "repo/src/prompts/learned_rules.md"),
    # Queued before rules carried a scope or a pack: the biometric pack's.
    ({}, None, f"packs/{BIO}/learned_rules.md"),
])
def test_a_learned_rule_lands_in_the_file_its_scope_names(
        estate, monkeypatch, entry, scope, expected):
    monkeypatch.setattr(paths, "REPO_ROOT", estate / "repo")

    assert Path(promote_rules.target_file_for(entry, scope)) == estate / expected


def _promote(estate, monkeypatch, entry, answers):
    """Run promote_rules against a temporary repository with `answers` typed."""
    prompts = estate / "repo" / "src" / "prompts"
    prompts.mkdir(parents=True, exist_ok=True)
    (prompts / "pending_rules.jsonl").write_text(json.dumps(entry) + "\n")
    monkeypatch.setattr(paths, "REPO_ROOT", estate / "repo")
    monkeypatch.setattr(promote_rules.subprocess, "run",
                        lambda *a, **k: MagicMock(stdout=""))
    typed = iter(answers)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(typed))
    promote_rules.promote_rules()
    return prompts


@pytest.mark.parametrize("proposed, answers, generic", [
    ("service", ["promote"], False),
    ("generic", ["promote"], True),
    ("service", ["generic", "promote"], True),
    ("generic", ["service", "promote"], False),
])
def test_the_operator_can_change_the_scope_at_promotion(
        estate, monkeypatch, proposed, answers, generic):
    rule = "Always cite the log line relied on."
    prompts = _promote(estate, monkeypatch,
                       {"eventId": "evt-1", "proposed_rule": rule,
                        "service_pack": NONE_SVC, "scope": proposed}, answers)

    generic_file = prompts / "learned_rules.md"
    pack_file = estate / "packs" / NONE_SVC / "learned_rules.md"
    written, untouched = (generic_file, pack_file) if generic else (pack_file, generic_file)
    assert f"- CRITICAL RULE: {rule}" in written.read_text()
    assert not untouched.exists()
    assert (prompts / "pending_rules.jsonl").read_text() == ""


def test_a_generic_rule_moves_every_packs_fingerprint(estate, monkeypatch):
    """The generic file is read into every service's Investigator prompt, and
    a pack's own file into that pack's only."""
    source = paths.REPO_ROOT / "src"
    base = estate / "repo" / "src"
    for name in orch.PROMPT_FILES:
        origin = source / "prompts" / name
        if origin.is_file():
            (base / "prompts" / name).parent.mkdir(parents=True, exist_ok=True)
            (base / "prompts" / name).write_bytes(origin.read_bytes())

    def fingerprints():
        sr.reset()
        return {pack: orch.compute_prompt_fingerprint(str(base), pack)
                for pack in (BIO, NONE_SVC)}

    before = fingerprints()
    (estate / "packs" / NONE_SVC / "learned_rules.md").write_text(
        "- CRITICAL RULE: a service rule\n", encoding="utf-8")
    service_rule = fingerprints()
    (base / "prompts" / "learned_rules.md").write_text(
        "- CRITICAL RULE: a generic rule\n", encoding="utf-8")
    generic_rule = fingerprints()

    assert service_rule[NONE_SVC] != before[NONE_SVC]
    assert service_rule[BIO] == before[BIO]
    assert all(generic_rule[p] != service_rule[p] for p in (BIO, NONE_SVC))
