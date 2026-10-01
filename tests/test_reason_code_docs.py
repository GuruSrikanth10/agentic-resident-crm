"""
The reason-code document store: lookup, rendering, and the validator
(REASON_CODE_DOCS_PLAN.md Phase 2).

Two of these tests are the ones that matter in CI:

* `test_the_committed_store_validates` gates every future edit to
  `src/reason_code_docs/`. The API exits at boot on a store with errors, so
  the alternative to catching it here is catching it in a deployment.
* `test_lookup_never_raises` pins the contract the whole design rests on. A
  documentation problem must cost one packet its document, never the packet.
"""
import json

import pytest

from src.utils import paths, reason_code_docs as rcd

FIXTURE_ROOT = paths.REPO_ROOT / "tests" / "fixtures" / "reason_code_docs"
COMMITTED_ROOT = paths.REPO_ROOT / "src" / "reason_code_docs"


def _store(tmp_path, document, name="svc.json"):
    """A store holding one service file, ready to validate or look up in."""
    services = tmp_path / rcd.SERVICES_DIRNAME
    services.mkdir(parents=True, exist_ok=True)
    body = document if isinstance(document, str) else json.dumps(document)
    (services / name).write_text(body, encoding="utf-8")
    return tmp_path


def _valid_document(**overrides):
    document = {
        "schema_version": 1,
        "service": "svc",
        "codes": [{"numeric_code": 1, "reason_code": "SOME_CODE",
                   "description": "Raised when the packet cannot be reconciled.",
                   "category": "Technical", "is_retryable": True}],
        "rules": {"total_rules": 0, "rules": []},
    }
    document.update(overrides)
    return document


def _errors(tmp_path, document, name="svc.json"):
    return rcd.validate(root=_store(tmp_path, document, name))[0]


# ---------------------------------------------------------------------------
# The committed store, and the fixture the pipeline tests run against.
# ---------------------------------------------------------------------------

def test_the_committed_store_validates():
    """The CI gate for every future edit to src/reason_code_docs/."""
    errors, _ = rcd.validate(root=COMMITTED_ROOT)
    assert errors == []


def test_the_committed_store_documents_the_enu_biometric_service():
    state = rcd.lookup("RESIDENT_MAN_DEDUPE_REJECT_WL_DEMOMATCH_TD", "E",
                       root=COMMITTED_ROOT)
    assert state["outcome"] == "hit"
    assert state["matched_type"] == "E"
    assert "cre-14f9c766" in state["text"]
    assert "MDD_POLICY_BATCH_1" in state["text"]


def test_the_fixture_store_validates():
    errors, warnings = rcd.validate(root=FIXTURE_ROOT)
    assert errors == []
    assert warnings == []


# ---------------------------------------------------------------------------
# Lookup: the outcome matrix of section 5.3.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["E", "N", "ENROLMENT", "ENROLLMENT", " e "])
def test_every_enrolment_alias_reaches_the_same_document(raw):
    state = rcd.lookup("FIXTURE_TYPED_CODE", raw, root=FIXTURE_ROOT)
    assert state["outcome"] == "hit"
    assert state["requested_type"] == "E"
    assert state["matched_type"] == "E"
    assert "fx-enrolment" in state["text"]
    assert "fx-update" not in state["text"]


@pytest.mark.parametrize("raw", ["U", "UPDATE"])
def test_an_update_gets_the_update_rule(raw):
    state = rcd.lookup("FIXTURE_TYPED_CODE", raw, root=FIXTURE_ROOT)
    assert state["matched_type"] == "U"
    assert "fx-update" in state["text"]
    assert "fx-enrolment" not in state["text"]


def test_a_typed_document_still_carries_the_type_agnostic_material():
    """A code entry describes the code itself, so it applies whatever the
    packet's type is -- leaving it out would mean an E packet is told less
    about its own reason code than a packet with no type at all."""
    state = rcd.lookup("FIXTURE_TYPED_CODE", "E", root=FIXTURE_ROOT)
    kinds = {ref["kind"] for ref in state["refs"]}
    assert kinds == {"code", "rule"}


@pytest.mark.parametrize("raw,requested", [("Z", "Z"), (None, None),
                                           ("", None), ("B/D", None)])
def test_an_unknown_type_falls_back_to_the_any_material(raw, requested):
    state = rcd.lookup("FIXTURE_ANY_CODE", raw, root=FIXTURE_ROOT)
    assert state["outcome"] == "hit"
    assert state["requested_type"] == requested
    assert state["matched_type"] == rcd.ANY


def test_a_rule_naming_no_type_applies_to_every_type():
    for raw in ("E", "U", "Z", None):
        state = rcd.lookup("FIXTURE_RULES_ONLY_CODE", raw, root=FIXTURE_ROOT)
        assert state["outcome"] == "hit", raw
        assert "fx-untyped" in state["text"]


def test_a_code_documented_only_for_another_type_is_a_miss(tmp_path):
    """Answering an enrolment packet with UPDATE-only rules would be worse
    than answering it with nothing: those rules cannot have fired."""
    document = _valid_document(codes=[], rules={"rules": [{
        "rule_id": "r1", "reject_reason_code": "UPDATE_ONLY",
        "module": "M", "condition_description": "the enrolment type is 'UPDATE'",
        "description": "Fires when the enrolment type is 'UPDATE'. REJECTED."}]})
    root = _store(tmp_path, document)
    assert rcd.lookup("UPDATE_ONLY", "U", root=root)["outcome"] == "hit"
    assert rcd.lookup("UPDATE_ONLY", "E", root=root)["outcome"] == "miss"


def test_an_unknown_reason_code_is_a_miss():
    state = rcd.lookup("NO_SUCH_CODE", "E", root=FIXTURE_ROOT)
    assert state["outcome"] == "miss"
    assert state["refs"] == [] and state["text"] is None


def test_a_reason_code_that_fails_the_pattern_is_a_miss():
    state = rcd.lookup("has spaces!", "E", root=FIXTURE_ROOT)
    assert state["outcome"] == "miss"
    assert state["detail"] == "invalid reason code"


@pytest.mark.parametrize("value", [None, ""])
def test_no_reason_code_has_its_own_outcome(value):
    """Distinct from a miss: a packet with no code is not a documentation
    gap, so it must not show up in the miss rate that drives authoring."""
    assert rcd.lookup(value, "E", root=FIXTURE_ROOT)["outcome"] == "no_reason_code"


def test_a_store_that_is_not_there_is_a_miss_not_an_exception(tmp_path):
    """The pipeline runs without a store; it just has nothing to say."""
    state = rcd.lookup("FIXTURE_ANY_CODE", "E", root=tmp_path / "gone")
    assert state["outcome"] == "miss"


def test_an_unreadable_file_is_an_error_not_an_exception(tmp_path):
    root = _store(tmp_path, "{ not json")
    state = rcd.lookup("FIXTURE_ANY_CODE", "E", root=root)
    assert state["outcome"] == "error"
    assert "ReasonCodeDocError" in state["detail"]


def test_lookup_never_raises(tmp_path, monkeypatch):
    """The contract the graph node depends on. Anything at all may go wrong
    inside; the caller gets a state dict either way."""
    def explode(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(rcd, "_file_entries", explode)
    state = rcd.lookup("FIXTURE_ANY_CODE", "E", root=FIXTURE_ROOT)
    assert state["outcome"] == "error"
    assert state["detail"] == "RuntimeError: boom"
    assert state["text"] is None and state["resolution_guidance"] == []


def test_every_non_hit_outcome_has_the_full_state_shape():
    """Partial states make provenance records that differ packet to packet."""
    keys = set(rcd.lookup("FIXTURE_ANY_CODE", "E", root=FIXTURE_ROOT))
    for state in (rcd.lookup(None, "E", root=FIXTURE_ROOT),
                  rcd.lookup("NO_SUCH_CODE", "E", root=FIXTURE_ROOT)):
        assert set(state) == keys
        assert state["text"] is None and state["sha256"] is None
        assert state["refs"] == [] and state["resolution_guidance"] == []


def test_the_store_root_is_read_at_call_time(monkeypatch):
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", FIXTURE_ROOT)
    assert rcd.lookup("FIXTURE_ANY_CODE", "E")["outcome"] == "hit"


# ---------------------------------------------------------------------------
# Enrolment-type normalisation (D4).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("E", "E"), ("N", "E"), ("e", "E"), (" n ", "E"),
    ("ENROLMENT", "E"), ("ENROLLMENT", "E"),
    ("U", "U"), ("u", "U"), ("UPDATE", "U"),
    ("Z", "Z"), ("z", "Z"), ("ANY", "ANY"),
    ("B/D", None), ("", None), (None, None), ("  ", None),
    ("TOOLONGFORATYPECODE", None),
])
def test_normalize_doc_type(raw, expected):
    assert rcd.normalize_doc_type(raw) == expected


# ---------------------------------------------------------------------------
# Rendering (5.6).
# ---------------------------------------------------------------------------

def test_each_block_names_the_file_and_entry_it_came_from():
    """A claim in an investigation has to be traceable back to a source."""
    text = rcd.lookup("FIXTURE_TYPED_CODE", "E", root=FIXTURE_ROOT)["text"]
    assert "[Source: services/fixture-service.json, code 9002]" in text
    assert "[Source: services/fixture-service.json, rule fx-enrolment]" in text
    assert text.startswith("# FIXTURE_TYPED_CODE -- New Enrolment (E)")
    # One blank line between blocks, and no blank line inside a header.
    assert "\n\n[Source:" in text


def test_a_rule_block_separates_its_condition_from_its_outcome():
    text = rcd.lookup("FIXTURE_TYPED_CODE", "U", root=FIXTURE_ROOT)["text"]
    assert "Fires when: the enrolment type is 'UPDATE' (Update); AND the " \
           "applicant matched a different parent." in text
    assert "Outcome: The APPLICANT is REJECTED." in text
    # The generated preamble is dropped: the heading already names both.
    assert "Fixture rejection rule in FIXTURE_BATCH_1" not in text


def test_the_sha256_identifies_the_exact_text_the_model_saw():
    first = rcd.lookup("FIXTURE_ANY_CODE", "E", root=FIXTURE_ROOT)
    again = rcd.lookup("FIXTURE_ANY_CODE", "Z", root=FIXTURE_ROOT)
    other = rcd.lookup("FIXTURE_TYPED_CODE", "E", root=FIXTURE_ROOT)

    assert first["sha256"].startswith("sha256:")
    assert first["sha256"] == again["sha256"], "same text, same digest"
    assert first["sha256"] != other["sha256"]


def test_an_oversized_document_is_truncated_and_says_so(monkeypatch):
    monkeypatch.setenv("REASON_CODE_DOC_MAX_CHARS", "200")
    state = rcd.lookup("FIXTURE_ANY_CODE", "E", root=FIXTURE_ROOT)

    assert state["truncated"] is True
    assert len(state["text"]) <= 200
    assert "characters omitted from the end of the reason-code documentation" \
        in state["text"]


@pytest.mark.parametrize("raw,expected", [
    (None, 16000), ("", 16000), ("nonsense", 16000), ("0", 16000),
    ("-5", 16000), ("2000", 2000), (" 2000 ", 2000),
])
def test_max_chars_falls_back_on_an_unusable_value(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("REASON_CODE_DOC_MAX_CHARS", raising=False)
    else:
        monkeypatch.setenv("REASON_CODE_DOC_MAX_CHARS", raw)
    assert rcd.max_chars() == expected


# ---------------------------------------------------------------------------
# Resolution guidance (content rule 5).
# ---------------------------------------------------------------------------

def test_guidance_is_parsed_from_the_rendered_text():
    state = rcd.lookup("FIXTURE_ANY_CODE", "E", root=FIXTURE_ROOT)
    assert state["resolution_guidance"] == [{
        "action": "REPLAY", "resident_action": "PENDING",
        "when": "the downstream service has recovered"}]


def test_guidance_without_a_when_clause_parses():
    text = "## Resolution guidance\n- action: MANUAL_REVIEW | resident_action: PENDING"
    assert rcd.parse_resolution_guidance(text) == [
        {"action": "MANUAL_REVIEW", "resident_action": "PENDING", "when": None}]


def test_a_guidance_line_outside_its_section_is_not_guidance():
    text = ("## Summary\n- action: REPLAY | resident_action: PENDING\n"
            "## Resolution guidance\n- action: QC_REPLAY | resident_action: NEW_PACKET")
    assert [g["action"] for g in rcd.parse_resolution_guidance(text)] == ["QC_REPLAY"]


def test_a_later_heading_closes_the_guidance_section():
    text = ("## Resolution guidance\n- action: REPLAY | resident_action: PENDING\n"
            "## Sources\n- action: QC_REPLAY | resident_action: NEW_PACKET")
    assert [g["action"] for g in rcd.parse_resolution_guidance(text)] == ["REPLAY"]


# ---------------------------------------------------------------------------
# Provenance: the casebook's copy carries no document text.
# ---------------------------------------------------------------------------

def test_provenance_drops_the_text_and_the_guidance():
    state = rcd.lookup("FIXTURE_ANY_CODE", "E", root=FIXTURE_ROOT)
    record = rcd.provenance(state)

    assert "text" not in record and "resolution_guidance" not in record
    assert record["sha256"] == state["sha256"]
    assert record["refs"] == state["refs"]
    assert json.dumps(record).find("cannot reconcile the packet") == -1


def test_provenance_of_nothing_is_nothing():
    assert rcd.provenance(None) is None


def test_provenance_tolerates_the_disabled_shorthand():
    """`{"outcome": "disabled"}` is the whole state when the switch is off."""
    assert rcd.provenance({"outcome": "disabled"}) == {
        "outcome": "disabled", "reason_code": None, "requested_type": None,
        "matched_type": None, "refs": [], "sha256": None, "truncated": False,
        "detail": None, "scope": None}


# ---------------------------------------------------------------------------
# The validator: one test per error in the store contract.
# ---------------------------------------------------------------------------

def test_an_empty_store_is_an_error(tmp_path):
    (tmp_path / rcd.SERVICES_DIRNAME).mkdir()
    errors, _ = rcd.validate(root=tmp_path)
    assert any("No service files" in e for e in errors)


def test_a_missing_store_is_an_error(tmp_path):
    errors, _ = rcd.validate(root=tmp_path / "gone")
    assert any("does not exist" in e for e in errors)


def test_invalid_json_is_an_error(tmp_path):
    assert any("not valid JSON" in e for e in _errors(tmp_path, "{ nope"))


def test_a_non_object_file_is_an_error(tmp_path):
    assert any("not an object" in e for e in _errors(tmp_path, "[1, 2]"))


def test_a_wrong_schema_version_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(schema_version=2))
    assert any("schema_version" in e for e in errors)


def test_an_unknown_top_level_key_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(reason_codes={}))
    assert any("unknown key(s) ['reason_codes']" in e for e in errors)


def test_a_missing_service_name_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(service=""))
    assert any("'service' must be a non-empty string" in e for e in errors)


def test_two_files_claiming_one_service_is_an_error(tmp_path):
    root = _store(tmp_path, _valid_document(), "a.json")
    _store(root, _valid_document(
        codes=[{"numeric_code": 2, "reason_code": "OTHER_CODE",
                "description": "Another code.", "category": "Technical",
                "is_retryable": False}]), "b.json")
    errors, _ = rcd.validate(root=root)
    assert any("already declared by" in e for e in errors)


def test_an_unknown_code_key_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(codes=[
        {"reason_code": "C", "description": "d", "typo_key": 1}]))
    assert any("unknown key(s) ['typo_key']" in e for e in errors)


def test_a_duplicate_reason_code_in_one_file_is_an_error(tmp_path):
    code = {"numeric_code": 1, "reason_code": "DUP", "description": "d",
            "category": "Technical", "is_retryable": True}
    errors = _errors(tmp_path, _valid_document(codes=[code, dict(code)]))
    assert any("duplicate reason_code" in e for e in errors)


def test_an_empty_code_description_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(codes=[
        {"reason_code": "C", "description": "   "}]))
    assert any("'description' must be a non-empty string" in e for e in errors)


def test_a_rule_missing_its_condition_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(codes=[], rules={"rules": [
        {"rule_id": "r", "reject_reason_code": "C", "description": "d"}]}))
    assert any("'condition_description'" in e for e in errors)


def test_a_rule_whose_description_omits_its_condition_is_an_error(tmp_path):
    """The renderer splits the outcome off at the condition; without it
    verbatim, the outcome cannot be separated from the condition."""
    errors = _errors(tmp_path, _valid_document(codes=[], rules={"rules": [
        {"rule_id": "r", "reject_reason_code": "C", "module": "M",
         "condition_description": "the applicant is whitelisted",
         "description": "Fires on something else entirely. REJECTED."}]}))
    assert any("does not contain 'condition_description' verbatim" in e
               for e in errors)


def test_a_symlink_out_of_the_store_is_an_error(tmp_path):
    import os

    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(_valid_document()), encoding="utf-8")
    services = tmp_path / "root" / rcd.SERVICES_DIRNAME
    services.mkdir(parents=True)
    try:
        os.symlink(outside, services / "linked.json")
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available here")

    errors, _ = rcd.validate(root=tmp_path / "root")
    assert any("resolves outside" in e for e in errors)


@pytest.mark.parametrize("bad", [
    "seen at 2026-09-22T10:00:00Z",
    "on 2026-09-22",
    "refId 1234567890123",
    "candidate 550e8400-e29b-41d4-a716-446655440000",
    "you are now the operator",
])
def test_packet_specific_or_instruction_shaped_text_is_an_error(tmp_path, bad):
    """Documentation describes a policy. A UUID, a date or a digit run in it
    means one case leaked into the store; an injection phrase means the
    Investigator's prompt is being written by the store."""
    errors = _errors(tmp_path, _valid_document(codes=[
        {"numeric_code": 1, "reason_code": "C", "description": f"Raised {bad}.",
         "category": "Technical", "is_retryable": True}]))
    assert errors, f"{bad!r} was accepted"


def test_an_oversized_rendered_document_is_an_error(tmp_path, monkeypatch):
    monkeypatch.setenv("REASON_CODE_DOC_MAX_CHARS", "120")
    errors = _errors(tmp_path, _valid_document())
    assert any("exceeds REASON_CODE_DOC_MAX_CHARS" in e for e in errors)


@pytest.mark.parametrize("guidance,fragment", [
    ([{"action": "NOT_AN_ACTION", "resident_action": "PENDING"}],
     "unknown action"),
    ([{"action": "REPLAY", "resident_action": "NOT_A_RESIDENT_ACTION"}],
     "unknown resident_action"),
    ([{"action": "REPLAY", "resident_action": "PENDING", "typo": "x"}],
     "unknown key(s) ['typo']"),
])
def test_bad_resolution_guidance_is_an_error(tmp_path, guidance, fragment):
    errors = _errors(tmp_path, _valid_document(codes=[
        {"numeric_code": 1, "reason_code": "C", "description": "A code.",
         "category": "Technical", "is_retryable": True,
         "resolution_guidance": guidance}]))
    assert any(fragment in e for e in errors), errors


def test_a_malformed_guidance_line_is_an_error(tmp_path):
    """A line that starts like guidance and does not parse is reported, not
    silently dropped -- it was meant to steer Synthesis and would not."""
    errors = _errors(tmp_path, _valid_document(codes=[
        {"numeric_code": 1, "reason_code": "C",
         "description": "## Resolution guidance\n- action: REPLAY but no pipe",
         "category": "Technical", "is_retryable": True}]))
    assert any("malformed resolution-guidance line" in e for e in errors)


def test_a_guidance_line_outside_its_heading_is_an_error(tmp_path):
    errors = _errors(tmp_path, _valid_document(codes=[
        {"numeric_code": 1, "reason_code": "C",
         "description": "- action: REPLAY | resident_action: PENDING",
         "category": "Technical", "is_retryable": True}]))
    assert any("sits outside a 'Resolution guidance' section" in e
               for e in errors)


# ---------------------------------------------------------------------------
# The validator: warnings are reported, never fatal.
# ---------------------------------------------------------------------------

def test_an_unaddressable_reason_code_warns_once_and_is_skipped(tmp_path):
    """`(CRE_REJECT_APPLICANT)` is real generated data, not a typo: the
    generator could not resolve that rule's reject reason code. It can never
    match a payload value, so failing a deploy over it would mean the file
    could never ship."""
    rules = [{"rule_id": f"r{i}", "reject_reason_code": "(UNRESOLVED)",
              "module": "M", "condition_description": "something happened",
              "description": "Fires when something happened. REJECTED."}
             for i in range(3)]
    errors, warnings = rcd.validate(
        root=_store(tmp_path, _valid_document(rules={"rules": rules})))

    assert errors == []
    assert len([w for w in warnings if "(UNRESOLVED)" in w]) == 1
    assert rcd.lookup("(UNRESOLVED)", "E", root=tmp_path)["outcome"] == "miss"


def test_the_committed_store_carries_no_unresolved_rule_key():
    """The generator's `(CRE_REJECT_APPLICANT)` rules are stripped before a
    file is committed: no payload ever carries that key, so they only cost a
    warning at every boot. The validator still tolerates one in a fetched or
    mounted file; this catches a regenerated file committed as-is."""
    _, warnings = rcd.validate(root=COMMITTED_ROOT)
    assert not [w for w in warnings if "cannot match a payload" in w]


def test_a_file_publishing_nothing_warns(tmp_path):
    _, warnings = rcd.validate(
        root=_store(tmp_path, _valid_document(codes=[], rules={"rules": []})))
    assert any("publishes no addressable reason code" in w for w in warnings)


def test_coverage_names_reason_codes_with_a_runbook_and_no_docs(tmp_path,
                                                                monkeypatch):
    from src.utils import runbook_store

    drafts = tmp_path / "drafts"
    (drafts / "enu-biometric").mkdir(parents=True)
    (drafts / "enu-biometric" / "UNDOCUMENTED_CODE__E.json").write_text(
        "{}", encoding="utf-8")
    monkeypatch.setattr(runbook_store, "RUNBOOK_DRAFT_DIR", drafts)
    monkeypatch.setattr(runbook_store, "RUNBOOK_FINAL_DIR", tmp_path / "none")

    _, without = rcd.validate(root=FIXTURE_ROOT, coverage=False)
    _, with_coverage = rcd.validate(root=FIXTURE_ROOT, coverage=True)

    assert not any("UNDOCUMENTED_CODE" in w for w in without)
    assert any("UNDOCUMENTED_CODE has a runbook" in w for w in with_coverage)


# ---------------------------------------------------------------------------
# The CLI and the start-up check.
# ---------------------------------------------------------------------------

def test_the_cli_exits_zero_on_the_committed_store(capsys):
    from src.tools.check_reason_code_docs import main

    assert main(["--dir", str(COMMITTED_ROOT)]) == 0
    assert "The store is valid" in capsys.readouterr().out


def test_the_cli_exits_one_on_a_broken_store(tmp_path, capsys):
    from src.tools.check_reason_code_docs import main

    assert main(["--dir", str(_store(tmp_path, "{ broken"))]) == 1
    assert "ERROR:" in capsys.readouterr().err


def test_the_api_boots_past_a_broken_store_while_the_switch_is_off(tmp_path,
                                                                   monkeypatch):
    """Only a deployment that actually reads documents is stopped by them."""
    from src.main_api import validate_reason_code_docs

    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", _store(tmp_path, "{ broken"))
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    validate_reason_code_docs()  # must not raise or exit


def test_the_api_refuses_to_boot_on_a_broken_store(tmp_path, monkeypatch):
    from src.main_api import validate_reason_code_docs

    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", _store(tmp_path, "{ broken"))
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    with pytest.raises(SystemExit) as exit_info:
        validate_reason_code_docs()
    assert exit_info.value.code == 1


def test_the_api_boots_on_a_store_that_only_warns(tmp_path, monkeypatch):
    from src.main_api import validate_reason_code_docs

    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", COMMITTED_ROOT)
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    validate_reason_code_docs()


def test_the_api_does_not_check_the_disk_when_it_will_download(tmp_path,
                                                              monkeypatch):
    """There is nothing valid on disk yet at boot: the fetch runs in the
    background and validates what it fetched before swapping it in, and /ready
    waits for the first copy."""
    from src.main_api import validate_reason_code_docs

    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    monkeypatch.setenv(rcd.ENV_S3_DOWNLOAD, "true")
    monkeypatch.setenv("REASON_CODE_DOCS_DIR", str(tmp_path / "store"))
    monkeypatch.setenv("CASEBOOK_S3_BUCKET", "b")
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", _store(tmp_path, "{ broken"))

    validate_reason_code_docs()  # must not raise or exit


@pytest.mark.parametrize("missing", ["REASON_CODE_DOCS_DIR", "bucket"])
def test_the_api_refuses_to_boot_on_a_download_it_cannot_do(tmp_path, monkeypatch,
                                                           missing):
    """Left at the default directory the swap would replace the copy shipped
    inside `src/`; with no bucket there is nothing to fetch, and the store
    would silently stay empty."""
    from src.main_api import validate_reason_code_docs

    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    monkeypatch.setenv(rcd.ENV_S3_DOWNLOAD, "true")
    if missing == "REASON_CODE_DOCS_DIR":
        monkeypatch.delenv("REASON_CODE_DOCS_DIR", raising=False)
        monkeypatch.setenv("CASEBOOK_S3_BUCKET", "b")
    else:
        monkeypatch.setenv("REASON_CODE_DOCS_DIR", str(tmp_path / "store"))
        monkeypatch.delenv("CASEBOOK_S3_BUCKET", raising=False)
        monkeypatch.delenv("S3_LOGS_BUCKET", raising=False)

    with pytest.raises(SystemExit) as exit_info:
        validate_reason_code_docs()
    assert exit_info.value.code == 1


# ---------------------------------------------------------------------------
# The switch and the S3 placeholder.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    (None, False), ("false", False), ("true", True), ("1", True), ("yes", True),
])
def test_docs_enabled_is_off_unless_asked_for(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    else:
        monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", raw)
    assert rcd.docs_enabled() is expected


def test_the_s3_prefix_is_configurable(monkeypatch):
    monkeypatch.delenv("REASON_CODE_DOCS_S3_PREFIX", raising=False)
    assert rcd.s3_prefix() == rcd.DEFAULT_S3_PREFIX

    monkeypatch.setenv("REASON_CODE_DOCS_S3_PREFIX", "/other/place/")
    assert rcd.s3_prefix() == "other/place"


def test_the_download_does_nothing_while_it_is_switched_off():
    """Off by default: the files ship in the image, and a deployment that
    fetches them another way only points REASON_CODE_DOCS_DIR at them."""
    assert rcd.download_service_docs() is False


@pytest.mark.parametrize("raw,expected", [
    (None, False), ("false", False), ("true", True),
])
def test_the_download_switch(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv(rcd.ENV_S3_DOWNLOAD, raising=False)
    else:
        monkeypatch.setenv(rcd.ENV_S3_DOWNLOAD, raw)
    assert rcd.s3_download_enabled() is expected


@pytest.mark.parametrize("raw,expected", [
    (None, 0.0), ("", 0.0), ("not a number", 0.0), ("-5", 0.0), ("900", 900.0),
])
def test_the_refresh_interval(monkeypatch, raw, expected):
    """An unusable value means "only at start-up" rather than an error: a typo
    in a tunable must not stop the API booting."""
    if raw is None:
        monkeypatch.delenv(rcd.ENV_REFRESH, raising=False)
    else:
        monkeypatch.setenv(rcd.ENV_REFRESH, raw)
    assert rcd.refresh_seconds() == expected


# ---------------------------------------------------------------------------
# The download from S3 (MULTI_SERVICE_PLAN.md D10).
# ---------------------------------------------------------------------------

def _download_on(monkeypatch, tmp_path, fake, prefix="reason_code_docs"):
    """The download switched on, writing into `tmp_path`, reading from `fake`."""
    from tests import s3_fakes

    s3_fakes.install(monkeypatch, fake)
    monkeypatch.setenv(rcd.ENV_S3_DOWNLOAD, "true")
    monkeypatch.setenv("REASON_CODE_DOCS_S3_PREFIX", prefix)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", tmp_path / "store")
    return tmp_path / "store"


def _uploaded(fake, prefix, name, document):
    fake._store(f"{prefix}/{name}.json", json.dumps(document))


def test_a_downloaded_store_replaces_the_one_on_disk(tmp_path, monkeypatch):
    from tests.s3_fakes import FakeS3

    fake = FakeS3()
    root = _download_on(monkeypatch, tmp_path, fake)
    _uploaded(fake, "reason_code_docs", "svc", _valid_document())

    assert rcd.download_service_docs() is True
    assert (root / rcd.SERVICES_DIRNAME / "svc.json").is_file()
    assert rcd.lookup("SOME_CODE", "E")["outcome"] == "hit"


def test_a_store_uploaded_with_its_services_directory_downloads(tmp_path,
                                                                monkeypatch):
    """Either layout works: the files under the prefix, or under
    `<prefix>/services/` as the store keeps them on disk."""
    from tests.s3_fakes import FakeS3

    fake = FakeS3()
    root = _download_on(monkeypatch, tmp_path, fake)
    _uploaded(fake, "reason_code_docs/services", "svc", _valid_document())

    assert rcd.download_service_docs() is True
    assert (root / rcd.SERVICES_DIRNAME / "svc.json").is_file()


def test_objects_that_are_not_service_files_are_ignored(tmp_path, monkeypatch):
    from tests.s3_fakes import FakeS3

    fake = FakeS3()
    root = _download_on(monkeypatch, tmp_path, fake)
    _uploaded(fake, "reason_code_docs", "svc", _valid_document())
    fake._store("reason_code_docs/README.md", "not a service file")
    fake._store("reason_code_docs/nested/deeper/svc2.json", "{}")

    assert rcd.download_service_docs() is True
    assert [path.name for path in
            sorted((root / rcd.SERVICES_DIRNAME).iterdir())] == ["svc.json"]


def _last_good(root, monkeypatch):
    """A valid copy already on disk, as an earlier run would have left."""
    services = root / rcd.SERVICES_DIRNAME
    services.mkdir(parents=True, exist_ok=True)
    (services / "svc.json").write_text(
        json.dumps(_valid_document(
            codes=[{"numeric_code": 1, "reason_code": "SOME_CODE",
                    "description": "The copy that was already on disk.",
                    "category": "Technical", "is_retryable": True}])),
        encoding="utf-8")


@pytest.mark.parametrize("break_it,reason", [
    ("invalid", "the downloaded files do not validate"),
    ("empty", "the bucket holds no service files"),
    ("unreadable", "the objects cannot be read"),
])
def test_a_failed_download_keeps_the_last_good_copy(tmp_path, monkeypatch,
                                                   break_it, reason):
    """The whole point of staging: a bad or missing download costs the packets
    nothing, because the copy that was serving keeps serving."""
    from tests.s3_fakes import FakeS3

    fake = FakeS3(fail_reads=(break_it == "unreadable"))
    root = _download_on(monkeypatch, tmp_path, fake)
    _last_good(root, monkeypatch)
    if break_it == "invalid":
        _uploaded(fake, "reason_code_docs", "svc",
                  _valid_document(schema_version=99))
    elif break_it == "unreadable":
        _uploaded(fake, "reason_code_docs", "svc", _valid_document())

    assert rcd.download_service_docs() is False, reason
    state = rcd.lookup("SOME_CODE", "E")
    assert state["outcome"] == "hit"
    assert "already on disk" in state["text"]


def test_the_download_needs_a_bucket(tmp_path, monkeypatch):
    from tests.s3_fakes import FakeS3

    _download_on(monkeypatch, tmp_path, FakeS3())
    monkeypatch.delenv("CASEBOOK_S3_BUCKET", raising=False)
    monkeypatch.delenv("S3_LOGS_BUCKET", raising=False)

    assert rcd.download_service_docs() is False


def test_two_objects_naming_one_service_keep_the_last_good_copy(tmp_path,
                                                               monkeypatch):
    """The two layouts must not be mixed for one service: whichever won would
    depend on the listing order."""
    from tests.s3_fakes import FakeS3

    fake = FakeS3()
    root = _download_on(monkeypatch, tmp_path, fake)
    _last_good(root, monkeypatch)
    _uploaded(fake, "reason_code_docs", "svc", _valid_document())
    _uploaded(fake, "reason_code_docs/services", "svc", _valid_document())

    assert rcd.download_service_docs() is False
    assert "already on disk" in rcd.lookup("SOME_CODE", "E")["text"]


def test_docs_available_is_true_unless_the_download_has_nothing_yet(tmp_path,
                                                                   monkeypatch):
    """What /ready waits on: the first copy only. A refresh serves the copy it
    has, so it never unreadies a pod that was ready."""
    from tests.s3_fakes import FakeS3

    fake = FakeS3()
    root = _download_on(monkeypatch, tmp_path, fake)
    assert rcd.docs_available() is False

    _uploaded(fake, "reason_code_docs", "svc", _valid_document())
    assert rcd.download_service_docs() is True
    assert rcd.docs_available() is True

    monkeypatch.delenv(rcd.ENV_S3_DOWNLOAD, raising=False)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", tmp_path / "nothing here")
    assert rcd.docs_available() is True, "with the download off, the disk is all "\
                                        "there is and /ready must not wait"


def test_the_readiness_probe_waits_for_the_first_copy(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from src.api import routes
    from tests.s3_fakes import FakeS3

    fake = FakeS3()
    _download_on(monkeypatch, tmp_path, fake)
    monkeypatch.setattr(routes, "_check_kafka_producer_ready", lambda: True)
    monkeypatch.setattr("src.core.checkpointer.health_check", lambda: True)

    with pytest.raises(HTTPException) as raised:
        routes.readiness_check()
    assert raised.value.status_code == 503
    assert "reason-code" in str(raised.value.detail).lower()

    _uploaded(fake, "reason_code_docs", "svc", _valid_document())
    assert rcd.download_service_docs() is True
    assert routes.readiness_check() == {"status": "ready"}


def test_the_background_download_is_not_started_while_it_is_off(monkeypatch):
    monkeypatch.delenv(rcd.ENV_S3_DOWNLOAD, raising=False)
    assert rcd.start_background_download() is None


# ---------------------------------------------------------------------------
# Whose file answered: the packet's own service first (MULTI_SERVICE_PLAN.md
# D10).
# ---------------------------------------------------------------------------

def _two_service_store(tmp_path):
    """A store where both services document SHARED_CODE, and only `other`
    documents OTHER_ONLY."""
    _store(tmp_path, _valid_document(
        service="mine",
        codes=[{"numeric_code": 1, "reason_code": "SHARED_CODE",
                "description": "Raised by mine when the packet is unreadable.",
                "category": "Technical", "is_retryable": True}]),
        name="mine.json")
    _store(tmp_path, _valid_document(
        service="other",
        codes=[{"numeric_code": 2, "reason_code": "SHARED_CODE",
                "description": "Raised by other when the index is absent.",
                "category": "Technical", "is_retryable": True},
               {"numeric_code": 3, "reason_code": "OTHER_ONLY",
                "description": "Raised by other when the operator is unknown.",
                "category": "Technical", "is_retryable": False}]),
        name="other.json")
    return tmp_path


def test_the_packets_own_service_file_answers_alone(tmp_path):
    """Two services document the same code, and the packet gets its own
    service's account of it -- not both, which would ask the model to choose."""
    root = _two_service_store(tmp_path)

    state = rcd.lookup("SHARED_CODE", "E", root=root, service_file="mine")

    assert state["outcome"] == "hit"
    assert state["scope"] == rcd.SCOPE_OWN
    assert "unreadable" in state["text"]
    assert "index is absent" not in state["text"]
    assert [ref["source"] for ref in state["refs"]] == ["services/mine.json"]


def test_another_services_file_answers_only_when_the_own_file_is_silent(tmp_path):
    """A code the packet's own service does not document is still worth
    showing -- it may come from a shared library -- but never silently: the
    text says whose it is."""
    root = _two_service_store(tmp_path)

    state = rcd.lookup("OTHER_ONLY", "E", root=root, service_file="mine")

    assert state["outcome"] == "hit"
    assert state["scope"] == rcd.SCOPE_OTHER_SERVICE
    assert rcd.OTHER_SERVICE_NOTE.format(service="mine") in state["text"]
    assert "operator is unknown" in state["text"]


def test_without_a_service_file_every_file_answers_together(tmp_path):
    """The shape before services were known, kept for a caller that has no
    pack: both accounts, and the scope says so."""
    root = _two_service_store(tmp_path)

    state = rcd.lookup("SHARED_CODE", "E", root=root)

    assert state["scope"] == rcd.SCOPE_ALL
    assert {ref["source"] for ref in state["refs"]} == {"services/mine.json",
                                                       "services/other.json"}


def test_a_code_nobody_documents_is_a_miss_with_no_scope(tmp_path):
    root = _two_service_store(tmp_path)

    state = rcd.lookup("NOT_DOCUMENTED", "E", root=root, service_file="mine")

    assert state["outcome"] == "miss"
    assert state["scope"] is None


def test_the_own_file_documenting_another_type_stays_a_miss(tmp_path):
    """`own` covers the code, so the other services' files are not consulted:
    "documented, but not for this enrolment type" is the store's answer, and
    another service's rule would not fix it."""
    _store(tmp_path, _valid_document(
        service="mine", codes=[],
        rules={"total_rules": 1, "rules": [
            {"rule_id": "R1", "reject_reason_code": "TYPED_CODE",
             "module": "MAN_DEDUP",
             "condition_description": "the enrolment type is 'UPDATE'",
             "description": "Rejected when the enrolment type is 'UPDATE'."}]}),
        name="mine.json")
    _store(tmp_path, _valid_document(
        service="other",
        codes=[{"numeric_code": 9, "reason_code": "TYPED_CODE",
                "description": "Raised by other for any type.",
                "category": "Technical", "is_retryable": False}]),
        name="other.json")

    state = rcd.lookup("TYPED_CODE", "E", root=tmp_path, service_file="mine")

    assert state["outcome"] == "miss"
    assert state["requested_type"] == "E"


def test_the_pack_supplies_the_enrolment_type_words(tmp_path):
    """The labels in the rendered text are the service's own, so one service's
    "Update" is never printed in another's words."""
    _store(tmp_path, _valid_document(
        service="mine", codes=[],
        rules={"total_rules": 1, "rules": [
            {"rule_id": "R1", "reject_reason_code": "TYPED_CODE",
             "module": "MAN_DEDUP",
             "condition_description": "the enrolment type is 'UPDATE'",
             "description": "Rejected when the enrolment type is 'UPDATE'."}]}),
        name="mine.json")

    state = rcd.lookup("TYPED_CODE", "U", root=tmp_path, service_file="mine",
                       type_labels={"U": "Biometric Update (U)"},
                       type_families={"U": "U"})

    assert state["matched_type"] == "U"
    assert "Biometric Update (U)" in state["text"]
    # Without the pack's words, the neutral default.
    plain = rcd.lookup("TYPED_CODE", "U", root=tmp_path, service_file="mine")
    assert "Update (U)" in plain["text"]
    assert "Biometric" not in plain["text"]


def test_a_file_not_named_after_its_service_is_an_error(tmp_path):
    """The file name is how a packet's own file is found, so a mismatch would
    silently make every one of that service's codes another service's."""
    errors = _errors(tmp_path, _valid_document(service="svc"), name="other.json")

    assert any("must be named after its service" in error for error in errors)
