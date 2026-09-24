"""
opencode harness configuration invariants.

Two settings in this harness are shared across a language boundary, and both
had drifted. Neither failure is visible at run time: the harness catches every
exception from a task and falls back to the direct LLM, so a broken
configuration and a harness that is merely slow look identical in the logs.
That is what these tests are for -- the divergence has to be caught here,
because production will not announce it.
"""
import ast
import json
import re
from pathlib import Path

import pytest

from src.utils import opencode_runner

REPO_ROOT = Path(__file__).resolve().parent.parent
ENTRYPOINT = REPO_ROOT / "entrypoint.sh"


def _entrypoint_model_default() -> str:
    """The `OPENCODE_MODEL` fallback baked into entrypoint.sh."""
    text = ENTRYPOINT.read_text(encoding="utf-8")
    match = re.search(r'MODEL="\$\{OPENCODE_MODEL:-([^}]+)\}"', text)
    assert match, "entrypoint.sh no longer assigns MODEL from OPENCODE_MODEL"
    return match.group(1)


# ---------------------------------------------------------------------------
# The model default crosses a language boundary.
# ---------------------------------------------------------------------------

def test_entrypoint_model_default_matches_runner():
    """entrypoint.sh and opencode_runner must default to the same model.

    The first segment of the id is the PROVIDER key. entrypoint.sh writes the
    provider block in ~/.config/opencode/config.json under that key, and
    opencode_runner requests a model under that key via `--model`. Disagree,
    and the config declares a provider nothing asks for: every task fails and
    every node degrades to the direct LLM, silently.
    """
    assert _entrypoint_model_default() == opencode_runner.DEFAULT_MODEL


def test_model_default_carries_a_provider_segment():
    """A bare model name gives entrypoint.sh no provider key to write."""
    provider = opencode_runner._provider_of(opencode_runner.DEFAULT_MODEL)
    assert provider, (
        f"DEFAULT_MODEL {opencode_runner.DEFAULT_MODEL!r} has no 'provider/' "
        "prefix; entrypoint.sh would write a provider block named after the "
        "whole string."
    )
    assert "/" not in provider


def test_model_env_var_overrides_the_default(monkeypatch):
    monkeypatch.setenv(opencode_runner.ENV_MODEL, "someprovider/some-model")
    assert opencode_runner._model() == "someprovider/some-model"


# ---------------------------------------------------------------------------
# The task timeout has exactly one reader.
# ---------------------------------------------------------------------------

HARNESS_CALL_SITES = (
    REPO_ROOT / "src" / "core" / "agent_orchestrator.py",
    REPO_ROOT / "src" / "dlt" / "orchestrator.py",
)


def test_only_opencode_runner_reads_the_task_timeout():
    """No call site may carry its own OPENCODE_TASK_TIMEOUT_SECONDS default.

    All four harness call sites used to read the variable themselves, and the
    defaults did not agree: the rejection Investigator used 120s while the
    other three used 300s. With the variable unset, the shortest budget landed
    on the heaviest task -- the one that reads the documentation corpus from
    cold -- so it timed out first and fell back to the direct LLM.
    """
    for path in HARNESS_CALL_SITES:
        text = path.read_text(encoding="utf-8")
        # Comments may name the variable; code may not read it.
        code_lines = [
            line for line in text.splitlines()
            if "OPENCODE_TASK_TIMEOUT_SECONDS" in line
            and not line.lstrip().startswith("#")
        ]
        assert not code_lines, (
            f"{path.name} reads OPENCODE_TASK_TIMEOUT_SECONDS directly: "
            f"{code_lines}. Pass no timeout and let "
            "opencode_runner._task_timeout() decide, so all call sites agree."
        )


def test_no_harness_call_site_passes_its_own_timeout():
    """`run_task_json(timeout=...)` reintroduces the per-site divergence."""
    for path in HARNESS_CALL_SITES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute)
                    and func.attr in ("run_task", "run_task_json")):
                continue
            passed = {kw.arg for kw in node.keywords}
            assert "timeout" not in passed, (
                f"{path.name}:{node.lineno} passes an explicit timeout to "
                f"{func.attr}(); let _task_timeout() be the single reader."
            )


def test_task_timeout_default_and_override(monkeypatch):
    monkeypatch.delenv("OPENCODE_TASK_TIMEOUT_SECONDS", raising=False)
    assert opencode_runner._task_timeout() == opencode_runner.DEFAULT_TIMEOUT_SECONDS

    monkeypatch.setenv("OPENCODE_TASK_TIMEOUT_SECONDS", "45")
    assert opencode_runner._task_timeout() == 45


# ---------------------------------------------------------------------------
# The corpus-wait loop must not crash on the path it exists to handle.
# ---------------------------------------------------------------------------

def test_harness_nodes_can_sleep_while_waiting_for_the_corpus():
    """Both Investigator nodes poll `corpus_available()` with `time.sleep`.

    `agent_orchestrator` called `time.sleep(1)` without importing `time`, so
    the NameError fired exactly when the loop mattered -- harness on, corpus
    still downloading, packet already arriving -- and propagated out of the
    node to DLQ the packet. The DLT lane imported `time` locally and was fine,
    which is why only one of the two ever failed.
    """
    import src.core.agent_orchestrator as rejection_graph
    import src.dlt.orchestrator as dlt_graph

    for module in (rejection_graph, dlt_graph):
        source = Path(module.__file__).read_text(encoding="utf-8")
        if "time.sleep" not in source:
            continue
        tree = ast.parse(source, filename=module.__file__)
        imports_time = any(
            isinstance(node, (ast.Import, ast.ImportFrom))
            and any(alias.name.split(".")[0] == "time" for alias in node.names)
            for node in ast.walk(tree)
        )
        assert imports_time, (
            f"{module.__name__} calls time.sleep but never imports time"
        )


# ---------------------------------------------------------------------------
# The sandbox is part of the contract, not an incidental default.
# ---------------------------------------------------------------------------

def test_agent_cannot_shell_out_or_reach_the_network():
    """The agent's world is the filesystem and the one file it writes."""
    permissions = opencode_runner._permissions()
    assert permissions["bash"] == "deny"
    assert permissions["webfetch"] == "deny"


def test_harness_is_off_unless_explicitly_enabled(monkeypatch):
    monkeypatch.delenv(opencode_runner.ENV_DISABLE, raising=False)
    assert opencode_runner.is_enabled() is False

    monkeypatch.setenv(opencode_runner.ENV_DISABLE, "true")
    assert opencode_runner.is_enabled() is True


def test_run_task_refuses_when_the_harness_is_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv(opencode_runner.ENV_DISABLE, "false")
    with pytest.raises(opencode_runner.OpencodeUnavailable):
        opencode_runner.run_task("prompt", str(tmp_path / "out.json"))


# ---------------------------------------------------------------------------
# .env.example is what operators copy; it must not undo the defaults above.
# ---------------------------------------------------------------------------

def _env_example_value(key: str) -> str:
    text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    match = re.search(rf"^{key}=(.*)$", text, flags=re.MULTILINE)
    assert match, f".env.example no longer sets {key}"
    return match.group(1).strip()


def test_env_example_matches_the_runner_defaults():
    """.env.example carried `opencode/...` and 120s, the exact provider
    mismatch and short budget the code defaults were changed to remove."""
    assert _env_example_value("OPENCODE_MODEL") == opencode_runner.DEFAULT_MODEL
    assert (int(_env_example_value("OPENCODE_TASK_TIMEOUT_SECONDS"))
            == opencode_runner.DEFAULT_TIMEOUT_SECONDS)


# ---------------------------------------------------------------------------
# Concurrent tasks must never share a prompt file.
# ---------------------------------------------------------------------------

_FAKE_OPENCODE = """#!{python}
import json, re, sys
task = sys.argv[-1]
prompt_file = re.search(r"Read the file at (.+?) and follow", task).group(1)
output_path = task.rsplit(": ", 1)[1]
with open(prompt_file, encoding="utf-8") as handle:
    prompt = handle.read()
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump({{"prompt": prompt, "prompt_file": prompt_file}}, handle)
"""


def test_each_case_gets_its_own_prompt_file(monkeypatch, tmp_path):
    """The prompt file was named after the output's basename alone, which is
    the same for every case ("investigation.json"), in one shared directory.
    With MAX_CONCURRENT_INVESTIGATIONS tasks in flight, one task could read
    another case's instructions."""
    import sys

    fake = tmp_path / "opencode"
    fake.write_text(_FAKE_OPENCODE.format(python=sys.executable), encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv(opencode_runner.ENV_DISABLE, "true")
    monkeypatch.setenv(opencode_runner.ENV_BINARY, str(fake))

    results = {}
    for case in ("casebook_a", "casebook_b"):
        output = tmp_path / case / "investigation.json"
        output.parent.mkdir()
        results[case] = opencode_runner.run_task_json(
            f"instructions for {case}", str(output))["result"]

    for case, result in results.items():
        assert result["prompt"] == f"instructions for {case}"
        assert Path(result["prompt_file"]).parent == tmp_path / case
    assert results["casebook_a"]["prompt_file"] != results["casebook_b"]["prompt_file"]


# ---------------------------------------------------------------------------
# Both Investigator paths describe the enrolment type the same way.
# ---------------------------------------------------------------------------

def test_every_payload_enrolment_code_has_one_description():
    """The harness and direct paths each carried their own map, and they
    disagreed: no "E" on the direct path, no "Z" on the harness path, and
    two different descriptions of "U"."""
    import src.core.agent_orchestrator as orch
    from src.tools.tool_registry import _ENROLMENT_TYPE_ALIASES

    payload_codes = {code for code in _ENROLMENT_TYPE_ALIASES if len(code) == 1}
    assert payload_codes <= set(orch.ENROLMENT_TYPE_DISPLAY)

    def display(code):
        return orch.enrolment_type_display({"packetMetaData": {"enrolmentType": code}})

    assert display("E") == display("N") == display(" n ")
    assert "1:N" in display("U") and "1:1" in display("U")
    assert display("X") == "X"
    assert orch.enrolment_type_display({"packetMetaData": None}) == "Unknown"


# ---------------------------------------------------------------------------
# The harness Reviewers: their own evidence, a real fallback, and the
# learning-rule loop.
# ---------------------------------------------------------------------------

class _StubAgent:
    def __init__(self, reply="APPROVED"):
        self.reply = reply
        self.calls = 0

    def invoke(self, _messages):
        from langchain_core.messages import AIMessage
        self.calls += 1
        return {"messages": [AIMessage(content=self.reply)]}


def _rejection_reviewer(monkeypatch, agent):
    from unittest.mock import MagicMock
    import src.core.agent_orchestrator as orch

    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "_prompt_fingerprint", orch._prompt_fingerprint)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "create_react_agent", lambda *a, **k: agent)
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    graph = orch._build_agent()
    return graph.builder.nodes["review"].runnable.func


def _dlt_reviewer(monkeypatch, agent):
    from unittest.mock import MagicMock
    import src.dlt.orchestrator as dlt

    monkeypatch.setattr(dlt, "_agent", None)
    monkeypatch.setattr(dlt, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(dlt, "create_react_agent", lambda *a, **k: agent)
    graph = dlt._build_dlt_agent()
    return graph.builder.nodes["review"].runnable.func


_REJECTION_STATE = {
    "payload": {"eventId": "evt-1",
                "packetMetaData": {"enrolmentType": "E"}},
    "logs": "a log line",
    "db_rule": "the rule",
    "investigation": "the findings",
    "retry_count": 0,
}


def _harness_on(monkeypatch, tmp_path, verdict):
    import src.utils.paths as paths

    monkeypatch.setenv(opencode_runner.ENV_DISABLE, "true")
    monkeypatch.setattr(paths, "LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr(opencode_runner, "run_task_json",
                        lambda prompt, output_path, node=None: {
                            "result": verdict, "seconds": 0,
                            "trace": {"llm_calls": 1, "tools": {},
                                      "tokens": {}, "cost": 0.0,
                                      "session_id": "ses_test"}})


def test_rejection_reviewer_writes_its_own_evidence(monkeypatch, tmp_path):
    """The Reviewer wrote investigation_text.txt into a case directory it
    assumed existed, outside its try block: a missing directory raised out
    of the node instead of falling back."""
    review = _rejection_reviewer(monkeypatch, _StubAgent())
    _harness_on(monkeypatch, tmp_path, {"verdict": "APPROVED", "feedback": ""})

    result = review(dict(_REJECTION_STATE))

    case_dir = tmp_path / "casebook_evt-1"
    assert result["reviewer_feedback"] == "APPROVED"
    assert (case_dir / "investigation_text.txt").read_text(encoding="utf-8") == "the findings"
    context = json.loads((case_dir / "context.json").read_text(encoding="utf-8"))
    assert context["db_rule"] == "the rule"
    assert context["enrolment_type"].startswith("New Enrolment")


def test_rejection_reviewer_falls_back_when_the_case_dir_is_unwritable(monkeypatch, tmp_path):
    stub = _StubAgent("APPROVED")
    review = _rejection_reviewer(monkeypatch, stub)
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("", encoding="utf-8")
    _harness_on(monkeypatch, blocker, {"verdict": "APPROVED", "feedback": ""})

    result = review(dict(_REJECTION_STATE))

    assert result["reviewer_feedback"] == "APPROVED"
    assert stub.calls == 1, "the direct LLM should have taken over"


def test_harness_reviewer_rule_reaches_the_validated_queue(monkeypatch, tmp_path):
    """The harness Reviewer has no add_learning_rule tool, so while the
    harness was on the self-learning loop received nothing."""
    import src.core.agent_orchestrator as orch

    proposed = []

    def refuse(rule_text):
        # Refused, so nothing is appended to the real pending_rules.jsonl.
        proposed.append(rule_text)
        return ["refused by the test"]

    monkeypatch.setattr(orch, "validate_learning_rule", refuse)
    review = _rejection_reviewer(monkeypatch, _StubAgent())
    _harness_on(monkeypatch, tmp_path, {
        "verdict": "REJECTED",
        "feedback": "wrong enrolment type",
        "learning_rule": {"rule_text": "Always state the enrolment type.",
                          "reasoning": "it was missing"},
    })

    result = review(dict(_REJECTION_STATE))

    assert result["reviewer_feedback"] == "wrong enrolment type"
    assert proposed == ["Always state the enrolment type."]


def test_an_approval_proposes_no_rule(monkeypatch, tmp_path):
    import src.core.agent_orchestrator as orch

    proposed = []
    monkeypatch.setattr(orch, "validate_learning_rule",
                        lambda text: proposed.append(text) or ["refused"])
    review = _rejection_reviewer(monkeypatch, _StubAgent())
    _harness_on(monkeypatch, tmp_path, {
        "verdict": "APPROVED", "feedback": "",
        "learning_rule": {"rule_text": "ignored", "reasoning": ""},
    })

    review(dict(_REJECTION_STATE))

    assert proposed == []


def test_dlt_reviewer_writes_its_own_evidence(monkeypatch, tmp_path):
    review = _dlt_reviewer(monkeypatch, _StubAgent())
    _harness_on(monkeypatch, tmp_path, {"verdict": "APPROVED", "feedback": ""})

    result = review({"case_id": "ref-1", "failure": {"root_fqcn": "x.Y"},
                     "investigation": "the findings", "retry_count": 0})

    case_dir = tmp_path / "casebook_ref-1"
    assert result["reviewer_feedback"] == "APPROVED"
    assert (case_dir / "dlt_investigation_text.txt").exists()
    assert json.loads((case_dir / "dlt_failure.json").read_text(encoding="utf-8")) == {"root_fqcn": "x.Y"}
    assert (case_dir / "dlt_evidence.txt").exists()


# ---------------------------------------------------------------------------
# The task trace: how many LLM calls, which tools, how many tokens.
# ---------------------------------------------------------------------------
#
# A harness task is an agentic loop -- several LLM round-trips, a tool call
# between each -- and none of that was visible. Piped to a subprocess with no
# TTY, `opencode run` prints the final assistant text and nothing else, so a
# task that burned twenty calls and a task that burned two logged the same
# single line, and the four nodes metered nothing at all while the harness
# was on.

#: One real `opencode run --format json` stdout stream, captured from a task
#: that made two LLM calls around a single `write`. Trimmed to the fields the
#: parser reads.
_EVENT_STREAM = [
    {"type": "step_start", "sessionID": "ses_abc",
     "part": {"type": "step-start"}},
    {"type": "text", "sessionID": "ses_abc",
     "part": {"type": "text", "text": "Writing the output file now."}},
    {"type": "tool_use", "sessionID": "ses_abc",
     "part": {"type": "tool", "tool": "grep",
              "state": {"status": "completed", "input": {"pattern": "RC-501"}}}},
    {"type": "tool_use", "sessionID": "ses_abc",
     "part": {"type": "tool", "tool": "write",
              "state": {"status": "completed",
                        "input": {"filePath": "investigation.json"}}}},
    {"type": "step_finish", "sessionID": "ses_abc",
     "part": {"type": "step-finish", "reason": "tool-calls", "cost": 0.5,
              "tokens": {"input": 4211, "output": 37, "reasoning": 5,
                         "cache": {"read": 64, "write": 8}}}},
    {"type": "step_start", "sessionID": "ses_abc",
     "part": {"type": "step-start"}},
    {"type": "text", "sessionID": "ses_abc",
     "part": {"type": "text", "text": "Done."}},
    {"type": "step_finish", "sessionID": "ses_abc",
     "part": {"type": "step-finish", "reason": "stop", "cost": 0.25,
              "tokens": {"input": 4390, "output": 9, "reasoning": 0,
                         "cache": {"read": 0, "write": 0}}}},
]


def _trace_of(events):
    trace = opencode_runner._Trace()
    for event in events:
        trace.add(event)
    return trace


def test_trace_counts_one_llm_call_per_step():
    """`step_start` brackets a round-trip; the task is the loop around them."""
    assert _trace_of(_EVENT_STREAM).summary()["llm_calls"] == 2


def test_trace_sums_tokens_and_cost_across_steps():
    summary = _trace_of(_EVENT_STREAM).summary()
    assert summary["tokens"] == {"input": 8601, "output": 46, "reasoning": 5,
                                 "cache_read": 64, "cache_write": 8}
    assert summary["cost"] == 0.75
    assert summary["session_id"] == "ses_abc"


def test_trace_counts_tool_calls_by_name():
    assert _trace_of(_EVENT_STREAM).summary()["tools"] == {"grep": 1, "write": 1}


def test_trace_keeps_the_agents_last_words_for_the_failure_message():
    """With `--format json` the last stdout line is a `step_finish` envelope,
    which says nothing about why a task wrote no output."""
    assert _trace_of(_EVENT_STREAM).last_text == "Done."


def test_trace_notices_a_failed_tool_call():
    trace = _trace_of([
        {"type": "tool_use", "sessionID": "ses_abc",
         "part": {"type": "tool", "tool": "read",
                  "state": {"status": "error", "input": {"filePath": "gone.md"}}}},
    ])
    assert trace.tool_errors == ["read: error"]


def test_trace_survives_a_malformed_stream():
    """A truncated line or a missing field must not take the task down with
    it -- the harness already falls back to the direct LLM on any exception,
    and losing an investigation to a log-parsing bug would be absurd."""
    trace = _trace_of([
        {"type": "step_finish", "part": {"tokens": {"input": "nonsense"}}},
        {"type": "step_finish", "part": {"cost": None, "tokens": None}},
        {"type": "tool_use", "part": {}},
        {"type": "unheard_of", "part": {"type": "something-new"}},
        {"type": "text", "part": {}},
    ])
    assert trace.summary()["tokens"]["input"] == 0
    assert trace.summary()["tools"] == {"unknown": 1}


def test_only_json_event_lines_are_parsed_as_events():
    """Everything else is opencode's own diagnostics: a provider error, a
    stack trace, a startup warning. Those are the only channel a failure the
    event stream never reaches arrives on, so they must stay loggable."""
    assert opencode_runner._parse_event("Error: provider unreachable") is None
    assert opencode_runner._parse_event("{not json") is None
    assert opencode_runner._parse_event('{"no":"type field"}') is None
    assert opencode_runner._parse_event('{"type":"text"}') == {"type": "text"}


def test_tool_detail_names_what_the_call_asked_for():
    detail = opencode_runner._tool_detail(
        {"state": {"input": {"pattern": "RESIDENT_BIOMETRIC_UPDATE"}}})
    assert detail == "pattern=RESIDENT_BIOMETRIC_UPDATE"


# ---------------------------------------------------------------------------
# The trace reaches the process boundary: argv, metrics, result.
# ---------------------------------------------------------------------------

#: The runner merges the child's stderr into the stdout pipe it parses, so
#: the fake records its argv to a file beside the output instead.
_FAKE_STREAMING_OPENCODE = """#!{python}
import json, re, sys
task = sys.argv[-1]
prompt_file = re.search(r"Read the file at (.+?) and follow", task).group(1)
output_path = task.rsplit(": ", 1)[1]
with open(output_path + ".argv.json", "w", encoding="utf-8") as handle:
    json.dump(sys.argv, handle)
for event in {events}:
    print(json.dumps(event), flush=True)
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump({{"investigation": "done"}}, handle)
"""


def _streaming_binary(tmp_path, monkeypatch):
    import sys

    fake = tmp_path / "opencode"
    fake.write_text(
        _FAKE_STREAMING_OPENCODE.format(python=sys.executable,
                                        events=repr(_EVENT_STREAM)),
        encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv(opencode_runner.ENV_DISABLE, "true")
    monkeypatch.setenv(opencode_runner.ENV_BINARY, str(fake))
    return fake


def test_run_task_returns_the_trace_it_parsed(monkeypatch, tmp_path):
    _streaming_binary(tmp_path, monkeypatch)
    output = tmp_path / "casebook_x" / "investigation.json"
    output.parent.mkdir()

    result = opencode_runner.run_task_json("instructions", str(output))

    assert result["trace"]["llm_calls"] == 2
    assert result["trace"]["tools"] == {"grep": 1, "write": 1}
    assert result["trace"]["tokens"]["input"] == 8601
    assert result["result"] == {"investigation": "done"}


def test_a_harness_task_meters_itself_under_its_node(monkeypatch, tmp_path):
    """Nothing metered the harness path: `record_llm_usage` reads
    `usage_metadata` off a LangChain response, and a harness task returns a
    file. Both nodes reported zero while doing all the work."""
    from src.utils import metrics

    recorded = []
    monkeypatch.setattr(metrics, "record_harness_usage",
                        lambda node, trace: recorded.append((node, trace)))
    _streaming_binary(tmp_path, monkeypatch)
    output = tmp_path / "casebook_x" / "investigation.json"
    output.parent.mkdir()

    opencode_runner.run_task_json("instructions", str(output),
                                  node="investigator")

    assert [node for node, _ in recorded] == ["investigator"]
    assert recorded[0][1]["llm_calls"] == 2


def test_a_task_that_meters_nothing_is_not_an_error(monkeypatch, tmp_path):
    """No `node`, no metrics -- and no crash. The runner is also called from
    tests and tools that have no graph node to label."""
    _streaming_binary(tmp_path, monkeypatch)
    output = tmp_path / "casebook_x" / "investigation.json"
    output.parent.mkdir()

    assert opencode_runner.run_task_json("instructions", str(output))["trace"]


def _argv_of_one_run(monkeypatch, tmp_path):
    _streaming_binary(tmp_path, monkeypatch)
    output = tmp_path / "casebook_x" / "investigation.json"
    output.parent.mkdir()
    opencode_runner.run_task_json("instructions", str(output))
    return json.loads(Path(str(output) + ".argv.json").read_text(encoding="utf-8"))


def test_the_task_asks_for_the_json_event_stream(monkeypatch, tmp_path):
    """Without `--format json` stdout carries the final assistant text alone:
    no steps, no tools, no tokens."""
    argv = _argv_of_one_run(monkeypatch, tmp_path)
    assert argv[argv.index("--format") + 1] == "json"


def test_the_json_format_flag_precedes_the_attach_splice(monkeypatch, tmp_path):
    """`--attach` is spliced in at argv[2:2], immediately after `run`. A flag
    added ahead of that index would be silently displaced."""
    argv = _argv_of_one_run(monkeypatch, tmp_path)
    assert argv[1] == "run"
    assert argv.index("--format") > 1


def test_the_task_never_passes_title(monkeypatch, tmp_path):
    """`--title` would stamp the event id on the session and skip opencode's
    per-task title-generation LLM call, but on opencode 1.18.20 it hangs
    `run` at startup before it reaches the model -- reproducibly, with the
    same invocation succeeding once the flag is removed. The session id in
    the trace is the correlation handle instead."""
    assert "--title" not in _argv_of_one_run(monkeypatch, tmp_path)


def test_every_harness_call_site_labels_its_node():
    """An unlabelled call site is a node that silently meters nothing, which
    is the state all four were in."""
    expected = {"investigator", "reviewer", "dlt_investigator", "dlt_reviewer"}
    found = set()
    for path in HARNESS_CALL_SITES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            if not (isinstance(func, ast.Attribute)
                    and func.attr in ("run_task", "run_task_json")):
                continue
            node = next((kw.value for kw in call.keywords if kw.arg == "node"), None)
            assert isinstance(node, ast.Constant), (
                f"{path.name}:{call.lineno} calls {func.attr}() without a "
                "literal node= label; its LLM calls and tokens go unrecorded."
            )
            found.add(node.value)
    assert found == expected


# ---------------------------------------------------------------------------
# One switch per lane, and the shell must reach the same answer.
# ---------------------------------------------------------------------------
#
# The rejection lane moves off opencode (REASON_CODE_DOCS_PLAN.md) while the
# DLT lane stays on it, so a single global switch can no longer express the
# deployment. A lane's own switch wins when it holds a non-empty value;
# otherwise the lane inherits the older single switch. Whitespace around a
# value is now ignored on both sides of the language boundary -- it was not
# before, and `USE_OPENCODE_HARNESS=" true "` turned nothing on anywhere.

#: (legacy value, lane value, expected). `None` means the variable is unset.
_LANE_CASES = (
    (None, None, False),
    (None, "", False),
    (None, "true", True),
    (None, "TRUE", True),
    (None, " true ", True),
    (None, "false", False),
    ("true", None, True),
    ("true", "", True),
    ("true", "true", True),
    ("true", "TRUE", True),
    ("true", " true ", True),
    ("true", "false", False),
    ("false", None, False),
    ("false", "", False),
    ("false", "true", True),
    ("false", "TRUE", True),
    ("false", " true ", True),
    ("false", "false", False),
    (" TRUE ", None, True),
)


def _set_switches(monkeypatch, legacy, rejection, dlt):
    for name, value in ((opencode_runner.ENV_DISABLE, legacy),
                        (opencode_runner.ENV_LANES["rejection"], rejection),
                        (opencode_runner.ENV_LANES["dlt"], dlt)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)


@pytest.mark.parametrize("legacy,lane,expected", _LANE_CASES)
@pytest.mark.parametrize("lane_name", sorted(opencode_runner.ENV_LANES))
def test_a_lane_switch_wins_over_the_legacy_one(monkeypatch, lane_name,
                                                legacy, lane, expected):
    other = next(n for n in opencode_runner.ENV_LANES if n != lane_name)
    values = {lane_name: lane, other: None}
    _set_switches(monkeypatch, legacy, values["rejection"], values["dlt"])
    assert opencode_runner.lane_enabled(lane_name) is expected


def test_the_other_lane_is_unaffected(monkeypatch):
    """The point of the split: one lane off while the other stays on."""
    _set_switches(monkeypatch, None, "false", "true")
    assert opencode_runner.lane_enabled("rejection") is False
    assert opencode_runner.lane_enabled("dlt") is True


@pytest.mark.parametrize("rejection,dlt,expected", [
    (None, None, False),
    ("false", "false", False),
    ("true", "false", True),
    ("false", "true", True),
    ("true", "true", True),
])
def test_is_enabled_means_some_lane_needs_the_server(monkeypatch, rejection,
                                                     dlt, expected):
    _set_switches(monkeypatch, None, rejection, dlt)
    assert opencode_runner.is_enabled() is expected


def test_an_unknown_lane_is_a_programming_error():
    with pytest.raises(ValueError):
        opencode_runner.lane_enabled("other")


def test_each_orchestrator_asks_only_about_its_own_lane():
    """A node reading `is_enabled()` takes the harness path whenever the OTHER
    lane is on opencode -- which is exactly the target production shape."""
    expected = {
        REPO_ROOT / "src" / "core" / "agent_orchestrator.py": "rejection",
        REPO_ROOT / "src" / "dlt" / "orchestrator.py": "dlt",
    }
    for path, lane in expected.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        lanes_asked, calls_is_enabled = set(), False
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id == "lane_enabled":
                    assert node.args and isinstance(node.args[0], ast.Constant), (
                        f"{path.name}:{node.lineno} calls lane_enabled() "
                        "without a literal lane name.")
                    lanes_asked.add(node.args[0].value)
                elif node.func.id in ("is_enabled", "harness_enabled"):
                    calls_is_enabled = True
            if isinstance(node, ast.ImportFrom) and node.module and \
                    node.module.endswith("opencode_runner"):
                for alias in node.names:
                    assert alias.name != "is_enabled", (
                        f"{path.name}:{node.lineno} imports is_enabled; a node "
                        f"must ask lane_enabled({lane!r}) about its own lane.")
        assert lanes_asked == {lane}, f"{path.name} asks about {lanes_asked}"
        assert not calls_is_enabled, f"{path.name} still calls is_enabled()"


# ---------------------------------------------------------------------------
# entrypoint.sh resolves the lanes exactly as Python does.
# ---------------------------------------------------------------------------

def _harness_lanes_block() -> str:
    text = ENTRYPOINT.read_text(encoding="utf-8")
    match = re.search(r"# BEGIN harness-lanes\n(.*?)# END harness-lanes",
                      text, flags=re.DOTALL)
    assert match, "entrypoint.sh no longer marks the harness-lanes block"
    return match.group(1)


def test_entrypoint_is_syntactically_valid():
    import shutil
    import subprocess

    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash is not available")
    assert subprocess.run([bash, "-n", str(ENTRYPOINT)]).returncode == 0


@pytest.mark.parametrize("legacy,lane,expected", _LANE_CASES)
def test_the_shell_and_python_agree_on_every_switch_value(monkeypatch, legacy,
                                                          lane, expected):
    """The shell writes the provider config and Python asks for the model.
    A value the two read differently means a config for a provider nothing
    requests: every task fails and every node degrades to the direct LLM."""
    import os
    import shutil
    import subprocess

    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash is not available")

    # The rejection lane carries the case; the DLT lane is left to inherit,
    # so one run covers both a set and an unset lane switch.
    _set_switches(monkeypatch, legacy, lane, None)
    script = _harness_lanes_block() + '\necho "$HARNESS_REJECTION|$HARNESS_DLT"'
    completed = subprocess.run([bash, "-c", script], capture_output=True,
                               text=True, env=dict(os.environ))
    assert completed.returncode == 0, completed.stderr
    shell_rejection, shell_dlt = completed.stdout.strip().split("|")

    assert (shell_rejection == "true") is expected
    assert (shell_rejection == "true") is opencode_runner.lane_enabled("rejection")
    assert (shell_dlt == "true") is opencode_runner.lane_enabled("dlt")


def test_env_example_ships_both_lane_switches_off():
    """A lane on without the binary leaves /ready waiting for a server that
    never starts."""
    for variable in opencode_runner.ENV_LANES.values():
        assert _env_example_value(variable) == "false"
