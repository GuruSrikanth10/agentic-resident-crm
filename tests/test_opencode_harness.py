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
