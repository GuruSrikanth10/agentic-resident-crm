"""
opencode harness runner — adapted from DROA's opencode_runner.py.

Runs opencode as a subprocess to perform LLM-driven investigation work.
One long-lived `opencode serve` process is started at API startup and
shared across all concurrent packets. Each task is a fresh `opencode run
--attach` invocation — a fresh conversation, no context bleed between cases.

The contract: "run this prompt against this directory; the agent writes
its output to a file on disk; the runner reads it back."

Feature-flagged per lane: USE_OPENCODE_HARNESS_REJECTION and
USE_OPENCODE_HARNESS_DLT, each falling back to the single older switch
USE_OPENCODE_HARNESS when its own value is unset. A lane whose switch is off
uses the existing direct ChatOpenAI path.
"""
import contextlib
import json
import os
import re
import secrets
import subprocess
import threading
import time
from typing import Any, Dict, Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

_ANSI = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')

ENV_BINARY = "OPENCODE_BINARY"
ENV_MODEL = "OPENCODE_MODEL"
ENV_DISABLE = "USE_OPENCODE_HARNESS"

#: The per-lane switches. Each lane can run on the harness independently, so
#: the rejection lane can move to the direct path with reason-code
#: documentation while the DLT lane stays on opencode. A lane with no value of
#: its own inherits ENV_DISABLE, so a deployment that sets only the old switch
#: behaves exactly as it did before.
ENV_LANES = {
    "rejection": "USE_OPENCODE_HARNESS_REJECTION",
    "dlt": "USE_OPENCODE_HARNESS_DLT",
}

#: The model every task requests when OPENCODE_MODEL is unset.
#:
#: The first segment is the PROVIDER key, and entrypoint.sh writes the
#: provider block in ~/.config/opencode/config.json under that same key. The
#: two defaults must therefore be identical: if they disagree, the generated
#: config declares a provider nothing asks for, every task fails, and every
#: node falls back to the direct LLM -- a silent degradation that reads as
#: "the harness is doing nothing" rather than as a broken configuration.
#: tests/test_opencode_harness.py asserts they stay equal.
DEFAULT_MODEL = "uidai/glm-5.2-fp8"

#: Wall-clock budget for one task. Read via _task_timeout(), which is the
#: ONLY reader of OPENCODE_TASK_TIMEOUT_SECONDS: the four harness call sites
#: pass no timeout of their own, so all four agree by construction. They did
#: not always -- the rejection Investigator carried its own 120s default while
#: the other three carried 300s, putting the shortest budget on the task that
#: reads the documentation corpus from cold.
DEFAULT_TIMEOUT_SECONDS = 300


class OpencodeUnavailable(Exception):
    """Raised when the harness cannot run."""


def _switched_on(raw: Optional[str]) -> bool:
    """One reading of a harness switch, shared with entrypoint.sh.

    Surrounding whitespace is stripped before the comparison. The shell side
    (the `harness-lanes` block in entrypoint.sh) normalises the same way, and
    tests/test_opencode_harness.py runs that block and compares it with this
    function -- a value that turns the Python side on and leaves the shell
    side off writes no provider config and fails every task.
    """
    return str(raw or "").strip().lower() == "true"


def lane_enabled(lane: str) -> bool:
    """Whether `lane` ("rejection" or "dlt") runs on the opencode harness.

    The lane's own switch wins whenever it holds a non-empty value; otherwise
    the lane inherits ENV_DISABLE, which is unset by default and therefore off.
    """
    try:
        variable = ENV_LANES[lane]
    except KeyError:
        raise ValueError(
            f"Unknown harness lane {lane!r}; expected one of {sorted(ENV_LANES)}."
        ) from None

    own = os.environ.get(variable, "")
    if own.strip():
        return _switched_on(own)
    return _switched_on(os.environ.get(ENV_DISABLE))


def is_enabled() -> bool:
    """True when ANY lane uses the harness, i.e. the server is needed.

    This is the condition for downloading the corpus, starting
    `opencode serve`, gating /ready on both, and writing the provider config.
    It is NOT the condition for a given node taking the harness path -- that
    is `lane_enabled(<lane>)`.
    """
    return any(lane_enabled(lane) for lane in ENV_LANES)


def _binary() -> Optional[str]:
    raw = os.environ.get(ENV_BINARY, "").strip()
    if raw:
        return raw
    import shutil
    return shutil.which("opencode")


def _model() -> str:
    return os.environ.get(ENV_MODEL, DEFAULT_MODEL).strip()


def _provider_of(model_name: str) -> str:
    name = str(model_name or "")
    return name.split("/", 1)[0].strip() if "/" in name else ""


def _harness_config(model_name: str) -> Dict[str, Any]:
    """opencode's own timeouts for this run, as `OPENCODE_CONFIG_CONTENT`.

    Returns an empty dict to skip overriding the operator's
    `~/.config/opencode` config. The provider config (baseURL, model
    names, npm package) is already set there; sending a partial config
    here replaces it and loses the baseURL, causing 'Request is not
    supported by this version of OpenCode Server' errors.
    """
    return {}


def _permissions() -> Dict[str, Any]:
    return {
        "bash": "deny",
        "webfetch": "deny",
    }


def _task_timeout() -> int:
    return int(os.environ.get("OPENCODE_TASK_TIMEOUT_SECONDS",
                              str(DEFAULT_TIMEOUT_SECONDS)))


# ======================================================================
# Session management
# ======================================================================

class Session:
    """One `opencode serve` for the API process lifetime.

    Cold boot is ~15s; attached calls are ~3s. Each task is a fresh
    `opencode run --attach` — a fresh conversation, so no context bleed
    between cases.
    """

    def __init__(self, port: int = 0):
        self.port = port or 4096
        self.password = secrets.token_urlsafe(24)
        self._process: Optional[subprocess.Popen] = None

    def __enter__(self) -> "Session":
        binary = _binary()
        if not binary:
            raise OpencodeUnavailable("`opencode` was not found.")
        env = {**os.environ, "OPENCODE_SERVER_PASSWORD": self.password}
        # Ensure localhost is never proxied — the opencode server runs on
        # 127.0.0.1 and a corporate proxy will return an HTML error page
        # instead of the opencode API response, producing "Request is not
        # supported by this version of OpenCode Server".
        env["NO_PROXY"] = f"{env.get('NO_PROXY', '')},127.0.0.1,localhost".lstrip(",")
        env["no_proxy"] = env["NO_PROXY"]
        config = _harness_config(_model())
        if config:
            env["OPENCODE_CONFIG_CONTENT"] = json.dumps(config)

        self._process = subprocess.Popen(
            [binary, "serve", "--port", str(self.port), "--hostname", "127.0.0.1"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace")

        for _ in range(40):
            time.sleep(0.5)
            if self._process.poll() is not None:
                raise OpencodeUnavailable("`opencode serve` exited during startup.")
            try:
                import urllib.request
                urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=1)
                break
            except Exception:
                continue
        return self

    def __exit__(self, *_exc) -> None:
        if self._process and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self._process.kill()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


_ACTIVE: Optional[Session] = None


@contextlib.contextmanager
def session_scope(port: int = 0):
    """Start one `opencode serve` for the whole API process lifetime.

    Yields the Session (or None if opencode is unavailable). Each task
    attaches to this server with a fresh conversation.
    """
    global _ACTIVE
    if _ACTIVE is not None or not is_enabled() or not _binary():
        yield _ACTIVE
        return
    try:
        with Session(port=port) as session:
            _ACTIVE = session
            logger.info("opencode server started",
                        url=session.url, model=_model())
            yield session
    except OpencodeUnavailable as error:
        logger.warning("opencode server unavailable; falling back to direct LLM",
                       error=str(error))
        yield None
    finally:
        _ACTIVE = None


def current_session() -> Optional[Session]:
    return _ACTIVE


# ======================================================================
# Task execution
# ======================================================================

def server_ready() -> bool:
    """True when the opencode server is running and ready to accept tasks."""
    return _ACTIVE is not None and _ACTIVE._process is not None and _ACTIVE._process.poll() is None


# ======================================================================
# Task trace
# ======================================================================
#
# `opencode run --format json` writes one JSON object per line to stdout.
# `step_start`/`step_finish` bracket a single LLM round-trip: one task is an
# agentic loop, so it makes several, and until this existed the pipeline had
# no idea how many. The default format shows none of it -- piped to a
# subprocess with no TTY it prints the final assistant text and nothing more,
# so a task that burned twenty calls and a task that burned two logged the
# same one line.
#
# The runner never parsed stdout for results (it reads `output_path` off
# disk), so the format is free to change; only the log lines and this trace
# depend on it.


class _Trace:
    """Running tally of one task, built from the `--format json` stream."""

    def __init__(self) -> None:
        self.session_id: Optional[str] = None
        self.llm_calls = 0
        self.tools: Dict[str, int] = {}
        self.tokens = {"input": 0, "output": 0, "reasoning": 0,
                       "cache_read": 0, "cache_write": 0}
        self.cost = 0.0
        self.last_text = ""
        self.tool_errors: list = []

    def add(self, event: Dict[str, Any]) -> None:
        if self.session_id is None:
            self.session_id = event.get("sessionID")
        kind = event.get("type")
        part = event.get("part") or {}

        if kind == "step_start":
            self.llm_calls += 1
        elif kind == "step_finish":
            tokens = part.get("tokens") or {}
            cache = tokens.get("cache") or {}
            self.tokens["input"] += _int(tokens.get("input"))
            self.tokens["output"] += _int(tokens.get("output"))
            self.tokens["reasoning"] += _int(tokens.get("reasoning"))
            self.tokens["cache_read"] += _int(cache.get("read"))
            self.tokens["cache_write"] += _int(cache.get("write"))
            try:
                self.cost += float(part.get("cost") or 0.0)
            except (TypeError, ValueError):
                pass
        elif kind == "tool_use":
            name = str(part.get("tool") or "unknown")
            self.tools[name] = self.tools.get(name, 0) + 1
            # Defensive: a failed tool call is the most useful thing in the
            # stream and the cheapest to miss. Any status that is not a
            # completion is worth keeping, whatever opencode calls it.
            state = part.get("state") or {}
            status = str(state.get("status") or "")
            if status and status not in ("completed", "running", "pending"):
                self.tool_errors.append(f"{name}: {status}")
        elif kind == "text":
            text = str(part.get("text") or "").strip()
            if text:
                self.last_text = text

    def summary(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "llm_calls": self.llm_calls,
            "tools": dict(sorted(self.tools.items())),
            "tokens": dict(self.tokens),
            "cost": round(self.cost, 6),
        }


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_event(line: str) -> Optional[Dict[str, Any]]:
    """One stdout line as an event, or None when it is not one.

    Anything that is not a JSON object with a `type` is opencode's own
    diagnostics -- a provider error, a stack trace, a startup warning. Those
    still matter (they are the only channel that carries a failure the event
    stream never reaches), so the caller logs them verbatim.
    """
    if not line.startswith("{"):
        return None
    try:
        event = json.loads(line)
    except ValueError:
        return None
    return event if isinstance(event, dict) and "type" in event else None


def _tool_detail(part: Dict[str, Any]) -> str:
    """A one-line description of what a tool call actually asked for."""
    state = part.get("state") or {}
    inputs = state.get("input") or {}
    if isinstance(inputs, dict):
        for key in ("pattern", "filePath", "path", "query", "command", "description"):
            value = inputs.get(key)
            if value:
                return f"{key}={str(value)[:200]}"
    return str(state.get("title") or "")[:200]


def run_task(prompt: str, output_path: str,
             session: Optional[Session] = None,
             timeout: Optional[int] = None,
             node: Optional[str] = None) -> Dict[str, Any]:
    """Run one task via opencode and return what the agent wrote.

    The agent writes its output to `output_path` as a file on disk.
    The runner reads it back and returns the parsed result.

    `node` is the metrics label for the calling graph node (`investigator`,
    `reviewer`, `dlt_investigator`, `dlt_reviewer`). Given one, the task's
    LLM calls and tokens are metered under it, the same labels the direct
    LLM path uses, so the two paths are comparable on one graph.

    Raises OpencodeUnavailable on failure.
    """
    if not is_enabled():
        raise OpencodeUnavailable(
            "no lane uses the opencode harness: set "
            + " or ".join(sorted(ENV_LANES.values()))
            + f" (or {ENV_DISABLE}) to true.")

    binary = _binary()
    if not binary:
        raise OpencodeUnavailable("`opencode` was not found.")

    task_timeout = timeout or _task_timeout()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    # Always write the prompt to a file and pass the file path to the
    # subprocess. This breaks the taint chain from file-sourced prompt
    # content to the subprocess argument list (Fortify command-injection
    # sink) and avoids platform command-line length limits entirely.
    #
    # The file sits beside the output, so it is exactly as unique as the
    # output path and is removed with the case directory. It used to live in
    # a shared `_prompts/` directory named after the output's basename alone,
    # which is the same for every case ("investigation.json"): with several
    # tasks in flight, one task could read another case's instructions.
    prompt_file = f"{os.path.abspath(output_path)}.prompt.txt"
    with open(prompt_file, "w", encoding="utf-8") as handle:
        handle.write(prompt)
    task_prompt = (f"Read the file at {prompt_file} and follow the "
                   f"instructions in it exactly. Write your output to "
                   f"the path specified in those instructions: {output_path}")
    # `--format json` turns stdout into a machine-readable event stream. It
    # must stay ahead of the `argv[2:2]` splice below, which inserts
    # `--attach` immediately after `run`.
    #
    # `--title` is deliberately absent. It looks like the natural way to stamp
    # the event id on the session and to skip the extra title-generation LLM
    # call opencode makes per task, but on opencode 1.18.20 passing it hangs
    # `run` at startup before it reaches the model -- reproducibly, with the
    # same invocation succeeding the moment the flag is removed. The session
    # id in the trace below is the correlation handle instead.
    argv = [binary, "run", "--auto", "--format", "json", "--model", _model(),
            "--dir", repo_root, task_prompt]

    env = {**os.environ, "OPENCODE_PERMISSION": json.dumps(_permissions())}
    # Ensure localhost is never proxied — the opencode server runs on
    # 127.0.0.1 and a corporate proxy will return an HTML error page
    # instead of the opencode API response.
    env["NO_PROXY"] = f"{env.get('NO_PROXY', '')},127.0.0.1,localhost".lstrip(",")
    env["no_proxy"] = env["NO_PROXY"]
    config = _harness_config(_model())
    if config:
        env["OPENCODE_CONFIG_CONTENT"] = json.dumps(config)

    session = session or current_session()
    if session:
        argv[2:2] = ["--attach", session.url]
        env["OPENCODE_SERVER_PASSWORD"] = session.password

    started = time.time()
    task_name = os.path.basename(output_path)

    try:
        process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            env=env, stdin=subprocess.DEVNULL,
            encoding="utf-8", errors="replace")
    except FileNotFoundError as error:
        raise OpencodeUnavailable(f"opencode binary not found: {error}") from error

    output_lines = []
    trace = _Trace()

    def _stream():
        for line in process.stdout:
            output_lines.append(line)
            clean = _ANSI.sub("", line.rstrip())
            if not clean:
                continue
            event = _parse_event(clean)
            if event is None:
                logger.info(f"  [Harness] [{task_name}] | {clean}")
                continue
            trace.add(event)
            _log_event(task_name, event)

    def _log_event(task: str, event: Dict[str, Any]) -> None:
        """Tool calls at INFO -- they are the trace worth having in the log;
        step and text events at DEBUG, since the end-of-task summary carries
        the totals and the agent's text lands in the output file anyway."""
        kind = event.get("type")
        part = event.get("part") or {}
        if kind == "tool_use":
            logger.info(f"  [Harness] [{task}] | tool {part.get('tool')} "
                        f"{_tool_detail(part)}")
        elif kind == "step_finish":
            tokens = part.get("tokens") or {}
            logger.debug(f"  [Harness] [{task}] | step finish "
                         f"reason={part.get('reason')} "
                         f"in={tokens.get('input')} out={tokens.get('output')}")
        elif kind == "text":
            text = str(part.get("text") or "").strip()
            if text:
                logger.debug(f"  [Harness] [{task}] | {text[:400]}")

    reader = threading.Thread(target=_stream, daemon=True)
    reader.start()

    def _meter() -> None:
        """Meter what the task spent, however it ended.

        Called on the timeout path too: a task killed at the deadline still
        burned every token it had already spent, and leaving those unrecorded
        understates exactly the runs that cost the most. There the reader
        thread may not have drained yet, so the tally can be short by a step --
        an undercount on a run that already failed, which beats recording
        nothing for it.
        """
        if node:
            from src.utils import metrics
            metrics.record_harness_usage(node, trace.summary())

    try:
        process.wait(timeout=task_timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        reader.join(timeout=5)
        _meter()
        logger.warning("opencode task timed out",
                       task=task_name, seconds=task_timeout, **trace.summary())
        raise OpencodeUnavailable(
            f"task exceeded {task_timeout}s wall-clock deadline "
            f"after {trace.llm_calls} LLM calls.")

    reader.join(timeout=5)
    _meter()
    elapsed = time.time() - started

    # The agent may write to a different filename than requested (e.g.
    # findings.json instead of investigation.json). Check the expected
    # path first, then fall back to any .json file in the same directory
    # that was created during this task run.
    if not os.path.exists(output_path):
        output_dir = os.path.dirname(output_path) or "."
        fallback = None
        if os.path.isdir(output_dir):
            for name in sorted(os.listdir(output_dir), reverse=True):
                if name.endswith(".json") and name != "context.json":
                    candidate = os.path.join(output_dir, name)
                    if os.path.getmtime(candidate) > started:
                        fallback = candidate
                        break
        if fallback:
            logger.info("Agent wrote to alternate filename; using it",
                        expected=os.path.basename(output_path),
                        actual=os.path.basename(fallback))
            output_path = fallback
        else:
            # The agent's own last words beat the last stdout line: with
            # `--format json` that line is a `step_finish` envelope, which
            # says nothing about why nothing was written.
            reason = trace.last_text
            if not reason:
                transcript = "".join(output_lines)
                reason = (transcript.strip().splitlines() or ["no output"])[-1]
            if trace.tool_errors:
                reason = f"{reason} (tool errors: {', '.join(trace.tool_errors[:3])})"
            logger.warning("opencode task wrote no output",
                           task=task_name, elapsed=round(elapsed, 1),
                           **trace.summary())
            raise OpencodeUnavailable(
                f"task wrote no output to {output_path} after {elapsed:.0f}s "
                f"and {trace.llm_calls} LLM calls. Last: {reason[:200]}")

    with open(output_path, encoding="utf-8") as handle:
        raw = handle.read()

    logger.info("opencode task completed",
                task=task_name, elapsed=round(elapsed, 1), model=_model(),
                **trace.summary())

    return {
        "output": raw,
        "output_path": output_path,
        "seconds": round(elapsed, 1),
        "model": _model(),
        "trace": trace.summary(),
    }


def run_task_json(prompt: str, output_path: str,
                  session: Optional[Session] = None,
                  timeout: Optional[int] = None,
                  node: Optional[str] = None) -> Dict[str, Any]:
    """Run a task and parse the output as JSON.

    Extracts the first JSON object from the output file.
    """
    result = run_task(prompt, output_path, session, timeout, node)
    raw = result["output"]

    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end <= start:
        raise OpencodeUnavailable(f"{output_path} holds no JSON object.")
    try:
        parsed = json.loads(raw[start:end + 1])
    except ValueError as error:
        raise OpencodeUnavailable(f"{output_path} is not valid JSON: {error}") from error

    result["result"] = parsed
    return result
