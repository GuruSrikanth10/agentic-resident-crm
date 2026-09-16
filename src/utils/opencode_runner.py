"""
opencode harness runner — adapted from DROA's opencode_runner.py.

Runs opencode as a subprocess to perform LLM-driven investigation work.
One long-lived `opencode serve` process is started at API startup and
shared across all concurrent packets. Each task is a fresh `opencode run
--attach` invocation — a fresh conversation, no context bleed between cases.

The contract: "run this prompt against this directory; the agent writes
its output to a file on disk; the runner reads it back."

Feature-flagged via USE_OPENCODE_HARNESS. When false, all nodes use the
existing direct ChatOpenAI path.
"""
import contextlib
import json
import os
import re
import secrets
import subprocess
import tempfile
import threading
import time
from typing import Any, Dict, Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

_ANSI = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')

ENV_BINARY = "OPENCODE_BINARY"
ENV_MODEL = "OPENCODE_MODEL"
ENV_DISABLE = "USE_OPENCODE_HARNESS"

DEFAULT_MODEL = "uidai/glm-5.2-fp8"
DEFAULT_TIMEOUT_SECONDS = 300


class OpencodeUnavailable(Exception):
    """Raised when the harness cannot run."""


def is_enabled() -> bool:
    return os.environ.get(ENV_DISABLE, "false").lower() == "true"


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


def run_task(prompt: str, output_path: str,
             session: Optional[Session] = None,
             timeout: Optional[int] = None) -> Dict[str, Any]:
    """Run one task via opencode and return what the agent wrote.

    The agent writes its output to `output_path` as a file on disk.
    The runner reads it back and returns the parsed result.

    Raises OpencodeUnavailable on failure.
    """
    if not is_enabled():
        raise OpencodeUnavailable(f"{ENV_DISABLE} is not set to true.")

    binary = _binary()
    if not binary:
        raise OpencodeUnavailable("`opencode` was not found.")

    task_timeout = timeout or _task_timeout()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    # Windows command-line limit: write large prompts to a temp file
    if len(prompt) > 30000:
        prompt_dir = os.path.join(repo_root, "local_casesheets", "_prompts")
        os.makedirs(prompt_dir, exist_ok=True)
        prompt_file = os.path.join(prompt_dir, f"{os.path.basename(output_path)}.prompt.txt")
        with open(prompt_file, "w", encoding="utf-8") as handle:
            handle.write(prompt)
        short_prompt = (f"Read the file at {prompt_file} and follow the "
                        f"instructions in it exactly. Write your output to "
                        f"the path specified in those instructions: {output_path}")
        argv = [binary, "run", "--auto", "--model", _model(),
                "--dir", repo_root, short_prompt]
    else:
        argv = [binary, "run", "--auto", "--model", _model(),
                "--dir", repo_root, prompt]

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

    def _stream():
        for line in process.stdout:
            output_lines.append(line)
            clean = _ANSI.sub("", line.rstrip())
            if clean:
                logger.info(f"  [Harness] [{task_name}] | {clean}")

    reader = threading.Thread(target=_stream, daemon=True)
    reader.start()

    try:
        process.wait(timeout=task_timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        reader.join(timeout=5)
        raise OpencodeUnavailable(
            f"task exceeded {task_timeout}s wall-clock deadline.")

    reader.join(timeout=5)
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
            transcript = "".join(output_lines)
            tail = (transcript.strip().splitlines() or ["no output"])[-1]
            raise OpencodeUnavailable(
                f"task wrote no output to {output_path} after {elapsed:.0f}s. "
                f"Last line: {tail[:200]}")

    with open(output_path, encoding="utf-8") as handle:
        raw = handle.read()

    logger.info("opencode task completed",
                task=task_name, elapsed=round(elapsed, 1), model=_model())

    return {
        "output": raw,
        "output_path": output_path,
        "seconds": round(elapsed, 1),
        "model": _model(),
    }


def run_task_json(prompt: str, output_path: str,
                  session: Optional[Session] = None,
                  timeout: Optional[int] = None) -> Dict[str, Any]:
    """Run a task and parse the output as JSON.

    Extracts the first JSON object from the output file.
    """
    result = run_task(prompt, output_path, session, timeout)
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
