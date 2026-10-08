"""
Langfuse tracing for the LLM lanes (the LANGFUSE_* settings in .env.example).

Off unless LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are both set. While it
is off, every function here is a pass-through: an agent is invoked exactly as
it was before tracing existed, with the same arguments, and nothing is
imported from the SDK. While it is on, tracing can never fail a packet: a
client that cannot start, a trace that cannot open or an export that cannot
reach the server degrades to running untraced, with a warning in the log.

One trace is one unit of LLM work, and its session is the thing it was about:

    analyze-rejection   one run of the rejection graph     session: the eventId
    analyze-dlt         one run of the DLT analysis lane   session: the case key
    draft-runbook       one runbook draft (build_runbooks)  no session

so a packet that is redelivered, resumed from a checkpoint or replayed has
all its traces side by side in the session view. The tree under a rejection:

    analyze-rejection (chain)        the packet's identifiers in, the resolution out
      fetch-logs (retriever)
      lookup-runbook (retriever)
      investigate (span)             once per attempt
        investigator-agent (agent)   the deep agent, via the LangChain handler:
          model > ChatOpenAI           a generation per model call, with
          tools > <tool name>          the tool calls it requested beside it
        investigator-agent (agent)   or, on the opencode harness, the same agent
          harness-model-call           rebuilt from the task's event stream
          <tool name>                  (`harness_task`)
      review (evaluator)
      synthesize (span)

The graph's nodes are spans of our own (`node`), not the handler's. Given the
outer graph, the handler would record every node with the whole graph state
as its input -- the logs once per node -- and the routing functions besides.
The deep agents' own middleware hooks are dropped for the same reason
(`_is_middleware_hook`): each recorded the full message history twice per
model call and showed nothing a reader needs.

MASKING. This is resident data. Every attribute of every span is passed
through `log_pipeline.redaction` before it leaves the process
(`mask_spans`), the same patterns and personal-data JSON keys the log
pipeline applies, and always -- K8S_REDACT_ENABLED does not switch it off.
The trace's own eventId or case key is allowlisted, as the log pipeline
allowlists it, so the identifier a reader searches by survives. Point
LANGFUSE_BASE_URL at a self-hosted Langfuse: a cloud region would put
resident packets' prompts outside the network, masked or not.
"""
import contextlib
import contextvars
import functools
import json
import os
import re
import threading
from typing import Any, Callable, Literal, Optional

from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: The repository's version file, read for the release when LANGFUSE_RELEASE
#: is unset.
VERSION_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "version.json")

#: What a propagated metadata value may hold (Langfuse drops a longer one).
_METADATA_VALUE_LIMIT = 200

#: The observation types this module opens, in Langfuse's names.
ObservationType = Literal["span", "chain", "retriever", "evaluator", "agent"]

#: Name of the opencode harness's model-call generations.
HARNESS_GENERATION = "harness-model-call"

_client = None
_client_failed = False
_client_lock = threading.Lock()


class _Trace:
    """The trace this thread is recording into, or none (`observation` None)."""

    def __init__(self, observation=None):
        self.observation = observation

    @property
    def active(self) -> bool:
        return self.observation is not None

    @property
    def trace_id(self) -> Optional[str]:
        return getattr(self.observation, "trace_id", None)

    def update(self, **fields) -> None:
        """Set fields on the root observation; a failure is logged, not raised."""
        if self.observation is None:
            return
        try:
            self.observation.update(**fields)
        except Exception as e:
            logger.warning("Could not update the Langfuse trace",
                           error=f"{type(e).__name__}: {e}")


_UNTRACED = _Trace()

#: The trace open on this thread. Context variables are copied into the
#: threads LangGraph runs nodes on, so a node sees the trace its run opened.
_current: contextvars.ContextVar[_Trace] = contextvars.ContextVar(
    "langfuse_trace", default=_UNTRACED)


def enabled() -> bool:
    """Whether traces are recorded: both keys set, and not switched off."""
    if not (os.environ.get("LANGFUSE_PUBLIC_KEY") and os.environ.get("LANGFUSE_SECRET_KEY")):
        return False
    return get_bool_env("LANGFUSE_TRACING_ENABLED", True)


def agent_name(role: str) -> str:
    """What `role`'s deep agent is called in a trace: `investigator-agent`.

    The LangChain handler types a run as an `agent` only when its name says
    so, and the harness's rebuilt agent uses the same name, so an evaluator or
    a dashboard that targets one role sees it on both paths."""
    return f"{role.replace('_', '-')}-agent"


def _environment() -> str:
    """LANGFUSE_TRACING_ENVIRONMENT, else ENV (dev, prod), in the form
    Langfuse accepts: lower case, letters, digits, `-` and `_`."""
    raw = os.environ.get("LANGFUSE_TRACING_ENVIRONMENT") or os.environ.get("ENV") or "dev"
    cleaned = re.sub(r"[^a-z0-9_-]", "-", raw.strip().lower()) or "dev"
    # The `langfuse` prefix is reserved for Langfuse's own environments.
    return f"env-{cleaned}" if cleaned.startswith("langfuse") else cleaned


def _release() -> Optional[str]:
    """LANGFUSE_RELEASE, else the version in version.json."""
    release = os.environ.get("LANGFUSE_RELEASE")
    if release:
        return release
    try:
        with open(VERSION_FILE, encoding="utf-8") as handle:
            return str(json.load(handle).get("version") or "") or None
    except (OSError, ValueError, AttributeError):
        return None


#: A deep-agent middleware hook run as a graph node -- "TodoListMiddleware.
#: after_model". Summarization is kept: its hook can call the model, and
#: dropping a span whose children are exported would orphan them.
_MIDDLEWARE_HOOK = re.compile(r"^(?!Summarization)\w+Middleware\.(before|after)_(agent|model)$")


def _is_middleware_hook(name: Optional[str]) -> bool:
    return bool(name and _MIDDLEWARE_HOOK.match(name))


def _should_export(span) -> bool:
    from langfuse.span_filter import is_default_export_span

    return is_default_export_span(span) and not _is_middleware_hook(span.name)


def _build_client(**overrides):
    """The Langfuse client. Keys, LANGFUSE_BASE_URL and LANGFUSE_SAMPLE_RATE
    are read by the SDK itself."""
    from langfuse import Langfuse

    return Langfuse(environment=_environment(), release=_release(),
                    mask_otel_spans=mask_spans, should_export_span=_should_export,
                    **overrides)


def _get_client():
    """The client, built on first use; None while tracing is off or after it
    failed to start."""
    global _client, _client_failed
    if not enabled():
        return None
    if _client is not None or _client_failed:
        return _client
    with _client_lock:
        if _client is None and not _client_failed:
            try:
                _client = _build_client()
                logger.info("Langfuse tracing on", environment=_environment(),
                            release=_release())
            except Exception as e:
                _client_failed = True
                logger.warning("Langfuse tracing could not start; running untraced",
                               error=f"{type(e).__name__}: {e}")
    return _client


def _string_metadata(metadata: Optional[dict]) -> dict:
    """Propagated metadata is string to string, each value at most 200
    characters: anything else would be dropped by the SDK with a warning."""
    return {str(key): str(value)[:_METADATA_VALUE_LIMIT]
            for key, value in (metadata or {}).items() if value is not None}


def _record_failure(observation, error: BaseException) -> None:
    """Mark `observation` with what ended it. An abandoned run is a WARNING:
    the server stopped waiting, nothing in the run failed (run_control)."""
    from src.core.run_control import RunCancelled

    abandoned = isinstance(error, RunCancelled)
    try:
        observation.update(
            level="WARNING" if abandoned else "ERROR",
            status_message=(f"Abandoned: {error}" if abandoned
                            else f"{type(error).__name__}: {error}")[:1000])
    except Exception:
        pass


@contextlib.contextmanager
def run(name: str, *, input: Any = None, session_id: Optional[str] = None,
        tags=(), metadata: Optional[dict] = None, version: Optional[str] = None,
        as_type: ObservationType = "chain"):
    """Record what runs inside as one trace named `name`.

    `input` is the root observation's input, which Langfuse shows as the
    trace's: what a reader needs at a glance, never the raw payload. Set the
    output with `.update(output=...)` on what this yields. `session_id`,
    `tags`, `metadata` and `version` are propagated to every observation in
    the trace; metadata values are coerced to strings. Yields an untraced
    handle, and the body runs exactly the same, when tracing is off or the
    trace cannot be opened.
    """
    client = _get_client()
    if client is None:
        yield _UNTRACED
        return

    from langfuse import propagate_attributes

    stack = contextlib.ExitStack()
    trace = _UNTRACED
    try:
        observation = stack.enter_context(client.start_as_current_observation(
            as_type=as_type, name=name, input=input))
        stack.enter_context(propagate_attributes(
            trace_name=name, session_id=session_id, tags=list(tags) or None,
            metadata=_string_metadata(metadata) or None, version=version))
        trace = _Trace(observation)
        stack.callback(_current.reset, _current.set(trace))
    except Exception as e:
        stack.close()
        trace = _UNTRACED
        logger.warning("Could not open a Langfuse trace; running untraced",
                       trace_name=name, error=f"{type(e).__name__}: {e}")

    with stack:
        try:
            yield trace
        except BaseException as error:
            if trace.active:
                _record_failure(trace.observation, error)
            raise


@contextlib.contextmanager
def _child(name: str, as_type: ObservationType):
    """A child of the current observation, or None outside a trace."""
    client = _client if _current.get().active else None
    if client is None:
        yield None
        return

    stack = contextlib.ExitStack()
    observation = None
    try:
        observation = stack.enter_context(
            client.start_as_current_observation(as_type=as_type, name=name))
    except Exception as e:
        stack.close()
        observation = None
        logger.warning("Could not open a Langfuse observation", name=name,
                       error=f"{type(e).__name__}: {e}")

    with stack:
        try:
            yield observation
        except BaseException as error:
            if observation is not None:
                _record_failure(observation, error)
            raise


def node(name: str, fn: Callable, *, as_type: ObservationType = "span",
         output: Optional[Callable[[Any], Any]] = None) -> Callable:
    """Graph node `fn`, recorded as an observation named `name` when it runs
    inside a trace. `output` turns the node's state update into what the
    observation shows (default: the update as it is)."""
    @functools.wraps(fn)
    def traced(state):
        with _child(name, as_type) as observation:
            update = fn(state)
            if observation is not None:
                try:
                    observation.update(output=output(update) if output else update)
                except Exception as e:
                    logger.warning("Could not record a node's output", node=name,
                                   error=f"{type(e).__name__}: {e}")
            return update
    return traced


def invoke(runnable, request):
    """`runnable.invoke(request)`, with the Langfuse handler as its callback
    inside a trace. Outside one the call is made exactly as before tracing
    existed -- no config argument at all."""
    if not _current.get().active:
        return runnable.invoke(request)
    try:
        from langfuse.langchain import CallbackHandler

        # By key: with a second Langfuse client in the process, a handler
        # without one is given a disabled client, and records nothing.
        handler = CallbackHandler(public_key=os.environ.get("LANGFUSE_PUBLIC_KEY"))
    except Exception as e:
        logger.warning("Could not create the Langfuse callback handler",
                       error=f"{type(e).__name__}: {e}")
        return runnable.invoke(request)
    return runnable.invoke(request, config={"callbacks": [handler]})


def flush() -> None:
    """Send what is queued. For a script that exits when it is done."""
    if _client is not None:
        try:
            _client.flush()
        except Exception as e:
            logger.warning("Langfuse flush failed", error=f"{type(e).__name__}: {e}")


def shutdown() -> None:
    """Flush and stop the exporter. For the API's shutdown."""
    if _client is not None:
        try:
            _client.shutdown()
        except Exception as e:
            logger.warning("Langfuse shutdown failed", error=f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# The opencode harness
# ---------------------------------------------------------------------------

class _HarnessRecorder:
    """Rebuilds an opencode task as an agent observation, live, from the
    `--format json` events opencode_runner already reads.

    opencode makes its model calls in its own process, where the LangChain
    handler cannot see them. Each step becomes a generation -- the model, its
    tokens and cost, its text and the tool calls it requested -- and each
    tool call a tool observation beside it, as the direct path records them.
    A generation's input is what the model was given new for that step: the
    task's instructions for the first, the previous step's tool results
    after that. The full context stays inside opencode.

    `add` runs on the runner's stdout reader thread, `finish` and `fail` on
    the caller's; the lock orders them, and nothing arriving after the task
    is closed is recorded.
    """

    def __init__(self, agent, model: Optional[str], prompt: str):
        self._agent = agent
        self._model = model
        self._lock = threading.Lock()
        self._closed = False
        self._generation = None
        self._text: list = []
        self._requested: list = []
        self._next_input: Any = [{"role": "user", "content": prompt}]
        self._seen_calls: set = set()

    def add(self, event: dict) -> None:
        try:
            with self._lock:
                if not self._closed:
                    self._add(event)
        except Exception as e:
            logger.debug("Could not record a harness event",
                         error=f"{type(e).__name__}: {e}")

    def _add(self, event: dict) -> None:
        kind = event.get("type")
        part = event.get("part") or {}
        if kind == "step_start":
            self._end_generation()
            self._generation = self._agent.start_observation(
                name=HARNESS_GENERATION, as_type="generation", model=self._model,
                input=self._next_input)
            self._next_input = []
            self._text, self._requested = [], []
        elif kind == "text":
            text = str(part.get("text") or "").strip()
            if text:
                self._text.append(text)
        elif kind == "tool_use":
            self._tool(part)
        elif kind == "step_finish":
            tokens = part.get("tokens") or {}
            cache = tokens.get("cache") or {}
            usage = {"input": _int(tokens.get("input")), "output": _int(tokens.get("output"))}
            if _int(cache.get("read")):
                usage["input_cache_read"] = _int(cache.get("read"))
            if _int(cache.get("write")):
                usage["input_cache_write"] = _int(cache.get("write"))
            cost = _float(part.get("cost"))
            self._end_generation(
                usage_details=usage,
                cost_details={"total": cost} if cost else None,
                metadata={"finish_reason": part.get("reason"),
                          "reasoning_tokens": _int(tokens.get("reasoning"))})

    def _tool(self, part: dict) -> None:
        state = part.get("state") or {}
        status = str(state.get("status") or "")
        if status not in ("completed", "error"):
            return
        name = str(part.get("tool") or "unknown")
        call_id = str(part.get("callID") or part.get("id") or len(self._seen_calls))
        if call_id in self._seen_calls:
            return
        self._seen_calls.add(call_id)
        inputs = state.get("input") if isinstance(state.get("input"), dict) else {}
        output = (state.get("output") if status == "completed"
                  else f"The tool failed: {state.get('error')}")
        self._agent.start_observation(
            name=name, as_type="tool", input=inputs, output=output,
            level="ERROR" if status == "error" else None,
            status_message=str(state.get("error"))[:1000] if status == "error" else None,
            metadata={"call_id": call_id, "title": state.get("title")},
        ).end()
        self._requested.append({"id": call_id, "type": "function", "function": {
            "name": name, "arguments": json.dumps(inputs, ensure_ascii=False, default=str)}})
        self._next_input.append({"role": "tool", "tool_call_id": call_id, "name": name,
                                 "content": "" if output is None else str(output)})

    def _end_generation(self, **fields) -> None:
        if self._generation is None:
            return
        message: dict[str, Any] = {"role": "assistant", "content": "\n\n".join(self._text)}
        if self._requested:
            message["tool_calls"] = list(self._requested)
        self._generation.update(output=message, **{k: v for k, v in fields.items() if v})
        self._generation.end()
        self._generation = None

    def finish(self, result: dict) -> None:
        """Close the task with what it wrote and what it spent."""
        with self._lock:
            self._closed = True
            self._end_generation()
        self._agent.update(
            output=result.get("result", result.get("output")),
            metadata={"path": "harness", "seconds": result.get("seconds"),
                      "output_path": result.get("output_path"),
                      **(result.get("trace") or {})})

    def fail(self, error: BaseException) -> None:
        with self._lock:
            self._closed = True
            self._end_generation()
        self._agent.update(metadata={"path": "harness"})
        _record_failure(self._agent, error)


class _NoRecorder:
    def add(self, event: dict) -> None:
        pass

    def finish(self, result: dict) -> None:
        pass

    def fail(self, error: BaseException) -> None:
        pass


def _int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


@contextlib.contextmanager
def harness_task(role: Optional[str], *, prompt: str, model: Optional[str]):
    """An opencode task for `role`, recorded as that role's agent.

    Yields a recorder: pass it every event (`add`) and the task's result
    (`finish`). An exception out of the block marks the agent failed -- a
    harness failure falls back to the direct path, and both then show in the
    node, the failed attempt first. A no-op outside a trace."""
    if not _current.get().active:
        yield _NoRecorder()
        return
    # opencode names models provider/model; the direct path's generations
    # carry the model alone, and the two must compare.
    model_name = model.split("/", 1)[-1] if model else None
    with _child(agent_name(role or "harness"), "agent") as agent:
        if agent is None:
            yield _NoRecorder()
            return
        try:
            agent.update(input=prompt)
        except Exception:
            pass
        recorder = _HarnessRecorder(agent, model_name, prompt)
        try:
            yield recorder
        except BaseException as error:
            recorder.fail(error)
            raise


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------

#: Attributes the SDK writes that are identifiers, enums or numbers, and never
#: resident data: left as they are. `session.id` is the eventId or case key.
_UNMASKED = frozenset({
    "session.id", "user.id",
    "langfuse.observation.type", "langfuse.observation.level",
    "langfuse.observation.model.name", "langfuse.observation.usage_details",
    "langfuse.observation.cost_details", "langfuse.observation.completion_start_time",
    "langfuse.environment", "langfuse.release", "langfuse.version",
    "langfuse.trace.name", "langfuse.trace.tags", "langfuse.trace.public",
})

#: The span attributes holding the identifiers redaction must keep: the
#: session (the eventId or case key) and the same values in the metadata.
_ALLOWLIST_ATTRIBUTES = ("session.id", "langfuse.trace.metadata.event_id",
                         "langfuse.trace.metadata.case_id")

#: Keys that mark a dict as a message, a tool call or a tool definition. Its
#: `name` is then a tool's or an agent's -- `lookup_rule`, `investigator` --
#: not a person's, and redacting it would leave every tool call in the trace
#: unnamed. A resident record carries none of these keys.
_STRUCTURAL_KEYS = frozenset({"args", "arguments", "parameters", "role", "content",
                              "tool_call_id"})

_MASK_FAILED = "[REDACTED:MASKING_FAILED]"


def _mask_json(value, allowlist: list, personal_keys: frozenset):
    """`value`, a parsed JSON document, with personal data redacted: string
    leaves through `redact_text`, and the value of a personal-data key
    replaced outright, as `redaction.redact_json_fields` replaces it."""
    from src.log_pipeline.redaction import JSON_FIELD_LABEL, redact_text

    if isinstance(value, str):
        return redact_text(value, allowlist=allowlist).text
    if isinstance(value, list):
        return [_mask_json(item, allowlist, personal_keys) for item in value]
    if not isinstance(value, dict):
        return value
    structural = bool(_STRUCTURAL_KEYS & value.keys())
    masked = {}
    for key, item in value.items():
        personal = str(key).lower() in personal_keys
        if personal and key == "name" and structural and isinstance(item, str):
            personal = False
        if personal and item is not None and not isinstance(item, bool):
            masked[key] = f"[REDACTED:{JSON_FIELD_LABEL}]"
        else:
            masked[key] = _mask_json(item, allowlist, personal_keys)
    return masked


def mask_text(text: str, allowlist: Optional[list] = None) -> str:
    """One attribute value with personal data redacted. A JSON document --
    the SDK serialises inputs, outputs and metadata as JSON -- is walked;
    anything else goes through `redact_text` whole."""
    from src.log_pipeline.redaction import json_keys, redact_text

    allowlist = [value for value in (allowlist or []) if value]
    stripped = text.lstrip()
    if stripped[:1] in ("{", "["):
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = None
        if isinstance(parsed, (dict, list)):
            masked = _mask_json(parsed, allowlist, frozenset(json_keys()))
            return json.dumps(masked, ensure_ascii=False)
    return redact_text(text, allowlist=allowlist).text


def _mask_attribute(value, allowlist: list):
    if isinstance(value, str):
        return mask_text(value, allowlist)
    if isinstance(value, (list, tuple)) and value and all(isinstance(v, str) for v in value):
        return type(value)(mask_text(v, allowlist) for v in value)
    return value


def mask_spans(*, params):
    """Langfuse's `mask_otel_spans` hook: redact every span before export.

    Runs on the exporter's thread, one batch at a time. An attribute that
    cannot be masked is replaced with a placeholder rather than sent as it
    is; the SDK would drop the whole batch if this raised.
    """
    from langfuse.types import MaskOtelSpansResult, OtelSpanPatch

    patches = {}
    for identifier, span in params.spans.items():
        attributes = span.attributes or {}
        allowlist = [str(attributes[key]) for key in _ALLOWLIST_ATTRIBUTES
                     if attributes.get(key)]
        replacements = {}
        for key, value in attributes.items():
            if key in _UNMASKED or key.startswith("langfuse.internal."):
                continue
            try:
                masked = _mask_attribute(value, allowlist)
            except Exception:
                masked = _MASK_FAILED
            if masked != value:
                replacements[key] = masked
        if replacements:
            patches[identifier] = OtelSpanPatch(set_attributes=replacements)
    return MaskOtelSpansResult(span_patches=patches)
