"""
Where the agents' tools come from: the MCP servers, in one place.

Every consumer reads this -- the deep agents (src/tools/mcp_client.py), the
opencode harness (the `mcp` entry the runner hands `opencode serve`), the API
that starts the local server, and the supervisor that waits for it -- so they
can never disagree about which servers exist.

AGENT_MCP_SERVE (auto | true | false, default auto)
    Whether this deployment runs the bundled tool server
    (src/tools/mcp_server.py, serving src/tools/agent_tools) as a child of
    the API. `auto` runs it exactly when some bundled toolset is switched on,
    so a deployment with no tools enabled runs no extra process at all.
AGENT_MCP_HOST / AGENT_MCP_PORT (default 127.0.0.1 / 8765)
    Where that local server listens.
AGENT_MCP_SERVERS
    The servers the agents connect to, as a JSON object:
        {"agent_tools": {"url": "http://127.0.0.1:8765/mcp"}}
        {"uidai_tools": {"url": "https://mcp.example/mcp",
                         "headers": {"Authorization": "Bearer ${UIDAI_MCP_TOKEN}"}}}
    `${NAME}` in a url or header value is read from the environment, so a
    token need not sit inside the JSON. Unset means the local server alone
    when it runs, and no servers otherwise. Moving to a hosted server is a
    change to this variable (and AGENT_MCP_SERVE=false), not to any agent.
    Once it is set the local server is no longer added implicitly: list it
    too when both are wanted.

    A server's optional `kind` is "tools" (the default) or "docs":
        {"agent_tools": {"url": "http://127.0.0.1:8765/mcp"},
         "droa_docs": {"url": "http://docs.ns.svc.cluster.local:8080/mcp",
                       "kind": "docs"}}
    A "tools" server reads live systems, and what its tools return is
    evidence about the packet. A "docs" server serves the DROA service
    documentation corpus -- the same content as docs_cache/ -- and publishes
    none of this repository's `_meta`. Its tools go to the AGENT_DOCS_ROLES
    roles for every service, their results are never recorded as evidence,
    and they reach only the deep agents: the opencode harness reads the
    corpus from disk and is given no docs server (mcp_client).
AGENT_DOCS_ROLES (default: every role)
    The roles that get a docs server's tools: a comma-separated list of
    agent roles, or `none`. AGENT_TOOLS_<ROLE> does not govern them.
AGENT_MCP_TIMEOUT_SECONDS (default 60)
    One tool call or listing, end to end.
AGENT_MCP_RETRY_SECONDS (default 30)
    How long an incomplete tool list -- a server that could not be reached --
    is kept before it is fetched again.

A server's name is how its tools are addressed where names must be unique
across servers: opencode calls the local server's bio_get_parking_status
`agent_tools_bio_get_parking_status`.
"""
import json
import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

ENV_SERVE = "AGENT_MCP_SERVE"
ENV_HOST = "AGENT_MCP_HOST"
ENV_PORT = "AGENT_MCP_PORT"
ENV_SERVERS = "AGENT_MCP_SERVERS"
ENV_TIMEOUT = "AGENT_MCP_TIMEOUT_SECONDS"
ENV_RETRY = "AGENT_MCP_RETRY_SECONDS"
ENV_DOCS_ROLES = "AGENT_DOCS_ROLES"
SETTINGS = (ENV_SERVE, ENV_HOST, ENV_PORT, ENV_SERVERS, ENV_TIMEOUT, ENV_RETRY,
            ENV_DOCS_ROLES)

#: The name the bundled server is known by, to opencode and in the default
#: server list.
LOCAL_SERVER_NAME = "agent_tools"
MCP_PATH = "/mcp"
HEALTH_PATH = "/health"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_RETRY_SECONDS = 30.0

#: What a server serves: tools that read live systems, or the service
#: documentation corpus.
KIND_TOOLS = "tools"
KIND_DOCS = "docs"
KINDS = (KIND_TOOLS, KIND_DOCS)
_NONE = "none"

#: Keys of a tool listing's `_meta` that this repository's server publishes
#: and its client reads. Reverse-DNS style, as MCP asks of custom keys.
META_TOOLSET = "uidai.crm/toolset"
META_AGENTS = "uidai.crm/agents"
META_GUIDANCE = "uidai.crm/guidance"
#: The services whose packets the tool is for, or ["*"] for every service
#: (MULTI_SERVICE_PLAN.md D7). A tool whose listing has none reaches no
#: service until AGENT_TOOLS_COMMON or a pack's tools.include names it.
META_SERVICES = "uidai.crm/services"

#: A server name: it becomes a prefix of tool names in opencode and a JSON key
#: in its config, so it is kept to what both accept.
_SERVER_NAME = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass(frozen=True)
class ServerConfig:
    name: str
    url: str
    #: Sorted (header, value) pairs; a tuple so the config is hashable and
    #: comparable, which is how a stale tool list is recognised.
    headers: tuple = ()
    #: KIND_TOOLS or KIND_DOCS.
    kind: str = KIND_TOOLS

    def header_dict(self) -> dict:
        return dict(self.headers)


def serve_mode() -> str:
    raw = os.environ.get(ENV_SERVE, "").strip().lower()
    return raw or "auto"


def serve_locally() -> bool:
    """Whether this deployment runs the bundled tool server."""
    mode = serve_mode()
    if mode in ("true", "1", "yes"):
        return True
    if mode in ("false", "0", "no"):
        return False
    # auto (and anything unrecognised, which validate() reports at boot):
    # serve when there is something to serve.
    from src.tools import agent_tools
    return agent_tools.any_enabled()


def local_host() -> str:
    return os.environ.get(ENV_HOST, "").strip() or DEFAULT_HOST


def local_port() -> int:
    raw = os.environ.get(ENV_PORT, "").strip()
    try:
        port = int(raw) if raw else DEFAULT_PORT
    except ValueError:
        return DEFAULT_PORT
    return port if 0 < port < 65536 else DEFAULT_PORT


def local_url() -> str:
    host = local_host()
    # A server bound to every interface is still reached on loopback.
    if host in ("0.0.0.0", "::", ""):
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{local_port()}{MCP_PATH}"


def _seconds(variable: str, default: float) -> float:
    raw = os.environ.get(variable, "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError:
        return default
    return value if value > 0 else default


def timeout_seconds() -> float:
    return _seconds(ENV_TIMEOUT, DEFAULT_TIMEOUT_SECONDS)


def retry_seconds() -> float:
    return _seconds(ENV_RETRY, DEFAULT_RETRY_SECONDS)


def _expand(value: str, where: str) -> str:
    def replace(match: re.Match) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"{where} refers to ${{{name}}}, which is not set.")
        return os.environ[name]
    return _ENV_REFERENCE.sub(replace, value)


def _parse_servers(raw: str) -> list:
    try:
        document = json.loads(raw)
    except ValueError as e:
        raise ValueError(f"{ENV_SERVERS} is not valid JSON: {e}") from None
    if not isinstance(document, dict):
        raise ValueError(f"{ENV_SERVERS} must be a JSON object of name -> server.")

    servers = []
    for name, spec in document.items():
        where = f"{ENV_SERVERS}[{name!r}]"
        if not isinstance(name, str) or not _SERVER_NAME.match(name):
            raise ValueError(f"{where}: a server name must match {_SERVER_NAME.pattern}.")
        if isinstance(spec, str):
            spec = {"url": spec}
        if not isinstance(spec, dict):
            raise ValueError(f"{where} must be a URL or an object with a url.")
        unknown = sorted(set(spec) - {"url", "headers", "kind"})
        if unknown:
            raise ValueError(f"{where} has unknown key(s) {unknown}; use url, headers "
                             f"and kind.")
        kind = spec.get("kind", KIND_TOOLS)
        if kind not in KINDS:
            raise ValueError(f"{where}.kind must be one of {list(KINDS)}, got {kind!r}.")
        url = _expand(str(spec.get("url") or ""), f"{where}.url")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"{where}.url must be an http(s) URL, got {url!r}.")
        headers = spec.get("headers") or {}
        if not isinstance(headers, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
            raise ValueError(f"{where}.headers must map header names to strings.")
        expanded = {k: _expand(v, f"{where}.headers[{k!r}]") for k, v in headers.items()}
        servers.append(ServerConfig(name=name, url=url,
                                    headers=tuple(sorted(expanded.items())), kind=kind))
    return sorted(servers, key=lambda server: server.name)


def servers() -> list:
    """The servers the agents use, in a stable order.

    Raises ValueError on a malformed AGENT_MCP_SERVERS; `validate()` reports
    the same at boot, so a running process never meets it.
    """
    raw = os.environ.get(ENV_SERVERS, "").strip()
    if raw:
        return _parse_servers(raw)
    if serve_locally():
        return [ServerConfig(name=LOCAL_SERVER_NAME, url=local_url())]
    return []


def find_server(name: str) -> Optional[ServerConfig]:
    return next((server for server in servers() if server.name == name), None)


def of_kind(configured, kind: str) -> tuple:
    """The servers in `configured` of `kind`, in their order."""
    return tuple(server for server in configured if server.kind == kind)


def docs_roles() -> tuple:
    """The roles that get a docs server's tools (AGENT_DOCS_ROLES), in
    AGENT_ROLES order.

    Raises ValueError on an unknown role; `validate()` reports the same at
    boot, so a running process never meets it.
    """
    from src.tools.agent_tools import AGENT_ROLES

    raw = os.environ.get(ENV_DOCS_ROLES, "").strip()
    if not raw:
        return AGENT_ROLES
    if raw.lower() == _NONE:
        return ()
    named = {part.strip() for part in raw.split(",") if part.strip()}
    unknown = sorted(named - set(AGENT_ROLES))
    if unknown:
        raise ValueError(f"{ENV_DOCS_ROLES} names unknown role(s) {unknown}; the roles "
                         f"are {list(AGENT_ROLES)}, or {_NONE}.")
    return tuple(role for role in AGENT_ROLES if role in named)


def is_loopback(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in ("127.0.0.1", "localhost", "::1") or host.startswith("127.")


def validate() -> list:
    """Configuration errors, for `validate_config()` at boot."""
    errors = []
    if serve_mode() not in ("auto", "true", "false", "1", "0", "yes", "no"):
        errors.append(f"{ENV_SERVE} must be auto, true or false; got {serve_mode()!r}.")
    for variable in (ENV_PORT, ENV_TIMEOUT, ENV_RETRY):
        raw = os.environ.get(variable, "").strip()
        if not raw:
            continue
        try:
            valid = float(raw) > 0 and (variable != ENV_PORT or
                                        (raw.isdigit() and 0 < int(raw) < 65536))
        except ValueError:
            valid = False
        if not valid:
            errors.append(f"{variable} must be a positive number"
                          f"{' and a valid port' if variable == ENV_PORT else ''}; got {raw!r}.")
    for check in (servers, docs_roles):
        try:
            check()
        except ValueError as e:
            errors.append(str(e))
    return errors
