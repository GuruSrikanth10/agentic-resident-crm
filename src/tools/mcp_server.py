"""
The agent tool server: every tool registered in src/tools/agent_tools, over MCP.

Both kinds of agent reach tools only through MCP -- the deep agents through
src/tools/mcp_client.py, the opencode harness through the `mcp` entry the
runner gives `opencode serve` -- and this is the server they talk to while
the tools live in this repository. When the organisation hosts its own MCP
server it replaces or joins this one through AGENT_MCP_SERVERS; no agent
changes (src/tools/mcp_config.py).

Streamable HTTP, stateless, with JSON responses: every request stands alone,
so a restarted server is invisible to its clients and several replicas can
sit behind one URL, which is how a hosted server would run. Tool functions
are synchronous and the SDK runs them on worker threads, so a slow lookup
never holds up another request.

Besides its name, description and argument schema, each tool's listing
carries the MCP read-only hint (from its toolset) and, under `_meta`, the
roles that get it by default and the guidance their prompts include
(mcp_config.META_*). A client needs none of this repository's code to act on
them.

Run it on its own:
    python3 -m src.tools.mcp_server [--host 127.0.0.1] [--port 8765]
or let the API run it as a supervised child (LocalToolServer, below), which
it does when mcp_config.serve_locally() says so.
"""
import argparse
import http.client
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.tools import agent_tools, mcp_config
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

INSTRUCTIONS = (
    "Tools for the Agentic Resident CRM investigation agents. Every tool is "
    "keyed by a packet's refId. A result saying a lookup was switched off or "
    "failed means nothing was read; a lookup that ran and found nothing is a "
    "finding. Each tool's _meta names the agent roles it is meant for "
    f"({mcp_config.META_AGENTS}) and the guidance for using it "
    f"({mcp_config.META_GUIDANCE})."
)


def build_server():
    """An MCPServer serving every enabled registered tool."""
    from mcp.server.mcpserver import MCPServer
    from mcp_types import ToolAnnotations
    from starlette.requests import Request
    from starlette.responses import JSONResponse

    server = MCPServer(name=mcp_config.LOCAL_SERVER_NAME,
                       title="Agentic Resident CRM agent tools",
                       instructions=INSTRUCTIONS)
    served = []
    for entry in agent_tools.enabled_entries():
        server.add_tool(
            entry.function,
            name=entry.tool.name,
            description=entry.tool.description,
            annotations=ToolAnnotations(read_only_hint=entry.toolset.read_only),
            meta={
                mcp_config.META_TOOLSET: entry.toolset.name,
                mcp_config.META_AGENTS: list(entry.toolset.agents),
                mcp_config.META_GUIDANCE: entry.toolset.guidance,
            },
            # The tools return text; a structured copy of it would only
            # double what every client receives.
            structured_output=False,
        )
        served.append(entry.tool.name)

    @server.custom_route(mcp_config.HEALTH_PATH, methods=["GET"])
    async def health(_request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "server": mcp_config.LOCAL_SERVER_NAME,
                             "tools": served})

    return server


def build_app(host: str = mcp_config.DEFAULT_HOST):
    """The Starlette app serving `build_server()` at mcp_config.MCP_PATH.

    `host` is where it will listen: bound to loopback, the SDK accepts only
    loopback Host and Origin headers, which stops a web page in a browser on
    the same machine from reaching it by DNS rebinding.
    """
    return build_server().streamable_http_app(
        streamable_http_path=mcp_config.MCP_PATH,
        json_response=True,
        stateless_http=True,
        host=host,
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python3 -m src.tools.mcp_server",
                                     description="Serve the agent tools over MCP.")
    parser.add_argument("--host", default=mcp_config.local_host())
    parser.add_argument("--port", type=int, default=mcp_config.local_port())
    args = parser.parse_args(argv)

    errors = agent_tools.validate()
    if errors:
        for error in errors:
            print(f"Agent tool server: {error}", file=sys.stderr)
        return 1

    import uvicorn

    names = [entry.tool.name for entry in agent_tools.enabled_entries()]
    logger.info("Agent tool server starting", host=args.host, port=args.port,
                path=mcp_config.MCP_PATH, tools=names)
    # No websockets: MCP's streamable HTTP does not use them.
    uvicorn.run(build_app(args.host), host=args.host, port=args.port,
                log_level="warning", ws="none")
    return 0


# ---------------------------------------------------------------------------
# The API's own local server, as a supervised child process
# ---------------------------------------------------------------------------

def probe_health(host: str, port: int, timeout: float = 1.0) -> bool:
    """Whether an agent tool server answers /health on host:port.

    http.client rather than urllib or httpx: it never consults proxy
    settings, and a corporate proxy answering for 127.0.0.1 is exactly the
    failure opencode_runner and start.py already had to route around.
    """
    try:
        connection = http.client.HTTPConnection(host, port, timeout=timeout)
        try:
            connection.request("GET", mcp_config.HEALTH_PATH)
            response = connection.getresponse()
            body = response.read()
        finally:
            connection.close()
        if response.status != 200:
            return False
        return json.loads(body).get("server") == mcp_config.LOCAL_SERVER_NAME
    except Exception:
        return False


class LocalToolServer:
    """The bundled tool server as a child of the API, restarted if it dies.

    A child process rather than a thread: the tools then run exactly as they
    will on a hosted server -- their own process, their own connection pool,
    reached only over MCP -- and a tool that misbehaves cannot take the API
    with it.
    """

    #: Restart backoff, doubling from the first to the last.
    FIRST_BACKOFF_SECONDS = 1.0
    MAX_BACKOFF_SECONDS = 30.0

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        self.host = host or mcp_config.local_host()
        self.port = port or mcp_config.local_port()
        self._process: Optional[subprocess.Popen] = None
        self._stopping = threading.Event()
        self._watchdog: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def probe_host(self) -> str:
        return "127.0.0.1" if self.host in ("0.0.0.0", "::", "") else self.host

    def _spawn(self) -> None:
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        self._process = subprocess.Popen(
            [sys.executable, "-m", "src.tools.mcp_server",
             "--host", self.host, "--port", str(self.port)],
            cwd=str(REPO_ROOT), env=env)
        logger.info("Agent tool server process started", pid=self._process.pid,
                    host=self.host, port=self.port)

    def start(self) -> "LocalToolServer":
        with self._lock:
            if self._process is not None:
                return self
            self._stopping.clear()
            self._spawn()
            self._watchdog = threading.Thread(target=self._watch, name="agent-tool-server",
                                              daemon=True)
            self._watchdog.start()
        return self

    def _watch(self) -> None:
        backoff = self.FIRST_BACKOFF_SECONDS
        while not self._stopping.wait(1.0):
            process = self._process
            if process is None or process.poll() is None:
                if process is not None and self.healthy():
                    backoff = self.FIRST_BACKOFF_SECONDS
                continue
            logger.error("Agent tool server exited; restarting",
                         exit_code=process.returncode, backoff_seconds=backoff)
            if self._stopping.wait(backoff):
                return
            with self._lock:
                if self._stopping.is_set():
                    return
                self._spawn()
            backoff = min(backoff * 2, self.MAX_BACKOFF_SECONDS)

    def healthy(self) -> bool:
        process = self._process
        return (process is not None and process.poll() is None
                and probe_health(self.probe_host, self.port))

    def wait_healthy(self, seconds: float) -> bool:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.healthy():
                return True
            if self._stopping.wait(0.25):
                return False
        return self.healthy()

    def stop(self, grace_seconds: float = 10.0) -> None:
        self._stopping.set()
        with self._lock:
            process, self._process = self._process, None
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        logger.info("Agent tool server stopped")


_LOCAL: Optional[LocalToolServer] = None


def start_local_server() -> Optional[LocalToolServer]:
    """Start this process's tool server when the configuration asks for one."""
    global _LOCAL
    if _LOCAL is None and mcp_config.serve_locally():
        _LOCAL = LocalToolServer().start()
    return _LOCAL


def local_server() -> Optional[LocalToolServer]:
    return _LOCAL


def stop_local_server() -> None:
    global _LOCAL
    server, _LOCAL = _LOCAL, None
    if server is not None:
        server.stop()


if __name__ == "__main__":
    sys.exit(main())
