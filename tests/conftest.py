"""Test-suite isolation from the developer's `.env`.

`src/utils/env.py` calls `load_dotenv()` at import time, and every production
module imports it transitively. So without this file, `pytest` inherits
whatever happens to be in `.env` -- and the suite's verdict depends on the
machine it runs on.

That was not hypothetical. A checkout with `LOG_SOURCE=kubernetes`,
`K8S_DEFAULT_NAMESPACE=offline` and `K8S_FIXTURE_DIR=...` set locally failed
three tests that passed in CI, because the CI job sets `LOG_SOURCE=elastic`
and nothing else. A developer seeing red tests they cannot attribute learns to
stop reading the suite.

Two mechanisms, and both are needed:

* `load_dotenv` is stubbed out at conftest import, so no later import can
  re-read `.env`. This is the one that actually closes the hole -- see the
  comment on the stub below.
* A session-scoped autouse fixture clears anything the parent shell exported
  and installs the CI defaults.

The fixture only clears variables that select a *deployment shape* -- a
backend, a source chain, a feature flag. Tunables (timeouts, caps, thresholds)
are left alone: tests that care about those set them explicitly via
monkeypatch, and clearing them here would only mask a missing setenv.
"""
import os

import dotenv
import pytest

# ---------------------------------------------------------------------------
# Neutralise `.env` loading for the whole session.
# ---------------------------------------------------------------------------
# Popping the variables in a fixture is NOT sufficient on its own. Several
# modules call `load_dotenv()` at *import* time -- `src/utils/env.py` and every
# consumer entrypoint -- and a test module importing one of them mid-session
# re-reads `.env` and puts every value straight back. Since the fixture had
# already popped them, `load_dotenv`'s "don't override what is already set"
# behaviour does not save us: it sees them as unset and sets them.
#
# This runs at conftest import, which pytest does before it imports any test
# module, and therefore before any production module is imported. From here on
# `load_dotenv()` is a no-op everywhere.
#
# CI has no `.env`, so this makes local runs behave the way CI already does
# rather than changing what CI does.
dotenv.load_dotenv = lambda *args, **kwargs: False

#: Variables whose value must come from the test, never from a developer's
#: `.env`. Each one selects a backend, a source, or a feature -- i.e. changes
#: which code path runs, not how fast it runs.
ISOLATED_ENV_VARS = (
    # Log source chain and its two backends.
    "LOG_SOURCE",
    "ES_MOCK_FILE",
    "ES_HOST",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_INDEX_PATTERN",
    "ES_APP_NAMES",
    "ES_SEARCH_WINDOW_DAYS",
    # Kubernetes source: fixtures, namespace, and service resolution.
    "K8S_FIXTURE_DIR",
    "K8S_DEFAULT_NAMESPACE",
    "K8S_DEFAULT_APP",
    "K8S_APP_NAMES",
    "K8S_SERVICE_MAP",
    "K8S_SEARCH_FIELDS",
    "K8S_DEFAULT_SINCE_HOURS",
    "KUBECONFIG_PATH",
    "K8S_CONTEXT",
    # Storage and checkpoint backends.
    "CASEBOOK_STORAGE_BACKEND",
    "CASEBOOK_S3_BUCKET",
    "CASEBOOK_S3_PREFIX",
    "S3_LOGS_BUCKET",
    "CHECKPOINT_BACKEND",
    "CHECKPOINT_POSTGRES_URI",
    "CHECKPOINT_MYSQL_URI",
    "LOCAL_CASESHEETS_DIR",
    "LOCAL_CHECKPOINTS_DIR",
    # Feature switches. Every one of these changes which branch executes.
    "RUNBOOK_MODE",
    "RUNBOOK_SERVE_ALLOWLIST",
    "ENABLE_LOG_FETCHING",
    "ENABLE_LOG_FILTER_AGENT",
    "ENABLE_AUTO_REPLAY",
    "DLT_ENABLED",
    "DLT_AUTO_REPLAY_ENABLED",
    "DLT_REUSE_ENABLED",
    "DLT_REGISTRY_PATH",
    "LOG_SNAPSHOT_REUSE",
    # The opencode harness, per lane. A lane switch left over in the parent
    # shell would silently route a node through a harness the test never
    # intended, or hide the fallback to the older single switch.
    "USE_OPENCODE_HARNESS",
    "USE_OPENCODE_HARNESS_REJECTION",
    "USE_OPENCODE_HARNESS_DLT",
    # Reason-code documentation. The switch selects a branch, and the two
    # paths select which store is read -- a stray value in either would make
    # the Investigator's prompt depend on the developer's machine.
    "REJECTION_REASON_CODE_DOCS_ENABLED",
    "REASON_CODE_DOCS_DIR",
    "REASON_CODE_DOCS_S3_PREFIX",
    # The download: on, it makes the store come from S3 and /ready wait for
    # it, so a stray value would have tests reaching for a bucket.
    "REASON_CODE_DOCS_S3_DOWNLOAD",
    "REASON_CODE_DOCS_REFRESH_SECONDS",
    # A cap rather than a tunable: below a document's length it switches
    # truncation on, so a stray value changes which branch runs.
    "REASON_CODE_DOC_MAX_CHARS",
    # Likewise a cap: below the logs' length it switches trimming on.
    "REJECTION_PROMPT_MAX_CHARS",
    "REJECTION_REVIEWER_EVIDENCE",
    "REJECTION_SYNTHESIS_DOC_GUIDANCE",
    # The duplicate-invocation claim. A stray value would either disable the
    # guard for the whole session or make every live claim look abandoned.
    "PACKET_CLAIM_ENABLED",
    "PACKET_CLAIM_TTL_SECONDS",
    # Agent tools: the process DB toolset's switch (the `process` database of
    # agent_tools/_database.py; another key's AGENT_DB_<KEY>_ENABLED names a
    # database no shipped tool reads), the MCP tool servers the
    # agents use (and whether the API runs its own), and the per-role
    # selections. Each decides which tools a built agent is offered, and so
    # what its system prompt says; AGENT_MCP_SERVE also decides whether a
    # tool server process is started at all.
    "PROCESS_DB_ENABLED",
    "AGENT_MCP_SERVE",
    "AGENT_MCP_SERVERS",
    "AGENT_MCP_HOST",
    "AGENT_MCP_PORT",
    "AGENT_TOOLS_INVESTIGATOR",
    "AGENT_TOOLS_REVIEWER",
    "AGENT_TOOLS_SYNTHESIS",
    "AGENT_TOOLS_LOG_FILTER",
    "AGENT_TOOLS_DLT_INVESTIGATOR",
    "AGENT_TOOLS_DLT_REVIEWER",
    "AGENT_TOOLS_DLT_SYNTHESIS",
    # Undeclared tools treated as tools for every service: widens what every
    # service's agents are offered.
    "AGENT_TOOLS_COMMON",
    # The service registry and the intake gate. The directory selects which
    # packs exist; the other four decide whether a packet is analysed at all,
    # and the pilot list also whether its Synthesis may stage a replay.
    "SERVICE_PACKS_DIR",
    "REJECTION_SERVICE_GATE",
    "REJECTION_SERVICES_ENABLED",
    "REJECTION_SERVICES_PILOT",
    "REJECTION_UNRESOLVED_SERVICE",
    # The DLT lane's own gate (Phase 8): whether a dead-lettered record of a
    # service is analysed, and with which pack.
    "DLT_SERVICE_GATE",
    "DLT_SERVICES_ENABLED",
    # A cap: below a pack's size it turns the pack into a boot error.
    "SERVICE_PACK_MAX_CHARS",
    # LLM provider selection.
    "USE_HF",
    "MOCK_LLM_WITH_MISTRAL",
    # Consumer role: resolved at import time, so a stray value here would
    # point kafkaConsumer at the wrong topic for the whole session.
    "CONSUMER_ROLE",
)

#: What the suite runs against when a test does not say otherwise. Matches the
#: `env:` block of the CI job so local and CI runs agree by construction.
TEST_ENV_DEFAULTS = {
    "LOG_SOURCE": "elastic",
    "KAFKA_CONSUMER_BROKERS": "localhost:9092",
    "AGENTIC_RESIDENT_CRM_API_KEY": "test-key",
    "USE_MOCK_DB": "true",
    "CASEBOOK_STORAGE_BACKEND": "local",
    "CHECKPOINT_BACKEND": "sqlite",
    # No tool server process unless a test starts one (tests/mcp_fixtures.py).
    "AGENT_MCP_SERVE": "false",
}


@pytest.fixture(autouse=True)
def isolated_packet_claims(tmp_path_factory, monkeypatch):
    """Give every test its own duplicate-invocation claim store.

    The claim `/analyze-rejection` takes reaches storage through
    `get_scoped_storage`, not through the `get_casebook_storage` name the test
    fixtures patch per module -- so without this it writes into the real
    `local_casesheets/packet_claims/` and the claims survive the run. The
    symptom is nasty: a test that exercises the analyze path passes the first
    time and fails every time after, because its event id is now held by a
    claim left behind by the previous run.

    Function-scoped and autouse rather than added to each fixture that needs
    it, so a future test cannot forget.
    """
    from src.storage.local import LocalFilesystemCasebookStorage
    from src.utils import packet_claims

    root = tmp_path_factory.mktemp("packet_claims")
    store = LocalFilesystemCasebookStorage(base_dir=str(root))
    monkeypatch.setattr(packet_claims, "get_claim_storage", lambda: store)


@pytest.fixture(scope="session", autouse=True)
def hermetic_env():
    """Clear deployment-shaped configuration and install the test defaults.

    Session-scoped rather than function-scoped on purpose: several production
    modules resolve configuration at *import* time (`kafkaConsumer`'s topics,
    `utils.paths`' directories, `fetcher.LOG_MAX_DOCUMENTS`), so the only
    useful moment to do this is before the first import, not before each test.
    """
    for name in ISOLATED_ENV_VARS:
        os.environ.pop(name, None)

    for name, value in TEST_ENV_DEFAULTS.items():
        os.environ.setdefault(name, value)

    yield


@pytest.fixture(autouse=True)
def fresh_service_registry():
    """Each test reads the service packs it points at, not a registry an
    earlier test loaded and the process kept."""
    from src.utils import service_registry

    service_registry.reset()
    yield
    service_registry.reset()


@pytest.fixture(autouse=True)
def fresh_tool_catalog():
    """Each test lists the tool servers it configures, not an earlier test's."""
    from src.tools import mcp_client

    mcp_client.reset()
    yield
    mcp_client.reset()


@pytest.fixture
def tool_server(monkeypatch):
    """Start the real agent tool server over HTTP, in this process.

    Yields a function: call it -- after registering any probe tools, since the
    server serves what is registered when it starts -- to start a server on a
    free loopback port and point AGENT_MCP_SERVERS at it. It returns the URL.
    In-process so a test can patch what the tools read (a SQLite engine for
    the process DB) and so every server stops with the test.
    """
    import json

    from src.tools.mcp_server import build_app

    started = []

    def start(name: str = "agent_tools") -> str:
        url = _serve_in_process(build_app("127.0.0.1"), started)
        monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({name: {"url": url}}))
        return url

    yield start
    _stop_servers(started)


#: The tools the fake documentation server serves.
DOCS_TOOLS = ("docs_list_services", "docs_read", "docs_search")


@pytest.fixture
def docs_server():
    """Start a fake DROA documentation server over HTTP, in this process.

    Like the real one it publishes none of this repository's `_meta`. Yields
    a function that starts one and returns its URL, without touching
    AGENT_MCP_SERVERS: a test lists it with `"kind": "docs"` itself.
    `docs_read` of the path "boom" raises, so the server answers with a tool
    error.
    """
    from mcp.server.mcpserver import MCPServer

    started = []

    def docs_list_services() -> str:
        """List the documented services."""
        return "enu-biometric\nenu-demographic"

    def docs_search(query: str, service: str = "") -> str:
        """Search the documentation corpus."""
        return f"hits for {query} in {service or 'every service'}"

    def docs_read(service: str, path: str) -> str:
        """Read one document."""
        if path == "boom":
            raise RuntimeError("no such document")
        return f"{service}/{path}: the document"

    def start() -> str:
        server = MCPServer(name="droa_docs")
        for function in (docs_list_services, docs_search, docs_read):
            server.add_tool(function, structured_output=False)
        app = server.streamable_http_app(streamable_http_path="/mcp", json_response=True,
                                         stateless_http=True, host="127.0.0.1")
        return _serve_in_process(app, started)

    yield start
    _stop_servers(started)


def _serve_in_process(app, started: list) -> str:
    """Serve `app` on a free loopback port on a daemon thread, once it is
    up; returns its MCP URL and adds the server to `started`."""
    import socket
    import threading
    import time

    import uvicorn

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                           log_level="warning", ws="none"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("the test tool server did not start")
        time.sleep(0.02)
    started.append((server, thread))
    return f"http://127.0.0.1:{port}/mcp"


def _stop_servers(started: list) -> None:
    for server, thread in started:
        server.should_exit = True
        thread.join(timeout=10)
