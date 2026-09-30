import asyncio
import os
import sys
from fastapi import FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.routes import begin_draining, drain_and_shutdown
from src.api.routes import router as api_router
from src.utils.config_validator import validate_config


def validate_reason_code_docs() -> None:
    """Refuse to boot on a broken reason-code document store.

    Deliberately not folded into `validate_config()`: the consumers call that
    too, and they never read a document. Only the API does, so only the API
    checks.

    The convention is `validate_config`'s -- log each error, print it to
    stderr so a boot failure is visible even when the log stream is shipped
    elsewhere, and exit 1. A store that cannot be read is a configuration
    failure and has to be loud: at run time the same problem is silent by
    design, because `lookup` never raises and every packet simply loses its
    documentation.
    """
    from src.utils import reason_code_docs
    from src.utils.reason_code_docs import docs_enabled, validate

    if not docs_enabled():
        return

    if reason_code_docs.s3_download_enabled():
        # The store is fetched in the background after the API has started, so
        # there is nothing on disk to validate yet -- the download validates
        # what it fetched before swapping it in, and /ready waits for the first
        # copy. What can be checked now is that the fetch can work at all, and
        # that it has somewhere of its own to write: left at the default the
        # swap would replace the copy that ships inside `src/`.
        errors = []
        if not os.environ.get("REASON_CODE_DOCS_DIR", "").strip():
            errors.append(
                "REASON_CODE_DOCS_S3_DOWNLOAD is on but REASON_CODE_DOCS_DIR "
                "is unset; set it to a writable directory outside src/ so the "
                "download does not replace the files shipped in the image.")
        if not (os.environ.get("CASEBOOK_S3_BUCKET", "").strip()
                or os.environ.get("S3_LOGS_BUCKET", "").strip()):
            errors.append(
                "REASON_CODE_DOCS_S3_DOWNLOAD is on but no S3 bucket is "
                "configured; set CASEBOOK_S3_BUCKET or S3_LOGS_BUCKET.")
        for error in errors:
            _docs_logger().error("Reason-code document store error", detail=error)
            print(f"Reason-code document store error: {error}", file=sys.stderr)
        if errors:
            sys.exit(1)
        _docs_logger().info("Reason-code documentation will be downloaded at "
                            "start-up; skipping the on-disk check")
        return

    errors, warnings = validate()
    for warning in warnings:
        _docs_logger().warning("Reason-code document store warning", detail=warning)
    if errors:
        for error in errors:
            _docs_logger().error("Reason-code document store error", detail=error)
            print(f"Reason-code document store error: {error}", file=sys.stderr)
        sys.exit(1)
    _docs_logger().info("Reason-code document store validated")


def _docs_logger():
    from src.utils.logging_config import get_logger
    return get_logger(__name__)


def validate_service_registry() -> None:
    """Refuse to boot on a broken service registry (MULTI_SERVICE_PLAN.md 5.7).

    API-only, for the reason `validate_reason_code_docs` is: the consumers
    never read a service pack. Always on, unlike the documentation check,
    because every rejection packet is resolved to a service whatever the
    gate mode. At run time a pack with errors is silently left out of the
    registry, which under `enforce` would skip that service's packets; here
    the same error stops the API instead.
    """
    from src.utils import service_registry

    errors, warnings = service_registry.validate()
    for warning in warnings:
        _docs_logger().warning("Service registry warning", detail=warning)
    if errors:
        for error in errors:
            _docs_logger().error("Service registry error", detail=error)
            print(f"Service registry error: {error}", file=sys.stderr)
        sys.exit(1)
    _docs_logger().info("Service registry validated",
                        services=list(service_registry.load().services()),
                        enabled=sorted(service_registry.enabled_services()),
                        gate_mode=service_registry.gate_mode())


validate_config()
validate_reason_code_docs()
validate_service_registry()


def _install_draining_signal_handlers():
    """Fail /ready the moment SIGTERM lands, not when uvicorn tears down.

    `drain_and_shutdown` sets the draining flag, but it runs from the lifespan
    shutdown hook -- after uvicorn has closed the listening socket. An
    orchestrator probing /ready then gets a connection refusal rather than the
    503 the code intends, so the flag it sets was never observable by anyone.

    Chaining to uvicorn's own handler rather than replacing it: uvicorn needs
    its handler to run for the normal graceful shutdown to proceed at all.
    """
    import signal

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            previous = signal.getsignal(sig)
        except (ValueError, OSError):
            continue

        def _handler(signum, frame, _previous=previous):
            begin_draining()
            if callable(_previous):
                _previous(signum, frame)

        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            # Not the main thread (a test runner, an embedded server).
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup, then a bounded drain on the way down.

    The shutdown half was an empty yield: uvicorn stopped accepting
    connections while agent.invoke() kept running on non-daemon threads until
    SIGKILL, leaving an IN_PROGRESS status.json behind for every interrupted
    packet and blocking its reprocessing for MAX_IN_PROGRESS_AGE_SECONDS (G9).
    """
    # Installed here, not at import: uvicorn registers its own handlers as it
    # starts serving, so this has to run after that to be able to chain to them.
    _install_draining_signal_handlers()

    # Background reaper: removes stale local casebook directories left
    # behind by crashes, timeouts, or any path where the immediate cleanup
    # after save_terminal did not run.
    from src.utils.case_cleanup import start_reaper
    start_reaper()

    # Ask the S3 endpoint, once, whether it honours the conditional writes
    # `update_json` relies on. An endpoint that accepts creates but refuses
    # If-Match overwrites is otherwise silent: every record is created and
    # never updated again. Off the startup path, like the harness below, so
    # a slow or unreachable store cannot delay the API binding its port.
    if os.environ.get("CASEBOOK_STORAGE_BACKEND", "local").strip().lower() == "s3":
        import threading
        from src.storage import s3 as s3_storage
        threading.Thread(target=s3_storage.probe_configured_endpoint,
                         name="s3-cas-probe", daemon=True).start()

    # The bundled agent tool server (src/tools/mcp_server.py), when this
    # deployment serves its own tools. Every agent reaches its tools over MCP,
    # so it starts first: a child process, supervised and restarted if it
    # dies, and /ready waits for it. Starting it returns at once.
    from src.tools import mcp_server
    tool_server = mcp_server.start_local_server()

    # The reason-code store from S3, when this process fetches it
    # (MULTI_SERVICE_PLAN.md D10): off the startup path like the harness
    # below, and /ready fails until the first copy is on disk. Started even
    # when the documents are switched off for the prompts, so a deployment can
    # keep the store current before turning them on.
    from src.utils import reason_code_docs
    reason_code_docs.start_background_download()

    # The reason-code -> service map, when it is fetched from S3: the same
    # way, and /ready fails until the first copy is on disk.
    from src.utils import reason_code_service_map
    reason_code_service_map.start_background_download()

    # Start the opencode harness in a background thread so the API binds
    # its port immediately. The corpus download and `opencode serve` cold
    # boot take 15-30s; doing them in the lifespan blocked the API from
    # accepting connections, causing consumers to fail with connection
    # refused errors.
    #
    # `is_enabled()` is the ANY-lane question, which is the right one here:
    # one server and one corpus serve whichever lanes are on opencode.
    from src.utils.opencode_runner import is_enabled as harness_enabled
    _harness_thread = None
    if harness_enabled():
        import threading
        from src.utils import docs_loader, opencode_runner

        def _start_harness():
            try:
                docs_loader.download_corpus()
                # An enabled service with no corpus directory still runs, but
                # its harness tasks start from documentation that is not there
                # (MULTI_SERVICE_PLAN.md 5.7). Checked now, not at boot: the
                # corpus only exists once this download has finished.
                from src.utils import service_registry
                missing = service_registry.missing_corpus_dirs(docs_loader.docs_cache_dir())
                if missing:
                    _docs_logger().warning(
                        "Enabled services have no directory in the documentation "
                        "corpus", services=missing)
                # opencode connects to its MCP servers once, as it starts: a
                # tool server that is not up yet would leave every harness task
                # without tools until the next restart.
                if tool_server is not None and not tool_server.wait_healthy(120):
                    print("Agent tool server is not healthy after 120s; starting "
                          "opencode anyway, and its tasks will lack the tools.")
                global _opencode_session
                _opencode_session = opencode_runner.session_scope()
                _opencode_session.__enter__()
            except Exception as e:
                print(f"opencode harness failed to start: {e}")

        _harness_thread = threading.Thread(target=_start_harness, daemon=True)
        _harness_thread.start()

    yield

    # Run the drain off the event loop: it blocks for up to
    # API_SHUTDOWN_DRAIN_SECONDS and would otherwise stall the loop that the
    # in-flight investigations are still being awaited on.
    await asyncio.to_thread(drain_and_shutdown)

    if _harness_thread is not None:
        from src.utils import opencode_runner
        session = opencode_runner.current_session()
        if session:
            session.__exit__(None, None, None)

    # Last: the drain above may still have been calling tools.
    mcp_server.stop_local_server()

app = FastAPI(
    title="Agentic Resident CRM API",
    description="AI-driven, self-learning service to ingest, analyze, and resolve rejected packets within the UIDAI ecosystem.",
    version="1.0.0",
    lifespan=lifespan
)
app.include_router(api_router)

# DLT analysis (DLT_PLAN.md). A separate router on the same app: the flow is
# parallel to the rejection pipeline, not part of it.
from src.api.dlt_routes import router as dlt_router  # noqa: E402
app.include_router(dlt_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
