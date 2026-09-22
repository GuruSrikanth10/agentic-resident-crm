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

validate_config()


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

    # Start the opencode harness in a background thread so the API binds
    # its port immediately. The corpus download and `opencode serve` cold
    # boot take 15-30s; doing them in the lifespan blocked the API from
    # accepting connections, causing consumers to fail with connection
    # refused errors.
    from src.utils.opencode_runner import is_enabled as harness_enabled
    _harness_thread = None
    if harness_enabled():
        import threading
        from src.utils import docs_loader, opencode_runner

        def _start_harness():
            try:
                docs_loader.download_corpus()
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

app = FastAPI(
    title="Agentic Resident CRM API",
    description="AI-driven, self-learning service to ingest, analyze, and resolve rejected biometric packets within the UIDAI ecosystem.",
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
