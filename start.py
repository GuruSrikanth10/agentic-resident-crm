"""Process supervisor for the Agentic Resident CRM ecosystem.

Spawns the API and both Kafka consumers -- fast_consumer.py (rejections ->
POST /fetch-logs) and slow_consumer.py (the analysis queue -> POST
/analyze-rejection) -- then holds all three open. If any exits, the rest are
terminated rather than left running half a system: a consumer with no API
forwards every packet into a connection error, and an API with no consumers
silently stops receiving work (F12). Each consumer sets its own CONSUMER_ROLE
before importing kafkaConsumer.py, so no special per-child environment is
needed here to keep their topics, heartbeat files, and health ports apart.
"""
import os
import signal
import subprocess
import sys
import time
from dotenv import load_dotenv

load_dotenv()

# How long a child gets to shut down gracefully before SIGKILL. Must exceed
# the consumer's SHUTDOWN_DRAIN_SECONDS so its drain can actually finish.
TERMINATE_GRACE_SECONDS = 30

_children = []
_stopping = False


def _terminate_all():
    """SIGTERM every child, then SIGKILL whatever ignored it."""
    global _stopping
    if _stopping:
        return
    _stopping = True

    for name, process in _children:
        if process.poll() is None:
            print(f"Stopping {name}.")
            process.terminate()

    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    for name, process in _children:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            print(f"{name} did not stop within {TERMINATE_GRACE_SECONDS}s; killing.")
            process.kill()
            process.wait()


def _handle_signal(signum, _frame):
    print(f"\nReceived signal {signum}; stopping services.")
    _terminate_all()
    sys.exit(0)


def main():
    print("Starting the Agentic Resident CRM ecosystem.\n")

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handle_signal)

    print("Starting the API server (main_api.py).")
    _children.append(("API", subprocess.Popen([sys.executable, "src/main_api.py"])))

    # Wait for the API to bind its port, then wait for the corpus download
    # to complete (if the opencode harness is enabled).
    #
    # Uses http.client directly instead of urllib to completely bypass
    # proxy env vars — on Windows, urllib's ProxyHandler({"http": None})
    # does not reliably bypass the corporate proxy for localhost.
    import http.client

    def _http_get(path):
        """GET /path on 127.0.0.1:8000. Returns (status_code, body) or (None, error)."""
        try:
            conn = http.client.HTTPConnection("127.0.0.1", 8000, timeout=2)
            conn.request("GET", path)
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            code = resp.status
            conn.close()
            return code, body
        except Exception as e:
            return None, str(e)

    # Step 1: wait for the API to bind its port
    api_ready = False
    for _ in range(120):
        code, _ = _http_get("/health")
        if code == 200:
            api_ready = True
            break
        time.sleep(1)

    if not api_ready:
        print("WARNING: API did not bind within 120s; starting consumers anyway.")
    else:
        print("API server is listening.")

    # Step 2: if the opencode harness is enabled, wait for the corpus
    # download AND the opencode server to be ready. The /ready endpoint
    # returns 503 "Downloading documentation corpus" while downloading,
    # 503 "Starting opencode server" while the server boots, and 503 with
    # other details for Kafka/checkpoint issues (which we accept).
    harness_enabled = os.environ.get("USE_OPENCODE_HARNESS", "false").lower() == "true"
    if harness_enabled:
        print("Waiting for documentation corpus and opencode server...")
        harness_ready = False
        for i in range(300):
            code, body = _http_get("/ready")
            if code == 200:
                harness_ready = True
                print("API ready (corpus + opencode + services).")
                break
            if code == 503:
                # "Downloading" and "Starting opencode" mean not ready
                # Any other 503 (Kafka, checkpoint) is fine — consumers handle it
                if "Downloading" not in body and "Starting opencode" not in body:
                    harness_ready = True
                    print("Corpus and opencode ready; starting consumers.")
                    break
            if i % 15 == 0:
                print(f"  ...still waiting ({i*2}s). /ready: {code} {body[:80] if body else ''}")
            time.sleep(2)

        if not harness_ready:
            print("WARNING: Harness did not become ready within 600s; starting consumers anyway.")

    print("Starting the fast consumer (fast_consumer.py) -- rejections -> /fetch-logs.")
    _children.append(("FastConsumer", subprocess.Popen([sys.executable, "src/fast_consumer.py"])))

    print("Starting the slow consumer (slow_consumer.py) -- analysis queue -> /analyze-rejection.")
    _children.append(("SlowConsumer", subprocess.Popen([sys.executable, "src/slow_consumer.py"])))

    # Off by default: a deployment with no dead-letter topic configured would
    # otherwise start two consumers that fail to subscribe, and the supervisor
    # would tear down the whole ecosystem when they exited (DLT_PLAN.md
    # Phase 4).
    if os.environ.get("DLT_ENABLED", "false").strip().lower() in ("true", "1", "yes"):
        print("Starting the DLT consumer (dlt_consumer.py) -- dead-letter topic -> /fetch-dlt-logs.")
        _children.append(("DltConsumer", subprocess.Popen([sys.executable, "src/dlt_consumer.py"])))

        dlt_analysis = "src/dlt_analysis_consumer.py"
        if os.path.exists(dlt_analysis):
            print("Starting the DLT analysis consumer -- DLT queue -> /analyze-dlt.")
            _children.append(("DltAnalysisConsumer",
                              subprocess.Popen([sys.executable, dlt_analysis])))

    print(f"\nAll {len(_children)} services are running. Press Ctrl+C to stop them.")

    try:
        # Wait on all three concurrently. Waiting on the API and only then on
        # the consumer(s) would let an API crash leave the consumers running
        # unattended while the supervisor sat blocked.
        while True:
            for name, process in _children:
                code = process.poll()
                if code is not None:
                    print(f"\n{name} exited with code {code}; stopping the other services.")
                    _terminate_all()
                    sys.exit(code or 0)
            time.sleep(0.5)
    except KeyboardInterrupt:
        _handle_signal(signal.SIGINT, None)


if __name__ == "__main__":
    main()
