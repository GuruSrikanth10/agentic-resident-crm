# Remediation Plan — 2026-08-21 Codebase Audit

Full read of `src/` (16,733 lines), plus `pytest` (986 tests), `ruff`, and
`pyright`. Findings below are ordered into phases by dependency and risk, not
by severity alone: Phase 0 exists because you cannot verify anything else
while the baseline is red.

Every finding was reproduced or read to a specific line. Where a claim is
inferred rather than executed, it says so.

**Baseline as found**

| Gate | State |
| --- | --- |
| `pytest` | 4 failed, 977 passed, 7 skipped, 1 xfailed |
| `ruff check .` | 1 error (`F401`) — the CI `lint` job is a hard gate, so **CI is red on main** |
| `pyright` | 300 errors (advisory, `continue-on-error: true`); 53 in `src/` |

---

## Phase 0 — Restore a green, trustworthy baseline

Nothing later can be validated while four tests fail for two unrelated reasons
and the lint gate is red. This phase changes no production behaviour.

### 0.1 — Fix the lint error blocking CI

`src/tools/parse_reason_codes.py:39` imports `typing.Optional` and never uses
it. `.github/workflows/test.yml` runs `ruff check .` as a hard gate, so the
`lint` job fails on every push.

**Change:** delete the import (`ruff check --fix .`).

**Accept:** `ruff check .` exits 0.

### 0.2 — Make the test suite hermetic

`src/utils/env.py:2-5` calls `load_dotenv()` at import time. Every test run
therefore inherits the developer's `.env`. On this machine `.env` sets
`LOG_SOURCE=kubernetes`, `K8S_DEFAULT_NAMESPACE=offline` and
`K8S_FIXTURE_DIR=…/local_fixtures`, which makes three tests fail locally that
pass in CI (CI sets `LOG_SOURCE=elastic` and nothing else):

- `tests/test_log_sources.py::test_reduce_logs_end_to_end_through_the_csv_mock`
- `tests/test_log_sources.py::test_reduce_logs_propagates_source_exceptions`
- `tests/test_audit_phase1.py::test_unparseable_output_preserves_the_evidence` (partly)

There is no `tests/conftest.py` at all.

**Change:** add `tests/conftest.py` with a session-scoped autouse fixture that
neutralises deployment-shaped configuration before any test module imports
production code:

```python
import os
import pytest

# Variables whose value must come from the test, never from a developer's .env.
_ISOLATED = (
    "LOG_SOURCE", "ES_MOCK_FILE", "ES_HOST", "ES_APP_NAMES", "ES_SEARCH_WINDOW_DAYS",
    "K8S_FIXTURE_DIR", "K8S_DEFAULT_NAMESPACE", "K8S_DEFAULT_APP", "K8S_APP_NAMES",
    "K8S_SERVICE_MAP", "K8S_SEARCH_FIELDS", "K8S_DEFAULT_SINCE_HOURS",
    "CASEBOOK_STORAGE_BACKEND", "CHECKPOINT_BACKEND", "S3_LOGS_BUCKET",
    "CASEBOOK_S3_BUCKET", "RUNBOOK_MODE", "ENABLE_LOG_FETCHING",
    "ENABLE_LOG_FILTER_AGENT", "DLT_ENABLED", "ENABLE_AUTO_REPLAY",
    "DLT_AUTO_REPLAY_ENABLED",
)

@pytest.fixture(scope="session", autouse=True)
def _hermetic_env():
    for name in _ISOLATED:
        os.environ.pop(name, None)
    # Defaults every test can rely on, matching the CI job.
    os.environ.setdefault("LOG_SOURCE", "elastic")
    os.environ.setdefault("KAFKA_CONSUMER_BROKERS", "localhost:9092")
    os.environ.setdefault("PACKET_CRM_API_KEY", "test-key")
    os.environ.setdefault("USE_MOCK_DB", "true")
```

Then drop the now-redundant `LOG_SOURCE: elastic` from the CI job's `env:`
block, or keep it — it becomes a no-op either way.

**Accept:** `pytest` produces identical results with and without `.env`
present. Verify with `env -i PATH=$PATH .venv/bin/python -m pytest -q` against
a run with the real environment.

### 0.3 — Update two stale tests to the current casebook shape

`casebook["packet_status"]["rejection_data"]["rejection_logs"]` used to be a
string; `src/api/routes.py:757-762` now writes
`{"path": …, "gaps": …}`. Two tests still assert the old shape:

- `tests/test_phase1_fixes.py:264` — `logs_field.startswith("X" * 100)`
- `tests/test_audit_phase1.py:322` — `rejection["rejection_logs"] == "ERROR something went wrong"`

**Change:** assert on `rejection_logs["path"]` and, for the S3-unset case, that
it equals `"Logs persisted to local storage (S3 unavailable)."`. The raw log
text is no longer embedded in the casebook by design — it is persisted as the
`raw_logs.txt` artifact by `pipeline._save_raw_logs`. Assert *that* instead if
the test's intent was "the evidence survives".

**Accept:** `pytest` exits 0.

---

## Phase 1 — Evidence integrity

Three defects in what the LLM is shown. This system's stated first principle
is that "we looked and found nothing" must never be confused with "we could
not look" (`src/log_pipeline/types.py`, `FetchResult.ok`). All three break it,
and all three are silent.

### 1.1 — Elasticsearch truncation drops the newest logs and announces nothing

**Severity: critical. Reproduced by reading; deterministic.**

`src/log_pipeline/fetcher.py:237-275`:

```python
sort_criteria = [{"@timestamp": {"order": "asc"}}, {"_id": {"order": "asc"}}]
...
if len(logs) >= max_documents:
    log.warning("Hit the LOG_MAX_DOCUMENTS cap; truncating results", ...)
    logs = logs[:max_documents]
    break
```

The sort is ascending, so `logs[:max_documents]` keeps the **oldest** 50,000
lines and discards the newest — the end of the trace, which is where the
failure is. `ElasticLogSource.fetch` (`src/log_pipeline/sources/elastic.py:56-64`)
then returns a `FetchResult` with `gaps=[]` and `ok=True`.

Downstream, `reducer.branch_on_error` decides between the "stuck packet" and
"clean rejection" paths purely on `level == "ERROR"`. A noisy event whose
error was truncated away is therefore classified as a **clean rejection**, with
no banner, at full confidence. That is exactly the misclassification
`sources/k8s/parser.py`'s own docstring calls "a correctness regression that no
test of the client itself would catch".

**Change, two parts:**

1. Make `fetch_logs` return truncation state. Simplest shape that does not
   break its 4 other callers: keep returning the list, but set a module-level
   sentinel on the returned object is not viable — instead change the signature
   to return `(logs, truncated: bool)` and update `ElasticLogSource.fetch` plus
   the two direct callers in tests. Alternatively add an out-parameter dict.
   Prefer the tuple; it is honest.
2. In `ElasticLogSource.fetch`, when truncated, attach
   `EvidenceGap(GapType.TRUNCATED, …)` naming the cap and the fact that the
   **most recent** lines were dropped, so `gaps.render_banner` puts it in front
   of the trace and `apply_confidence_policy` caps confidence at
   `SYNTHESIS_GAP_CONFIDENCE_CEILING`.

**Also fix the ordering, separately:** truncating the tail is the wrong half to
keep. Either request `desc` and reverse client-side, or keep `asc` and take
`logs[-max_documents:]`. Prefer taking the tail — it preserves the query and
changes one slice. Note this in the code comment: the cap exists for memory
(`1.10`), and the tail is the evidence.

**Test:** `tests/test_log_sources.py` — patch `LOG_MAX_DOCUMENTS` to 5, feed 12
mock hits with the ERROR on the last one, assert (a) the ERROR survives,
(b) `result.gaps` contains a `TRUNCATED` gap, (c) `reduce_logs` output starts
with `BANNER_HEADER`.

### 1.2 — The reduction pipeline has no cap on decision-vocabulary lines

**Severity: critical. Reproduced — see below.**

`src/log_pipeline/reducer.py:218-227` collects *every* raw line matching
`DECISION_VOCABULARY_REGEX` with no bound, and
`pipeline._format_normal_path` prints all of them into the LLM prompt.

The default regex (`src/log_pipeline/config.py:41-50`) includes
`packet.*status`, `rejected`, and `approved` — among the most common strings in
this domain's logs. Combined with `LOG_MAX_DOCUMENTS=50000`, the "reduced"
output can be larger than the input it was meant to compress.

Reproduced:

```
$ 20,000 raw lines, each matching "packet status ..."
decision lines kept: 20000
formatted chars: 1,138,246   (~285,000 tokens)
```

`LLM_TIMEOUT_SECONDS=60` and any real context window mean this becomes either a
hard model error (→ DLQ) or an enormous bill. The codebase already knows to cap
this kind of list — `corroborate.MAX_CITATIONS = 20` does exactly that.

**Change:** add to `src/log_pipeline/config.py`:

```python
# Decision-vocabulary lines are kept in full, so an unbounded list can make the
# "reduced" output larger than the raw trace. Bounded like corroborate.MAX_CITATIONS.
MAX_DECISION_VOCABULARY_LINES = int(os.environ.get("LOG_MAX_DECISION_LINES", "300"))
```

In `apply_evidence_guardrails`, keep the **first and last** N/2 matches rather
than the first N — the decision sequence's beginning and end both carry
information, the middle repeats. Return the dropped count in the assembled
dict, and have `_format_normal_path` render an explicit
`… N further decision-vocabulary lines omitted (LOG_MAX_DECISION_LINES) …`
marker so the omission is visible to the model rather than silent.

Add the same bound to `boundary_lines`? No — it is already exactly 2.

**Also add a total-size guard.** Even with the cap above,
`_format_error_path` can emit up to `LOG_ERROR_CONTEXT_LINES` +
`LOG_ERROR_TRAILING_LINES` (400 default) plus every ERROR between them. Add a
final `LOG_MAX_REDUCED_CHARS` (default ~120,000) check in `reduce_logs` that
truncates with an explicit marker and records a `TRUNCATED` gap.

**Test:** `tests/test_log_snapshot.py` or a new `tests/test_reducer_bounds.py` —
20,000 matching lines in, assert output length under the cap and that the
omission marker is present.

### 1.3 — Pod logs escape unredacted when a container's current-instance read fails

**Severity: high (PII at rest). Reproduced — see below.**

`src/log_pipeline/sources/k8s/retrieval.py:255-334`. `read_pod_logs` reads the
*previous* container instance first and appends its records to
`outcome.records`. If the *current*-instance read then raises, the handler at
line 303 sets `outcome.ok = False` and **returns at line 311 — before the
`redaction.redact_records` call at line 323.**

`read_all` (line ~440) extends `outcome.records` with `result.records`
regardless of `result.ok`. `KubernetesLogSource._fetch`
(`sources/k8s/source.py:137-139`) then calls `snapshot.save(...)` on those
records, writing `raw_logs_k8s.jsonl` to disk or S3.

Reproduced:

```
$ previous-instance read returns {"message": "uid 123456789012 seen"}
$ current-instance read raises
ok: False
records: [{'message': 'uid 123456789012 seen', ...}]     # unredacted
read_all records: [{'message': 'uid 123456789012 seen', ...}]
```

`123456789012` is Aadhaar-shaped and `DEFAULT_PATTERNS` would have scrubbed it.
`pipeline.reduce_logs` redacts again before *its* persistence, so the leak is
confined to the snapshot artifact — but the snapshot is durable, is reused on
every retry, and in a biometric enrolment system this is exactly the data
`redaction.py`'s "ORDERING IS LOAD-BEARING" contract exists to protect.

**Change:** move redaction into a `finally`, or redact immediately before every
`return outcome` path. Cleanest:

```python
    finally:
        # Redact on EVERY exit path. A previous-instance read that succeeded
        # before the current-instance read raised still put records on this
        # outcome, and read_all keeps them regardless of `ok` -- so an early
        # return used to hand unredacted text to snapshot.save (F10 regression).
        outcome.redaction_counts = redaction.redact_records(
            outcome.records, allowlist=allowlist
        )
```

Redaction is idempotent (`redaction.py` says so explicitly), so a double pass is
safe.

**Test:** new `tests/test_redaction.py` case — force the current-instance read
to raise after a previous-instance read returned a 12-digit value; assert
`outcome.records[0]["message"]` contains `[REDACTED:AADHAAR]`.

---

## Phase 2 — API concurrency

### 2.1 — Every blocking call in the investigation path runs on the event loop

**Severity: high. Read to line; not load-tested.**

`POST /process-rejection` and `POST /analyze-rejection` are `async def`
(`src/api/routes.py:534, 559`) and `await _investigate_packet(...)`, which is
itself `async def` (line 588). The deliberate design — documented at
`routes.py:34-49` — is that `agent.invoke()` goes to a dedicated bounded
executor so it never occupies Starlette's threadpool. That part works. But
everything *around* it runs directly on the single event-loop thread:

| Line | Call | Cost |
| --- | --- | --- |
| 595 | `storage.load(event_id, "casebook.json")` | FileLock + read, or S3 GET |
| 596 | `storage.load(event_id, "status.json")` | FileLock + read, or S3 GET |
| 607 | `get_agent()` | first call: reads 5 prompt files, builds 2 LLM clients, 4 react agents, opens the checkpoint DB (Postgres `setup()` DDL) |
| 609 | `agent.get_state(config)` | checkpointer read (SQLite/Postgres) |
| 634 | `storage.save(...)` | FileLock + write, or S3 PUT |
| 754 | `upload_logs_to_s3(event_id, raw_logs)` | **network PUT of the entire log body** |
| 856 | `storage.terminal_status(event_id)` | 2 more loads |
| 868 | `storage.save_terminal(...)` | 2 writes |

That is ~8 storage round-trips plus one large S3 upload per packet, serialised
on the loop that also serves `/health`, `/ready`, `/metrics`, the
`get_api_key` / `rate_limiter` dependencies, and the dispatch of `/fetch-logs`.
Under `CASEBOOK_STORAGE_BACKEND=s3` with `MAX_CONCURRENT_INVESTIGATIONS=5`,
this reintroduces precisely the head-of-line blocking that remediation 2.6
removed.

**Change:** wrap every blocking call in the two async functions with
`await asyncio.to_thread(...)`. `main_api.py:29` already uses this idiom for
`drain_and_shutdown`, so it is the established pattern here.

Suggested shape — add a small helper at the top of `routes.py`:

```python
async def _off_loop(fn, *args, **kwargs):
    """Run a blocking storage/network call off the event loop.

    Every call below is filesystem or S3 I/O. On the loop they block /health,
    /ready and the auth dependencies for the duration -- the same head-of-line
    blocking the dedicated agent executor was introduced to remove (2.6).
    """
    return await asyncio.to_thread(functools.partial(fn, *args, **kwargs))
```

and apply it at lines 595, 596, 607, 609, 634, 754, 856, 868.

Note `get_agent()` at 607 is the expensive one on first call and must go
off-loop too — but see 2.2 first, because moving it off the loop is what makes
it concurrently reachable.

**Also reduce the round-trip count while you are here.** Lines 595/596 load
both files, and line 856 calls `terminal_status`, which loads both files
*again*. On S3 that is four GETs where two would do. `terminal_status` at 856 is
guarding against a terminal status written by *another actor since* the invoke
started, so it genuinely must re-read — but it only needs `status.json` (the
consumer's timeout handler writes both via `save_terminal`). Consider a
`terminal_status(event_id, files=("status.json",))` parameter.

**Accept:** with a stubbed slow storage backend (100 ms per call), `/health`
responds in under 50 ms while 5 investigations are in flight. Add this as a
test in `tests/test_phase2_fixes.py`.

### 2.2 — Both agent graph caches are unsynchronised lazy globals

**Severity: medium. Read to line.**

`src/core/agent_orchestrator.py:92,142-143` and
`src/dlt/orchestrator.py:35,113-114` both use the
`if _agent is not None: return _agent` … `_agent = build()` pattern with no
lock. Two concurrent first-callers each build the full graph: two sets of LLM
clients, four react agents each, and — for the rejection graph — two calls to
`get_checkpointer()`.

For the rejection path this is currently masked because `get_agent()` runs on
the event loop (2.1), which serialises it. **Fixing 2.1 removes that
accidental protection**, so this must land in the same phase.

For the DLT path it is already reachable today: `analyze_dlt` is a plain `def`
(`src/api/dlt_routes.py:330`), so it runs on Starlette's threadpool, and two
concurrent DLT cases can both enter `get_dlt_agent()`.

**Change:** guard both with a `threading.Lock` and re-check inside, exactly as
`src/core/checkpointer.py:118-127` and
`src/log_pipeline/sources/k8s/client.py:78-101` already do. Follow the
`checkpointer.py` shape for consistency.

Apply the same treatment to the other unguarded lazy globals found:
`src/storage/factory.py:6-19` (`_STORAGE_CACHE`),
`src/log_pipeline/pipeline.py:28-36` (`_cached_catalog`),
`src/tools/tool_registry.py:22-23` (`_DB_CACHE`, `_LIVE_DB_ENGINE`,
`_RULE_INDEX_CACHE`). None of these are corrupting — the duplicate object is
discarded — but `get_live_db_engine` racing creates a second SQLAlchemy pool of
10+20 connections that is then leaked.

**Test:** `tests/test_phase2_fixes.py` — 16 threads calling `get_agent()`
against a patched builder that increments a counter; assert the counter is 1.

---

## Phase 3 — Bring the DLT lane to parity with the rejection lane

The DLT lane (`src/api/dlt_routes.py`) reuses the rejection lane's *shape* but
not three of its hard-won protections. Each of these is a bug the rejection
path already fixed and documented.

### 3.1 — `/analyze-dlt` has no server-side invocation budget

**Severity: high. Read to line.**

`analyze_dlt` calls `orchestrator.investigate(...)` (line 388) with no timeout.
The DLT analysis consumer's client-side budget is
`DLT_ANALYSIS_TIMEOUT_SECONDS` (default 300s,
`src/utils/kafkaConsumer.py:63-64`). When it fires, the consumer writes
`FAILED_TIMEOUT` to `dlt_cases/` via `DltAdapter.timeout_casebook` and DLQs the
message — while the API thread keeps running the abandoned investigation.

This is bug 0.8 verbatim. The rejection path solved it with
`_get_agent_invoke_timeout_seconds()` (`routes.py:190-203`) and
`asyncio.wait_for`.

**Change:** make `analyze_dlt` `async def`, run the graph on a bounded executor
under `asyncio.wait_for`, mirroring `routes.py:646-682`:

```python
DLT_ANALYZE_TIMEOUT_SECONDS = float(os.environ.get(
    "DLT_ANALYZE_TIMEOUT_SECONDS",
    max(float(os.environ.get("DLT_ANALYSIS_TIMEOUT_SECONDS", "300")) - 30, 30),
))
```

On timeout: `storage.save_terminal(case_id, {... "packet_status": {"status": "FAILED_TIMEOUT"} ...})`
and return `{"status": "failed_timeout", "case_id": case_id}`.

### 3.2 — `/analyze-dlt` has no late-result guard

**Severity: high. Read to line.**

`analyze_dlt` checks `storage.terminal_status(case_id)` once at line 341, then
runs for minutes, then calls `storage.save_terminal(case_id, casebook)` at line
443 with **no re-check**. A `FAILED_TIMEOUT` written by the consumer while the
analysis was in flight is silently overwritten with a "successful" casebook,
while the DLQ message stays queued.

The rejection path guards this at `routes.py:849-866` using
`PROTECTED_TERMINAL_STATUSES`.

**Change:** immediately before line 443:

```python
    # Same guard as routes.py:856 -- the consumer's own client-side timeout may
    # have written FAILED_TIMEOUT/DLQ while this analysis was still running,
    # and a late "successful" casebook must not overwrite that verdict (0.8/F4).
    recorded = storage.terminal_status(case_id)
    if recorded in PROTECTED_TERMINAL_STATUSES:
        log.warning("Discarding late DLT result; a terminal status was already "
                    "recorded by another actor", recorded_status=recorded)
        return {"status": "already_processed", "case_id": case_id}
```

Import `PROTECTED_TERMINAL_STATUSES` from `src.storage.base` (line 42 already
imports `LOGS_FETCHED_STATUS, TERMINAL_STATUSES` from there).

### 3.3 — `/analyze-dlt` runs multi-minute LLM work on Starlette's shared threadpool

**Severity: medium. Read to line.**

Being a sync `def`, `analyze_dlt` occupies one of anyio's default 40 threadpool
slots for the whole investigation — the same pool `/health`, `/ready`,
`/fetch-logs` and `/fetch-dlt-logs` are dispatched on. `routes.py:34-49`
documents at length why the rejection path does not do this.

**Change:** covered by 3.1 — making it `async def` on a dedicated
`concurrent.futures.ThreadPoolExecutor` sized from
`MAX_CONCURRENT_INVESTIGATIONS` solves 3.1 and 3.3 together. Reuse
`routes._agent_invoke_executor` or create a sibling
`_dlt_invoke_executor`; a sibling is better, so a DLT backlog cannot starve the
rejection lane.

**Test:** new `tests/test_dlt_timeout.py` — patch `orchestrator.investigate` to
sleep past the budget; assert `FAILED_TIMEOUT` is recorded and that a
pre-existing `FAILED_TIMEOUT` is not overwritten by a slow-returning analysis.

---

## Phase 4 — Shutdown and lifecycle

### 4.1 — The consumer's poll loop can block through its entire drain budget

**Severity: high. Read to line.**

`src/utils/kafkaConsumer.py:766-773`:

```python
for msg in messages:
    if _shutdown.is_set():
        break
    _queue_semaphore.acquire()      # <-- unbounded blocking wait
    _handle_one_message(tp, msg)
```

`acquire()` takes no timeout. When all `MAX_CONCURRENT_INVESTIGATIONS` workers
are busy, the loop parks here. `request_shutdown` sets `_shutdown` from the
SIGTERM handler, but a thread already inside `acquire()` does not observe it —
it waits until a worker finishes, which for the slow consumer is up to
`PACKET_TIMEOUT_SECONDS` (300s). `SHUTDOWN_DRAIN_SECONDS` is 25s and a typical
`terminationGracePeriodSeconds` is 30s, so the pod is SIGKILLed before
`_drain_and_commit` ever runs — defeating the entire F12 drain design under
exactly the sustained load it was built for.

**Change:**

```python
                    # Bounded so a SIGTERM arriving while every worker slot is
                    # busy is observed within a second, rather than parking here
                    # for a full PACKET_TIMEOUT_SECONDS and being SIGKILLed
                    # before the drain runs (F12).
                    while not _queue_semaphore.acquire(timeout=1.0):
                        if _shutdown.is_set():
                            break
                    else:
                        _handle_one_message(tp, msg)
                        continue
                    break
```

or, more readably, factor it into a `_acquire_slot() -> bool` helper that
returns `False` on shutdown, and `break` out of both loops when it does. Not
dispatching is correct: the offset is never committed, so Kafka redelivers.

**Test:** `tests/test_resilience.py` — drain the semaphore, set `_shutdown` from
another thread, assert the poll loop returns within ~2s.

### 4.2 — `_draining` is set too late to be observed

**Severity: low. Inferred from uvicorn's shutdown sequence; not executed.**

`drain_and_shutdown` sets `_draining` (`routes.py:82`) and its docstring says
`/ready` "starts failing so the orchestrator stops routing new work here". But
it is only ever called from the lifespan shutdown hook
(`src/main_api.py:24-29`), which uvicorn runs *after* it has already closed the
listening socket. By then no orchestrator can probe `/ready` at all — readiness
fails by connection refusal, not by the 503 the code intends.

The behaviour is accidentally correct; the mechanism described in the comment
does not exist.

**Change (small):** install a SIGTERM handler in `main_api.py` that sets
`routes._draining` immediately, chaining to uvicorn's own handler so the normal
shutdown still proceeds. That gives a real window where the pod answers 503 on
`/ready` while still serving in-flight requests. If you would rather not add
signal handling to the API process, instead correct the docstrings at
`routes.py:76-82` and `routes.py:396-401` to describe what actually happens.

### 4.3 — `_in_flight_events` is a set, so a duplicate event id deregisters early

**Severity: low. Read to line.**

`routes.py:57-58, 152-166`. If the same `event_id` is in flight twice (a
redelivery race that slips past the dedupe), the first `finally` block's
`discard` removes the id while the second invocation is still running, so
`drain_and_shutdown` will not mark it. Change `_in_flight_events` to a
`collections.Counter` and decrement.

---

## Phase 5 — Multi-pod correctness

Two mechanisms coordinate through the local filesystem while the data they
guard lives in shared storage. Both are silent under
`CASEBOOK_STORAGE_BACKEND=s3` with more than one replica — the configuration
Phase F of the deployment plan exists to enable.

### 5.1 — DLT group counters use a local lock over shared storage

**Severity: medium. Read to line.**

`src/dlt/groups.py:49-61` builds its `FileLock` under `LOCAL_CHECKPOINTS_DIR`,
and its docstring claims it is "a cross-process guard so two workers cannot lose
an increment … the DLT analysis role is meant to scale out to several pods". But
`get_group_storage()` (`src/dlt/case_storage.py:31-42`) returns an
`S3CasebookStorage` when the backend is s3, while the lock stays on each pod's
own disk. `record_occurrence` and `attach_recommendation` are then unguarded
read-modify-write cycles against S3: occurrence counts are lost, and two pods
analysing the same novel fingerprint can each write a different
`recommendation`, last-writer-wins.

`src/storage/s3.py`'s own module docstring already acknowledges S3 gives no
mutual exclusion — the group path is the one place that materially depends on
it.

**Change (choose one, in order of preference):**

1. Use a conditional write. `put_object` with `IfNoneMatch: "*"` for creation
   plus an `If-Match: <etag>` retry loop for updates gives real
   compare-and-swap on S3 today. Requires adding an etag-aware
   `save_if_unchanged` to `CasebookStorage`.
2. Accept the imprecision explicitly: make `occurrence_count` advisory, document
   it in the module docstring and in the casebook's `provenance` block, and add
   an `S3-backed group records are not transactionally counted` note. Cheap and
   honest.
3. Gate it: have `config_validator` reject
   `CASEBOOK_STORAGE_BACKEND=s3` + `DLT_ENABLED=true` + `API_REPLICA_COUNT > 1`
   until (1) exists, mirroring the existing check at
   `src/utils/config_validator.py:88-101`.

Correct the docstring at `groups.py:49-56` either way — it currently asserts a
guarantee the code does not provide.

### 5.2 — The replay approval queue is per-pod local disk

**Severity: medium. Read to line.**

`tool_registry.queue_for_replay` (line ~525) writes to
`src/db/pending_replays.jsonl` on the local filesystem, and
`src/tools/approve_replays.py:14-16` reads the same local path. With more than
one API replica, a packet nominated for replay on pod A is invisible to an
operator running `approve_replays.py` anywhere else. This becomes materially
worse once `DLT_AUTO_REPLAY_ENABLED` is turned on
(`src/dlt/auto_replay.py`), since that path nominates packets automatically.

**Change:** route the queue through `CasebookStorage` — append to a
per-event artifact (`pending_replay.json` beside the casebook) rather than one
shared append-only file, and have `approve_replays.py` enumerate via
`storage.list_events()`, exactly as `outcomes.iter_outcomes()` was changed to do
for the same reason (G2). Per-event files also remove the append-under-lock
contention entirely.

---

## Phase 6 — Correctness and observability cleanups

Small, independent, low-risk. Do them in any order.

### 6.1 — `INVESTIGATOR_RETRIES` never records the escalation path

`agent_orchestrator.py:601` observes the histogram only in `synthesis_node`.
`escalate_node` — reached exactly when `retry_count >= MAX_INVESTIGATION_RETRIES`,
i.e. the packets with the *most* retries — never observes. The histogram
therefore has a hard ceiling at `max_retries - 1` and understates the tail it
exists to measure. Add the same `observe()` to `escalate_node`.

### 6.2 — `ceilings_applied` lists ceilings that did not bind

`src/models/dlt_synthesis.py:118-123`:

```python
def cap(value: float, label: str):
    nonlocal ceiling
    if value < ceiling:
        ceiling = value
    applied.append(label)      # <-- outside the if
```

The docstring says "every one that binds is named in `ceilings_applied`", but
every ceiling *evaluated* is named. A reader auditing why a confidence is low is
shown ceilings that had no effect. Move `applied.append(label)` inside the `if`.

### 6.3 — The DLT log window's trailing bound is never applied

`src/dlt/window.py:97-105`. `LogWindow.end_ms` is computed and the docstring
says "The trailing bound … is applied during filtering instead" — but nothing
filters on it. `to_time_window()` yields `start → now`, so for the plan's own
43-hour reference sample the fetch spans 43 hours of every pod rather than the
intended ~7-minute window. `too_old` (24h default) currently masks this for the
reference case, but not for a 20-hour-old one.

Either implement the trailing filter in `retrieval._read_instance` (drop
records whose kubelet timestamp exceeds `end_ms`) or delete the claim from the
docstring and from `LogWindow.describe()`.

### 6.4 — `call_with_retry`'s kwargs can collide with its own parameters

`src/log_pipeline/sources/k8s/retry.py:83` — `call_with_retry(func, *args,
max_attempts=…, sleep=…, **kwargs)` is called with `**list_kwargs` at
`discovery.py:355`. Any Kubernetes API kwarg named `max_attempts` or `sleep`
would silently rebind the retry policy. Make them keyword-only *and* prefix
them (`_max_attempts`, `_sleep`), matching the `_request_timeout` /
`_preload_content` convention the kubernetes client already uses.

### 6.5 — `record_llm_usage` sums usage across all messages in a response

`src/utils/metrics.py:295-310` iterates every message in the react agent's
returned list and sums `usage_metadata`. That is correct for a single invoke,
but `synthesis_node` returns `res["messages"]` into graph state
(`agent_orchestrator.py:646`), and on a retry loop the same messages can be
counted again. Verify against a real multi-turn run before changing anything —
this one is flagged for confirmation, not asserted.

### 6.6 — Producer consolidation

`dlq_publisher.get_producer`, `analysis_queue_publisher.get_producer` and
`analysis_queue_publisher.get_dlt_producer` are three copies of the same
`KafkaProducer(bootstrap_servers=brokers, value_serializer=…, acks="all",
retries=3)` construction, and every process holds all three connection pools.
The comment at `analysis_queue_publisher.py:66-70` justifies separate *topics*,
which is right, but not separate *producers* — a Kafka producer multiplexes
topics. Factor into one `_get_producer()` in a shared module. Purely a resource
saving; no behaviour change.

### 6.7 — Duplicate DLQ publish attempt on a poison pill

`kafkaConsumer.py:441-447` publishes to the DLQ for a poison pill, then
`_record_completion`. If that publish raises, the outer `except` at line 466
calls `_dlq_and_abandon`, which publishes *again*. Harmless (the first failed)
but the log reads as two failures. Guard with a flag or narrow the outer handler.

---

## Sequencing

Phases 0 → 1 → 2 must run in order: 0 gives you a signal, and 2.2 must land with
2.1 because 2.1 removes the accidental serialisation that currently hides 2.2.

Phases 3, 4, 5 and 6 are independent of each other and of 1–2, and can be
parallelised across agents after Phase 0 lands.

| Phase | Files touched | Suggested budget |
| --- | --- | --- |
| 0 | `pyproject.toml` area, `tests/conftest.py` (new), 2 test files | 1–2 h |
| 1 | `fetcher.py`, `sources/elastic.py`, `reducer.py`, `config.py`, `pipeline.py`, `sources/k8s/retrieval.py` | 1 day |
| 2 | `api/routes.py`, `core/agent_orchestrator.py`, `dlt/orchestrator.py`, `storage/factory.py`, `tools/tool_registry.py` | 1 day |
| 3 | `api/dlt_routes.py` | half a day |
| 4 | `utils/kafkaConsumer.py`, `main_api.py`, `api/routes.py` | half a day |
| 5 | `dlt/groups.py`, `dlt/case_storage.py`, `tools/tool_registry.py`, `tools/approve_replays.py`, `utils/config_validator.py` | 1 day |
| 6 | scattered, small | half a day |

Every phase must leave `pytest`, `ruff check .` and the new tests green before
the next begins.
