# Agentic Resident CRM: Kubernetes Log Source -- Engineering Design

Design date: 2026-08-11. Status: **Phases 1-10 implemented. Phase 0 (the
Elasticsearch diagnostic) is still outstanding -- its tooling is built but has
not been run against live infrastructure, so Section 12 remains unfilled.**

**Update (2026-08-12): the default was flipped to `kubernetes,elastic`**
(operator decision, made without Phase 0/5 real-cluster validation -- see the
caveat in section 12). It no longer ships fully dark: on any deployment where
`K8S_DEFAULT_NAMESPACE`/`KUBECONFIG_PATH` are unset, the Kubernetes leg fails
fast on missing config and every fetch falls straight through to
Elasticsearch, so behaviour is unchanged in practice until those are filled
in -- but the moment they are, Kubernetes becomes the first thing tried on
every single packet, without Open Questions 1-3 having been answered from a
real cluster first.

**Update (2026-08-13): pod discovery adapted to a namespace-scoped-only RBAC
model.** A validated reference script (working, in-cluster, against a
different namespace/service) confirmed the ServiceAccount pattern in use here
cannot `list` namespaces or pods cluster-wide -- only a targeted `get` on an
already-known namespace, and a `list` scoped to it. Discovery now does a
`read_namespace` pre-flight before listing anything (5.2), and pod matching
defaults to a client-side name-substring match (`PodMatchSpec`) rather than a
server-side label selector, since the reference pattern lists every pod in
the namespace with no selector at all. Label-selector matching is still
available as an opt-in per app via `K8S_SERVICE_MAP`. See 5.2, 6.5, and 7 for
the updated design, RBAC, and config surface.

**How to use this document.** Sections 1-8 are the design. Section 9 is the
implementation plan, broken into self-contained phases. Each phase lists the
exact files it touches, the config it adds, its tests, its exit criteria, and
what is explicitly out of scope. Every phase leaves the test suite green, so
work can stop after any phase and resume later -- or in a fresh session --
without carrying context forward.

---

## 1. Problem statement

Elasticsearch is the only log source today (`src/log_pipeline/fetcher.py`), and
lines are reported missing from it. An investigation built on incomplete
evidence produces a *confident but wrong* casebook, which is materially worse
than producing none: the Reviewer cannot catch a hallucination whose supporting
evidence was never fetched, and the resulting resolution reaches a real
resident.

This document specifies a **supplementary** log source that reads directly from
pods via the Kubernetes API, and specifies how the system behaves when evidence
is known to be incomplete.

**Kubernetes is not a replacement for Elasticsearch.** Kubelet retention is
minutes to hours; Elasticsearch holds the long tail and remains the system of
record. The Kubernetes source exists to cover the recent window when ES has
dropped lines. The two are selected by configuration, optionally as a fallback
chain.

This work also retires the `fetch_kubernetes_logs` stub in
`src/tools/tool_registry.py:244`, which returns a hardcoded `[MOCK]` string and
is registered in `_TOOLS_MAP` but invoked by nothing.

---

## 2. Phase 0: diagnose before building

**A significant fraction of the "missing" logs may be self-inflicted.** Three
findings from the current query in `src/log_pipeline/fetcher.py`:

**2.1 -- A hardcoded single-service filter (lines 110-112).**

```python
filter_clauses = [
    {"term": {"application_name.keyword": "enu-biometric"}}
]
```

Every line from every other microservice is excluded *by construction*. If a
packet traverses more than one service, those logs were never requested. This
is the primary suspect and is a one-line fix if confirmed.

**2.2 -- An analyzer-dependent phrase query (line 108).**

```python
{"query_string": {"query": f'"{event_id}"'}}
```

Behaviour depends entirely on the field's analyzer and how the id is tokenized
in the indexed document. Mismatches fail silently -- zero hits looks identical
to "the service logged nothing."

**2.3 -- A latent evidence-deleting filter (lines 114-118).**

The catalog-driven `must_not` clauses are inert today because no
`local_checkpoints/template_catalog.json` exists. Building a catalog turns them
on, and remediation item 1.2 (open in `ARCHITECTURE.md`) documents that a
catalog built under the pre-fix Drain3 clustering classifies nearly everything
as boilerplate -- stripping real evidence *before it is fetched*. A landmine to
defuse, not a current cause.

### Procedure

Steps 1-2 are automated by `src/tools/es_diagnostic.py`. Steps 3-4 require
cluster access and are manual.

1. Pick 5 recently rejected `eventId`s with known-incomplete casebooks.
2. Run the diagnostic from a host with network access to `ES_HOST`:

   ```bash
   python3 -m src.tools.es_diagnostic --event-ids ID1 ID2 ID3 ID4 ID5 \
       --json phase0_results.json
   ```

   It runs four query variants per id and aggregates which `application_name`
   values actually logged that id:

   | Variant | Query |
   |---|---|
   | A | Exactly today's production query (app filter + quoted `query_string` + catalog `must_not`) |
   | B | A minus the `application_name` filter |
   | C | A with `multi_match` instead of the quoted `query_string` |
   | D | Neither the filter nor the quoted syntax (widest) |

   Isolating one change per variant makes a hit-count difference attributable
   to exactly one cause. `--dry-run` prints the queries without contacting ES.

   Variant A is asserted byte-identical to `fetcher.fetch_logs`'s query by
   `tests/test_es_diagnostic.py`; if the fetcher changes and the diagnostic
   does not, that test fails rather than silently invalidating the gate.

3. In parallel, `kubectl logs` the relevant pods for the same ids and count
   occurrences.
4. Record how long after the event the investigation ran, and the node's actual
   `containerLogMaxSize` / `containerLogMaxFiles`.

The tool prints the exact `kubectl` invocations for steps 3-4 at the end of its
report.

### Decision gate

| Outcome | Action |
|---|---|
| (b) or (c) recovers most missing lines | **Stop.** Fix the ES query. Re-evaluate whether this project is needed. |
| ES genuinely lossy, kubelet retention exceeds investigation lag | Proceed; target `kubernetes,elastic` chain |
| ES lossy, kubelet retention shorter than investigation lag | Proceed, but **snapshot-first (4.3) becomes mandatory** -- without it the source is useless on delayed paths |

Half a day that can invalidate weeks of work. Not optional.

---

## 3. Design principles

1. **Elasticsearch remains primary.** Kubernetes supplements the recent window;
   it never becomes the system of record.
2. **Incomplete evidence is announced, never inferred.** Any gap between what
   was requested and what was retrieved is surfaced explicitly to the LLM.
3. **Three outcomes stay distinguishable:** found / confirmed-absent /
   could-not-look. Collapsing the last two lets the agent conclude "no errors
   occurred" when the truth is "we could not read the logs."
4. **A source failing must not fail the packet.** In a chain, one source dying
   advances to the next; total exhaustion returns `None`, never an error string.
5. **The existing local CSV mock is unchanged.** `ES_MOCK_FILE` keeps working
   exactly as it does today, byte for byte. Any Kubernetes fixture mechanism is
   strictly additive and independent.
6. **Capture time is decoupled from analysis time.** Kubelet retention is short;
   investigations replay days later.
7. **Nothing unredacted touches disk.** This system handles biometric enrolment
   data.
8. **Deterministically invoked, never LLM-callable.** The Investigator runs with
   `tools=[]` by design (remediation 1.16); fetching stays in Python.
9. **Bounded everything** -- pods, bytes, concurrency, wall-clock.

---

## 4. Architecture

### 4.1 The integration seam

Stages 2-4 of the reduction pipeline are source-agnostic. They consume:

```python
class LogRecord(TypedDict):
    timestamp: str      # RFC3339; ordering key
    level: str          # ERROR | WARN | INFO | DEBUG | TRACE
    message: str
    app_name: str
    # optional, K8s-only
    pod_name: NotRequired[str]
    container: NotRequired[str]
    source: NotRequired[str]              # "elastic" | "kubernetes"
    container_instance: NotRequired[str]  # "current" | "previous"
```

`branch_on_error` reads `level`; `cluster_logs` reads `message` and
`timestamp`; the guardrails read all four; `pipeline._format_*` and
`_save_raw_logs` render `timestamp` / `app_name` / `level` / `message`.

**Emitting this shape means Drain3 clustering, the evidence guardrails, the S3
offload, and the casebook wiring all work unchanged.** This is a Stage 1
addition, not a pipeline rewrite.

### 4.2 The `LogSource` protocol

Mirroring the existing `CasebookStorage` Protocol in `src/storage/base.py`:

```python
class LogSource(Protocol):
    name: str
    def fetch(self, identifier: str, window: TimeWindow,
              ctx: FetchContext) -> FetchResult: ...

@dataclass
class FetchResult:
    records: list[LogRecord]
    gaps: list[EvidenceGap]        # first-class, not a side channel
    diagnostics: FetchDiagnostics  # counts, bytes, latency, pods touched
    ok: bool                       # False == could-not-look
```

Gaps are part of the return type. A source that cannot express "I returned less
than you asked for" structurally cannot satisfy principle 2.

`ElasticLogSource` wraps today's fetcher **without modifying it** -- including
its `ES_MOCK_FILE` CSV branch. `KubernetesLogSource` is new.

### 4.3 Snapshot-first capture

Kubelet retention is roughly 10MB x 5 files per container; a busy service can
lose a two-hour window in minutes. Investigations do *not* reliably run
promptly -- consumer lag, DLQ replays, `MAX_IN_PROGRESS_AGE_SECONDS` staleness
resumption, checkpoint resumes, and the retry loop all re-enter `fetch_logs`
well after the event. On every one of those paths a naive
fetch-at-analysis-time design returns nothing.

Therefore:

1. On first successful K8s fetch for an `event_id`, persist canonical records to
   `local_casesheets/casebook_<event_id>/raw_logs_k8s.jsonl` -- structured
   JSONL, not the formatted `raw_logs.txt`, so Stages 2-4 can be re-run later.
2. Persist capture-time gaps to `log_snapshot_meta.json` alongside.
3. On any subsequent fetch for the same `event_id`, if a snapshot exists and
   `LOG_SNAPSHOT_REUSE=true` (default), load it and **skip the API entirely**.
4. Write atomically via `.tmp` + `os.replace` under a `filelock`, reusing the
   discipline in `src/storage/local.py`.

Retries become deterministic and free, the cluster is not re-hit per retry-loop
iteration, and evidence captured while it existed survives long after the
kubelet dropped it. A **failed** fetch is never snapshotted -- a failure must
not poison future attempts with a cached empty result.

### 4.4 Source selection: an ordered chain

`LOG_SOURCE` is an ordered, comma-separated chain. A single value means a single
source; multiple values mean fallback in order.

| `LOG_SOURCE` | Behaviour |
|---|---|
| `kubernetes,elastic` | Try Kubernetes; fall back to Elasticsearch. **Default.** |
| `elastic,kubernetes` | Try Elasticsearch; fall back to Kubernetes. |
| `elastic` | Elasticsearch only (behaviour prior to the Kubernetes source). |
| `kubernetes` | Kubernetes only, no fallback. |

**Fallback triggers when a source returns `ok=False` (failed) or an empty
record set.** Both mean "we did not get logs here," which is the operative
condition. The chain stops at the first source returning a non-empty result;
exhausting the chain returns `None`.

Provenance is preserved: the winning source is recorded in each record's
`source` field, and a `SOURCE_FALLBACK` note lists what was tried and why each
attempt was abandoned. An operator reading a casebook can always tell where its
evidence came from.

**Results are never merged or de-duplicated across sources.** One source wins
per fetch. Rationale in Appendix A.

---

## 5. Component design

### 5.1 Client lifecycle and authentication

Load order:

1. `config.load_incluster_config()` -- preferred; no secrets on disk.
2. `config.load_kube_config(config_file=KUBECONFIG_PATH, context=K8S_CONTEXT)`.
3. Neither -- log once at startup, mark the source unavailable, **never crash
   the process.** A log source is not a hard dependency of the API.

The `CoreV1Api` client is a module-level singleton, matching the discipline
applied to the Drain3 miner (2.5) and the SQLAlchemy engine
(`tool_registry.get_live_db_engine`). Per-call construction re-parses the
kubeconfig and re-establishes TLS on every packet.

Security:

- `KUBECONFIG_PATH` must point **outside the repository.** `.gitignore` has no
  `*.yaml` rule, so an in-tree kubeconfig would be committed. Add the ignore
  rule before anyone is tempted.
- TLS verification defaults on. `K8S_VERIFY_SSL=false` is an explicit,
  warning-logged opt-out with optional `K8S_CA_CERT_PATH`. Same rule as the ES
  `verify_certs` fix in 1.9.
- Prefer a long-lived ServiceAccount token over exec-based auth (OIDC / cloud
  plugins), which needs helper binaries in the container and fails opaquely.

### 5.2 Pod discovery

No Kubernetes API aggregates logs across a Deployment; the tool lists and
iterates. The ServiceAccounts available in this cluster are namespace-scoped
only -- there is no `list namespaces` or `list pods --all-namespaces`
permission, confirmed against a real, working reference script pulling logs
for `centralized-rule-engine` in the `ankalan` namespace. Two consequences
follow directly from that constraint:

- **The namespace must always be resolved up front, never discovered.** Before
  listing anything, a targeted `v1.read_namespace(name=namespace)` verifies
  the namespace exists and is reachable with the current credentials. A 403
  here means RBAC denial; a 404 means the namespace name is wrong -- both are
  distinct, both are logged distinctly, and both short-circuit discovery
  before `_list_pods` is attempted.
- **Pod matching defaults to a client-side name substring, not a label
  selector.** The reference pattern lists every pod in the namespace with no
  `label_selector` and matches with `search_string in pod.metadata.name`,
  because the real services here don't guarantee consistent labels. This is
  modelled as `PodMatchSpec(mode, value)`:
  - `mode="name_contains"` (the default) -- lists the namespace with no
    selector kwarg at all, then keeps pods whose name contains `value`
    client-side. Still a namespace-scoped `list`, not a cluster-wide one.
  - `mode="label"` (opt-in) -- passes `label_selector=value` to
    `list_namespaced_pod` server-side, for a service that does have reliable
    labels.
  `resolve_service(app, namespace)` returns `(namespace, PodMatchSpec)`.
  `K8S_SERVICE_MAP` selects the mode per app: a `"label_selector"` key opts
  into label mode; a `"name_contains"` key overrides the substring (default
  is the app name itself); neither key falls back to `name_contains` on
  `app`.
- **Phase filter.** Skip only `Pending` (no logs exist yet). **Explicitly
  include `Failed` and `Succeeded`** -- a terminated pod frequently holds the
  exact crash evidence needed. Filtering to `Running` is the most common
  mistake in code like this.
- **Container selection.** Read `pod.spec.containers`; drop sidecars via
  `K8S_SIDECAR_DENYLIST`. One remaining container is used directly; several are
  each fetched and tagged. Omitting `container=` on a multi-container pod is an
  API error, not a default.
- **Fan-out cap.** `K8S_MAX_PODS` (default 20), most recently started first;
  record a `TRUNCATED` gap when it bites.

### 5.3 Log retrieval

```python
v1.read_namespaced_pod_log(
    name=pod_name, namespace=namespace, container=container_name,
    since_seconds=int(window.hours * 3600),
    timestamps=True,
    limit_bytes=K8S_MAX_BYTES_PER_POD,
    _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
    _preload_content=False,
)
```

- **`timestamps=True` is mandatory.** The kubelet prefixes every line with an
  RFC3339Nano timestamp, giving a reliable ordering key even when the
  application's own format is unparseable.
- **`_preload_content=False` is mandatory.** All filtering is client-side, so
  logs must be streamed and filtered line-by-line. Buffering 20 pods x 10MB is
  200MB resident in a process that also runs the graph.
- **Parallel fan-out** via a bounded `ThreadPoolExecutor`
  (`K8S_FETCH_CONCURRENCY`, default 5), matching the bounded-executor pattern
  from 2.6.

### 5.4 Restart and crash handling

Read `pod.status.container_statuses[].restart_count`.

**In-place restart (pod survives).** The default call returns only the current
container -- the pre-crash lines, usually the most valuable in the trace, are
invisible. When `restart_count > 0`, issue a second call with `previous=True`
and prepend those records tagged `container_instance="previous"`.
`previous=True` returns HTTP 400 when no previous container exists; expected,
caught, logged at debug, never an error.

**Pod replaced (pod deleted).** Permanently unrecoverable through the API. The
tool detects it: if a matching pod's `status.start_time` is later than the
window start, part of the window was served by a pod that no longer exists.
Emit `POD_REPLACED` rather than presenting a partial trace as complete.

### 5.5 Parsing to the canonical record

Kubernetes returns raw text where ES returned structured `_source`. Per line:

1. **Strip the kubelet timestamp** -- becomes `timestamp`, the authoritative
   ordering key.
2. **Try JSON** (common for Spring Boot with a logstash encoder): read `level`,
   `message`, `application_name`, preferring the app's own `@timestamp`.
3. **Fall back to regex** -- extract `ERROR|WARN|INFO|DEBUG|TRACE` from the
   first ~80 characters; `message` is the remaining line.
4. **Default to `INFO`** only after both fail.

**Format auto-detection.** Sample the first 50 non-empty lines per
`(namespace, container)`; if >80% parse as JSON, use JSON mode for that
container, else regex. Cache for the process lifetime. Avoids per-service
configuration that will drift.

**This is the highest-risk component in the design.** `branch_on_error` chooses
between the "stuck packet" and "clean rejection" paths purely on
`level == "ERROR"`. If parsing silently yields `INFO` for everything, the ERROR
branch never fires and every stuck packet is misclassified -- a correctness
regression no test of the Kubernetes client itself would catch. Mitigations:
unit tests using **real captured samples** from each target service, and a
`LEVEL_PARSE_DEGRADED` gap when the failure rate exceeds
`K8S_LEVEL_PARSE_WARN_THRESHOLD` (default 0.9).

### 5.6 Filtering and context windows

The Kubernetes API has no server-side grep, so filtering is client-side against
streamed lines.

- **Search terms.** `K8S_SEARCH_FIELDS` (default `eventId,refId`) -- keep a line
  if any configured identifier value appears. Which id the services actually log
  is Open Question 1; searching both is cheap insurance.
- **Context windows.** A bare identifier match is often one line of a stack
  trace whose useful part never repeats the id. Keep
  `K8S_CONTEXT_LINES_BEFORE` (5) and `K8S_CONTEXT_LINES_AFTER` (20) around each
  match, merging overlapping windows.

### 5.7 Ordering

Within the Kubernetes source, order by kubelet timestamp, with `previous`
container instances preceding `current` for the same pod.

**Clock skew is real** -- kubelet timestamps come from node clocks, which drift
relative to each other. Ordering across pods is therefore approximate, and
records carry `pod_name` so the LLM can attribute a line to a replica rather
than relying on interleaving alone. Cross-*source* ordering never arises,
because sources are never merged (4.4).

### 5.8 Evidence gaps

| Gap | Trigger |
|---|---|
| `LOG_ROTATION` | Oldest returned line is newer than `now - since_hours` |
| `POD_REPLACED` | A matched pod's `start_time` is later than the window start |
| `TRUNCATED` | `limit_bytes` or `K8S_MAX_PODS` hit |
| `POD_VANISHED` | A pod 404'd between list and read |
| `LEVEL_PARSE_DEGRADED` | Level-parse failure rate above threshold |
| `SOURCE_FALLBACK` | A source returned nothing and the chain advanced |

Rendered as a banner ahead of the trace:

```text
--- EVIDENCE GAPS (the trace below is INCOMPLETE) ---
LOG_ROTATION: requested from 08:00:00Z, oldest available line is 09:14:22Z.
  Logs before 09:14:22Z were rotated off the node and are unrecoverable.
POD_REPLACED: pod enu-biometric-7d4f started 09:02:11Z, after the window began.
  An earlier pod served part of this window; its logs are gone.
--- END EVIDENCE GAPS ---
```

`InvestigatorAgent.md` and `ReviewerAgent.md` gain a clause: when a gap banner
is present, absence of evidence must not be treated as evidence of absence; the
finding is qualified, and the Reviewer should reject a conclusion that depends
on the missing window.

### 5.9 PII redaction

Raw pod logs are unfiltered where the ES path source-filtered to four fields. In
a biometric enrolment context they may carry resident identifiers or full
request payloads -- and this text is persisted to `raw_logs.txt`, to the
snapshot, and potentially to S3 via the >5000-char offload.

**Ordering matters and is easy to get wrong:**

```text
fetch -> filter by identifier -> extract context -> REDACT -> persist -> pipeline
```

Redaction runs **after** identifier filtering (the raw id must stay matchable)
and **before** any persistence. Unredacted text never reaches disk.

| Pattern | Replacement |
|---|---|
| 12-digit run, optionally `4-4-4` spaced | `[REDACTED:AADHAAR]` |
| 16-digit run | `[REDACTED:VID]` |
| 10-digit starting 6-9 | `[REDACTED:MOBILE]` |
| Email address | `[REDACTED:EMAIL]` |
| `K8S_REDACT_EXTRA_PATTERNS` | `[REDACTED:CUSTOM]` |

Placeholders are retained so the LLM knows a value existed. Internal
correlation ids (`eventId`, `refId`, `srn`) are allowlisted -- operational
identifiers, not resident PII, and redacting them would destroy the
investigation.

**Over-redaction is a real risk** (a 12-digit correlation id would be scrubbed).
Per-pattern redaction counts are logged and included in `FetchDiagnostics`, so
over-scrubbing is visible rather than mysterious.

---

## 6. Cross-cutting concerns

### 6.1 Retry and circuit breaking

Add `k8s_breaker` to `src/utils/resilience.py`, mirroring `es_breaker`
(`fail_max=3`, `reset_timeout=60`).

`kubernetes.client.exceptions.ApiException` must **not** be blanket-retried --
retrying a 403 wastes the packet's entire budget on a call that can never
succeed. This requires a predicate, not an exception-type tuple:

| Condition | Retry | Rationale |
|---|---|---|
| 401 | No | Credentials invalid; will not self-heal |
| 403 | No | RBAC misconfiguration; log distinctly |
| 404 | No -- skip pod | Pod vanished; continue the loop |
| 400 on `previous=True` | No -- expected | No previous container exists |
| 410 Gone | No | Log expired server-side |
| 429 | Yes -- backoff + jitter | Rate limited |
| 500 / 502 / 503 / 504 | Yes | Transient server error |
| Connection / read timeout | Yes | Transient network |

Jitter is required on 429: synchronised retries across five worker threads
against a rate-limited API server is a self-inflicted thundering herd.

### 6.2 Resource bounds and performance budget

`AGENT_INVOKE_TIMEOUT_SECONDS` defaults to `PACKET_TIMEOUT_SECONDS - 30` = 270s,
and the LLM cycle needs the bulk of it.

| Bound | Value | Rationale |
|---|---|---|
| `K8S_TOTAL_FETCH_TIMEOUT_SECONDS` | 60 | p95 target; leaves >200s for the agents |
| `K8S_REQUEST_TIMEOUT_SECONDS` | 30 | Per API call |
| `K8S_MAX_BYTES_PER_POD` | 10 MiB | Server-side cap |
| `K8S_MAX_PODS` | 20 | Fan-out cap |
| `K8S_FETCH_CONCURRENCY` | 5 | Matches consumer concurrency |
| Peak resident memory | < 50 MB | Guaranteed by streaming, not by the caps |

The total-fetch budget is a wall-clock deadline checked between pods, so a slow
cluster degrades to partial results plus `TRUNCATED` rather than consuming the
whole packet budget.

### 6.3 Degradation matrix

| Condition | `elastic` | `kubernetes` | `kubernetes,elastic` |
|---|---|---|---|
| ES fails | `None` | n/a | K8s result, or `None` if K8s also empty |
| K8s fails | n/a | `None` | Falls back to ES + `SOURCE_FALLBACK` |
| Both fail | `None` | `None` | `None` |
| K8s empty, ES has logs | n/a | empty | ES result + `SOURCE_FALLBACK` |
| All sources empty, fetch OK | empty | empty | empty (confirmed-absent) |

Per 1.6, total failure returns `None`, never an error string.

### 6.4 Observability

Structured `structlog` events with `event_id` bound, per 2.8:

| Event | Level | Fields |
|---|---|---|
| `log_source_selected` | info | `chain`, `attempted`, `winner` |
| `k8s_fetch_started` | info | `namespace`, `match_mode`, `match_value`, `since_hours` |
| `k8s_pods_discovered` | info | `pod_count`, `skipped_pending`, `truncated` |
| `k8s_pod_fetched` | debug | `pod_name`, `container`, `bytes`, `lines`, `matched`, `latency_ms` |
| `k8s_previous_fetched` | debug | `pod_name`, `restart_count` |
| `k8s_fetch_completed` | info | `total_matched`, `total_bytes`, `pods_ok`, `pods_failed`, `latency_ms`, `gap_count` |
| `k8s_evidence_gap` | warning | `gap_type`, gap-specific fields |
| `k8s_rbac_denied` | error | `namespace`, `verb` |
| `k8s_parse_degraded` | warning | `container`, `failure_rate` |
| `k8s_redaction_applied` | info | per-pattern counts |
| `log_snapshot_reused` | info | `event_id`, `record_count`, `captured_at` |

There is no metrics backend here, so these fields *are* the metrics -- they must
be machine-aggregatable. `gap_type` and RBAC denials should be alertable.

### 6.5 Security

Least-privilege RBAC, scoped per namespace:

```yaml
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
```

`namespaces:get` backs the pre-flight `read_namespace` check (5.2) -- it is a
`get` on the one already-known namespace, not `list`, and grants no ability to
enumerate other namespaces. `pods:list` is bound via a `RoleBinding`
(namespace-scoped), never a `ClusterRoleBinding`; the ServiceAccount cannot
list pods or namespaces outside the namespace it's bound to, matching the
real-world constraint this design was adapted to (5.2).

No `watch`, no cluster-wide binding, no other resources. A `403` is surfaced
distinctly, never retried, never swallowed into an empty result. Pod-log access
is itself auditable via `k8s_fetch_started`.

### 6.6 Data retention

`local_casesheets/` already grows without bound -- `prune_checkpoints.py` prunes
the checkpoint DB, nothing prunes casesheets. K8s snapshots are larger and
denser than the ES projection, so disk exhaustion arrives sooner. A companion
`src/tools/prune_casesheets.py` (age-based, terminal casebooks only,
`--dry-run` first) lands in Phase 8.

---

## 7. Configuration reference

| Variable | Default | Meaning |
|---|---|---|
| `LOG_SOURCE` | `kubernetes,elastic` | Ordered chain; set to `elastic` to disable the Kubernetes leg entirely |
| `LOG_SNAPSHOT_REUSE` | `true` | Reuse persisted snapshot instead of refetching |
| `ES_MOCK_FILE` | *(unset)* | **Unchanged.** Local Kibana CSV for the ES source |
| `KUBECONFIG_PATH` | *(unset)* | Remote kubeconfig; in-cluster tried first |
| `K8S_CONTEXT` | *(unset)* | Context when the kubeconfig has several |
| `K8S_DEFAULT_NAMESPACE` | *(unset)* | Namespace when caller passes none |
| `K8S_DEFAULT_APP` | `enu-biometric` | Matches today's ES filter |
| `K8S_SERVICE_MAP` | `{}` | JSON `app -> {namespace, name_contains \| label_selector}`; `name_contains` (client-side substring) is the default match mode, `label_selector` opts into server-side label matching |
| `K8S_DEFAULT_SINCE_HOURS` | `2` | Look-back window |
| `K8S_SEARCH_FIELDS` | `eventId,refId` | Identifiers to match on |
| `K8S_MAX_PODS` | `20` | Fan-out cap |
| `K8S_MAX_BYTES_PER_POD` | `10485760` | `limit_bytes` per container |
| `K8S_FETCH_CONCURRENCY` | `5` | Bounded parallel pod reads |
| `K8S_REQUEST_TIMEOUT_SECONDS` | `30` | Per API call |
| `K8S_TOTAL_FETCH_TIMEOUT_SECONDS` | `60` | Whole multi-pod operation |
| `K8S_CONTEXT_LINES_BEFORE` | `5` | Context before a match |
| `K8S_CONTEXT_LINES_AFTER` | `20` | Context after a match |
| `K8S_LEVEL_PARSE_WARN_THRESHOLD` | `0.9` | Degraded-parse gap threshold |
| `K8S_SIDECAR_DENYLIST` | `istio-proxy,linkerd-proxy` | Containers to skip |
| `K8S_REDACT_ENABLED` | `true` | Master redaction switch |
| `K8S_REDACT_EXTRA_PATTERNS` | *(unset)* | Additional regexes |
| `K8S_VERIFY_SSL` | `true` | Explicit opt-out only |
| `K8S_CA_CERT_PATH` | *(unset)* | Custom CA bundle |
| `K8S_FIXTURE_DIR` | *(unset)* | Offline K8s fixtures (additive; see 10.1) |

All added to `.env.example` with comments, per 1.4. `requirements.txt` gains
`kubernetes` (not currently installed), pinned consistently.

---

## 8. Scenario matrix

| # | Scenario | Handling |
|---|---|---|
| 1 | Single running pod | Direct read |
| 2 | Multiple replicas | Namespace list, client-side name-match (or label selector), bounded parallel fan-out, timestamp merge |
| 3 | Container restarted in place | Second call with `previous=True`, tagged |
| 4 | Pod deleted and replaced | Unrecoverable; `POD_REPLACED` gap |
| 5 | Logs rotated off node | Unrecoverable; `LOG_ROTATION` gap |
| 6 | CrashLoopBackOff | Current may be empty; `previous` holds the evidence |
| 7 | Pod `Pending` | No logs exist; skip silently |
| 8 | Pod `Failed` / `Succeeded` | **Included** -- often holds the crash evidence |
| 9 | Multi-container with sidecars | Denylist, else fetch each and tag |
| 10 | Match spec matches nothing | Empty + warning (usually wrong namespace/name_contains/label) |
| 11 | Identifier absent from all logs | Successful empty result -- **not** a failure |
| 12 | `403 Forbidden` | RBAC error; no retry; distinct log |
| 13 | `404` between list and read | Skip pod, continue, `POD_VANISHED` gap |
| 14 | Namespace missing | Caught by `read_namespace` pre-flight before listing; hard config error, no retry |
| 15 | Very large volume | Byte/pod caps; `TRUNCATED` gap |
| 16 | API server unreachable | Retry, then breaker opens |
| 17 | Clock skew across nodes | Kubelet ts for ordering; `pod_name` for attribution |
| 18 | Unparseable log format | Regex fallback; `LEVEL_PARSE_DEGRADED` gap |
| 19 | Snapshot exists from earlier attempt | Reuse; skip API entirely |
| 20 | PII in log body | Redacted before persistence |
| 21 | K8s empty, chain has ES | Fall back; `SOURCE_FALLBACK` note |
| 22 | K8s fails, chain has ES | Fall back; `SOURCE_FALLBACK` note |
| 23 | Chain exhausted | `None` |

---

## 9. Implementation phases

Each phase is self-contained, leaves the suite green, and can be completed in
one working session. **Do not start a phase before its predecessor's exit
criteria are met.**

### Status

| Phase | State | Notes |
|---|---|---|
| 0 | **Tooling ready, awaiting data** | `src/tools/es_diagnostic.py` built and tested. Must be run from a host with `ES_HOST` access against 5 real event ids; results go in Section 12. |
| 1 | **Complete** | `types.py`, `sources/base.py`, `sources/elastic.py`; `reduce_logs` routes through the seam. `fetcher.py` untouched. 16 tests. |
| 2 | **Complete** | `k8s/client.py`, `k8s/fixtures.py`, `k8s/discovery.py`; `kubernetes` added to requirements; config in `.env.example`. 26 tests. |
| 3 | **Complete** | `k8s/parser.py`, `k8s/retrieval.py`. 59 tests, including the `branch_on_error` regression guard. |
| 4 | **Complete** | `k8s/filtering.py` (identifier matching, context windows), `k8s/gaps.py` (detection + banner). 33 tests. Retrieval's boolean line filter became a stateful selector, since context windows need lookback. |
| 5 | **Complete** | `src/tools/fetch_pod_logs.py` operator CLI. 16 tests. Rotation detection was corrected during this phase: it now uses the *unfiltered* stream boundary and is suppressed when the pod is younger than the missing span (see 5.8). |
| 6 | **Complete** | `src/log_pipeline/redaction.py`, wired into retrieval after filtering and before return. 20 tests including the over-redaction guard. |
| 7 | **Complete** | `k8s/retry.py` (per-status predicate + jittered backoff), `k8s_breaker`, wall-clock fetch deadline. 26 tests. |
| 8 | **Complete** | `src/log_pipeline/snapshot.py` (atomic JSONL + meta, gap replay), `src/tools/prune_casesheets.py`. 18 tests. |
| 9 | **Complete** (default later flipped, 2026-08-12) | `k8s/source.py`, `sources/chain.py`, pipeline dispatch on `LOG_SOURCE` (shipped **default `elastic`**, since changed to **default `kubernetes,elastic`** -- see the update note at the top of this document), gap banner ahead of the trace, `pod_name` rendered, `fetch_kubernetes_logs` mock retired. 26+ tests. |
| 10 | **Complete** | `InvestigatorAgent.md` and `ReviewerAgent.md` gained binding evidence-gap guidance. 14 tests pin the prompts to the banner and gap types the code actually emits. |

Full suite: 313 passed.

Phases 2+ were built ahead of the Phase 0 decision gate at the user's explicit
direction. Nothing is wired into the packet path yet: the pipeline still uses
Elasticsearch unconditionally until Phase 9, so this remains inert in
production regardless of the Phase 0 outcome.

---

### Phase 0 -- Elasticsearch diagnostic

**Goal.** Determine whether the missing logs are an ES ingestion problem or a
query problem, per Section 2.

- **Code changes:** none.
- **Deliverable:** hit counts for query variants (a)/(b)/(c) across 5 event ids;
  `kubectl logs` comparison; node rotation settings; observed investigation lag.
- **Exit criteria:** decision-gate table resolved and recorded in this document.
- **Out of scope:** everything else.

---

### Phase 1 -- Types and the `LogSource` seam

**Goal.** Introduce the contract and route the existing ES path through it, with
zero behaviour change.

- **New:** `src/log_pipeline/types.py` (`LogRecord`, `EvidenceGap`,
  `FetchResult`, `FetchDiagnostics`, `TimeWindow`, `FetchContext`);
  `src/log_pipeline/sources/__init__.py`; `src/log_pipeline/sources/base.py`
  (`LogSource` Protocol); `src/log_pipeline/sources/elastic.py`
  (`ElasticLogSource` wrapping `fetcher.fetch_logs`).
- **Modified:** `src/log_pipeline/pipeline.py` -- `reduce_logs` obtains records
  through the source object instead of calling `fetch_logs` directly.
- **Untouched:** `src/log_pipeline/fetcher.py`. The ES fetcher, including its
  `ES_MOCK_FILE` CSV branch, is wrapped, not edited.
- **Config:** none.
- **Tests:** `tests/test_log_sources.py` -- the adapter returns the canonical
  shape; `ES_MOCK_FILE` CSV loading still works identically.
- **Exit criteria:** full existing suite green, unchanged. ES behaviour provably
  identical.
- **Out of scope:** any Kubernetes code; any change to ES query logic.

---

### Phase 2 -- Kubernetes client and pod discovery

**Goal.** Connect to the cluster and select the right pods and containers. No
log reading yet.

- **New:** `src/log_pipeline/sources/k8s/__init__.py`;
  `.../k8s/client.py` (in-cluster then kubeconfig, singleton, TLS, graceful
  unavailability); `.../k8s/discovery.py` (selector resolution, phase filter,
  container selection, sidecar denylist, `K8S_MAX_PODS`);
  `.../k8s/fixtures.py` (`K8S_FIXTURE_DIR` loader).
- **Modified:** `requirements.txt` (add `kubernetes`); `.env.example`.
- **Config:** `KUBECONFIG_PATH`, `K8S_CONTEXT`, `K8S_DEFAULT_NAMESPACE`,
  `K8S_DEFAULT_APP`, `K8S_SERVICE_MAP`, `K8S_MAX_PODS`,
  `K8S_SIDECAR_DENYLIST`, `K8S_VERIFY_SSL`, `K8S_CA_CERT_PATH`,
  `K8S_FIXTURE_DIR`.
- **Tests:** `tests/test_k8s_discovery.py` -- scenarios 7, 8, 9, 10; missing
  config does not raise.
- **Exit criteria:** discovery returns the expected pod/container list from
  fixtures; absent config degrades to "source unavailable" without an exception.
- **Out of scope:** reading logs; parsing; filtering.

---

### Phase 3 -- Log retrieval and parsing

**Goal.** Turn pod log streams into `LogRecord`s.

- **New:** `.../k8s/retrieval.py` (streaming read, `since_seconds`,
  `timestamps=True`, `limit_bytes`, `previous=True`, bounded fan-out);
  `.../k8s/parser.py` (kubelet-prefix strip, JSON, regex fallback, format
  auto-detection).
- **Config:** `K8S_DEFAULT_SINCE_HOURS`, `K8S_MAX_BYTES_PER_POD`,
  `K8S_FETCH_CONCURRENCY`, `K8S_REQUEST_TIMEOUT_SECONDS`.
- **Tests:** `tests/test_k8s_parser.py` -- JSON / plain / malformed lines, level
  extraction, auto-detection, **and the regression guard: `branch_on_error`
  fires on a K8s-sourced ERROR record**. `tests/test_k8s_retrieval.py` --
  scenarios 3, 6, restart handling, 400-on-previous swallowed.
- **Exit criteria:** fixtures parse to canonical records; the `branch_on_error`
  guard passes.
- **Out of scope:** filtering, gaps, redaction, integration.

---

### Phase 4 -- Filtering, context windows, evidence gaps

**Goal.** Reduce a pod's stream to the lines that matter, and report what is
missing.

- **New:** `.../k8s/gaps.py` (detection + banner rendering).
- **Modified:** `.../k8s/retrieval.py` (identifier filter, context windows).
- **Config:** `K8S_SEARCH_FIELDS`, `K8S_CONTEXT_LINES_BEFORE`,
  `K8S_CONTEXT_LINES_AFTER`, `K8S_LEVEL_PARSE_WARN_THRESHOLD`.
- **Tests:** `tests/test_k8s_gaps.py` -- each gap type with correct timestamps;
  overlapping context windows merge; scenarios 4, 5, 13, 15, 18.
- **Exit criteria:** banner renders correctly for rotation, replacement,
  truncation, vanished pod, degraded parse.
- **Out of scope:** redaction; wiring into the pipeline.

---

### Phase 5 -- Operator CLI (de-risking gate)

**Goal.** A standalone tool an operator runs by hand, validating every
assumption above against the real cluster before anything depends on them.

- **New:** `src/tools/fetch_pod_logs.py` -- flags `--identifier`,
  `--namespace`, `--app`, `--since-hours`, `--output`, `--show-gaps`,
  `--raw`.
- **Config:** none new.
- **Tests:** `tests/test_fetch_pod_logs_cli.py` -- argument handling, fixture
  end-to-end.
- **Exit criteria:** an operator retrieves logs for a real `eventId` from the
  real cluster, and **Open Questions 1, 2, 3 are answered from real output and
  recorded in this document.**
- **Out of scope:** any change to the packet path.

> **This is the second decision gate.** If real logs do not contain the searched
> identifier, or the format defeats the parser, fix that here -- before Phases
> 6+ build on the assumption.

---

### Phase 6 -- PII redaction

**Goal.** Ensure nothing unredacted is ever persisted.

- **New:** `src/log_pipeline/redaction.py` (patterns, allowlist, counts).
- **Modified:** `.../k8s/retrieval.py` -- redact after filtering, before return.
- **Config:** `K8S_REDACT_ENABLED`, `K8S_REDACT_EXTRA_PATTERNS`.
- **Tests:** `tests/test_redaction.py` -- each pattern; allowlisted `eventId` /
  `refId` / `srn` survive; over-redaction counters populated; scenario 20.
- **Exit criteria:** no fixture output contains an unredacted synthetic Aadhaar,
  VID, mobile, or email.
- **Out of scope:** applying redaction to the ES path (possible later).

---

### Phase 7 -- Resilience and bounds

**Goal.** Make failure modes bounded and correct.

- **New:** `.../k8s/retry.py` (status-code predicate).
- **Modified:** `src/utils/resilience.py` (add `k8s_breaker`);
  `.../k8s/retrieval.py` (wall-clock deadline between pods).
- **Config:** `K8S_TOTAL_FETCH_TIMEOUT_SECONDS`.
- **Tests:** `tests/test_k8s_retry.py` -- one case per row of the 6.1 table;
  scenarios 12, 14, 16; deadline produces partial results plus `TRUNCATED`.
- **Exit criteria:** 403/404/401 provably not retried; 429/5xx retried with
  jitter.
- **Out of scope:** integration.

---

### Phase 8 -- Snapshot persistence and reuse

**Goal.** Decouple capture time from analysis time.

- **New:** `src/log_pipeline/snapshot.py` (atomic JSONL write under `filelock`,
  meta file, reuse); `src/tools/prune_casesheets.py`.
- **Modified:** `.../k8s/source.py` -- consult the snapshot before the API.
- **Config:** `LOG_SNAPSHOT_REUSE`.
- **Tests:** `tests/test_log_snapshot.py` -- write-then-reuse skips the API; a
  failed fetch writes no snapshot; concurrent writes are safe; scenario 19.
- **Exit criteria:** a second fetch for the same `event_id` makes zero API
  calls.
- **Out of scope:** pruning policy tuning.

---

### Phase 9 -- Chain integration (goes live)

**Goal.** Wire the source chain into the pipeline behind config.

- **New:** `.../k8s/source.py` completing the `LogSource` implementation;
  `src/log_pipeline/sources/chain.py` (ordered chain, fallback semantics,
  `SOURCE_FALLBACK` note).
- **Modified:** `src/log_pipeline/pipeline.py` (chain dispatch);
  `src/log_pipeline/pipeline.py` formatters (render `pod_name`);
  `src/tools/tool_registry.py` (retire the `fetch_kubernetes_logs` mock);
  `ARCHITECTURE.md`.
- **Config:** `LOG_SOURCE`. Shipped defaulting to `elastic` (dark); changed to
  default `kubernetes,elastic` on 2026-08-12 per an explicit operator
  decision -- see the update note at the top of this document for the caveat
  that Open Questions 1-3 were still unanswered from a real cluster at that
  point.
- **Tests:** `tests/test_log_source_chain.py` -- all four chain values;
  scenarios 21, 22, 23; degradation matrix 6.3; `ES_MOCK_FILE` still drives the
  ES leg of a chain.
- **Exit criteria:** full suite green with `LOG_SOURCE` set to each of
  `elastic`, `kubernetes`, `kubernetes,elastic`, `elastic,kubernetes`.
- **Out of scope:** prompt changes.

---

### Phase 10 -- Prompt updates for evidence gaps

**Goal.** Make the agents reason correctly about incomplete evidence.

- **Modified:** `src/prompts/InvestigatorAgent.md`,
  `src/prompts/ReviewerAgent.md`.
- **Tests:** none automated -- this changes LLM behaviour.
- **Exit criteria:** human review against a sample casebook carrying a gap
  banner; the Investigator qualifies its finding and the Reviewer rejects
  conclusions that depend on a missing window.
- **Out of scope:** everything else.

---

### Dependency graph

```text
0 -> 1 -> 2 -> 3 -> 4 -> 5 (gate) -> 6 -> 7 -> 8 -> 9 -> 10
```

Strictly sequential. Phases 6, 7, and 8 are mutually independent and may be
reordered among themselves if convenient.

---

## 10. Testing strategy

### 10.1 Mocking -- existing behaviour preserved

**`ES_MOCK_FILE` is unchanged.** The local Kibana CSV path in
`fetcher.fetch_logs` keeps working exactly as today; Phase 1 wraps that function
without editing it, and a chain containing `elastic` uses the CSV when the env
var is set. No test or local workflow that relies on it changes.

The Kubernetes source needs its own offline mechanism because it cannot read a
Kibana CSV. `K8S_FIXTURE_DIR` is **strictly additive** -- a separate env var,
consumed only by the Kubernetes source, with no effect on the ES path:

```text
tests/fixtures/k8s/<namespace>/<pod-name>/
├── current.log
├── previous.log     # optional; exercises restart handling
└── meta.json        # phase, start_time, restart_count, containers
```

### 10.2 Layers

- **Unit** -- parsing, level extraction, format auto-detection, redaction
  including the over-redaction guard, gap detection per type, retry predicate
  per status code.
- **Integration (fixtures)** -- all 23 scenarios; snapshot write-then-reuse;
  each `LOG_SOURCE` chain value; fallback and degradation.
- **Contract (opt-in)** -- `pytest -m cluster`, skipped by default, run manually
  against a real namespace to verify client-library assumptions
  (`previous=True` 400 behaviour, `limit_bytes` semantics, timestamp format)
  that fixtures cannot prove.
- **Regression guard** -- the single most important test:
  **`branch_on_error` fires on a K8s-sourced ERROR record.** End-to-end proof
  that adding a source preserves Stage 2 semantics.

---

## 11. Operational runbook

| Symptom | First checks |
|---|---|
| Empty results, `pod_count: 0` | Wrong namespace, or `name_contains`/label selector doesn't match; verify `K8S_SERVICE_MAP` against `kubectl get pods -n <namespace>` (add `--show-labels` only if that app uses label mode) |
| `k8s_rbac_denied` on discovery (before any pod list) | ServiceAccount lacks `namespaces:get` on that namespace -- the pre-flight check (5.2) failed |
| Empty results, pods found | Identifier mismatch -- do the services log `eventId` or `refId`? Check `K8S_SEARCH_FIELDS` |
| Always falling back to ES | K8s returning empty; check retention vs investigation lag, then the identifier |
| `LOG_ROTATION` gaps constantly | Retention shorter than lag; reduce `since_hours`, raise kubelet `containerLogMaxSize`, or lead the chain with `elastic` |
| `k8s_rbac_denied` | ServiceAccount lacks `pods/log` get in that namespace |
| `LEVEL_PARSE_DEGRADED` | Log format unrecognised; capture a sample, extend the parser tests |
| Everything scrubbed | Over-redaction; inspect `k8s_redaction_applied` counts, allowlist the id |
| Fetch dominating packet latency | Lower `K8S_MAX_PODS` / `K8S_MAX_BYTES_PER_POD` / `K8S_TOTAL_FETCH_TIMEOUT_SECONDS` |

---

## 12. Phase 0 findings

**Status: NOT YET RUN.** The diagnostic requires network access to `ES_HOST`
and a kubeconfig with cluster access; neither is reachable from the development
machine. Fill this section in after running the tool, then resolve the decision
gate in Section 2.

**Operational note (2026-08-12):** `LOG_SOURCE` now defaults to
`kubernetes,elastic` (an explicit operator decision -- see the update note at
the top of this document) even though this section is still empty and Phase
5's Open Questions 1-3 have not been answered against a real cluster. Today
that is low-risk everywhere the Kubernetes leg is unconfigured: discovery
fails fast on a missing namespace and falls straight through to
Elasticsearch. **The risk activates the moment `KUBECONFIG_PATH` and
`K8S_DEFAULT_NAMESPACE` are filled in** -- from that point on, every single
packet tries Kubernetes first, and if the identifier, namespace, or selector
turn out to be wrong, every packet pays up to `K8S_TOTAL_FETCH_TIMEOUT_SECONDS`
(default 60s) before falling back to Elasticsearch instead of going straight
there. Run `src/tools/fetch_pod_logs.py` against the real cluster (Phase 5)
*before* filling in cluster credentials in any shared environment.

### 12.1 Query variant hit counts

| eventId | A (prod) | B (no app filter) | C (multi_match) | D (widest) | Verdict |
|---|---|---|---|---|---|
| *(pending)* | | | | | |
| *(pending)* | | | | | |
| *(pending)* | | | | | |
| *(pending)* | | | | | |
| *(pending)* | | | | | |

### 12.2 Services logging these event ids

From the diagnostic's `by_service` aggregation. **If any service other than
`enu-biometric` appears, the hardcoded filter in `fetcher.py:110-112` is
discarding real evidence and that is the primary finding.**

| Service (`application_name`) | Doc count | Excluded by current filter? |
|---|---|---|
| *(pending)* | | |

### 12.3 Pod-log comparison (step 3)

| eventId | ES hits (variant D) | Pod-log occurrences | Delta |
|---|---|---|---|
| *(pending)* | | | |

### 12.4 Retention and lag

| Measurement | Value | Answers |
|---|---|---|
| `containerLogMaxSize` | *(pending)* | Open Question 3 |
| `containerLogMaxFiles` | *(pending)* | Open Question 3 |
| Effective retention for the busiest service | *(pending)* | Open Question 3 |
| Investigation lag, p50 | *(pending)* | Open Question 5 |
| Investigation lag, p95 | *(pending)* | Open Question 5 |

### 12.5 Gate decision

- **Outcome selected:** *(pending -- one of the three rows in Section 2)*
- **Decided by:** *(pending)*
- **Date:** *(pending)*
- **Rationale:** *(pending)*

If the outcome is "fix the ES query", record the fix here and re-run the
diagnostic before reconsidering Phase 1.

---

## 13. Open questions

1. **Which identifier do the services actually log?** ES searches `eventId`; if
   pods log `refId`, the filter must match that. Default searches both; confirm
   in Phase 5.
2. **Is `enu-biometric` one deployment, or does the service vary by
   `flowMetaData.stage`?** If it varies, `K8S_SERVICE_MAP` must be keyed by
   stage. Confirm in Phase 5.
3. **What is the real `containerLogMaxSize` / `containerLogMaxFiles`?**
   Determines whether the K8s leg is worth leading the chain with. Confirm in
   Phase 0 and 5.
4. **Where does agentic-resident-crm run?** In-cluster deployment removes kubeconfig
   distribution entirely and is strongly preferable.
5. **What is the p50/p95 lag between rejection and investigation?** Determines
   whether snapshot-first is an optimisation or the load-bearing mechanism.

---

## 14. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Log parsing misses `ERROR` | Stuck packets misclassified as clean rejections | Real-sample tests; `LEVEL_PARSE_DEGRADED`; Phase 5 validates before integration |
| Retention shorter than lag | K8s leg returns nothing on delayed paths | Snapshot-first (4.3); `elastic` in the chain |
| Client-side filtering cost | Packet latency budget consumed | Streaming, byte caps, bounded concurrency, wall-clock deadline |
| Over-redaction | Evidence destroyed | Allowlist internal ids; log redaction counts |
| Credential blast radius | Pod-log read across namespaces | Least-privilege RBAC; kubeconfig outside repo |
| Snapshot disk growth | Disk exhaustion | `prune_casesheets.py` in Phase 8 |
| Scope creep from Phase 0 | Weeks on an unnecessary subsystem | Hard decision gate before Phase 1 |

---

## Appendix A: Rejected alternatives

**Merging and de-duplicating both sources into one stream.** Rejected. The
requirement is fallback, not union: Kubernetes supplements Elasticsearch rather
than combining with it. Beyond that, dedup on `(timestamp, message)` is fragile
across sources with different clock sources and whitespace normalisation, and a
dedup bug silently deletes evidence -- exactly the failure this project exists
to eliminate. One source wins per fetch, and `source` records which.

**Replacing Elasticsearch.** Rejected explicitly. Kubelet retention makes
Kubernetes unusable as a system of record; it covers the recent window only.

**Exposing the fetcher as an LLM-callable tool.** Rejected. The Investigator
runs with `tools=[]` by design; remediation 1.16 rewrote its prompt precisely
because it instructed the model to call tools it did not have.

**Changing or replacing the `ES_MOCK_FILE` CSV mock.** Rejected. The local CSV
workflow is preserved unchanged; the Kubernetes fixture directory is a separate,
additive mechanism.

**A log-shipping sidecar or collector.** Out of scope. That is what the ES
ingestion pipeline is supposed to be; fixing it is a different and probably
cheaper project -- which is the point of the Phase 0 gate.
