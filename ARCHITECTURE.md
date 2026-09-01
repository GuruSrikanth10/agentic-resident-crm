# Agentic Resident CRM: Deep Dive and Architecture

## Overview
**Agentic Resident CRM** is an AI-driven, self-learning service built to ingest, analyze, and resolve rejected biometric packets within the UIDAI ecosystem. 

When a packet fails enrollment or deduplication (e.g., due to a `RESIDENT_MAN_DEDUP_REJECT_TD` error), the system automatically spins up a suite of LangGraph-powered LLM agents. These agents investigate the error against a rules database, validate their findings, permanently learn from their mistakes, and format the output into structured JSON casebooks.

---

## 1. High-Level Workflow

Ingestion is split into two independently-scalable stages -- fetching
Kubernetes/Elasticsearch logs (bounded, fast I/O) and running the LLM
investigation (unbounded, slow) -- so a backlog in the LLM stage can never
stall log collection and let short-retention Kubernetes logs rotate away
before a packet is even fetched. See section 3.11 for the full design.

### 1.1 Two-Stage Ingestion

```mermaid
sequenceDiagram
    participant K1 as Kafka: rejections
    participant FC as fast_consumer.py
    participant API as FastAPI (/fetch-logs)
    participant FS as CasebookStorage
    participant K2 as Kafka: analysis-queue
    participant SC as slow_consumer.py
    participant API2 as FastAPI (/analyze-rejection)

    K1->>FC: Push Rejected Packet JSON
    FC->>FC: Filter packetStatus == "REJECTED"
    FC->>API: HTTP POST /fetch-logs
    API->>API: Fetch Kubernetes + Elasticsearch logs
    API->>FS: Persist fetched_logs.txt + status.json (LOGS_FETCHED)
    API->>K2: Republish the payload
    API-->>FC: 200 OK

    K2->>SC: Push the same payload
    SC->>API2: HTTP POST /analyze-rejection
    Note over API2,FS: See section 1.2 -- runs the LangGraph orchestration,<br/>reading logs already persisted by /fetch-logs
    API2->>FS: Save terminal casebook.json + status.json
```

### 1.2 Analysis Workflow (inside POST /analyze-rejection)

```mermaid
sequenceDiagram
    participant SC as Slow Consumer
    participant API as FastAPI (/analyze-rejection)
    participant M as RejectionManagerAgent
    participant RB as Runbook Store
    participant LF as LogFilterAgent
    participant I as InvestigatorAgent
    participant R as ReviewerAgent
    participant S as SynthesisAgent
    participant T as Tool Registry
    participant FS as CasebookStorage

    SC->>API: HTTP POST /analyze-rejection
    
    API->>API: Validate via Pydantic (MessagePayload)
    API->>M: Invoke Orchestrator

    M->>FS: fetch_logs_node reads fetched_logs.txt
    Note over M,FS: Cache hit (the normal path): no live fetch.<br/>Cache miss falls back to a live fetch inline --<br/>e.g. direct /process-rejection or local_run.py use.
    
    M->>RB: Lookup runbook by (reason_code, enrolment_type)
    alt Runbook Hit (RUNBOOK_MODE=serve)
        RB-->>M: Return pre-built resolution
        M-->>API: Short-circuit with runbook resolution
    else No Runbook or RUNBOOK_MODE=off/shadow
        M->>T: lookup_rule_by_reason_code (pre-fetched in Python)
        T-->>M: Rule row(s), filtered by enrolmentType
        
        opt ENABLE_LOG_FILTER_AGENT=true
            M->>LF: Dispatch raw sliding-window logs
            LF-->>M: Cleaned logs (cross-packet noise removed)
        end
        
        M->>I: Dispatch payload + logs + rule for Investigation
        I-->>M: Return detailed technical findings
        
        M->>R: Dispatch findings for Validation
        R->>R: Validate logic and accuracy
        
        alt Mistake Detected
            R->>T: add_learning_rule()
            T->>FS: Stages proposal to src/prompts/pending_rules.jsonl
            R-->>M: Return corrective feedback (loop back to I)
        else Validated
            R-->>M: Reply "APPROVED"
        end
        
        M->>S: Dispatch for Synthesis
        S->>T: queue_for_replay (only if Action is REPLAY/QC_REPLAY)
        S-->>M: Return strict analytical JSON
    end
    
    M-->>API: Pass analytical JSON to backend
    API->>API: Extract metadata & construct hierarchical Casebook
    API->>FS: Save to local_casesheets/casebook_{eventId}/casebook.json
```

### 1.3 Complete Flow -- Every Path

Sections 1.1 and 1.2 show the happy path. This section is the normative
reference: every branch, degradation, and duplicate-arrival case the code
actually implements. Where a diagram and the prose disagree, the code wins --
each node below is annotated with the module that owns it.

Five views, because one diagram covering all of it would be unreadable:

| View | Answers |
| --- | --- |
| 1.3.1 Master flow | What happens to a packet from Kafka to terminal casebook |
| 1.3.2 Log acquisition | Where logs come from, and what happens when they do not arrive |
| 1.3.3 LangGraph state machine | Runbook modes, the retry loop, synthesis repair, abstention |
| 1.3.4 Idempotency | What happens when the same packet arrives twice |
| 1.3.5 Offset and failure matrix | Which failures commit, which redeliver, which stall |

#### 1.3.1 Master flow

```mermaid
flowchart TD
    classDef term fill:#7f1d1d,stroke:#450a0a,color:#ffffff
    classDef skip fill:#1e40af,stroke:#1e3a8a,color:#ffffff
    classDef good fill:#14532d,stroke:#052e16,color:#ffffff
    classDef store fill:#334155,stroke:#0f172a,color:#ffffff
    classDef dlq fill:#78350f,stroke:#451a03,color:#ffffff

    K1(["Kafka: rejections topic"])

    subgraph FAST["fast_consumer.py -- CONSUMER_ROLE=fast, kafkaConsumer.py"]
        K1 --> POLL["poll, enable_auto_commit=false"]
        POLL --> SEM{"worker slot free?<br/>_queue_semaphore"}
        SEM -->|"no: block BEFORE parsing"| POLL
        SEM -->|yes| TRK["OffsetTracker.dispatched tp, offset"]
        TRK --> VAL{"decode utf-8, json.loads,<br/>MessagePayload validate"}
        VAL -->|"invalid: poison pill"| PP["publish_to_dlq raw string"]
        VAL -->|valid| RJ{"packetStatus == REJECTED?"}
        RJ -->|no| SK1["skip: non-rejected packet"]
        RJ -->|yes| D1{"terminal casebook exists?<br/>storage.exists terminal_only"}
        D1 -->|"yes: DUPLICATE"| SK2["skip: already processed"]
        D1 -->|no| SUB["worker pool submit"]
    end

    PP --> RC1["record_completion -- offset commits"]
    SK1 --> RC1
    SK2 --> RC1

    SUB --> POST1["POST /fetch-logs<br/>FAST_CONSUMER_TIMEOUT_SECONDS=90"]

    subgraph FL["POST /fetch-logs -- routes.py, sync, no LLM"]
        POST1 --> AUTH1{"X-API-Key valid?<br/>hmac.compare_digest"}
        AUTH1 -->|no| E403["403"]
        AUTH1 -->|yes| RL1{"rate limit<br/>exempt CIDR or under RATE_LIMIT?"}
        RL1 -->|no| E429["429"]
        RL1 -->|yes| T1{"terminal_status in<br/>TERMINAL_STATUSES?"}
        T1 -->|"yes: DUPLICATE"| AP1["200 already_processed"]
        T1 -->|no| ART{"fetched_logs.txt exists?"}
        ART -->|"yes: reuse artifact"| STAT
        ART -->|no| FETCH["fetch_and_persist_logs<br/>see 1.3.2"]
        FETCH --> FOK{"logs returned?"}
        FOK -->|"yes or 'Log fetching disabled.'"| SAVEA["save_artifact fetched_logs.txt"]
        FOK -->|"None: breaker open or pipeline raised"| NOSAVE["persist nothing<br/>slow side will retry live"]
        SAVEA --> STAT
        NOSAVE --> STAT
        STAT{"status.json is<br/>absent or LOGS_FETCHED?"}
        STAT -->|yes| WSTAT["write status.json = LOGS_FETCHED"]
        STAT -->|"no: IN_PROGRESS or terminal"| KEEP["leave status.json untouched<br/>must not mask the IN_PROGRESS guard"]
        WSTAT --> PUB
        KEEP --> PUB
        PUB{"publish_to_analysis_queue"}
        PUB -->|raises| E500["500 -- offset NOT committed"]
        PUB -->|ok| Q200["200 queued_for_analysis"]
    end

    E403 --> DLQ1
    E429 --> DLQ1
    E500 --> DLQ1["_dlq_and_abandon:<br/>publish DLQ, then release the floor"]
    AP1 --> RC1
    Q200 --> RC1

    Q200 --> K2(["Kafka: packet-analysis-queue"])

    subgraph SLOW["slow_consumer.py -- CONSUMER_ROLE=slow, same module"]
        K2 --> POLL2["identical guards:<br/>poison pill, REJECTED, terminal dedupe"]
        POLL2 --> POST2["POST /analyze-rejection<br/>PACKET_TIMEOUT_SECONDS=500"]
    end

    subgraph AN["POST /analyze-rejection -- _investigate_packet, async"]
        POST2 --> T2{"casebook.json terminal?"}
        T2 -->|"yes: DUPLICATE"| AP2["already_processed"]
        T2 -->|no| CKPT["agent.get_state thread_id=eventId<br/>has_active_checkpoint = bool state.next"]
        CKPT --> IP{"status.json == IN_PROGRESS?"}
        IP -->|no| STUB
        IP -->|yes| STALE{"age exceeds MAX_IN_PROGRESS_AGE_SECONDS<br/>default 1800s?"}
        STALE -->|"no + active checkpoint"| RES["already_processing_resumed<br/>invoke None, resumes graph"]
        STALE -->|"no + no checkpoint"| BUSY["already_processing -- skip"]
        STALE -->|"yes + no checkpoint"| STUB["write status.json = IN_PROGRESS"]
        STALE -->|"yes + active checkpoint"| RES
        STUB --> INV["run_in_executor _agent_invoke_executor<br/>asyncio.wait_for AGENT_INVOKE_TIMEOUT_SECONDS"]
        RES --> INV
        INV --> GRAPH["LangGraph -- see 1.3.3"]
        GRAPH --> OUT{"outcome"}
        OUT -->|"asyncio.TimeoutError"| FT["save_terminal FAILED_TIMEOUT"]
        OUT -->|"unhandled exception"| DQ["publish_to_dlq +<br/>save_terminal DLQ"]
        OUT -->|"returned a state"| PARSE{"parse_synthesis<br/>against the contract"}
        PARSE -->|invalid| FSP["FAILED_SYNTHESIS_PARSE<br/>evidence kept, verdict replaced"]
        PARSE -->|valid| LOGSZ{"raw logs size"}
        LOGSZ -->|"none / 'Log fetching disabled.'"| BUILD
        LOGSZ -->|"5000 chars or fewer"| BUILD["build casebook"]
        LOGSZ -->|"over 5000 chars"| S3{"upload_logs_to_s3"}
        S3 -->|"URL"| BUILD
        S3 -->|"None: no bucket or failed"| TRUNC["truncate to 5000 + notice"] --> BUILD
        FSP --> BUILD
        BUILD --> LATE{"terminal_status in<br/>PROTECTED_TERMINAL_STATUSES<br/>FAILED_TIMEOUT, DLQ?"}
        LATE -->|"yes: another actor won"| DISC["discard late result"]
        LATE -->|no| SAVE["save_terminal<br/>casebook.json + status.json together"]
    end

    AP2 --> RC2["record_completion -- offset commits"]
    FT --> RC2
    DQ --> RC2
    RES --> RC2
    BUSY --> RC2
    DISC --> RC2
    SAVE --> RC2

    class PP,DLQ1,DQ,FT dlq
    class SK1,SK2,AP1,AP2,BUSY,DISC,KEEP,NOSAVE skip
    class SAVE,Q200,RC1,RC2 good
    class SAVEA,WSTAT,STUB store
    class E403,E429,E500,FSP term
```

#### 1.3.2 Log acquisition -- source chain and every failure mode

`LOG_SOURCE` is an ordered chain, default `kubernetes,elastic`. Fallback fires
when a source fails **or** returns zero records -- both mean "we did not get
logs here". Sources are never merged; one wins per fetch.

```mermaid
flowchart TD
    classDef gap fill:#78350f,stroke:#451a03,color:#ffffff
    classDef bad fill:#7f1d1d,stroke:#450a0a,color:#ffffff
    classDef good fill:#14532d,stroke:#052e16,color:#ffffff

    A["fetch_and_persist_logs<br/>tool_registry.py"] --> EN{"ENABLE_LOG_FETCHING?"}
    EN -->|false| DIS["logs = 'Log fetching disabled.'<br/>persisted verbatim"]
    EN -->|true| CHAIN["reduce_logs, then fetch_with_fallback<br/>over the LOG_SOURCE chain"]

    subgraph K8S["Kubernetes source -- sources/k8s/"]
        CHAIN --> BRK{"k8s_breaker open?<br/>3 failures / 60s"}
        BRK -->|"yes: fail fast"| KFAIL["FetchResult.failure"]
        BRK -->|no| SNAP{"LOG_SNAPSHOT_REUSE<br/>and snapshot exists?"}
        SNAP -->|"yes: replay capture"| KOK["records from raw_logs_k8s.jsonl<br/>deterministic, free, no API call"]
        SNAP -->|no| DISC1{"namespace resolved?<br/>K8S_DEFAULT_NAMESPACE / K8S_SERVICE_MAP"}
        DISC1 -->|no| KFAIL
        DISC1 -->|yes| CLI{"client available?<br/>in-cluster, then kubeconfig"}
        CLI -->|"no: unconfigured"| KFAIL
        CLI -->|yes| NSV{"read_namespace ok?"}
        NSV -->|"403 RBAC / 404"| KFAIL
        NSV -->|yes| LIST["list_namespaced_pod<br/>label or name_contains match"]
        LIST --> PHASE["skip Pending only<br/>Failed and Succeeded ARE read"]
        PHASE --> CAP{"pods over K8S_MAX_PODS<br/>default 20?"}
        CAP -->|yes| GAPT["gap: TRUNCATED<br/>newest-started kept"]
        CAP -->|no| TGT
        GAPT --> TGT{"any targets?"}
        TGT -->|"no: looked, found nothing"| KEMPTY["ok=true, records=[]"]
        TGT -->|yes| READ["read_all: bounded fan-out<br/>K8S_FETCH_CONCURRENCY=5"]
        READ --> PER["per pod: previous instance if restarted,<br/>then current; streamed, filtered client-side"]
        PER --> PERR{"per-pod outcome"}
        PERR -->|404| GV["gap: POD_VANISHED"]
        PERR -->|403| RBAC["logged distinctly, pod failed"]
        PERR -->|"429 / 5xx"| RETRY["retry with jitter<br/>never retries 400/401/403/404/410"]
        PERR -->|"byte cap K8S_MAX_BYTES_PER_POD"| GT2["gap: TRUNCATED"]
        PERR -->|ok| COLLECT
        RETRY --> COLLECT
        GV --> COLLECT
        RBAC --> COLLECT
        GT2 --> COLLECT["collect records"]
        READ --> DEAD{"K8S_TOTAL_FETCH_TIMEOUT_SECONDS<br/>expired?"}
        DEAD -->|yes| GT3["gap: TRUNCATED, N pods unread<br/>pool shutdown wait=false"]
        GT3 --> COLLECT
        COLLECT --> ALLF{"every queried pod failed?"}
        ALLF -->|"yes: COULD NOT LOOK"| KFAIL
        ALLF -->|no| GAPS["detect LOG_ROTATION,<br/>POD_REPLACED, LEVEL_PARSE_DEGRADED"]
        GAPS --> RED["redact PII<br/>allowlist = eventId, refId"]
        RED --> SS["snapshot.save if records"]
        SS --> KOK
    end

    KFAIL --> FB{"another source in the chain?"}
    KEMPTY --> FB
    FB -->|"yes: fall through"| ES
    FB -->|"no: last source raises through"| NONE

    subgraph ELASTIC["Elasticsearch source -- fetcher.py"]
        ES{"ES_MOCK_FILE set?"}
        ES -->|yes| MOCK["read CSV fixture"]
        ES -->|no| HOST{"ES_HOST set?"}
        HOST -->|no| MOCK2["single synthetic MOCK record"]
        HOST -->|yes| EBRK{"es_breaker open?"}
        EBRK -->|yes| EFAIL["raises through"]
        EBRK -->|no| QUERY["paginated search_after<br/>capped at LOG_MAX_DOCUMENTS=50000"]
        QUERY --> EOK["records"]
    end

    MOCK --> WIN
    MOCK2 --> WIN
    EOK --> WIN["winner selected<br/>gap: SOURCE_FALLBACK if anything was skipped"]
    EFAIL --> NONE
    KOK --> WIN

    NONE["no source returned logs"] --> EMPTY["'No logs found for ID: X'<br/>+ gap banner"]

    WIN --> RED2["redact before ANY persistence"]
    RED2 --> RAW["save raw_logs.txt"]
    RAW --> SIZE{"record count under 50?"}
    SIZE -->|yes| DIRECT["emit full trace verbatim"]
    SIZE -->|no| BR{"any level == ERROR?"}
    BR -->|"yes: stuck path"| ERRP["ERROR + 200 lines before<br/>+ 200 lines after"]
    BR -->|"no: approve/reject path"| CLU["Drain3 clustering<br/>+ evidence guardrails"]
    DIRECT --> RDX
    ERRP --> RDX
    CLU --> RDX["save reduced_logs.txt<br/>banner FIRST if gaps exist"]
    RDX --> PERSIST["save_artifact fetched_logs.txt"]
    EMPTY --> PERSIST
    DIS --> PERSIST

    class KFAIL,EFAIL,NONE bad
    class GAPT,GV,GT2,GT3,GAPS,EMPTY gap
    class KOK,EOK,PERSIST,SS good
```

> **Why an empty result is not a failure.** `FetchResult.ok` separates
> *could-not-look* from *looked-and-found-nothing*. Collapsing the two would let
> the Investigator conclude "no errors occurred" when the truth is "we could not
> read the logs". Every gap above is rendered into a banner placed **before** the
> trace, and `apply_confidence_policy` caps confidence at
> `SYNTHESIS_GAP_CONFIDENCE_CEILING` (0.6) whenever that banner is present.

#### 1.3.3 LangGraph state machine

Nodes are compiled once and cached (`get_agent`); the checkpointer is keyed on
`thread_id = eventId`, which is what makes a resume possible.

```mermaid
stateDiagram-v2
    [*] --> fetch_logs

    fetch_logs: fetch_logs_node
    note right of fetch_logs
        Cache-first: fetched_logs.txt from /fetch-logs.
        Artifact PRESENT - whatever its content, including
        the disabled/no-logs sentinels - means a fetch was
        already attempted. Absent falls back to a live
        fetch, which is what keeps /process-rejection and
        local_run.py working unchanged.
    end note

    fetch_logs --> runbook_lookup

    state runbook_lookup {
        [*] --> mode_check
        mode_check: RUNBOOK_MODE?
        mode_check --> agents_off: off (default) - counted as no lookup
        mode_check --> resolve: shadow or serve
        resolve --> miss_norc: no errorReasonCode
        resolve --> miss_none: no runbook file
        resolve --> miss_fp: rule_fingerprint mismatch (DB rule changed)
        resolve --> miss_err: any exception - never propagates
        resolve --> shadow_path: mode=shadow OR code not in RUNBOOK_SERVE_ALLOWLIST
        resolve --> hit: mode=serve AND code allowlisted
    }

    hit --> [*]: SHORT-CIRCUIT - zero LLM calls, synthesis = runbook resolution
    miss_norc --> route
    miss_none --> route
    miss_fp --> route
    miss_err --> route
    agents_off --> route
    shadow_path --> route: runbook answer carried as shadow_runbook_resolution

    route: check_runbook_hit
    route --> filter_logs: ENABLE_LOG_FILTER_AGENT=true
    route --> investigate: otherwise

    filter_logs --> investigate: skipped when logs<br/>empty or disabled

    investigate: investigator_node
    note left of investigate
        First pass sends a PROJECTED payload, the logs, and
        the DB rule. Retry sends prior analysis + reviewer
        feedback + the LOGS AGAIN - the reviewer's most
        common rejection is unsupported citations, so the
        retry must keep the evidence.
    end note

    investigate --> review
    review: reviewer_node - retry_count += 1

    state review_decision <<choice>>
    review --> review_decision
    review_decision --> synthesize: verdict starts with APPROVED
    review_decision --> escalate: retry_count reached MAX_INVESTIGATION_RETRIES, default 3
    review_decision --> investigate: rejected - loop back

    note right of review
        A rejection may also call add_learning_rule, which
        is validated (injection markers, identifiers, length)
        and queued to pending_rules.jsonl. Nothing reaches
        the Investigator prompt without a human typing
        "promote".
    end note

    state synthesize {
        [*] --> parse1
        parse1: parse_synthesis
        parse1 --> policy: valid
        parse1 --> repair: invalid - one repair attempt
        repair --> parse2
        parse2 --> policy: valid
        parse2 --> unrepairable: still invalid
        policy: apply_confidence_policy
        policy --> capped: gap banner present - cap at GAP_CONFIDENCE_CEILING
        policy --> abstain: confidence below SYNTHESIS_CONFIDENCE_THRESHOLD, 0 disables this
        policy --> ok
        capped --> ok
        abstain --> ok: action forced to MANUAL_REVIEW
        unrepairable --> ok: action forced to MANUAL_REVIEW
    }

    escalate: escalate_node - MANUAL_REVIEW plus full transcript
    synthesize --> [*]
    escalate --> [*]

    note left of synthesize
        Shadow comparison runs here: the runbook's action is
        compared against the agents' and RETURNED as
        shadow_comparison, so accuracy_report --shadow can
        answer "would this runbook have been right?" -
        the gate for promoting it to serve.
    end note
```

Every LLM node is wrapped in `@llm_breaker` over `@retry_transient`: three
tenacity attempts on transient provider errors, then the breaker opens after
three consecutive failures and fails fast for 60s.

#### 1.3.4 Idempotency -- the same packet arriving twice

There are **nine** distinct duplicate-arrival guards. They exist at different
layers because a duplicate can enter at any of them.

```mermaid
flowchart TD
    classDef skip fill:#1e40af,stroke:#1e3a8a,color:#ffffff
    classDef work fill:#14532d,stroke:#052e16,color:#ffffff

    DUP(["Same eventId arrives again"]) --> WHERE{"where?"}

    WHERE -->|"rejections topic redelivery"| G1{"terminal casebook?"}
    G1 -->|yes| S1["G1: consumer skips,<br/>commits offset"]
    G1 -->|no| G2

    WHERE -->|"POST /fetch-logs"| G2{"terminal_status set?"}
    G2 -->|yes| S2["G2: 200 already_processed"]
    G2 -->|no| G3{"fetched_logs.txt exists?"}
    G3 -->|yes| S3["G3: reuse artifact,<br/>no second cluster hit"]
    G3 -->|no| W1["fetch"]
    S3 --> G4
    W1 --> G4{"status.json already<br/>IN_PROGRESS or terminal?"}
    G4 -->|yes| S4["G4: do NOT rewrite to LOGS_FETCHED<br/>- would mask the IN_PROGRESS guard"]
    G4 -->|no| W2["write LOGS_FETCHED"]

    WHERE -->|"POST /analyze-rejection"| G5{"casebook.json terminal?"}
    G5 -->|yes| S5["G5: already_processed"]
    G5 -->|no| G6{"status.json IN_PROGRESS?"}
    G6 -->|no| W3["proceed: fresh investigation"]
    G6 -->|yes| G7{"active checkpoint?<br/>state.next non-empty"}
    G7 -->|yes| S6["G6: already_processing_resumed<br/>- invoke None, continues mid-graph"]
    G7 -->|"no + not stale"| S7["G7: already_processing<br/>- in flight between checkpoint writes"]
    G7 -->|"no + stale over 1800s"| W4["G8: reprocess from scratch,<br/>retry_count reset to 0"]

    WHERE -->|"slow run finishes AFTER<br/>a timeout/DLQ was recorded"| G9{"terminal_status in<br/>FAILED_TIMEOUT, DLQ?"}
    G9 -->|yes| S8["G9: discard late result<br/>- checks BOTH files, closing F4"]
    G9 -->|no| W5["save_terminal"]

    class S1,S2,S3,S4,S5,S6,S7,S8 skip
    class W1,W2,W3,W4,W5 work
```

| Guard | Location | Trigger | Result |
| --- | --- | --- | --- |
| G1 | `kafkaConsumer._handle_one_message` | terminal casebook exists | skip, commit offset |
| G2 | `routes.fetch_logs` | `terminal_status` in `TERMINAL_STATUSES` | `already_processed` |
| G3 | `routes.fetch_logs` | `fetched_logs.txt` present | reuse, no refetch |
| G4 | `routes.fetch_logs` | status is `IN_PROGRESS`/terminal | leave status alone |
| G5 | `_investigate_packet` | `casebook.json` terminal | `already_processed` |
| G6 | `_investigate_packet` | `IN_PROGRESS` + active checkpoint | `already_processing_resumed` |
| G7 | `_investigate_packet` | `IN_PROGRESS`, fresh, no checkpoint | `already_processing` |
| G8 | `_investigate_packet` | `IN_PROGRESS` stale, no checkpoint | reprocess, `retry_count=0` |
| G9 | `_investigate_packet` | `PROTECTED_TERMINAL_STATUSES` set | discard late result |

> **`retry_count` is reset explicitly on G8.** `thread_id` is the `eventId`, so a
> redelivered "fresh" invocation can otherwise resume a persisted checkpoint whose
> `retry_count` is already at `MAX_INVESTIGATION_RETRIES` and escalate instantly
> without doing any work.

> **Scope limit.** G3/G4 rely on `filelock` (local backend) or last-writer-wins
> (S3). Neither coordinates across pods, which is why `config_validator` refuses
> to boot with `API_REPLICA_COUNT > 1` unless both `CASEBOOK_STORAGE_BACKEND=s3`
> and `CHECKPOINT_BACKEND=postgres` are set.

#### 1.3.5 Offset and failure matrix

Offsets are never committed per message. `OffsetTracker` commits only the
**low-water mark**: the highest offset below which every dispatched message has
completed. Offsets 10, 11, 12 dispatched together with 12 finishing first
commits nothing until 10 and 11 land.

```mermaid
flowchart LR
    classDef good fill:#14532d,stroke:#052e16,color:#ffffff
    classDef warn fill:#78350f,stroke:#451a03,color:#ffffff
    classDef bad fill:#7f1d1d,stroke:#450a0a,color:#ffffff

    F{"failure"} --> A["poison pill"] --> C1["DLQ + completed, then COMMIT"]
    F --> B["non-REJECTED / duplicate"] --> C1
    F --> C["/fetch-logs returns 2xx"] --> C1
    F --> D["HTTP timeout"] --> D1["FAILED_TIMEOUT written best-effort,<br/>then DLQ, then COMMIT"]
    F --> E["any other forward error"] --> E1["DLQ, then abandoned,<br/>so the floor advances"]
    F --> G["DLQ itself unreachable"] --> G1["offset HELD uncommitted,<br/>stalls then redelivers"]
    F --> H["consumer SIGTERM"] --> H1["drain SHUTDOWN_DRAIN_SECONDS,<br/>commit what finished, rest redelivers"]
    F --> I["API SIGTERM mid-investigation"] --> I1["FAILED_SHUTDOWN written<br/>so nothing strands at IN_PROGRESS"]
    F --> J["partition revoked"] --> J1["commit safe floor, then forget<br/>- never commit for a partition we lost"]

    class C1,D1,E1,H1,I1,J1 good
    class G1 bad
```

The one deliberate stall is `G1`. If the DLQ publish fails there is nowhere left
to escalate to, so the offset stays dispatched: commits freeze for that
partition rather than advancing past a message that would then exist nowhere.
It self-heals on redelivery once the broker is reachable.

---

## 2. Directory Structure

The repository follows standard Python backend architecture for modularity and scalability:

```text
agentic-resident-crm/
├── .agents/
│   └── AGENTS.md                   # Agentic configurations and behavioral rules
├── agent_policy_context.md         # Foundational business logic & rules mapping for AI agents
├── start.py                        # Process supervisor: spawns main_api.py + fast_consumer.py + slow_consumer.py
├── local_run.py                    # CLI: POST a local packet JSON to the running API
├── test_payload.py                 # Static Pydantic validation smoke test
├── rules.csv                       # Rules export used by check_drift.py
├── reason_codes.csv                # 760 reject codes -> description, category, failure class
│                                   #   (generated from the BusinessReasonCode Java source by
│                                   #    src/tools/parse_reason_codes.py; the .txt source is an
│                                   #    input, not a runtime dependency, and is not kept here)
├── tests/
│   ├── test_resilience.py          # Resilience/idempotency/DLQ regression tests
│   ├── test_phase0_fixes.py        # Phase 0 correctness regression tests
│   ├── test_phase1_fixes.py        # Phase 1 reliability regression tests
│   ├── test_phase2_fixes.py        # Phase 2 optimization regression tests
│   ├── test_runbooks.py            # Runbook store, validator, and serving tests
│   ├── test_log_sources.py         # LogSource Protocol and ElasticLogSource tests
│   ├── test_log_source_chain.py    # Fallback chain (LOG_SOURCE) tests
│   ├── test_log_snapshot.py        # Evidence snapshot persistence tests
│   ├── test_redaction.py           # PII redaction tests
│   ├── test_prompt_gap_guidance.py # Prompt evidence-gap banner tests
│   ├── test_es_diagnostic.py       # ES diagnostic tool tests
│   ├── test_fetch_pod_logs_cli.py  # fetch_pod_logs CLI tests
│   ├── test_k8s_discovery.py       # Kubernetes pod/namespace discovery tests
│   ├── test_k8s_multi_service.py   # Multi-service fan-out, dedupe, partial failure
│   ├── test_k8s_gaps.py            # Kubernetes evidence gap detection tests
│   ├── test_k8s_parser.py          # Kubernetes log line parser tests
│   ├── test_k8s_retrieval.py       # Kubernetes pod log retrieval tests
│   ├── test_k8s_retry.py           # Kubernetes HTTP retry logic tests
│   ├── test_dlt_stacktrace.py      # DLT lane: headers, `Caused by:` parsing, fingerprint
│   ├── test_dlt_classify.py        # DLT lane: failure taxonomy A/B/C/U
│   ├── test_dlt_fetch.py           # DLT lane: /fetch-dlt-logs, log window, evidence
│   ├── test_dlt_corroborate.py     # DLT lane: trace-vs-log verdicts
│   ├── test_dlt_analysis.py        # DLT lane: /analyze-dlt with a mocked LLM
│   ├── test_dlt_analysis_replay.py # DLT lane: auto-replay and precheck, end to end
│   ├── test_dlt_{payload,reuse,report,sample,reason_codes,...}.py  # remaining phases
│   │                               #   -- Replay precheck (section 4.4.1) --
│   ├── test_code_check_probe.py    # C0: the probe's own decisions about what it reads
│   ├── test_dlt_deployed.py        # C1: running image version, and every way it degrades
│   ├── test_dlt_versions.py        # C3: ordering, incl. a brute-forced total-order check
│   ├── test_dlt_bitbucket.py       # C4: source adapter against recorded responses, no network
│   ├── test_dlt_code_check.py      # C5: the four verdicts and all three asymmetries
│   └── test_dlt_parked.py          # C7: parking, release, expiry, and the cap
├── src/
│   ├── main_api.py                 # FastAPI entry point (uvicorn, port 8000)
│   ├── slow_consumer.py            # Slow consumer entry point: analysis queue -> /analyze-rejection
│   ├── dlt_consumer.py             # DLT consumer entry point: dead-letter topic -> /fetch-dlt-logs
│   ├── dlt_analysis_consumer.py    # DLT analysis entry point: DLT queue -> /analyze-dlt
│   ├── api/
│   │   ├── routes.py               # REST endpoints (/fetch-logs, /analyze-rejection, /process-rejection, /health, /ready)
│   │   └── dlt_routes.py           # DLT endpoints (/fetch-dlt-logs, /analyze-dlt)
│   ├── core/
│   │   └── agent_orchestrator.py   # LangGraph StateGraph build + LLM provisioning
│   ├── dlt/                        # Dead-letter topic analysis (parallel flow; see DLT_PLAN.md)
│   │   ├── headers.py              # Spring DLT header contract, hex epoch decoding
│   │   ├── stacktrace.py           # `Caused by:` chain parsing, frames, fingerprint, FrameLocation
│   │   ├── classify.py             # Failure taxonomy A/B/C/U (pure; takes a catalog hook)
│   │   ├── registry.py             # Reason-code catalog: description, category, failure class
│   │   ├── identity.py             # case_id = dlt-{topic}-{partition}-{offset}
│   │   ├── payload.py              # refId resolution: key -> configured -> per-type -> search
│   │   ├── window.py               # Log window anchored on the last attempt
│   │   ├── corroborate.py          # Trace-vs-log check; the mis-cast detector
│   │   ├── groups.py               # Per-fingerprint occurrence records + recommendations
│   │   ├── reuse.py                # Whether a message needs the LLM at all
│   │   ├── canned.py               # Fixed treatments for Class B/C/U (no LLM)
│   │   ├── orchestrator.py         # DLT analysis lane: Investigate -> Review -> Synthesise
│   │   ├── case_storage.py         # DLT case + group storage, separate from casebooks
│   │   ├── auto_replay.py          # Opt-in auto-replay gate on a high-confidence redrive finding
│   │   │                           #   -- Replay precheck (section 4.4.1, DLT_PLAN.md 14) --
│   │   ├── deployed.py             # C1: which image version the pods are actually running
│   │   ├── versions.py             # C3: version parsing and ordering (never lexical)
│   │   ├── bitbucket.py            # C4: read-only source access, Server and Cloud APIs
│   │   ├── code_check.py           # C5: the verdict -- NO_CHANGE/NOT_DEPLOYED/FIX_DEPLOYED/UNKNOWN
│   │   └── parked.py               # C7: packets held until their fix deploys
│   ├── models/
│   │   ├── schemas.py              # Strict Pydantic data validation schemas
│   │   ├── synthesis.py            # Rejection finding contract + confidence policy
│   │   ├── dlt_schemas.py          # DltMessage: the DLT wire model
│   │   ├── dlt_payload_schemas.py  # EnrolmentEventResponse payload models + refId path registry
│   │   └── dlt_synthesis.py        # DltFinding contract + DLT confidence ceilings
│   ├── prompts/
│   │   ├── LogFilterAgent.md       # Log Filter agent context window sanitization instructions
│   │   ├── InvestigatorAgent.md    # Investigator context and instructions
│   │   ├── ReviewerAgent.md        # Reviewer context and validation logic
│   │   ├── SynthesisAgent.md       # Synthesis output contract (strict JSON keys)
│   │   ├── RunbookGenerator.md     # LLM prompt for generic runbook template generation
│   │   ├── DltInvestigatorAgent.md # DLT investigator: trace vs logs, and what it may not invent
│   │   ├── DltReviewerAgent.md     # DLT reviewer: approval rule
│   │   └── DltSynthesisAgent.md    # DLT finding output contract
│   ├── runbooks/
│   │   ├── draft/                  # LLM-generated runbook drafts (pending human review)
│   │   └── final/                  # Human-approved runbook templates (served online)
│   ├── storage/
│   │   ├── base.py                 # CasebookStorage Protocol
│   │   ├── local.py                # Atomic .tmp + filelock local filesystem backend
│   │   ├── s3.py                   # S3 backend (stub, NotImplementedError)
│   │   └── factory.py              # Backend selection via CASEBOOK_STORAGE_BACKEND
│   ├── tools/
│   │   ├── tool_registry.py        # Custom Python tools (DB lookup, logs, replay queue)
│   │   ├── approve_replays.py      # CLI: approve queued packet replays
│   │   ├── promote_rules.py        # CLI: promote + git-commit learned rules
│   │   ├── check_drift.py          # CLI: rules.csv schema drift detector
│   │   ├── build_catalog.py        # CLI: Stage 0 offline template catalog builder
│   │   ├── eval_harness.py         # CLI: Stage 6 evaluation harness for pipeline accuracy
│   │   ├── prune_checkpoints.py    # CLI: SQLite checkpoint pruning utility
│   │   ├── prune_casesheets.py     # CLI: Old/orphaned casesheet cleanup
│   │   ├── es_diagnostic.py        # CLI: Elasticsearch connectivity and query diagnostics
│   │   ├── fetch_pod_logs.py       # CLI: Direct Kubernetes pod log retrieval
│   │   ├── build_runbooks.py       # CLI: Mine casebooks to draft generic runbook templates
│   │   ├── promote_runbooks.py     # CLI: Human-gate review and promotion of runbook drafts
│   │   ├── dlt_report.py           # CLI: read DLT output (--top, --group, --case, --unreviewed,
│   │   │                           #      --parked, --code-check, --code-check-accuracy)
│   │   ├── dlt_sample.py           # CLI: capture/analyse a real DLT corpus (Phase 0 gate)
│   │   ├── code_check_probe.py     # CLI: replay-precheck feasibility gate (C0; throwaway)
│   │   ├── release_parked_replays.py # CLI: release packets whose fix has now deployed (C7)
│   │   └── parse_reason_codes.py   # CLI: BusinessReasonCode Java source -> reason_codes.csv
│   ├── log_pipeline/
│   │   ├── config.py               # Pipeline constants and tunables
│   │   ├── types.py                # Canonical LogRecord TypedDict and shared types
│   │   ├── catalog.py              # Stage 0: Template classification catalog
│   │   ├── fetcher.py              # Stage 1: Source-filtered ES fetch + search_after
│   │   ├── reducer.py              # Stages 2-4: ERROR branch, Drain3 clustering, guardrails
│   │   ├── pipeline.py             # Top-level orchestrator wiring Stages 1-4
│   │   ├── redaction.py            # PII redaction for log records
│   │   ├── snapshot.py             # Evidence snapshot persistence (raw_logs_k8s.jsonl)
│   │   └── sources/
│   │       ├── base.py             # LogSource Protocol definition
│   │       ├── elastic.py          # ElasticLogSource: wraps fetcher.py
│   │       ├── chain.py            # FallbackChain: ordered LOG_SOURCE cascade
│   │       └── k8s/                # Kubernetes pod log source
│   │           ├── source.py       # KubernetesLogSource entry point
│   │           ├── client.py       # HTTP client for Kubernetes API
│   │           ├── discovery.py    # Pod/namespace discovery, multi-service fan-out
│   │           ├── retrieval.py    # Pod log fan-out and retrieval
│   │           ├── parser.py       # Raw log line parser
│   │           ├── gaps.py         # Evidence gap detection
│   │           ├── retry.py        # HTTP retry with status-aware backoff
│   │           ├── filtering.py    # Log filtering and deduplication
│   │           └── fixtures.py     # Shared test fixtures for k8s tests
│   └── utils/
│       ├── env.py                  # Environment variable configuration
│       ├── paths.py                # Centralized path constants (CHECKPOINT_DB_PATH, etc.)
│       ├── config_validator.py     # Fail-fast boot-time configuration validation
│       ├── logging_config.py       # structlog JSON logging setup
│       ├── kafkaConsumer.py        # Background topic polling + bounded worker pool (CONSUMER_ROLE=fast|slow|dlt|dlt_analysis)
│       ├── message_adapters.py     # Per-role record handling: rejection payload vs DLT case
│       ├── atomic.py               # Atomic file replace with retry (Windows os.replace)
│       ├── metrics.py              # Counters for the DLT lane and LLM usage
│       ├── llm_utils.py            # LLM factory (local OpenAI-compatible / Mistral / HF)
│       ├── resilience.py           # tenacity retries + pybreaker circuit breakers
│       ├── dlq_publisher.py        # Dead Letter Queue producer
│       ├── analysis_queue_publisher.py # Publishes fetched payloads onto the analysis queue
│       ├── s3_uploader.py          # Uploads large Elastic logs to S3
│       ├── runbook_store.py        # Runbook load/save, TTL cache, fingerprinting, path guard
│       └── runbook_validator.py    # Generic-text regex validator (no UUIDs/dates/SRNs)
├── local_casesheets/               # Generated. Five roots under one storage backend:
│                                   #   casebook_<eventId>/  rejection casebooks + logs
│                                   #   dlt_cases/           DLT casebooks + trace/header artifacts
│                                   #   dlt_groups/          per-fingerprint records
│                                   #   dlt_parked_replays/  packets waiting for a deploy (C7)
│                                   #   pending_replays/     replays awaiting human approval
└── local_checkpoints/              # Generated: checkpoints.db, drain3_state/, consumer heartbeat
```

---

## 3. Core Components Deep Dive

### 3.1 Environment Configuration (`.env`)
The system manages all operational feature flags, LLM credentials, MySQL database connections, and Kafka connectivity settings via a strictly typed `.env` file (loaded via `python-dotenv` in `src/utils/env.py`).
- **Template:** A reference file containing all placeholders is available at `.env.example`.
- **Database Modes:** Set `USE_MOCK_DB=true` to parse rules locally from a CSV, or `USE_MOCK_DB=false` to dynamically query the live MySQL `rules` table via SQLAlchemy/PyMySQL.
- **Security:** The actual `.env` file is excluded via `.gitignore` to prevent secret leakage.
- **Credentials the process holds:** the LLM provider key, MySQL, Elasticsearch, a Kubernetes kubeconfig (or an in-cluster ServiceAccount), `OIS_API_KEY` for the replay endpoint, and -- when the replay precheck is configured -- `BITBUCKET_TOKEN`. The last is **read-only and scoped to the repositories named in `DLT_REPO_MAP`**: `src/dlt/bitbucket.py` has no code path that writes, and leaving `BITBUCKET_BASE_URL` empty disables every source lookup outright.
- **Feature flags are layered, not global.** Several capabilities are gated by two or three independent switches rather than one, so "let the system nominate an action" and "let the action actually happen" are always separate decisions -- `DLT_AUTO_REPLAY_ENABLED` vs `ENABLE_AUTO_REPLAY`, and `DLT_CODE_CHECK_ENABLED` vs `DLT_CODE_CHECK_GATES_REPLAY` vs `DLT_CODE_CHECK_PARK_ENABLED` (section 4.4.1).

### 3.2 Environment & Local LLM Integration (`llm_utils.py`)
Unlike generic AI projects bound to OpenAI, `agentic-resident-crm` is designed for on-premise, secure environments.
`get_llm(tier)` is a three-way factory selected by environment flags, in priority order:
1. `USE_HF=true` -> `ChatHuggingFace` over `HuggingFaceEndpoint` (requires `HF_TOKEN`).
2. `MOCK_LLM_WITH_MISTRAL=true` -> `ChatMistralAI` (development/demo path, requires `langchain-mistralai`).
3. Default -> `ChatOpenAI` pointed at an OpenAI-compatible local endpoint via `LLM_BASE_URL_COMPLEX` (e.g. `http://localhost:8000/v1`).

The factory accepts `tier="complex"` and `tier="simple"` and raises `ValueError`
on any other tier. The Investigator and Synthesis agents use `complex`; the
Reviewer -- a bounded verdict task -- uses the cheaper `simple` tier, so both
tiers are now load-bearing rather than one being constructed and discarded.
`.env`, `.env.example`, and `llm_utils.py` all use this same
`_COMPLEX`/`_SIMPLE` env var suffix vocabulary; `config_validator.py` checks
whichever key the *selected* provider (`USE_HF` / `MOCK_LLM_WITH_MISTRAL` /
default OpenAI-compatible) actually reads, not a hardcoded `OPENAI_API_KEY`.

### 3.3 Core Pipeline (Deterministic StateGraph)
Instead of relying on an unpredictable LLM to orchestrate the subagents, the system uses a highly robust, strictly deterministic Python `StateGraph` (via `langgraph`) in `src/core/agent_orchestrator.py`. This ensures the exact sequential execution of every step.

1. **Log Fetcher Node**: Cache-first (section 3.11). Reads `fetched_logs.txt` from `CasebookStorage` -- persisted by `POST /fetch-logs` before `/analyze-rejection` ever invokes the graph -- and uses it directly if present, with no live fetch. Only when that artifact is absent (a direct `/process-rejection` call, `local_run.py`, or any caller that invokes the graph without going through `/fetch-logs` first) does it fall back to fetching live: if `ENABLE_LOG_FETCHING=true`, `fetch_and_persist_logs` triggers the same log-reduction pipeline (`fetch_logs_for`) to pull relevant Kibana/Kubernetes traces using the `eventId` and persists the result for next time.
2. **Runbook Lookup Node**: Checks `RUNBOOK_MODE` (off/serve/shadow). If `serve`, it looks up a final runbook by `(reason_code, enrolment_type)` in `src/runbooks/final/`, verifies the DB rule fingerprint hasn't changed, and short-circuits the graph directly to `END` with the pre-built resolution (no LLM calls). In `shadow` mode, it records the runbook match but lets the agents run normally; `synthesis_node` later compares the two results and logs any divergence. If `off` (the default) or no runbook matches, it falls through to the Investigator.
3. **Investigator Node**: A React agent constructed with an **empty tool list**. All external lookups are performed deterministically in Python before the call: `lookup_rule_by_reason_code` is invoked by the node itself, the result is filtered by `enrolmentType`, and the rule text is injected into the prompt. If the rule lookup fails or returns nothing, it falls back to `get_error_description` (from `tool_registry.py`) to inject hardcoded error definitions (e.g., for `RESIDENT_BIOMETRIC_UPDATE_IDENTIFY_FAILURE`). This removes a whole class of tool-call hallucination and redundant DB round-trips. The prompt is projected down to only the fields the Investigator needs (`eventId`, `packetMetaData`, `packetExecutionSummary`, `flowMetaData.stage`) rather than the full raw Kafka message, and on a retry it sends only the delta -- the prior investigation plus the Reviewer's feedback -- instead of resending the full payload/logs/rule context again.
4. **Reviewer Node**: A distinct React agent, built once at graph-construction time (not per review) and bound to the `simple` LLM tier, that acts as a strict QC validator holding one tool (`add_learning_rule`). The tool no longer closes over the current `event_id`/investigation text per call -- it reads them from a pair of `contextvars.ContextVar`s that `reviewer_node` sets before each invocation, since each packet already runs on its own dedicated thread.
5. **Conditional Router & Loop Guard**: A pure Python control edge that checks the Reviewer's output via `is_reviewer_approved()`: the (markdown/whitespace-stripped) feedback must *start with* the literal token `APPROVED`, not merely contain it -- this closes the "NOT APPROVED"/"DISAPPROVED" false-positive that a substring match would produce. Otherwise it increments `retry_count`; once `retry_count >= MAX_INVESTIGATION_RETRIES` it routes to the `escalate` node (preventing infinite LLM loops), else it loops back to the Investigator Node. A fresh (non-resumed) invocation always starts `retry_count` at 0, so a redelivered packet can never resume a stale checkpoint with the retry budget already exhausted.
6. **Synthesis Node**: The final agent that takes the approved, heavily vetted technical diagnosis and translates it into a human-readable JSON `Casebook`. It holds the `queue_for_replay` tool. In shadow mode, it also compares its output to the runbook's pre-built resolution and logs a warning on any `action` divergence.
7. **Log Processor & S3 Uploader**: After the graph completes, `routes.py` structures the final casebook's `packet_status.rejection_data.rejection_logs` field into an object containing `path` and `gaps`. `upload_logs_to_s3()` pushes raw logs to AWS S3 via `boto3` (`src/utils/s3_uploader.py`), and the resulting `s3://...` URL is embedded in the `path` field. If `S3_LOGS_BUCKET` is unset or upload fails, `path` instead embeds a local file reference rather than losing the trace. The `gaps` field explicitly parses and captures any missing timeframe banners from the raw trace so operators retain full context without inline clutter.

### 3.4 Resilience & Hardening (Phase 1 & 2)
The architecture incorporates several resilience mechanisms to prevent runaway costs, silent failures, file corruption, and pipeline deadlocks:
- **Idempotency & Staleness Guards**: The API intercepts requests and validates against the `CasebookStorage` interface. `IN_PROGRESS` stubs are written immediately to a separate `status.json` file to prevent duplicate runs without polluting the final `casebook.json`. Upon successful completion, `status.json` is overwritten with the terminal status. If an `IN_PROGRESS` stub goes stale (exceeding `MAX_IN_PROGRESS_AGE_SECONDS`), the pipeline safely resumes from a LangGraph checkpoint or fresh start. Terminal statuses include `COMPLETED`, `REJECTED`, `NEEDS_MANUAL_REVIEW`, `FAILED_PERMANENT`, `DLQ`, and `FAILED_TIMEOUT`. `POST /fetch-logs` writes a non-terminal `LOGS_FETCHED` status ahead of `IN_PROGRESS` (section 3.11); it only ever advances `status.json` from absent/`LOGS_FETCHED`, never overwriting an `IN_PROGRESS` or terminal status a concurrent `/analyze-rejection` call may already have written, so a redelivered fetch can't hide the marker that `_investigate_packet`'s own dedupe guard depends on.
- **6-Stage Log Reduction Pipeline (`src/log_pipeline/`)**: Elasticsearch logs are no longer dumped raw into the LLM context. Instead, they pass through a production-grade pipeline: Stage 1 (source-filtered fetch with `search_after` and an `_id` tiebreaker for broad ES version compatibility, a hard `LOG_MAX_DOCUMENTS` cap, TLS verification on by default (`ES_VERIFY_CERTS`), and local Kibana CSV mock support via `ES_MOCK_FILE` for offline testing), Stage 2 (branch on ERROR -- stuck packets skip clustering, with both a leading *and* trailing context window so a cascading failure can't pull in the entire trace), Stage 3 (Drain3 clustering with file-persisted state for stable template IDs, held as a process-wide `TemplateMiner` singleton so the state file is only read/deserialized once per process rather than per packet, serialized by a thread lock + cross-process `FileLock` so concurrent packets can't corrupt the shared parse tree, and scoped to emit only the clusters this call's own logs actually matched -- never another packet's templates), and Stage 4 (evidence assembly guardrails enforcing decision-vocabulary regex matches, rare-template retention, and flow-boundary context). An offline Stage 0 catalog (`build_catalog.py`) classifies templates as boilerplate/informative/decision-marker and flags an implausibly high boilerplate share, and a Stage 6 eval harness (`eval_harness.py`) validates pipeline accuracy before production use.
- **Pluggable Log Sources (`src/log_pipeline/sources/`)**: Stage 1 sits behind a `LogSource` Protocol (mirroring `CasebookStorage`), so Stages 2-4 are source-agnostic -- any source emitting the canonical `LogRecord` (`timestamp`/`level`/`message`/`app_name`, defined in `src/log_pipeline/types.py`) works with Drain3 clustering, the guardrails, the S3 offload, and the casebook wiring unchanged. `ElasticLogSource` wraps the existing fetcher without modifying it, so the `ES_MOCK_FILE` CSV workflow is unchanged. See section 3.10 for the Kubernetes log source and the fallback chain architecture.
- **Decoupled Fetch/Analyze Consumers, Bounded Concurrency, & At-Least-Once Delivery**: `fast_consumer.py` and `slow_consumer.py` (both thin entry points over `src/utils/kafkaConsumer.py`, selected by `CONSUMER_ROLE`; section 3.11) each isolate their own Kafka polling loop and submit tasks to a `ThreadPoolExecutor` bounded by a `Semaphore` (`MAX_CONCURRENT_INVESTIGATIONS`, sized independently per process). To guarantee At-Least-Once delivery and prevent consumer rebalances during slow AI processing, each consumer is configured with `KAFKA_MAX_POLL_RECORDS` and a high `KAFKA_MAX_POLL_INTERVAL_MS`. Offsets are never committed immediately upon dispatch; instead, an `OffsetTracker` records completions and each poll cycle commits only the safe low-water mark -- the highest offset below which every dispatched message on that partition has completed -- so a batch that finishes out of order can never commit past one still in flight. If a crash or 429 error occurs, the offset is not marked complete, and Kafka safely redelivers the packet.
- **DLQ, Poison-pill, & Checkpointing**: LangGraph uses `SqliteSaver` (with WAL mode enabled) for scalable crash recovery. Structurally invalid Kafka messages (poison-pills) and unrecoverable pipeline crashes are immediately published to a Dead Letter Queue (`rejected-packets-dlq`) via `dlq_publisher.py`.
- **Pipeline Timeouts**: `PACKET_TIMEOUT_SECONDS` bounds the **slow consumer's HTTP client** waiting on `/analyze-rejection` -- the LLM investigation budget, same variable and meaning this had before the fetch/analyze split. The fast consumer gets its own, much shorter `FAST_CONSUMER_TIMEOUT_SECONDS` for the bounded `/fetch-logs` call. The API side is independently bounded too: `routes.py` runs `agent.invoke()` (from `/analyze-rejection` or `/process-rejection`) on a dedicated executor thread with its own budget (`AGENT_INVOKE_TIMEOUT_SECONDS`, defaulting to `PACKET_TIMEOUT_SECONDS - 30s`) so the server is authoritative about its own failure and returns `FAILED_TIMEOUT` before the consumer's deadline fires. `/fetch-logs` has no equivalent dedicated executor -- it's bounded I/O (`K8S_TOTAL_FETCH_TIMEOUT_SECONDS`, `ES_REQUEST_TIMEOUT_SECONDS`), not a multi-minute LLM call, so it runs on Starlette's own sync-dispatch threadpool like `/health`/`/ready`. Both the LLM clients (`LLM_TIMEOUT_SECONDS`, `max_retries=0`) and `agent.invoke` are bounded. If a terminal `FAILED_TIMEOUT`/`DLQ` status is already recorded by the time a slow invocation finally returns, that late result is discarded rather than overwriting it.
- **Human-in-the-Loop Replays**: Agents cannot fire destructive API requests directly. Unless `ENABLE_AUTO_REPLAY=true`, replay actions invoked by the LLM are queued for an operator to approve via `approve_replays.py`. The queue is **one document per packet id** under the `pending_replays` storage root (`get_scoped_storage`), not the `src/db/pending_replays.jsonl` file it used to be: that file lived on whichever pod happened to write it, so under `CASEBOOK_STORAGE_BACKEND=s3` with more than one replica an operator saw only their own pod's entries and the rest were invisible indefinitely. `approve_replays.py` still drains any leftover local jsonl for backwards compatibility. Both the auto-replay call and `approve_replays.py` send the replay payload as an authenticated (`OIS_API_KEY`) JSON body rather than query params, so PII like `notificationEmail`/`notificationMobile` doesn't land in server access logs. The packet id is pattern-guarded (`EVENT_ID_PATTERN`) before it is interpolated into a storage path.
- **Safe Self-Learning & Drift Checks**: The Reviewer's `add_learning_rule` tool stages suggestions to `src/prompts/pending_rules.jsonl` using `filelock`. A human runs `src/tools/promote_rules.py` (which includes top-level locking and git-status safety checks) to approve and Git-commit the rules; only promoted entries are removed from the pending file, so skipped/errored/concurrently-appended entries survive. Additionally, `src/tools/check_drift.py` detects database schema/policy drift, and distinguishes a genuinely changed schema from a malformed single-column CSV export.
- **External Call Resilience**: `tenacity` handles exponential backoff retries, and `pybreaker` provides circuit breakers for database, Elasticsearch, LLM, Kubernetes, and Bitbucket calls (`db_breaker`, `es_breaker`, `llm_breaker`, `k8s_breaker`, `bitbucket_breaker` -- all `fail_max=3`, `reset_timeout=60`). Every one is sampled onto the `breaker_state` gauge at scrape time rather than on transition, so a breaker that reset on a timeout doesn't leave a stale "open" reading behind. The two newest are read-only paths whose failure must only ever *degrade* a result: a tripped `k8s_breaker` makes the running image version unknown, and a tripped `bitbucket_breaker` makes a replay-precheck verdict `UNKNOWN` -- neither raises into the analysis lane.
- **Storage Abstraction & Schema Versioning**: The `CasebookStorage` interface implements retried atomic `.tmp` writes (to safely handle concurrent readers/AV scanners holding the file on Windows) and enforces a `"schema_version"` field on every saved casebook for backwards compatibility.
- **Structured Logging & Health Checks**: `agent_orchestrator.py`, `tool_registry.py`, `kafkaConsumer.py`, `dlq_publisher.py`, `analysis_queue_publisher.py`, `s3_uploader.py` and the entire `log_pipeline/` package log through the same `structlog` logger as `routes.py` (bound to `event_id` where available) rather than bare `print()`. Verbosity is set by `LOG_LEVEL`. The operator CLIs still print to stdout deliberately -- they are interactive tools, not services. The FastAPI server provides `/health` (reporting both the fast and slow consumers' heartbeats, under `fast_consumer`/`slow_consumer`, plus a top-level `last_heartbeat`/`consumer_alive` alias for the fast consumer that predates the split) and `/ready` (verifying SQLite and Kafka producer connectivity) endpoints; the Kafka producer check is cached for `PRODUCER_HEALTH_TTL_SECONDS` (default 30s) so a burst of readiness probes can't each force a fresh broker connection attempt. `validate_config()` provides fail-fast configuration validation at boot.
- **Agent Caching**: Investigator, Synthesis, and Reviewer React agents are all created once at graph construction time and reused across invocations, avoiding per-packet (and, for the Reviewer, per-retry) LLM handshake overhead.
- **Non-Blocking Request Handling**: `/process-rejection` and `/analyze-rejection` are both `async def`; `agent.invoke()` runs on a dedicated `ThreadPoolExecutor` sized to `MAX_CONCURRENT_INVESTIGATIONS`, separate from Starlette's own sync-dispatch threadpool. A multi-minute investigation therefore can't starve `/health`, `/ready`, `/fetch-logs`, or the sync auth/rate-limit dependencies of a worker slot. `/fetch-logs` is deliberately plain `def`, not `async def` -- its bounded I/O runs on Starlette's own threadpool, the same one `/health`/`/ready` use, since it never needs the dedicated executor a multi-minute LLM call does.
- **Indexed Mock Rule Lookups**: `lookup_rule_by_reason_code` builds a `reason_code -> row positions` index over the mock rules table once (cached for the process lifetime) instead of rescanning and re-casting every row on every lookup; a missing/unreadable mock DB file is also cached so the filesystem isn't re-probed on every call.
- **Rate Limiter Eviction**: The in-memory IP rate limiter evicts stale entries when it exceeds 1000 tracked IPs to prevent unbounded memory growth.

### 3.5 The Agent Ecosystem
The intelligence of the system relies on a multi-agent hierarchy. Both the Investigator and Synthesis agents are strictly instructed to reference the business logic outlined in `agent_policy_context.md` to understand success criteria and parse deviations correctly.
- **Dynamic Context Injection**: The Python orchestrator dynamically intercepts and filters database rules (e.g., checking the `enrolmentType` from the payload) before injecting the exact correct rule into the agent's prompt to avoid LLM hallucinations.
- **RejectionManager (not an LLM)**: The conductor is the compiled `StateGraph` itself, not an agent. Routing is plain Python, so the sequence of steps cannot be altered by a model.
- **LogFilterAgent**: (Optional). Because logs are fetched from Kubernetes using a sliding window (e.g., 5 lines before, 20 lines after a match), the resulting block often contains log lines and errors from highly concurrent, unrelated packets. If `ENABLE_LOG_FILTER_AGENT=true`, this agent reads the block and cleanly deletes any errors belonging to other `eventId`s or `refId`s before the investigation begins, writing its output to a `filtered_logs.txt` artifact for local debugging before uploading to S3.
- **InvestigatorAgent**: The detective. It correlates error codes (`reasonCode`) with the internal business rule (`ruleId`) that the orchestrator pre-fetched for it, cross-references the reduced Elasticsearch trace, and determines the technical failure. It holds no tools of its own. It is explicitly hardened against "Context Confusion," meaning it is strictly instructed to verify the `eventId` of any ERROR log before trusting it, preventing cross-packet hallucinations when the LogFilterAgent is disabled.
- **ReviewerAgent**: The auditor. It checks the Investigator's homework to eliminate hallucinations.
- **SynthesisAgent**: The resolution writer. Once the investigation is validated, this agent synthesizes the findings into plain English, categorizes the remediation steps into strict enums (e.g., `NEW_PACKET`, `REPLAY`), and generates the analytical JSON block.

### 3.6 6-Stage Log Reduction Pipeline
Elasticsearch logs are heavily compressed to prevent LLM context window exhaustion and save tokens, using a production-grade map-reduce and clustering architecture (`src/log_pipeline/`):
- **Stage 0 (Offline Catalog)**: `build_catalog.py` samples historical logs to identify structural templates, classifying them as `boilerplate`, `informative`, or `decision-marker` based on cross-flow frequency.
- **Stage 1 (Fetch)**: Source-filters Elastic logs to minimal fields, uses `search_after` with `_seq_no` for stable pagination, and uses catalog-driven `must_not` filters to drop pure boilerplate.
- **Stage 2 (ERROR Branching)**: Instantly detects `level=ERROR` logs. If found, it trims the trace to the exact error plus a 200-line preceding context window, bypassing clustering entirely to preserve raw crash forensics.
- **Stage 3 (Drain3 Clustering)**: For non-crashing (logic/rule rejection) flows, it uses Drain3 to strip dynamic noise (UUIDs, IPs) and cluster identical logs into structural templates. Clustering state is file-persisted to keep template IDs stable.
- **Stage 4 (Evidence Guardrails)**: Regardless of clustering, it forces full-text retention for matches against a decision-vocabulary regex (e.g., `Validation Failed`), rare templates (count < 5), and flow boundaries.
- **Stage 5 & 6 (LLM & Eval)**: The heavily compressed, structured output is injected into the LLM context (and simultaneously persisted to `reduced_logs.txt` for human audits). `eval_harness.py` provides an offline safety check to measure evidence-citation accuracy against ground truth before trusting the pipeline in production.

### 3.7 The Self-Learning Loop (human-gated)
If the `ReviewerAgent` spots a mistake (e.g., the Investigator recommended a solution that contradicts the business rule), the Reviewer invokes the `add_learning_rule` tool, defined inline in `agent_orchestrator.reviewer_node`.

The tool does **not** mutate any prompt directly. It appends a JSON proposal
(`eventId`, timestamp, proposed rule, reviewer reasoning, original investigator output)
to `src/prompts/pending_rules.jsonl` under a `filelock`. A human then runs
`src/tools/promote_rules.py`, which refuses to run if `src/prompts/` has uncommitted
changes, prompts per rule, appends approved ones to `src/prompts/InvestigatorAgent.md`
as `- CRITICAL RULE:` lines, and Git-commits each promotion. This keeps the learning
loop auditable and prevents an LLM from silently rewriting its own instructions.

### 3.8 Storage & Casesheets
Outputs are stored in `local_casesheets/casebook_<event_id>/`. This directory contains:
- `casebook.json`: The final structured JSON block (terminal state only).
- `status.json`: The in-flight lifecycle marker (`IN_PROGRESS`, then the terminal status).
- `raw_logs.txt`: The complete uncompressed Elasticsearch log trace.
- `reduced_logs.txt`: The heavily compressed logs that were injected into the LLM.
- `*.lock` / `*.tmp`: `filelock` and atomic-write scratch files.

**Five roots share one backend.** `storage.factory.get_scoped_storage(root)`
returns the same `CasebookStorage` implementation -- same locking, same atomic
`.tmp` writes, same terminal-status handling -- scoped to a subdirectory
(local) or key prefix (S3). This is configuration, not a second storage
implementation:

| Root | Written by | Holds |
|---|---|---|
| `casebook_<eventId>/` | the rejection lane | casebooks, status, raw/reduced logs |
| `dlt_cases/` | `/fetch-dlt-logs`, `/analyze-dlt` | DLT casebooks, `headers.json`, `trace.txt`, `parsed_trace.json`, `payload_summary.txt`, `fetched_logs.txt`, `deployed.json` |
| `dlt_groups/` | `dlt/groups.py` | one record per fingerprint: counts, members, recommendation, latest code-check verdict |
| `dlt_parked_replays/` | `dlt/parked.py` | packets waiting for their fix to deploy (section 4.4.1) |
| `pending_replays/` | `queue_for_replay` | replays awaiting human approval |

Keeping DLT cases out of `casebook_<eventId>/` matters: `accuracy_report`,
`prune_casesheets` and everything else that walks `list_events()` expects
rejection casebooks, and a DLT case has a different schema, lifecycle and
audience.

Group and parked records are mutated through `CasebookStorage.update_json`,
never load-then-save. Both are read-modify-write on a counter, and the DLT
analysis role is meant to scale out -- a `filelock` under
`LOCAL_CHECKPOINTS_DIR` coordinates processes on a shared filesystem and does
nothing at all for two pods on different nodes, which is precisely the
deployment it was written for. `update_json` puts the atomicity in the backend
that can actually provide it: a held lock locally, a conditional write on S3.

`eventId` is constrained by a Pydantic pattern (`^[A-Za-z0-9_.:-]{1,128}$`) before
it is ever interpolated into a path, and `LocalFilesystemCasebookStorage`
independently refuses to resolve a directory outside its storage root as
defense in depth. The DLT `case_id` and a parked entry's key carry the same
guard. Only `save()` creates a casebook directory as a side
effect -- `load()`/`exists()` are read-only and never create one, so probing
for an event that doesn't exist (or was skipped) doesn't leave an empty
directory behind.

To ensure zero hallucinations, `routes.py` deterministically extracts static metadata directly from the Kafka payload. All keys are `snake_case`. The output is a hierarchical JSON block formatted for downstream systems:
- **packet_metadata** (`srn`, `eid`, `ref_id`, `source`, `packet_type`, `created_at`, ...)
- **packet_status** (`status`, `service`, `sub_service`, `rejection_data`)
- **resolution** (`source`, `synthesis`, `action`, `resident_action` -- `source` is `"agent"` for LLM-generated or `"runbook:<id>@v<version>"` for runbook-served results)
- **schema_version** (injected by the storage layer, currently `"1.1"`)

`packet_metadata.is_mbu`, `update_type`, and `is_child` are currently emitted as `null`
because the mapping is not derivable from the payload alone.

### 3.9 Runbook Pipeline
For repeated rejections, the system implements a Runbook pattern to short-circuit the multi-minute LLM loop.
- **Drafting (Offline)**: `build_runbooks.py` mines `local_casesheets/` for completed resolutions sharing the same `errorReasonCode` and `enrolmentType`. It uses a strictly prompted LLM (the `simple` tier) to synthesize a generic resolution template that contains zero packet-specific values (enforced by a regex validator checking for UUIDs, dates, SRNs, etc.). The result is saved to `src/runbooks/draft/`.
- **Promotion (Offline)**: `promote_runbooks.py` acts as a human review gate. Operators inspect the generic template and approve it. The tool checks for rule fingerprint staleness, bumps the version, and git-commits the final template to `src/runbooks/final/`.
- **Serving (Online)**: A `runbook_lookup` node runs immediately after `fetch_logs`. If `RUNBOOK_MODE=serve` and a final runbook matches the current packet's reason code (with the `rule_fingerprint` matching the live DB rule), the graph short-circuits the agents and directly emits the runbook's generic resolution. To preserve auditability, `resolution.source` in the casebook is marked with `runbook:<id>@v<version>`. If `RUNBOOK_MODE=shadow`, the agents still run and any divergence is logged. `RUNBOOK_SERVE_ALLOWLIST` narrows `serve` to specific reason codes: a code not on the list keeps running the agents and is shadow-compared, which is how it earns its place. (Until 2026-08-15 the fingerprint check raised `TypeError` and DLQ'd every runbook-matching packet.)

### 3.10 Kubernetes Log Source (`src/log_pipeline/sources/k8s/`)
Elasticsearch is the primary log source and system of record, but it can drop lines under heavy load or indexing delays. The Kubernetes log source reads pod logs directly from the kubelet API to cover those gaps. **It is supplementary, not a replacement.** The full design is documented in `KUBERNETES_LOGS_PLAN.md`.

#### Fallback Chain (`LOG_SOURCE`)
The `LOG_SOURCE` environment variable is an ordered, comma-separated chain that controls which sources are tried:
- `kubernetes,elastic` (default) -- try pods first, fall back to Elasticsearch. With no cluster configured (`KUBECONFIG_PATH`/`K8S_DEFAULT_NAMESPACE` unset), the Kubernetes leg fails fast and every fetch falls straight through to Elasticsearch.
- `elastic,kubernetes` -- the reverse.
- `elastic` -- Elasticsearch only (behaviour prior to the Kubernetes source).
- `kubernetes` -- Kubernetes only, no fallback.

Fallback triggers when a source fails OR returns no records. Sources are never merged -- one wins per fetch. A `SOURCE_FALLBACK` evidence gap records what was tried. The chain is implemented in `src/log_pipeline/sources/chain.py`.

#### Architecture
The `KubernetesLogSource` (`sources/k8s/source.py`) ties together five internal modules:

1. **Discovery** (`discovery.py`): Verifies the namespace with a targeted `read_namespace` pre-flight (the ServiceAccount cannot list namespaces or pods cluster-wide), then lists pods within it -- by default a client-side name-substring match (`PodMatchSpec`, `K8S_SERVICE_MAP`), or a server-side label selector where an app opts in -- filters out sidecars (`istio-proxy`, `linkerd-proxy`, `vault-agent`), skips `Pending` pods, and caps the target list at `K8S_MAX_PODS` (default 20). Reports `TRUNCATED_PODS` evidence gaps when the cap is reached.

   **Multi-service (2026-08-21).** A refId passes through several services, so `K8S_APP_NAMES` -- falling back to `ES_APP_NAMES`, so one list drives both sources -- names every service to search. Each resolves its own namespace and match spec, and the results are merged. Three properties make the merge safe: pods are **deduped** on (namespace, pod, container), because `name_contains` is a substring test and `enu-biometric` therefore also matches `enu-biometric-abis-mw-consumer`'s pods -- reading such a pod once per matching service would duplicate every line it contributed; a service that cannot be searched **degrades rather than fails**, yielding a `SERVICE_UNAVAILABLE` gap while the others still return logs, so an unreachable hop is announced instead of being mistaken for a silent one; and `K8S_MAX_PODS` applies **per service** with merging done round-robin, so adding a service never shrinks another's representation and the optional `K8S_MAX_TOTAL_PODS` ceiling trims every service evenly rather than dropping whichever was configured last.
2. **Retrieval** (`retrieval.py`): Reads logs for each discovered `(pod, container)` pair using a concurrent `ThreadPoolExecutor` fan-out. Streams logs line-by-line (`_preload_content=False`) to avoid buffering hundreds of megabytes. Requests kubelet timestamps (`timestamps=True`) for reliable cross-pod ordering. Also reads `previous=True` logs for restarted containers so pre-crash evidence is not lost. The entire fan-out is bounded by a wall-clock deadline (`K8S_TOTAL_FETCH_TIMEOUT_SECONDS`), enforced with `as_completed(timeout=...)` plus an explicit `shutdown(wait=False, cancel_futures=True)` -- a `with ThreadPoolExecutor(...)` block would call `shutdown(wait=True)` on exit and wait for every slow pod regardless of the deadline.
3. **Parser** (`parser.py`): Splits each line into a kubelet RFC3339Nano timestamp and a body, then extracts a structured `LogRecord` with `level`, `message`, and `app_name`. Tracks parse statistics (`ParseStats`) so degradation can be detected.
4. **Filtering** (`filtering.py`): Applies client-side identifier matching (the kubelet API has no server-side grep). Matches by `eventId`, `refId`, and any extra identifiers. Uses a `KeepAllSelector` fallback when no identifier is available.
5. **Gap Detection** (`gaps.py`): The mechanism that makes the source trustworthy. Detects four types of evidence gaps:
   - **Log Rotation**: Oldest observed line is newer than the requested window start, meaning earlier logs were rotated off the node.
   - **Pod Replacement**: A pod's `startTime` is after the requested window start, meaning the previous instance's logs are gone.
   - **Parse Degradation**: More than 10% of lines failed level extraction, suggesting an unexpected log format.
   - **Truncation**: The pod list exceeded `K8S_MAX_PODS`.

   Gaps are rendered as a banner (`--- EVIDENCE GAPS (the trace below is INCOMPLETE) ---`) prepended to the text handed to the LLM, so the Investigator and Reviewer know to qualify their findings.

#### PII Redaction (`src/log_pipeline/redaction.py`)
Log lines may carry Aadhaar numbers, VIDs, mobile numbers, or email addresses. Redaction runs in `pipeline.reduce_logs` -- the one seam **every** source passes through, so Elasticsearch is covered too -- after identifier filtering and before any persistence. The Kubernetes source additionally redacts before writing its own snapshot, which is a separate, earlier persistence point; redaction is idempotent, so the second pass is a no-op:

```
fetch -> filter by identifier -> extract context -> REDACT -> persist
```

Patterns matched (longest-first to prevent partial matches): 16-digit VIDs, 12-digit Aadhaar numbers, spaced Aadhaar (`NNNN NNNN NNNN`), 10-digit mobile numbers, email addresses. Operational identifiers (`eventId`, `refId`) are allowlisted so they remain matchable. Placeholders are retained rather than deleted, so the LLM can see that a value existed.

#### Evidence Snapshot (`src/log_pipeline/snapshot.py`)
Kubelet retention is short (roughly 10MB x 5 files per container), but investigations routinely happen much later -- consumer lag, DLQ replays, checkpoint resumes, and the Investigator retry loop all re-enter the fetch path. The first successful Kubernetes fetch is persisted as structured JSONL (`raw_logs_k8s.jsonl`) alongside a metadata file (`log_snapshot_meta.json`). Every later fetch reuses the snapshot. This makes retries deterministic and free, and preserves evidence that the kubelet has since discarded.

#### HTTP Client & Retries
- `client.py`: Thin wrapper over `urllib3` / `kubernetes.client` for the Kubernetes API.
- `retry.py`: Status-aware backoff: retries on 429/5xx with full jitter, never retries a 403 (RBAC misconfiguration should fail fast, not loop). Wrapped around all three Kubernetes API calls (`read_namespace`, `list_namespaced_pod`, `read_namespaced_pod_log`); `k8s_breaker` guards `KubernetesLogSource.fetch` so a cluster that is down entirely fails fast rather than costing every packet a full fan-out timeout.

### 3.11 Fetch/Analyze Consumer Split

Log fetching (bounded Kubernetes/Elasticsearch I/O) and LLM analysis
(unbounded, minutes-long) used to run back-to-back inside one
`agent.invoke()` call reached via one Kafka consumer. That coupled their
scaling: the consumer's effective throughput was capped by LLM latency, and
under any backlog a packet's Kubernetes pod logs -- short retention, roughly
10MB x 5 files per container -- could rotate away before the packet was even
fetched. The two stages are now fully decoupled, each independently
scalable, connected by a second Kafka topic.

**Topics, consumers, routes:**

| | Topic (default) | Consumer | Route |
|---|---|---|---|
| Fetch | `rejections` (`KAFKA_CONSUMER_TOPIC_NAME`) | `src/fast_consumer.py` | `POST /fetch-logs` |
| Analyze | `packet-analysis-queue` (`PACKET_ANALYSIS_TOPIC_NAME` / `SLOW_CONSUMER_TOPIC_NAME`) | `src/slow_consumer.py` | `POST /analyze-rejection` |

Both consumers are thin entry points over the same, otherwise-unmodified
`src/utils/kafkaConsumer.py` engine -- the offset tracker, rebalance
listener, heartbeat, health server, bounded worker pool, DLQ routing, and
graceful shutdown drain are all identical code, reused rather than
duplicated. Each entry point sets `CONSUMER_ROLE` (`fast`/`slow`) before
importing the module, which is what selects that process's topic, group id,
internal endpoint, per-message timeout, heartbeat file, and health port --
see the `CONSUMER_ROLE` block at the top of `kafkaConsumer.py`. No
role-specific branching exists anywhere else in that file: the analysis
queue carries the exact same payload the original topic did (still
`packetStatus == "REJECTED"`, still validated by the same `MessagePayload`
schema), so the existing poison-pill check and terminal-casebook dedupe are
exactly the right guards for both roles, unchanged.

**`POST /fetch-logs`** (fast consumer's target): fetches Kubernetes and
Elasticsearch logs for the payload (`fetch_and_persist_logs` in
`tool_registry.py` -- the same `ENABLE_LOG_FETCHING` check and
`fetch_logs_for` call `fetch_logs_node` used to run itself), persists the
result to `CasebookStorage` as `fetched_logs.txt`, writes a non-terminal
`LOGS_FETCHED` status, and republishes the payload onto the analysis queue.
It touches neither the LangGraph agent nor its checkpointer. It is
idempotent on redelivery: an already-terminal event is skipped entirely, an
already-fetched event reuses the persisted artifact instead of re-fetching,
and it never overwrites a `status.json` that has already advanced to
`IN_PROGRESS` or a terminal status (see 3.4's Idempotency bullet) -- so a
duplicate delivery on the *original* topic can't reopen a dedupe race on the
*analysis* side. Runs as a plain `def` route (Starlette's own sync
threadpool), not `async def`, since the work is bounded I/O, not a
multi-minute LLM call.

**`POST /analyze-rejection`** (slow consumer's target): byte-for-byte the
same body as `/process-rejection` -- both call the same `_investigate_packet`
under the same metrics/in-flight-tracking wrappers, because that function has
no idea, and no need to know, where `state["logs"]` came from. The only
thing that differs in practice is *how* `fetch_logs_node` gets its logs.

**Checkpoint and state safety.** The graph itself (node set, edges,
`thread_id = event_id` checkpointer keying in `agent_orchestrator.py`) is
completely unchanged by this split. `fetch_logs_node` is cache-first: it
checks `CasebookStorage.load_artifact(event_id, "fetched_logs.txt")` first
and returns that if present (the normal path once `/fetch-logs` has run);
only when it's absent does it fall back to a live fetch, via the same
`fetch_and_persist_logs` function `/fetch-logs` uses (which persists the
artifact so a later retry doesn't re-fetch). That fallback is what keeps
`/process-rejection`, `local_run.py`, and every pre-split test working
unmodified -- none of them ever populate `fetched_logs.txt`, so they always
take the live-fetch path, exactly as before the split -- and it's also what
lets `/analyze-rejection` degrade gracefully if it's ever reached before
`/fetch-logs` (a manual publish to the analysis queue, or a race). Because
the artifact's presence (not its content) is what fetch_logs_node checks,
even the "disabled"/"no logs found" sentinel strings are cached and treated
as a completed fetch, never re-attempted.

**Local development:** `start.py` spawns all three processes (API,
`fast_consumer.py`, `slow_consumer.py`); no special per-child environment is
needed since each consumer sets its own `CONSUMER_ROLE`. See section 4.

**The DLT lane repeats this split, not the code.** `dlt_consumer.py` and
`dlt_analysis_consumer.py` are two more entry points over the same
`kafkaConsumer.py` engine (`CONSUMER_ROLE=dlt` / `dlt_analysis`), feeding
`POST /fetch-dlt-logs` and `POST /analyze-dlt`. The same reasoning applies for
the same reason -- bounded I/O must not queue behind a multi-minute LLM call --
and the DLT analysis route gets its own bounded executor
(`MAX_CONCURRENT_DLT_ANALYSES`), a *sibling* of the rejection lane's rather
than the same pool, so a DLT backlog cannot starve the rejection lane or vice
versa. Section 4.4 covers what actually runs in each.

---

## 4. How to Run Locally

1. **Install Dependencies:**
   **Mac/Linux:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

   **Windows (Command Prompt):**
   ```cmd
   python -m venv .venv
   .venv\Scripts\activate.bat
   pip install -r requirements.txt
   ```

   **Windows (PowerShell):**
   ```powershell
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. **Configuration:**
   Copy `.env.example` to `.env` and set at minimum `USE_MOCK_DB`, `MOCK_DB_PATH`,
   `LLM_BASE_URL_COMPLEX` / `LLM_MODEL_COMPLEX`, and `AGENTIC_RESIDENT_CRM_API_KEY`.
   For fully offline runs, set `ES_MOCK_FILE` to a Kibana CSV export.

3. **Start all three services:**
   ```bash
   python3 start.py
   ```
   *This supervisor spawns `src/main_api.py` (FastAPI on port 8000),
   `src/fast_consumer.py` (rejections -> `/fetch-logs`), and
   `src/slow_consumer.py` (analysis queue -> `/analyze-rejection`) as three
   separate processes. See section 3.11 for the fetch/analyze split.*

   To run them individually:
   ```bash
   python3 src/main_api.py        # API only
   python3 src/fast_consumer.py   # Fast consumer only
   python3 src/slow_consumer.py   # Slow consumer only
   ```

### 4.2 API Documentation (Swagger UI)
Because the application is built on FastAPI with populated metadata, interactive API documentation is automatically generated.
- Navigate to `http://localhost:8000/docs` to view the Swagger UI.
- Here you can see the fully expanded `MessagePayload` schema (including optional fields like `flowMetaData`, `resubmissionSummary`, etc.) and test the `/process-rejection` endpoint directly from your browser.

4. **Testing Pipeline (No Kafka Required):**
   ```bash
   python3 test_payload.py                 # static Pydantic validation smoke test
   python3 local_run.py path/to/packet.json # POST a real packet to a running API
   PYTHONPATH=. python3 -m pytest tests/ -q # full regression suite
   ```

### 4.3 Operator CLIs
```bash
# Self-learning & rules
python3 -m src.tools.promote_rules          # review + git-commit staged learning rules
python3 -m src.tools.approve_replays        # approve queued packet replays
python3 -m src.tools.check_drift            # detect rules.csv schema drift

# Runbooks
python3 -m src.tools.build_runbooks --dry-run          # draft generic runbooks from casebooks
python3 -m src.tools.promote_runbooks                  # review + approve runbook drafts
python3 -m src.tools.promote_runbooks --list            # list drafts and check staleness

# Maintenance & diagnostics
python3 -m src.tools.prune_checkpoints --dry-run        # SQLite checkpoint pruning
python3 -m src.tools.prune_casesheets --dry-run         # old casesheet cleanup
python3 -m src.tools.es_diagnostic                     # Elasticsearch connectivity diagnostics
python3 -m src.tools.fetch_pod_logs                    # direct Kubernetes pod log retrieval

# Log pipeline
python3 -m src.tools.build_catalog --refids-file refids.txt   # Stage 0 catalog builder
python3 -m src.tools.eval_harness --test-cases test_cases.json # Stage 6 evaluation harness

# DLT replay precheck (DLT_PLAN.md 14)
python3 -m src.tools.code_check_probe --all --repo ENU/enu-biometric  # C0 feasibility gate
python3 -m src.tools.dlt_report --parked                # packets waiting for a deploy
python3 -m src.tools.dlt_report --code-check            # verdict distribution
python3 -m src.tools.dlt_report --code-check-accuracy   # were the verdicts right?
python3 -m src.tools.release_parked_replays --dry-run   # release what a deploy unblocked
```

---

## 4.4 Dead-Letter Topic (DLT) Analysis

A **parallel flow** to the rejection pipeline, specified in full in
`DLT_PLAN.md`. It consumes a Spring `@RetryableTopic` dead-letter topic,
fingerprints the failure from its stack trace, checks that trace against the
service's own pod logs, and writes an advisory casebook.

Two of the plan's original non-goals now have a narrow, opt-in exception, and
both are off by default:

- **"No remediation"** -- `auto_replay.py` may call `queue_for_replay` on a
  high-confidence redrive finding (point 8 below).
- **"No source-code analysis"** -- the replay precheck reads `release` in
  Bitbucket to answer whether the code at the failure site changed and whether
  that change is running (point 9, and section 4.4.1 in full). It is
  **read-only**, and it produces a *deployment* verdict, never a diagnosis: no
  source is read into an LLM prompt, and nothing in it explains why a bug
  happened.

Everything else still holds -- no writes to any upstream system beyond that one
replay call, and no database access.

It shares this system's log pipeline, storage abstraction, consumer
scaffolding and confidence policy. It shares neither `MessagePayload`, the
rejection casebook schema, `rules.csv`, nor the runbook key space.

```
dlt_consumer.py  -> POST /fetch-dlt-logs  -> dlt-analysis-queue
                 -> dlt_analysis_consumer.py -> POST /analyze-dlt -> casebook

  fetch-dlt-logs:  parse headers -> classify -> fingerprint -> persist evidence
                   -> fetch pod logs -> capture running version -> queue
  analyze-dlt:     corroborate -> group/reuse -> finding -> CODE CHECK
                   -> replay gate -> park -> casebook
```

Ten things are worth knowing without reading the whole plan:

1. **The root cause is the last `Caused by:`, never the headers.**
   `kafka_exception-cause-fqcn` carries a Spring/JDK wrapper that is identical
   for every failure in every consumer in the organisation.

2. **The log window is anchored on `retry_topic-backoff-timestamp`**, not
   `kafka_original-timestamp`. In both real samples those are 43 hours apart,
   so the wrong anchor searches a stale window and finds nothing. The header is
   hex-encoded epoch millis, and decodes to the same instant the
   `TimestampedException` in the trace names.

3. **A cached recommendation is never served blind.** Logs are fetched and
   corroborated on every message; only the LLM call is skipped. That keeps the
   mis-cast detector -- the system's highest-value output -- live on every
   occurrence.

4. **The refId comes from the Kafka record key first.** The record is keyed on
   it, and the key survives a payload we cannot deserialise -- which is exactly
   the case the DLT adapter exists to keep alive. Four layers are tried (key,
   configured path, path registered for the payload's `__TypeId__`, bounded
   search) and the casebook records which one answered, because a value that
   fell through to the search is a guess that landed and one read off the key
   is not. A key/payload disagreement is surfaced as an evidence gap, never
   silently resolved.

5. **`event_id` on the payload is not the `refId`.** They are different UUIDs.
   This project's own vocabulary calls refId "the event id", so the field
   literally named `event_id` is the one you would reach for -- and it fails as
   an empty log window rather than an error. It is denylisted, along with
   `candidateRefId`, which belongs to a different enrolment entirely.

6. **The reason-code catalog can move a case out of the expensive lane.**
   `BusinessReasonCode implements IRejectCode`, so all 760 published reject
   codes can arrive inside a `BusinessException` -- and 198 are declared
   `TECHNICAL_EXCEPTION` at source. Without the catalog,
   `BusinessException: [KAFKA_PRODUCER_EXCEPTION]` reads as a business failure
   and costs an LLM call to reach the answer "redrive once the broker
   recovers". `registry.class_for` moves it to Class C, where the canned
   treatment already says that. The override is one-directional: A to C only,
   never the reverse and never to B, since a code defect is identified by its
   exception type rather than by a reject code.

7. **Nothing is enabled by default.** `DLT_ENABLED=false` keeps the consumers
   out of `start.py`.

8. **Auto-replay is opt-in and gated on the final, ceiling-capped confidence.**
   `src/dlt/auto_replay.py` lets `/analyze-dlt` call `queue_for_replay` (the
   same tool the rejection flow's synthesis agent uses) when a finding's
   action is `REDRIVE_AFTER_RECOVERY` and its confidence clears
   `DLT_REPLAY_CONFIDENCE_THRESHOLD` (default 0.55) -- off by default via
   `DLT_AUTO_REPLAY_ENABLED`. Canned Class B/C/U findings never qualify:
   `canned.py` attaches no confidence to them ("no model produced this"), so a
   missing score never reads as a passing one. In practice this fires on the
   mis-cast path -- corroboration came back `CONTRADICTED`, the LLM concluded
   the declared exception wasn't the real story -- and 0.55 sits just under
   the 0.6 `CONTRADICTED` ceiling on purpose, since a higher default would
   make the feature permanently inert. `queue_for_replay`'s own
   `ENABLE_AUTO_REPLAY` switch still governs what happens once called:
   straight to OIS, or queued for human approval via `approve_replays.py`.

9. **The DLT casebook has its own schema version**, currently `"1.1"`
   (`DLT_CASEBOOK_SCHEMA_VERSION`), independent of the rejection casebook's --
   different schema, different lifecycle. `1.0` -> `1.1` added the
   `code_check` and `parked` blocks.

10. **The replay precheck answers deployment, never relevance.** Before a
   packet is replayed, `code_check.py` asks whether the code at the failure
   site changed since it failed, and whether that change is running. "This
   commit is running" is not "this commit fixes your bug" -- relevance comes
   only from the frame-to-file mapping, so the casebook keeps `code_check`
   separate from `finding` and a reader can disagree with either. Section
   4.4.1 has the whole thing.

Operator entry points:

```bash
python -m src.tools.dlt_report --top          # what is failing most
python -m src.tools.dlt_sample --analyze <d>  # corpus measurements (Phase 0)
python -m src.tools.parse_reason_codes        # regenerate reason_codes.csv
```

### 4.4.1 Replay precheck (DLT_PLAN.md section 14, phases C0-C8)

`auto_replay.decide()` used to gate a replay on the finding alone -- action,
confidence, refId. Nothing in that decision knew whether the bug had been
fixed last Tuesday, or whether the fix had reached the pods. So a replay was a
guess, and a packet whose fix had not shipped simply dead-lettered again.

**The version number is the join key.** ~95% of changes bump it in `pom.xml`
(or the service's equivalent), the image tag carries the same value, and the
pod's image tag is readable from Kubernetes. That chain is what removes the
need for a git SHA, and with it Gitea, ArgoCD, Harbor digests and any change
to the Jenkins pipeline.

**Gitea is deliberately not used.** It holds the *desired* state. A replay
executes against whatever the pods are running now, so a manifest updated but
not yet synced by ArgoCD would report a version that is not running --
precisely the wrong answer. The pod's image tag is the only authority, and it
doubles as the sync signal: if the pod is on the new version, it synced.

```
fast lane     parse -> classify -> fingerprint -> persist -> fetch logs
                    -> capture running version            (deployed.json)

analysis lane corroborate -> group/reuse -> finding
                    -> CODE CHECK -> replay gate -> park  (casebook)

offline       release_parked_replays -> queue_for_replay -> pending_replays
```

Four verdicts, written into every casebook's `code_check` block -- always
present, and `UNKNOWN` when the feature is off, so "we did not look" and "we
looked and found nothing" never read alike:

| Verdict | Condition | Consequence, with C6 on |
|---|---|---|
| `NO_CHANGE` | Nothing on `release` has touched the failure site since the packet failed | Replay withheld -- it would reproduce the same dead letter |
| `NOT_DEPLOYED` | A change exists; the version carrying it is not running | Replay withheld, and the packet **parked** until the pods reach that version |
| `FIX_DEPLOYED` | The running build is at or beyond the version carrying the change | No effect -- the verdict is a veto only |
| `UNKNOWN` | Frame unmappable, repo unreachable, version unparseable, no baseline | No effect, ever |

`NOT_DEPLOYED` is the operative one: it turns "replay and see" into "replay
after the next deploy", which is a scheduling decision the system makes and
acts on by itself.

**The modules.**

| Phase | Module | Job |
|---|---|---|
| C1 | `dlt/deployed.py` | Reads `status.container_statuses[].image` off the service's pods. Sidecars excluded via `K8S_SIDECAR_DENYLIST`. Reports the *set* of versions and refuses to name a winner mid-rollout. |
| C2 | `dlt/stacktrace.py` | `FrameLocation` keeps the file and line the parser already captured and discarded -- beside the fingerprint, never inside it. |
| C3 | `dlt/versions.py` | Parses and orders `1.0.0-release.42`, `1.0.1-SNAPSHOT`, `1.0.0`. Unparseable means `None`, and `None` propagates through every comparison. |
| C4 | `dlt/bitbucket.py` | Read-only, both API flavours. Four calls: commits touching a path, a commit's changed paths, a file at a ref, a repository listing. |
| C5 | `dlt/code_check.py` | Composes the above into the verdict. |
| C6 | `dlt/auto_replay.py` | The veto, in `decide()`, placed last. |
| C7 | `dlt/parked.py` + `tools/release_parked_replays.py` | The waiting queue, and what comes back for it. |
| C8 | `tools/dlt_report.py` | `--parked`, `--code-check`, `--code-check-accuracy`. |

**Three asymmetries, all pointing the same way.** A wrong `FIX_DEPLOYED`
causes a replay that fails again; a wrong `NOT_DEPLOYED` only delays one. So
the *highest* candidate version is required (several commits touched the site
and we cannot tell which is the fix), the *lowest* running version is compared
(a replay may land on any pod mid-rollout), and a positive verdict needs a
recorded baseline while a negative one does not.

**`None` and `[]` mean different things**, and the difference decides a
replay. `[]` from `bitbucket.commits_touching` is "the server answered, and
nothing has touched this file", which becomes `NO_CHANGE` and a withheld
replay. `None` is "we could not look", which becomes `UNKNOWN`. Collapsing
them would let a Bitbucket outage read as "the code definitely has not
changed" and stop every replay in the system on no evidence at all -- the same
distinction `FetchResult.ok` and `Corroboration.could_not_look` already make
in the log lane.

**Six traps** are documented in DLT_PLAN.md 14.4, numbered T5-T10 to continue
that document's existing series. The two most likely to bite a future editor:
a Maven pom declares `<parent><version>` *before* its own, so the first
`<version>` tag is the parent's (T5); and `"1.0.10" < "1.0.9"` is true as
strings and wrong as versions, which is why `versions.py` is its own module
with a brute-forced total-order test (T7).

**Three flags, and none of them is the same switch.**

| Flag | Default | What it decides |
|---|---|---|
| `DLT_CODE_CHECK_ENABLED` | `false` | Whether the lookup happens and a verdict is recorded. Changes no behaviour. |
| `DLT_CODE_CHECK_GATES_REPLAY` | `false` | Whether the verdict may withhold a replay. |
| `DLT_CODE_CHECK_PARK_ENABLED` | `false` | Whether a withheld `NOT_DEPLOYED` packet is parked for later. |

Run the first alone until `dlt_report --code-check-accuracy` shows the
verdicts are right. That report joins each verdict to whether the packet
dead-lettered **again** after its replay -- the only outcome this system can
observe by itself, and the reason it is worth running *before* the gate goes
on, while replays still fire regardless of the verdict.

Parking cannot become a second replay path: a packet parks only when
`auto_replay.decide` would have said yes *without* the veto, so with
`DLT_AUTO_REPLAY_ENABLED` off nothing parks. And releasing still calls
`queue_for_replay`, so `ENABLE_AUTO_REPLAY` still decides whether the packet
reaches OIS or lands in `pending_replays` for a human.

**Status.** Phases C0-C8 are merged and unit-tested against fixtures.
**C0 has never been run against a real Bitbucket or cluster**, so DLT_PLAN.md
14.3's five findings are all open. The code is on `main` but **inert**:
`BITBUCKET_BASE_URL` is empty and all three flags default `false`, so every
verdict reads `UNKNOWN` and nothing about the DLT lane behaves differently
from before it existed. **C0 is a gate on configuring it, not on merging it**
-- `src/dlt/bitbucket.py`'s API flavour, path resolution and version-file
handling all depend on answers nobody has yet. C1 is the exception worth keeping
regardless: it closes DLT_PLAN.md Open Question 3 and mitigates Risk R4 on its
own, and it is deliberately not behind a feature flag.

**Status (the DLT lane as a whole).** Phases 1-9 are implemented and
unit-tested against fixtures; Phase 0 -- the corpus capture and its
measurements -- has never been run against a real broker or cluster. Whether
`enu-biometric` pod log lines actually carry `refId` remains a hard gate on
the log lane being useful at all.

---

## 5. Known Gaps & Deviations

This section records where the running code diverges from the design intent above.
It is maintained deliberately so the document stays a truthful source of truth.

**Update 2026-09-01:** The DLT lane gained a **replay precheck** (section
4.4.1; `DLT_PLAN.md` section 14, phases C0-C8). It decides, before a packet is
replayed, whether the code running in the pods can handle it, by joining the
stack trace to `release` in Bitbucket through the version number. Read-only,
three flags, all defaulting off.

Open items, in the order they should be closed:

| # | Item | Why it matters |
|---|------|----------------|
| 1 | **C0 has never been run.** `DLT_PLAN.md` 14.3's five findings are all `*pending*` | Do not set `BITBUCKET_BASE_URL` until they are filled in: `src/dlt/bitbucket.py`'s API flavour, path resolution and version-file handling all depend on the answers. Run `python -m src.tools.code_check_probe --all --repo <PROJECT>/<REPO>`. |
| 2 | **Does the replay even land on this service?** (`DLT_PLAN.md` 14.5 Q3) | `queue_for_replay` posts to OIS, which re-drives the packet from a stage this system does not choose. If it re-enters *upstream* of the failing service, the running version that matters belongs to a different service than the pods `deployed.py` reads -- which would invalidate the comparison rather than merely weaken it. **Settle this before enabling `DLT_CODE_CHECK_GATES_REPLAY`.** |
| 3 | **The verdicts have not been validated against reality** | `dlt_report --code-check-accuracy` exists precisely to produce that evidence, and it needs replays that actually fired. Run C5 alone (`DLT_CODE_CHECK_ENABLED=true`, gate off) for at least two weeks first. |
| 4 | **Is the 5% uniform?** (Trap T9) | A fix merged with no version bump yields a false `FIX_DEPLOYED`. Mitigated by requiring the version to be *strictly ahead*, but if one team never bumps versions the error rate for their repos is 100%, not 5%. C0's Q4 sample must be stratified by repo, not pooled. |
| 5 | **One service per release worker** | `release_parked_replays.py` reads one app's version. A parked entry records its repository but nothing maps that back to a Kubernetes app, so a deployment with several DLT-producing services must run it once per app. |

Deferred deliberately: letting `FIX_DEPLOYED` *enable* a replay the existing
gate declined. That is what would finally make **Class B** replayable -- the
NPEs and cast failures that today get a canned `NEEDS_MANUAL_REVIEW` and never
replay at all -- and it is the one change that lets this feature *cause*
replays rather than only withhold them. `DLT_PLAN.md` section 14 lists its
preconditions, including at least 30 hand-checked `FIX_DEPLOYED` verdicts.

**Update 2026-08-20:** DLT gained an opt-in auto-replay path
(`src/dlt/auto_replay.py`, section 4.4 point 8; `DLT_AUTO_REPLAY_ENABLED`,
default `false`). One open item: `queue_for_replay`'s `idType`, `category`,
`priority` and `fromSedaStart` arguments are placeholders for a DLT-originated
redrive -- the rejection flow always calls the tool with `id=eventId`, and a
DLT case has no eventId, only `refId`. These have not been confirmed against
the live OIS `/forceReplay` contract; treat `DLT_AUTO_REPLAY_ENABLED=true`
together with `ENABLE_AUTO_REPLAY=true` as unverified until they are.

**Update 2026-08-17:** Log fetching and LLM analysis are decoupled into two
Kafka topics, two consumer processes, and two API routes (section 3.11), so
LLM backlog can no longer stall log collection. No catalog of remaining gaps
from this change -- the split reuses the existing checkpointer, storage
layer, and idempotency guards unmodified rather than introducing new ones.

**Update 2026-08-15:** Second full audit completed and Phases A-F are now implemented. 

The Phase 0 (correctness-breaking, P0), Phase 1 (reliability/operability, P1),
and Phase 2 (optimization, P2) items from the past remediation plans
have all been implemented, covered by
`tests/test_phase0_fixes.py`, `tests/test_phase1_fixes.py`, and
`tests/test_phase2_fixes.py` respectively -- with one deliberate exception (2.9,
below). What remains:

| # | Area | Gap |
|---|------|-----|
| 1 | `rules.csv` data quality | The checked-in `rules.csv` parses as a single garbled column (416 rows, all under a lone `rule_id` header) -- almost certainly an export with unescaped commas/newlines inside `rule_data`'s JSON. `check_drift.py` now detects and reports this distinctly from a genuine schema change, but the file itself still needs a proper re-export from the source DB; no code change can fix corrupted source data. |
| 2 | Template catalog not yet rebuilt | `build_catalog.py` no longer inherits the Drain3 cross-flow leak (fixed at the source in `reducer.cluster_logs`), and now warns if the boilerplate share of a build is implausibly high, but this requires live ES access to real event IDs to actually run -- no catalog has been (re)built under the fixed pipeline yet. |
| 3 | Rate limiter eviction strategy (Phase 2, 2.9) | `routes.py`'s in-memory rate limiter still scans all tracked IPs with a `max()` per entry once past 1000 entries. Left as-is deliberately -- the remediation plan itself notes this is cheap at current request volume, and recommends revisiting with a per-IP `deque` only if that volume grows; not a currently-observable problem. |


