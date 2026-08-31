 # Agentic Resident CRM: Dead-Letter Topic (DLT) Analysis -- Engineering Design

Design date: 2026-08-18. Last revised 2026-08-20 against a second real sample.
Status: **Phases 1-9 implemented; Phase 0 outstanding.**
The code is complete and unit-tested against fixtures, but has never run
against a real broker, cluster or registry. Phase 0 (the corpus capture and
its five measurements) still has to be run from a host with Kafka access, and
its item 5 -- confirming `enu-biometric` pod log lines carry `refId` -- remains
a hard gate on the log lane being useful at all.

The 2026-08-20 sample resolved Open Question 1 (the record key carries the
refId), retired the single-payload-schema assumption, and added two traps to
section 3: the payload's `event_id` is a different UUID from its `refId`, and
its `eventTimestamp` is local time. See 3.3.

**Nothing is enabled by default.** `DLT_ENABLED=false` keeps the consumers out
of `start.py`, and the rejection pipeline is untouched.

**How to use this document.** Sections 1-9 are the design. Section 10 is the
implementation plan, broken into self-contained phases. Each phase lists the
exact files it touches, the config it adds, its tests, its exit criteria, and
what is explicitly out of scope. Every phase leaves the test suite green, so
work can stop after any phase and resume later -- or in a fresh session --
without carrying context forward.

**Relationship to the rejection pipeline.** This is a **parallel flow**, not an
extension of the rejection flow. It shares the log pipeline
(`src/log_pipeline/`), the storage abstraction (`src/storage/`), the consumer
scaffolding (`src/utils/kafkaConsumer.py`), and the confidence policy
(`src/models/synthesis.py`). It does **not** share `MessagePayload`, the
rejection casebook schema, `rules.csv`, or the runbook key space. A rejection
and a stuck packet are different problems and are modelled separately.

---

## 1. Problem statement

Packets that fail processing are retried by Spring Kafka's `@RetryableTopic`
machinery and, after the configured attempts are exhausted, published to a
dead-letter topic. Today the triage process is manual: a developer opens the
DLT in Kafka UI, reads `kafka_exception-stacktrace` from the message headers,
and reasons about what went wrong from experience.

The volume is ~2,000 messages/day. No one reads 2,000 stack traces.

This system consumes that DLT, extracts and normalises the failure, classifies
it, corroborates the declared exception against the service's own logs, and
writes a casebook with a root-cause narrative, a recommendation, and a
confidence score. **It does not fix anything.** No replay, no redrive, no
mutation of any upstream system. The output is advisory and, for now, lands
only in casebook storage.

### 1.1 Why the stack trace alone is insufficient

Two reasons, and the second is the one that justifies the whole log lane:

1. **The trace names the failure site, not the cause.** `UidOriginTracker data
   not found` tells you a row was absent. It does not tell you whether the row
   was never written, written late, written under a different key, or deleted.
   With no source access and no database access (Section 2), the ceiling on
   this is a *per-code* answer, not a per-packet one. That is accepted --
   see 9.1.

2. **The declared exception may be a lie.** Application code catches a
   technical fault and rethrows it as a business exception. When that happens
   the trace confidently reports the wrong root cause, and every downstream
   consumer of that trace -- human or machine -- inherits the error. Logs from
   the same pod at the same instant are the only available check. **Surfacing
   that discrepancy is the highest-value output of this system**, because it is
   the one thing the developer reading Kafka UI structurally cannot see.

---

## 2. Non-goals

- **No remediation, with one narrow, opt-in exception (2026-08-20).**
  `src/dlt/auto_replay.py` lets `/analyze-dlt` call `queue_for_replay` on a
  finding whose action is `REDRIVE_AFTER_RECOVERY` and whose confidence
  clears `DLT_REPLAY_CONFIDENCE_THRESHOLD` -- off by default
  (`DLT_AUTO_REPLAY_ENABLED=false`). Everything else stated in this section
  still holds: no source access, no database access, no writes to any
  upstream service beyond that one tool call. See section 5.9.
- **No source-code analysis.** `com.uidai.enu.biometric` is not accessible.
  Class B failures (Section 4) are enriched and routed, never diagnosed.
- **No database access.** We cannot confirm why a row is missing, only that the
  code said it was.
- **No production routing in v1.** Output goes to casebook storage. Jira/Slack/
  email integration is out of scope.
- **No automatic serving of unreviewed recommendations.** Until a human review
  mechanism exists, every recommendation is a draft (Section 7.4).
- **No multi-schema payload support in v1.** One payload schema is assumed;
  the extractor is built to be configurable so adding a second is config, not
  code (Section 5.3).

---

## 3. Established facts

These come from two real DLT samples and from operator answers. Everything in
the design rests on them; if one turns out to be wrong, the phase that depends
on it is where it will surface.

The second sample (2026-08-20, `tests/fixtures/dlt/reference_abis_mw_response.json`)
is the first with a payload attached, and it moved three rows in this table.
They are marked below.

| Fact | Value | Source |
|---|---|---|
| DLT producer | Spring Kafka `DeadLetterPublishingRecoverer` + `@RetryableTopic` | header shape |
| Topics consumed | **At least two.** `ENU.UPDATE.CHECKER.COMPLETION.V1` (group `enu-biodedup-cg`) and `ENU.MWARE.DEDUPE.PROCESS.COMPLETION.V1` (group `enu-biodedup-abismw-cg`) | both samples |
| Service | `enu-biometric`, namespace `ankalan` | operator |
| Retry-topic consumers | Same pods as the original consumer | operator |
| Log correlation id | `refId` (the same identifier this project already calls `event_id` -- but **not** the payload field literally named `event_id`, see 3.3) | operator, sample 2 |
| refId location | **The Kafka record key**, and again in the payload | sample 2 |
| Payload schema | **Two confirmed**, named by `__TypeId__`: `com.uidai.enu.common.model.EventMessage` and `in.gov.uidai.uidabismiddlewaresb.kafka.model.EnrolmentEventResponse`. The single-schema assumption is retired | sample 2 |
| Registry | **760 published reject codes**, from the `BusinessReasonCode` Java source: code -> description + declared category. 489 `BUSINESS_VALIDATION_ERROR`, 198 `TECHNICAL_EXCEPTION`, 7 `BUSINESS_EXCEPTION`, 56 inferred, 10 uncategorised | source drop, 2026-08-20 |
| Volume | ~2,000 messages/day | operator |
| Source access | None | operator |
| Database access | None | operator |
| Output destination | Casebook storage only; testing phase | operator |
| Confidence score | Required, same shape as the rejection pipeline | operator |

### 3.1 The header contract

From the reference sample. Header names are Spring constants and are stable.

| Header | Example | Use |
|---|---|---|
| `kafka_original-topic` | `ENU.UPDATE.CHECKER.COMPLETION.V1` | case id, routing |
| `kafka_original-partition` | `63` | case id |
| `kafka_original-offset` | `3352` | case id, idempotency |
| `kafka_original-timestamp` | `1786864805192` (epoch ms) | original produce time |
| `kafka_dlt-original-consumer-group` | `enu-biodedup-cg` | service identity |
| `kafka_exception-fqcn` | `ListenerExecutionFailedException` | **ignore** -- Spring wrapper |
| `kafka_exception-cause-fqcn` | `java.lang.RuntimeException` | **ignore** -- see 3.2 |
| `kafka_exception-message` | `Listener failed; ...BusinessException: [CODE] ...` | fallback root extraction |
| `kafka_exception-stacktrace` | full `Caused by:` chain | **primary source** |
| `retry_topic-attempts` | `5` | attempt count |
| `retry_topic-original-timestamp` | `01A009712548` (hex epoch ms) | equals `kafka_original-timestamp` |
| `retry_topic-backoff-timestamp` | `01A012AB41BF` (hex epoch ms) | **last attempt time -- log window anchor** |
| `__TypeId__` | `com.uidai.enu.common.model.EventMessage` | payload schema selection |
| `event-source` | `scanner` | ignored (operator: meaningless) |

**The `retry_topic-*` timestamps are hex-encoded epoch milliseconds.** Verified
against the sample: `int("01A009712548", 16) == 1786864805192`, byte-identical
to `kafka_original-timestamp`, and `int("01A012AB41BF", 16)` decodes to
`2026-08-18T02:20:08.511Z`, which matches the `TimestampedException` timestamp
embedded in the trace text to the millisecond.

Re-verified on sample 2, on a different topic: `int("01A0192B922D", 16)` is
`2026-08-19T08:38:01.005Z`, matching that trace's `TimestampedException` to the
millisecond, and sitting 43.0 hours after its `kafka_original-timestamp`. Trap
2 is not an artefact of one sample.

### 3.3 The record key, and the identifier that looks like one

Sample 2 settles Open Question 1 and adds a trap of its own.

**The DLT record is keyed on the `refId`.** `c5d21184-08f4-4c32-9e5e-5c108c33eb14`
appears as the Kafka message key and again in the payload at
`abisMWResponseNewSeda.refId` (and, duplicated, at
`abisMWResponseNewSeda.abisResponses.referenceId`). The key is therefore the
primary source and the payload the corroborating one, because the key survives
a payload we cannot deserialise -- the exact case `DltAdapter` exists to keep
alive. Before this, an undecodable payload cost the refId, the logs, and any
possibility of corroboration, on a case whose stacktrace was perfectly intact.

**Trap 3: the payload's `event_id` is not the `refId`.** The same payload
carries a top-level `event_id` of `b733ab61-78c4-4aa9-b959-7216435c2544` -- a
different UUID. This document has called refId "the same identifier this
project already calls `event_id`" since section 3, so the field literally named
`event_id` is precisely the one a reader would reach for. It correlates to
nothing the service logs, and the failure mode is an empty log window, not an
error. `_DENY_KEYS` in `src/dlt/payload.py` makes it unconfigurable.

A third identifier class on the same payload: `candidateRefId` values under
`abisResponses.abisResponse[].candidates.matchedCandidate[]`. Those are the
refIds of *other* enrolments the biometric matcher returned. Correlating on one
would pull an unrelated packet's log lines while missing this packet's
entirely. Also denied.

**Trap 4: the payload's `eventTimestamp` is local time.** Sample 2 reads
`2026-08-17 19:07:47.552` where `kafka_original-timestamp` is `1786973867552` =
`13:37:47.552Z`. The same instant, expressed at +05:30, with no offset written
down. Never a log-window anchor; the headers carry real epoch millis.

### 3.2 Two traps that will silently destroy the system

**Trap 1: fingerprinting on the exception headers.** In the sample,
`kafka_exception-fqcn` is `ListenerExecutionFailedException` and
`kafka_exception-cause-fqcn` is `java.lang.RuntimeException`. Both are Spring/
JDK wrappers that will be *identical for every failure in every Spring Kafka
consumer in the organisation*. Keying on them collapses the entire DLT into one
group. The root cause is the **last** `Caused by:` in the stacktrace text, four
levels down in the sample:

```
ListenerExecutionFailedException
  -> TimestampedException
    -> ListenerExecutionFailedException
      -> RuntimeException
        -> BusinessException: [UID_ORIGIN_TRACKER_DATA_NOT_FOUND]   <-- this one
```

**Trap 2: anchoring the log window on `kafka_original-timestamp`.** In the
sample the original message was produced at `2026-08-16T07:20:05Z` and the
final attempt failed at `2026-08-18T02:20:08Z` -- **43.0 hours apart**. Pod logs
for the original produce time are long gone. The window must be anchored on
`retry_topic-backoff-timestamp`. With the current `K8S_DEFAULT_SINCE_HOURS=2`
default and the wrong anchor, every fetch returns nothing.

---

## 4. Failure taxonomy

Classification is deterministic and happens before any LLM call. It decides
how much effort a message is worth.

| Class | Definition | Treatment | LLM? |
|---|---|---|---|
| **A** | Root exception is a `BusinessException` carrying a `[CODE]` | Registry lookup + log corroboration + recommendation | Yes, on novel fingerprints |
| **B** | Root is a code defect: `NullPointerException`, `IndexOutOfBoundsException`, `ClassCastException`, `NumberFormatException`, ... | Enrich, group, `NEEDS_MANUAL_REVIEW`. No diagnosis attempted -- no source access | No |
| **C** | Root is technical/transient: timeouts, connection resets, serialization, broker faults | Recommendation is always "redrive after the dependency recovers" | No |
| **U** | Unclassifiable -- trace unparseable, truncated, or root unrecognised | `NEEDS_MANUAL_REVIEW`, trace attached verbatim | No |

Class B is deliberately cheap. The operator's instruction was "just forward to
the dev team with whatever info we have." The value added over Kafka UI is not
diagnosis -- it is **aggregation**: this is occurrence 47 of this fingerprint,
first seen 2026-08-12, here are the affected refIds, here is the normalised
frame list. That is worth building and costs no tokens.

Class membership is derived from the root exception FQCN via a configurable
map (`DLT_CLASS_MAP`), defaulting to a table shipped in code. An unrecognised
FQCN is Class U, never silently Class B.

---

## 5. Component design

### 5.1 Stacktrace parsing and fingerprinting

The load-bearing component. Everything else -- caching, grouping, cost control
-- depends on the fingerprint being stable across occurrences and distinct
across genuinely different bugs.

**Parsing.** Split the stacktrace text on `\nCaused by: ` into an ordered chain.
Each link yields an exception FQCN, a message, and a frame list. The last link
is the root. `... N more` markers terminate a link's frames and are discarded.

**Frame normalisation.** A raw frame is
`at com.uidai.enu.biometric.service.impl.BioDeDuplicationServiceImpl.filterCandidatesAndBuildRefIdUidMap(BioDeDuplicationServiceImpl.java:4067)`.
Normalisation applies, in order:

1. **Keep only application frames** -- those whose FQCN starts with a prefix in
   `DLT_APP_PACKAGES` (default `com.uidai.,in.gov.uidai.`). Framework and JDK
   frames (`org.springframework.`, `java.base/`, `jdk.internal.`) are dropped.
   The sample's 4-level chain contains 60+ frames and 9 application ones.
2. **Drop line numbers.** `BioDeDuplicationServiceImpl` is a 4,000+ line class;
   line numbers shift on every release and would fragment a fingerprint that
   should be stable.
3. **Drop synthetic frames.** `GeneratedMethodAccessor781` (the counter varies
   per JVM run), `$$SpringCGLIB$$0`, `<generated>`, `$$Lambda$1234/0x00007f...`.
   All present in the sample. Left in, they guarantee that no two occurrences
   ever fingerprint alike.
4. **Drop exception plumbing** listed in `DLT_BOILERPLATE_FRAMES`. Added
   during Phase 1 after running against the reference sample: the top
   application frame of a `BusinessException` is
   `CommonErrorFactory.instantiateException`, because that factory constructs
   every business exception in the codebase. It is therefore identical across
   all Class A failures -- it contributes nothing to the fingerprint, displaces
   a frame that would, and makes the signature name the factory instead of the
   code that failed. Inferred from one sample; Phase 0's corpus should confirm
   it and reveal any siblings. Setting the variable empty disables the filter.
5. **Truncate to `DLT_FINGERPRINT_FRAMES`** (default 5) from the top.

**Fingerprint.**

```
sha256(root_fqcn + "|" + business_code_or_empty + "|" + "\n".join(normalised_frames))
```

Stored alongside a human-readable `signature` string so an operator can read a
group without decoding a hash.

**Version dimension.** The deployed build is not available in the headers
(Open Question 3). Until it is, the fingerprint carries no version, and a
fingerprint whose underlying bug has been fixed will keep matching. Mitigated
by recording `first_seen`/`last_seen` on the group so a stale group is visible;
see Risk R4.

### 5.2 Log window derivation

```
last_attempt   = hex_epoch_ms(retry_topic-backoff-timestamp)
                 or kafka_original-timestamp if the header is absent/unparseable
window_start   = last_attempt - DLT_LOG_LEAD_SECONDS      (default 300)
window_end     = last_attempt + DLT_LOG_TRAIL_SECONDS     (default 120)
```

The Kubernetes source takes `since_seconds` relative to *now*, so the fetch
passes `TimeWindow(seconds = now - window_start)` and the trailing bound is
applied during filtering. When `now - window_start` exceeds
`DLT_MAX_LOG_AGE_SECONDS` (default 86400) the fetch is skipped entirely and the
case is recorded `UNVERIFIABLE` with a `LOGS_TOO_OLD` gap, rather than burning
a fetch that is certain to return nothing.

Identifier matched against log lines: **`refId`**, not the case id. This
requires a small change to `reduce_logs` -- see 5.5.

### 5.3 Identifier resolution

Four layers, first hit wins (`src/dlt/payload.py:resolve_ref_id`):

1. **The Kafka record key.** Primary, per 3.3. Accepted only if it is shaped
   like an identifier -- a key is whatever the producer chose, and feeding a
   routing token like `ABIS1` to the log query returns nothing, or worse
   another packet's lines.
2. `DLT_REFID_PATH`, a global dotted-path override.
3. The path registered for the payload's `__TypeId__`, from
   `REFID_PATHS_BY_TYPE` in `src/models/dlt_payload_schemas.py` or the
   `DLT_REFID_PATHS_BY_TYPE` env override.
4. A bounded breadth-first search for `DLT_REFID_KEYS`, depth- and node-capped.

On total miss the case is still processed -- header-only. Logs are skipped and
corroboration comes out `UNVERIFIABLE`. Losing the message would be worse than
losing its logs.

**Every result carries the layer it came from** (`ref_id_source` on the
casebook). A refId that fell through to the search is a guess that happened to
land; one read off the record key is the producer's own partitioning key. An
operator asking "why did this case have no logs" needs to tell them apart.

**A key/payload disagreement is surfaced, not resolved.** When both yield an
identifier and they differ, the key wins -- it has no path to misconfigure --
and the case gains a `REFID_KEY_PAYLOAD_MISMATCH` evidence gap. Silently
picking one of two identifiers that should have been equal is how a stale
configured path stays invisible for months.

Adding a third payload schema stays config, not code.

### 5.3.1 The payload as evidence

Until sample 2 the payload was read for one identifier and then discarded. It
is evidence in its own right. Sample 2's trace fails inside
`filterCandidatesAndBuildRefIdUidMap -> getIndexMasterData`; the candidates
that loop iterates are sitting in the payload. `summarise_payload` renders a
bounded description -- request type, response status, per-ABIS candidate counts
-- into the case artifacts and the analyst's evidence block.

Two constraints on this, both load-bearing:

* **Bounded.** A wide ABIS response must not push the stacktrace or the logs
  out of the context window. Candidates are capped, and an unmodelled payload
  gets a key listing rather than a verbatim dump -- which is a redaction
  surface as much as a budget one.
* **The narrative it feeds is reused.** Under the reuse policy a Class A
  finding is stored per fingerprint and re-served to every later packet with
  the same signature. A narrative naming *this* packet's candidate ids would
  later be shown to an operator looking at a different packet, where it is
  simply false. `DltInvestigatorAgent.md` states this as a hard limit: describe
  the shape of the input, never its values.

### 5.3.2 The reason-code catalog

`BusinessReasonCode implements IRejectCode`. Every one of the 760 published
codes can therefore arrive inside a `BusinessException` -- and **198 of them are
declared `TECHNICAL_EXCEPTION` at source**.

That breaks the assumption section 4 was built on. On the exception type alone,
`BusinessException: [KAFKA_PRODUCER_EXCEPTION]` and
`BusinessException: [INDEX_MASTER_DATA_NOT_FOUND]` are the same shape. Both
classify as A, both go to the analysis lane, and the first comes back with a
business narrative about a Kafka publish error whose entire treatment is
"redrive once the broker recovers". The catalog is what separates them:
`registry.class_for` is passed into `classify()` as its `code_class` hook and
moves such a case to Class C, where the canned treatment already says exactly
that -- with no LLM call.

The override is one-directional by construction. It moves A to C, never the
reverse, and **never to B**: a code defect is identified by its exception type,
never by a reject code, so no data file can route cases into the "no diagnosis
possible" lane.

**Declared vs inferred.** The Java file holds two structures. The
`BusinessReasonCode` enum declares a category per entry. The id-keyed
`bioDedupReasonCodes` map declares none, so the category assigned to those 69
entries is *inferred* from the numeric id range (`17xxx` business, `37xxx`
technical, `12xxx` data access). Every row records which it is, and a canned
finding built on an inference says so and calls itself provisional. The
inference is corroborated, not proven: three codes appear in both structures,
and all three are declared `TECHNICAL_EXCEPTION` in the enum, matching what the
`37xxx` rule predicts.

Ten `23xxx` codes are left uncategorised on purpose. They sit under no section
header and mix business rejects with technical processing failures; since
`classify()` acts on this file, a guess there is worse than silence.

`reason_codes.csv` is the artifact the running system reads and the thing that
is committed. The Java source is its *input*, not a runtime dependency, and is
not kept in the repo -- drop the next version in as `reason_codes.txt` and run
`python -m src.tools.parse_reason_codes`. When the source is present a test
asserts the stored CSV still matches it, so drift fails the suite; when it is
absent that test skips and the catalog tests carry on against the CSV.

### 5.4 Case identity and idempotency

```
case_id = f"dlt-{original_topic}-{partition}-{offset}"
```

`(topic, partition, offset)` is the only naturally unique, naturally idempotent
key available. It survives redrive: if a developer replays from the DLT after a
fix, the same message produces the same case id and is skipped by the existing
terminal-status check. `refId` is *not* used as the case id -- one packet can
fail at several stages and produce several distinct DLT messages.

The generated id must satisfy `EVENT_ID_PATTERN`
(`^[A-Za-z0-9_.:-]{1,128}$`) in `src/models/schemas.py`. Topic names contain
dots and uppercase, both permitted. Any character outside the class is replaced
with `-`, and the id is truncated to 128 with a hash suffix if a topic name is
pathologically long.

### 5.5 Reuse of the log pipeline

`reduce_logs(event_id, extra_identifiers)` currently uses one identifier for
both *searching* and *persisting*. The DLT path needs them separate: search on
`refId`, persist under `case_id`.

Minimal change: add an optional `storage_key: Optional[str] = None` parameter,
defaulting to `event_id`, used only for artifact persistence. Every existing
caller is unaffected. Also add an optional `window: Optional[TimeWindow] = None`
so the DLT path can supply the derived window instead of
`K8S_DEFAULT_SINCE_HOURS`.

`K8S_SERVICE_MAP` gets one entry mapping consumer group `enu-biodedup-cg` to
namespace `ankalan` / app `enu-biometric`. Because the retry-topic consumers
run in the same pods (Section 3), no separate discovery is needed.

### 5.6 Corroboration -- deterministic, before the LLM

Takes the parsed trace and the fetched logs and returns one of:

| Verdict | Condition |
|---|---|
| `CORROBORATED` | An ERROR line for this `refId` within the window names the same root exception FQCN (or its business code) |
| `CONTRADICTED` | An ERROR line within the window names a *different* exception FQCN, and the trace's declared root does not appear anywhere in the window |
| `PARTIAL` | The declared root appears, but so do unexplained ERROR lines from other FQCNs |
| `UNVERIFIABLE` | No logs fetched, `refId` unknown, window too old, or no ERROR lines matched |

`CONTRADICTED` and `PARTIAL` are the mis-cast detector. They do not assert a
verdict on their own -- they escalate to the LLM lane with the discrepancy as
the framing question, and the resulting casebook leads with it.

**No real example of a mis-cast case exists yet** (Open Question 2). The check
is therefore built to *surface* discrepancies for a human rather than to
adjudicate them, and its thresholds are configuration. Phase 0 should try to
find one; if it cannot, the check ships conservative and is tightened later.

### 5.7 Group store and recommendation reuse

A **group** is the durable record for one fingerprint:

```
{
  "fingerprint": "<sha256>",
  "signature": "BusinessException[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] @ BioDataBaseHelperServiceImpl.getUidOriginTrackerData",
  "failure_class": "A",
  "business_code": "UID_ORIGIN_TRACKER_DATA_NOT_FOUND",
  "first_seen": "...", "last_seen": "...", "occurrence_count": 47,
  "members": ["dlt-...-63-3352", ...],          // capped, see below
  "recommendation": { ... } | null,
  "recommendation_state": "none|draft|final",
  "corroboration_history": {"CORROBORATED": 44, "PARTIAL": 2, "CONTRADICTED": 1}
}
```

`members` is capped at `DLT_GROUP_MEMBER_CAP` (default 200) keeping the newest;
the full count lives in `occurrence_count`. At 2,000 messages/day an uncapped
list would grow without bound.

**Reuse policy -- the operative decision.** Never serve a cached recommendation
blind. Every message gets its logs fetched and its corroboration run; only the
*LLM* is skipped:

| Situation | Action |
|---|---|
| Novel fingerprint | Full LLM lane; write group with `recommendation_state: draft` |
| Known fingerprint, `CORROBORATED` | Serve the group's recommendation. No LLM call. Confidence carried from the group, minus a reuse decay |
| Known fingerprint, `PARTIAL`/`CONTRADICTED` | Full LLM lane. The discrepancy leads the casebook. Group's `corroboration_history` updated |
| Known fingerprint, `UNVERIFIABLE` | Serve the recommendation, capped at `DLT_UNVERIFIED_CONFIDENCE_CEILING` |
| Class B/C, any | Never calls the LLM; the group's canned treatment applies |

This is what makes 2,000/day affordable: logs are fetched in the fast stage and
are cheap; the LLM runs only on novel fingerprints and on discrepancies.
Expected steady-state LLM volume is tens of runs per day, not thousands. It
also keeps the mis-cast detector live on **every** message, which blind cache
reuse would have disabled.

### 5.8 Confidence policy

Reuses `apply_confidence_policy` in `src/models/synthesis.py`, extended with
DLT-specific ceilings applied *after* the model's own score:

| Condition | Ceiling |
|---|---|
| Class B (no source access) | Hard `NEEDS_MANUAL_REVIEW`, confidence never above `DLT_CLASS_B_CEILING` (0.3) |
| Class U | Same as Class B |
| `UNVERIFIABLE` corroboration | `DLT_UNVERIFIED_CONFIDENCE_CEILING` (0.5) |
| `CONTRADICTED` | `DLT_CONTRADICTED_CEILING` (0.6) -- we know the trace is wrong, not what is right |
| Registry miss on a Class A code | `DLT_REGISTRY_MISS_CEILING` (0.5) |
| Evidence gap banner present | existing `SYNTHESIS_GAP_CONFIDENCE_CEILING` (0.6) |
| Reused recommendation | group confidence x `DLT_REUSE_DECAY` (0.95) |

Ceilings compose by taking the minimum.

### 5.9 Auto-replay (2026-08-20)

`src/dlt/auto_replay.py`. The one deliberate exception to section 2's
no-remediation stance, added because a real remediation mechanism exists
downstream that this system had no way to know about: a Temporal workflow
that repairs the DB inconsistency behind codes like
`INDEX_MASTER_DATA_NOT_FOUND` when it sees them on the retry/DLT topics, after
which the packet has a real chance of succeeding on redrive.

Runs after `apply_dlt_confidence_policy`, in `analyze_dlt`, on the **final**
finding -- ceilings and reuse decay already applied. Every condition is
independently sufficient to withhold replay, and every one is named in the
casebook's `replay.reason`, whether or not replay was attempted:

1. `DLT_AUTO_REPLAY_ENABLED` must be `true` (default `false`).
2. `finding.action` must be in the replay-worthy set --
   `REDRIVE_AFTER_RECOVERY` by default, via `DLT_REPLAY_ACTIONS`.
   `DATA_FIX_REQUIRED` (Class A's typical action) is deliberately excluded: a
   data-not-found packet's row is still missing after a replay, since
   nothing about the row changed just because time passed.
3. `finding.confidence` must be present and at or above
   `DLT_REPLAY_CONFIDENCE_THRESHOLD` (default 0.55). `canned.py` never
   attaches a confidence to a Class B/C/U finding -- "no model produced
   this" -- so a canned `REDRIVE_AFTER_RECOVERY` (Class C's fixed treatment)
   can never clear this and never auto-replays. Only an LLM-synthesised (or
   group-reused) finding carries a real number.
4. The case must have a `refId` to identify the packet with.

In practice this means the LLM-driven mis-cast path is what actually
qualifies: `DltSynthesisAgent.md` instructs the model to choose
`REDRIVE_AFTER_RECOVERY` specifically when corroboration is `CONTRADICTED` and
the logs point at a transient fault instead of the declared business
exception -- and `CONTRADICTED` is capped at `DLT_CONTRADICTED_CEILING` (0.6)
regardless of what the model reports. The default threshold (0.55) sits just
under that ceiling on purpose: any higher and the feature could never fire at
all, since nothing can exceed a ceiling that has already been applied.

**Two independent switches, not one.** Passing the gate above calls
`queue_for_replay` -- the same tool the rejection flow's synthesis agent
already uses (`src/tools/tool_registry.py`) -- but that tool's own
`ENABLE_AUTO_REPLAY` switch still decides what happens next: `true` posts
straight to the OIS `/forceReplay` endpoint, `false` (the safer default)
appends to `pending_replays.jsonl` for a human to approve via
`approve_replays.py`. `DLT_AUTO_REPLAY_ENABLED=true` with `ENABLE_AUTO_REPLAY`
left off means "let the DLT lane nominate packets, but still make a human
press the button" -- the posture worth reaching for first.

**Not yet confirmed against the live endpoint.** The rejection flow always
calls `queue_for_replay` with `id=eventId`; a DLT case has no eventId, only
`refId`. `idType`/`category`/`priority`/`fromSedaStart` are placeholders
(`DLT_REPLAY_ID_TYPE` etc.), not values confirmed against OIS's actual
contract for a DLT-originated redrive. A `queue_for_replay` failure is
exception-shielded -- it degrades the casebook's `replay` block, never costs
the casebook itself.

---

## 6. Architecture

Mirrors the existing two-stage split, for the same reason: a backlog in the LLM
stage must never stall log collection while pod logs rotate away.

```mermaid
flowchart TD
    classDef det fill:#1e40af,stroke:#1e3a8a,color:#ffffff
    classDef llm fill:#7c2d12,stroke:#431407,color:#ffffff
    classDef store fill:#334155,stroke:#0f172a,color:#ffffff

    K1(["Kafka: DLT topic"]) --> DC["dlt_consumer.py<br/>CONSUMER_ROLE=dlt"]
    DC --> PARSE["parse headers + stacktrace<br/>fingerprint, classify"]
    PARSE --> API1["POST /fetch-dlt-logs"]
    API1 --> WIN["derive window from<br/>retry_topic-backoff-timestamp"]
    WIN --> FETCH["reduce_logs, search on refId,<br/>persist under case_id"]
    FETCH --> ST1[("case artifacts<br/>+ status LOGS_FETCHED")]
    ST1 --> K2(["Kafka: dlt-analysis-queue"])

    K2 --> DAC["dlt_analysis_consumer.py<br/>CONSUMER_ROLE=dlt_analysis"]
    DAC --> API2["POST /analyze-dlt"]
    API2 --> CORR["corroborate trace vs logs<br/>deterministic"]
    CORR --> CLS{"class + group lookup"}
    CLS -->|"B / C / U"| CANNED["canned treatment<br/>no LLM"]
    CLS -->|"A, known fp, CORROBORATED"| REUSE["serve group recommendation<br/>no LLM"]
    CLS -->|"A, novel fp"| LLM["Investigator -> Reviewer -> Synthesis"]
    CLS -->|"any, CONTRADICTED/PARTIAL"| LLM
    CANNED --> CB[("casebook + group record")]
    REUSE --> CB
    LLM --> CB

    class PARSE,WIN,FETCH,CORR,CLS det
    class LLM llm
    class ST1,CB store
```

### 6.1 Consumer role seam

`src/utils/kafkaConsumer.py` resolves topic/group/endpoint/timeout/heartbeat
from `CONSUMER_ROLE` at import time and is otherwise role-agnostic. Two things
block reuse as-is:

1. **Headers are discarded.** `_handle_one_message` reads only `msg.value`
   (`kafkaConsumer.py:388`). The DLT flow's primary evidence is in
   `msg.headers`.
2. **Validation and dedupe are `MessagePayload`-shaped.** The DLT payload is an
   `EventMessage` and would fail validation outright; the dedupe key is
   `eventId`, which does not exist here.

The fix is a **message adapter** protocol selected by role. Each adapter
implements:

```python
class MessageAdapter(Protocol):
    def case_id(self, msg) -> str: ...
    def validate(self, msg) -> tuple[bool, Optional[dict], Optional[str]]: ...
    def should_skip(self, case_id: str, parsed: dict) -> Optional[str]: ...
    def request_body(self, msg, parsed: dict) -> dict: ...
```

The existing fast/slow behaviour moves into a `RejectionAdapter` verbatim --
same `MessagePayload` validation, same `packetStatus == REJECTED` filter, same
terminal-casebook dedupe -- so the rejection path is provably unchanged. All
offset tracking, the rebalance listener, the semaphore, heartbeats, and the
shutdown drain are untouched and shared.

---

## 7. Storage layout

Separate root from the rejection casebooks, sharing the `CasebookStorage`
protocol and both backends.

```
<storage_root>/
  dlt_cases/
    dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352/
      casebook.json          # the finding
      status.json            # LOGS_FETCHED -> terminal
      headers.json           # verbatim DLT headers, audit
      trace.txt              # verbatim stacktrace
      parsed_trace.json      # exception chain, frames, fingerprint
      raw_logs.txt           # existing pipeline artifacts
      reduced_logs.txt
  dlt_groups/
    <fingerprint>.json       # the group record from 5.7
```

`headers.json` and `trace.txt` are stored verbatim and **before** analysis, so
a parser bug is always recoverable without re-consuming Kafka. Both pass
through `src/log_pipeline/redaction.py` first -- a stacktrace message can carry
a UID.

### 7.1 Casebook schema

Deliberately not the rejection casebook. Fields:

```
schema_version, case_id, detected_at
source: {original_topic, partition, offset, consumer_group, attempts,
         original_timestamp, last_attempt_timestamp, type_id}
packet: {ref_id, ...any other extracted identifiers}
failure: {class, root_fqcn, business_code, registry_description,
          signature, fingerprint, chain: [...]}
evidence: {corroboration, log_window, gaps: [...], log_artifact_locators}
finding: {narrative, discrepancy | null, recommendation, action}
confidence: {score, ceilings_applied: [...], abstained}
provenance: {source: agent|group_reuse|canned, group_fingerprint,
             prompt_fingerprint, recommendation_state}
replay: {attempted, reason, queued, result}  # 5.9, always present
```

`action` vocabulary for this flow (not the rejection one):
`NEEDS_MANUAL_REVIEW`, `ROUTE_TO_DEV`, `REDRIVE_AFTER_RECOVERY`,
`DATA_FIX_REQUIRED`, `NO_ACTION`.

---

## 8. Configuration reference

All new. Added to `.env.example` in the phase that first reads them.

| Variable | Default | Meaning |
|---|---|---|
| `DLT_CONSUMER_TOPIC_NAME` | `packet-dlt` | Topic the DLT consumer reads |
| `DLT_CONSUMER_GROUP_ID` | `dlt-analysis-group` | Dedicated consumer group |
| `DLT_CONSUMER_ENDPOINT` | `http://localhost:8000/fetch-dlt-logs` | Fast-stage endpoint |
| `DLT_CONSUMER_TIMEOUT_SECONDS` | `90` | Fast-stage HTTP budget |
| `DLT_ANALYSIS_TOPIC_NAME` | `dlt-analysis-queue` | Second-stage topic |
| `DLT_ANALYSIS_GROUP_ID` | `dlt-analysis-slow-group` | Second-stage group |
| `DLT_ANALYSIS_ENDPOINT` | `http://localhost:8000/analyze-dlt` | Second-stage endpoint |
| `DLT_ANALYSIS_TIMEOUT_SECONDS` | `300` | LLM budget |
| `DLT_APP_PACKAGES` | `com.uidai.,in.gov.uidai.` | Frames kept during normalisation |
| `DLT_FINGERPRINT_FRAMES` | `5` | Frames in the fingerprint |
| `DLT_BOILERPLATE_FRAMES` | `in.gov.uidai.common.factory.CommonErrorFactory` | Exception-plumbing frames dropped before fingerprinting (5.1). Empty disables |
| `DLT_CLASS_MAP` | (built-in) | JSON, exception FQCN prefix -> class. **Extends** the built-in map, never replaces it |
| `DLT_BUSINESS_EXCEPTIONS` | `in.gov.uidai.common.exception.BusinessException` | Extra business-exception FQCNs; any `*BusinessException` already qualifies |
| `DLT_REFID_PATH` | (unset) | Dotted path to refId in the payload |
| `DLT_REFID_KEYS` | `refId,ref_id,referenceId` | Recursive-search fallback keys |
| `DLT_REGISTRY_PATH` | `business_errors.csv` | BusinessException registry |
| `DLT_LOG_LEAD_SECONDS` | `300` | Window before last attempt |
| `DLT_LOG_TRAIL_SECONDS` | `120` | Window after last attempt |
| `DLT_MAX_LOG_AGE_SECONDS` | `86400` | Skip the fetch beyond this age |
| `DLT_GROUP_MEMBER_CAP` | `200` | Members retained per group |
| `DLT_REUSE_ENABLED` | `true` | Master switch for recommendation reuse |
| `DLT_CLASS_B_CEILING` | `0.3` | Confidence ceiling, Class B/U |
| `DLT_UNVERIFIED_CONFIDENCE_CEILING` | `0.5` | Ceiling when corroboration is UNVERIFIABLE |
| `DLT_CONTRADICTED_CEILING` | `0.6` | Ceiling when the trace is contradicted |
| `DLT_REGISTRY_MISS_CEILING` | `0.5` | Ceiling when a Class A code is unknown |
| `DLT_REUSE_DECAY` | `0.95` | Multiplier on a reused confidence |
| `DLT_HEALTH_PORT` / `DLT_ANALYSIS_HEALTH_PORT` | (unset) | Per-role health servers |
| `DLT_AUTO_REPLAY_ENABLED` | `false` | Master switch, section 5.9 |
| `DLT_REPLAY_CONFIDENCE_THRESHOLD` | `0.55` | Minimum confidence to auto-replay |
| `DLT_REPLAY_ACTIONS` | `REDRIVE_AFTER_RECOVERY` | Actions that qualify as "replay is the fix" |
| `DLT_REPLAY_ID_TYPE` / `DLT_REPLAY_OPERATOR_NAME` / `DLT_REPLAY_CATEGORY` / `DLT_REPLAY_PRIORITY` / `DLT_REPLAY_FROM_SEDA_START` | see `.env.example` | `queue_for_replay` arguments; unconfirmed against the live OIS contract |

---

## 9. Accepted tradeoffs

1. **Per-code answers, not per-packet.** Without source or database access, two
   packets with the same business code get the same narrative. This is a
   runbook, and it is what the available evidence supports. Stated plainly in
   every casebook so no reader mistakes it for a per-packet diagnosis.
2. **Class B adds aggregation, not diagnosis.** Accepted per operator
   direction.
3. **Line numbers are excluded from the fingerprint.** Two genuinely different
   bugs in the same method will merge. Judged the lesser evil against
   fragmenting every group on each release.
4. **No deploy-version dimension.** See Risk R4.
5. **The corroboration check ships without a validating example.** See Open
   Question 2.

---

## 10. Implementation phases

Each phase is self-contained, leaves the suite green, and can be completed in
one working session. **Do not start a phase before its predecessor's exit
criteria are met.**

Phases 1-3 and 6-7 are pure logic with no I/O and no wiring -- they can be
built and fully tested against fixtures without a cluster, a broker, or the
registry. Nothing touches the running packet path until Phase 4, and no LLM is
called until Phase 8.

### Status

| Phase | State | Notes |
|---|---|---|
| 0 | **Tooling ready, awaiting data** | `src/tools/dlt_sample.py` built and tested; the reference sample is committed as a fixture. Must be run against the live DLT from a host with broker access; results go in Section 11. |
| 1 | **Complete** | `src/dlt/headers.py`, `src/dlt/stacktrace.py`. 39 tests, including both trap regressions. Boilerplate-frame filter added during the phase -- see 5.1 step 4. |
| 2 | **Complete** | `src/dlt/classify.py`, `src/dlt/registry.py`, fixture registry CSV. 36 tests. `dlt_sample.py` gained `--analyze`, which computes the Section 11 measurements from a captured corpus. |
| 3 | **Complete** | `src/dlt/payload.py`, `src/dlt/identity.py`, `src/models/dlt_schemas.py`. 41 tests. Case-id sanitisation asserts the real invariant (no path separator), not the stricter one -- see the note in `test_path_traversal_cannot_survive_sanitisation`. |
| 4 | **Complete** | `src/utils/message_adapters.py`, `src/dlt/case_storage.py`, `src/dlt_consumer.py`; `kafkaConsumer.py` gained the `dlt`/`dlt_analysis` roles and routes through the adapter. 30 tests. **Six existing tests were repointed** from `kc.get_casebook_storage` to `src.storage.factory.get_casebook_storage` -- the dedupe moved into `RejectionAdapter.should_skip`, so the old patch target no longer exists. Behaviour is unchanged; only the injection point moved. |
| 5 | **Complete** | `src/dlt/window.py`, `src/api/dlt_routes.py`; `reduce_logs` gained `storage_key`, `window` and `storage`; DLT analysis-queue producer; DLT metrics. 25 tests. One existing stub in `test_phase_b_fixes.py` gained `**_kw` to absorb the new `storage=` argument. **Still gated on Phase 0 item 5** -- the refId-in-logs check is unverified. |
| 6 | **Complete** | `src/dlt/corroborate.py`. 25 tests. Verdicts stay advisory and matching is deliberately generous (FQCN, simple name, or business code all count) -- a false CONTRADICTED is worse than a missed one. Open Question 2 is still open. |
| 7 | **Complete** | `src/dlt/groups.py`, `src/dlt/reuse.py`. 35 tests, including the cost-model proof: 400 messages across 3 bugs yield 3 groups and 3 LLM calls. Real-corpus numbers still need Phase 0. |
| 8 | **Complete** | `src/dlt/orchestrator.py`, `src/dlt/canned.py`, `src/models/dlt_synthesis.py`, three prompts, `POST /analyze-dlt`, `src/dlt_analysis_consumer.py`. 24 tests with a mocked LLM. Occurrence recording moved ahead of the reuse decision so a canned finding's count includes the case it describes. |
| 9 | **Complete** | `src/tools/dlt_report.py` (`--top`, `--group`, `--case`, `--unreviewed`, `--stats`); DLT metrics in `metrics.py`; all four consumer heartbeats on `/health`. 21 tests. |

---

### Phase 0 -- Sample capture and decision gate

**Goal.** Replace the assumptions in Section 3 with measurements. No production
code.

- **Deliverable:** `tests/fixtures/dlt/` containing 50-100 real DLT messages
  captured verbatim (headers + payload), redacted. Plus a findings section
  appended to this document (Section 11) reporting:
  1. Class distribution -- what share is A / B / C / U?
  2. Distinct fingerprint count over the sample, and the count for the top 5
     fingerprints. Validates the caching premise.
  3. Is `kafka_exception-stacktrace` ever **truncated**? Check for messages
     that do not terminate in a complete frame list.
  4. **The `refId` path inside `EventMessage`** -- exact dotted path.
  5. **Do `enu-biometric` pod log lines carry `refId`?** Pull `kubectl logs`
     for one known failure and confirm the log pattern includes it (MDC). If
     not, the log lane cannot filter and Phase 5 must be redesigned.
  6. Whether any message shows a mis-cast exception (Open Question 2).
  7. Observed lag between `retry_topic-backoff-timestamp` and DLT arrival.
- **Tooling:** `src/tools/dlt_sample.py`, built in this phase. Capture with
  `--limit`/`--redact` (needs broker access); then `--analyze <dir>` computes
  items 1, 2, 3, 4 and 7 above and writes `_analysis.json` (needs no broker,
  so the two halves can run on different hosts). Item 5 is still a manual
  `kubectl logs` check.
- **Exit criteria:** items 1-5 answered and recorded in Section 11. Item 5 is a
  hard gate on Phase 5.
- **Out of scope:** everything else.

---

### Phase 1 -- Header and stacktrace parsing

**Goal.** Turn a raw DLT message into a structured, fingerprinted failure. Pure
functions, no I/O.

- **New:** `src/dlt/__init__.py`; `src/dlt/headers.py` (header extraction, hex
  epoch decoding, `DltHeaders` dataclass); `src/dlt/stacktrace.py` (`Caused by:`
  chain parsing, frame normalisation, fingerprinting).
- **Config:** `DLT_APP_PACKAGES`, `DLT_FINGERPRINT_FRAMES`.
- **Tests:** `tests/test_dlt_stacktrace.py`, using the reference sample in this
  document as a fixture:
  - The root of the reference chain is `BusinessException`, **not**
    `RuntimeException` -- a direct regression guard on Trap 1 (3.2).
  - `int("01A009712548", 16) == 1786864805192`, equal to
    `kafka_original-timestamp`.
  - `retry_topic-backoff-timestamp` decodes to `2026-08-18T02:20:08.511Z`.
  - Normalisation removes `GeneratedMethodAccessor781`, `$$SpringCGLIB$$0`,
    `<generated>`, and all `org.springframework.`/`java.base/` frames, leaving
    only `com.uidai.`/`in.gov.uidai.` frames.
  - Two traces differing only in line numbers produce the **same** fingerprint.
  - Two traces with different root FQCNs produce **different** fingerprints.
  - A truncated trace (`Caused by:` with no frames) does not raise.
  - A stacktrace header that is absent entirely does not raise.
- **Exit criteria:** the reference sample fingerprints deterministically across
  runs and processes; all guards above green.
- **Out of scope:** classification, registry, any I/O, any wiring.

---

### Phase 2 -- Classification and the BusinessException registry

**Goal.** Assign a failure class and resolve a business code to its
description.

- **New:** `src/dlt/classify.py` (Class A/B/C/U from root FQCN + `[CODE]`
  extraction); `src/dlt/registry.py` (CSV loader, process-cached, code lookup).
- **Modified:** `.env.example`.
- **Config:** `DLT_CLASS_MAP`, `DLT_REGISTRY_PATH`.
- **Tests:** `tests/test_dlt_classify.py`:
  - The reference sample classifies as **A** with code
    `UID_ORIGIN_TRACKER_DATA_NOT_FOUND`.
  - A synthetic NPE classifies as **B**; a `SocketTimeoutException` as **C**;
    an unknown FQCN as **U**, never B.
  - Business code extraction handles `[CODE] message`, a message with no
    brackets, and brackets appearing later in the text.
  - A missing registry file is a miss, not a crash. An unknown code is a miss.
  - Registry lookup is case-exact and whitespace-trimmed.
- **Exit criteria:** classification is total (every input yields a class) and
  the registry degrades to "miss" on every failure mode.
- **Out of scope:** anything using the class to make a decision.

---

### Phase 3 -- Payload adapter and case identity

**Goal.** Extract `refId` from the payload and derive a valid, idempotent case
id.

- **New:** `src/dlt/payload.py` (configurable dotted path + bounded recursive
  fallback); `src/dlt/identity.py` (`case_id` derivation, `EVENT_ID_PATTERN`
  sanitisation and length capping); `src/models/dlt_schemas.py`
  (`DltMessage` Pydantic model -- headers + payload + parsed failure,
  `extra="allow"` on the payload).
- **Config:** `DLT_REFID_PATH`, `DLT_REFID_KEYS`.
- **Tests:** `tests/test_dlt_payload.py`:
  - Dotted path extraction, including a path that does not exist.
  - Recursive fallback finds `refId` at depth; respects the depth cap; returns
    `None` rather than raising when absent.
  - `case_id` for the reference sample is
    `dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352` and matches
    `EVENT_ID_PATTERN`.
  - A topic name containing `/` or a space is sanitised and still matches the
    pattern.
  - A pathologically long topic name yields an id <= 128 chars, still unique.
  - The same `(topic, partition, offset)` always yields the same id.
- **Exit criteria:** every id produced from the Phase 0 fixture corpus
  validates against `EVENT_ID_PATTERN` and is unique per message.
- **Out of scope:** storage, consumers.

---

### Phase 4 -- Consumer role seam and the DLT consumer

**Goal.** Consume the DLT topic with headers intact, without changing rejection
behaviour. No fetching, no analysis -- parse, log, commit.

- **New:** `src/utils/message_adapters.py` (`MessageAdapter` protocol,
  `RejectionAdapter` -- today's logic moved verbatim -- and `DltAdapter`);
  `src/dlt_consumer.py` (entrypoint setting `CONSUMER_ROLE=dlt`).
- **Modified:** `src/utils/kafkaConsumer.py` -- add the `dlt` role branch;
  route validation/dedupe/body-construction through the role's adapter; pass
  `msg.headers` to the adapter (**this is the fix for `kafkaConsumer.py:388`,
  which discards them today**). `src/utils/paths.py` -- DLT heartbeat path.
  `start.py` -- supervise the new process. `.env.example`.
- **Config:** `DLT_CONSUMER_TOPIC_NAME`, `DLT_CONSUMER_GROUP_ID`,
  `DLT_CONSUMER_ENDPOINT`, `DLT_CONSUMER_TIMEOUT_SECONDS`, `DLT_HEALTH_PORT`.
- **Tests:** `tests/test_dlt_consumer.py` and additions to
  `tests/test_audit_phase1.py`:
  - **Rejection-path regression:** the existing fast and slow consumer tests
    pass unchanged. `RejectionAdapter` must be behaviourally identical --
    same `MessagePayload` validation, same `packetStatus == REJECTED` filter,
    same terminal-casebook dedupe, same poison-pill DLQ path.
  - Headers survive from `msg` to the outgoing request body, including a
    50KB stacktrace.
  - A header value that is `bytes` (kafka-python's native type) decodes; a
    non-UTF-8 header does not raise.
  - A message whose payload is unparseable JSON goes to the DLQ, offset
    commits.
  - A redelivered message with an existing terminal case is skipped and
    commits.
  - Offset semantics are unchanged: dispatch tracked, committed only on
    completion.
- **Exit criteria:** full existing suite green and unchanged. The DLT consumer
  runs against a fixture broker, parses every Phase 0 sample without raising,
  and commits.
- **Out of scope:** log fetching, the analysis queue, any LLM.

---

### Phase 5 -- Log window, fetch endpoint, analysis queue

**Goal.** Fetch the right logs for the right window and hand off to stage two.

**Gated on Phase 0 item 5** -- if pod log lines do not carry `refId`, stop and
redesign.

- **New:** `src/dlt/window.py` (window derivation from
  `retry_topic-backoff-timestamp`, `DLT_MAX_LOG_AGE_SECONDS` skip);
  `src/api/dlt_routes.py` (`POST /fetch-dlt-logs`); `src/dlt/case_storage.py`
  (the `dlt_cases/` layout from Section 7, over `CasebookStorage`).
- **Modified:** `src/log_pipeline/pipeline.py` -- `reduce_logs` gains optional
  `storage_key` and `window` parameters, both defaulting to current behaviour
  (5.5). `src/utils/analysis_queue_publisher.py` -- publish to the DLT analysis
  topic. `src/api/routes.py` -- mount the new router. `.env.example`.
- **Config:** `DLT_LOG_LEAD_SECONDS`, `DLT_LOG_TRAIL_SECONDS`,
  `DLT_MAX_LOG_AGE_SECONDS`, `DLT_ANALYSIS_TOPIC_NAME`.
- **Tests:** `tests/test_dlt_window.py`, `tests/test_dlt_fetch_route.py`:
  - The reference sample's window anchors on `2026-08-18T02:20:08Z`, **not**
    `2026-08-16T07:20:05Z` -- a direct regression guard on Trap 2 (3.2), with
    the 43-hour gap asserted explicitly.
  - A missing/unparseable backoff header falls back to
    `kafka_original-timestamp`.
  - A window older than `DLT_MAX_LOG_AGE_SECONDS` skips the fetch and records
    a `LOGS_TOO_OLD` gap rather than fetching.
  - `reduce_logs` searches on `refId` and persists under `case_id`.
  - **Existing callers of `reduce_logs` are unaffected** -- the rejection
    pipeline's log tests pass unchanged.
  - `headers.json` and `trace.txt` are persisted **before** any analysis, and
    pass through redaction (a UID embedded in an exception message is scrubbed;
    the `refId` is allowlisted and survives).
  - A missing `refId` still produces a case, with logs skipped.
  - The endpoint is idempotent: a second call for the same case id reuses the
    persisted artifacts.
- **Exit criteria:** a Phase 0 sample flows end-to-end from topic to
  `dlt_cases/<case_id>/` with logs and a `LOGS_FETCHED` status, and lands on
  the analysis queue.
- **Out of scope:** corroboration, grouping, LLM.

---

### Phase 6 -- Corroboration

**Goal.** Compare the declared trace against the fetched logs. Deterministic,
no LLM.

- **New:** `src/dlt/corroborate.py` -- returns `CORROBORATED` / `PARTIAL` /
  `CONTRADICTED` / `UNVERIFIABLE` with the supporting log lines cited.
- **Tests:** `tests/test_dlt_corroborate.py`, all against synthetic log
  fixtures:
  - Logs containing the declared root FQCN -> `CORROBORATED`.
  - Logs containing the business code but not the FQCN -> `CORROBORATED`.
  - Logs containing **only** a different exception (a timeout) where the
    declared root is a `BusinessException` -> `CONTRADICTED`. **This is the
    mis-cast case and is the reason the log lane exists.**
  - Declared root present *plus* unexplained ERRORs -> `PARTIAL`.
  - Empty logs, no `refId`, or a `LOGS_TOO_OLD` gap -> `UNVERIFIABLE`.
  - "Could not look" (fetch failure) and "looked, found nothing" both map to
    `UNVERIFIABLE` and are distinguishable in the returned detail -- mirrors
    the `FetchResult.ok` distinction the log pipeline already makes.
  - Every verdict cites the specific log lines it relied on.
- **Exit criteria:** verdicts are stable and every one carries citations.
- **Out of scope:** acting on the verdict.

---

### Phase 7 -- Group store and reuse policy

**Goal.** Persist per-fingerprint groups and decide, deterministically, whether
a message needs the LLM.

- **New:** `src/dlt/groups.py` (group record read/write over `CasebookStorage`,
  member cap, occurrence counting, `corroboration_history`);
  `src/dlt/reuse.py` (the decision table from 5.7 as a pure function returning
  `LLM_REQUIRED` / `REUSE_GROUP` / `CANNED`).
- **Config:** `DLT_GROUP_MEMBER_CAP`, `DLT_REUSE_ENABLED`.
- **Tests:** `tests/test_dlt_groups.py`, `tests/test_dlt_reuse.py`:
  - Novel fingerprint -> `LLM_REQUIRED`, group created with
    `recommendation_state: none`.
  - Known fingerprint + `CORROBORATED` + a `draft`/`final` recommendation ->
    `REUSE_GROUP`.
  - Known fingerprint + `CONTRADICTED` -> `LLM_REQUIRED`, regardless of cache.
  - Known fingerprint + `PARTIAL` -> `LLM_REQUIRED`.
  - Class B/C -> `CANNED`, never `LLM_REQUIRED`, at any occurrence count.
  - `DLT_REUSE_ENABLED=false` forces `LLM_REQUIRED` for Class A.
  - Member list caps at `DLT_GROUP_MEMBER_CAP` keeping the newest;
    `occurrence_count` keeps counting past the cap.
  - Concurrent updates to one group from two workers do not lose an increment
    (file lock, as `pending_rules.jsonl` already does).
- **Exit criteria:** replaying the Phase 0 corpus produces a group count
  matching the Phase 0 measurement, and the implied LLM call count is
  materially below the message count -- **this is the number that proves the
  cost model.** Record it in Section 11.
- **Out of scope:** producing a recommendation.

---

### Phase 8 -- Analysis lane

**Goal.** Produce the finding. The only phase that calls an LLM.

- **New:** `src/prompts/DltInvestigatorAgent.md`, `DltReviewerAgent.md`,
  `DltSynthesisAgent.md`; `src/dlt/orchestrator.py` (LangGraph:
  investigate -> review -> synthesise, reusing the retry and synthesis-repair
  patterns in `src/core/agent_orchestrator.py`); `src/dlt/canned.py` (Class
  B/C/U treatments); `src/models/dlt_synthesis.py` (result schema + DLT
  confidence ceilings); `src/dlt_analysis_consumer.py` (entrypoint);
  `POST /analyze-dlt` in `src/api/dlt_routes.py`.
- **Modified:** `src/models/synthesis.py` -- extend `apply_confidence_policy`
  with the DLT ceilings (5.8), additive, rejection behaviour unchanged.
  `src/utils/kafkaConsumer.py` -- `dlt_analysis` role. `start.py`,
  `.env.example`.
- **Config:** `DLT_ANALYSIS_*`, all `DLT_*_CEILING`, `DLT_REUSE_DECAY`.
- **Prompt design notes:**
  - The Investigator's question is **not** "what went wrong" -- the trace
    already says. It is "does the log evidence support the trace's claim, and
    if not, what does it show instead?"
  - Registry descriptions are one line and may be incomplete. The prompt must
    treat the description as a seed, and must **never** invent detail about
    *why* a record is missing -- we have no database access. Abstaining is the
    correct answer for a per-packet cause.
  - Every claim cites a log line or a trace frame. The Reviewer rejects
    uncited claims, exactly as the rejection Reviewer does.
- **Tests:** `tests/test_dlt_analysis.py` with a mocked LLM:
  - Class B never invokes the LLM and always yields `NEEDS_MANUAL_REVIEW`
    with confidence <= `DLT_CLASS_B_CEILING`.
  - `REUSE_GROUP` never invokes the LLM; confidence carries the decay.
  - `CONTRADICTED` invokes the LLM and the resulting casebook's `discrepancy`
    field is populated and leads the narrative.
  - Confidence ceilings compose by minimum; each applied ceiling is named in
    `ceilings_applied`.
  - Malformed synthesis output triggers repair, then
    `FAILED_SYNTHESIS_PARSE` -- same contract as the rejection path.
  - Every recommendation is written `recommendation_state: draft`. **No path
    writes `final` in v1** (Section 2).
  - A rejection-path confidence test proves `apply_confidence_policy` is
    unchanged for non-DLT callers.
- **Exit criteria:** end-to-end from DLT topic to a casebook with a
  confidence score, against fixtures and a mocked LLM. Rejection suite green.
- **Out of scope:** promoting drafts; any external routing.

---

### Phase 9 -- Operator CLI and observability

**Goal.** Make the output usable and the system legible.

- **New:** `src/tools/dlt_report.py` -- `--top` (fingerprints by volume),
  `--group <fingerprint>` (inspect: signature, members, corroboration history,
  recommendation), `--case <case_id>` (full casebook + trace), `--unreviewed`
  (drafts awaiting review, the queue a human will eventually work).
- **Modified:** `src/utils/metrics.py` -- counters by class, corroboration
  verdict, reuse decision, group count, LLM invocations, registry misses;
  histogram of window age at fetch time. `src/api/routes.py` -- `/health` and
  `/ready` report the two new consumers' heartbeats alongside the existing two.
  `ARCHITECTURE.md` -- a DLT section cross-referencing this document.
- **Tests:** `tests/test_dlt_report.py` -- each subcommand against a fixture
  store; metrics increment on each path.
- **Exit criteria:** an operator can go from "what is failing most this week"
  to a specific trace in two commands.
- **Out of scope:** dashboards, alerting, the review/approval mechanism.

---

### Dependency graph

```
Phase 0 (data gate) ──────────────┐
                                  ├──> Phase 5 (hard gate: item 5)
Phase 1 ──> Phase 2 ──> Phase 3 ──┴──> Phase 4 ──> Phase 5 ──> Phase 6 ──> Phase 7 ──> Phase 8 ──> Phase 9
```

Phases 1-3 are pure logic and may be built in parallel with Phase 0's data
collection. Phase 4 must not merge before Phase 0 confirms the payload shape.

---

## 11. Phase 0 findings

*(Unfilled. Populate from Phase 0 before starting Phase 5.)*

### 11.1 Class distribution
### 11.2 Fingerprint cardinality
### 11.3 Stacktrace truncation
### 11.4 refId path in EventMessage
Partly answered ahead of the corpus run: sample 2 shows the record key carries
the refId, so `--analyze` now reports which of the four layers resolved each
message and whether key and payload ever disagree. What the corpus still has to
show is whether the key is populated on *every* message or only on this topic.
### 11.5 refId presence in pod log lines -- **hard gate on Phase 5**
### 11.6 Mis-cast examples found
### 11.7 DLT arrival lag
### 11.8 Measured LLM call reduction (from Phase 7)

---

## 12. Open questions

1. ~~**Where is `refId` in the `EventMessage` payload?**~~ **Answered
   (2026-08-20), for `EnrolmentEventResponse`.** The DLT record is *keyed* on
   the refId, and the payload repeats it at `abisMWResponseNewSeda.refId`. The
   key is now the primary source, so this no longer depends on knowing any
   payload path (3.3, 5.3). Still open for `EventMessage` specifically -- no
   payload has been captured for that type, and no path is registered for it
   rather than guessing one that would silently miss. It falls through to the
   search, which reports `search` as its provenance.
2. **Is there a real mis-cast example?** The corroboration check is designed
   against a hypothesis. Until one real case validates it, `CONTRADICTED`
   thresholds stay conservative and the verdict is advisory only.
3. **Is the deployed build version available anywhere** -- a header, a pod
   label, an image tag? Without it, a fingerprint cannot be retired when its
   bug is fixed. See Risk R4.
4. **What is the actual DLT topic name and the retry backoff configuration?**
   The 43-hour span in the sample is either a long configured backoff or
   consumer lag before the first attempt. If it is lag, the last attempt may be
   much older than assumed and `DLT_MAX_LOG_AGE_SECONDS` needs revisiting.
5. **Who works the draft queue?** There is no feedback loop -- output goes
   nowhere external. Until someone reviews drafts, a wrong recommendation on a
   novel fingerprint is served to every subsequent occurrence. Mitigated in v1
   by never writing `final`, so every reuse is explicitly marked unreviewed.

---

## 13. Risks

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | Pod log lines do not carry `refId` | The log lane cannot filter; corroboration is permanently `UNVERIFIABLE` and the mis-cast detector never fires | Phase 0 item 5 is a hard gate on Phase 5 |
| R2 | Fingerprint over-groups (generic wrapper leaks through normalisation) | Distinct bugs share one recommendation; wrong advice at scale | Trap-1 regression test in Phase 1; Phase 7 exit criteria compares group count against the Phase 0 measurement |
| R3 | Fingerprint under-groups (a synthetic frame survives) | Cache never hits; LLM cost scales with the 2,000/day message rate | Phase 1 normalisation tests name every synthetic form seen in the sample; Phase 7 records the measured reduction |
| R4 | No deploy-version dimension | A fixed bug keeps serving its old recommendation indefinitely | `first_seen`/`last_seen` on the group make staleness visible; `dlt_report --top` surfaces it; revisit when Open Question 3 is answered |
| R5 | Stacktrace truncated in headers | The root `Caused by:` -- the only part that matters -- is exactly what is cut | Phase 0 item 3 measures it; parser degrades to Class U rather than fingerprinting a wrapper |
| R6 | 2,000/day overwhelms the fast stage | Log fetch backlog, pod logs rotate before capture | Same two-stage split that already protects the rejection path; fast stage is bounded I/O only; `MAX_CONCURRENT_INVESTIGATIONS` applies per role |
| R7 | Registry arrives in an unexpected format | Phase 2 loader mismatch | Loader is isolated in `src/dlt/registry.py` behind a single lookup function; a format change touches one file |
| R8 | Adapter refactor regresses the rejection path | Live pipeline breaks | `RejectionAdapter` moves today's logic verbatim; Phase 4 exit criteria requires the existing suite green and unchanged |

---

## 14. Replay precheck (phases C0-C8)

An extension, not a revision. It amends one non-goal in section 2 -- "no
source-code analysis" -- in the same narrow, opt-in way section 5.9 amends
"no remediation", and it answers Open Question 3 along the way.

**The question it answers.** `auto_replay.decide()` gates a replay on the
finding alone: action, confidence, refId. Nothing in that decision knows
whether the bug was fixed last Tuesday, or whether the fix reached the pods.
So a replay is a guess, and a packet whose fix has not shipped simply
dead-letters again.

**The join key is the version number**, not a git SHA. ~95% of changes bump
the version in `pom.xml` (or the service's equivalent), the image tag carries
that same version, and the pod's image tag is readable from Kubernetes. That
chain is what removes the need for Gitea, ArgoCD, Harbor digests and any
change to the Jenkins pipeline.

**Gitea is deliberately not used.** It holds the *desired* state. A replay
executes against whatever the pods are running now, so a manifest updated but
not yet synced would actively mislead the verdict. The pod's image tag is the
only authority, and it doubles as the sync signal.

### 14.1 Verdicts

| Verdict | Condition | Consequence |
|---|---|---|
| `NO_CHANGE` | No commit on `release` has touched the failure site since this packet failed | Replay reproduces the same dead letter |
| `NOT_DEPLOYED` | A candidate commit exists; its first-containing version is ahead of the running pod's | Park the packet; replay when the pods reach that version |
| `FIX_DEPLOYED` | The running version is at or beyond the candidate's first-containing version | Replay is worth trying |
| `UNKNOWN` | Frame unmappable, repo unreachable, version unparseable, or no running version captured | Fall through to today's behaviour, unchanged |

`NOT_DEPLOYED` is the operative one: it turns "replay and see" into "replay
after the next deploy", which is a scheduling decision the system can make and
act on by itself.

The verdict answers *deployment*, never *relevance*. "This commit is running"
is not "this commit fixes your bug"; relevance comes from mapping the top
application frame to a file. The casebook keeps the two claims separate so a
reader can disagree with either.

### 14.2 Two flags, not one

Mirroring the split section 5.9 already makes between
`DLT_AUTO_REPLAY_ENABLED` and `ENABLE_AUTO_REPLAY`:

- `DLT_CODE_CHECK_ENABLED` -- do the lookup, write the verdict into the
  casebook. Observe only, and safe from day one.
- `DLT_CODE_CHECK_GATES_REPLAY` -- let the verdict veto or park a replay.

This is the posture Open Question 2 already takes for the mis-cast detector:
advisory until real samples validate it.

---

### Phase C0 -- Feasibility gate

**Goal.** Confirm the five assumptions this design rests on, against the real
systems, before any adapter code is written. Same shape as Phase 0.

- **New:** `src/tools/code_check_probe.py` -- a throwaway CLI carrying its own
  minimal Bitbucket client, deliberately *not* depending on C4 (whose shape
  its output is meant to determine). `tests/test_code_check_probe.py` covers
  the decisions it makes about what it reads, since those are copied forward
  into C3 and C4.
- **Run:** `python -m src.tools.code_check_probe --all --repo ENU/enu-biometric`
- **Exit criteria:** all five questions in 14.3 answered in writing. If Q4
  reports `AT-RELEASE-CUT`, C5 must implement the forward-walk (Trap T6)
  rather than reading the version at the fix commit.
- **Out of scope:** anything that writes. Any dependency on this tool from
  shipped code.

### 14.3 Phase C0 findings

Filled in by whoever runs the probe against the real systems. Until then
every row is open, and C4 must not merge.

| # | Question | Answer |
|---|---|---|
| Q1 | Bitbucket reachable from the cluster, and Server/DC or Cloud? | *pending* |
| Q2 | Is `release` the deployed branch, per repo? | *pending* |
| Q3 | What does the image tag look like, and does it order? | *pending* |
| Q4 | Version bumped in the fix commit, or at release cut? | *pending* |
| Q5 | Multi-module layout; which pom is the image tagged from? | *pending* |

**Q4 is the load-bearing one.** `IN-FIX-COMMIT` means the version at the fix
commit is already the first-containing version. `AT-RELEASE-CUT` means reading
it there is systematically wrong, in the direction that causes replays which
fail again. `MIXED` means build the forward-walk, which is correct under either
convention -- and is what C5 builds regardless.

**Q4 must be stratified by repo, not pooled.** If one team never bumps
versions, Trap T9 is not a 5% error rate for them but a 100% one.

---

### Phase C1 -- Deployed version capture

**Goal.** Record which build was running when the packet failed. Answers Open
Question 3 and mitigates Risk R4 on its own, independently of the code check
that consumes it -- so it is deliberately **not** behind a feature flag.

- **New:** `src/dlt/deployed.py` -- `running_version(app, namespace)` lists the
  service's pods and reads `status.container_statuses[].image`, returning a
  `DeployedVersions` record: the distinct versions seen, the (pod, container,
  image) rows behind them, and a reason when nothing could be read. Guarded by
  `k8s_breaker`; the retry lives in `k8s/retry.py`, which the pod listing
  already goes through.
- **Modified:** `src/log_pipeline/sources/k8s/discovery.py` -- a public
  `list_pods_for_service()` wrapping `resolve_service` + `_list_pods`, so the
  DLT lane does not reach into a private function. Additive; no existing path
  changes. `src/api/dlt_routes.py` -- capture in `fetch_dlt_logs`, persist as
  `deployed.json`, carry `baseline_versions` on the queued message.
  `src/utils/metrics.py` -- `record_dlt_deployed_version_read`.
- **Config:** `DLT_DEPLOYED_VERSION_TTL_SECONDS` (default 60). Reuses
  `K8S_DEFAULT_NAMESPACE`, `K8S_DEFAULT_APP`, `K8S_SERVICE_MAP`.
- **Design notes:**
  - **A separate Kubernetes call, not a change to the log pipeline.** Threading
    an image field through `PodTarget` -> `DiscoveryResult` -> `FetchResult` ->
    `reduce_logs` would touch code the rejection lane depends on (Risk R8), for
    a value only the DLT lane wants.
  - **No version ordering here.** A rolling deploy has pods on two versions at
    once; this module reports the set and refuses to name a single winner.
    Ordering is C3's, and doing it here would mean comparing version strings
    lexically -- Trap T7.
  - **Only successful reads are cached.** Caching a failure would hold a whole
    TTL of cases at `UNKNOWN` after a transient blip.
  - The pod is the authority, not the manifest. A version that ArgoCD has not
    yet synced is not running, and is exactly the wrong answer for deciding
    whether a replay will work.
- **Tests:** `tests/test_dlt_deployed.py` -- both image-reference shapes, a
  digest that must not be read as a version, a registry port that must not be
  read as a tag; a rolling deploy reports both versions and no single one; a
  dead cluster, an unresolved namespace and a pod with no container status each
  return a reason rather than raising; the cache collapses repeats, a failure
  is not cached, and a zero TTL disables it. `tests/test_dlt_fetch.py` -- the
  artifact is written on every case, and the fetch still succeeds when the
  version cannot be read.
- **Exit criteria:** every new DLT case carries a `baseline_versions` value, or
  an explicit empty list with a recorded reason. Existing suite green.
- **Out of scope:** comparing versions, and any use of the value. C1 only
  observes.

---

### Phase C2 -- Frame locations

**Goal.** Preserve the file and line the parser already captures and discards,
without letting either near the fingerprint.

- **Modified:** `src/dlt/stacktrace.py` -- `_parse_link` keeps the regex's
  optional `location` group as a tuple index-parallel with `frames`; a new
  `FrameLocation` dataclass carries `(target, file, line)` and derives the
  repository path suffix; `normalise_frame_locations()` applies the same
  keep/drop rule as `normalise_frames`, now factored into one shared
  `_is_app_frame` predicate so the two can never disagree about which frames
  are ours. `src/api/dlt_routes.py` -- `build_failure` returns a `locations`
  list beside `frames`.
- **Config:** none. The existing `DLT_APP_PACKAGES` and
  `DLT_BOILERPLATE_FRAMES` govern both projections.
- **Design notes:**
  - **A bare frame holds a `None` slot rather than being skipped.** Skipping
    it would shift every later location onto the wrong frame -- silently, and
    only for traces that mix the two forms.
  - **The path is built from the package plus the file name the JVM
    reported**, not from the class name. A frame in `com.foo.Outer$Inner.run`
    reports `Outer.java`, which is the file that exists; deriving the name
    from the class would ask the repository for `Outer$Inner.java`.
  - `compute_fingerprint` and `build_signature` keep their exact inputs.
- **Tests:** `tests/test_dlt_stacktrace.py` -- the reference sample's
  fingerprint is pinned as a literal and asserted byte-identical to its
  pre-C2 value (Risk R3); locations parse off the reference trace
  (`BioDataBaseHelperServiceImpl.java:257`); locations stay index-parallel
  with the normalised frames; a frame with no location keeps its place;
  `Native Method`, `Unknown Source` and a non-numeric suffix all yield a
  `None` line rather than a guess; inner-class and lambda frames resolve to
  the right file. `tests/test_dlt_fetch.py` -- `build_failure` carries them
  and the fingerprint is unchanged.
- **Exit criteria:** locations available downstream; every existing
  fingerprint unchanged.
- **Out of scope:** using them. C4 is the first consumer.

---

### Phase C3 -- Version algebra

**Goal.** Parse and order the version strings that join a commit to a running
pod. The single most likely place for this feature to be quietly wrong for
months, because a bad comparison looks exactly like a good one.

- **New:** `src/dlt/versions.py` -- `parse()`, `compare()`, `at_least()`,
  `is_ahead()`, `lowest()`, and `version_of()` (image reference -> version
  string). Pure.
- **Modified:** `src/dlt/deployed.py` -- `version_of` now delegates here, so
  the two copies of image-reference parsing cannot drift apart. **And a bug
  C1 shipped is fixed:** sidecar containers were contributing their own image
  versions to the set. Left in, an istio proxy's version would join the set
  `lowest()` reduces on a rolling deploy, comparing the application against
  the mesh proxy. Filtering now reuses `K8S_SIDECAR_DENYLIST` through the log
  pipeline's own `select_containers`, so an operator maintains one list.
- **Config:** `DLT_VERSION_PATTERN` -- optional regex with a `(?P<version>)`
  group, for a repo whose tag puts the version somewhere other than the front.
- **Rules:**
  - Numeric core compared component-wise as integers, never lexically, and
    zero-padded so `1.0` equals `1.0.0`.
  - `-SNAPSHOT` sorts *before* the same release version.
  - A trailing build counter (`release.42`) breaks ties on an equal core. A
    name (`rc1`) does not -- only a counter orders.
  - **Equal cores with an ambiguous qualifier compare equal, not ordered.**
    `1.0.0` is the pom's number and `1.0.0-release.42` is a build of it;
    nothing in either string says which came first. This is what lets C5
    catch Trap T9 by requiring `is_ahead` rather than `at_least`.
  - **Anything unparseable returns None, and None propagates through every
    comparison.** C5 turns that into `UNKNOWN`. Never a guess.
  - `lowest()` returns None if *any* entry is unparseable rather than skipping
    it -- the unreadable version might be the low one, and skipping it would
    report a higher floor than actually exists.
- **Tests:** `tests/test_dlt_versions.py` -- `1.0.10 > 1.0.9`,
  `release.9 < release.12`, `SNAPSHOT < release`; the ambiguous pairs compare
  equal; every unparseable input yields None rather than an ordering; a
  brute-forced **antisymmetry and transitivity check over the whole corpus**,
  which a comparator with one branch backwards does not survive; `is_ahead`
  and `at_least` differ exactly on equality; a broken `DLT_VERSION_PATTERN`
  degrades to the default parser.
- **Exit criteria:** every tag collected in C0 parses, or is explicitly
  recorded as unparseable, and no comparison returns a wrong ordering for that
  corpus.
- **Out of scope:** deciding anything. C3 only orders.

---

### Phase C4 -- Bitbucket adapter

**Goal.** Read-only access to `release`: which commits touched a file, and
what the version file said at a commit.

> **Gate.** This phase must not merge to `main` before section 14.3's findings
> are filled in. Its API flavour, path resolution and version-file handling
> all depend on what C0 reports, and `BITBUCKET_BASE_URL` left empty keeps it
> completely inert in the meantime.

- **New:** `src/dlt/bitbucket.py` -- `repo_for`, `resolve_path`,
  `commits_touching`, `changed_paths`, `file_at`, `version_at`,
  `parse_pom_version`, `touches_version_file`. Both API flavours.
- **Modified:** `src/utils/resilience.py` -- a `bitbucket_breaker` beside the
  existing four. `src/utils/metrics.py` -- it joins the breaker-state gauge.
- **Config:** `BITBUCKET_BASE_URL`, `BITBUCKET_TOKEN`, `BITBUCKET_USERNAME`
  (Cloud app passwords only), `BITBUCKET_API_FLAVOUR`,
  `BITBUCKET_TIMEOUT_SECONDS`, `DLT_REPO_MAP`, `DLT_CODE_CHECK_TTL_SECONDS`,
  `DLT_CODE_CHECK_MAX_COMMITS`.
- **Design notes:**
  - **`None` and `[]` mean different things, and the difference decides a
    replay.** `[]` is "the server answered, and nothing has touched this
    file", which C5 turns into `NO_CHANGE` and a withheld replay. `None` is
    "we could not look". Collapsing them would let an unreachable Bitbucket
    read as "the code definitely has not changed" and stop every replay in the
    system on no evidence at all. Same distinction as `FetchResult.ok` and
    `Corroboration.could_not_look`. A `None` is never cached.
  - **Path resolution prefers construction over search.** C2 already knows the
    path suffix, so the first strategy tries each configured source root and
    checks whether the file exists: one call, identical on both flavours, and
    a multi-module layout is a config change. Listing the repository is the
    Server-only fallback, and **two matches resolve to `UNKNOWN`, not a coin
    toss** -- picking either would attribute a commit to the wrong module.
  - **Trap T5 is handled by parsing the XML, not by a regex.** A pom declares
    `<parent><version>` *before* its own `<version>`, so the first `<version>`
    tag is the parent's. `/project/version` is read specifically, falling back
    to `/project/parent/version` only when the project declares none -- the
    case Maven's own inheritance rule covers.
  - An unmapped package resolves to no repository (Trap T8), a malformed
    `DLT_REPO_MAP` maps nothing at all rather than half of it, and a file over
    `MAX_FILE_BYTES` is refused rather than parsed.
- **Tests:** `tests/test_dlt_bitbucket.py`, no network -- longest-prefix repo
  matching; the shared-library package mapping to nothing; construction in one
  call, root ordering, the listing fallback, and ambiguity yielding None;
  Server and Cloud commit shapes including ISO-vs-epoch timestamps; a commit
  with no timestamp kept rather than dropped; the parent-version trap, an
  inheriting pom, a namespace-less pom, and unparseable input; 401/403/404/
  500/502, a transport failure, a non-JSON body and a tripped breaker all
  degrading rather than raising; Bearer vs Basic auth; the cache serving
  repeats and refusing to cache a failure.
- **Exit criteria:** the reference sample's top application frame resolves to
  a real path in the real repository, and its version file reads correctly at
  HEAD of `release`.
- **Out of scope:** writes of any kind, diff parsing, and reading source into
  an LLM prompt.

---

### Phase C5 -- Verdict engine (observe only)

**Goal.** Compose C1-C4 into one of the four verdicts and write it into the
casebook. Changes nothing about replay.

- **New:** `src/dlt/code_check.py` -- `evaluate(failure, failed_at_ms,
  baseline_versions, running_versions) -> CodeCheck`, a frozen dataclass
  carrying its own evidence, in the shape `corroborate.py` already uses.
- **Modified:** `src/api/dlt_routes.py` -- runs in `analyze_dlt` after the
  group is recorded and before the replay gate, off-loop via `_off_loop`; a
  `code_check` block on the casebook; `DLT_CASEBOOK_SCHEMA_VERSION` -> `1.1`;
  `_recorded_baseline` reads `deployed.json`. `src/dlt/groups.py` --
  `attach_code_check` through `update_json`, plus `code_check` and
  `code_check_history` on `_blank`. `src/utils/metrics.py` --
  `record_dlt_code_check`.
- **Config:** `DLT_CODE_CHECK_ENABLED` (default `false`),
  `DLT_CODE_CHECK_FRAMES`, `DLT_CODE_CHECK_MAX_CANDIDATES`,
  `DLT_CODE_CHECK_BRANCH`.
- **Design notes:**
  - **The verdict answers deployment, never relevance.** "This commit is
    running" is not "this commit fixes your bug". The casebook keeps
    `code_check` separate from `finding` so a reader can disagree with either.
  - **Three asymmetries, all pointing the same way.** A wrong `FIX_DEPLOYED`
    causes a replay that fails again; a wrong `NOT_DEPLOYED` only delays one.
    So the *highest* candidate version is required (several commits touched
    the site and we cannot tell which is the fix), the *lowest* running
    version is compared (a replay may land on any pod mid-rollout), and a
    positive verdict requires a baseline while a negative one does not.
  - **Trap T6 is handled by walking forward, unconditionally.** The fix
    commit's own change set is checked first; only when it did not touch the
    version file does the check walk forward to the next commit on the branch
    that did. Correct under either convention, which is why it is built
    regardless of what C0's Q4 reports.
  - **Trap T9 is handled by requiring the version to have moved.** If the
    change's version is not strictly ahead of the failing build's, a bump was
    probably skipped and the version carries no signal -- `UNKNOWN`, not
    `FIX_DEPLOYED`.
  - **`baseline` is read from the artifact, not from today.** Substituting the
    current version for the one that was running would silently defeat the T9
    guard.
  - **The group record is a record, not a cache.** Cost control lives in
    `bitbucket.py`, whose reads are already keyed on things that repeat within
    a group; `code_check` on the group is what the operator CLI reads and what
    the accuracy loop joins against.
  - Runs for Class A and B only.
- **Tests:** `tests/test_dlt_code_check.py` -- every failure mode yields
  `UNKNOWN`; the flag off makes zero calls; an unreadable repository is
  `UNKNOWN` and never `NO_CHANGE`; both T6 branches; T9 in both directions;
  the highest-required and lowest-running asymmetries; `1.0.10` not behind
  `1.0.9` end to end; a raised exception degrading rather than propagating.
  `tests/test_dlt_analysis_replay.py` -- the casebook always carries the
  block, the verdict reaches the group, and **the replay decision is
  unchanged for every verdict**.
- **Exit criteria:** verdicts appear in production casebooks with the flag on,
  and no replay behaviour has changed. Then leave it running and collect
  cases.
- **Out of scope:** acting on the verdict. That is C6.

---

### Phase C6 -- Replay veto

**Goal.** Let a `NO_CHANGE` or `NOT_DEPLOYED` verdict withhold a replay that
would otherwise fire.

> **Precondition.** Do not enable until C5 has run for at least two weeks and
> the recorded verdicts have been checked against what replays actually did.
> Same evidence bar Open Question 2 sets for the corroboration verdict.

- **Modified:** `src/dlt/auto_replay.py` -- `decide(finding, ref_id,
  code_check=None)` and `maybe_replay(..., code_check=None)`, with the new
  check placed **last**, after the existing four. `src/api/dlt_routes.py`
  passes the C5 verdict through.
- **Config:** `DLT_CODE_CHECK_GATES_REPLAY` (default `false`), independent of
  `DLT_CODE_CHECK_ENABLED`. The gate is inert unless both are on.
- **Design notes:**
  - **A veto only: it can subtract a replay, never add one.** That asymmetry
    is the whole safety argument -- a wrong verdict can delay a packet, and
    cannot cause a replay that fails again. Letting `FIX_DEPLOYED` *enable* a
    replay the existing gate declined is a real capability, and is what would
    finally make Class B replayable, but it is deferred (see below).
  - **`UNKNOWN` changes nothing.** A Bitbucket outage, an unmapped package or
    a disabled flag must not silently stop every replay in the system.
  - **The veto goes last**, so a replay declined for its own reasons still
    reports that reason rather than blaming the precheck.
  - `NOT_DEPLOYED` withholds here and is *parked* in C7 -- withholding without
    coming back to it would lose the packet.
- **Tests:** extends `tests/test_dlt_auto_replay.py` -- the gate flag off
  leaves all four verdicts inert; `NO_CHANGE` and `NOT_DEPLOYED` withhold and
  name why, the latter naming the version to wait for; `FIX_DEPLOYED` and
  `UNKNOWN` change nothing; the veto cannot rescue a declined finding; the
  existing conditions still report first. `tests/test_dlt_analysis_replay.py`
  -- the same case that replays under C5 does not under C6.
- **Exit criteria:** a replay that would have failed is withheld, and the
  casebook says exactly which commit-absence withheld it.
- **Out of scope:** parking the withheld packet (C7), and any path that lets a
  verdict cause a replay.

---

### Deferred -- Class B replay

The capability this unlocks is replaying **Class B** -- the NPEs, index errors
and cast failures that today get a canned `NEEDS_MANUAL_REVIEW` and never
replay at all, because `canned.py` attaches no confidence and `decide()`
rejects a finding without one. Class B is also the only class where "the code
changed, so the replay may now work" is a coherent claim: Class A's typical
`DATA_FIX_REQUIRED` means a row is missing, and no commit makes a row appear.

Deferred rather than scheduled, because it is the one change that lets this
feature *cause* replays rather than only withhold them. Preconditions, all of
them:

- C6 enabled and stable.
- At least 30 `FIX_DEPLOYED` verdicts checked by hand against real replay
  outcomes.
- A measured false-positive rate from the C8 accuracy report, not an assumed
  one.
- Its own flag, defaulting off, on the pattern the other two already follow.

---

### Phase C7 -- Parked replays

**Goal.** Hold a `NOT_DEPLOYED` packet, then replay it once the pods reach the
version that carries the fix. This is the part that turns "replay it and see"
into "replay it after Thursday's deploy".

- **New:** `src/dlt/parked.py` -- a `dlt_parked_replays` storage root via
  `get_scoped_storage`, one document per case; `maybe_park`, `list_parked`,
  `release_ready`. `src/tools/release_parked_replays.py` -- an offline CLI
  (`--list`, `--dry-run`, `--version`, `--json`), idempotent, run after a
  deploy or on a schedule.
- **Modified:** `src/api/dlt_routes.py` -- parks after the replay gate; a
  `parked` block on the casebook, present whether or not anything was parked.
- **Config:** `DLT_CODE_CHECK_PARK_ENABLED`,
  `DLT_PARKED_REPLAY_TTL_SECONDS` (30 days), `DLT_PARKED_REPLAY_CAP` (500).
- **Design notes:**
  - **One document per case, not a shared file.** `_queue_pending_replay`
    learned this the hard way: a `pending_replays.jsonl` lived on whichever
    pod wrote it, so under the S3 backend with more than one replica the queue
    fragmented. A parked queue is worse to lose -- nobody is watching it.
  - **Parking requires everything a replay requires, minus the version.** A
    packet parks only when `auto_replay.decide` would have said yes *without*
    the veto. Otherwise the release worker becomes a second replay path that
    bypasses `DLT_AUTO_REPLAY_ENABLED`, replaying packets the operator never
    agreed to replay.
  - **Releasing does not necessarily replay.** It calls `queue_for_replay`,
    whose `ENABLE_AUTO_REPLAY` switch still decides whether the packet reaches
    OIS or lands in `pending_replays` for a human.
  - **An unreadable running version leaves everything parked.** Not being able
    to read a version is not evidence that a fix shipped.
  - **Expiry beats release.** A month-old packet is not obviously safe to
    replay just because the version finally moved.
- **Tests:** `tests/test_dlt_parked.py` -- only `NOT_DEPLOYED` parks; nothing
  parks with auto-replay off, with the veto off, or for a finding the gate
  declines on its own; an unusable case id is refused; the cap and TTL both
  bind; release is numeric not lexical, is idempotent, and leaves an
  unreadable version parked; a dry run changes nothing; expiry beats release.
  `tests/test_dlt_analysis_replay.py` -- parked end to end, and the casebook
  always carries the block.
- **Exit criteria:** a packet parked before a deploy is queued for replay
  after it, with no human involved in the timing decision.
- **Out of scope:** mapping a parked entry's repository back to a Kubernetes
  service. The release worker reads one app's version, so a deployment with
  several DLT-producing services runs it once per app.
