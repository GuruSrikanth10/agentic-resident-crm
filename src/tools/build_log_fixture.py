#!/usr/bin/env python3
"""
Turn a downloaded prod log file into a Kubernetes fixture tree
(KUBERNETES_LOGS_PLAN.md 10.1).

Why this and not ES_MOCK_FILE
-----------------------------
`ES_MOCK_FILE` feeds `fetcher.fetch_logs` directly, so it bypasses the entire
Kubernetes source -- discovery, the context-window selector, gap detection,
redaction and snapshot reuse never run, and its identifier filter is a bare
substring test with no context window at all. `K8S_FIXTURE_DIR` bypasses only
`client.get_client()`: everything downstream executes exactly as it does
against a live cluster. For standing in for prod, that is the seam that
actually exercises the code under test.

No pre-filtering is needed, or wanted. The Kubernetes API has no server-side
grep, so the live path already reads a pod's whole buffer and filters it
client-side in `filtering.ContextWindowSelector`. A full multi-packet dump is
therefore the *faithful* input: it is exactly what a real pod read returns.

Layout produced (consumed by sources/k8s/fixtures.py):

    <out>/<namespace>/<pod-name>/
        current.log     every line prefixed with an RFC3339 timestamp,
                        mimicking `timestamps=True` on a live read
        meta.json       phase, start_time, labels, containers, restart_counts

Pods are grouped by the record's `HOSTNAME` field, which in these logs holds
the real pod name (`enu-biometric-abis-mw-consumer-dcdr-6bc6456d4f-h8c66`).
That preserves genuine per-replica attribution AND satisfies discovery's
default `name_contains` match mode, which tests the configured service name
against the pod name as a substring.

Input formats, detected per line
--------------------------------
  1. `{"@timestamp":...}`        bare JSON         -- the format these logs use
  2. `<RFC3339> {"level":...}`   kubelet-prefixed  -- copied through
  3. `"<date>","{""@timestamp""...}"`  Kibana CSV  -- unescaped, then as (1)
  4. anything else               plain text        -- e.g. Hibernate's `select
                                                      ...` stdout, which
                                                      carries no timestamp of
                                                      its own

Usage:
    python3 -m src.tools.build_log_fixture prod_logs.txt
    python3 -m src.tools.build_log_fixture prod_logs.txt --namespace enu
    python3 -m src.tools.build_log_fixture prod_logs.txt --shard-bytes 0
"""
import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

#: Same shape sources/k8s/parser.py strips back off. Kept as its own copy so
#: this tool stays runnable without importing the pipeline.
RFC3339 = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
_KUBELET_TS = re.compile(rf"^({RFC3339})\s+")
_LEADING_TS = re.compile(rf"^\[?({RFC3339})\]?[\s,]*")

#: Kibana CSV rows look like:  "Jul 31, 2026 @ 01:54:25.000","{""@timestamp""...}"
_CSV_JSON = re.compile(r'"(\{.*\})"\s*(?:,|$)', re.DOTALL)

#: Fields carrying the service name, in the order parser.py prefers them.
#: `logger_name` is deliberately NOT here even though parser.py falls back to
#: it: it holds a fully-qualified Java class, which is not a service.
APP_KEYS = ("application_name", "app", "service", "service_name")

#: Fields that may carry the emitting pod's name.
POD_KEYS = ("HOSTNAME", "hostname", "kubernetes.pod_name", "pod_name", "host")

#: Far in the past, so a fixture pod never trips POD_REPLACED. Matches the
#: default in fixtures.py.
POD_START = "2000-01-01T00:00:00+00:00"

#: Just under the K8S_MAX_BYTES_PER_POD default (10MiB), so a fixture never
#: raises a spurious TRUNCATED gap without the operator retuning anything.
DEFAULT_SHARD_BYTES = 9 * 1024 * 1024

#: discovery._max_pods() default. Exceeding it silently drops pods (with a
#: TRUNCATED gap), so the report tells the operator to raise it.
DEFAULT_MAX_PODS = 20

_SAFE_NAME = re.compile(r"[^a-z0-9.-]+")


def sanitise(name: str) -> str:
    """Reduce a name to something usable as a directory / pod name."""
    cleaned = _SAFE_NAME.sub("-", str(name).strip().lower()).strip("-.")
    return cleaned or "unknown"


def _dig(payload: dict, dotted: str):
    """Look up `a.b.c`, tolerating flattened `"a.b.c"` keys."""
    if dotted in payload:
        return payload[dotted]
    node = payload
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _first(payload: dict, keys):
    for key in keys:
        value = _dig(payload, key)
        if value:
            return value
    return None


def extract_json(line: str):
    """Return the JSON object embedded in `line`, or None.

    Handles the bare `{...}` case and the Kibana CSV case, where the object is
    quoted as a CSV field and every inner quote is doubled.
    """
    stripped = line.strip()
    if stripped.startswith("{"):
        try:
            payload = json.loads(stripped)
            return payload if isinstance(payload, dict) else None
        except ValueError:
            return None

    match = _CSV_JSON.search(stripped)
    if not match:
        return None
    candidate = match.group(1)
    if '""' in candidate:
        candidate = candidate.replace('""', '"')
    try:
        payload = json.loads(candidate)
        return payload if isinstance(payload, dict) else None
    except ValueError:
        return None


def normalise_timestamp(raw) -> str:
    """Coerce a log timestamp into the RFC3339 form the parser's regex accepts.

    The full-match fast path matters: these logs carry nanosecond precision
    (`.081060589`), which `datetime.fromisoformat` truncates to microseconds.
    Matching first passes the original string through untouched, so the
    ordering key keeps every digit the service emitted.

    Returns "" when it cannot parse, so the caller falls back to the previous
    line's timestamp rather than emitting a line the parser will not recognise.
    """
    if not raw:
        return ""
    text = str(raw).strip()
    if re.fullmatch(RFC3339, text):
        return text
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.isoformat()


class Shard:
    """One `<pod>/current.log` being written, capped at `limit` bytes."""

    def __init__(self, root: Path, namespace: str, pod_name: str, limit: int):
        self.pod_name = pod_name
        self.dir = root / namespace / pod_name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.handle = (self.dir / "current.log").open("w", encoding="utf-8")
        self.bytes = 0
        self.lines = 0
        self.limit = limit

    @property
    def full(self) -> bool:
        return self.limit > 0 and self.bytes >= self.limit

    def write(self, text: str):
        self.handle.write(text)
        self.bytes += len(text.encode("utf-8", errors="replace"))
        self.lines += 1

    def close(self, app: str, container: str):
        self.handle.close()
        (self.dir / "meta.json").write_text(
            json.dumps(
                {
                    "phase": "Running",
                    "start_time": POD_START,
                    "labels": {"app": app},
                    "containers": [container],
                    "restart_counts": {container: 0},
                },
                indent=2,
            ),
            encoding="utf-8",
        )


class PodWriter:
    """One real pod's lines, rolled across shards to stay under the cap."""

    def __init__(self, root: Path, namespace: str, pod_name: str, app: str, limit: int):
        self.root, self.namespace, self.limit = root, namespace, limit
        self.base_name = pod_name
        self.app = app
        self.shards = [Shard(root, namespace, pod_name, limit)]
        self.lines = 0
        self.bytes = 0

    def write(self, text: str):
        if self.shards[-1].full:
            # Suffix rather than a fresh name: the base pod name must survive
            # so discovery's `name_contains` match on the service still hits
            # every shard.
            self.shards.append(Shard(
                self.root, self.namespace,
                f"{self.base_name}-p{len(self.shards)}", self.limit,
            ))
        self.shards[-1].write(text)
        self.lines += 1
        self.bytes += len(text.encode("utf-8", errors="replace"))

    def close(self, container: str):
        for shard in self.shards:
            shard.close(self.app, container)


def build(args) -> int:
    source = Path(args.input).expanduser()
    if not source.is_file():
        print(f"error: {source} is not a file", file=sys.stderr)
        return 2

    out_root = Path(args.out).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    pods: dict[str, PodWriter] = {}
    formats = Counter()
    apps: Counter = Counter()
    skipped_blank = 0
    undated = 0
    last_timestamp = ""
    last_pod = ""
    last_app = ""
    oldest_ts = newest_ts = ""

    with source.open("r", encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            line = raw.rstrip("\n").rstrip("\r")
            if not line.strip():
                skipped_blank += 1
                continue

            payload = extract_json(line)
            timestamp, pod, app, body = "", None, args.app, line

            if payload is not None:
                formats["json"] += 1
                timestamp = normalise_timestamp(_first(
                    payload, ("@timestamp", "timestamp", "time", "ts")))
                pod = _first(payload, POD_KEYS)
                if app is None:
                    app = _first(payload, APP_KEYS)
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            else:
                kubelet = _KUBELET_TS.match(line)
                leading = None if kubelet else _LEADING_TS.match(line)
                if kubelet:
                    formats["kubelet"] += 1
                    timestamp, body = kubelet.group(1), line[kubelet.end():]
                elif leading:
                    formats["plain-dated"] += 1
                    timestamp, body = leading.group(1), line[leading.end():]
                else:
                    formats["plain"] += 1

            # A Hibernate `select ...` line, or a stack-trace continuation,
            # carries no timestamp, pod or service of its own. All three are
            # inherited from the record that preceded it:
            #
            #  * the timestamp keeps it adjacent to the statement that emitted
            #    it after read_all's global sort, instead of sinking to the top
            #    of the trace under an empty ordering key;
            #  * the pod keeps it in the same current.log as that statement, so
            #    the context window that catches the statement catches this too
            #    -- filed under a synthetic "unknown" pod it would land
            #    somewhere the selector never reads.
            if not timestamp:
                timestamp = last_timestamp
                if not timestamp:
                    # Nothing to inherit yet: this is preamble before the first
                    # timestamped record. Dropping it is safer than inventing a
                    # time that would reorder the trace.
                    undated += 1
                    continue
            else:
                last_timestamp = timestamp

            pod = sanitise(pod) if pod else last_pod
            app = sanitise(app) if app else last_app
            if not pod:
                pod = sanitise(f"{app or args.default_app}-offline-0")
            if not app:
                app = args.default_app
            last_pod, last_app = pod, app

            # RFC3339 with a fixed offset sorts lexicographically, and every
            # line in these dumps carries the same +05:30, so string
            # comparison is a correct min/max here.
            if not oldest_ts or timestamp < oldest_ts:
                oldest_ts = timestamp
            if timestamp > newest_ts:
                newest_ts = timestamp

            apps[app] += 1
            if pod not in pods:
                pods[pod] = PodWriter(out_root, args.namespace, pod, app, args.shard_bytes)
            pods[pod].write(f"{timestamp} {body}\n")

    if not pods:
        print("error: no usable log lines found", file=sys.stderr)
        return 1

    for writer in pods.values():
        writer.close(args.container or writer.app)

    _report(args, out_root, pods, formats, apps, skipped_blank, undated,
            oldest_ts, newest_ts)
    return 0


def _safe_since_hours(oldest_ts: str):
    """Largest look-back that will not raise a spurious LOG_ROTATION gap.

    `gaps.detect_rotation_gap` reports rotation when the oldest line observed
    is newer than the start of the requested window -- on a live cluster that
    means the kubelet dropped the earlier lines. A downloaded dump looks
    identical: its oldest line is whenever the dump begins, which for a fresh
    download is minutes ago, so the default two-hour window would flag every
    single fetch as INCOMPLETE and the banner would stop meaning anything.

    Keeping the window inside the dump's own age removes the false positive
    while leaving genuine gap detection intact. Returns None when the dump is
    already older than the default, where nothing needs overriding.
    """
    if not oldest_ts:
        return None
    try:
        oldest = datetime.fromisoformat(oldest_ts)
    except ValueError:
        return None
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)

    # A slack margin below the true age, mirroring K8S_ROTATION_SLACK_SECONDS,
    # so the value stays correct rather than sitting exactly on the boundary.
    age_hours = (datetime.now(timezone.utc) - oldest).total_seconds() / 3600.0 - 0.05
    if age_hours >= 2.0:
        return None
    return max(0.1, round(age_hours, 2))


def _report(args, out_root, pods, formats, apps, skipped_blank, undated,
            oldest_ts="", newest_ts=""):
    total_lines = sum(w.lines for w in pods.values())
    total_bytes = sum(w.bytes for w in pods.values())
    shard_count = sum(len(w.shards) for w in pods.values())
    biggest = max((s.bytes for w in pods.values() for s in w.shards), default=0)

    print(f"\nFixture written to {out_root}")
    print(f"  namespace        {args.namespace}")
    print(f"  lines written    {total_lines:,}")
    print(f"  bytes written    {total_bytes / (1024 * 1024):.1f} MiB")
    print(f"  pods             {len(pods)} real -> {shard_count} fixture pod(s)")
    if skipped_blank:
        print(f"  blank skipped    {skipped_blank:,}")
    if undated:
        print(f"  undated dropped  {undated:,}  (preamble before the first timestamp)")

    since_hours = _safe_since_hours(oldest_ts)
    if oldest_ts:
        print(f"\n  log span         {oldest_ts}\n"
              f"                   {newest_ts}")

    print("\nLine formats detected")
    for name, count in formats.most_common():
        print(f"  {name:<14} {count:,}")

    print("\nPods")
    for pod, writer in sorted(pods.items(), key=lambda kv: -kv[1].lines):
        note = f"  ({len(writer.shards)} shards)" if len(writer.shards) > 1 else ""
        print(f"  {pod:<58} {writer.lines:>9,} lines  "
              f"{writer.bytes / (1024 * 1024):>6.1f} MiB{note}")

    print("\nServices (application_name)")
    for app, count in apps.most_common():
        print(f"  {app:<58} {count:>9,} lines")

    # Discovery matches the configured service name against the POD name as a
    # substring. A service whose pods are not named after it would silently
    # discover nothing, so check it here rather than letting the operator find
    # out from an empty trace.
    unmatched = [p for p in pods if not any(a in p for a in apps)]

    print("\n" + "=" * 72)
    print("Add to .env  (replaces Elasticsearch and the live cluster)")
    print("=" * 72)
    print(f"K8S_FIXTURE_DIR={out_root}")
    print(f"K8S_DEFAULT_NAMESPACE={args.namespace}")
    print(f"K8S_APP_NAMES={','.join(sorted(apps))}")
    print("K8S_SEARCH_FIELDS=eventId,refId")
    if since_hours is not None:
        print(f"K8S_DEFAULT_SINCE_HOURS={since_hours}")
    # Elastic must leave the chain: with fixtures serving Kubernetes, a packet
    # whose identifier matches nothing would otherwise fall through and try to
    # reach the real ES_HOST.
    print("LOG_SOURCE=kubernetes")
    print("LOG_SNAPSHOT_REUSE=false")
    if shard_count > DEFAULT_MAX_PODS:
        print(f"K8S_MAX_PODS={shard_count + 5}")
    if args.shard_bytes <= 0 and biggest > 10 * 1024 * 1024:
        print(f"K8S_MAX_BYTES_PER_POD={biggest + (1024 * 1024)}")
    print("=" * 72)

    if since_hours is not None:
        print(f"\nK8S_DEFAULT_SINCE_HOURS={since_hours} suppresses a false "
              "LOG_ROTATION banner. gaps.detect_rotation_gap\nfires when the "
              "oldest line is newer than (now - window), which is exactly what a\n"
              "freshly downloaded dump looks like -- it would stamp every trace "
              "INCOMPLETE.\nFixtures ignore the window when reading, so this only "
              "affects gap detection.")

    if shard_count > DEFAULT_MAX_PODS:
        print(f"\nK8S_MAX_PODS is needed: {shard_count} fixture pods exceeds the "
              f"default cap of {DEFAULT_MAX_PODS}.\nOver the cap discovery keeps "
              "only the most recently started pods -- and every fixture pod\n"
              "shares one start_time, so which ones survive would be arbitrary.")

    if unmatched:
        print(f"\nWARNING: {len(unmatched)} pod(s) are not named after any "
              "application_name, so\ndiscovery's name_contains match will not "
              "find them. Map them explicitly, e.g.\n"
              '  K8S_SERVICE_MAP={"<service>": {"name_contains": "<pod-name-fragment>"}}')
        for pod in unmatched[:5]:
            print(f"    {pod}")

    print("\nLOG_SNAPSHOT_REUSE=false is only while you iterate on the fixture:")
    print("left on, the first fetch's result is cached per event and replayed,")
    print("so later edits to the fixture appear to have no effect.\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert a downloaded prod log file into a K8S_FIXTURE_DIR tree.",
    )
    parser.add_argument("input", help="the downloaded log file")
    parser.add_argument("--out", default="local_fixtures",
                        help="fixture root to write (default: local_fixtures)")
    parser.add_argument("--namespace", default="offline",
                        help="fixture namespace; set K8S_DEFAULT_NAMESPACE to match")
    parser.add_argument("--app", default=None,
                        help="force every line to one service instead of reading "
                             "application_name from the record")
    parser.add_argument("--default-app", default="unknown-service",
                        help="service name for lines that carry none")
    parser.add_argument("--container", default=None,
                        help="container name recorded in meta.json "
                             "(default: the pod's application_name, which is "
                             "what parser.py attributes untagged lines to)")
    parser.add_argument("--shard-bytes", type=int, default=DEFAULT_SHARD_BYTES,
                        help="roll to a new fixture pod past this many bytes; "
                             "0 writes one pod per real pod")
    args = parser.parse_args(argv)
    return build(args)


if __name__ == "__main__":
    raise SystemExit(main())
