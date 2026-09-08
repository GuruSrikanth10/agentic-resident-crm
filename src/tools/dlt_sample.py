#!/usr/bin/env python3
"""Capture a corpus of real DLT messages for the Phase 0 decision gate.

See DLT_PLAN.md section 10, Phase 0.

Two modes. `--analyze` needs no broker, so the capture can be run on a host
with Kafka access and the analysis anywhere:

    capture   pull messages off the DLT and write them as JSON fixtures
    --analyze read a captured corpus and report the Phase 0 measurements

Deliberately never joins a consumer group. It `assign()`s partitions directly
and never commits, so it cannot rebalance anyone else's consumer, cannot
advance any group's offsets, and is safe to run against a live DLT while
developers are browsing the same topic in Kafka UI.

Usage:
    python -m src.tools.dlt_sample --limit 100
    python -m src.tools.dlt_sample --limit 500 --from earliest --output /tmp/corpus
    python -m src.tools.dlt_sample --limit 20 --no-redact   # never on real data
"""
import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.dlt.headers import decode_kafka_headers  # noqa: E402
from src.dlt.payload import decode_key, key_as_ref_id, resolve_ref_id  # noqa: E402
from src.log_pipeline import redaction  # noqa: E402

#: Headers whose values are structural Kafka/Spring metadata -- offsets,
#: partitions, epoch timestamps, topic names, Java FQCNs. They never carry
#: resident PII, and redacting them would corrupt the very fields the parser
#: keys on. A 12-digit epoch-second timestamp would otherwise be scrubbed as
#: an Aadhaar number.
STRUCTURAL_HEADERS = frozenset({
    "kafka_original-offset",
    "kafka_original-partition",
    "kafka_original-topic",
    "kafka_original-timestamp",
    "kafka_original-timestamp-type",
    "kafka_dlt-original-consumer-group",
    "kafka_exception-fqcn",
    "kafka_exception-cause-fqcn",
    "retry_topic-attempts",
    "retry_topic-original-timestamp",
    "retry_topic-backoff-timestamp",
    "__TypeId__",
})


#: Canonical implementation lives in `src.dlt.headers` so the consumer and this
#: tool decode headers identically. Re-exported under the name this module has
#: always used.
decode_headers = decode_kafka_headers


def redact_headers(headers: dict) -> dict:
    """Redact free-text header values, leaving structural metadata intact."""
    out = {}
    for name, value in headers.items():
        if value is None or name in STRUCTURAL_HEADERS:
            out[name] = value
        else:
            out[name] = redaction.redact_text(value).text
    return out


def decode_payload(raw_value):
    """Best-effort payload decode. Returns (parsed, raw_text)."""
    if raw_value is None:
        return None, None
    text = raw_value.decode("utf-8", errors="replace") if isinstance(raw_value, bytes) else str(raw_value)
    try:
        return json.loads(text), text
    except (ValueError, TypeError):
        return None, text


def _seek_targets(consumer, topic: str, limit: int, start_from: str):
    """Assign every partition and position it so we read at most `limit`."""
    from kafka.structs import TopicPartition

    partition_ids = consumer.partitions_for_topic(topic)
    if not partition_ids:
        raise SystemExit(f"Topic '{topic}' has no partitions visible to this client.")

    targets = [TopicPartition(topic, p) for p in sorted(partition_ids)]
    consumer.assign(targets)

    if start_from == "earliest":
        consumer.seek_to_beginning(*targets)
        return targets

    # Latest: walk back per-partition so the sample is the most recent traffic.
    per_partition = max(1, -(-limit // len(targets)))
    end_offsets = consumer.end_offsets(targets)
    beginning = consumer.beginning_offsets(targets)
    for tp in targets:
        start = max(beginning[tp], end_offsets[tp] - per_partition)
        consumer.seek(tp, start)
    return targets


def capture(topic: str, brokers: list, limit: int, start_from: str, do_redact: bool) -> list:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=15000,
    )
    try:
        _seek_targets(consumer, topic, limit, start_from)

        captured = []
        for msg in consumer:
            headers = decode_headers(msg.headers)
            if do_redact:
                headers = redact_headers(headers)

            parsed, raw_text = decode_payload(msg.value)
            if do_redact and raw_text is not None:
                raw_text = redaction.redact_text(raw_text).text
                try:
                    parsed = json.loads(raw_text)
                except (ValueError, TypeError):
                    parsed = None

            # The key is captured because the 2026-08-20 sample is keyed on
            # the refId. Phase 0 item 5 asks whether the log lane can filter
            # at all; if the key carries the refId corpus-wide, that question
            # is answered without needing the payload to parse.
            captured.append({
                "_source": {
                    "dlt_topic": topic,
                    "dlt_partition": msg.partition,
                    "dlt_offset": msg.offset,
                    "dlt_timestamp": msg.timestamp,
                    "redacted": do_redact,
                },
                "key": decode_key(msg.key),
                "headers": headers,
                "payload": parsed,
                "payload_raw": None if parsed is not None else raw_text,
            })
            if len(captured) >= limit:
                break
        return captured
    finally:
        consumer.close()


def summarise(captured: list) -> dict:
    """The few things measurable without Phases 1-2."""
    header_names = {}
    missing_stacktrace = 0
    for item in captured:
        for name in item["headers"]:
            header_names[name] = header_names.get(name, 0) + 1
        if not item["headers"].get("kafka_exception-stacktrace"):
            missing_stacktrace += 1
    return {
        "captured": len(captured),
        "header_names": dict(sorted(header_names.items(), key=lambda kv: -kv[1])),
        "missing_stacktrace": missing_stacktrace,
    }


#: Payload keys that plausibly hold the log-correlation identifier. Phase 0
#: item 4 asks for the real path; this reports every place one was found.
_REFID_KEYS = ("refId", "ref_id", "referenceId", "referenceID", "rid")


def _find_refid_paths(node, prefix="", depth=0, found=None):
    """Every dotted path at which a refId-shaped key appears."""
    found = found if found is not None else []
    if depth > 8:
        return found
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key in _REFID_KEYS and isinstance(value, (str, int)):
                found.append(path)
            _find_refid_paths(value, path, depth + 1, found)
    elif isinstance(node, list) and node:
        _find_refid_paths(node[0], f"{prefix}[]", depth + 1, found)
    return found


def analyse(corpus_dir: Path) -> dict:
    """Compute the Phase 0 measurements over a captured corpus.

    Needs no broker. Fills DLT_PLAN.md section 11 items 1, 2, 3, 4 and 7.
    """
    from src.dlt.classify import classify
    from src.dlt import registry
    from src.dlt.headers import parse_headers
    from src.dlt.stacktrace import (
        build_signature,
        compute_fingerprint,
        normalise_frames,
        parse_stacktrace,
    )

    classes = {"A": 0, "B": 0, "C": 0, "U": 0}
    fingerprints = {}
    truncated = 0
    missing_backoff = 0
    refid_paths = {}
    lags_seconds = []
    total = 0
    key_stats = {"present": 0, "usable": 0, "agrees_with_payload": 0,
                 "disagrees_with_payload": 0, "key_only": 0, "payload_only": 0,
                 "neither": 0}
    refid_sources = {}

    for path in sorted(corpus_dir.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        total += 1

        headers = parse_headers(item.get("headers"))
        trace = parse_stacktrace(headers.stacktrace)
        # Same hook production uses, so this report cannot drift from what the
        # consumers will actually decide.
        result = classify(trace, headers.exception_message,
                          code_class=registry.class_for)

        classes[result.failure_class.value] += 1
        if trace.truncated:
            truncated += 1
        if headers.anchor_is_fallback:
            missing_backoff += 1

        frames = normalise_frames(trace.root_frames)
        fingerprint = compute_fingerprint(trace.root.fqcn if trace.root else None,
                                          frames, result.business_code or "")
        entry = fingerprints.setdefault(fingerprint, {
            "count": 0,
            "signature": build_signature(trace.root.fqcn if trace.root else None,
                                         frames, result.business_code or ""),
            "class": result.failure_class.value,
        })
        entry["count"] += 1

        for found in _find_refid_paths(item.get("payload")):
            refid_paths[found] = refid_paths.get(found, 0) + 1

        # Phase 0 item 5: does the record key carry the refId corpus-wide?
        # Run through the same resolver the consumer uses, so what this reports
        # is what production would actually do -- not a second opinion that
        # could drift from it.
        key = decode_key(item.get("key"))
        extraction = resolve_ref_id(item.get("payload"), key=key,
                                    type_id=headers.type_id)
        refid_sources[extraction.source] = refid_sources.get(extraction.source, 0) + 1

        if key:
            key_stats["present"] += 1
        if key_as_ref_id(key):
            key_stats["usable"] += 1
        if key_as_ref_id(key) and extraction.payload_ref_id:
            key_stats["disagrees_with_payload" if extraction.mismatch
                      else "agrees_with_payload"] += 1
        elif key_as_ref_id(key):
            key_stats["key_only"] += 1
        elif extraction.payload_ref_id:
            key_stats["payload_only"] += 1
        else:
            key_stats["neither"] += 1

        arrived = (item.get("_source") or {}).get("dlt_timestamp")
        if arrived and headers.last_attempt_ms:
            lags_seconds.append((arrived - headers.last_attempt_ms) / 1000.0)

    top = sorted(fingerprints.items(), key=lambda kv: -kv[1]["count"])[:10]
    llm_eligible = sum(e["count"] for e in fingerprints.values() if e["class"] == "A")

    return {
        "messages": total,
        "class_distribution": classes,
        "distinct_fingerprints": len(fingerprints),
        "top_fingerprints": [
            {"fingerprint": fp[:16], "count": e["count"],
             "class": e["class"], "signature": e["signature"]}
            for fp, e in top
        ],
        "truncated_stacktraces": truncated,
        "missing_backoff_timestamp": missing_backoff,
        "refid_paths": dict(sorted(refid_paths.items(), key=lambda kv: -kv[1])),
        "record_key": key_stats,
        "refid_sources": dict(sorted(refid_sources.items(), key=lambda kv: -kv[1])),
        "arrival_lag_seconds": {
            "samples": len(lags_seconds),
            "min": round(min(lags_seconds), 1) if lags_seconds else None,
            "median": round(sorted(lags_seconds)[len(lags_seconds) // 2], 1) if lags_seconds else None,
            "max": round(max(lags_seconds), 1) if lags_seconds else None,
        },
        "class_a_messages": llm_eligible,
        "class_a_distinct_fingerprints": sum(
            1 for e in fingerprints.values() if e["class"] == "A"),
    }


def print_analysis(report: dict) -> None:
    total = report["messages"]
    print(f"\nCorpus: {total} messages\n")

    print("Class distribution (section 11.1):")
    for name, count in report["class_distribution"].items():
        share = f"{100 * count / total:.1f}%" if total else "-"
        print(f"  {name}  {count:6d}  {share:>7}")

    print(f"\nDistinct fingerprints (11.2): {report['distinct_fingerprints']}")
    if total:
        print(f"  reduction ratio: {total / max(1, report['distinct_fingerprints']):.1f}x "
              "messages per fingerprint")
    print("\n  Top fingerprints:")
    for entry in report["top_fingerprints"]:
        print(f"    {entry['count']:5d}  [{entry['class']}]  {entry['signature']}")

    print(f"\nTruncated stacktraces (11.3): {report['truncated_stacktraces']}")
    print(f"Missing backoff timestamp:    {report['missing_backoff_timestamp']}")

    keys = report["record_key"]
    print(f"\nrefId resolution across the corpus ({report['messages']} messages):")
    for source, count in report["refid_sources"].items():
        print(f"  {count:5d}  {source}")
    print(f"  record key present: {keys['present']}, "
          f"usable as a refId: {keys['usable']}")
    print(f"  key and payload agree: {keys['agrees_with_payload']}, "
          f"disagree: {keys['disagrees_with_payload']}")
    print(f"  key only: {keys['key_only']}, payload only: {keys['payload_only']}, "
          f"no identifier at all: {keys['neither']}")
    if keys["disagrees_with_payload"]:
        print("  -> DISAGREEMENTS FOUND. The configured payload path is likely")
        print("     wrong for at least one __TypeId__. Inspect before trusting")
        print("     the log lane on those types.")
    if keys["neither"]:
        print(f"  -> {keys['neither']} message(s) have no correlation id at all;")
        print("     those cases run header-only and cannot be corroborated.")

    print("\nrefId paths found in payloads (11.4):")
    if report["refid_paths"]:
        for path, count in report["refid_paths"].items():
            print(f"  {count:5d}  {path}")
        print("  -> set DLT_REFID_PATH to the most frequent path above")
    else:
        print("  NONE FOUND. Phase 5 is gated on this -- without a correlation")
        print("  id the log lane cannot filter. Inspect a payload by hand.")

    lag = report["arrival_lag_seconds"]
    print(f"\nDLT arrival lag after last attempt (11.7), {lag['samples']} samples:")
    print(f"  min {lag['min']}s  median {lag['median']}s  max {lag['max']}s")

    a_msgs = report["class_a_messages"]
    a_fps = report["class_a_distinct_fingerprints"]
    print(f"\nCost model: {a_msgs} Class A messages across {a_fps} fingerprints.")
    if a_msgs:
        print(f"  With reuse, LLM runs drop from {a_msgs} to ~{a_fps} "
              f"({100 * (1 - a_fps / a_msgs):.0f}% saving).")


def main():
    parser = argparse.ArgumentParser(description="Capture DLT messages for the Phase 0 corpus")
    parser.add_argument("--analyze", metavar="DIR",
                        help="Analyse a captured corpus instead of capturing. Needs no broker.")
    parser.add_argument("--topic", default=os.environ.get("DLT_CONSUMER_TOPIC_NAME", "packet-dlt"))
    parser.add_argument("--brokers", default=os.environ.get("KAFKA_CONSUMER_BROKERS", "localhost:9092"))
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--from", dest="start_from", choices=("latest", "earliest"), default="latest")
    parser.add_argument("--output", default="tests/fixtures/dlt/corpus")
    parser.add_argument("--no-redact", dest="redact", action="store_false",
                        help="Skip PII redaction. Never use against real data.")
    parser.set_defaults(redact=True)
    args = parser.parse_args()

    if args.analyze:
        corpus = Path(args.analyze)
        if not corpus.is_dir():
            raise SystemExit(f"No such corpus directory: {corpus}")
        report = analyse(corpus)
        if not report["messages"]:
            raise SystemExit(f"No message JSON files found in {corpus}")
        print_analysis(report)
        (corpus / "_analysis.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nWrote {corpus}/_analysis.json -- paste into DLT_PLAN.md section 11.")
        return

    brokers = [b.strip() for b in args.brokers.split(",") if b.strip()]
    print(f"Capturing up to {args.limit} messages from '{args.topic}' ({args.start_from}) ...")

    captured = capture(args.topic, brokers, args.limit, args.start_from, args.redact)
    if not captured:
        print("No messages captured. Is the topic empty, or the name wrong?")
        return

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    for item in captured:
        src = item["_source"]
        name = f"{src['dlt_partition']:04d}-{src['dlt_offset']:012d}.json"
        (out_dir / name).write_text(json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")

    summary = summarise(captured)
    (out_dir / "_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\nWrote {summary['captured']} messages to {out_dir}/")
    if not args.redact:
        print("WARNING: captured WITHOUT redaction. Do not commit this corpus.")
    if summary["missing_stacktrace"]:
        print(f"WARNING: {summary['missing_stacktrace']} message(s) carry no stacktrace header.")
    print("\nHeader names seen:")
    for name, count in summary["header_names"].items():
        print(f"  {count:5d}  {name}")
    print("\nNext: run `dlt_report.py --corpus` once Phases 1-2 land to fill "
          "DLT_PLAN.md section 11.")


if __name__ == "__main__":
    main()
