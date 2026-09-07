"""
Offline mock log file for the Kubernetes source.

Purpose: let a developer with no cluster access exercise the *whole* pipeline
against a captured trace. `K8S_MOCK_LOG_FILE` replaces one thing and one thing
only -- the transport that would have streamed bytes out of the kubelet. Every
stage after it is the real code path:

  * `build_selector` does identifier matching and context windows, so
    `identifier_match` is computed here exactly as it is against a live pod
    (which is why two event ids read from the same file return different
    record counts);
  * `parse_line` recovers level/app/timestamp, so `branch_on_error` keys off
    the same levels;
  * redaction, `ParseStats`, LEVEL_PARSE_DEGRADED and TRUNCATED gaps all run;
  * Stages 2-4 (reduction, Drain3, guardrails) are untouched and cannot tell
    the difference.

This is deliberately NOT `K8S_FIXTURE_DIR` (see fixtures.py). That path
materialises real `V1Pod` objects to exercise discovery and per-pod fan-out,
and needs a namespace/pod directory layout plus the `kubernetes` client. This
one takes a single flat text file and skips discovery entirely, because the
machine running it cannot reach an API server at all.

Accepted line formats, tried in order:

  1. The pipeline's own rendered form, so a `reduced_logs.txt` or a trace
     pasted out of a casebook can be fed straight back in:

         [<timestamp>] [<app>@<pod>] [<LEVEL>] [context] <message>

     `[context]` is stripped rather than honoured: it is recomputed by the
     selector below against the identifier actually being searched, which is
     the point of routing the file through the real selector.

  2. Anything else -- raw kubelet output, a `kubectl logs` capture, JSON
     lines, stack-trace continuations -- falls through to `parse_line`,
     exactly as a live stream would.
"""
import os
import re
import time
from typing import Optional

from src.log_pipeline import redaction
from src.log_pipeline.sources.k8s import gaps
from src.log_pipeline.sources.k8s.filtering import build_selector, resolve_search_values
from src.log_pipeline.sources.k8s.parser import ParseStats, parse_line
from src.log_pipeline.types import (
    EvidenceGap,
    FetchContext,
    FetchDiagnostics,
    FetchResult,
    GapType,
    TimeWindow,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Format 1 above. The level group is deliberately narrow (letters only) so a
#: message that merely opens with three bracketed fields is not mistaken for a
#: rendered line and silently stripped of its first words.
_RENDERED_LINE = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+"
    r"\[(?P<origin>[^\]]*)\]\s+"
    r"\[(?P<level>[A-Za-z]+)\]"
    r"(?P<ctx>\s+\[context\])?"
    r"\s?(?P<message>.*)$"
)


def mock_file_path() -> Optional[str]:
    """Read at call time, never at import, so tests can set it per-case."""
    raw = (os.environ.get("K8S_MOCK_LOG_FILE") or "").strip()
    return raw or None


def is_active() -> bool:
    path = mock_file_path()
    return bool(path) and os.path.isfile(path)


def _default_app() -> str:
    return os.environ.get("K8S_MOCK_APP_NAME", "mock-service")


def _default_pod() -> str:
    return os.environ.get("K8S_MOCK_POD_NAME", "mock-file")


def _apply_window() -> bool:
    """Whether to drop records outside the requested TimeWindow.

    Off by default, and that default is the important part. A captured trace
    is replayed hours or weeks after the timestamps inside it, so honouring
    `since_seconds` would return zero records and present it as
    "looked, found nothing" -- the one failure mode a debugging aid must not
    have. Set K8S_MOCK_APPLY_WINDOW=true when the window itself is what you
    are testing.
    """
    return os.environ.get("K8S_MOCK_APPLY_WINDOW", "false").lower() == "true"


def _max_bytes() -> int:
    """Same cap as a real pod read, so the TRUNCATED path stays reachable."""
    try:
        return int(os.environ.get("K8S_MAX_BYTES_PER_POD", str(10 * 1024 * 1024)))
    except ValueError:
        return 10 * 1024 * 1024


def _split_origin(origin: str) -> tuple:
    """`app@pod` -> (app, pod). A bare value is an app with no pod attribution.

    Returning None rather than a placeholder pod matters: `pipeline._origin`
    renders `app@pod` only when a pod is present, so a file without pod
    attribution round-trips to the same text it came from.
    """
    origin = (origin or "").strip()
    if not origin:
        return _default_app(), None
    app, sep, pod = origin.partition("@")
    if not sep:
        return app.strip() or _default_app(), None
    return app.strip() or _default_app(), pod.strip() or None


def _parse_rendered(line: str) -> Optional[dict]:
    """Parse format 1, or return None to let `parse_line` handle the line."""
    match = _RENDERED_LINE.match(line)
    if not match:
        return None

    level = match.group("level").upper()
    # A bracketed token that is not a level means this is an ordinary message
    # that happens to start with brackets -- hand it to parse_line untouched.
    if level not in ("ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE",
                     "FATAL", "CRITICAL", "SEVERE"):
        return None

    app, pod = _split_origin(match.group("origin"))
    return {
        "timestamp": match.group("ts").strip(),
        "level": level,
        "message": match.group("message"),
        "app_name": app,
        "pod_name": pod,
    }


def _iter_lines(path: str, cap: int):
    """Yield (line, bytes_so_far, hit_cap), mirroring retrieval's stream shape.

    Read as UTF-8 with `errors="replace"` because a captured trace routinely
    carries whatever encoding the origin service emitted, and a debugging aid
    that dies on one bad byte is useless.
    """
    total = 0
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > cap:
                yield line, total, True
                return
            yield line, total, False


def _timestamp_within(raw: str, cutoff: float, until: Optional[float]) -> bool:
    """Best-effort window test. An unparseable timestamp is kept, not dropped.

    Dropping it would silently delete evidence over a formatting detail, which
    is the same mistake as windowing the file by default.
    """
    from datetime import datetime, timezone

    text = (raw or "").strip()
    if not text:
        return True
    # RFC3339Nano carries 9 fractional digits; fromisoformat accepts at most 6.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text.replace("Z", "+00:00"))
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    epoch = parsed.timestamp()
    if epoch < cutoff:
        return False
    return not (until is not None and epoch > until)


def fetch(identifier: str, window: TimeWindow, ctx: FetchContext,
          extra_identifiers: Optional[list] = None,
          started: Optional[float] = None) -> FetchResult:
    """Read the mock file as though it were the merged output of every pod."""
    path = mock_file_path()
    started = started if started is not None else time.monotonic()
    log = logger.bind(event_id=ctx.event_id)
    log.info("Kubernetes source using mock log file", path=path)

    extra = list(extra_identifiers or [])
    # Identical to the live path: the searched identifiers double as the
    # redaction allowlist, since they are operational ids rather than PII.
    allowlist = resolve_search_values(identifier, extra)
    selector = build_selector(identifier, extra)
    selector.reset()

    cutoff = until = None
    if _apply_window():
        now = time.time()
        cutoff = now - (window.hours * 3600.0)
        until = window.until if getattr(window, "until", None) else None

    records, stats = [], ParseStats()
    total_bytes, truncated, windowed_out = 0, False, 0
    #: Last timestamp actually seen, carried onto continuation lines. A live
    #: read has `timestamps=True`, so the kubelet stamps every line including
    #: the frames of a stack trace; a file does not. Leaving those blank would
    #: sort them to the front of the trace in `cluster_logs` and detach them
    #: from the exception they belong to.
    last_timestamp = ""

    for line, total_bytes, hit_cap in _iter_lines(path, _max_bytes()):
        if line.strip():
            # The selector is fed the raw line and is what decides whether a
            # line matched or was pulled in as context -- the one place that
            # knows, exactly as in retrieval._read_instance.
            for emitted, matched in selector.feed(line):
                rendered = _parse_rendered(emitted.rstrip("\n"))
                if rendered is not None:
                    record = {
                        "timestamp": rendered["timestamp"],
                        "level": rendered["level"],
                        "message": rendered["message"],
                        "app_name": rendered["app_name"],
                    }
                    if rendered["pod_name"]:
                        record["pod_name"] = rendered["pod_name"]
                    level_ok, was_json = True, False
                else:
                    record, level_ok, was_json = parse_line(
                        emitted, default_app=_default_app()
                    )

                if record.get("timestamp"):
                    last_timestamp = record["timestamp"]
                else:
                    record["timestamp"] = last_timestamp

                if cutoff is not None and not _timestamp_within(
                    record.get("timestamp", ""), cutoff, until
                ):
                    windowed_out += 1
                    continue

                record.setdefault("pod_name", _default_pod())
                record["container"] = record.get("app_name") or _default_app()
                record["container_instance"] = "current"
                record["source"] = "kubernetes"
                record["identifier_match"] = matched
                records.append(record)

                stats.total += 1
                if level_ok:
                    stats.level_parsed += 1
                if was_json:
                    stats.json_lines += 1

        if hit_cap:
            truncated = True
            break

    redaction_counts = redaction.redact_records(records, allowlist=allowlist) or {}

    collected = []
    if truncated:
        collected.append(EvidenceGap(
            GapType.TRUNCATED,
            f"mock log file {path} hit the {_max_bytes()} byte cap; "
            f"later lines were not read.",
            {"mock_file": path},
        ))
    degraded = gaps.detect_parse_degradation_gap(stats)
    if degraded:
        collected.append(degraded)
    collected = gaps.dedupe_gaps(collected)
    gaps.log_gaps(collected, event_id=ctx.event_id)

    latency_ms = (time.monotonic() - started) * 1000.0
    log.info(
        # logging_config._add_section_separator turns any "completed"
        # event into the --- BANNER --- form; adding dashes here doubles them.
        "Kubernetes mock-file fetch completed",
        mock_file=path,
        total_matched=len(records),
        bytes_read=total_bytes,
        windowed_out=windowed_out,
        gap_count=len(collected),
        latency_ms=round(latency_ms, 1),
    )

    # Deliberately no snapshot.save(): re-reading the file is already
    # deterministic and free, and persisting mock records under a real
    # event_id would let a later live run reuse them as though they had come
    # from the cluster.
    return FetchResult(
        records=records,
        gaps=collected,
        diagnostics=FetchDiagnostics(
            source="kubernetes",
            records_returned=len(records),
            bytes_read=total_bytes,
            latency_ms=latency_ms,
            pods_queried=0,
            pods_failed=0,
            redaction_counts=redaction_counts,
        ),
    )
