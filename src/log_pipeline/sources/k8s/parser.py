"""
Kubernetes log line parsing (KUBERNETES_LOGS_PLAN.md 5.5).

THIS IS THE HIGHEST-RISK COMPONENT IN THE DESIGN.

`branch_on_error` chooses between the "stuck packet" and "clean rejection"
paths purely on `level == "ERROR"`. If parsing silently yields INFO for
everything, the ERROR branch never fires and every stuck packet is
misclassified as a clean rejection -- a correctness regression that no test of
the Kubernetes client itself would catch.

Two mitigations live here:
  * a regex fallback for non-JSON formats, so a level is still recovered;
  * a parse-failure rate returned to the caller, which raises
    LEVEL_PARSE_DEGRADED when the format is not understood at all.

Kubernetes returns raw text where Elasticsearch returned structured _source,
so every line goes through:
  1. strip the kubelet RFC3339Nano timestamp prefix (from timestamps=True)
  2. try JSON  (Spring Boot + logstash encoder and similar)
  3. regex fallback for a level token
  4. default INFO only after both fail
"""
import json
import re
from dataclasses import dataclass, field
from typing import Optional

from src.log_pipeline.types import LogRecord

#: Levels we recognise, ordered so longer tokens match first.
KNOWN_LEVELS = ("CRITICAL", "ERROR", "WARNING", "WARN", "INFO", "DEBUG", "TRACE", "FATAL")

#: Normalisation onto the vocabulary Stages 2-4 expect.
_LEVEL_ALIASES = {
    "WARNING": "WARN",
    "FATAL": "ERROR",
    "CRITICAL": "ERROR",
    "SEVERE": "ERROR",
}

#: The kubelet prefixes each line with an RFC3339Nano timestamp then a space.
_KUBELET_TS = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))\s+"
)

#: A level token in the first ~80 characters, on a word boundary so that
#: "ERRORS_TOTAL=0" or a message merely containing "error" does not match.
_LEVEL_TOKEN = re.compile(
    r"\b(" + "|".join(KNOWN_LEVELS) + r"|SEVERE)\b", re.IGNORECASE
)
_LEVEL_SCAN_WINDOW = 80

#: Field names commonly carrying the level / message / service in JSON logs.
_JSON_LEVEL_KEYS = ("level", "severity", "log_level", "loglevel", "lvl")
_JSON_MESSAGE_KEYS = ("message", "msg", "log", "text")
_JSON_APP_KEYS = ("application_name", "app", "service", "logger_name", "service_name")
_JSON_TIME_KEYS = ("@timestamp", "timestamp", "time", "ts")


@dataclass
class ParseStats:
    """How well the parser understood this stream."""

    total: int = 0
    level_parsed: int = 0
    json_lines: int = 0

    @property
    def level_failure_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return 1.0 - (self.level_parsed / self.total)

    @property
    def json_ratio(self) -> float:
        if self.total == 0:
            return 0.0
        return self.json_lines / self.total


@dataclass
class ParsedStream:
    records: list = field(default_factory=list)
    stats: ParseStats = field(default_factory=ParseStats)


def normalise_level(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    upper = str(raw).strip().upper()
    if upper in _LEVEL_ALIASES:
        return _LEVEL_ALIASES[upper]
    if upper in KNOWN_LEVELS:
        return upper
    return None


def split_kubelet_timestamp(line: str) -> tuple:
    """Return (timestamp_or_None, remainder).

    The kubelet timestamp is authoritative for ordering even when the
    application's own format is unparseable, which is why `timestamps=True`
    is mandatory on every read.
    """
    match = _KUBELET_TS.match(line)
    if not match:
        return None, line
    return match.group(1), line[match.end():]


def _first_key(payload: dict, keys) -> Optional[str]:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def parse_line(line: str, default_app: str = "unknown-service") -> tuple:
    """Parse one raw line into (LogRecord, level_was_parsed, was_json).

    `level_was_parsed` distinguishes a genuine INFO from a defaulted one --
    without it, a totally unparseable stream looks identical to a healthy
    all-INFO stream.
    """
    kubelet_ts, remainder = split_kubelet_timestamp(line.rstrip("\n"))
    remainder = remainder.strip()

    level: Optional[str] = None
    message = remainder
    app_name = default_app
    app_timestamp: Optional[str] = None
    was_json = False

    if remainder.startswith("{"):
        try:
            payload = json.loads(remainder)
            if isinstance(payload, dict):
                was_json = True
                level = normalise_level(_first_key(payload, _JSON_LEVEL_KEYS))
                message = _first_key(payload, _JSON_MESSAGE_KEYS) or remainder
                app_name = _first_key(payload, _JSON_APP_KEYS) or default_app
                app_timestamp = _first_key(payload, _JSON_TIME_KEYS)
        except (ValueError, TypeError):
            was_json = False

    if level is None:
        window = remainder[:_LEVEL_SCAN_WINDOW]
        match = _LEVEL_TOKEN.search(window)
        if match:
            level = normalise_level(match.group(1))

    level_was_parsed = level is not None

    record: LogRecord = {
        # Prefer the kubelet timestamp: it is a single clock per node, whereas
        # application timestamps vary in format and timezone handling.
        "timestamp": kubelet_ts or app_timestamp or "",
        "level": level or "INFO",
        "message": str(message),
        "app_name": str(app_name),
        "source": "kubernetes",
    }
    return record, level_was_parsed, was_json


def parse_stream(text: str, default_app: str = "unknown-service") -> ParsedStream:
    """Parse a whole pod log payload."""
    result = ParsedStream()
    for line in text.splitlines():
        if not line.strip():
            continue
        record, level_ok, was_json = parse_line(line, default_app=default_app)
        result.records.append(record)
        result.stats.total += 1
        if level_ok:
            result.stats.level_parsed += 1
        if was_json:
            result.stats.json_lines += 1
    return result
