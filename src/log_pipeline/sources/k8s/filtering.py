"""
Client-side identifier filtering and context windows
(KUBERNETES_LOGS_PLAN.md 5.6).

The Kubernetes API has no server-side grep: `read_namespaced_pod_log` returns
the whole buffer. Filtering therefore happens here, against the streamed
lines, which is also why streaming (rather than buffering) is mandatory.

Context windows matter more than they look. A bare identifier match is often
one line of a stack trace whose useful part -- the exception, the caused-by
chain -- never repeats the identifier. Without surrounding context the
reduction pipeline receives isolated lines and the Investigator has nothing to
reason over.

They also carry a cost that has to be paid downstream. On a pod serving
packets concurrently, the lines around a match belong to *other* refIds, and
one of them erroring has nothing to do with the packet under investigation.
So `feed` reports, per line, whether the line itself matched -- context lines
are still emitted, but they are emitted labelled. `retrieval` stamps the flag
onto the record, `pipeline` renders it, and `dlt/corroborate.py` uses it to
avoid reading a neighbour's exception as evidence that this packet's stack
trace is lying.
"""
import os
from collections import deque
from typing import Callable, Optional


#: Payload fields that may carry a correlation id worth grepping for.
DEFAULT_SEARCH_FIELDS = ("eventId", "refId")


def search_fields() -> tuple:
    """Which payload fields supply identifier values (`K8S_SEARCH_FIELDS`).

    This env var was documented in .env.example from the start but read
    nowhere in the codebase, and `extra_identifiers` was never populated, so
    only `eventId` was ever matched. If the services log `refId` instead, the
    Kubernetes source returned zero lines and the chain silently fell through
    to Elasticsearch with no signal that identifier matching was the reason
    (F11).
    """
    raw = os.environ.get("K8S_SEARCH_FIELDS", "").strip()
    if not raw:
        return DEFAULT_SEARCH_FIELDS
    fields = tuple(part.strip() for part in raw.split(",") if part.strip())
    return fields or DEFAULT_SEARCH_FIELDS


def identifiers_from_payload(payload: dict) -> tuple:
    """Pull every configured search field's value out of a Kafka payload.

    Looks in `packetMetaData` first (where refId/srn live) and then at the
    top level (where eventId lives), so one field list covers both.
    """
    if not payload:
        return ()

    packet_meta = payload.get("packetMetaData") or {}
    values = []
    for field in search_fields():
        value = packet_meta.get(field)
        if value in (None, ""):
            value = payload.get(field)
        if value in (None, ""):
            continue
        value = str(value)
        if value not in values:
            values.append(value)
    return tuple(values)


def resolve_search_values(identifier: str,
                          extra: Optional[list] = None) -> list:
    """Identifier values to match lines against.

    Searching more than one value is cheap insurance against Open Question 1
    -- we do not yet know which id the services actually log.
    """
    values = [identifier] if identifier else []
    for value in (extra or []):
        if value and value not in values:
            values.append(value)
    return values


def build_matcher(values: list, case_sensitive: bool = True) -> Callable[[str], bool]:
    """Return a predicate matching any of `values` in a line.

    Case-sensitive by default: identifiers are UUIDs and reference numbers, so
    a case-insensitive match buys nothing and risks false positives.
    """
    if not values:
        return lambda _line: False

    if case_sensitive:
        needles = list(values)
        return lambda line: any(needle in line for needle in needles)

    lowered = [v.lower() for v in values]
    return lambda line: any(needle in line.lower() for needle in lowered)


class KeepAllSelector:
    """Emit every line. The default when no identifier filtering applies.

    Every line is reported as a match. Not a white lie: with no identifier to
    filter on there is no such thing as a context line here, and reporting
    these as context would tell corroboration to discard the entire trace.
    """

    def feed(self, line: str) -> list:
        return [(line, True)]

    def reset(self):
        pass


class ContextWindowSelector:
    """Emit matching lines plus surrounding context, merging overlaps.

    Fed one line at a time in order, returning `(line, matched)` pairs to emit
    for that input, where `matched` says whether that line itself carried a
    searched identifier. Overlapping windows merge naturally: a fresh match
    resets the trailing counter, and lines already emitted are never
    re-buffered, so no line is emitted twice.

    The flag rather than a second filtering pass because the emitted list
    interleaves the two kinds -- a match arrives with its leading context in
    one call -- so the distinction is only knowable here, at the point the
    matcher runs.
    """

    def __init__(self, matcher: Callable[[str], bool],
                 before: int = 5, after: int = 20):
        self._matcher = matcher
        self._before = deque(maxlen=max(0, before))
        self._after = max(0, after)
        self._after_remaining = 0

    def feed(self, line: str) -> list:
        if self._matcher(line):
            # Buffered leading context never matched -- that is why it was
            # buffered rather than emitted when it arrived.
            emitted = [(held, False) for held in self._before] + [(line, True)]
            self._before.clear()
            self._after_remaining = self._after
            return emitted

        if self._after_remaining > 0:
            self._after_remaining -= 1
            return [(line, False)]

        # Not emitted: hold it as potential leading context for a later match.
        if self._before.maxlen:
            self._before.append(line)
        return []

    def reset(self):
        self._before.clear()
        self._after_remaining = 0


def context_lines_before() -> int:
    try:
        return int(os.environ.get("K8S_CONTEXT_LINES_BEFORE", "5"))
    except ValueError:
        return 5


def context_lines_after() -> int:
    try:
        return int(os.environ.get("K8S_CONTEXT_LINES_AFTER", "20"))
    except ValueError:
        return 20


def build_selector(identifier: str, extra: Optional[list] = None):
    """Build the selector for a fetch.

    An empty identifier yields `KeepAllSelector`: filtering on nothing would
    otherwise silently discard the entire trace.
    """
    values = resolve_search_values(identifier, extra)
    if not values:
        return KeepAllSelector()
    return ContextWindowSelector(
        build_matcher(values),
        before=context_lines_before(),
        after=context_lines_after(),
    )
