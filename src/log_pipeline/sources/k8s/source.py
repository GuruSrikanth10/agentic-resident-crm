"""
KubernetesLogSource -- the `LogSource` implementation (Phase 9).

Ties together discovery, retrieval, filtering, gap detection, redaction, and
snapshot reuse behind the same protocol the Elasticsearch source implements,
so Stages 2-4 cannot tell the two apart.
"""
import time

import pybreaker

from src.log_pipeline import snapshot
from src.utils.resilience import k8s_breaker
from src.log_pipeline.sources.k8s import discovery, gaps, mockfile, retrieval
from src.log_pipeline.sources.k8s.filtering import build_selector, resolve_search_values
from src.log_pipeline.types import (
    FetchContext,
    FetchDiagnostics,
    FetchResult,
    TimeWindow,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class KubernetesLogSource:
    """Reads pod logs directly via the Kubernetes API."""

    name = "kubernetes"

    def fetch(self, identifier: str, window: TimeWindow,
              ctx: FetchContext) -> FetchResult:
        """Fetch pod logs, guarded by the Kubernetes circuit breaker.

        The breaker stops a cluster that is down entirely from costing every
        packet a full discovery + fan-out timeout. It is opened by repeated
        failures and, once open, fails fast -- which the fallback chain then
        treats as "this source produced nothing" and falls through to
        Elasticsearch (F8).
        """
        try:
            return k8s_breaker.call(self._fetch, identifier, window, ctx)
        except pybreaker.CircuitBreakerError:
            logger.bind(event_id=ctx.event_id).warning(
                "Kubernetes circuit breaker is OPEN; failing fast."
            )
            return FetchResult.failure(self.name, "kubernetes circuit breaker open")

    def _fetch(self, identifier: str, window: TimeWindow,
               ctx: FetchContext) -> FetchResult:
        log = logger.bind(event_id=ctx.event_id)
        started = time.monotonic()

        # Mock file before anything else. It replaces the transport only --
        # identifier matching, context windows, parsing, redaction and gap
        # detection all still run (see mockfile.py) -- so Stages 2-4 receive
        # exactly the record shape a live cluster would have produced. Checked
        # ahead of the snapshot so the file is unambiguously the single source
        # of truth while it is set.
        if mockfile.is_active():
            return mockfile.fetch(
                identifier, window, ctx,
                extra_identifiers=_extra_identifiers(ctx),
                started=started,
            )

        # Snapshot first. Kubelet retention is minutes to hours, but retries,
        # DLQ replays, and checkpoint resumes re-enter this path much later --
        # reusing the capture keeps those deterministic and free (plan 4.3).
        if snapshot.reuse_enabled():
            cached = snapshot.load(ctx.event_id)
            if cached is not None:
                records, cached_gaps = cached
                return FetchResult(
                    records=records,
                    gaps=cached_gaps,
                    diagnostics=FetchDiagnostics(
                        source=self.name,
                        records_returned=len(records),
                        latency_ms=(time.monotonic() - started) * 1000.0,
                    ),
                )

        found = discovery.discover_targets(namespace=ctx.namespace, app=ctx.app)
        if not found.ok:
            log.warning("Kubernetes discovery failed", reason=found.reason)
            return FetchResult.failure(self.name, found.reason or "discovery failed")

        if not found.targets:
            # Looked successfully, found no pods. Distinct from a failure.
            return FetchResult(
                records=[],
                gaps=list(found.gaps),
                diagnostics=FetchDiagnostics(
                    source=self.name,
                    latency_ms=(time.monotonic() - started) * 1000.0,
                ),
            )

        # The searched identifiers are also the redaction allowlist: they are
        # operational ids, not resident PII, and scrubbing them would destroy
        # the investigation.
        allowlist = resolve_search_values(identifier, _extra_identifiers(ctx))

        outcome = retrieval.read_all(
            found.targets,
            window,
            selector_factory=lambda: build_selector(identifier, _extra_identifiers(ctx)),
            allowlist=allowlist,
        )

        collected = list(found.gaps) + list(outcome.gaps)

        rotation = gaps.detect_rotation_gap(
            outcome.oldest_line_timestamp, window, outcome.earliest_pod_start
        )
        if rotation:
            collected.append(rotation)

        collected.extend(gaps.detect_pod_replaced_gaps(found.targets, window))

        degraded = gaps.detect_parse_degradation_gap(outcome.stats)
        if degraded:
            collected.append(degraded)

        collected = gaps.dedupe_gaps(collected)
        gaps.log_gaps(collected, event_id=ctx.event_id)

        # Every pod failing is a could-not-look, not a confirmed-absent.
        every_pod_failed = (
            outcome.pods_queried > 0 and outcome.pods_failed == outcome.pods_queried
        )
        if every_pod_failed and not outcome.records:
            log.warning("Every Kubernetes pod read failed",
                        pods_queried=outcome.pods_queried)
            return FetchResult.failure(
                self.name, "all pod log reads failed", gaps=collected
            )

        latency_ms = (time.monotonic() - started) * 1000.0
        log.info(
            "Kubernetes fetch completed",
            total_matched=len(outcome.records),
            total_bytes=outcome.bytes_read,
            pods_ok=outcome.pods_queried - outcome.pods_failed,
            pods_failed=outcome.pods_failed,
            latency_ms=round(latency_ms, 1),
            gap_count=len(collected),
            # Which hops of the packet's journey these logs actually cover.
            # A count of matched lines means little without knowing whether
            # every service was reachable when they were collected.
            services_searched=found.services_searched,
            services_failed=found.services_failed,
        )

        # Only a successful fetch is snapshotted: caching a failure would
        # poison every later attempt with an empty result.
        if outcome.records:
            snapshot.save(ctx.event_id, outcome.records, gaps=collected,
                          source=self.name)

        return FetchResult(
            records=outcome.records,
            gaps=collected,
            diagnostics=FetchDiagnostics(
                source=self.name,
                records_returned=len(outcome.records),
                bytes_read=outcome.bytes_read,
                latency_ms=latency_ms,
                pods_queried=outcome.pods_queried,
                pods_failed=outcome.pods_failed,
                redaction_counts=outcome.redaction_counts,
            ),
        )


def _extra_identifiers(ctx: FetchContext) -> list:
    """Additional identifiers to match on alongside the primary one.

    We do not yet know whether the services log `eventId` or `refId`
    (Open Question 1), so a caller holding both should supply both.
    """
    return [value for value in (ctx.extra_identifiers or ()) if value]
