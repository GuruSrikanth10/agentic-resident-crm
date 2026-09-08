"""
Ordered source chain with fallback (KUBERNETES_LOGS_PLAN.md 4.4).

`LOG_SOURCE` is an ordered, comma-separated chain. A single value means a
single source; several mean fallback in order:

    kubernetes,elastic    try Kubernetes, fall back to Elasticsearch (the default)
    elastic,kubernetes    the reverse
    elastic               Elasticsearch only (pre-Kubernetes-source behaviour)
    kubernetes            Kubernetes only, no fallback

Default is Kubernetes-first: pod logs are the more complete source for the
recent window (Elasticsearch is the one known to drop lines -- see
KUBERNETES_LOGS_PLAN.md section 2), and a misconfigured or unreachable
cluster fails fast (client resolution is cached after the first attempt; see
sources/k8s/client.py) so this degrades to today's Elasticsearch-only
behaviour with negligible added latency when Kubernetes isn't set up.

Fallback triggers when a source fails (`ok=False`) OR returns no records --
both mean "we did not get logs here", which is the operative condition.

Sources are never merged or de-duplicated: one wins per fetch. Kubernetes
supplements Elasticsearch rather than combining with it, and a dedup bug
across sources with different clock sources would silently delete evidence,
which is precisely the failure this whole project exists to eliminate.
"""
import os
from typing import Optional

from src.log_pipeline.sources.base import LogSource
from src.log_pipeline.sources.elastic import ElasticLogSource
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

DEFAULT_CHAIN = "kubernetes,elastic"


def _build_source(name: str) -> Optional[LogSource]:
    if name == "elastic":
        return ElasticLogSource()
    if name == "kubernetes":
        from src.log_pipeline.sources.k8s.source import KubernetesLogSource
        return KubernetesLogSource()
    logger.warning("Unknown log source in LOG_SOURCE; ignoring", source=name)
    return None


def configured_chain() -> list:
    """Parse `LOG_SOURCE` into an ordered list of sources.

    Read at call time so tests and reloads pick up changes. An empty or
    entirely unrecognised chain falls back to Elasticsearch rather than
    leaving the pipeline with no source at all.
    """
    raw = os.environ.get("LOG_SOURCE", DEFAULT_CHAIN).strip() or DEFAULT_CHAIN
    names = [part.strip().lower() for part in raw.split(",") if part.strip()]

    sources = []
    for name in names:
        source = _build_source(name)
        if source is not None:
            sources.append(source)

    if not sources:
        logger.warning("LOG_SOURCE yielded no usable sources; using elastic",
                       configured=raw)
        sources = [ElasticLogSource()]
    return sources


def fetch_with_fallback(identifier: str, window: TimeWindow,
                        ctx: FetchContext) -> FetchResult:
    """Try each source in order; the first with records wins."""
    sources = configured_chain()
    log = logger.bind(event_id=ctx.event_id)

    attempted, last_result = [], None

    for index, source in enumerate(sources):
        is_last = index == len(sources) - 1
        try:
            result = source.fetch(identifier, window, ctx)
        except Exception as e:
            # The final source's exception propagates: that preserves the
            # @retry_transient semantics on fetch_logs_for, which dispatch on
            # exception type, and lets a CircuitBreakerError reach the handler
            # that turns it into None. Mid-chain we swallow it and advance,
            # because a later source may succeed -- which is the whole point,
            # and is what an es_breaker wrapped around the entire chain used to
            # prevent (G10).
            if is_last:
                raise
            log.warning(
                "Log source raised; advancing down the chain",
                source=source.name,
                error=f"{type(e).__name__}: {e}",
            )
            attempted.append(f"{source.name} (raised {type(e).__name__})")
            continue

        last_result = result

        if result.ok and result.records:
            if attempted:
                result.gaps = list(result.gaps) + [EvidenceGap(
                    GapType.SOURCE_FALLBACK,
                    f"tried {', '.join(attempted)} first without success; "
                    f"the trace below came from {source.name}.",
                    {"attempted": attempted, "winner": source.name},
                )]
            log.info("Log source selected",
                     chain=[s.name for s in sources],
                     attempted=attempted,
                     winner=source.name,
                     records=len(result.records))
            return result

        reason = "failed" if not result.ok else "returned no records"
        attempted.append(f"{source.name} ({reason})")
        if not is_last:
            log.info("Log source produced nothing; falling back",
                     source=source.name, reason=reason)

    # Chain exhausted. Preserve the last source's outcome so a
    # looked-and-found-nothing does not masquerade as a could-not-look.
    log.warning("Log source chain exhausted",
                chain=[s.name for s in sources], attempted=attempted)

    if last_result is not None:
        # Only annotate when something was actually tried and abandoned, or a
        # source failed. A single source that looked and legitimately found
        # nothing is a clean confirmed-absent result: labelling it
        # SOURCE_FALLBACK and flagging the trace "INCOMPLETE" would blur the
        # very distinction design principle 3 exists to protect, and would
        # put a scary banner on every quiet packet.
        worth_reporting = len(sources) > 1 or not last_result.ok
        if worth_reporting:
            last_result.gaps = list(last_result.gaps) + [EvidenceGap(
                GapType.SOURCE_FALLBACK,
                f"no source returned logs; tried {', '.join(attempted)}.",
                {"attempted": attempted},
            )]
        return last_result

    return FetchResult(
        records=[],
        diagnostics=FetchDiagnostics(source="none"),
        ok=False,
    )
