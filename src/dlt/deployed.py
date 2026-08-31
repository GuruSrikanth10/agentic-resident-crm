"""Phase C1 of DLT_PLAN.md section 14 -- which build is actually running.

This answers Open Question 3 ("is the deployed build version available
anywhere -- a header, a pod label, an image tag?") and mitigates Risk R4 ("no
deploy-version dimension: a fixed bug keeps serving its old recommendation
indefinitely"). It is worth having on those grounds alone, whether or not the
rest of the replay precheck is ever built.

**The pod is the authority, not the manifest.** The deployment manifest in
Gitea holds the *desired* state; a replay executes against whatever the pods
are running now. A manifest updated but not yet synced by ArgoCD would report
a version that is not running, which is precisely the wrong answer for
deciding whether a replay will work. Reading the pod's own image reference
also makes the sync check redundant -- if the pod is on the new version, it
synced.

**A separate Kubernetes call, not a change to the log pipeline.** Threading an
image field through `PodTarget` -> `DiscoveryResult` -> `FetchResult` ->
`reduce_logs` would touch code the rejection lane depends on, for a value only
the DLT lane wants (Risk R8). This module makes its own bounded call and
caches it, because the deployed version changes on a deploy cadence, not a
per-message one: at ~2,000 messages/day the default 60s TTL collapses the
whole day's traffic into roughly one call a minute.

**No version ordering here.** A rolling deploy has pods on two versions at
once, and this module reports the full set without deciding which is lowest.
Ordering is `src/dlt/versions.py` (phase C3); doing it here would mean either
importing C3 before it exists or comparing version strings lexically, which is
Trap T7 and wrong roughly ten percent of the time.
"""
import os
import threading
from dataclasses import dataclass, field
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_TTL_SECONDS = 60

#: How many (pod, container, image) rows to keep on the record. Enough for an
#: operator to see a rolling deploy in progress, bounded so a 200-pod
#: namespace cannot land verbatim in a casebook.
MAX_RECORDED_IMAGES = 12

_lock = threading.Lock()
_cache = None
_cache_ttl = None


@dataclass(frozen=True)
class DeployedVersions:
    """What was running, as observed. Never a judgement, only a reading."""

    #: Distinct version strings across every matched container, sorted for a
    #: stable record. Sorted *lexically* -- this is presentation order, not a
    #: version ordering, and no caller may treat it as one.
    versions: tuple = ()
    #: (pod, container, image) triples, capped.
    images: tuple = ()
    pods: int = 0
    ok: bool = False
    #: Always populated when `ok` is False, so an absent version is auditable
    #: rather than mysterious.
    reason: str = ""
    details: dict = field(default_factory=dict)

    @property
    def single(self) -> Optional[str]:
        """The one version running, or None when zero or several are.

        None on a rolling deploy is deliberate. A caller that needs one value
        must resolve the set through `versions.py`, which knows that
        `1.0.10` is ahead of `1.0.9` and that the *lowest* is the safe reading
        (a replay may land on any pod).
        """
        return self.versions[0] if len(self.versions) == 1 else None

    @property
    def mixed(self) -> bool:
        """True mid-rollout, when pods disagree about what is running."""
        return len(self.versions) > 1

    def as_dict(self) -> dict:
        return {
            "versions": list(self.versions),
            "images": [list(row) for row in self.images],
            "pods": self.pods,
            "ok": self.ok,
            "reason": self.reason,
            "mixed": self.mixed,
            "version": self.single,
            **({"details": self.details} if self.details else {}),
        }


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def ttl_seconds() -> float:
    try:
        return max(0.0, float(os.environ.get("DLT_DEPLOYED_VERSION_TTL_SECONDS",
                                             str(DEFAULT_TTL_SECONDS))))
    except (ValueError, TypeError):
        return float(DEFAULT_TTL_SECONDS)


# ---------------------------------------------------------------------------
# Image reference parsing
# ---------------------------------------------------------------------------

def version_of(reference: Optional[str]) -> str:
    """Pull the version out of an image reference.

    Two shapes are in use here and both must work:

        mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric/1.0.0-release.4
        mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric:1.0.0

    A trailing `@sha256:...` digest is stripped first: it identifies the image
    *content*, says nothing about which commit built it, and must never be
    mistaken for a version. The colon is only read as a tag separator when it
    appears in the last path segment, so a registry port (`host:5000/ns/app`)
    is not misread as one.

    Returns "" rather than None for anything unusable, so the caller's set
    arithmetic stays total.
    """
    if not reference:
        return ""
    head = str(reference).split("@", 1)[0].strip()
    if not head:
        return ""
    last = head.rsplit("/", 1)[-1]
    if ":" in last:
        return last.rsplit(":", 1)[-1]
    return last


def _rows_from_pods(pods) -> tuple:
    """(pod, container, image) for every running container we can see."""
    rows = []
    for pod in pods or []:
        name = getattr(getattr(pod, "metadata", None), "name", None) or "?"
        statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
        for status in statuses:
            image = getattr(status, "image", None)
            if image:
                rows.append((name, getattr(status, "name", "?"), str(image)))
    return tuple(rows)


# ---------------------------------------------------------------------------
# The read
# ---------------------------------------------------------------------------

def _read(app: Optional[str], namespace: Optional[str]) -> DeployedVersions:
    """One uncached Kubernetes read. Never raises."""
    try:
        from src.log_pipeline.sources.k8s import discovery
    except Exception as e:
        return DeployedVersions(
            ok=False, reason=f"kubernetes support unavailable: {type(e).__name__}: {e}")

    try:
        from src.utils.resilience import k8s_breaker
        pods = k8s_breaker.call(discovery.list_pods_for_service, app, namespace)
    except Exception as e:
        # Includes CircuitBreakerError. A tripped breaker means "we could not
        # look", which is an unknown version, not an absent one.
        return DeployedVersions(
            ok=False, reason=f"pod listing failed: {type(e).__name__}: {e}")

    if pods is None:
        return DeployedVersions(
            ok=False,
            reason="no namespace resolved, or the Kubernetes client is unavailable")

    rows = _rows_from_pods(pods)
    if not rows:
        return DeployedVersions(
            pods=len(pods), ok=False,
            reason=f"{len(pods)} pod(s) matched but none reported a container image")

    versions = tuple(sorted({version_of(image) for _, _, image in rows} - {""}))
    if not versions:
        return DeployedVersions(
            images=rows[:MAX_RECORDED_IMAGES], pods=len(pods), ok=False,
            reason="no version could be read from any image reference")

    return DeployedVersions(
        versions=versions,
        images=rows[:MAX_RECORDED_IMAGES],
        pods=len(pods),
        ok=True,
        reason="",
        details={"app": app or "", "namespace": namespace or ""},
    )


def running_version(app: Optional[str] = None,
                    namespace: Optional[str] = None) -> DeployedVersions:
    """What the pods for `app` are running, cached on a short TTL.

    Total: every failure mode -- no cluster, no namespace, a tripped breaker,
    pods with no image -- returns a `DeployedVersions` with `ok=False` and a
    reason. It never raises, because a missing version must degrade a case's
    verdict to UNKNOWN, not cost the case.
    """
    global _cache, _cache_ttl

    ttl = ttl_seconds()
    key = (app or "", namespace or "")

    if ttl > 0:
        with _lock:
            if _cache is None or _cache_ttl != ttl:
                # Rebuilt when the TTL changes so a test (or an operator
                # reconfiguring at runtime) is not served entries that were
                # admitted under the old expiry.
                from cachetools import TTLCache
                _cache = TTLCache(maxsize=32, ttl=ttl)
                _cache_ttl = ttl
            hit = _cache.get(key)
        if hit is not None:
            return hit

    result = _read(app, namespace)

    # Only successful reads are cached. Caching a failure would hold a whole
    # TTL of cases at UNKNOWN after a transient blip, when the next call would
    # have succeeded.
    if ttl > 0 and result.ok:
        with _lock:
            if _cache is not None:
                _cache[key] = result

    if not result.ok:
        logger.warning("Could not read the deployed version", app=app,
                       namespace=namespace, reason=result.reason)

    return result


def reset_cache() -> None:
    """Drop the cached reading. For tests, and for an operator forcing a
    re-read after a deploy rather than waiting out the TTL."""
    global _cache, _cache_ttl
    with _lock:
        _cache = None
        _cache_ttl = None
