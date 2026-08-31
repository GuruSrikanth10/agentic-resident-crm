"""
Pod and container discovery (KUBERNETES_LOGS_PLAN.md 5.2).

No Kubernetes API aggregates logs across a Deployment, so the source lists
pods within a namespace and iterates. This module decides *which* pods and
*which* containers within them.

The ServiceAccount this runs as has namespace-scoped RBAC only: `get` on a
named `namespaces` object and `list`/`get` on `pods` *within* a namespace it
already knows the name of. There is no cluster-wide `list namespaces` or
`list pods --all-namespaces` access, so a namespace is never enumerated or
guessed -- it must already be resolved (via `K8S_DEFAULT_NAMESPACE` /
`K8S_SERVICE_MAP`) and its accessibility is verified with a single targeted
`read_namespace` call before anything is listed within it. This mirrors a
validated reference pattern run against the real cluster under this exact
RBAC grant.

That same constraint shapes pod matching. Label-based selection assumes every
target pod carries a reliable, known label -- an assumption the validated
reference does not make: it lists every pod in the namespace and matches by
substring against the pod name instead. `PodMatchSpec` supports both, with
name-substring as the default because it is the mode actually proven to work
here; label selection remains available as an explicit per-service opt-in via
`K8S_SERVICE_MAP` for services known to be labelled reliably.

**Multi-service discovery (2026-08-21).** A refId travels through several
services, so the logs that explain a failure are rarely all in one of them.
`K8S_APP_NAMES` (falling back to `ES_APP_NAMES`, so one list can drive both
sources) lists every service to search; each resolves its own namespace and
match spec through `resolve_service`, and the results are merged.

Three things make the merge non-trivial, and all three are handled here:

* **Overlapping match specs double-count.** `name_contains` is a substring
  test, so `enu-biometric` and `enu-biometric-abis-mw-consumer` both match the
  pod `enu-biometric-abis-mw-consumer-dcdr-...`. Reading it twice would
  duplicate every line it contributed, inflating the evidence and the token
  bill. Targets are deduped on (namespace, pod, container).

* **One bad service must not sink the fetch.** A service whose namespace is
  unreadable, or whose pod list is denied, yields a `SERVICE_UNAVAILABLE` gap
  while the others still return logs. Only a total failure -- nothing
  searchable anywhere -- is reported as `ok=False`.

* **A cap must not starve a service.** Pods are merged round-robin across
  services rather than concatenated, so truncation trims each service's tail
  evenly instead of dropping whichever service happened to be configured last.
"""
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import fixtures
from src.log_pipeline.sources.k8s import retry
from src.log_pipeline.types import EvidenceGap, GapType
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Pods in this phase have no logs yet, so they are skipped silently.
SKIPPED_PHASES = frozenset({"Pending"})

DEFAULT_SIDECAR_DENYLIST = "istio-proxy,linkerd-proxy,vault-agent"

#: Used when neither K8S_APP_NAMES, ES_APP_NAMES nor K8S_DEFAULT_APP is set.
DEFAULT_APP = "enu-biometric"

MATCH_MODE_LABEL = "label"
MATCH_MODE_NAME_CONTAINS = "name_contains"


@dataclass(frozen=True)
class PodMatchSpec:
    """How to find the pods for a service, within an already-known namespace.

    `label`         -- server-side `label_selector=` on `list_namespaced_pod`.
    `name_contains`  -- lists every pod in the namespace (no selector) and
                        keeps those whose name contains `value`, client-side.
                        This is the validated pattern and the default mode.
    """

    mode: str
    value: str

    def describe(self) -> str:
        if self.mode == MATCH_MODE_LABEL:
            return f"label selector '{self.value}'"
        return f"pod name contains '{self.value}'"


@dataclass(frozen=True)
class PodTarget:
    """One (pod, container) pair to read logs from."""

    namespace: str
    pod_name: str
    container: str
    restart_count: int = 0
    phase: str = "Unknown"
    start_time: Optional[datetime] = None
    #: Which configured service this pod was matched for. Defaults to "" so
    #: every existing construction stays valid; carried so a multi-service
    #: fetch can say which hop a pod belongs to.
    app: str = ""

    @property
    def pod_key(self) -> tuple:
        """Identifies the pod, ignoring the container. Two services matching
        the same pod must not read it twice."""
        return (self.namespace, self.pod_name)

    @property
    def target_key(self) -> tuple:
        return (self.namespace, self.pod_name, self.container)

    @property
    def restarted(self) -> bool:
        """True when a `previous=True` read is worth attempting (plan 5.4)."""
        return self.restart_count > 0


@dataclass
class DiscoveryResult:
    targets: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    pods_seen: int = 0
    pods_skipped_pending: int = 0
    truncated: bool = False
    ok: bool = True
    reason: Optional[str] = None
    #: Services that were searched successfully, and those that could not be.
    #: A caller comparing the two knows which hops of the packet's journey are
    #: actually represented in the logs it is about to reason over.
    services_searched: list = field(default_factory=list)
    services_failed: list = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.targets


# ======================================================================
# Configuration -- all read at call time so tests can monkeypatch
# ======================================================================

def _service_map() -> dict:
    raw = os.environ.get("K8S_SERVICE_MAP", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception as e:
        logger.warning("K8S_SERVICE_MAP is not valid JSON; ignoring", error=str(e))
        return {}


def resolve_service(app: Optional[str] = None,
                    namespace: Optional[str] = None) -> tuple:
    """Resolve a logical app name to (namespace, PodMatchSpec).

    `K8S_SERVICE_MAP` keeps this out of Kafka payloads and lets an operator
    correct a mapping without a code change. A per-service entry may set
    `label_selector` to opt into server-side label matching; otherwise the
    default is `name_contains` on the app name itself, matching the
    validated reference pattern.
    """
    app = app or os.environ.get("K8S_DEFAULT_APP") or "enu-biometric"
    mapping = _service_map().get(app, {})

    resolved_namespace = (
        namespace
        or mapping.get("namespace")
        or os.environ.get("K8S_DEFAULT_NAMESPACE")
    )

    if mapping.get("label_selector"):
        match_spec = PodMatchSpec(MATCH_MODE_LABEL, mapping["label_selector"])
    else:
        match_spec = PodMatchSpec(MATCH_MODE_NAME_CONTAINS, mapping.get("name_contains") or app)

    return resolved_namespace, match_spec


def _sidecar_denylist() -> set:
    raw = os.environ.get("K8S_SIDECAR_DENYLIST", DEFAULT_SIDECAR_DENYLIST)
    return {name.strip() for name in raw.split(",") if name.strip()}


def _max_pods() -> int:
    try:
        return max(1, int(os.environ.get("K8S_MAX_PODS", "20")))
    except ValueError:
        return 20


def _max_total_pods() -> int:
    """Ceiling on distinct pods across ALL services. 0 disables it.

    Disabled by default so adding this knob changes nothing: `K8S_MAX_PODS`
    still bounds each service individually, and the real fan-out cost is
    already bounded by `K8S_FETCH_CONCURRENCY` and
    `K8S_TOTAL_FETCH_TIMEOUT_SECONDS`. It exists for the operator who would
    rather cap the total than reason about per-service caps times the number
    of services.
    """
    try:
        return max(0, int(os.environ.get("K8S_MAX_TOTAL_PODS", "0")))
    except ValueError:
        return 0


def app_names() -> list:
    """Every service to search, in configured order.

    `K8S_APP_NAMES` first, then `ES_APP_NAMES`. The fallback is deliberate:
    the two sources want the same list of services, and asking an operator to
    maintain that list twice is asking for the two to drift -- at which point
    Kubernetes and Elasticsearch would quietly search different parts of the
    packet's journey. `K8S_APP_NAMES` exists only for when they genuinely must
    differ, because a pod-name substring and an Elasticsearch
    `application_name` value are not always the same string.

    Falls back to a single `K8S_DEFAULT_APP`, which is exactly the pre-2026-08-21
    behaviour for anyone who sets neither list.
    """
    for var in ("K8S_APP_NAMES", "ES_APP_NAMES"):
        raw = os.environ.get(var, "").strip()
        if not raw:
            continue
        names, seen = [], set()
        for name in (n.strip() for n in raw.split(",")):
            # Deduped here as well as on targets: a repeated name would
            # otherwise list the same namespace twice for no reason.
            if name and name not in seen:
                seen.add(name)
                names.append(name)
        if names:
            return names

    return [os.environ.get("K8S_DEFAULT_APP") or DEFAULT_APP]


# ======================================================================
# Selection
# ======================================================================

def select_containers(pod) -> list:
    """Container names worth reading from `pod`.

    Sidecars are dropped, but if the denylist would remove *every* container
    the original list is kept: returning nothing would silently produce an
    empty trace, which is worse than reading a proxy's logs.
    """
    spec = getattr(pod, "spec", None)
    containers = [c.name for c in (getattr(spec, "containers", None) or [])]
    if not containers:
        return []

    denied = _sidecar_denylist()
    kept = [name for name in containers if name not in denied]
    if not kept:
        logger.debug(
            "Sidecar denylist removed every container; keeping all",
            pod=pod.metadata.name,
            containers=containers,
        )
        return containers
    return kept


def _restart_count(pod, container_name: str) -> int:
    statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
    for status in statuses:
        if status.name == container_name:
            return int(status.restart_count or 0)
    return 0


def _start_time(pod) -> Optional[datetime]:
    return getattr(getattr(pod, "status", None), "start_time", None)


def _sort_key(pod):
    """Most recently started first; pods without a start time sort last."""
    started = _start_time(pod)
    if started is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return started


def _verify_namespace(namespace: str, request_timeout: float) -> tuple:
    """Confirm the namespace is readable before listing anything inside it.

    Mirrors the validated reference pattern's `v1.read_namespace(name=...)`.
    The ServiceAccount here has no `list namespaces` access -- only `get` on
    a namespace whose name is already known -- so this is always a targeted
    read of one name, never an enumeration. Returns (ok, reason).
    """
    if fixtures.is_active():
        if fixtures.namespace_exists(namespace):
            return True, None
        return False, f"namespace '{namespace}' not found in fixtures"

    api = k8s_client_module.get_client()
    if api is None:
        return False, k8s_client_module.unavailable_reason()

    try:
        # Retries 429/5xx with jitter, never retries a 403 (F8).
        retry.call_with_retry(
            api.read_namespace, name=namespace, _request_timeout=request_timeout
        )
        return True, None
    except Exception as e:
        status = getattr(e, "status", None)
        if status == 403:
            logger.error("Kubernetes RBAC denied namespace read",
                         namespace=namespace, verb="get")
        elif status == 404:
            logger.error("Kubernetes namespace not found", namespace=namespace)
        else:
            logger.error("Kubernetes namespace read failed",
                         namespace=namespace, error=f"{type(e).__name__}: {e}")
        return False, f"{type(e).__name__}: {e}"


def _list_pods(namespace: str, match_spec: PodMatchSpec, request_timeout: float):
    """List pods from fixtures when configured, otherwise from the API.

    `name_contains` mode lists every pod in the (already-verified) namespace
    with no `label_selector` -- this is still a namespace-scoped `list`, not
    a cluster-wide one -- and filters by name client-side, mirroring the
    validated reference pattern exactly.
    """
    if fixtures.is_active():
        return fixtures.list_pods(namespace, match_spec)

    api = k8s_client_module.get_client()
    if api is None:
        return None

    list_kwargs = dict(
        namespace=namespace,
        timeout_seconds=int(request_timeout),
        _request_timeout=request_timeout,
    )
    if match_spec.mode == MATCH_MODE_LABEL:
        list_kwargs["label_selector"] = match_spec.value

    response = retry.call_with_retry(api.list_namespaced_pod, **list_kwargs)
    pods = list(response.items or [])

    if match_spec.mode == MATCH_MODE_NAME_CONTAINS:
        pods = [p for p in pods if match_spec.value in (p.metadata.name or "")]

    return pods


def list_pods_for_service(app: Optional[str] = None,
                          namespace: Optional[str] = None,
                          request_timeout: Optional[float] = None):
    """Raw pods for one service, or None when they cannot be listed.

    A read-only view for callers that want pod *metadata* rather than log
    targets -- the DLT lane reads the running image tag off these to record
    which build was live when a packet failed (DLT_PLAN.md 14, phase C1).

    Deliberately does not verify the namespace the way `discover_for_service`
    does. That check exists so a log fetch fails loudly on a misconfigured
    namespace rather than returning an empty trace the agent might read as
    "no errors occurred"; here an empty result is simply an unknown version,
    which every caller already has to handle, and the extra RBAC round-trip
    would be paid on the packet path for nothing.
    """
    resolved_namespace, match_spec = resolve_service(app=app, namespace=namespace)
    if not resolved_namespace:
        return None

    timeout = (request_timeout if request_timeout is not None
               else float(os.environ.get("K8S_REQUEST_TIMEOUT_SECONDS", "30")))
    return _list_pods(resolved_namespace, match_spec, timeout)


def discover_for_service(app: Optional[str] = None,
                         namespace: Optional[str] = None,
                         verified: Optional[dict] = None) -> DiscoveryResult:
    """Find every (pod, container) to read for ONE service.

    `verified` is a namespace -> (ok, reason) cache shared across the services
    in a single `discover_targets` call. Several services usually share one
    namespace, and re-reading it once per service would multiply the RBAC
    round-trips for no new information.
    """
    resolved_namespace, match_spec = resolve_service(app=app, namespace=namespace)
    service_label = app or os.environ.get("K8S_DEFAULT_APP") or DEFAULT_APP

    if not resolved_namespace:
        return DiscoveryResult(
            ok=False,
            reason="no namespace configured (set K8S_DEFAULT_NAMESPACE or K8S_SERVICE_MAP)",
        )

    if not fixtures.is_active() and not k8s_client_module.is_available():
        return DiscoveryResult(
            ok=False,
            reason=k8s_client_module.unavailable_reason() or "Kubernetes client unavailable",
        )

    request_timeout = float(os.environ.get("K8S_REQUEST_TIMEOUT_SECONDS", "30"))

    # Verify the namespace is readable before listing anything in it -- this
    # ServiceAccount has no cluster-wide access, so a 403/404 here is
    # reported distinctly rather than surfacing as a confusing failure from
    # the pod list call that follows.
    if verified is not None and resolved_namespace in verified:
        ns_ok, ns_reason = verified[resolved_namespace]
    else:
        ns_ok, ns_reason = _verify_namespace(resolved_namespace, request_timeout)
        if verified is not None:
            verified[resolved_namespace] = (ns_ok, ns_reason)
    if not ns_ok:
        return DiscoveryResult(
            ok=False,
            reason=ns_reason or f"namespace '{resolved_namespace}' not accessible",
        )

    try:
        pods = _list_pods(resolved_namespace, match_spec, request_timeout)
    except Exception as e:
        # A 403 here is an RBAC misconfiguration, not a transient fault. It is
        # surfaced distinctly rather than folded into an empty result.
        status = getattr(e, "status", None)
        if status == 403:
            logger.error(
                "Kubernetes RBAC denied pod list",
                namespace=resolved_namespace,
                verb="list",
            )
        else:
            logger.error(
                "Kubernetes pod list failed",
                namespace=resolved_namespace,
                error=f"{type(e).__name__}: {e}",
            )
        return DiscoveryResult(ok=False, reason=f"{type(e).__name__}: {e}")

    if pods is None:
        return DiscoveryResult(
            ok=False,
            reason=k8s_client_module.unavailable_reason() or "Kubernetes client unavailable",
        )

    pods_seen = len(pods)
    gaps = []

    # Skip only Pending. Failed and Succeeded pods are INCLUDED -- a
    # terminated pod frequently holds the exact crash evidence the
    # investigation needs, and filtering to Running is the most common
    # mistake in code like this.
    eligible, skipped_pending = [], 0
    for pod in pods:
        phase = getattr(getattr(pod, "status", None), "phase", None) or "Unknown"
        if phase in SKIPPED_PHASES:
            skipped_pending += 1
            continue
        eligible.append(pod)

    eligible.sort(key=_sort_key, reverse=True)

    cap = _max_pods()
    truncated = len(eligible) > cap
    if truncated:
        logger.warning(
            "Pod fan-out capped",
            matched=len(eligible),
            cap=cap,
            namespace=resolved_namespace,
        )
        gaps.append(EvidenceGap(
            GapType.TRUNCATED,
            f"{len(eligible)} pods matched but only the {cap} most recently "
            f"started were read (K8S_MAX_PODS).",
            {"matched": len(eligible), "cap": cap, "namespace": resolved_namespace},
        ))
        eligible = eligible[:cap]

    targets = []
    for pod in eligible:
        phase = getattr(getattr(pod, "status", None), "phase", None) or "Unknown"
        for container in select_containers(pod):
            targets.append(PodTarget(
                namespace=resolved_namespace,
                pod_name=pod.metadata.name,
                container=container,
                restart_count=_restart_count(pod, container),
                phase=phase,
                start_time=_start_time(pod),
                app=service_label,
            ))

    logger.info(
        "Kubernetes pods discovered",
        app=service_label,
        namespace=resolved_namespace,
        match_mode=match_spec.mode,
        match_value=match_spec.value,
        pod_count=len(eligible),
        skipped_pending=skipped_pending,
        truncated=truncated,
        target_count=len(targets),
    )

    if not targets:
        logger.warning(
            "No pods matched; check the namespace and the match spec",
            app=service_label,
            namespace=resolved_namespace,
            match_mode=match_spec.mode,
            match_value=match_spec.value,
        )

    return DiscoveryResult(
        targets=targets,
        gaps=gaps,
        pods_seen=pods_seen,
        pods_skipped_pending=skipped_pending,
        truncated=truncated,
        services_searched=[service_label],
    )


# ======================================================================
# Multi-service fan-out
# ======================================================================

def _merge_round_robin(per_service: list, total_cap: int) -> tuple:
    """Interleave each service's pods, deduped, honouring a total pod cap.

    Round-robin rather than concatenation so that a cap trims every service's
    tail evenly. Concatenating would silently drop whichever service happened
    to be configured last -- and "this service contributed nothing" is
    indistinguishable, in the final logs, from "this service logged nothing",
    which is exactly the confusion the gap types exist to prevent.

    Containers of one pod stay together: the unit being capped and deduped is
    the pod, not the (pod, container) pair.
    """
    grouped = []
    for result in per_service:
        by_pod = {}
        for target in result.targets:
            by_pod.setdefault(target.pod_key, []).append(target)
        grouped.append(list(by_pod.values()))

    merged, seen_pods, duplicates = [], set(), 0
    capped = False
    index = 0
    while any(index < len(groups) for groups in grouped):
        for groups in grouped:
            if index >= len(groups):
                continue
            pod_targets = groups[index]
            key = pod_targets[0].pod_key
            if key in seen_pods:
                # Two services' match specs cover the same pod. Substring
                # matching makes this the normal case, not an edge one.
                duplicates += 1
                continue
            if total_cap and len(seen_pods) >= total_cap:
                capped = True
                break
            seen_pods.add(key)
            merged.extend(pod_targets)
        if capped:
            break
        index += 1

    return merged, duplicates, capped, len(seen_pods)


def discover_targets(namespace: Optional[str] = None,
                     app: Optional[str] = None) -> DiscoveryResult:
    """Find every (pod, container) to read, across every configured service.

    Passing `app` explicitly searches only that service -- the operator CLI
    and the single-service callers rely on this. With `app` unset, every name
    in `app_names()` is searched and the results merged; when that list holds
    one entry (the default), this is byte-for-byte the old behaviour.

    A service that cannot be searched degrades the result instead of failing
    it: the packet's other hops are still worth reading, and the missing hop
    is named in a `SERVICE_UNAVAILABLE` gap rather than left to be inferred
    from an absence. Only when nothing at all is searchable does this report
    `ok=False`.
    """
    services = [app] if app is not None else app_names()

    verified: dict = {}
    per_service, gaps, searched, failed = [], [], [], []
    first_failure = None

    for service in services:
        result = discover_for_service(app=service, namespace=namespace,
                                      verified=verified)
        if not result.ok:
            failed.append(service)
            if first_failure is None:
                first_failure = result.reason
            gaps.append(EvidenceGap(
                GapType.SERVICE_UNAVAILABLE,
                f"service '{service}' could not be searched: "
                f"{result.reason}. Any part of this packet's journey through "
                f"it is missing from these logs.",
                {"app": service, "reason": result.reason},
            ))
            logger.warning("Service discovery failed; continuing with the rest",
                           app=service, reason=result.reason)
            continue

        per_service.append(result)
        searched.extend(result.services_searched or [service])
        gaps.extend(result.gaps)

    if not per_service:
        # Nothing was searchable anywhere. Reports the first concrete reason
        # captured above rather than a generic one -- it is almost always the
        # same misconfiguration for every service (no namespace, no client).
        # Deliberately NOT re-running discovery to recover a reason: that
        # would re-issue the very API calls that just failed.
        return DiscoveryResult(
            ok=False,
            reason=first_failure or "no service could be searched",
            gaps=gaps,
            services_failed=failed,
        )

    targets, duplicates, capped, pod_count = _merge_round_robin(
        per_service, _max_total_pods())

    if duplicates:
        logger.info(
            "Overlapping match specs; deduped pods across services",
            duplicate_pods=duplicates, services=searched,
            hint="a name_contains value is a prefix of another service's pods",
        )

    truncated = any(r.truncated for r in per_service) or capped
    if capped:
        logger.warning("Total pod fan-out capped across services",
                       cap=_max_total_pods(), services=searched)
        gaps.append(EvidenceGap(
            GapType.TRUNCATED,
            f"pods from {len(searched)} services were capped at "
            f"{_max_total_pods()} in total (K8S_MAX_TOTAL_PODS).",
            {"cap": _max_total_pods(), "services": list(searched)},
        ))

    if len(services) > 1:
        logger.info(
            "Multi-service discovery complete",
            services_searched=searched, services_failed=failed,
            pods=pod_count, targets=len(targets),
        )

    return DiscoveryResult(
        targets=targets,
        gaps=gaps,
        pods_seen=sum(r.pods_seen for r in per_service),
        pods_skipped_pending=sum(r.pods_skipped_pending for r in per_service),
        truncated=truncated,
        services_searched=searched,
        services_failed=failed,
    )
