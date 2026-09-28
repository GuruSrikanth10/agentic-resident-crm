"""
Whose logs a packet's fetch reads, and how they are reduced
(MULTI_SERVICE_PLAN.md Phase 6).

A packet is fetched for one service: the pack it is analysed with. Its
`LogScope` says which applications to search (the pack's `logs.app_names` and
those of its `logs.also_search` services), how to find their pods, which
template catalog and Drain3 parse tree reduce the result, and which lines are
decision vocabulary.

Three kinds of caller, three scopes:

* **A packet analysed with its own service's pack** searches that service and
  its `also_search` services only, with that service's catalog.
* **A packet analysed with the `_default` pack** -- unresolved, and let
  through by `REJECTION_UNRESOLVED_SERVICE=default_pack` -- searches nothing.
  No service is known to hold its logs, and searching every service would
  hand the model other services' evidence as if it were this packet's.
* **Everything else** is unscoped and keeps the behaviour from before
  services had logs of their own: `ES_APP_NAMES` / `K8S_APP_NAMES`, the
  catalog at `CATALOG_PATH` and its parse tree, and the pre-registry pack's
  decision vocabulary. That covers the callers with no service (the DLT lane,
  the CLIs) and a packet `record` mode analyses with the pre-registry pack
  because the gate would have skipped it -- `record` changes nothing about
  analysis (D5).
"""
import os
import re
from dataclasses import dataclass
from typing import Optional

from src.log_pipeline.config import GENERIC_DECISION_VOCABULARY_REGEX
from src.utils.paths import DRAIN3_STATE_DIR, LOCAL_CHECKPOINTS_DIR


class DecisionVocabulary:
    """The generic decision-vocabulary regex OR a service's own.

    Kept as separate patterns rather than joined into one: the generic one
    may carry an inline `(?i)` flag, which Python accepts only at the very
    start of an expression. A service's pattern is matched case-insensitively,
    as the generic one is.
    """

    def __init__(self, service_pattern: Optional[str] = None):
        self.service_pattern = service_pattern
        self._patterns = (GENERIC_DECISION_VOCABULARY_REGEX,) + (
            (re.compile(service_pattern, re.IGNORECASE),) if service_pattern else ())

    def search(self, text: str) -> bool:
        return any(pattern.search(text) for pattern in self._patterns)


@dataclass(frozen=True)
class LogScope:
    #: The pack these logs are fetched for, or None when unscoped.
    service: Optional[str]
    #: Application names to search, for Elasticsearch's `application_name`
    #: filter and Kubernetes discovery alike. None means the environment's
    #: lists (`ES_APP_NAMES`, `K8S_APP_NAMES`).
    apps: Optional[tuple]
    #: ((app, {"name_contains" | "label_selector": value}), ...): the pod
    #: match each app's pack declares in `logs.k8s_match`.
    pod_matches: tuple
    #: The template catalog and the Drain3 parse tree its ids came from. None
    #: is the pair every packet used before services had their own:
    #: `CATALOG_PATH` and `DRAIN3_STATE_DIR/drain3_state.bin`.
    catalog_path: Optional[str]
    drain3_state_file: Optional[str]
    decision_vocabulary: DecisionVocabulary
    #: False when nothing is to be searched at all (the `_default` pack).
    searchable: bool = True


def catalog_path_for(service: str):
    """Where `build_catalog.py --service <service>` writes its catalog."""
    return LOCAL_CHECKPOINTS_DIR / f"template_catalog.{service}.json"


def drain3_state_file_for(service: str):
    """The Drain3 parse tree that `service`'s catalog template ids come from."""
    return DRAIN3_STATE_DIR / f"drain3_state.{service}.bin"


def _catalog_files(service: str) -> tuple:
    """(catalog path, Drain3 state file) for a service's packets.

    A catalog's template ids mean something only against the parse tree that
    produced them, so the two always go together. The pre-registry pack
    without a catalog of its own keeps the unscoped pair: that catalog was
    built from its packets (MULTI_SERVICE_PLAN.md 2.7), and its packets are
    then reduced exactly as before. Any other service without a catalog of
    its own gets an empty one, so nothing is filtered out of its fetch: losing
    an evidence line costs more than a longer trace.
    """
    from src.utils import service_registry

    own = catalog_path_for(service)
    if service == service_registry.PRE_REGISTRY_PACK and not os.path.exists(own):
        return None, None
    return str(own), str(drain3_state_file_for(service))


def unscoped() -> LogScope:
    """The scope of a caller with no service: the behaviour from before
    services had logs of their own."""
    from src.utils import service_registry

    options = service_registry.log_options(service_registry.PRE_REGISTRY_PACK)
    return LogScope(
        service=None,
        apps=None,
        pod_matches=(),
        catalog_path=None,
        drain3_state_file=None,
        decision_vocabulary=DecisionVocabulary(options.get("decision_vocabulary")),
    )


def for_service(service: Optional[str]) -> LogScope:
    """The scope of a fetch for `service`'s pack; unscoped for None.

    A name with no registered pack -- `_default` included -- searches
    nothing: there is no service whose logs are known to be this packet's.
    """
    from src.utils import service_registry

    if not service:
        return unscoped()

    registry = service_registry.load()
    if not registry.is_registered(service):
        return LogScope(service=service, apps=(), pod_matches=(),
                        catalog_path=None, drain3_state_file=None,
                        decision_vocabulary=DecisionVocabulary(),
                        searchable=False)

    own = service_registry.log_options(service, registry)
    apps, pod_matches = [], []
    for name in (service, *own["also_search"]):
        options = own if name == service else service_registry.log_options(name, registry)
        for app in options["app_names"]:
            if app in apps:
                continue
            apps.append(app)
            if options["k8s_match"]:
                pod_matches.append((app, dict(options["k8s_match"])))

    catalog_path, state_file = _catalog_files(service)
    return LogScope(
        service=service,
        apps=tuple(apps),
        pod_matches=tuple(pod_matches),
        catalog_path=catalog_path,
        drain3_state_file=state_file,
        decision_vocabulary=DecisionVocabulary(own["decision_vocabulary"]),
    )


def service_to_search(resolution: dict, pack: Optional[str]) -> Optional[str]:
    """The service whose logs a packet is fetched for, or None (unscoped).

    The pack it is analysed with, when that pack is its own service's or
    `_default`. A packet `record` mode analyses with the pre-registry pack
    because the gate would have skipped it is unscoped: its fetch, like the
    rest of its analysis, is what it was before the registry existed.
    """
    from src.utils import service_registry

    if not pack:
        return None
    if pack == service_registry.DEFAULT_PACK:
        return pack
    if pack == (resolution or {}).get("service"):
        return pack
    return None


def not_searched_message(event_id: str) -> str:
    """What the model is shown for a packet whose logs were not searched."""
    return (f"No logs were searched for ID: {event_id}. This packet's service "
            f"was not resolved, so no service is known to hold its logs, and "
            f"searching every service would mix other services' evidence into "
            f"this packet's. Reason from the payload and the documentation.")
