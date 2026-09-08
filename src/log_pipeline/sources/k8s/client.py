"""
Kubernetes API client lifecycle (KUBERNETES_LOGS_PLAN.md 5.1).

Load order:
  1. in-cluster config  -- preferred; no secrets on disk
  2. kubeconfig at KUBECONFIG_PATH, optionally a named context
  3. neither            -- log once, mark unavailable, NEVER raise

A log source is not a hard dependency of the API process: an unconfigured
cluster must degrade to "source unavailable", not crash startup or a packet.

The client is a module-level singleton. Per-call construction would re-parse
the kubeconfig and re-establish TLS on every packet, the same waste the Drain3
miner (remediation 2.5) and the SQLAlchemy engine already avoid.
"""
import os
import threading
from typing import Optional

from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

_lock = threading.Lock()
_client = None
_resolved = False          # have we attempted resolution yet?
_unavailable_reason = None  # set when resolution failed


def _build_client():
    """Attempt to build a CoreV1Api. Returns (client, None) or (None, reason)."""
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.config.config_exception import ConfigException

    kubeconfig_path = os.environ.get("KUBECONFIG_PATH")
    context = os.environ.get("K8S_CONTEXT") or None

    loaded_via = None
    try:
        k8s_config.load_incluster_config()
        loaded_via = "in-cluster"
    except ConfigException:
        try:
            k8s_config.load_kube_config(config_file=kubeconfig_path, context=context)
            loaded_via = f"kubeconfig({kubeconfig_path or 'default'})"
        except Exception as e:
            return None, f"no usable Kubernetes config: {type(e).__name__}: {e}"
    except Exception as e:
        return None, f"in-cluster config failed unexpectedly: {type(e).__name__}: {e}"

    try:
        configuration = k8s_client.Configuration.get_default_copy()
    except Exception:
        configuration = k8s_client.Configuration()

    # TLS verification defaults ON. Disabling it is an explicit, logged
    # decision -- the same rule applied to Elasticsearch in remediation 1.9.
    verify_ssl = get_bool_env("K8S_VERIFY_SSL", True)
    configuration.verify_ssl = verify_ssl
    if not verify_ssl:
        logger.warning(
            "Kubernetes TLS verification is DISABLED via K8S_VERIFY_SSL=false",
            loaded_via=loaded_via,
        )
    ca_cert = os.environ.get("K8S_CA_CERT_PATH")
    if ca_cert:
        configuration.ssl_ca_cert = ca_cert

    api_client = k8s_client.ApiClient(configuration=configuration)
    logger.info("Kubernetes client ready", loaded_via=loaded_via, verify_ssl=verify_ssl)
    return k8s_client.CoreV1Api(api_client), None


def get_client():
    """Return a cached CoreV1Api, or None when the cluster is unconfigured.

    The unavailable verdict is cached too, so a misconfigured deployment logs
    once rather than once per packet. Call `reset_client()` after changing
    configuration.
    """
    global _client, _resolved, _unavailable_reason

    if _resolved:
        return _client

    with _lock:
        if _resolved:  # another thread resolved while we waited
            return _client

        try:
            client_obj, reason = _build_client()
        except ImportError as e:
            client_obj, reason = None, f"kubernetes package not installed: {e}"

        _client = client_obj
        _unavailable_reason = reason
        _resolved = True

        if client_obj is None:
            logger.warning("Kubernetes log source unavailable", reason=reason)

    return _client


def is_available() -> bool:
    return get_client() is not None


def unavailable_reason() -> Optional[str]:
    """Why the source is unavailable, or None when it is available."""
    get_client()
    return _unavailable_reason


def reset_client():
    """Drop the cached client. For tests and for config reloads."""
    global _client, _resolved, _unavailable_reason
    with _lock:
        _client = None
        _resolved = False
        _unavailable_reason = None
