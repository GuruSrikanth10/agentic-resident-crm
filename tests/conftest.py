"""Test-suite isolation from the developer's `.env`.

`src/utils/env.py` calls `load_dotenv()` at import time, and every production
module imports it transitively. So without this file, `pytest` inherits
whatever happens to be in `.env` -- and the suite's verdict depends on the
machine it runs on.

That was not hypothetical. A checkout with `LOG_SOURCE=kubernetes`,
`K8S_DEFAULT_NAMESPACE=offline` and `K8S_FIXTURE_DIR=...` set locally failed
three tests that passed in CI, because the CI job sets `LOG_SOURCE=elastic`
and nothing else. A developer seeing red tests they cannot attribute learns to
stop reading the suite.

Two mechanisms, and both are needed:

* `load_dotenv` is stubbed out at conftest import, so no later import can
  re-read `.env`. This is the one that actually closes the hole -- see the
  comment on the stub below.
* A session-scoped autouse fixture clears anything the parent shell exported
  and installs the CI defaults.

The fixture only clears variables that select a *deployment shape* -- a
backend, a source chain, a feature flag. Tunables (timeouts, caps, thresholds)
are left alone: tests that care about those set them explicitly via
monkeypatch, and clearing them here would only mask a missing setenv.
"""
import os

import dotenv
import pytest

# ---------------------------------------------------------------------------
# Neutralise `.env` loading for the whole session.
# ---------------------------------------------------------------------------
# Popping the variables in a fixture is NOT sufficient on its own. Several
# modules call `load_dotenv()` at *import* time -- `src/utils/env.py` and every
# consumer entrypoint -- and a test module importing one of them mid-session
# re-reads `.env` and puts every value straight back. Since the fixture had
# already popped them, `load_dotenv`'s "don't override what is already set"
# behaviour does not save us: it sees them as unset and sets them.
#
# This runs at conftest import, which pytest does before it imports any test
# module, and therefore before any production module is imported. From here on
# `load_dotenv()` is a no-op everywhere.
#
# CI has no `.env`, so this makes local runs behave the way CI already does
# rather than changing what CI does.
dotenv.load_dotenv = lambda *args, **kwargs: False

#: Variables whose value must come from the test, never from a developer's
#: `.env`. Each one selects a backend, a source, or a feature -- i.e. changes
#: which code path runs, not how fast it runs.
ISOLATED_ENV_VARS = (
    # Log source chain and its two backends.
    "LOG_SOURCE",
    "ES_MOCK_FILE",
    "ES_HOST",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_INDEX_PATTERN",
    "ES_APP_NAMES",
    "ES_SEARCH_WINDOW_DAYS",
    # Kubernetes source: fixtures, namespace, and service resolution.
    "K8S_FIXTURE_DIR",
    "K8S_DEFAULT_NAMESPACE",
    "K8S_DEFAULT_APP",
    "K8S_APP_NAMES",
    "K8S_SERVICE_MAP",
    "K8S_SEARCH_FIELDS",
    "K8S_DEFAULT_SINCE_HOURS",
    "KUBECONFIG_PATH",
    "K8S_CONTEXT",
    # Storage and checkpoint backends.
    "CASEBOOK_STORAGE_BACKEND",
    "CASEBOOK_S3_BUCKET",
    "CASEBOOK_S3_PREFIX",
    "S3_LOGS_BUCKET",
    "CHECKPOINT_BACKEND",
    "CHECKPOINT_POSTGRES_URI",
    "LOCAL_CASESHEETS_DIR",
    "LOCAL_CHECKPOINTS_DIR",
    # Feature switches. Every one of these changes which branch executes.
    "RUNBOOK_MODE",
    "RUNBOOK_SERVE_ALLOWLIST",
    "ENABLE_LOG_FETCHING",
    "ENABLE_LOG_FILTER_AGENT",
    "ENABLE_AUTO_REPLAY",
    "DLT_ENABLED",
    "DLT_AUTO_REPLAY_ENABLED",
    "DLT_REUSE_ENABLED",
    "DLT_REGISTRY_PATH",
    "LOG_SNAPSHOT_REUSE",
    # LLM provider selection.
    "USE_HF",
    "MOCK_LLM_WITH_MISTRAL",
    # Consumer role: resolved at import time, so a stray value here would
    # point kafkaConsumer at the wrong topic for the whole session.
    "CONSUMER_ROLE",
)

#: What the suite runs against when a test does not say otherwise. Matches the
#: `env:` block of the CI job so local and CI runs agree by construction.
TEST_ENV_DEFAULTS = {
    "LOG_SOURCE": "elastic",
    "KAFKA_CONSUMER_BROKERS": "localhost:9092",
    "PACKET_CRM_API_KEY": "test-key",
    "USE_MOCK_DB": "true",
    "CASEBOOK_STORAGE_BACKEND": "local",
    "CHECKPOINT_BACKEND": "sqlite",
}


@pytest.fixture(scope="session", autouse=True)
def hermetic_env():
    """Clear deployment-shaped configuration and install the test defaults.

    Session-scoped rather than function-scoped on purpose: several production
    modules resolve configuration at *import* time (`kafkaConsumer`'s topics,
    `utils.paths`' directories, `fetcher.LOG_MAX_DOCUMENTS`), so the only
    useful moment to do this is before the first import, not before each test.
    """
    for name in ISOLATED_ENV_VARS:
        os.environ.pop(name, None)

    for name, value in TEST_ENV_DEFAULTS.items():
        os.environ.setdefault(name, value)

    yield
