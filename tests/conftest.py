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
    "CHECKPOINT_MYSQL_URI",
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
    # The opencode harness, per lane. A lane switch left over in the parent
    # shell would silently route a node through a harness the test never
    # intended, or hide the fallback to the older single switch.
    "USE_OPENCODE_HARNESS",
    "USE_OPENCODE_HARNESS_REJECTION",
    "USE_OPENCODE_HARNESS_DLT",
    # Reason-code documentation. The switch selects a branch, and the two
    # paths select which store is read -- a stray value in either would make
    # the Investigator's prompt depend on the developer's machine.
    "REJECTION_REASON_CODE_DOCS_ENABLED",
    "REASON_CODE_DOCS_DIR",
    "REASON_CODE_DOCS_S3_PREFIX",
    # A cap rather than a tunable: below a document's length it switches
    # truncation on, so a stray value changes which branch runs.
    "REASON_CODE_DOC_MAX_CHARS",
    # Likewise a cap: below the logs' length it switches trimming on.
    "REJECTION_PROMPT_MAX_CHARS",
    "REJECTION_REVIEWER_EVIDENCE",
    "REJECTION_SYNTHESIS_DOC_GUIDANCE",
    # The duplicate-invocation claim. A stray value would either disable the
    # guard for the whole session or make every live claim look abandoned.
    "PACKET_CLAIM_ENABLED",
    "PACKET_CLAIM_TTL_SECONDS",
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
    "AGENTIC_RESIDENT_CRM_API_KEY": "test-key",
    "USE_MOCK_DB": "true",
    "CASEBOOK_STORAGE_BACKEND": "local",
    "CHECKPOINT_BACKEND": "sqlite",
}


@pytest.fixture(autouse=True)
def isolated_packet_claims(tmp_path_factory, monkeypatch):
    """Give every test its own duplicate-invocation claim store.

    The claim `/analyze-rejection` takes reaches storage through
    `get_scoped_storage`, not through the `get_casebook_storage` name the test
    fixtures patch per module -- so without this it writes into the real
    `local_casesheets/packet_claims/` and the claims survive the run. The
    symptom is nasty: a test that exercises the analyze path passes the first
    time and fails every time after, because its event id is now held by a
    claim left behind by the previous run.

    Function-scoped and autouse rather than added to each fixture that needs
    it, so a future test cannot forget.
    """
    from src.storage.local import LocalFilesystemCasebookStorage
    from src.utils import packet_claims

    root = tmp_path_factory.mktemp("packet_claims")
    store = LocalFilesystemCasebookStorage(base_dir=str(root))
    monkeypatch.setattr(packet_claims, "get_claim_storage", lambda: store)


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
