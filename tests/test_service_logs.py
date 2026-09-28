"""
Logs and privacy per service (MULTI_SERVICE_PLAN.md Phase 6).

- Routing: a packet of service S searches S and its `also_search` services
  only, through Elasticsearch's app filter and Kubernetes discovery alike; a
  packet of the `_default` pack fetches nothing; a caller with no service --
  and a packet `record` mode analyses with another pack -- searches the
  environment's lists, as before.
- The template catalog and Drain3 parse tree per service: a service's own
  catalog is used when present, and with none no `must_not` clause is sent.
- The decision vocabulary: the generic words OR the pack's.
- Key-based redaction of JSON fields: nested, escaped, arrays, scalars.
- The redaction audit's counting.

Most tests build their own registry under tmp_path: enu-biometric, a fixture
service `svc-demo` that also searches `svc-shared`, an unrelated `svc-other`,
and `_default`.
"""
import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
import src.tools.tool_registry as tool_registry
from src.api.routes import fetch_logs
from src.log_pipeline import catalog as catalog_module
from src.log_pipeline import fetcher, pipeline, redaction, reducer
from src.log_pipeline import scope as log_scope
from src.log_pipeline.sources import chain as source_chain
from src.log_pipeline.sources.k8s import discovery
from src.log_pipeline.types import FetchContext, FetchDiagnostics, FetchResult
from src.models.schemas import MessagePayload
from src.storage.local import LocalFilesystemCasebookStorage
from src.tools import build_catalog, redaction_audit
from src.utils import paths
from src.utils import service_registry as sr

BIO = "enu-biometric"
DEMO = "svc-demo"
SHARED = "svc-shared"
OTHER = "svc-other"


def _write_pack(root, document):
    directory = root / document["service"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "service.json").write_text(json.dumps(document), encoding="utf-8")
    (directory / "policy.md").write_text("The policy of this service.", encoding="utf-8")


def _pack(name, prefix, stages, **extra):
    document = {"schema_version": 1, "service": name, "display_name": f"{name} service",
                "tool_prefix": prefix, "match": {"stages": stages},
                "rule_source": {"type": "none"}}
    document.update(extra)
    return document


@pytest.fixture
def estate(tmp_path, monkeypatch):
    root = tmp_path / "packs"
    _write_pack(root, {"schema_version": 1, "service": "_default",
                       "display_name": "Default", "match": {},
                       "rule_source": {"type": "none"}})
    _write_pack(root, _pack(BIO, "bio", ["Biometric"], rule_source={"type": "rules_db"},
                            logs={"app_names": [BIO], "k8s_match": {"name_contains": BIO},
                                  "decision_vocabulary": "MAN_DEDUP"}))
    _write_pack(root, _pack(DEMO, "demo", ["Demographic"],
                            logs={"app_names": ["demo-api", "demo-worker"],
                                  "k8s_match": {"name_contains": "demo"},
                                  "also_search": [SHARED],
                                  "decision_vocabulary": "demographic.*mismatch"}))
    _write_pack(root, _pack(SHARED, "shared", ["Shared"],
                            logs={"app_names": ["shared-gw"]}))
    # No logs section at all: its name is its app name.
    _write_pack(root, _pack(OTHER, "other", ["Other"]))
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", root)
    assert not sr.load().errors
    return root


@pytest.fixture
def catalogs(tmp_path, monkeypatch):
    """Per-service catalogs and parse trees under tmp_path, and no catalog
    cached from another test."""
    monkeypatch.setattr(log_scope, "LOCAL_CHECKPOINTS_DIR", tmp_path / "checkpoints")
    monkeypatch.setattr(log_scope, "DRAIN3_STATE_DIR", tmp_path / "checkpoints" / "drain3")
    monkeypatch.setattr(pipeline, "_cached_service_catalogs", {})
    return tmp_path / "checkpoints"


def _write_catalog(path, phrase="Heartbeat from the scheduler thread"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([{"template_id": "t_0001", "template": phrase,
                                 "classification": "boilerplate"}]), encoding="utf-8")


# ======================================================================
# Which apps a packet searches
# ======================================================================

def test_a_service_searches_itself_and_its_also_search_services_only(estate):
    scope = log_scope.for_service(DEMO)

    assert scope.searchable
    assert scope.apps == ("demo-api", "demo-worker", "shared-gw")
    # The pack's pod match goes with each of its own apps; svc-shared
    # declares none, so its app keeps the default (its name).
    assert scope.pod_matches == (("demo-api", {"name_contains": "demo"}),
                                 ("demo-worker", {"name_contains": "demo"}))


def test_a_service_with_no_app_names_searches_its_own_name(estate):
    assert log_scope.for_service(OTHER).apps == (OTHER,)


def test_the_default_pack_and_an_unregistered_name_search_nothing(estate):
    for name in (sr.DEFAULT_PACK, "svc-unknown"):
        scope = log_scope.for_service(name)
        assert not scope.searchable
        assert scope.apps == ()


def test_a_caller_with_no_service_is_unscoped(estate):
    scope = log_scope.unscoped()
    assert scope.service is None
    assert scope.apps is None
    same = log_scope.for_service(None)
    assert (same.service, same.apps, same.catalog_path, same.drain3_state_file) == \
        (None, None, None, None)
    assert same.decision_vocabulary.service_pattern == \
        scope.decision_vocabulary.service_pattern


@pytest.mark.parametrize("resolved, pack, expected", [
    # Analysed with its own service's pack: that service's logs.
    (DEMO, DEMO, DEMO),
    (BIO, BIO, BIO),
    # Unresolved and let through with the default pack: nothing.
    (sr.UNRESOLVED, sr.DEFAULT_PACK, sr.DEFAULT_PACK),
    # `record` mode analysing a packet the gate would skip with the
    # pre-registry pack: the environment's lists, as before.
    (OTHER, BIO, None),
    (sr.UNRESOLVED, BIO, None),
    (DEMO, None, None),
])
def test_the_service_to_search(estate, resolved, pack, expected):
    assert log_scope.service_to_search({"service": resolved}, pack) == expected


def test_the_default_pack_fetches_nothing(estate, monkeypatch):
    monkeypatch.setattr(source_chain, "fetch_with_fallback",
                        MagicMock(side_effect=AssertionError("must not fetch")))

    text = pipeline.reduce_logs("evt-default", service=sr.DEFAULT_PACK)

    assert text == log_scope.not_searched_message("evt-default")


def test_the_not_searched_message_is_persisted_as_the_fetch(estate, tmp_path, monkeypatch):
    """So the analysis stage reads it rather than fetching live."""
    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "store"))
    monkeypatch.setattr(tool_registry, "get_casebook_storage", lambda: store)
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "true")
    monkeypatch.setattr(source_chain, "fetch_with_fallback",
                        MagicMock(side_effect=AssertionError("must not fetch")))

    tool_registry.fetch_and_persist_logs("evt-default", {"eventId": "evt-default"},
                                         service=sr.DEFAULT_PACK)

    assert store.load_artifact("evt-default", "fetched_logs.txt") == \
        log_scope.not_searched_message("evt-default")


def _capture_context(monkeypatch):
    seen = []

    def fake(identifier, window, ctx):
        seen.append(ctx)
        return FetchResult(records=[], diagnostics=FetchDiagnostics(source="fake"))

    monkeypatch.setattr(source_chain, "fetch_with_fallback", fake)
    return seen


def test_the_pipeline_fetches_with_the_packets_scope(estate, catalogs, monkeypatch):
    seen = _capture_context(monkeypatch)

    pipeline.reduce_logs("evt-demo", service=DEMO)
    pipeline.reduce_logs("evt-none")

    assert seen[0].apps == ("demo-api", "demo-worker", "shared-gw")
    assert dict(seen[0].pod_matches)["demo-api"] == {"name_contains": "demo"}
    assert seen[1].apps is None and seen[1].pod_matches == ()


# ======================================================================
# Elasticsearch
# ======================================================================

class _FakeEs:
    def __init__(self):
        self.queries = []

    def search(self, **kwargs):
        self.queries.append(kwargs["query"])
        return {"hits": {"hits": []}}


@pytest.fixture
def fake_es(monkeypatch):
    client = _FakeEs()
    monkeypatch.setenv("ES_HOST", "https://es.example")
    monkeypatch.setattr(fetcher, "_get_es_client", lambda host, auth: client)
    return client


def _terms(query):
    for clause in query["bool"].get("filter", []):
        if "terms" in clause:
            return clause["terms"]["application_name.keyword"]
    return None


def test_the_elasticsearch_filter_is_the_packets_apps(fake_es, monkeypatch):
    monkeypatch.setenv("ES_APP_NAMES", "everything-a,everything-b")

    fetcher.fetch_logs("evt-1", apps=("demo-api", "shared-gw"))
    fetcher.fetch_logs("evt-2")

    assert _terms(fake_es.queries[0]) == ["demo-api", "shared-gw"]
    # A caller with no service: the configured list, as before.
    assert _terms(fake_es.queries[1]) == ["everything-a", "everything-b"]


def test_a_packet_of_a_service_searches_its_apps_end_to_end(
        estate, catalogs, fake_es, monkeypatch):
    monkeypatch.setenv("LOG_SOURCE", "elastic")
    monkeypatch.setenv("ES_APP_NAMES", "svc-other")

    pipeline.reduce_logs("evt-demo", service=DEMO)

    assert _terms(fake_es.queries[0]) == ["demo-api", "demo-worker", "shared-gw"]


# ======================================================================
# Kubernetes
# ======================================================================

def test_discovery_searches_exactly_the_given_apps(monkeypatch):
    calls = []

    def fake(app=None, namespace=None, verified=None, match=None):
        calls.append((app, match))
        return discovery.DiscoveryResult(ok=False, reason="fake")

    monkeypatch.setattr(discovery, "discover_for_service", fake)
    monkeypatch.setenv("K8S_APP_NAMES", "configured-a,configured-b")

    discovery.discover_targets(apps=["demo-api", "shared-gw", "demo-api"],
                               pod_matches=(("demo-api", {"name_contains": "demo"}),))
    assert calls == [("demo-api", {"name_contains": "demo"}), ("shared-gw", None)]

    calls.clear()
    discovery.discover_targets()
    assert [app for app, _ in calls] == ["configured-a", "configured-b"]


def test_the_kubernetes_source_passes_the_scope(estate, catalogs, monkeypatch):
    seen = {}

    def fake(namespace=None, app=None, apps=None, pod_matches=()):
        seen.update(apps=apps, pod_matches=pod_matches)
        return discovery.DiscoveryResult(ok=False, reason="fake")

    monkeypatch.setenv("LOG_SOURCE", "kubernetes")
    monkeypatch.setattr(discovery, "discover_targets", fake)
    monkeypatch.setattr("src.log_pipeline.sources.k8s.client.is_available", lambda: True)

    pipeline.reduce_logs("evt-demo", service=DEMO)

    assert seen["apps"] == ("demo-api", "demo-worker", "shared-gw")
    assert dict(seen["pod_matches"]) == {"demo-api": {"name_contains": "demo"},
                                         "demo-worker": {"name_contains": "demo"}}


def test_the_pod_match_comes_from_the_environment_then_the_pack_then_the_name(monkeypatch):
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "ns")
    pack_match = {"name_contains": "demo"}

    _, spec = discovery.resolve_service(app="demo-api", match=pack_match)
    assert (spec.mode, spec.value) == ("name_contains", "demo")

    _, spec = discovery.resolve_service(app="demo-api", match={"label_selector": "app=demo"})
    assert (spec.mode, spec.value) == ("label", "app=demo")

    _, spec = discovery.resolve_service(app="demo-api")
    assert (spec.mode, spec.value) == ("name_contains", "demo-api")

    # An operator's K8S_SERVICE_MAP entry still wins, and only the
    # environment names the namespace.
    monkeypatch.setenv("K8S_SERVICE_MAP", json.dumps(
        {"demo-api": {"namespace": "demo-ns", "label_selector": "tier=api"}}))
    namespace, spec = discovery.resolve_service(app="demo-api", match=pack_match)
    assert namespace == "demo-ns"
    assert (spec.mode, spec.value) == ("label", "tier=api")


# ======================================================================
# The template catalog and the parse tree
# ======================================================================

def _must_not(query):
    return query["bool"].get("must_not", [])


def test_a_services_own_catalog_is_used(estate, catalogs, fake_es, monkeypatch):
    monkeypatch.setenv("LOG_SOURCE", "elastic")
    _write_catalog(log_scope.catalog_path_for(DEMO))

    scope = log_scope.for_service(DEMO)
    pipeline.reduce_logs("evt-demo", service=DEMO)

    assert scope.catalog_path == str(log_scope.catalog_path_for(DEMO))
    assert scope.drain3_state_file == str(log_scope.drain3_state_file_for(DEMO))
    assert _must_not(fake_es.queries[0]) == [
        {"match_phrase": {"message": "Heartbeat from the scheduler thread"}}]


def test_a_service_with_no_catalog_sends_no_must_not(estate, catalogs, fake_es,
                                                     monkeypatch, tmp_path):
    """Not even the unscoped catalog's: that one was built from another
    service's logs, and losing an evidence line costs more than a longer
    trace."""
    monkeypatch.setenv("LOG_SOURCE", "elastic")
    legacy = tmp_path / "legacy_catalog.json"
    _write_catalog(legacy, phrase="A biometric boilerplate phrase to drop")
    monkeypatch.setattr(catalog_module, "CATALOG_PATH", str(legacy))
    monkeypatch.setattr(pipeline, "_cached_catalog", None)

    pipeline.reduce_logs("evt-demo", service=DEMO)
    pipeline.reduce_logs("evt-none")

    assert _must_not(fake_es.queries[0]) == []
    assert _must_not(fake_es.queries[1]) != []


def test_the_pre_registry_pack_keeps_the_unscoped_catalog_until_it_has_its_own(
        estate, catalogs):
    before = log_scope.for_service(BIO)
    assert (before.catalog_path, before.drain3_state_file) == (None, None)

    _write_catalog(log_scope.catalog_path_for(BIO))
    after = log_scope.for_service(BIO)
    assert after.catalog_path == str(log_scope.catalog_path_for(BIO))
    assert after.drain3_state_file == str(log_scope.drain3_state_file_for(BIO))


def test_each_parse_tree_is_its_own(tmp_path, monkeypatch):
    monkeypatch.setattr(reducer, "DRAIN3_STATE_DIR", str(tmp_path / "default"))
    monkeypatch.setattr(reducer, "_template_miner_instance", None)
    monkeypatch.setattr(reducer, "_service_template_miners", {})
    logs = [{"timestamp": f"t{i}", "level": "INFO", "message": f"step {i} done",
             "app_name": "a"} for i in range(3)]

    own = tmp_path / "trees" / "drain3_state.svc-demo.bin"
    reducer.cluster_logs(logs, state_file=str(own))

    assert own.exists()
    assert not (tmp_path / "default" / "drain3_state.bin").exists()
    assert reducer._template_miner_instance is None


def test_build_catalog_writes_a_services_own_catalog(estate, catalogs, monkeypatch):
    seen = []

    def fake(identifier, window, ctx):
        seen.append(ctx)
        records = [{"timestamp": f"t{i}", "level": "INFO",
                    "message": "demographic field mismatch found", "app_name": "demo-api"}
                   for i in range(3)]
        return FetchResult(records=records, diagnostics=FetchDiagnostics(source="fake"))

    monkeypatch.setattr(source_chain, "fetch_with_fallback", fake)

    build_catalog.main(["--service", DEMO, "--refids", "ref-1", "ref-2"])

    written = json.loads(log_scope.catalog_path_for(DEMO).read_text(encoding="utf-8"))
    assert [entry["classification"] for entry in written] == ["decision-marker"]
    assert log_scope.drain3_state_file_for(DEMO).exists()
    assert all(ctx.apps == ("demo-api", "demo-worker", "shared-gw") for ctx in seen)


def test_build_catalog_refuses_an_unregistered_service(estate, catalogs):
    with pytest.raises(SystemExit):
        build_catalog.main(["--service", "svc-unknown", "--refids", "ref-1"])


# ======================================================================
# The decision vocabulary
# ======================================================================

def test_the_decision_vocabulary_is_the_generic_words_or_the_packs(estate):
    demo = log_scope.for_service(DEMO).decision_vocabulary
    other = log_scope.for_service(OTHER).decision_vocabulary

    assert demo.search("Packet was REJECTED by the policy")
    assert demo.search("DEMOGRAPHIC field MISMATCH on the name")
    assert not other.search("demographic field mismatch on the name")
    assert not demo.search("MAN_DEDUP candidates found")


def test_a_caller_with_no_service_keeps_the_biometric_words():
    """The shipped registry: the unscoped vocabulary is what every caller
    matched before vocabularies were per service."""
    vocabulary = log_scope.unscoped().decision_vocabulary
    for line in ("man_dedup candidates found", "biometric face match score 0.9",
                 "dedup result: reject", "quality check failed", "packet status REJECTED"):
        assert vocabulary.search(line), line


def test_the_guardrails_keep_the_packs_decision_lines(estate):
    lines = [{"timestamp": "t1", "level": "INFO", "app_name": "demo-api",
              "message": "demographic mismatch on field 7"}]

    kept = reducer.apply_evidence_guardrails(
        [], lines, vocabulary=log_scope.for_service(DEMO).decision_vocabulary)
    generic = reducer.apply_evidence_guardrails([], lines)

    assert len(kept["decision_vocabulary_lines"]) == 1
    assert generic["decision_vocabulary_lines"] == []


# ======================================================================
# The routes and the graph fetch for the packet's service
# ======================================================================

def _payload(event_id, stage="Biometric", code="RESIDENT_MAN_DEDUP_REJECT_TD"):
    return {
        "eventId": event_id,
        "flowMetaData": {"stage": stage, "subStage": "MDD_POLICY_BATCH_1"},
        "packetMetaData": {"refId": "REF-1", "srn": "SRN-1", "enrolmentType": "U"},
        "packetExecutionSummary": {
            "packetStatus": "REJECTED",
            "errorData": [{"type": None, "errorReasonCode": code}],
        },
    }


@pytest.fixture
def storage(tmp_path, monkeypatch):
    import src.api.routes as routes
    import src.core.checkpointer as checkpointer

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "store"))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(pipeline, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(tool_registry, "get_casebook_storage", lambda: store)
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", lambda payload: None)
    monkeypatch.setenv("RUNBOOK_MODE", "off")
    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "checkpoints.db")
    checkpointer.reset_checkpointer()
    monkeypatch.setattr(orch, "_agent", None)
    yield store
    checkpointer.reset_checkpointer()


@pytest.mark.parametrize("stage, code, policy, expected", [
    # The shipped registry: a biometric packet searches enu-biometric.
    ("Biometric", "RESIDENT_MAN_DEDUP_REJECT_TD", "skip", BIO),
    # Unresolved, `record` mode: analysed with the pre-registry pack, so its
    # fetch is the environment's lists, as before.
    ("Nowhere", "NOT_A_DOCUMENTED_CODE", "skip", None),
    # Unresolved and admitted with the default pack: nothing is searched.
    ("Nowhere", "NOT_A_DOCUMENTED_CODE", "default_pack", sr.DEFAULT_PACK),
])
def test_fetch_logs_searches_the_packets_service(storage, monkeypatch, stage, code,
                                                 policy, expected):
    monkeypatch.setenv(sr.ENV_UNRESOLVED, policy)
    fetch = MagicMock(return_value="logs")
    monkeypatch.setattr("src.api.routes.fetch_and_persist_logs", fetch)

    fetch_logs(MessagePayload(**_payload("evt-route", stage=stage, code=code)))

    assert fetch.call_args.kwargs["service"] == expected


def test_the_graphs_live_fetch_searches_the_packets_service(storage, monkeypatch):
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content=json.dumps({
        "rejection_description": "rejected", "synthesis": "resubmit",
        "action": "RESIDENT_PACKET_RESUBMIT", "resident_action": "NEW_PACKET",
        "confidence": 0.9}))]}
    monkeypatch.setattr(orch, "is_reviewer_approved", lambda _feedback: True)
    fetch = MagicMock(return_value="Log fetching disabled.")

    with patch.object(orch, "build_agent", side_effect=lambda *a, **k: agent), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "fetch_and_persist_logs", fetch):
        orch.get_agent().invoke(
            {"payload": _payload("evt-graph"), "retry_count": 0},
            config={"configurable": {"thread_id": "evt-graph"}})

    assert fetch.call_args.kwargs["service"] == BIO


# ======================================================================
# Key-based redaction
# ======================================================================

@pytest.fixture
def clean_redaction(monkeypatch):
    for var in ("K8S_REDACT_ENABLED", "K8S_REDACT_EXTRA_PATTERNS", "REDACT_JSON_KEYS"):
        monkeypatch.delenv(var, raising=False)


PLACEHOLDER = "[REDACTED:JSON_FIELD]"


def test_json_field_values_are_redacted_at_any_depth(clean_redaction):
    text = '{"packet":{"resident":{"name":"Ramesh Kumar","dob":"1990-01-01"},"refId":"R-1"}}'

    result = redaction.redact_text(text)

    assert "Ramesh" not in result.text and "1990" not in result.text
    assert json.loads(result.text)["packet"]["resident"] == {"name": PLACEHOLDER,
                                                             "dob": PLACEHOLDER}
    assert '"refId":"R-1"' in result.text
    assert result.counts == {"JSON_FIELD": 2}


def test_escaped_json_inside_a_string_is_redacted(clean_redaction):
    # A payload logged as a JSON string: its quotes are escaped, and the name
    # itself holds an escaped quote.
    inner = json.dumps({"name": 'Ra"mesh', "gender": "M", "ok": True})
    text = "request body=" + json.dumps(inner)

    result = redaction.redact_text(text)

    assert "Ra" not in result.text.replace("[REDACTED", "")
    decoded = json.loads(json.loads(result.text.split("=", 1)[1]))
    assert decoded == {"name": PLACEHOLDER, "gender": PLACEHOLDER, "ok": True}


def test_arrays_objects_and_numbers_are_redacted_whole(clean_redaction):
    text = ('{"address":{"line1":"12 MG Road","city":"Pune"},'
            '"firstName":["Asha","Devi"],"pincode":560001,"names":["kept"]}')

    result = redaction.redact_text(text)

    assert json.loads(result.text) == {"address": PLACEHOLDER, "firstName": PLACEHOLDER,
                                       "pincode": PLACEHOLDER, "names": ["kept"]}


def test_brackets_and_quotes_inside_strings_do_not_end_a_value_early(clean_redaction):
    text = '{"address":{"line1":"Flat [2] {B}","note":"say \\"hi\\""},"after":"kept"}'

    result = redaction.redact_text(text)

    assert json.loads(result.text) == {"address": PLACEHOLDER, "after": "kept"}


def test_empty_and_null_values_and_other_keys_are_left_alone(clean_redaction):
    text = '{"Name": null, "gender": "", "married": false, "state": "IN_PROGRESS"}'

    result = redaction.redact_text(text)

    assert result.text == text
    assert result.counts == {}


def test_keys_match_case_insensitively_and_a_cut_off_value_is_redacted_to_the_end(
        clean_redaction):
    result = redaction.redact_text('{"DateOfBirth": "1990-01-01", "NAME": "Ramesh Ku')

    assert result.text == f'{{"DateOfBirth": "{PLACEHOLDER}", "NAME": "{PLACEHOLDER}"'


def test_key_redaction_is_idempotent(clean_redaction):
    once = redaction.redact_text('{"name":"Ramesh","nested":"{\\"dob\\":\\"1990\\"}"}')
    twice = redaction.redact_text(once.text)

    assert twice.text == once.text
    assert twice.counts == {}


def test_the_key_list_is_configurable(clean_redaction, monkeypatch):
    monkeypatch.setenv("REDACT_JSON_KEYS", "motherTongue, Religion")

    result = redaction.redact_text('{"name":"Ramesh","religion":"X","motherTongue":"Y"}')

    assert json.loads(result.text) == {"name": "Ramesh", "religion": PLACEHOLDER,
                                       "motherTongue": PLACEHOLDER}
    assert redaction.json_keys() == ("mothertongue", "religion")


def test_a_blank_key_list_means_the_default(clean_redaction, monkeypatch):
    monkeypatch.setenv("REDACT_JSON_KEYS", "  ")
    assert "dateofbirth" in redaction.json_keys()


def test_key_redaction_runs_on_every_record_the_pipeline_persists(clean_redaction):
    records = [{"timestamp": "t", "level": "INFO", "app_name": "a",
                "message": 'resident {"name":"Ramesh","refId":"123456789012"}'}]

    counts = redaction.redact_records(records, allowlist=["123456789012"])

    assert records[0]["message"] == f'resident {{"name":"{PLACEHOLDER}","refId":"123456789012"}}'
    assert counts == {"JSON_FIELD": 1}


# ======================================================================
# The redaction audit
# ======================================================================

def test_the_audit_counts_what_redaction_leaves(clean_redaction, tmp_path):
    sample = tmp_path / "sample" / "raw_logs.txt"
    sample.parent.mkdir()
    sample.write_text("\n".join([
        # Redacted, so nothing is left.
        '[t] [svc] [INFO] {"name":"Ramesh","dob":"1990-01-01"} mobile 9876543210',
        # A form JSON-key redaction does not cover: counted.
        "[t] [svc] [INFO] ResidentDto(name=Ramesh, gender=F, pincode=560001)",
        "[t] [svc] [INFO] {'name': 'Ravi'}",
        # Nothing personal in these.
        "[t] [svc] [INFO] name: null, gender=, thread_name=worker-1",
        "",
    ]), encoding="utf-8")

    result = redaction_audit.audit("svc-demo", [str(tmp_path / "sample")])

    assert (result.files, result.lines) == (1, 4)
    assert result.redacted == {"JSON_FIELD": 2, "MOBILE": 1}
    assert result.residual == {"key:name": 2, "key:gender": 1, "key:pincode": 1}
    assert result.examples["key:name"] == [f"{sample}:2", f"{sample}:3"]


def test_the_audit_passes_and_fails_by_exit_status(clean_redaction, tmp_path, capsys):
    clean = tmp_path / "clean.log"
    clean.write_text('{"name":"Ramesh"} and 123456789012\n', encoding="utf-8")
    dirty = tmp_path / "dirty.log"
    dirty.write_text("name=Ramesh\n", encoding="utf-8")

    assert redaction_audit.main(["--service", "svc", str(clean)]) == 0
    assert "The audit passes" in capsys.readouterr().out

    assert redaction_audit.main(["--service", "svc", str(dirty), "--json"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["residual_total"] == 1
    # Findings say where, never what.
    assert "Ramesh" not in json.dumps(report)

    assert redaction_audit.main(["--service", "svc", str(tmp_path / "missing")]) == 2
