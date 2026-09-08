"""
Phase 9 of KUBERNETES_LOGS_PLAN.md -- the source chain going live.

`LOG_SOURCE` defaults to `elastic`, so this ships dark: the packet path
behaves exactly as it did until an operator opts in.
"""
import json
from unittest.mock import patch

import pytest
from elasticsearch import ConnectionError as ESConnectionError

from src.log_pipeline.sources import chain
from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.types import (
    EvidenceGap,
    FetchContext,
    FetchDiagnostics,
    FetchResult,
    GapType,
    TimeWindow,
)

KUBELET_TS = "2026-01-01T10:15:30.000000000Z"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("LOG_SOURCE", "ES_MOCK_FILE", "K8S_FIXTURE_DIR",
                "K8S_DEFAULT_NAMESPACE", "LOG_SNAPSHOT_REUSE", "ES_HOST"):
        monkeypatch.delenv(var, raising=False)
    k8s_client_module.reset_client()
    yield
    k8s_client_module.reset_client()


def _ctx(event_id="evt-1"):
    return FetchContext(event_id=event_id)


def _result(records, ok=True, source="test"):
    return FetchResult(
        records=records,
        diagnostics=FetchDiagnostics(source=source, records_returned=len(records)),
        ok=ok,
    )


def _record(msg="line"):
    return {"timestamp": KUBELET_TS, "level": "INFO", "message": msg, "app_name": "svc"}


class _FakeSource:
    def __init__(self, name, result=None, raises=None):
        self.name = name
        self._result = result
        self._raises = raises
        self.calls = 0

    def fetch(self, identifier, window, ctx):
        self.calls += 1
        if self._raises:
            raise self._raises
        return self._result


# ======================================================================
# Chain parsing
# ======================================================================

def test_default_chain_is_kubernetes_then_elastic():
    names = [s.name for s in chain.configured_chain()]
    assert names == ["kubernetes", "elastic"]


@pytest.mark.parametrize("value,expected", [
    ("elastic", ["elastic"]),
    ("kubernetes", ["kubernetes"]),
    ("kubernetes,elastic", ["kubernetes", "elastic"]),
    ("elastic,kubernetes", ["elastic", "kubernetes"]),
    ("  KUBERNETES , elastic ", ["kubernetes", "elastic"]),
])
def test_chain_parsing(monkeypatch, value, expected):
    monkeypatch.setenv("LOG_SOURCE", value)
    assert [s.name for s in chain.configured_chain()] == expected


def test_unknown_source_is_ignored(monkeypatch):
    monkeypatch.setenv("LOG_SOURCE", "kubernetes,bogus")
    assert [s.name for s in chain.configured_chain()] == ["kubernetes"]


def test_entirely_unusable_chain_falls_back_to_elastic(monkeypatch):
    """Leaving the pipeline with no source at all would be worse than
    ignoring the misconfiguration."""
    monkeypatch.setenv("LOG_SOURCE", "bogus,alsobogus")
    assert [s.name for s in chain.configured_chain()] == ["elastic"]


def test_blank_log_source_falls_back_to_the_default_chain(monkeypatch):
    monkeypatch.setenv("LOG_SOURCE", "   ")
    assert [s.name for s in chain.configured_chain()] == ["kubernetes", "elastic"]


# ======================================================================
# Fallback semantics -- scenarios 21, 22, 23
# ======================================================================

def test_first_source_with_records_wins():
    first = _FakeSource("kubernetes", _result([_record("from-k8s")]))
    second = _FakeSource("elastic", _result([_record("from-es")]))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.records[0]["message"] == "from-k8s"
    assert second.calls == 0, "the chain must stop at the first success"


def test_empty_source_falls_through(monkeypatch):
    """Scenario 21: K8s returned nothing, ES has the logs."""
    first = _FakeSource("kubernetes", _result([]))
    second = _FakeSource("elastic", _result([_record("from-es")]))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.records[0]["message"] == "from-es"
    assert second.calls == 1


def test_failed_source_falls_through():
    """Scenario 22: K8s failed outright, ES has the logs."""
    first = _FakeSource("kubernetes", _result([], ok=False))
    second = _FakeSource("elastic", _result([_record("from-es")]))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.records[0]["message"] == "from-es"


def test_fallback_is_recorded_as_a_gap():
    """Provenance: an operator reading a casebook must be able to tell where
    its evidence came from and what was tried first."""
    first = _FakeSource("kubernetes", _result([]))
    second = _FakeSource("elastic", _result([_record()]))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    fallback = [g for g in result.gaps if g.gap_type == GapType.SOURCE_FALLBACK]
    assert len(fallback) == 1
    assert "kubernetes" in fallback[0].detail
    assert "elastic" in fallback[0].detail


def test_no_fallback_gap_when_the_first_source_wins():
    only = _FakeSource("kubernetes", _result([_record()]))
    with patch.object(chain, "configured_chain", return_value=[only]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.gaps == []


def test_exhausted_chain_preserves_the_last_outcome():
    """Scenario 23. Looked-and-found-nothing must not become
    could-not-look just because the chain ran out."""
    first = _FakeSource("kubernetes", _result([], ok=False))
    second = _FakeSource("elastic", _result([], ok=True))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.records == []
    assert result.ok is True, "the final source looked successfully"
    assert any(g.gap_type == GapType.SOURCE_FALLBACK for g in result.gaps)


def test_single_source_finding_nothing_is_not_flagged_incomplete():
    """A lone source that looked and legitimately found nothing is a clean
    confirmed-absent. Labelling it SOURCE_FALLBACK and flagging the trace
    INCOMPLETE would blur exactly the distinction principle 3 protects, and
    would put a scary banner on every quiet packet."""
    only = _FakeSource("elastic", _result([], ok=True))

    with patch.object(chain, "configured_chain", return_value=[only]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.records == []
    assert result.ok is True
    assert result.gaps == []


def test_single_source_failing_is_still_flagged():
    only = _FakeSource("elastic", _result([], ok=False))

    with patch.object(chain, "configured_chain", return_value=[only]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.ok is False
    assert any(g.gap_type == GapType.SOURCE_FALLBACK for g in result.gaps)


def test_all_sources_failing_reports_could_not_look():
    first = _FakeSource("kubernetes", _result([], ok=False))
    second = _FakeSource("elastic", _result([], ok=False))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.ok is False


# ======================================================================
# Exception handling -- preserving retry/breaker semantics
# ======================================================================

def test_mid_chain_exception_advances_to_the_next_source():
    first = _FakeSource("kubernetes", raises=RuntimeError("cluster exploded"))
    second = _FakeSource("elastic", _result([_record("from-es")]))

    with patch.object(chain, "configured_chain", return_value=[first, second]):
        result = chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())

    assert result.records[0]["message"] == "from-es"


def test_final_source_exception_propagates():
    """Critical: @retry_transient and @es_breaker on fetch_elastic_logs
    dispatch on exception TYPE. Swallowing the last source's exception would
    silently disable both."""
    only = _FakeSource("elastic", raises=ESConnectionError("cluster down"))

    with patch.object(chain, "configured_chain", return_value=[only]):
        with pytest.raises(ESConnectionError):
            chain.fetch_with_fallback("evt-1", TimeWindow.default(), _ctx())


# ======================================================================
# End-to-end through reduce_logs
# ======================================================================

def _write_kibana_csv(path, event_id):
    def row(ts, level, msg):
        doc = ('{""@timestamp"":""%s"",""level"":""%s"",'
               '""message"":""%s"",""application_name"":""enu-biometric""}' % (ts, level, msg))
        return f'"{ts}","{doc}"\n'
    path.write_text('"time","_source"\n' + row("2026-01-01T00:00:01Z", "INFO", f"hello {event_id}"))


def _write_k8s_fixture(root, namespace, pod, lines):
    pod_dir = root / namespace / pod
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running", "labels": {"app": "enu-biometric"},
        "containers": ["app"], "restart_counts": {},
    }), encoding="utf-8")


def test_es_mock_file_still_drives_the_elastic_leg(monkeypatch, tmp_path):
    """The existing local CSV workflow must keep working, including as a leg
    of a chain (design principle 5)."""
    from src.log_pipeline import pipeline

    csv_path = tmp_path / "logs.csv"
    _write_kibana_csv(csv_path, "evt-csv")
    monkeypatch.setenv("ES_MOCK_FILE", str(csv_path))
    monkeypatch.setenv("LOG_SOURCE", "elastic")

    out = pipeline.reduce_logs("evt-csv")
    assert "hello evt-csv" in out


def test_kubernetes_only_chain_end_to_end(monkeypatch, tmp_path):
    from src.log_pipeline import pipeline

    fixtures_root = tmp_path / "k8s"
    _write_k8s_fixture(fixtures_root, "enu", "enu-biometric-a", [
        f"{KUBELET_TS} INFO processing evt-k8s",
        f"{KUBELET_TS} ERROR dedup rejected evt-k8s",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(fixtures_root))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    monkeypatch.setenv("LOG_SOURCE", "kubernetes")
    monkeypatch.setenv("LOG_SNAPSHOT_REUSE", "false")
    monkeypatch.setattr(
        "src.log_pipeline.snapshot._casesheets_root", lambda: tmp_path / "sheets"
    )

    out = pipeline.reduce_logs("evt-k8s")

    assert "dedup rejected evt-k8s" in out
    # Pod attribution is rendered so the LLM can tell replicas apart.
    assert "enu-biometric-a" in out


def test_chain_falls_back_from_kubernetes_to_elastic_end_to_end(monkeypatch, tmp_path):
    from src.log_pipeline import pipeline

    # Kubernetes has pods but nothing matching the identifier.
    fixtures_root = tmp_path / "k8s"
    _write_k8s_fixture(fixtures_root, "enu", "enu-biometric-a", [f"{KUBELET_TS} INFO unrelated"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(fixtures_root))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    monkeypatch.setattr(
        "src.log_pipeline.snapshot._casesheets_root", lambda: tmp_path / "sheets"
    )

    csv_path = tmp_path / "logs.csv"
    _write_kibana_csv(csv_path, "evt-both")
    monkeypatch.setenv("ES_MOCK_FILE", str(csv_path))
    monkeypatch.setenv("LOG_SOURCE", "kubernetes,elastic")

    out = pipeline.reduce_logs("evt-both")

    assert "hello evt-both" in out
    assert "SOURCE_FALLBACK" in out, "the banner must record what was tried"


def test_gap_banner_precedes_the_trace(monkeypatch, tmp_path):
    """The LLM must see that a trace is incomplete before it reads it."""
    from src.log_pipeline import pipeline

    fake = _FakeSource("kubernetes", FetchResult(
        records=[_record("some line")],
        diagnostics=FetchDiagnostics(source="kubernetes"),
        gaps=[EvidenceGap(GapType.LOG_ROTATION, "rotated at 09:14")],
    ))

    with patch.object(chain, "configured_chain", return_value=[fake]):
        out = pipeline.reduce_logs("evt-gap")

    assert out.index("EVIDENCE GAPS") < out.index("some line")
    assert "LOG_ROTATION" in out


def test_no_logs_still_reports_gaps(monkeypatch):
    """Otherwise 'no logs found' looks like confirmed-absent when it was
    really could-not-look."""
    from src.log_pipeline import pipeline

    fake = _FakeSource("kubernetes", FetchResult(
        records=[],
        diagnostics=FetchDiagnostics(source="kubernetes"),
        gaps=[EvidenceGap(GapType.TRUNCATED, "budget expired")],
        ok=False,
    ))

    with patch.object(chain, "configured_chain", return_value=[fake]):
        out = pipeline.reduce_logs("evt-none")

    assert "No logs found" in out
    assert "TRUNCATED" in out


# ======================================================================
# The retired mock
# ======================================================================

def test_fetch_kubernetes_logs_mock_is_gone():
    from src.tools.tool_registry import _TOOLS_MAP
    assert "fetch_kubernetes_logs" not in _TOOLS_MAP
