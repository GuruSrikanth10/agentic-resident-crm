"""Phase 1 of REMEDIATION_PLAN_2026_08_21.md -- evidence integrity.

Three ways the pipeline used to hand the LLM an incomplete trace without
saying so. Design principle 2: a fetch that returned less than was asked for
must announce it, because "we looked and found nothing" and "we could not
look" lead to opposite conclusions.
"""
from unittest.mock import patch

import pytest

from src.log_pipeline import reducer
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.sources.k8s.filtering import KeepAllSelector
from src.log_pipeline.types import GapType, TimeWindow


# ======================================================================
# 1.1 -- LOG_MAX_DOCUMENTS keeps the newest lines and announces the cap
# ======================================================================

def _hit(index: int, level: str = "INFO"):
    return {
        "_source": {
            "@timestamp": f"2026-08-21T10:{index:02d}:00Z",
            "level": level,
            "message": f"line {index}",
            "application_name": "enu-biometric",
        },
        "sort": [index, str(index)],
    }


class _FakeES:
    """Serves hits newest-first, mirroring the DESC sort the fetcher now uses."""

    def __init__(self, total: int, error_at: int = None):
        # Newest first: index `total - 1` down to 0.
        self.pages = []
        for index in range(total - 1, -1, -1):
            level = "ERROR" if index == error_at else "INFO"
            self.pages.append(_hit(index, level))
        self.calls = 0

    def search(self, **kwargs):
        self.calls += 1
        after = kwargs.get("search_after")
        start = 0
        if after:
            start = next(i for i, h in enumerate(self.pages) if h["sort"] == after) + 1
        size = kwargs["size"]
        return {"hits": {"hits": self.pages[start:start + size]}}


def test_truncation_keeps_the_newest_lines_not_the_oldest(monkeypatch):
    """The end of the trace is where the failure is.

    An ascending scan capped at N kept the OLDEST N and never even requested
    the newer pages, so `branch_on_error` saw no ERROR and a stuck packet was
    classified as a clean rejection.
    """
    from src.log_pipeline import fetcher

    monkeypatch.setenv("ES_HOST", "http://es.invalid:9200")
    monkeypatch.setenv("LOG_MAX_DOCUMENTS", "5")

    fake = _FakeES(total=12, error_at=11)  # the ERROR is the NEWEST line
    with patch.object(fetcher, "_get_es_client", return_value=fake):
        logs = fetcher.fetch_logs("evt-cap")

    assert len(logs) == 5
    # Returned in ascending order regardless of the descending scan.
    assert [entry["message"] for entry in logs] == [
        "line 7", "line 8", "line 9", "line 10", "line 11"
    ]
    # The evidence that matters survived.
    assert any(entry["level"] == "ERROR" for entry in logs)


def test_truncation_is_reported_through_out_diagnostics(monkeypatch):
    from src.log_pipeline import fetcher

    monkeypatch.setenv("ES_HOST", "http://es.invalid:9200")
    monkeypatch.setenv("LOG_MAX_DOCUMENTS", "5")

    diagnostics = {}
    with patch.object(fetcher, "_get_es_client", return_value=_FakeES(total=12)):
        fetcher.fetch_logs("evt-cap", out_diagnostics=diagnostics)

    assert diagnostics["truncated"] is True
    assert diagnostics["max_documents"] == 5


def test_an_uncapped_fetch_reports_no_truncation(monkeypatch):
    """The flag must mean something -- it cannot be always-on."""
    from src.log_pipeline import fetcher

    monkeypatch.setenv("ES_HOST", "http://es.invalid:9200")
    monkeypatch.setenv("LOG_MAX_DOCUMENTS", "5000")

    diagnostics = {}
    with patch.object(fetcher, "_get_es_client", return_value=_FakeES(total=12)):
        logs = fetcher.fetch_logs("evt-small", out_diagnostics=diagnostics)

    assert diagnostics["truncated"] is False
    assert [entry["message"] for entry in logs] == [f"line {i}" for i in range(12)]


def test_elastic_source_raises_a_truncated_gap(monkeypatch):
    """The gap is what reaches the banner and caps the model's confidence."""
    from src.log_pipeline.sources.elastic import ElasticLogSource
    from src.log_pipeline.types import FetchContext

    def fake_fetch(identifier, catalog=None, out_diagnostics=None):
        if out_diagnostics is not None:
            out_diagnostics["truncated"] = True
            out_diagnostics["max_documents"] = 50000
        return [{"timestamp": "t", "level": "INFO", "message": "m", "app_name": "a"}]

    with patch("src.log_pipeline.sources.elastic.fetch_logs", side_effect=fake_fetch):
        result = ElasticLogSource().fetch(
            "evt-1", TimeWindow(hours=2), FetchContext(event_id="evt-1"))

    assert [gap.gap_type for gap in result.gaps] == [GapType.TRUNCATED]
    assert "50000" in result.gaps[0].detail


def test_elastic_source_raises_no_gap_when_nothing_was_capped():
    from src.log_pipeline.sources.elastic import ElasticLogSource
    from src.log_pipeline.types import FetchContext

    def fake_fetch(identifier, catalog=None, out_diagnostics=None):
        if out_diagnostics is not None:
            out_diagnostics["truncated"] = False
        return [{"timestamp": "t", "level": "INFO", "message": "m", "app_name": "a"}]

    with patch("src.log_pipeline.sources.elastic.fetch_logs", side_effect=fake_fetch):
        result = ElasticLogSource().fetch(
            "evt-1", TimeWindow(hours=2), FetchContext(event_id="evt-1"))

    assert result.gaps == []


# ======================================================================
# 1.2 -- the reduced output is bounded
# ======================================================================

def _matching_lines(count: int):
    return [
        {"timestamp": f"t{i}", "level": "INFO",
         "message": f"packet status is PENDING for line {i}", "app_name": "a"}
        for i in range(count)
    ]


def test_decision_vocabulary_lines_are_bounded(monkeypatch):
    """20,000 matching lines used to become 20,000 full lines in the prompt --
    ~1.1MB, ~285k tokens, in a call with a 60s timeout."""
    monkeypatch.setattr(reducer, "MAX_DECISION_VOCABULARY_LINES", 100)

    assembled = reducer.apply_evidence_guardrails([], _matching_lines(20000))

    assert len(assembled["decision_vocabulary_lines"]) == 100
    assert assembled["decision_vocabulary_omitted"] == 19900


def test_the_bound_keeps_both_ends_of_the_sequence(monkeypatch):
    """Head and tail, not the first N: what the flow attempted and how it
    ended both carry information; the middle is where the repetition lives."""
    monkeypatch.setattr(reducer, "MAX_DECISION_VOCABULARY_LINES", 4)

    assembled = reducer.apply_evidence_guardrails([], _matching_lines(100))
    kept = [entry["message"] for entry in assembled["decision_vocabulary_lines"]]

    assert kept[0].endswith("line 0")
    assert kept[1].endswith("line 1")
    assert kept[-2].endswith("line 98")
    assert kept[-1].endswith("line 99")


def test_a_short_list_is_not_bounded_or_marked(monkeypatch):
    monkeypatch.setattr(reducer, "MAX_DECISION_VOCABULARY_LINES", 100)

    assembled = reducer.apply_evidence_guardrails([], _matching_lines(10))

    assert len(assembled["decision_vocabulary_lines"]) == 10
    assert assembled["decision_vocabulary_omitted"] == 0


def test_the_omission_is_visible_in_the_formatted_output(monkeypatch):
    """An omission the model cannot see is one it reasons past as though the
    lines were consecutive."""
    monkeypatch.setattr(reducer, "MAX_DECISION_VOCABULARY_LINES", 10)
    from src.log_pipeline.pipeline import _format_normal_path

    assembled = reducer.apply_evidence_guardrails([], _matching_lines(5000))
    text = _format_normal_path("evt-1", assembled, 5000, "some/path")

    assert "4990 further decision-vocabulary lines omitted" in text
    assert "LOG_MAX_DECISION_LINES" in text


def test_reduced_output_has_a_total_size_ceiling(monkeypatch):
    """The per-section bounds cap the parts; this caps the whole. The ERROR
    branch is bounded in lines, not characters, so a stack-trace-heavy trace
    can still blow the context window on a few hundred lines."""
    from src.log_pipeline import pipeline

    monkeypatch.setattr(pipeline, "MAX_REDUCED_CHARS", 5000)

    trimmed = pipeline._with_banner("", "X" * 200000)

    assert len(trimmed) <= 5000
    assert "LOG_MAX_REDUCED_CHARS" in trimmed


def test_the_size_ceiling_leaves_a_normal_trace_untouched(monkeypatch):
    from src.log_pipeline import pipeline

    monkeypatch.setattr(pipeline, "MAX_REDUCED_CHARS", 120000)
    body = "a normal reduced trace"

    assert pipeline._with_banner("", body) == body


def test_the_banner_survives_the_size_ceiling(monkeypatch):
    """The trace is trimmed, never the notice that it is incomplete."""
    from src.log_pipeline import pipeline

    monkeypatch.setattr(pipeline, "MAX_REDUCED_CHARS", 2000)
    banner = "--- EVIDENCE GAPS (the trace below is INCOMPLETE) ---"

    result = pipeline._with_banner(banner, "X" * 100000)

    assert result.startswith(banner)


# ======================================================================
# 1.3 -- redaction runs on every exit path
# ======================================================================

@pytest.fixture
def _restore_read_instance():
    from src.log_pipeline.sources.k8s import retrieval
    original = retrieval._read_instance
    yield
    retrieval._read_instance = original


def test_records_are_redacted_when_the_current_instance_read_fails(
        _restore_read_instance, monkeypatch):
    """A restarted container's previous-instance records used to leave this
    function unredacted when the current-instance read raised -- and
    `_fetch` hands them straight to `snapshot.save`."""
    from src.log_pipeline.sources.k8s import retrieval

    monkeypatch.setenv("K8S_REDACT_ENABLED", "true")

    def fake_read_instance(target, window, previous, selector):
        if previous:
            return (
                [{"timestamp": "t", "level": "INFO",
                  "message": "resident uid 123456789012 seen", "app_name": "c"}],
                retrieval.ParseStats(total=1, level_parsed=1), 10, False, "t",
            )
        raise RuntimeError("current read blew up")

    retrieval._read_instance = fake_read_instance

    outcome = retrieval.read_pod_logs(
        PodTarget(namespace="ns", pod_name="p1", container="c", restart_count=1),
        TimeWindow(hours=1),
        selector=KeepAllSelector(),
    )

    assert outcome.ok is False          # the failure is still reported
    assert outcome.records              # and the salvaged records are still kept
    assert "123456789012" not in outcome.records[0]["message"]
    assert "[REDACTED:AADHAAR]" in outcome.records[0]["message"]


def test_read_all_never_yields_unredacted_records_from_a_failed_pod(
        _restore_read_instance, monkeypatch):
    """read_all keeps a failed target's records regardless of `ok`, so the
    guarantee has to hold at the aggregate too."""
    from src.log_pipeline.sources.k8s import retrieval

    monkeypatch.setenv("K8S_REDACT_ENABLED", "true")

    def fake_read_instance(target, window, previous, selector):
        if previous:
            return (
                [{"timestamp": "t", "level": "INFO",
                  "message": "uid 123456789012", "app_name": "c"}],
                retrieval.ParseStats(total=1, level_parsed=1), 10, False, "t",
            )
        raise RuntimeError("boom")

    retrieval._read_instance = fake_read_instance

    outcome = retrieval.read_all(
        [PodTarget(namespace="ns", pod_name="p1", container="c", restart_count=1)],
        TimeWindow(hours=1),
        selector_factory=lambda: KeepAllSelector(),
    )

    assert outcome.records
    assert all("123456789012" not in r["message"] for r in outcome.records)


def test_the_happy_path_is_still_redacted_exactly_once(
        _restore_read_instance, monkeypatch):
    """Redaction is idempotent, so the finally covering the success path too
    must not double-count."""
    from src.log_pipeline.sources.k8s import retrieval

    monkeypatch.setenv("K8S_REDACT_ENABLED", "true")

    def fake_read_instance(target, window, previous, selector):
        return (
            [{"timestamp": "t", "level": "INFO",
              "message": "uid 123456789012", "app_name": "c"}],
            retrieval.ParseStats(total=1, level_parsed=1), 10, False, "t",
        )

    retrieval._read_instance = fake_read_instance

    outcome = retrieval.read_pod_logs(
        PodTarget(namespace="ns", pod_name="p1", container="c", restart_count=0),
        TimeWindow(hours=1),
        selector=KeepAllSelector(),
    )

    assert outcome.ok is True
    assert outcome.redaction_counts == {"AADHAAR": 1}
