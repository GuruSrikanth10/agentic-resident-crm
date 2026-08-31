"""Phase C3 -- version parsing and ordering.

Deliberately hostile. A bad comparison here looks exactly like a good one and
stays invisible until someone audits a specific case, so the table below is
written to catch the orderings that a plausible-but-wrong implementation gets
right by accident and the ones it gets wrong silently.

Two rules carry most of the weight:

* Unparseable means None, and None propagates through every comparison. C5
  turns that into an `UNKNOWN` verdict. Never a guess.
* Equal cores with an ambiguous qualifier compare *equal*, not ordered -- so
  Trap T9 (a fix merged with no version bump) can be caught by requiring
  strictly-ahead rather than at-least.
"""
import pytest

from src.dlt import versions as V

HARBOR = "mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric"


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reference,expected", [
    (f"{HARBOR}/1.0.0-release.42", "1.0.0-release.42"),
    (f"{HARBOR}:1.0.0", "1.0.0"),
    (f"{HARBOR}:1.2.3@sha256:abcdef", "1.2.3"),
    ("registry.local:5000/ankalan/enu-biometric/1.0.0", "1.0.0"),
    ("1.0.0", "1.0.0"),
    ("", ""),
    (None, ""),
])
def test_a_version_is_extracted_from_an_image_reference(reference, expected):
    assert V.version_of(reference) == expected


def test_a_full_reference_parses_to_the_same_version_as_its_bare_tag():
    assert V.compare(f"{HARBOR}/1.0.0-release.42", "1.0.0-release.42") == 0


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,core,build,snapshot", [
    ("1.0.0", (1, 0, 0), None, False),
    ("1.0.0-release.42", (1, 0, 0), 42, False),
    ("1.0.0-42", (1, 0, 0), 42, False),
    ("1.0.1-SNAPSHOT", (1, 0, 1), None, True),
    ("1.0.1-snapshot", (1, 0, 1), None, True),
    ("2", (2,), None, False),
    ("1.0.0.4", (1, 0, 0, 4), None, False),
    ("v2.3.4", (2, 3, 4), None, False),
    ("  1.0.0  ", (1, 0, 0), None, False),
])
def test_a_version_parses_into_a_core_and_a_qualifier(text, core, build, snapshot):
    parsed = V.parse(text)
    assert parsed is not None
    assert parsed.core == core
    assert parsed.build == build
    assert parsed.snapshot is snapshot


@pytest.mark.parametrize("text", [
    "latest", "main", "release", "", "   ", None,
    "sha256:abcdef0123", "not-a-version", "-1.0.0",
])
def test_anything_without_a_numeric_core_refuses_to_parse(text):
    assert V.parse(text) is None


def test_rc1_is_a_name_not_a_build_counter():
    """`release.42` is a counter and orders; `rc1` is a name and does not."""
    assert V.parse("1.0.0-rc1").build is None
    assert V.parse("1.0.0-release.1").build == 1


# ---------------------------------------------------------------------------
# Ordering -- Trap T7
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lower,higher", [
    # The lexical trap. "1.0.10" < "1.0.9" as strings.
    ("1.0.9", "1.0.10"),
    ("1.9.0", "1.10.0"),
    ("9.0.0", "10.0.0"),
    # Build counters, same trap one level down.
    ("1.0.0-release.9", "1.0.0-release.12"),
    ("1.0.0-release.2", "1.0.0-release.100"),
    # Ordinary precedence.
    ("1.0.0", "1.0.1"),
    ("1.0.0", "2.0.0"),
    ("1.0.0", "1.1.0"),
    # A snapshot is the work leading up to the release, so it precedes it.
    ("1.0.1-SNAPSHOT", "1.0.1"),
    # And a snapshot of a later version still beats an earlier release.
    ("1.0.0", "1.0.1-SNAPSHOT"),
])
def test_ordering_is_numeric_not_lexical(lower, higher):
    assert V.compare(lower, higher) == -1
    assert V.compare(higher, lower) == 1


@pytest.mark.parametrize("left,right", [
    ("1.0.0", "1.0.0"),
    # Trailing zeros do not make a version smaller.
    ("1.0", "1.0.0"),
    ("1", "1.0.0"),
    # The ambiguous case, and the reason T9's mitigation works: `1.0.0` is the
    # pom's number and `1.0.0-release.42` is a build of it. Nothing in either
    # string says which came first.
    ("1.0.0", "1.0.0-release.42"),
    ("1.0.0-rc1", "1.0.0"),
])
def test_versions_that_carry_no_ordering_compare_equal(left, right):
    assert V.compare(left, right) == 0
    assert V.compare(right, left) == 0


@pytest.mark.parametrize("left,right", [
    ("latest", "1.0.0"),
    ("1.0.0", "latest"),
    (None, "1.0.0"),
    ("1.0.0", None),
    ("", ""),
])
def test_an_unparseable_side_yields_no_ordering(left, right):
    assert V.compare(left, right) is None


def test_comparison_is_a_total_order_over_the_parseable_set():
    """Antisymmetry and transitivity, checked over every pair.

    A hand-written comparator that gets one branch backwards usually still
    passes a handful of examples; it does not survive this.
    """
    corpus = ["1.0.0", "1.0.1", "1.0.9", "1.0.10", "1.1.0", "2.0.0",
              "1.0.0-release.1", "1.0.0-release.9", "1.0.0-release.12",
              "1.0.1-SNAPSHOT", "1.0", "1"]

    for a in corpus:
        assert V.compare(a, a) == 0
        for b in corpus:
            forward, backward = V.compare(a, b), V.compare(b, a)
            assert forward == -backward, f"{a} vs {b} is not antisymmetric"
            for c in corpus:
                if V.compare(a, b) < 0 and V.compare(b, c) < 0:
                    assert V.compare(a, c) < 0, f"{a} < {b} < {c} is not transitive"


# ---------------------------------------------------------------------------
# The two questions C5 actually asks
# ---------------------------------------------------------------------------

def test_at_least_answers_whether_a_fix_is_running():
    assert V.at_least("1.0.0-release.45", "1.0.0-release.43") is True
    assert V.at_least("1.0.0-release.43", "1.0.0-release.43") is True
    assert V.at_least("1.0.0-release.42", "1.0.0-release.43") is False


def test_at_least_returns_none_rather_than_false_when_unknown():
    """False would silently read "unknown" as "not deployed" and park a
    packet forever."""
    assert V.at_least("latest", "1.0.0") is None
    assert V.at_least(None, "1.0.0") is None


def test_is_ahead_requires_strictly_ahead_which_is_the_t9_mitigation():
    """A fix merged with no version bump leaves the pom reading the number
    already running. `at_least` calls that deployed; `is_ahead` does not."""
    assert V.is_ahead("1.0.0", "1.0.0") is False
    assert V.at_least("1.0.0", "1.0.0") is True
    assert V.is_ahead("1.0.1", "1.0.0") is True


def test_is_ahead_returns_none_rather_than_false_when_unknown():
    assert V.is_ahead("latest", "1.0.0") is None


# ---------------------------------------------------------------------------
# Rolling deploys
# ---------------------------------------------------------------------------

def test_the_lowest_version_is_the_safe_reading_mid_rollout():
    """A replay may land on any pod, so the floor is what matters."""
    assert str(V.lowest(["1.0.0-release.43", "1.0.0-release.42"])) == "1.0.0-release.42"
    assert str(V.lowest(["1.0.10", "1.0.9"])) == "1.0.9"
    assert str(V.lowest(["1.0.0"])) == "1.0.0"


def test_one_unparseable_entry_poisons_the_floor(monkeypatch):
    """Skipping it would report a higher floor than actually exists -- the
    unreadable version might be the low one."""
    assert V.lowest(["1.0.0", "latest"]) is None


def test_an_empty_set_has_no_lowest():
    assert V.lowest([]) is None
    assert V.lowest(None) is None


# ---------------------------------------------------------------------------
# The Version object itself
# ---------------------------------------------------------------------------

def test_versions_are_not_ordered_by_the_dataclass():
    """`order=True` would compare `raw` first -- a string comparison, which is
    exactly the bug this module exists to prevent."""
    a, b = V.parse("1.0.10"), V.parse("1.0.9")
    with pytest.raises(TypeError):
        a < b            # noqa: B015  -- the raise is the assertion


def test_a_version_renders_as_the_text_it_came_from():
    assert str(V.parse(f"{HARBOR}/1.0.0-release.42")) == "1.0.0-release.42"


# ---------------------------------------------------------------------------
# The operator escape hatch
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_pattern_cache(monkeypatch):
    monkeypatch.delenv("DLT_VERSION_PATTERN", raising=False)
    V._pattern_cache.clear()
    yield
    V._pattern_cache.clear()


def test_a_tag_with_the_version_at_the_back_needs_a_pattern(monkeypatch):
    assert V.parse("enu-biometric-1.0.0") is None

    monkeypatch.setenv("DLT_VERSION_PATTERN", r"^enu-biometric-(?P<version>.+)$")
    V._pattern_cache.clear()

    parsed = V.parse("enu-biometric-1.0.0")
    assert parsed is not None and parsed.core == (1, 0, 0)


def test_the_pattern_applies_after_the_image_reference_is_stripped(monkeypatch):
    monkeypatch.setenv("DLT_VERSION_PATTERN", r"^enu-biometric-(?P<version>.+)$")
    V._pattern_cache.clear()

    parsed = V.parse(f"{HARBOR}/enu-biometric-1.0.0-release.7")
    assert parsed is not None
    assert parsed.core == (1, 0, 0)
    assert parsed.build == 7


@pytest.mark.parametrize("pattern", [
    "(((",                      # does not compile
    r"^enu-(?P<v>.+)$",         # no `version` group
])
def test_a_broken_pattern_degrades_to_the_default_parser(monkeypatch, pattern):
    """A bad override must not refuse every version in the system."""
    monkeypatch.setenv("DLT_VERSION_PATTERN", pattern)
    V._pattern_cache.clear()

    parsed = V.parse("1.0.0")
    assert parsed is not None and parsed.core == (1, 0, 0)


def test_a_pattern_that_does_not_match_leaves_the_version_alone(monkeypatch):
    monkeypatch.setenv("DLT_VERSION_PATTERN", r"^other-(?P<version>.+)$")
    V._pattern_cache.clear()

    parsed = V.parse("1.0.0-release.42")
    assert parsed is not None and parsed.build == 42
