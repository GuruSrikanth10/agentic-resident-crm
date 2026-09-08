"""
Phase 3 of DLT_PLAN.md -- payload identifier extraction and case identity.

Two properties carry the phase:

* A case id must satisfy `EVENT_ID_PATTERN`. It is interpolated into
  filesystem paths and S3 keys, and that pattern is what stops a `../../`
  value escaping the storage root.

* `derive_case_id` returns None rather than inventing a placeholder when a
  coordinate is missing. A placeholder would let two unrelated messages
  collide on one case id, and the second would be silently skipped as a
  duplicate.
"""
import pytest
from pydantic import ValidationError

from src.dlt.identity import (
    MAX_CASE_ID_LENGTH,
    derive_case_id,
    is_valid_case_id,
    sanitise,
)
from src.dlt.payload import (
    extract_by_path,
    extract_by_search,
    extract_ref_id,
)
from src.models.dlt_schemas import DltMessage

REFERENCE_TOPIC = "ENU.UPDATE.CHECKER.COMPLETION.V1"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("DLT_REFID_PATH", "DLT_REFID_KEYS"):
        monkeypatch.delenv(var, raising=False)
    yield


# ======================================================================
# Dotted-path extraction
# ======================================================================

def test_path_extraction():
    payload = {"packetMetaData": {"refId": "REF-123"}}
    assert extract_by_path(payload, "packetMetaData.refId") == "REF-123"


def test_path_indexes_into_lists():
    payload = {"events": [{"refId": "first"}, {"refId": "second"}]}
    assert extract_by_path(payload, "events.0.refId") == "first"
    assert extract_by_path(payload, "events.1.refId") == "second"


def test_missing_path_returns_none_not_raises():
    """A stale configured path must degrade to the fallback search."""
    payload = {"packetMetaData": {"refId": "REF-123"}}
    for path in ("nope", "packetMetaData.absent", "packetMetaData.refId.deeper",
                 "events.0.refId", "packetMetaData.0"):
        assert extract_by_path(payload, path) is None


def test_path_out_of_range_index_returns_none():
    assert extract_by_path({"events": [{"refId": "x"}]}, "events.9.refId") is None


def test_empty_path_or_payload():
    assert extract_by_path({"a": 1}, None) is None
    assert extract_by_path({"a": 1}, "") is None
    assert extract_by_path(None, "a") is None


def test_non_scalar_target_is_rejected():
    """A dict at the target means the path is wrong, not that refId is a dict."""
    payload = {"packetMetaData": {"refId": {"nested": "value"}}}
    assert extract_by_path(payload, "packetMetaData.refId") is None


def test_integer_identifier_is_accepted_as_text():
    assert extract_by_path({"refId": 12345}, "refId") == "12345"


def test_boolean_is_not_an_identifier():
    assert extract_by_path({"refId": True}, "refId") is None


def test_blank_string_is_not_an_identifier():
    assert extract_by_path({"refId": "   "}, "refId") is None


# ======================================================================
# Fallback search
# ======================================================================

def test_search_finds_a_nested_identifier():
    payload = {"a": {"b": {"packetMetaData": {"refId": "DEEP"}}}}
    assert extract_by_search(payload) == "DEEP"


def test_search_prefers_the_shallowest_match():
    """In the reference trace the failing lookup is for a dedup *candidate*, so
    a nested refId may belong to a different packet than the one that failed."""
    payload = {"refId": "TOP", "candidates": [{"refId": "NESTED"}]}
    assert extract_by_search(payload) == "TOP"


def test_search_honours_key_order():
    assert extract_by_search({"referenceId": "B", "refId": "A"}) == "A"


def test_search_descends_into_lists():
    assert extract_by_search({"items": [{"refId": "IN_LIST"}]}) == "IN_LIST"


def test_search_respects_the_depth_cap():
    node = payload = {}
    for _ in range(20):
        node["next"] = {}
        node = node["next"]
    node["refId"] = "TOO_DEEP"
    assert extract_by_search(payload) is None


def test_search_respects_the_node_cap():
    """A wide payload is as expensive to walk as a deep one."""
    payload: dict = {"items": [{"filler": str(i)} for i in range(5000)]}
    payload["items"].append({"refId": "LAST"})
    assert extract_by_search(payload) is None


def test_search_returns_none_on_empty_payloads():
    for payload in (None, {}, [], {"unrelated": "value"}):
        assert extract_by_search(payload) is None


def test_search_keys_are_configurable(monkeypatch):
    monkeypatch.setenv("DLT_REFID_KEYS", "customId")
    assert extract_by_search({"customId": "X"}) == "X"
    assert extract_by_search({"refId": "X"}) is None


# ======================================================================
# Combined extraction
# ======================================================================

def test_configured_path_wins_over_search(monkeypatch):
    monkeypatch.setenv("DLT_REFID_PATH", "packetMetaData.refId")
    payload = {"refId": "FROM_SEARCH", "packetMetaData": {"refId": "FROM_PATH"}}
    assert extract_ref_id(payload) == "FROM_PATH"


def test_search_covers_a_wrong_configured_path(monkeypatch):
    """Getting DLT_REFID_PATH wrong is survivable, which is the point of it
    being config rather than code."""
    monkeypatch.setenv("DLT_REFID_PATH", "totally.wrong.path")
    assert extract_ref_id({"packetMetaData": {"refId": "RECOVERED"}}) == "RECOVERED"


def test_no_identifier_anywhere_is_not_an_error():
    """The case still proceeds header-only; only the log lane is skipped."""
    assert extract_ref_id({"nothing": "useful"}) is None


# ======================================================================
# Case identity
# ======================================================================

def test_reference_sample_case_id():
    case_id = derive_case_id(REFERENCE_TOPIC, 63, 3352)
    assert case_id == "dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352"
    assert is_valid_case_id(case_id)


def test_case_id_is_deterministic():
    assert derive_case_id(REFERENCE_TOPIC, 63, 3352) == derive_case_id(REFERENCE_TOPIC, 63, 3352)


def test_distinct_coordinates_yield_distinct_ids():
    ids = {
        derive_case_id(REFERENCE_TOPIC, 63, 3352),
        derive_case_id(REFERENCE_TOPIC, 63, 3353),
        derive_case_id(REFERENCE_TOPIC, 64, 3352),
        derive_case_id("OTHER.TOPIC", 63, 3352),
    }
    assert len(ids) == 4


def test_offset_zero_is_a_valid_coordinate():
    """`if not offset` would treat partition 0 / offset 0 as missing."""
    assert derive_case_id(REFERENCE_TOPIC, 0, 0) == "dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-0-0"


def test_incomplete_coordinates_yield_none():
    assert derive_case_id(None, 63, 3352) is None
    assert derive_case_id(REFERENCE_TOPIC, None, 3352) is None
    assert derive_case_id(REFERENCE_TOPIC, 63, None) is None


@pytest.mark.parametrize("topic", [
    "topic with spaces",
    "topic/with/slashes",
    "../../escape",
    "topic\nwith\nnewlines",
    "topic$with%symbols",
])
def test_hostile_topic_names_are_sanitised_and_still_valid(topic):
    case_id = derive_case_id(topic, 1, 2)
    assert case_id is not None
    assert is_valid_case_id(case_id)
    assert "/" not in case_id
    assert " " not in case_id


def test_path_traversal_cannot_survive_sanitisation():
    """`EVENT_ID_PATTERN` permits dots, so `..` survives as text -- but the
    property that matters is that no path *separator* does. The result is one
    literal directory name (`dlt-..-..-etc-passwd-1-2`), not a traversal, and
    it can never be exactly `.` or `..` because of the `dlt-` prefix. This is
    the same guarantee `eventId` relies on in `src/models/schemas.py`."""
    case_id = derive_case_id("../../etc/passwd", 1, 2)
    assert case_id is not None
    assert is_valid_case_id(case_id)
    assert "/" not in case_id and "\\" not in case_id
    assert case_id not in (".", "..")
    assert case_id.startswith("dlt-")

    import os
    root = os.path.join("storage", "root")
    assert os.path.normpath(os.path.join(root, case_id)).startswith(root)


def test_long_topic_is_truncated_within_the_length_bound():
    case_id = derive_case_id("A" * 300, 63, 3352)
    assert case_id is not None
    assert len(case_id) <= MAX_CASE_ID_LENGTH
    assert is_valid_case_id(case_id)
    assert case_id.endswith("-63-3352"), "coordinates must survive truncation"


def test_long_topics_sharing_a_prefix_do_not_collide():
    """Truncation alone would map both onto the same id."""
    a = derive_case_id("A" * 300 + "-alpha", 1, 2)
    b = derive_case_id("A" * 300 + "-beta", 1, 2)
    assert a is not None and b is not None
    assert a != b
    assert len(a) <= MAX_CASE_ID_LENGTH and len(b) <= MAX_CASE_ID_LENGTH


def test_sanitise_leaves_permitted_characters_alone():
    assert sanitise("Topic.NAME:v1-2_3") == "Topic.NAME:v1-2_3"


def test_is_valid_case_id_rejects_empties_and_overlong():
    assert not is_valid_case_id(None)
    assert not is_valid_case_id("")
    assert not is_valid_case_id("x" * (MAX_CASE_ID_LENGTH + 1))


# ======================================================================
# Wire model
# ======================================================================

def test_dlt_message_round_trips():
    message = DltMessage(
        case_id="dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352",
        headers={"kafka_original-topic": "T", "empty": None},
        payload={"packetMetaData": {"refId": "REF"}},
        ref_id="REF",
    )
    assert message.model_dump()["ref_id"] == "REF"
    assert DltMessage(**message.model_dump()) == message


def test_dlt_message_rejects_a_case_id_that_escapes_storage():
    with pytest.raises(ValidationError):
        DltMessage(case_id="../../etc/passwd")


def test_dlt_message_tolerates_unknown_fields():
    """Upstream adds fields; rejecting on one would be an outage."""
    message = DltMessage(case_id="dlt-T-0-1", something_new="value")
    assert message.case_id == "dlt-T-0-1"


def test_dlt_message_accepts_a_missing_payload_and_ref_id():
    message = DltMessage(case_id="dlt-T-0-1")
    assert message.payload is None
    assert message.ref_id is None
    assert message.headers == {}


def test_dlt_message_accepts_a_non_dict_payload():
    """The payload schema is upstream's contract, not ours."""
    assert DltMessage(case_id="dlt-T-0-1", payload=["a", "list"]).payload == ["a", "list"]


def test_dlt_message_keeps_raw_text_when_the_payload_is_unparseable():
    message = DltMessage(case_id="dlt-T-0-1", payload=None, payload_raw="not json")
    assert message.payload_raw == "not json"
