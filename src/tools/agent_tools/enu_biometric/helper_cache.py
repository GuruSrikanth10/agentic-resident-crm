"""
bio_helper_cache_store: the durable copy of dedup's intermediate working data.

The service keeps these objects in Redis, which expires them; the
asynchronous steps (rule-engine response, portal response, candidate
completion, update-checker join) often arrive after that, so the service
reads them back from here. One row per (refid, record_type), upserted --
only the latest version exists, and rows remain after processing ends because
cleanup deletes only the Redis copy.

The records are large JSON, so every tool returns the fields a question needs
rather than the blob: the per-type tools project the documented fields (and
tolerate the wrapper and key-casing variations the docs leave open), and
`bio_get_helper_record_fields` reads arbitrary paths with MySQL JSON_EXTRACT.
"""
import re

from src.tools.agent_tools import agent_tool
from src.tools.agent_tools.enu_biometric._process_db import (
    PROCESS_DB,
    InvalidArgument,
    as_list,
    as_number,
    bounded_json,
    field,
    max_rows,
    name_arg,
    parse_json,
    plain,
    query,
    redact,
    refid_arg,
    run_lookup,
)

ABIS_CANDIDATES = "AbisMwCandidateRecord"
CANDIDATE_FACTS = "ApplicantCandidateHelperRecord"
PARKING_VERDICTS = "ParkingHelperRecord"
UPDATE_CHECKER = "UpdateCheckerHelperRecord"

#: What a missing row of each documented type means. A missing record means
#: that step did not happen; it is not an error.
MISSING_MEANS = {
    ABIS_CANDIDATES: "No ABIS middleware response with matched candidates was "
                     "processed for this packet.",
    CANDIDATE_FACTS: "The case did not reach rule-engine batch PB1.",
    PARKING_VERDICTS: "The packet was not parked as an update packet (only parked "
                      "update packets have this record).",
    UPDATE_CHECKER: "No Update Checker response was recorded (only update packets "
                    "that went through the Update Checker, or that rule batch PB0 "
                    "flagged, have this record).",
}

_SCORE_FIELDS = ("scaledScore", "faceScore", "fingerLeftSlapScore",
                 "fingerRightSlapScore", "fingerBothThumbsScore",
                 "leftIrisScore", "rightIrisScore")

_MATCH_RESULT_FIELDS = ("modality", "applicantAttemptNumber", "candidateAttemptNumber",
                        "applicantQuality", "candidateQuality", "matchScore",
                        "matched", "errorCode", "errorMessage")

_APPLICANT_FLAGS = ("applicantRefId", "isWhitelistedApplicant",
                    "isFirstTimeBioUpdate", "sendToPortalDueToBMSError")

#: Entries listed from one per-candidate list before the rest are only counted.
MAX_LIST_ITEMS = 50
MAX_MATCH_RESULTS_PER_CANDIDATE = 30
MAX_DIAGNOSTICS_CHARS = 400
MAX_FIELD_VALUE_CHARS = 4000

#: MySQL JSON path legs: .key, ."quoted key", .*, [N], [*], **. Anything else
#: is refused before it reaches the server -- an invalid path is a server
#: error, which the retry decorator would take for a transient one.
_PATH = re.compile(r'^\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\."[^"\\]{1,200}"|\.\*'
                   r'|\[(?:\d{1,6}|\*)\]|\*\*)*$')
MAX_PATHS = 10
MAX_PATH_CHARS = 300


@agent_tool(PROCESS_DB)
def bio_list_helper_records(refid: str) -> str:
    """List which bio_helper_cache_store records exist for a packet, without their contents.

    There are four record types: AbisMwCandidateRecord (the candidates ABIS
    matched, with scores), ApplicantCandidateHelperRecord (per-candidate facts
    the rule engine decided on; only packets that reached PB1),
    ParkingHelperRecord (cross-match verdicts; only parked update packets) and
    UpdateCheckerHelperRecord (the Update Checker response; only update packets
    that went through it). A missing record means that step did not happen for
    this packet; it is not an error. Records hold only the latest version and
    remain after processing ends.

    refid: the packet's refId.
    """
    return run_lookup(lambda: _list_records(refid))


@agent_tool(PROCESS_DB)
def bio_get_abis_candidates(refid: str) -> str:
    """Which candidates ABIS returned for a packet, and with what scores (AbisMwCandidateRecord).

    Returns the request type and response status, and every matched candidate
    with its highest scaledScore across ABIS engines -- the service uses the
    highest -- plus each engine's face, finger-slap, both-thumbs and iris
    scores. Candidates are ordered by that highest score. A candidate the
    service dropped to break a deadlock can still be listed here, because the
    trimmed response is not written back. found=false means no ABIS response
    with matched candidates was processed for this packet.

    refid: the applicant packet's refId.
    """
    return run_lookup(lambda: _abis_candidates(refid), list_key="candidates")


@agent_tool(PROCESS_DB)
def bio_get_candidate_facts(refid: str, candidate_ref_id: str = "") -> str:
    """What the service knew about each candidate when the rule engine decided (ApplicantCandidateHelperRecord).

    Per candidate: eid, masterRefId, masterEid, masterRefIdCreationDate,
    packetType, packetStatus, uidStatus, maxAbisScore and the flags
    isCandidateWhiteListed, isInconsistent, isWrongCapture, isCandidateNonNRC
    and isCandidateTD (true duplicate); UIDs are masked. Also the applicant's
    flags (isWhitelistedApplicant, isFirstTimeBioUpdate,
    sendToPortalDueToBMSError) and which candidates have an Aadhaar. Written
    just before rule-engine batch PB1, so only packets that reached PB1 have it.
    Candidates are ordered by maxAbisScore, highest first.

    refid: the applicant packet's refId. candidate_ref_id: optional; one
    candidate's refId to return only that candidate.
    """
    return run_lookup(lambda: _candidate_facts(refid, candidate_ref_id),
                      list_key="candidates")


@agent_tool(PROCESS_DB)
def bio_get_parking_match_verdicts(refid: str, candidate_ref_id: str = "") -> str:
    """Per-candidate, per-modality cross-match verdicts kept while an update packet was parked (ParkingHelperRecord).

    Each candidate maps LEFT_SLAP, RIGHT_SLAP, BOTH_THUMBS, LEFT_IRIS,
    RIGHT_IRIS and FACE to MATCH, NO_MATCH, ANOMALOUS_MATCH or
    CANNOT_DETERMINE; counts per verdict are given per candidate and overall.
    Only parked update packets have this record.

    refid: the applicant packet's refId. candidate_ref_id: optional; one
    candidate's refId to return only that candidate.
    """
    return run_lookup(lambda: _parking_verdicts(refid, candidate_ref_id),
                      list_key="candidates")


@agent_tool(PROCESS_DB)
def bio_get_update_checker_result(refid: str) -> str:
    """What the Update Checker returned for an update packet (UpdateCheckerHelperRecord).

    Returns applicantRefid, masterRefid, latestBioUpdateRefid and isFlagged,
    and per candidate its modality match results (modality, attempt numbers,
    qualities, matchScore, matched, errorCode, errorMessage) with counts of
    matched, unmatched and errored results. Written when an Update Checker
    response arrives or when rule batch PB0 flags an applicant, so only update
    packets that went through the Update Checker have it.

    refid: the applicant packet's refId.
    """
    return run_lookup(lambda: _update_checker(refid), list_key="candidates")


@agent_tool(PROCESS_DB)
def bio_get_helper_record_fields(refid: str, record_type: str, json_paths: list[str]) -> str:
    """Read specific fields of one bio_helper_cache_store record with MySQL JSON_EXTRACT.

    Use it for a field the other helper-record tools do not return. Paths use
    MySQL JSON path syntax: $.key, $.key.child, $.list[0], $.list[*].key,
    $**.key, and double quotes around a key containing other characters, e.g.
    $.candidateHelperRecordMap."<candidateRefId>".maxAbisScore. A null value
    means the path does not exist in the record, or holds null. Long values are
    cut.

    refid: the packet's refId. record_type: the exact record type, e.g.
    ApplicantCandidateHelperRecord (bio_list_helper_records shows which exist).
    json_paths: 1 to 10 paths.
    """
    return run_lookup(lambda: _record_fields(refid, record_type, json_paths))


# ---------------------------------------------------------------------------

def _load(refid: str, record_type: str):
    """The newest row of one record type, as (metadata, parsed JSON), or None."""
    rows = query(
        "SELECT helper_record_key, sid, enrl_type, helper_record, "
        "creation_date, last_updated_date "
        "FROM bio_helper_cache_store "
        "WHERE refid = :refid AND record_type = :record_type "
        "ORDER BY helper_record_key DESC LIMIT 2",
        {"refid": refid, "record_type": record_type})
    if not rows:
        return None
    row = rows[0]
    meta = {"helper_record_key": plain(row.get("helper_record_key")),
            "sid": plain(row.get("sid")),
            "enrl_type": plain(row.get("enrl_type")),
            "creation_date": plain(row.get("creation_date")),
            "last_updated_date": plain(row.get("last_updated_date"))}
    if len(rows) > 1:
        # Writes are upserts, so this should not happen; say so if it does.
        meta["more_than_one_row"] = True
    return meta, parse_json(row.get("helper_record"))


def _not_found(refid: str, record_type: str) -> dict:
    return {"refid": refid, "record_type": record_type, "found": False,
            "meaning": MISSING_MEANS.get(record_type,
                                         "No row of this record type for this refid.")}


def _allowlist(refid: str, meta: dict) -> list:
    return [value for value in (refid, meta.get("sid")) if value]


def _unrecognised(refid: str, record_type: str, meta: dict, document) -> dict:
    return {"refid": refid, "record_type": record_type, "found": True,
            "record": meta, "shape_recognized": False,
            "top_level_keys": sorted(document)[:50] if isinstance(document, dict) else None,
            "hint": "The record does not have the documented shape; read the "
                    "fields you need with bio_get_helper_record_fields."}


def _list_records(refid) -> dict:
    refid = refid_arg(refid)
    rows = query(
        "SELECT helper_record_key, sid, enrl_type, record_type, "
        "LENGTH(helper_record) AS helper_record_bytes, "
        "creation_date, last_updated_date "
        "FROM bio_helper_cache_store WHERE refid = :refid "
        "ORDER BY helper_record_key LIMIT 50",
        {"refid": refid})
    records = [{column: plain(row.get(column))
                for column in ("record_type", "sid", "enrl_type", "helper_record_bytes",
                               "creation_date", "last_updated_date")}
               for row in rows]
    present = {row.get("record_type") for row in rows}
    return {
        "refid": refid,
        "found": bool(rows),
        "records": records,
        "missing_record_types": {record_type: meaning
                                 for record_type, meaning in MISSING_MEANS.items()
                                 if record_type not in present},
    }


def _abis_candidates(refid) -> dict:
    refid = refid_arg(refid)
    loaded = _load(refid, ABIS_CANDIDATES)
    if loaded is None:
        return _not_found(refid, ABIS_CANDIDATES)
    meta, document = loaded

    wrapper = field(document, "abisMWResponseNewSeda")
    root = wrapper if isinstance(wrapper, dict) else document
    container = field(root, "abisResponses")
    # {"abisResponse": [...]} as documented, or a bare list. A container
    # without the documented list is an unknown shape, never "no engines".
    engines = as_list(field(container, "abisResponse")) \
        if isinstance(container, dict) else as_list(container)
    if not isinstance(root, dict) or not engines:
        return _unrecognised(refid, ABIS_CANDIDATES, meta, document)

    by_candidate: dict = {}
    engine_summaries = []
    for engine in engines:
        abis_id = field(engine, "abisId")
        holder = field(engine, "candidates")
        # An engine that matched nobody carries an empty holder.
        matched = as_list(field(holder, "matchedCandidate")) \
            if isinstance(holder, dict) else as_list(holder)
        summary = {"abisId": abis_id, "matched_candidates": len(matched)}
        diagnostics = field(engine, "diagnostics")
        if diagnostics not in (None, "", [], {}):
            summary["diagnostics"] = bounded_json(diagnostics, MAX_DIAGNOSTICS_CHARS)
        engine_summaries.append(summary)

        for candidate in matched:
            ref = field(candidate, "candidateRefId")
            if not ref:
                continue
            scores = {name: field(candidate, name) for name in _SCORE_FIELDS
                      if field(candidate, name) is not None}
            entry = by_candidate.setdefault(str(ref), {
                "candidateRefId": str(ref), "highest_scaledScore": None, "engines": []})
            entry["engines"].append({"abisId": abis_id, **scores})
            score = as_number(scores.get("scaledScore"))
            best = as_number(entry["highest_scaledScore"])
            if score is not None and (best is None or score > best):
                entry["highest_scaledScore"] = scores.get("scaledScore")

    candidates = sorted(
        by_candidate.values(),
        key=lambda entry: (as_number(entry["highest_scaledScore"]) is None,
                           -(as_number(entry["highest_scaledScore"]) or 0.0)))
    limit = max_rows()
    result = {
        "refid": refid,
        "record_type": ABIS_CANDIDATES,
        "found": True,
        "record": meta,
        "requestType": field(root, "requestType"),
        "responseStatus": field(root, "responseStatus"),
        "referenceId": field(root, "referenceId"),
        "engines": engine_summaries,
        "candidate_count": len(candidates),
        "candidates": candidates[:limit],
    }
    if len(candidates) > limit:
        result["candidates_not_listed"] = len(candidates) - limit
    return redact(result, _allowlist(refid, meta))


def _candidate_facts(refid, candidate_ref_id) -> dict:
    refid = refid_arg(refid)
    wanted = name_arg("candidate_ref_id", candidate_ref_id, limit=64)
    loaded = _load(refid, CANDIDATE_FACTS)
    if loaded is None:
        return _not_found(refid, CANDIDATE_FACTS)
    meta, document = loaded

    candidate_map = field(document, "candidateHelperRecordMap")
    if not isinstance(candidate_map, dict):
        return _unrecognised(refid, CANDIDATE_FACTS, meta, document)

    result: dict = {
        "refid": refid,
        "record_type": CANDIDATE_FACTS,
        "found": True,
        "record": meta,
        "applicant": {name: field(document, name) for name in _APPLICANT_FLAGS},
        "candidatesWithAadhaar": _listed(field(document, "candidatesWithAadhaar")),
        "candidatesWithOutAadhaar": _listed(field(document, "candidatesWithOutAadhaar")),
        "candidate_count": len(candidate_map),
    }
    if wanted is not None:
        entry = _lookup_key(candidate_map, wanted)
        result["candidate_ref_id"] = wanted
        result["candidate_found"] = entry is not None
        result["candidates"] = [] if entry is None else [{"candidateRefId": wanted, **_as_dict(entry)}]
    else:
        entries = [{"candidateRefId": str(ref), **_as_dict(entry)}
                   for ref, entry in candidate_map.items()]
        entries.sort(key=lambda entry: (as_number(field(entry, "maxAbisScore")) is None,
                                        -(as_number(field(entry, "maxAbisScore")) or 0.0)))
        limit = max_rows()
        result["candidates"] = entries[:limit]
        if len(entries) > limit:
            result["candidates_not_listed"] = len(entries) - limit
    return redact(result, _allowlist(refid, meta))


def _parking_verdicts(refid, candidate_ref_id) -> dict:
    refid = refid_arg(refid)
    wanted = name_arg("candidate_ref_id", candidate_ref_id, limit=64)
    loaded = _load(refid, PARKING_VERDICTS)
    if loaded is None:
        return _not_found(refid, PARKING_VERDICTS)
    meta, document = loaded

    verdict_map = _verdict_map(document)
    if verdict_map is None:
        return _unrecognised(refid, PARKING_VERDICTS, meta, document)

    if wanted is not None:
        entry = _lookup_key(verdict_map, wanted)
        selected = {} if entry is None else {wanted: entry}
    else:
        selected = verdict_map

    overall: dict = {}
    candidates = []
    for ref, modalities in selected.items():
        counts: dict = {}
        for verdict in modalities.values():
            counts[str(verdict)] = counts.get(str(verdict), 0) + 1
            overall[str(verdict)] = overall.get(str(verdict), 0) + 1
        candidates.append({"candidateRefId": str(ref), "verdicts": modalities,
                           "counts": counts})

    limit = max_rows()
    result: dict = {
        "refid": refid,
        "record_type": PARKING_VERDICTS,
        "found": True,
        "record": meta,
        "candidate_count": len(verdict_map),
        "verdict_counts": overall,
        "candidates": candidates[:limit],
    }
    if wanted is not None:
        result["candidate_ref_id"] = wanted
        result["candidate_found"] = bool(selected)
    if len(candidates) > limit:
        result["candidates_not_listed"] = len(candidates) - limit
    return redact(result, _allowlist(refid, meta))


def _update_checker(refid) -> dict:
    refid = refid_arg(refid)
    loaded = _load(refid, UPDATE_CHECKER)
    if loaded is None:
        return _not_found(refid, UPDATE_CHECKER)
    meta, document = loaded
    if not isinstance(document, dict):
        return _unrecognised(refid, UPDATE_CHECKER, meta, document)

    candidates = []
    for item in as_list(field(document, "consistencyMasterResult")):
        results = as_list(field(item, "matchResults"))
        projected = [{name: field(entry, name) for name in _MATCH_RESULT_FIELDS
                      if field(entry, name) is not None}
                     for entry in results]
        matched = sum(1 for entry in projected if entry.get("matched") is True)
        unmatched = sum(1 for entry in projected if entry.get("matched") is False)
        errored = sum(1 for entry in projected if entry.get("errorCode") not in (None, ""))
        candidate = {"candidateRefid": field(item, "candidateRefid"),
                     "counts": {"matched": matched, "not_matched": unmatched,
                                "with_error": errored, "results": len(projected)},
                     "matchResults": projected[:MAX_MATCH_RESULTS_PER_CANDIDATE]}
        if len(projected) > MAX_MATCH_RESULTS_PER_CANDIDATE:
            candidate["matchResults_not_listed"] = \
                len(projected) - MAX_MATCH_RESULTS_PER_CANDIDATE
        candidates.append(candidate)

    limit = max_rows()
    result: dict = {
        "refid": refid,
        "record_type": UPDATE_CHECKER,
        "found": True,
        "record": meta,
        "applicantRefid": field(document, "applicantRefid"),
        "masterRefid": field(document, "masterRefid"),
        "latestBioUpdateRefid": field(document, "latestBioUpdateRefid"),
        "isFlagged": field(document, "isFlagged"),
        "candidate_count": len(candidates),
        "candidates": candidates[:limit],
    }
    if len(candidates) > limit:
        result["candidates_not_listed"] = len(candidates) - limit
    return redact(result, _allowlist(refid, meta))


def _record_fields(refid, record_type, json_paths) -> dict:
    refid = refid_arg(refid)
    record_type = name_arg("record_type", record_type, required=True)
    if isinstance(json_paths, str):
        json_paths = [json_paths]
    paths = [str(path).strip() for path in (json_paths or []) if str(path).strip()]
    paths = list(dict.fromkeys(paths))
    if not paths:
        raise InvalidArgument("json_paths needs at least one path, e.g. $.isFlagged.")
    if len(paths) > MAX_PATHS:
        raise InvalidArgument(f"json_paths takes at most {MAX_PATHS} paths.")
    for path in paths:
        if (len(path) > MAX_PATH_CHARS or not _PATH.match(path)
                or path.endswith("**")):
            raise InvalidArgument(f"{path!r} is not a supported JSON path; use "
                                  f"forms like $.key, $.list[0], $.list[*].key, "
                                  f"$.map.\"key-with-dashes\".")

    extracts = ", ".join(f"JSON_EXTRACT(helper_record, :path{index}) AS value{index}"
                         for index in range(len(paths)))
    params = {"refid": refid, "record_type": record_type}
    params.update({f"path{index}": path for index, path in enumerate(paths)})
    rows = query(
        f"SELECT helper_record_key, sid, last_updated_date, {extracts} "
        "FROM bio_helper_cache_store "
        "WHERE refid = :refid AND record_type = :record_type "
        "ORDER BY helper_record_key DESC LIMIT 1",
        params)
    if not rows:
        return _not_found(refid, record_type)

    row = rows[0]
    meta = {"helper_record_key": plain(row.get("helper_record_key")),
            "sid": plain(row.get("sid")),
            "last_updated_date": plain(row.get("last_updated_date"))}
    values = {}
    for index, path in enumerate(paths):
        values[path] = bounded_json(_extracted(row.get(f"value{index}")),
                                    MAX_FIELD_VALUE_CHARS)
    return redact({"refid": refid, "record_type": record_type, "found": True,
                   "record": meta, "fields": values},
                  _allowlist(refid, meta))


def _extracted(value):
    """JSON_EXTRACT's result as a value. MySQL returns JSON text ('"x"',
    'true', '{...}'); a value that is not JSON text is kept as it is."""
    parsed = parse_json(value)
    if parsed is None and isinstance(plain(value), str) and plain(value) != "null":
        return plain(value)
    return parsed


def _listed(value) -> dict:
    items = as_list(value)
    return {"count": len(items), "items": items[:MAX_LIST_ITEMS]}


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {"value": value}


def _lookup_key(mapping: dict, key: str):
    if key in mapping:
        return mapping[key]
    lowered = key.lower()
    for candidate, value in mapping.items():
        if str(candidate).lower() == lowered:
            return value
    return None


def _looks_like_verdicts(value) -> bool:
    return (isinstance(value, dict) and bool(value)
            and all(isinstance(modalities, dict)
                    and all(not isinstance(v, (dict, list)) for v in modalities.values())
                    for modalities in value.values()))


def _verdict_map(document):
    """{candidateRefId: {modality: verdict}}, unwrapped from a single-key
    wrapper object if the record carries one."""
    if _looks_like_verdicts(document):
        return document
    if isinstance(document, dict) and len(document) == 1:
        inner = next(iter(document.values()))
        if _looks_like_verdicts(inner):
            return inner
    if isinstance(document, dict) and not document:
        return {}
    return None
