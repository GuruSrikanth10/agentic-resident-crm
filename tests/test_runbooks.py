import pytest

from src.utils.runbook_store import (
    _resolve_runbook_path,
    generate_rule_fingerprint,
    RUNBOOK_ROOT
)
from src.utils.runbook_validator import validate_generic_text

def test_key_derivation():
    # Basic U/E types
    p1 = _resolve_runbook_path("ERR_123", "U", RUNBOOK_ROOT)
    assert p1.name == "ERR_123__U.json"
    
    p2 = _resolve_runbook_path("ERR_123", "E", RUNBOOK_ROOT)
    assert p2.name == "ERR_123__E.json"
    
    # ANY fallback when missing
    p3 = _resolve_runbook_path("ERR_123", None, RUNBOOK_ROOT)
    assert p3.name == "ERR_123__ANY.json"

def test_path_guard():
    # Must reject invalid reason_code format
    with pytest.raises(ValueError):
        _resolve_runbook_path("../escape", "U", RUNBOOK_ROOT)
        
    with pytest.raises(ValueError):
        _resolve_runbook_path("bad space", "U", RUNBOOK_ROOT)

def test_fingerprint_stability():
    rule1 = {"a": 1, "b": {"c": 2, "d": 3}}
    rule2 = {"b": {"d": 3, "c": 2}, "a": 1}
    
    fp1 = generate_rule_fingerprint(rule1)
    fp2 = generate_rule_fingerprint(rule2)
    assert fp1 == fp2
    assert fp1.startswith("sha256:")

def test_validator_regex():
    # Good text
    assert not validate_generic_text("The biometric data was of poor quality.", ["eid1"])
    
    # Bad texts
    assert validate_generic_text("Failed on a62c2f21-82fc-4ea5-b5f7-f1388651a134.", [])
    assert validate_generic_text("Date 2026-08-11T10:00:00Z.", [])
    assert validate_generic_text("Date 2026-08-11.", [])
    assert validate_generic_text("SRN is 12345678901234", [])
    
    # Source matching
    assert validate_generic_text("Source eid is 998877", ["998877"])
