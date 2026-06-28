import pytest
from databricks.labs.gbx.vizx._simplify import normalize_spec

def test_defaults_applied():
    s = normalize_spec(None)
    assert s["budget_mb"] == 64 and s["min_z"] == 0 and s["max_z"] == 10 and s["effort"] == "fast"

def test_override_and_validation():
    assert normalize_spec({"max_z": 12})["max_z"] == 12
    with pytest.raises(ValueError):
        normalize_spec({"min_z": 8, "max_z": 4})
    with pytest.raises(ValueError):
        normalize_spec({"effort": "turbo"})
