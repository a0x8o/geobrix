import pytest
from databricks.labs.gbx.stac._sign import resolve_signer


def test_none_is_identity():
    s = resolve_signer(None)
    assert s("http://x/y.tif?token=abc") == "http://x/y.tif?token=abc"


def test_callable_passthrough():
    s = resolve_signer(lambda h: h + "?signed")
    assert s("http://x") == "http://x?signed"


def test_unknown_raises():
    with pytest.raises(ValueError):
        resolve_signer("not-a-strategy")
