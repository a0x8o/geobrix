import mapbox_vector_tile as mvt
from shapely.geometry import Point
from shapely import to_wkb

from databricks.labs.gbx.pyvx import _mvt


def _decode(blob, layer="layer"):
    tile = mvt.decode(blob)
    return tile[layer]["features"]


def test_encode_layer_preserves_native_attr_types():
    feats = [
        {"geometry": to_wkb(Point(10, 20)), "properties": {"name": "a", "pop": 42, "h": 3.5, "ok": True}},
    ]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    props = _decode(blob)[0]["properties"]
    assert props["name"] == "a"
    assert props["pop"] == 42 and isinstance(props["pop"], int)
    assert props["h"] == 3.5 and isinstance(props["h"], float)
    assert props["ok"] is True


def test_encode_layer_unsupported_type_falls_back_to_string():
    feats = [{"geometry": to_wkb(Point(1, 1)), "properties": {"b": b"\x00\x01"}}]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    props = _decode(blob)[0]["properties"]
    assert isinstance(props["b"], str)  # bytes -> str fallback


def test_pyramid_tiles_caps_and_schema():
    # A point at lon/lat 0,0 over zooms 0..2 -> one tile per zoom (3 rows).
    rows = list(_mvt.pyramid_tiles(to_wkb(Point(0.0, 0.0)), {"id": 7}, 0, 2, "layer", 4096))
    zs = sorted(r[0] for r in rows)
    assert zs == [0, 1, 2]
    for (z, x, y, blob) in rows:
        assert isinstance(z, int) and isinstance(x, int) and isinstance(y, int)
        assert isinstance(blob, (bytes, bytearray)) and len(blob) > 0
        # Each emitted tile must be a well-formed, decodable MVT proto whose
        # attributes survive end-to-end with native types (id stays an int).
        feats = _decode(blob)
        assert len(feats) == 1
        assert feats[0]["properties"]["id"] == 7 and isinstance(feats[0]["properties"]["id"], int)


def test_pyramid_rejects_too_many_tiles():
    import pytest
    with pytest.raises(ValueError):
        from shapely.geometry import box
        list(_mvt.pyramid_tiles(to_wkb(box(-179, -85, 179, 85)), {}, 0, 20, "layer", 4096))
