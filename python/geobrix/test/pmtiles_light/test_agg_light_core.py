"""Spark-free tests for the light PMTiles archive assembler."""

import mapbox_vector_tile as mvt
import pytest
from pmtiles.reader import MmapSource, Reader
from shapely.geometry import Polygon

from databricks.labs.gbx.pmtiles._agg_light import _MAX_ARCHIVE_BYTES, _assemble_archive

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16  # sniffs as PNG


def _mvt_fake(i):  # arbitrary non-magic bytes => sniffs as MVT
    return b"mvt-payload-" + bytes([i % 256]) + b"\x00\x01\x02"


def _real_mvt_blob(poly_coords, prop_id: int, layer: str = "bldg") -> bytes:
    """Encode one POLYGON feature as a real MVT blob at extent=4096."""
    poly = Polygon(poly_coords)
    return mvt.encode(
        {
            "name": layer,
            "features": [
                {
                    "geometry": poly,
                    "properties": {"id": prop_id},
                }
            ],
        },
        default_options={"extents": 4096, "y_coord_down": True},
    )


_POLY_A = _real_mvt_blob(
    [(100, 100), (200, 100), (200, 200), (100, 200), (100, 100)], prop_id=1
)
_POLY_B = _real_mvt_blob(
    [(300, 300), (400, 300), (400, 400), (300, 400), (300, 300)], prop_id=2
)
_POLY_C_OTHER_LAYER = _real_mvt_blob(
    [(500, 500), (600, 500), (600, 600), (500, 600), (500, 500)],
    prop_id=3,
    layer="roads",
)


def _decode(blob, tmp_path):
    p = tmp_path / "a.pmtiles"
    p.write_bytes(blob)
    out = {}
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        for z in range(0, 6):
            n = 2**z
            for x in range(n):
                for y in range(n):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out


def _decode_mvt_from_archive(blob, z, x, y, tmp_path):
    """Extract and decode the MVT blob for (z, x, y) from a PMTiles archive."""
    p = tmp_path / "merge.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        raw = r.get(z, x, y)
    assert raw is not None, f"tile ({z},{x},{y}) missing from archive"
    return mvt.decode(raw)


def test_single_tile_roundtrip(tmp_path):
    blob = _assemble_archive([_mvt_fake(1)], [3], [2], [4], {})
    assert blob is not None
    assert _decode(blob, tmp_path) == {(3, 2, 4): _mvt_fake(1)}


def test_multi_zoom_roundtrip(tmp_path):
    data = [_mvt_fake(1), _mvt_fake(2), _mvt_fake(3)]
    zs, xs, ys = [2, 3, 3], [1, 2, 5], [1, 4, 6]
    got = _decode(_assemble_archive(data, zs, xs, ys, {}), tmp_path)
    assert got == {
        (2, 1, 1): _mvt_fake(1),
        (3, 2, 4): _mvt_fake(2),
        (3, 5, 6): _mvt_fake(3),
    }


def test_png_payload_roundtrip(tmp_path):
    got = _decode(_assemble_archive([_PNG], [1], [0], [0], {}), tmp_path)
    assert got == {(1, 0, 0): _PNG}


def test_metadata_roundtrip(tmp_path):
    blob = _assemble_archive([_mvt_fake(1)], [0], [0], [0], {"name": "demo", "n": 1})
    p = tmp_path / "m.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        assert r.metadata().get("name") == "demo"


def test_null_payloads_skipped(tmp_path):
    got = _decode(
        _assemble_archive(
            [None, _mvt_fake(2), None], [0, 1, 0], [0, 1, 0], [0, 1, 0], {}
        ),
        tmp_path,
    )
    assert got == {(1, 1, 1): _mvt_fake(2)}


def test_empty_group_returns_none():
    assert _assemble_archive([], [], [], [], {}) is None
    assert _assemble_archive([None], [0], [0], [0], {}) is None


def test_duplicate_tileid_dropped(tmp_path):
    # Two real MVT blobs for the same (z,x,y): new behavior MERGES them (not drop).
    # The archive must have exactly one entry for the tileid; it contains both features.
    blob = _assemble_archive([_POLY_A, _POLY_B], [2, 2], [1, 1], [1, 1], {})
    decoded = _decode_mvt_from_archive(blob, 2, 1, 1, tmp_path)
    feat_ids = {f["properties"]["id"] for f in decoded["bldg"]["features"]}
    assert feat_ids == {1, 2}, f"expected both feature ids merged; got {feat_ids}"


def test_cap_exceeded_raises():
    big = b"\x00" * (_MAX_ARCHIVE_BYTES + 1)
    with pytest.raises(ValueError, match="exceeds"):
        _assemble_archive([big], [0], [0], [0], {})


# ---------------------------------------------------------------------------
# New vector-merge tests
# ---------------------------------------------------------------------------


def test_vector_merge_two_features_same_tileid(tmp_path):
    """Two MVT blobs for the same (z,x,y) must merge into one tile with 2 features."""
    blob = _assemble_archive([_POLY_A, _POLY_B], [3, 3], [2, 2], [4, 4], {})
    assert blob is not None
    decoded = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    assert (
        "bldg" in decoded
    ), f"layer 'bldg' missing; got layers: {list(decoded.keys())}"
    feat_ids = {f["properties"]["id"] for f in decoded["bldg"]["features"]}
    assert feat_ids == {1, 2}, f"expected both feature ids; got {feat_ids}"


def test_vector_merge_geometry_type_preserved(tmp_path):
    """Merged features must retain POLYGON geometry type (not downgraded to Point)."""
    blob = _assemble_archive([_POLY_A, _POLY_B], [3, 3], [2, 2], [4, 4], {})
    decoded = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    for feat in decoded["bldg"]["features"]:
        assert feat["geometry"]["type"] == "Polygon", (
            f"feature id={feat['properties']['id']} geometry not Polygon: "
            f"{feat['geometry']['type']}"
        )


def test_vector_merge_multi_layer(tmp_path):
    """Blobs from different layers for the same tileid are both preserved."""
    blob = _assemble_archive([_POLY_A, _POLY_C_OTHER_LAYER], [3, 3], [2, 2], [4, 4], {})
    decoded = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    assert (
        "bldg" in decoded and "roads" in decoded
    ), f"expected both layers; got {list(decoded.keys())}"


def test_vector_merge_distinct_tileids_unchanged(tmp_path):
    """Blobs for distinct tileids are stored separately — no cross-tile bleed."""
    blob = _assemble_archive([_POLY_A, _POLY_B], [3, 3], [2, 4], [4, 6], {})
    decoded_a = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    decoded_b = _decode_mvt_from_archive(blob, 3, 4, 6, tmp_path)
    assert {f["properties"]["id"] for f in decoded_a["bldg"]["features"]} == {1}
    assert {f["properties"]["id"] for f in decoded_b["bldg"]["features"]} == {2}


def test_raster_first_wins_unchanged(tmp_path):
    """PNG tiles for the same (z,x,y) still keep first-wins (no change to raster path)."""
    _PNG2 = b"\x89PNG\r\n\x1a\n" + b"\x01" * 16
    blob = _assemble_archive([_PNG, _PNG2], [1, 1], [0, 0], [0, 0], {})
    tiles = _decode(blob, tmp_path)
    assert (
        tiles[(1, 0, 0)] == _PNG
    ), "raster first-wins violated after vector-merge change"
