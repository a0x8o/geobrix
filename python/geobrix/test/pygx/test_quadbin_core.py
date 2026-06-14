import pytest

pytest.importorskip("quadbin")
import quadbin  # noqa: E402

from databricks.labs.gbx.pygx import _quadbin


def test_pointascell_matches_lib():
    cell = _quadbin.point_as_cell(-122.4194, 37.7749, 10)
    assert cell == quadbin.point_to_cell(-122.4194, 37.7749, 10)
    assert _quadbin.resolution(cell) == 10


def test_resolution_bitformula():
    cell = quadbin.point_to_cell(0.0, 0.0, 14)
    assert _quadbin.resolution(cell) == ((cell >> 52) & 0x1F)


def test_kring_matches_lib_and_includes_center():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    ring = _quadbin.k_ring(cell, 1)
    assert cell in ring and len(ring) == 9
    assert sorted(ring) == sorted(quadbin.k_ring(cell, 1))


def test_distance_same_resolution_chebyshev():
    a = quadbin.point_to_cell(0.0, 0.0, 10)
    b = quadbin.point_to_cell(0.5, 0.5, 10)
    ta, tb = quadbin.cell_to_tile(a), quadbin.cell_to_tile(b)
    expected = max(abs(ta[0] - tb[0]), abs(ta[1] - tb[1]))
    assert _quadbin.distance(a, b) == expected


def test_distance_mismatched_resolution_raises():
    a = quadbin.point_to_cell(0.0, 0.0, 10)
    b = quadbin.point_to_cell(0.0, 0.0, 11)
    with pytest.raises(ValueError, match="same resolution"):
        _quadbin.distance(a, b)


def test_pointascell_resolution_validation():
    with pytest.raises(ValueError):
        _quadbin.point_as_cell(0.0, 0.0, 27)


from shapely import from_wkb, get_srid  # noqa: E402


def test_aswkb_is_ewkb_polygon_srid4326():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    g = from_wkb(_quadbin.as_wkb(cell))
    assert g.geom_type == "Polygon" and get_srid(g) == 4326
    w, s, e, n = quadbin.cell_to_bounding_box(cell)
    assert abs(g.bounds[0] - w) < 1e-9 and abs(g.bounds[2] - e) < 1e-9


def test_centroid_is_ewkb_point_srid4326():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    g = from_wkb(_quadbin.centroid(cell))
    assert g.geom_type == "Point" and get_srid(g) == 4326


def test_cellunion_is_ewkb_and_covers_cells():
    cells = list(quadbin.k_ring(quadbin.point_to_cell(0.0, 0.0, 8), 1))
    g = from_wkb(_quadbin.cell_union(cells))
    assert g.geom_type in ("Polygon", "MultiPolygon") and get_srid(g) == 4326


def test_cellunion_empty_or_none_is_none():
    assert _quadbin.cell_union([]) is None
    assert _quadbin.cell_union(None) is None
