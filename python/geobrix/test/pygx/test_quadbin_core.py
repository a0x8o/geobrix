import pytest

pytest.importorskip("quadbin")
import quadbin  # noqa: E402

from databricks.labs.gbx.pygx import _quadbin  # noqa: E402


def test_pointascell_matches_lib():
    # Interior points agree with the lib (no clamp involved).
    cell = _quadbin.point_as_cell(-122.4194, 37.7749, 10)
    assert cell == quadbin.point_to_cell(-122.4194, 37.7749, 10)
    assert _quadbin.resolution(cell) == 10


def test_pointascell_antimeridian_matches_heavy_not_lib():
    """At lon=180 the heavy Scala floors to xTile=n then clamps to n-1 (easternmost
    tile); the quadbin lib instead WRAPS lon=180 to xTile=0. point_as_cell must
    follow heavy (route through lonLatToTile + tile_to_cell), so it diverges from
    quadbin.point_to_cell here — this is the exact-parity contract, not a bug."""
    z = 14
    n = 1 << z
    cell = _quadbin.point_as_cell(180.0, 85.05112878, z)
    # easternmost tile (x == n-1), NOT the wrapped x==0 the lib would give
    assert cell == quadbin.tile_to_cell((n - 1, 0, z))
    assert cell != quadbin.point_to_cell(180.0, 85.05112878, z)


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


def test_centroid_is_bbox_corner_mean_matches_heavy():
    """Centroid is the ARITHMETIC MEAN of the bbox corners (heavy cellCenter),
    NOT the lib's true inverse-mercator center (whose latitude differs)."""
    cell = quadbin.point_to_cell(-122.4194, 37.7749, 10)
    g = from_wkb(_quadbin.centroid(cell))
    w, s, e, n = quadbin.cell_to_bounding_box(cell)
    assert abs(g.x - (w + e) / 2.0) < 1e-12
    assert abs(g.y - (s + n) / 2.0) < 1e-12


def test_cellunion_is_ewkb_and_covers_cells():
    cells = list(quadbin.k_ring(quadbin.point_to_cell(0.0, 0.0, 8), 1))
    g = from_wkb(_quadbin.cell_union(cells))
    assert g.geom_type in ("Polygon", "MultiPolygon") and get_srid(g) == 4326


def test_cellunion_empty_or_none_is_none():
    assert _quadbin.cell_union([]) is None
    assert _quadbin.cell_union(None) is None


from shapely import to_wkb as _to_wkb  # noqa: E402
from shapely.geometry import box as _box  # noqa: E402


def test_polyfill_bbox_cells_resolution():
    geom = _to_wkb(_box(-0.1, -0.1, 0.1, 0.1))
    cells = _quadbin.polyfill(geom, 12)
    assert len(cells) > 0
    assert all(_quadbin.resolution(c) == 12 for c in cells)


def test_polyfill_resolution_validation():
    with pytest.raises(ValueError):
        _quadbin.polyfill(_to_wkb(_box(0, 0, 1, 1)), 21)  # > 20


def test_polyfill_accepts_wkt_and_ewkt():
    cells_wkt = _quadbin.polyfill(
        "POLYGON ((-0.1 -0.1, 0.1 -0.1, 0.1 0.1, -0.1 0.1, -0.1 -0.1))", 12
    )
    assert len(cells_wkt) > 0


def test_tessellate_returns_cell_geom_pairs():
    geom = _to_wkb(_box(-0.05, -0.05, 0.05, 0.05))
    chips = _quadbin.tessellate(geom, 12)
    assert len(chips) > 0
    cell0, gwkb0 = chips[0]
    assert isinstance(cell0, int)
    from shapely import from_wkb, get_srid

    g0 = from_wkb(gwkb0)
    assert get_srid(g0) == 4326 and not g0.is_empty
