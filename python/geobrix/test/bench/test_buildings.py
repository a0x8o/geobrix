"""Unit tests for the notional-building generator (bench/buildings.py)."""

from shapely import from_wkb

from databricks.labs.gbx.bench import buildings as B


def test_all_shapes_produce_valid_polygons():
    rows = B.building_wkb_rows(
        len(B.SHAPES) * 3, is_degrees=True, cx0=-74.0, cy0=40.7, seed=1
    )
    for wkb, shape, size in rows:
        g = from_wkb(wkb)
        assert g.is_valid, f"{shape}/{size} invalid"
        assert g.area > 0
        assert shape in B.SHAPES and size in B.SIZES


def test_courtyard_has_a_hole():
    # courtyard is the 4th shape; force it by requesting only courtyard.
    rows = B.building_wkb_rows(
        1, is_degrees=True, cx0=-74.0, cy0=40.7, shapes=("courtyard",), sizes=("medium",)
    )
    g = from_wkb(rows[0][0])
    assert len(g.interiors) == 1, "courtyard must have exactly one interior ring (hole)"


def test_non_courtyard_shapes_have_no_hole():
    for sh in ("rect", "lshape", "ushape", "irregular"):
        g = from_wkb(
            B.building_wkb_rows(1, is_degrees=True, cx0=0.0, cy0=0.0, shapes=(sh,))[0][0]
        )
        assert len(g.interiors) == 0, f"{sh} should have no hole"


def test_deterministic_for_fixed_seed():
    a = B.building_wkb_rows(50, is_degrees=True, cx0=-74.0, cy0=40.7, seed=7)
    b = B.building_wkb_rows(50, is_degrees=True, cx0=-74.0, cy0=40.7, seed=7)
    assert [r[0] for r in a] == [r[0] for r in b]


def test_count_and_scatter():
    rows = B.building_wkb_rows(1000, is_degrees=True, cx0=-74.0, cy0=40.7, seed=3)
    assert len(rows) == 1000
    centroids = {tuple(round(c, 6) for c in from_wkb(w).centroid.coords[0]) for w, _, _ in rows}
    # Scatter should give many distinct locations (not all stacked on one point).
    assert len(centroids) > 900


def test_projected_crs_scale_is_metres():
    # is_degrees=False → metre CRS: a 50 m "medium" rect spans ~50 m in X.
    g = from_wkb(
        B.building_wkb_rows(
            1, is_degrees=False, cx0=530000.0, cy0=180000.0, shapes=("rect",), sizes=("medium",)
        )[0][0]
    )
    minx, miny, maxx, maxy = g.bounds
    assert 45.0 < (maxx - minx) < 55.0, f"expected ~50 m width, got {maxx - minx}"


def test_wgs84_scale_is_degrees():
    # is_degrees=True → a 50 m rect spans ~50/111320 deg ~= 4.5e-4 deg.
    g = from_wkb(
        B.building_wkb_rows(
            1, is_degrees=True, cx0=-74.0, cy0=40.7, shapes=("rect",), sizes=("medium",)
        )[0][0]
    )
    minx, _, maxx, _ = g.bounds
    assert 3e-4 < (maxx - minx) < 6e-4, f"expected ~4.5e-4 deg, got {maxx - minx}"
