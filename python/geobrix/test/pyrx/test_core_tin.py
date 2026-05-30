"""Pure-function tests for the pyrx TIN / IDW core (Spark-free)."""

import numpy as np
import shapely.wkb
from shapely.geometry import LineString, Point

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tin


# --- idw_grid ---------------------------------------------------------------
def test_idw_grid_output_is_float64_with_nodata():
    pts = np.array([[1.0, 1.0], [3.0, 3.0]])
    vals = np.array([10.0, 20.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 4.0, 4.0, 4, 4, 32633)
    with _serde.open_tile(out) as ds:
        assert ds.count == 1
        assert ds.dtypes[0] == "float64"
        assert ds.nodata == -9999.0
        assert ds.crs.to_epsg() == 32633
        assert (ds.width, ds.height) == (4, 4)


def test_idw_grid_interior_cell_matches_hand_computed_weighted_value():
    # Two points; a 1x1 grid over a 2x2 extent => single cell center at (1, 1).
    # p0 = (0,0) v=10 ; p1 = (2,2) v=30.
    # d0 = sqrt(2), d1 = sqrt(2). power=2 => w0 = w1 = 1/2.
    # weighted mean = (10*0.5 + 30*0.5) / (0.5 + 0.5) = 20.0
    pts = np.array([[0.0, 0.0], [2.0, 2.0]])
    vals = np.array([10.0, 30.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, power=2.0, max_pts=2)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    assert np.isclose(arr[0, 0], 20.0)


def test_idw_grid_asymmetric_weights_hand_computed():
    # Single cell center at (1,1). p0=(0,1) v=0 (d=1); p1=(3,1) v=100 (d=2).
    # power=2 => w0=1/1=1, w1=1/4=0.25.
    # value = (0*1 + 100*0.25) / (1 + 0.25) = 25 / 1.25 = 20.0
    pts = np.array([[0.0, 1.0], [3.0, 1.0]])
    vals = np.array([0.0, 100.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, power=2.0, max_pts=2)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    assert np.isclose(arr[0, 0], 20.0)


def test_idw_grid_coincident_point_returns_that_value():
    # Cell center sits exactly on a point => returns that point's value (d==0).
    # 1x1 grid over [0,2]x[0,2] => center (1,1). Put a point there.
    pts = np.array([[1.0, 1.0], [0.0, 0.0]])
    vals = np.array([42.0, 7.0])
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 2.0, 2.0, 1, 1, 32633, power=2.0, max_pts=2)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    assert np.isclose(arr[0, 0], 42.0)


def test_idw_grid_empty_points_all_nodata():
    out = tin.idw_grid(
        np.empty((0, 2)), np.empty((0,)), 0.0, 0.0, 4.0, 4.0, 4, 4, 32633
    )
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
        assert ds.nodata == -9999.0
        assert np.all(arr == -9999.0)


def test_idw_grid_defaults_power_2_max_12():
    # Smoke: default args (power=2.0, max_pts=12) run and produce finite interior.
    rng = np.random.default_rng(0)
    pts = rng.uniform(0, 10, size=(20, 2))
    vals = rng.uniform(0, 100, size=20)
    out = tin.idw_grid(pts, vals, 0.0, 0.0, 10.0, 10.0, 8, 8, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # all interior cells of the convex hull get an IDW value; many are valid.
    assert np.any(arr != -9999.0)


# --- delaunay_dtm -----------------------------------------------------------
def _plane(x, y):
    # z = 2x + 3y + 1
    return 2.0 * x + 3.0 * y + 1.0


def test_delaunay_dtm_planar_surface_interpolates_exactly_within_hull():
    # Points on a plane z = 2x + 3y + 1; barycentric interpolation of a planar
    # field is exact, so interior cells should match the plane.
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    with _serde.open_tile(out) as ds:
        assert ds.dtypes[0] == "float64"
        assert ds.nodata == -9999.0
        arr = ds.read(1)
        transform = ds.transform
    # Check a handful of interior cells against the analytic plane.
    for row, col in [(3, 3), (5, 5), (7, 2), (2, 7)]:
        wx, wy = transform * (col + 0.5, row + 0.5)
        if arr[row, col] != -9999.0:
            assert np.isclose(arr[row, col], _plane(wx, wy), atol=1e-6)
    # at least some interior cells were interpolated
    assert np.any(arr != -9999.0)


def test_delaunay_dtm_outside_hull_is_nodata():
    # Points clustered in the lower-left; cells in the far upper-right corner of
    # a larger extent fall outside the convex hull -> NoData.
    corners = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    # extent extends to 10x10 but points only span [0,2] => upper-right is outside.
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # top-right cell (row 0, col 9) center ~ (9.5, 9.5) is far outside the hull.
    assert arr[0, 9] == -9999.0


def test_delaunay_dtm_custom_nodata():
    corners = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    out = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633, no_data=-1.0)
    with _serde.open_tile(out) as ds:
        assert ds.nodata == -1.0
        arr = ds.read(1)
    assert arr[0, 9] == -1.0


def test_delaunay_dtm_breaklines_accepted_not_enforced():
    # PARITY: breaklines are accepted but NOT enforced as constraint edges.
    # With a planar field, folding breakline vertices in must not change the
    # interpolated (planar) result vs. running without breaklines.
    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    xyz = np.array([(x, y, _plane(x, y)) for x, y in corners])
    # Breakline along the diagonal with Z on the same plane.
    bl = LineString([(2.0, 2.0, _plane(2.0, 2.0)), (8.0, 8.0, _plane(8.0, 8.0))])
    bl_wkb = shapely.wkb.dumps(bl, output_dimension=3)
    out_with = tin.delaunay_dtm(xyz, [bl_wkb], 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    out_without = tin.delaunay_dtm(xyz, None, 0.0, 0.0, 10.0, 10.0, 10, 10, 32633)
    with _serde.open_tile(out_with) as ds_w, _serde.open_tile(out_without) as ds_n:
        aw = ds_w.read(1)
        an = ds_n.read(1)
    # Both are valid DTMs; on a planar field the interior values are identical.
    valid = (aw != -9999.0) & (an != -9999.0)
    assert np.any(valid)
    assert np.allclose(aw[valid], an[valid], atol=1e-6)


# --- WKB decode helpers -----------------------------------------------------
def test_points_xy_from_wkb_roundtrip():
    wkbs = [shapely.wkb.dumps(Point(1.0, 2.0)), shapely.wkb.dumps(Point(3.0, 4.0))]
    xy = tin.points_xy_from_wkb(wkbs)
    assert xy.shape == (2, 2)
    assert np.allclose(xy, [[1.0, 2.0], [3.0, 4.0]])


def test_points_xyz_from_wkb_requires_z():
    wkbs = [shapely.wkb.dumps(Point(1.0, 2.0, 9.0), output_dimension=3)]
    xyz = tin.points_xyz_from_wkb(wkbs)
    assert xyz.shape == (1, 3)
    assert np.allclose(xyz[0], [1.0, 2.0, 9.0])


def test_points_xyz_from_wkb_rejects_2d():
    import pytest

    wkbs = [shapely.wkb.dumps(Point(1.0, 2.0))]
    with pytest.raises(ValueError, match="Z coordinate"):
        tin.points_xyz_from_wkb(wkbs)
