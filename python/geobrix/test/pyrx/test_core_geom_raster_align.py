"""geom x raster alignment + non-overlap robustness (light tier).

A class of bugs where a function combines a GEOMETRY with a RASTER tile but
(a) does not reproject the geom from its SRID to the raster CRS, or
(b) hard-crashes when the geom does not overlap the raster.

These tests pin BOTH guarantees for the affected functions:
  * clip  -> non-overlap cutline returns None (no crash); CRS reproj already
             covered in test_core_clip_reproject.py.
  * sample -> reprojects a 4326 point onto a UTM raster; out-of-extent -> None.
  * viewshed -> reprojects a 4326 observer; out-of-extent observer -> all-0,
                no crash.

The by-design constructors (rasterize / gridfrompoints / dtmfromgeoms) take a
target SRID and assume the geom is already in it (matching heavy, which does NOT
reproject those). We still assert they degrade GRACEFULLY (empty / NoData tile)
when the geom falls outside the target extent rather than crashing.
"""

import numpy as np
import shapely
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely import set_srid
from shapely.geometry import Point, box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import analysis, edit, features
from databricks.labs.gbx.pyrx.core import ops as ops_core
from databricks.labs.gbx.pyrx.core import tin as tin_core


def _utm_geotiff_bytes(width=8, height=8, count=1):
    """In-memory GTiff in EPSG:32633 (UTM 33N). Origin (500000, 5000000), 100m px."""
    transform = from_origin(500000.0, 5000000.0, 100.0, 100.0)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs="EPSG:32633",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(count * width * height, dtype="float32").reshape(
        count, height, width
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


def _utm_to_lonlat(x, y):
    from rasterio.warp import transform as _transform

    xs, ys = _transform("EPSG:32633", "EPSG:4326", [x], [y])
    return xs[0], ys[0]


# --- clip: graceful non-overlap --------------------------------------------
def test_clip_non_overlapping_cutline_returns_none():
    """A cutline far outside the raster (same CRS) returns None, not a crash."""
    far = box(0.0, 0.0, 10.0, 10.0)  # UTM coords nowhere near (500000, 5000000)
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, far, all_touched=False)
    assert out is None


def test_clip_non_overlapping_4326_cutline_returns_none():
    """A reprojected 4326 cutline that lands off-raster returns None gracefully."""
    cutline = set_srid(box(-50.0, -40.0, -49.0, -39.0), 4326)  # opposite hemisphere
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, cutline, all_touched=True)
    assert out is None


# --- sample: reproject + graceful out-of-bounds ----------------------------
def test_sample_reprojects_4326_point_over_utm_raster():
    """A SRID=4326 point over a UTM raster samples the right pixel after reproj."""
    # Center of pixel (col=0, row=0): world (500050, 4999950) in UTM.
    lon, lat = _utm_to_lonlat(500050.0, 4999950.0)
    pt = set_srid(Point(lon, lat), 4326)
    with _serde.open_tile(_utm_geotiff_bytes(width=8, height=8)) as ds:
        vals = ops_core.sample(ds, pt)
    # band1 value at (row=0, col=0) = arange index 0 = 0.0
    assert vals == [0.0]


def test_sample_point_outside_extent_returns_none():
    """A point far outside the raster extent returns None (not the NoData fill)."""
    pt = Point(0.0, 0.0)  # UTM coords nowhere near the raster
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = ops_core.sample(ds, pt)
    assert out is None


def test_sample_4326_point_outside_extent_returns_none():
    """A 4326 point that reprojects off-raster returns None gracefully."""
    pt = set_srid(Point(-50.0, -40.0), 4326)
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = ops_core.sample(ds, pt)
    assert out is None


def test_sample_bare_wkb_no_srid_still_works():
    """Back-compat: raw WKB (SRID 0) sampled as-is in raster CRS."""
    # Pixel (col=1, row=0) center world = (500150, 4999950); band1 index 1 = 1.0
    pt_wkb = shapely.wkb.dumps(Point(500150.0, 4999950.0))
    with _serde.open_tile(_utm_geotiff_bytes(width=8, height=8)) as ds:
        vals = ops_core.sample(ds, pt_wkb)
    assert vals == [1.0]


# --- viewshed: reproject + graceful out-of-bounds --------------------------
def test_viewshed_observer_outside_extent_returns_all_invisible():
    """An observer far outside the DEM returns an all-0 tile, not a crash."""
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = analysis.viewshed(ds, 0.0, 0.0, 1.0, 0.0, None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert arr.dtype == np.uint8
        assert int(arr.max()) == 0  # nothing visible from off-raster observer


def test_viewshed_in_bounds_observer_still_produces_visibility():
    """A regression guard: an in-extent observer still yields some visible cells."""
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        # observer near the raster center, in UTM CRS
        out = analysis.viewshed(ds, 500400.0, 4999600.0, 5.0, 0.0, None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert int(arr.max()) == 255  # at least the observer cell is visible


# --- by-design constructors: graceful out-of-extent ------------------------
def test_rasterize_geom_outside_extent_is_all_nodata():
    """rasterize a geom that misses the target extent -> all-NoData tile, no crash."""
    geom_wkb = shapely.wkb.dumps(box(1000.0, 1000.0, 1010.0, 1010.0))
    out = features.rasterize_geom(geom_wkb, 42.0, 0.0, 0.0, 100.0, 100.0, 8, 8, 32633)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert np.all(arr == -9999.0)  # geom never overlaps -> all NoData


def test_gridfrompoints_points_outside_extent_no_crash():
    """IDW grid still produces a tile when all points are far outside the extent."""
    pts = [(5000.0, 5000.0), (5010.0, 5010.0)]
    out = tin_core.idw_grid(pts, [1.0, 2.0], 0.0, 0.0, 100.0, 100.0, 8, 8, 32633)
    with _serde.open_tile(out) as o:
        assert o.width == 8 and o.height == 8  # well-formed tile, no crash


def test_dtmfromgeoms_points_outside_extent_is_all_nodata():
    """DTM cells outside the points' convex hull -> NoData; off-extent -> all NoData."""
    pts_xyz = np.array(
        [[5000.0, 5000.0, 1.0], [5100.0, 5000.0, 2.0], [5050.0, 5100.0, 3.0]]
    )
    out = tin_core.delaunay_dtm(pts_xyz, None, 0.0, 0.0, 100.0, 100.0, 8, 8, 32633)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert np.all(arr == -9999.0)  # whole grid is outside the hull
