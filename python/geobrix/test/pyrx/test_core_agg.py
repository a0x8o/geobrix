"""Pure-function tests for core/agg.py reducers (Spark-free)."""

import numpy as np
import pytest
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import agg


def _ras(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    """GTiff bytes from a 2-D or 3-D numpy array with a known georef."""
    data = np.asarray(data, dtype="float32")
    if data.ndim == 2:
        data = data[None, :, :]
    bands, h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=bands,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


# --- merge_tiles ------------------------------------------------------------
def test_merge_tiles_union_extent():
    # Two adjacent 2x2 tiles side by side -> 2x4 mosaic spanning the union.
    left = _ras(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0, px=1.0)
    right = _ras(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0, px=1.0)
    out = agg.merge_tiles([left, right])
    with _serde.open_tile(out) as ds:
        assert ds.width == 4
        assert ds.height == 2
        b = ds.bounds
        assert b.left == pytest.approx(0.0)
        assert b.right == pytest.approx(4.0)


def test_merge_tiles_single_passthrough():
    one = _ras(np.array([[1, 2], [3, 4]]))
    assert agg.merge_tiles([one]) == one


def test_merge_tiles_overlap_last_wins():
    # Two 4x4 tiles overlapping in x=[2,4]: union mosaic is 4x6, overlap = cols
    # 2 and 3. Heavyweight MergeRasters builds a GDAL VRT (gdalbuildvrt), where
    # overlapping pixels take the LAST listed source. The order passed here is
    # [left, right], so the overlap must take the RIGHT tile's value (20), not
    # the left's (10) -- pre-fix rasterio defaults to first-wins and returns 10.
    left = _ras(np.full((4, 4), 10.0), ulx=0.0, uly=4.0, px=1.0)
    right = _ras(np.full((4, 4), 20.0), ulx=2.0, uly=4.0, px=1.0)
    out = agg.merge_tiles([left, right])
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
        assert arr.shape == (4, 6)
        # Non-overlap left cols (0,1) -> 10 ; overlap cols (2,3) -> 20 (last wins)
        assert np.all(arr[:, 0:2] == 10.0)
        assert np.all(arr[:, 2:4] == 20.0)
        # Non-overlap right cols (4,5) -> 20
        assert np.all(arr[:, 4:6] == 20.0)


# --- combineavg_tiles -------------------------------------------------------
def test_combineavg_tiles_mean():
    a = _ras(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _ras(np.array([[4.0, 8.0], [10.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        assert np.allclose(ds.read(1), [[3.0, 6.0], [8.0, 10.0]])


def test_combineavg_tiles_ignores_nodata():
    # Where one input is NoData, the mean is taken over the valid input only.
    a = _ras(np.array([[2.0, -9999.0], [6.0, 8.0]]))
    b = _ras(np.array([[4.0, 10.0], [-9999.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    # (2+4)/2=3 ; only-b=10 ; only-a=6 ; (8+12)/2=10
    assert np.allclose(got, [[3.0, 10.0], [6.0, 10.0]])


def test_combineavg_tiles_all_nodata_pixel_gets_fallback():
    a = _ras(np.array([[-9999.0, 4.0], [6.0, 8.0]]))
    b = _ras(np.array([[-9999.0, 8.0], [10.0, 12.0]]))
    out = agg.combineavg_tiles([a, b])
    with _serde.open_tile(out) as ds:
        got = ds.read(1)
    assert got[0, 0] == pytest.approx(-9999.0)


def test_combineavg_tiles_shape_mismatch_raises():
    a = _ras(np.array([[1.0, 2.0], [3.0, 4.0]]))
    b = _ras(np.array([[1.0, 2.0, 3.0]]))
    with pytest.raises(ValueError, match="aligned tiles"):
        agg.combineavg_tiles([a, b])


# --- frombands_tiles --------------------------------------------------------
def test_frombands_tiles_ascending_order():
    # Provide out of order: index 2 then index 0 then index 1.
    b0 = _ras(np.full((2, 2), 10.0))
    b1 = _ras(np.full((2, 2), 20.0))
    b2 = _ras(np.full((2, 2), 30.0))
    out = agg.frombands_tiles([(2, b2), (0, b0), (1, b1)])
    with _serde.open_tile(out) as ds:
        assert ds.count == 3
        assert np.allclose(ds.read(1), 10.0)
        assert np.allclose(ds.read(2), 20.0)
        assert np.allclose(ds.read(3), 30.0)


# --- rasterize_features -----------------------------------------------------
def test_rasterize_features_burns_values():
    # Extent 0..4 x 0..4, 4x4 px (1 unit/px). Two boxes, second overlaps first.
    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))  # left half -> value 1
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))  # overlaps col 1 -> value 2 (last wins)
    out = agg.rasterize_features([(g1, 1.0), (g2, 2.0)], 0, 0, 4, 4, 4, 4, 32633)
    with _serde.open_tile(out) as ds:
        arr = ds.read(1)
    # Column 0 only g1 -> 1 ; columns 1..3 -> g2 last-wins -> 2.
    assert np.all(arr[:, 0] == 1.0)
    assert np.all(arr[:, 1] == 2.0)


def test_rasterize_features_empty_returns_none():
    assert agg.rasterize_features([], 0, 0, 4, 4, 4, 4, 32633) is None


# --- derivedband_tiles ------------------------------------------------------
PYFUNC_SUM = """
def addbands(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = np.sum(in_ar, axis=0)
"""


def test_derivedband_tiles_sum_across_group():
    a = _ras(np.full((2, 2), 3.0))
    b = _ras(np.full((2, 2), 4.0))
    c = _ras(np.full((2, 2), 5.0))
    out = agg.derivedband_tiles([a, b, c], PYFUNC_SUM, "addbands")
    with _serde.open_tile(out) as ds:
        assert ds.count == 1
        assert np.allclose(ds.read(1), 12.0)
