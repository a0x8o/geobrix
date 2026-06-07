"""Focal-op semantics tests: pyrx focal.filt / focal.convolve must match the
heavyweight rasterx GDALBlock contract -- NoData-aware skip + edge window-shrink
(filter) / zero-pad (convolve), kernel applied UN-flipped (correlation).

These complement the smoke-level focal goldens in test_core_bandmath.py.
"""

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from scipy import ndimage

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import focal


def _tile(data, nodata=-9999.0):
    """Single-band float32 GTiff bytes from a 2-D array (nodata=None => undeclared)."""
    data = np.asarray(data, dtype="float32")
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
    )
    if nodata is not None:
        profile["nodata"] = nodata
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def _open(raster_bytes):
    """Materialize tile bytes into an open dataset whose lifetime we control."""
    mf = MemoryFile(raster_bytes)
    return mf.open()


def _read_band(raster_bytes):
    with _serde.open_tile(raster_bytes) as o:
        return o.read(1).astype("float64")


def _read_nodata(raster_bytes):
    with _serde.open_tile(raster_bytes) as o:
        return o.nodata


# focal.filt / focal.convolve accept an OPEN dataset (see functions.py bindings),
# so wrap _tile bytes in an open dataset before passing through.
def _run(fn, raster_bytes, *args):
    with _serde.open_tile(raster_bytes) as ds:
        return fn(ds, *args)


def test_convolve_edge_is_replicate_nearest():
    # convolve uses mode="nearest" (replicate edge) -- empirically the closest
    # scipy boundary to the heavyweight's GDAL block-halo convolution.
    data = np.arange(16, dtype="float64").reshape(4, 4)
    k = np.ones((3, 3), dtype="float64")
    expected = ndimage.correlate(data, k, mode="nearest")
    out = _read_band(_run(focal.convolve, _tile(data, nodata=None), k.tolist()))
    assert np.allclose(out, expected)
    # and NOT zero-padded (the regressed behavior we reverted)
    assert not np.allclose(out, ndimage.correlate(data, k, mode="constant", cval=0.0))


def test_convolve_asymmetric_kernel_is_unflipped_correlation():
    data = np.arange(25, dtype="float64").reshape(5, 5)
    k = np.array([[0, 0, 0], [0, 0, 1], [0, 0, 0]], dtype="float64")
    out = _read_band(_run(focal.convolve, _tile(data, nodata=None), k.tolist()))
    assert np.allclose(out, ndimage.correlate(data, k, mode="nearest"))
    assert not np.allclose(out, ndimage.convolve(data, k, mode="nearest"))


def test_filter_mean_shrinks_window_at_edge():
    data = np.arange(16, dtype="float64").reshape(4, 4)
    box = np.ones((3, 3))
    num = ndimage.correlate(data, box, mode="constant", cval=0.0)
    cnt = ndimage.correlate(np.ones_like(data), box, mode="constant", cval=0.0)
    expected = (num / cnt).astype("float32")
    out = _read_band(_run(focal.filt, _tile(data, nodata=None), 3, "mean"))
    assert np.allclose(out, expected, atol=1e-5)


def test_filter_mean_skips_nodata_neighbor():
    data = np.array([[1.0, 1.0, 1.0], [1.0, 5.0, 1.0], [1.0, 1.0, 1.0]])
    out = _read_band(_run(focal.filt, _tile(data, nodata=5.0), 3, "mean"))
    # nodata center excluded -> mean of valid {1,1,1} = 1.0
    assert abs(out[0, 0] - 1.0) < 1e-6


def test_all_nodata_window_yields_nodata():
    data = np.full((3, 3), 9.0)
    res = _run(focal.filt, _tile(data, nodata=9.0), 3, "median")
    band = _read_band(res)
    nd = _read_nodata(res)
    # every pixel's window all-invalid -> nodata
    assert np.all(band == nd)


def test_filter_min_shrinks_window_at_edge():
    # min over VALID in-bounds neighbors only (window shrinks at the border):
    # matches the NaN-aware shrink (cval=nan excludes out-of-bounds).
    data = np.arange(16, dtype="float64").reshape(4, 4)
    expected = ndimage.generic_filter(
        data, np.nanmin, size=3, mode="constant", cval=np.nan
    )
    out = _read_band(_run(focal.filt, _tile(data, nodata=None), 3, "min"))
    assert np.allclose(out, expected)


def test_filter_max_shrinks_window_at_edge():
    data = np.arange(16, dtype="float64").reshape(4, 4)
    expected = ndimage.generic_filter(
        data, np.nanmax, size=3, mode="constant", cval=np.nan
    )
    out = _read_band(_run(focal.filt, _tile(data, nodata=None), 3, "max"))
    assert np.allclose(out, expected)


def test_filter_min_max_skip_nodata_neighbor():
    # the NoData center must be excluded from a corner's min/max window
    data = np.array([[2.0, 2.0, 2.0], [2.0, -9999.0, 2.0], [2.0, 8.0, 2.0]])
    mn = _read_band(_run(focal.filt, _tile(data, nodata=-9999.0), 3, "min"))
    mx = _read_band(_run(focal.filt, _tile(data, nodata=-9999.0), 3, "max"))
    # corner [0,0] sees valid {2,2,2} (center -9999 skipped) -> min=max=2.0
    assert abs(mn[0, 0] - 2.0) < 1e-6
    assert abs(mx[0, 0] - 2.0) < 1e-6
