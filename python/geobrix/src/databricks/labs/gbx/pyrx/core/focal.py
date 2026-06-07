"""Spark-free focal ops via scipy.ndimage, matching rasterx GDALBlock semantics:
NoData-aware skip + edge window-shrink (filter) / replicate-edge (convolve).

GDALBlock.valuesAt (avg/min/max/median): a neighbor contributes only if it is
in-bounds AND in-mask AND not-NoData; out-of-bounds neighbors are skipped so the
window shrinks at edges; if no neighbor is valid the output pixel is NoData.
GDALBlock.convolveAt: ``sum += value * kernel(i)(j)`` over in-bounds valid
neighbors only (others contribute 0, no renormalization), kernel applied
UN-flipped (correlation).
"""

import numpy as np
from rasterio.io import MemoryFile
from scipy import ndimage

from databricks.labs.gbx.pyrx.core._nodata import read_masked

_NAN_AGG = {"min": np.nanmin, "max": np.nanmax, "median": np.nanmedian}


def _nodata_value(ds, default: float = -9999.0) -> float:
    return ds.nodata if ds.nodata is not None else default


def _out_profile(ds, dtype: str, nd: float, set_nodata: bool) -> dict:
    """Source profile minus block/tiling options that break small outputs."""
    profile = ds.profile.copy()
    for key in ("blockxsize", "blockysize", "tiled", "interleave"):
        profile.pop(key, None)
    profile.update(driver="GTiff", dtype=dtype)
    if set_nodata:
        profile.update(nodata=nd)
    return profile


def filt(ds, kernel_size, operation: str) -> bytes:
    """Apply a focal min/max/mean/median filter per band, GDALBlock semantics.

    Output dtype: ``mean`` -> Float32; ``min``/``max``/``median`` -> input dtype.
    All-invalid windows become NoData.
    """
    size = int(kernel_size)
    op = str(operation)
    box = np.ones((size, size), dtype="float64")
    nd = _nodata_value(ds, default=-9999.0)
    out_bands, any_invalid = [], False
    for i in range(1, ds.count + 1):
        data, valid = read_masked(ds, i)
        if op == "mean":
            num = ndimage.correlate(data * valid, box, mode="constant", cval=0.0)
            cnt = ndimage.correlate(
                valid.astype("float64"), box, mode="constant", cval=0.0
            )
            res = np.where(cnt > 0, num / np.where(cnt > 0, cnt, 1.0), nd)
            invalid = cnt == 0
        else:
            arr = np.where(valid, data, np.nan)
            res = ndimage.generic_filter(
                arr, _NAN_AGG[op], size=size, mode="constant", cval=np.nan
            )
            invalid = ~np.isfinite(res)
            res = np.where(invalid, nd, res)
        any_invalid = any_invalid or bool(invalid.any())
        out_bands.append(res)
    out_dtype = "float32" if op == "mean" else ds.dtypes[0]
    out = np.stack(out_bands)
    profile = _out_profile(ds, out_dtype, nd, any_invalid or ds.nodata is not None)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out.astype(out_dtype))
        return mf.read()


def convolve(ds, kernel) -> bytes:
    """2-D correlation (UN-flipped kernel) per band, replicate-edge boundary.

    Mirrors GDALBlock.convolveAt: ``sum`` starts at 0 and accumulates
    ``value * kernel(i)(j)`` over valid neighbors; invalid / NoData neighbors
    contribute 0 (via ``data * valid``) with no weight renormalization, and the
    returned sum is emitted verbatim (convolveAt never yields NoData). Output
    dtype is Float64.

    The heavyweight applies a GDAL block-halo convolution whose edge behavior
    matches no single scipy boundary mode exactly; empirically ``mode="nearest"``
    (replicate edge) minimizes the residual divergence vs the heavyweight, so it
    is used here. A small edge residual remains (the accepted GDAL-halo gap).
    """
    k = np.asarray(kernel, dtype="float64")
    nd = _nodata_value(ds, default=-9999.0)
    out_bands = []
    for i in range(1, ds.count + 1):
        data, valid = read_masked(ds, i)
        # data * valid zeroes out masked / NoData neighbors so they add 0,
        # exactly matching the heavyweight's skip-and-keep-sum behavior.
        res = ndimage.correlate(data * valid, k, mode="nearest")
        out_bands.append(res)
    out = np.stack(out_bands)
    profile = _out_profile(ds, "float64", nd, ds.nodata is not None)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out)
        return mf.read()
