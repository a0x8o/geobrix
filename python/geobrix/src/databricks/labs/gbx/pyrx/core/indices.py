"""Spark-free spectral indices (NumPy band math). Each returns a single-band
Float32 GTiff (NoData -9999.0); invalid/divide-by-zero results become NoData."""

import numpy as np
from rasterio.io import MemoryFile

_NODATA = -9999.0


def _band(ds, idx) -> np.ndarray:
    return ds.read(int(idx)).astype("float64")


def _emit(ds, arr: np.ndarray) -> bytes:
    out = np.where(np.isfinite(arr), arr, _NODATA).astype("float32")
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype="float32", nodata=_NODATA)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out, 1)
        return mf.read()


def _normalized_diff(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        return (a - b) / (a + b)


def ndvi(ds, red_band, nir_band) -> bytes:
    r, n = _band(ds, red_band), _band(ds, nir_band)
    return _emit(ds, _normalized_diff(n, r))


def ndwi(ds, green_idx, nir_idx) -> bytes:
    g, n = _band(ds, green_idx), _band(ds, nir_idx)
    return _emit(ds, _normalized_diff(g, n))


def nbr(ds, nir_idx, swir_idx) -> bytes:
    n, s = _band(ds, nir_idx), _band(ds, swir_idx)
    return _emit(ds, _normalized_diff(n, s))


def savi(ds, red_idx, nir_idx, l=0.5) -> bytes:  # noqa: E741
    r, n = _band(ds, red_idx), _band(ds, nir_idx)
    l_ = float(l)
    with np.errstate(divide="ignore", invalid="ignore"):
        arr = (n - r) / (n + r + l_) * (1.0 + l_)
    return _emit(ds, arr)


def evi(
    ds, red_idx, nir_idx, blue_idx, l=1.0, c1=6.0, c2=7.5, g=2.5  # noqa: E741
) -> bytes:  # noqa: E741
    r, n, b = _band(ds, red_idx), _band(ds, nir_idx), _band(ds, blue_idx)
    l_, c1, c2, g = float(l), float(c1), float(c2), float(g)
    with np.errstate(divide="ignore", invalid="ignore"):
        arr = g * (n - r) / (n + c1 * r - c2 * b + l_)
    return _emit(ds, arr)
