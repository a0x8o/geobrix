"""Spark-free focal ops via scipy.ndimage. Each returns new GTiff bytes."""

import numpy as np
from rasterio.io import MemoryFile
from scipy import ndimage

_FILTERS = {
    "min": ndimage.minimum_filter,
    "max": ndimage.maximum_filter,
    "median": ndimage.median_filter,
}


def filt(ds, kernel_size, operation: str) -> bytes:
    """Apply a focal filter over a ``kernel_size`` x ``kernel_size`` window per band.

    Args:
        ds:           Open rasterio DatasetReader.
        kernel_size:  Side length of the square neighbourhood (odd int).
        operation:    ``"min"``, ``"max"``, ``"mean"``, or ``"median"``.

    Returns:
        GTiff bytes.  Output dtype matches the input except for ``"mean"``,
        which always returns Float32 (uniform_filter operates on float64
        internally; we cast back to float32 to stay consistent with the
        input raster dtype and the rasterx heavyweight equivalent).
    """
    size = int(kernel_size)
    op = str(operation)
    data = ds.read()
    bands = []
    for i in range(data.shape[0]):
        if op == "mean":
            # uniform_filter on float64 avoids integer truncation; cast back
            # to float32 afterward to match the input band dtype.
            bands.append(
                ndimage.uniform_filter(data[i].astype("float64"), size=size).astype(
                    "float32"
                )
            )
        else:
            bands.append(_FILTERS[op](data[i], size=size))
    out = np.stack(bands)
    out_dtype = "float32" if op == "mean" else data.dtype
    profile = ds.profile.copy()
    profile.update(driver="GTiff", dtype=out_dtype)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out.astype(out_dtype))
        return mf.read()


def convolve(ds, kernel) -> bytes:
    """2-D convolution per band with ``kernel`` (a 2-D list/array of floats).

    Args:
        ds:     Open rasterio DatasetReader.
        kernel: 2-D array-like of floats (e.g. ``[[0,1,0],[1,-4,1],[0,1,0]]``).

    Returns:
        GTiff bytes with dtype Float64.  Float64 is used because arbitrary
        kernel coefficients can produce values outside the input dtype range.
    """
    k = np.asarray(kernel, dtype="float64")
    data = ds.read().astype("float64")
    out = np.stack(
        [ndimage.convolve(data[i], k, mode="nearest") for i in range(data.shape[0])]
    )
    profile = ds.profile.copy()
    profile.update(driver="GTiff", dtype="float64")
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out)
        return mf.read()
