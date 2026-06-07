"""Shared NoData / edge-mask helpers for pyrx neighborhood + band-math ops.

Matches the heavyweight (GDAL) semantics: read each band with its validity mask,
propagate input NoData through a 3x3 window AND mark the 1-px kernel border as
NoData (one binary_erosion with border_value=0), and emit GTiff bytes that set
those pixels to the output NoData value.
"""

from __future__ import annotations

import numpy as np
from rasterio.io import MemoryFile
from scipy import ndimage


def read_masked(ds, band: int = 1):
    """Return (data: float64 2-D, valid: bool 2-D) for *band*.

    `valid` comes from ds.read_masks (0 = invalid/nodata, 255 = valid). When the
    band has no declared nodata, read_masks is all-255 -> valid all-True -> no
    masking, mirroring the heavyweight's "mask only when nodata is declared".
    """
    data = ds.read(int(band)).astype("float64")
    valid = ds.read_masks(int(band)) != 0
    return data, valid


def propagate_invalid(valid: np.ndarray, size: int = 3) -> np.ndarray:
    """Invalid where ANY pixel in the size x size window is invalid OR out-of-array.

    binary_erosion with border_value=0 treats out-of-bounds as invalid, so the
    result includes BOTH the input-NoData propagation and the 1-px border ring
    that gdal.DEMProcessing produces by default.
    """
    structure = np.ones((size, size), dtype=bool)
    eroded = ndimage.binary_erosion(valid, structure=structure, border_value=0)
    return ~eroded


def emit(ds, arr: np.ndarray, nodata: float, invalid: np.ndarray, dtype: str) -> bytes:
    """Write *arr* as a single-band GTiff in memory, setting NoData where *invalid*
    (or non-finite). Float dtypes keep values; uint8 is clipped/rounded to [0,255]."""
    invalid = invalid | ~np.isfinite(arr)
    if dtype == "uint8":
        base = np.clip(np.round(arr), 0, 255)
    else:
        base = arr.astype("float64")
    out = np.where(invalid, nodata, base).astype(dtype)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype=dtype, nodata=nodata)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out, 1)
        return mf.read()
