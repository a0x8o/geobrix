"""Spark-free map algebra over one or more rasters via numexpr (safe math eval).

Band 1 of each input raster (in order) is bound to variables A, B, C, …; the
expression is evaluated with numexpr (no arbitrary code execution). Output is a
single-band Float32 GTiff using the first raster's georeference.
"""

import string

import numexpr as ne
import numpy as np
from rasterio.io import MemoryFile

_VARS = string.ascii_uppercase  # A..Z (up to 26 inputs)


def mapalgebra(rasters, expression: str) -> bytes:
    """Apply a math expression across one or more rasters.

    Band 1 of each raster (in order) is bound to A, B, C, …; the expression is
    evaluated with numexpr (safe math-only evaluator — no arbitrary code exec).
    Output is a single-band Float32 GTiff on the first raster's georeference.

    Args:
        rasters:    Sequence of raster bytes (at least one).
        expression: numexpr-compatible math expression, e.g. ``"(A - B) / (A + B)"``.

    Returns:
        GTiff bytes of the evaluated single-band Float32 raster.
    """
    if not rasters:
        raise ValueError("mapalgebra requires at least one raster")
    local_dict = {}
    opened = []
    try:
        first_profile = None
        for i, rb in enumerate(rasters):
            mf = MemoryFile(bytes(rb))
            ds = mf.open()
            opened.append((mf, ds))
            local_dict[_VARS[i]] = ds.read(1).astype("float64")
            if first_profile is None:
                first_profile = ds.profile.copy()
        result = ne.evaluate(str(expression), local_dict=local_dict)
        # numexpr may broadcast a scalar expression (e.g. "A * 2") to an ndarray
        # when A is an ndarray — the result is already an array in that case, but
        # ensure we always have a 2-D array matching the spatial grid.
        result = np.asarray(result, dtype="float32")
        first_profile.update(driver="GTiff", count=1, dtype="float32")
        with MemoryFile() as out:
            with out.open(**first_profile) as dst:
                dst.write(result, 1)
            return out.read()
    finally:
        for mf, ds in opened:
            ds.close()
            mf.close()
