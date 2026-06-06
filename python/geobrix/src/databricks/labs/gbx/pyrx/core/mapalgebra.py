"""Spark-free map algebra over one or more rasters via numexpr (safe math eval).

Band 1 of each input raster (in order) is bound to variables A, B, C, …; the
expression is evaluated with numexpr (no arbitrary code execution). Output is a
single-band Float32 GTiff using the first raster's georeference.
"""

import string

import numexpr as ne
import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core._nodata import emit, read_masked

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
    invalid = None
    template = None
    try:
        for i, rb in enumerate(rasters):
            mf = MemoryFile(bytes(rb))
            ds = mf.open()
            opened.append((mf, ds))
            data, valid = read_masked(ds, 1)
            local_dict[_VARS[i]] = data
            invalid = (~valid) if invalid is None else (invalid | ~valid)
            if template is None:
                template = ds
        result = ne.evaluate(str(expression), local_dict=local_dict)
        # numexpr may broadcast a scalar expression (e.g. "A * 2") to an ndarray
        # when A is an ndarray — the result is already an array in that case, but
        # ensure we always have a 2-D array matching the spatial grid. emit reads
        # template.profile and writes synchronously, before finally closes ds.
        result = np.asarray(result, dtype="float64")
        return emit(template, result, -9999.0, invalid, "float32")
    finally:
        for mf, ds in opened:
            ds.close()
            mf.close()
