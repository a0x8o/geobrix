"""Raster rendering pipeline for gbx.viz (decimation + percentile stretch).

Ported from notebooks/examples/eo-series/library.py. matplotlib/rasterio are
lazy-imported inside the public plotters (Task 3); the numeric helpers here use
only numpy and the rasterio dataset passed in.
"""

import numpy as np


def _decimated_read(src, max_pixels):
    """Read `src` (rasterio DatasetReader) decimated so max(width,height)<=max_pixels.

    Returns (data, transform, scale). masked=True so nodata is honored downstream.
    """
    import rasterio

    scale = max(src.width, src.height) / max_pixels
    if scale > 1:
        out_shape = (src.count, int(src.height // scale), int(src.width // scale))
        data = src.read(
            out_shape=out_shape,
            resampling=rasterio.enums.Resampling.bilinear,
            masked=True,
        )
        transform = src.transform * src.transform.scale(
            src.width / data.shape[-1],
            src.height / data.shape[-2],
        )
    else:
        data = src.read(masked=True)
        transform = src.transform
    return data, transform, scale


def _needs_percentile_stretch(data):
    """True when data is integer-typed with a max above matplotlib's RGB int 255."""
    if not np.issubdtype(data.dtype, np.integer):
        return False
    mx = np.ma.max(data) if isinstance(data, np.ma.MaskedArray) else data.max()
    if mx is np.ma.masked:
        return False
    return int(mx) > 255


def _percentile_stretch(data, lo_pct=2, hi_pct=98):
    """Per-band 2-98th percentile stretch to [0,1] float32; masked pixels excluded."""
    if data.ndim == 2:
        data = data[np.newaxis, ...]
    is_masked = isinstance(data, np.ma.MaskedArray)
    out = np.empty(data.shape, dtype=np.float32)
    for b in range(data.shape[0]):
        band = data[b]
        valid = band.compressed() if is_masked else np.asarray(band).ravel()
        if valid.size == 0:
            out[b] = 0.0
            continue
        lo, hi = np.percentile(valid, (lo_pct, hi_pct))
        rng = max(float(hi - lo), 1e-9)
        out[b] = np.clip((np.asarray(band, dtype=np.float32) - lo) / rng, 0.0, 1.0)
    return np.ma.MaskedArray(out, mask=data.mask) if is_masked else out
