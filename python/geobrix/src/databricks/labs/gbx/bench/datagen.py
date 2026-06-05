"""Seeded, valid-at-scale raster tile generator for benchmarking."""
from __future__ import annotations

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

# CRS -> (origin_x, origin_y, pixel_size in CRS units) for a consistent affine.
_CRS_GEO = {
    4326:  (-73.99, 40.75, 0.0001),     # WGS84 degrees (NYC-ish)
    3857:  (-8237000.0, 4970000.0, 10.0),  # WebMercator metres
    32618: (583000.0, 4507000.0, 10.0),    # UTM 18N metres
    27700: (530000.0, 180000.0, 10.0),     # BNG metres (London)
}

_NODATA = {"uint8": 255, "int16": -9999, "float32": -9999.0}
# Values represent non-negative reflectance/elevation-like magnitudes; keeping
# them >= 0 guarantees spectral-index validity across all dtypes (e.g. NDVI =
# (nir-red)/(nir+red) stays in [-1, 1] because the denominator never crosses
# zero). Terrain ops are unaffected -- they use gradients.
_DTYPE_RANGE = {"uint8": (0, 254), "int16": (0, 1000), "float32": (0.0, 1.0)}


def _base_field(tile_px: int, rng: np.random.Generator) -> np.ndarray:
    """A smooth gradient + low-amplitude noise + sinusoid, in [0,1]."""
    y, x = np.mgrid[0:tile_px, 0:tile_px].astype("float64") / max(tile_px - 1, 1)
    grad = 0.5 * (x + y) / 2.0 + 0.5 * x  # ramp
    sin = 0.15 * np.sin(6.0 * np.pi * x) * np.cos(6.0 * np.pi * y)
    noise = 0.05 * rng.standard_normal((tile_px, tile_px))
    f = grad + sin + noise
    f -= f.min()
    f /= max(f.max(), 1e-9)
    return f  # [0,1]


def _to_dtype(f01: np.ndarray, dtype: str) -> np.ndarray:
    lo, hi = _DTYPE_RANGE[dtype]
    arr = lo + f01 * (hi - lo)
    return arr.astype(dtype)


def make_tile_bytes(tile_px: int, bands: int, dtype: str, srid: int,
                    nodata_frac: float, seed: int,
                    nodata_mode: str = "sparse") -> bytes:
    """Generate one valid GeoTIFF tile as in-memory bytes (deterministic per seed).

    With ``nodata_mode="sparse"`` (default), the requested ``nodata_frac`` is hit
    exactly via an exact-count random pixel mask. With ``nodata_mode="border"``,
    the nodata region is an approximate frame whose actual fraction can diverge
    from ``nodata_frac`` (especially for small tiles or extreme fractions).
    """
    rng = np.random.default_rng(seed)
    ox, oy, px = _CRS_GEO[srid]
    transform = from_origin(ox, oy, px, px)
    nodata = _NODATA[dtype]

    base = _base_field(tile_px, rng)  # [0,1]
    data = np.empty((bands, tile_px, tile_px), dtype=dtype)
    for bi in range(bands):
        # Band-correlated: each band a monotone transform of the shared field,
        # so spectral indices (NDVI etc.) are non-degenerate and in-range.
        shifted = np.clip(base ** (1.0 + 0.3 * bi) + 0.02 * bi, 0.0, 1.0)
        data[bi] = _to_dtype(shifted, dtype)

    if nodata_frac > 0:
        n = int(round(nodata_frac * tile_px * tile_px))
        if nodata_mode == "border":
            mask = np.zeros((tile_px, tile_px), dtype=bool)
            w = max(1, int(round(nodata_frac * tile_px / 4)))
            mask[:w, :] = mask[-w:, :] = mask[:, :w] = mask[:, -w:] = True
        else:  # "sparse" (default): exact-count random pixel mask
            flat = rng.choice(tile_px * tile_px, size=min(n, tile_px * tile_px),
                              replace=False)
            mask = np.zeros(tile_px * tile_px, dtype=bool)
            mask[flat] = True
            mask = mask.reshape(tile_px, tile_px)
        for bi in range(bands):
            data[bi][mask] = nodata

    profile = {
        "driver": "GTiff", "width": tile_px, "height": tile_px, "count": bands,
        "dtype": dtype, "crs": rasterio.crs.CRS.from_epsg(srid),
        "transform": transform, "nodata": nodata,
    }
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return bytes(mf.read())
