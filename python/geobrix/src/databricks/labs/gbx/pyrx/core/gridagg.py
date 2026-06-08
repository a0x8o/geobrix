"""Spark-free raster->discrete-global-grid aggregation (H3 + quadbin).

Mirrors the heavyweight ``RST_{H3,Quadbin}_RasterToGrid`` family exactly:

* Per band, every valid pixel (non-zero mask) is mapped to a grid cell id by
  the pixel-centroid world coordinate (0.5-pixel offset through the
  geotransform). The raster is interpreted as EPSG:4326 lon/lat -- no
  reprojection (callers reproject upstream via ``rst_transform``).
* Pixel values are accumulated per cell, then reduced by the chosen aggregate.

Backed by the best-in-class libs ``h3`` (v4) and ``quadbin`` (CARTO v0).

Hot path is vectorized: valid pixels are gathered once, mapped to cell ids,
and grouped/reduced with numpy. The quadbin encoder ``_quadbin_cells`` is a
numpy reimplementation of ``quadbin.point_to_cell`` that is BIT-EXACT with the
upstream lib (the lib's per-call Python is otherwise ~20x slower than H3's
C-backed encoder); the H3 encoder stays a scalar comprehension over valid
pixels because ``h3.latlng_to_cell`` is C-backed and exposes no array API.
"""

import h3
import numpy as np

H3_MAX_RES = 15
QUADBIN_MAX_RES = 20

_AGGS = ("avg", "count", "min", "max", "median")

# quadbin 64-bit cell layout constants (see quadbin.main / quadbin.utils).
_QB_HEADER = np.uint64(0x4000000000000000)
_QB_FOOTER = np.uint64(0xFFFFFFFFFFFFF)
_QB_MODE = np.uint64(1) << np.uint64(59)
_QB_MAX_LAT = 89.0  # web-mercator clip bound used by clip_latitude


def _validate_resolution(resolution: int, grid: str) -> None:
    if grid == "h3":
        if resolution < 0 or resolution > H3_MAX_RES:
            raise ValueError(
                f"H3 resolution has to be between 0 and {H3_MAX_RES}; "
                f"found {resolution}"
            )
    elif grid == "quadbin":
        if resolution < 0 or resolution > QUADBIN_MAX_RES:
            raise ValueError(
                f"raster->quadbin: resolution must be in [0, {QUADBIN_MAX_RES}]; "
                f"got {resolution}"
            )
    else:
        raise ValueError(f"unknown grid {grid!r}; expected 'h3' or 'quadbin'")


def _h3_cells(lon: np.ndarray, lat: np.ndarray, resolution: int) -> np.ndarray:
    """Per-valid-pixel H3 cell ids (uint64). Scalar lib call; no array API."""
    cells = [
        h3.str_to_int(h3.latlng_to_cell(float(la), float(lo), resolution))
        for lo, la in zip(lon, lat)
    ]
    return np.array(cells, dtype="uint64")


def _quadbin_cells(lon: np.ndarray, lat: np.ndarray, resolution: int) -> np.ndarray:
    """Vectorized ``quadbin.point_to_cell`` -- bit-exact with the upstream lib.

    Reproduces, in numpy: longitude/latitude clipping, the web-mercator
    ``point_to_tile_fraction`` (+ floor), the tile-x wrap, and ``tile_to_cell``
    (the 32-bit shift, Morton bit-interleave, header/mode/zoom assembly, footer).
    """
    z = int(resolution)
    lon = np.asarray(lon, dtype="float64")
    lat = np.asarray(lat, dtype="float64")

    # clip_longitude / clip_latitude
    lon = np.clip(lon, -180.0, 180.0)
    lat = np.clip(lat, -_QB_MAX_LAT, _QB_MAX_LAT)

    z2 = float(1 << z)
    sinlat = np.sin(lat * np.pi / 180.0)
    xf = z2 * (lon / 360.0 + 0.5)
    yfraction = 0.5 - 0.25 * np.log((1.0 + sinlat) / (1.0 - sinlat)) / np.pi
    yf = np.clip(z2 * yfraction, 0.0, z2 - 1.0)

    # Wrap tile x: x %= z2; x += z2 if x < 0 (Python % on the float, then floor).
    xf = np.mod(xf, z2)
    xf = np.where(xf < 0.0, xf + z2, xf)

    x = np.floor(xf).astype("uint64")
    y = np.floor(yf).astype("uint64")

    shift = np.uint64(32 - z)
    x = x << shift
    y = y << shift

    # Morton spread (matches tile_to_cell's interleave masks/shifts exactly).
    b = [
        np.uint64(0x5555555555555555),
        np.uint64(0x3333333333333333),
        np.uint64(0x0F0F0F0F0F0F0F0F),
        np.uint64(0x00FF00FF00FF00FF),
        np.uint64(0x0000FFFF0000FFFF),
        np.uint64(0x00000000FFFFFFFF),
    ]
    s = [np.uint64(1), np.uint64(2), np.uint64(4), np.uint64(8), np.uint64(16)]

    x = (x | (x << s[4])) & b[4]
    y = (y | (y << s[4])) & b[4]
    x = (x | (x << s[3])) & b[3]
    y = (y | (y << s[3])) & b[3]
    x = (x | (x << s[2])) & b[2]
    y = (y | (y << s[2])) & b[2]
    x = (x | (x << s[1])) & b[1]
    y = (y | (y << s[1])) & b[1]
    x = (x | (x << s[0])) & b[0]
    y = (y | (y << s[0])) & b[0]

    interleaved = (x | (y << np.uint64(1))) >> np.uint64(12)
    footer = _QB_FOOTER >> np.uint64(z * 2)
    return (
        _QB_HEADER | _QB_MODE | (np.uint64(z) << np.uint64(52)) | interleaved | footer
    )


def _grouped_measures(cids: np.ndarray, vals: np.ndarray, agg: str):
    """Group ``vals`` by ``cids`` and reduce -> (unique_cids, measures).

    Loops at most over CELLS (median only), never over pixels. ``count`` yields
    Python ints; every other agg yields Python floats -- matching the original
    per-cell ``_reduce`` types and values exactly.
    """
    uniq, inv = np.unique(cids, return_inverse=True)
    inv = inv.astype(np.intp)
    counts = np.bincount(inv, minlength=uniq.size)

    if agg == "count":
        measures = [int(c) for c in counts]
        return uniq, measures

    vals = np.asarray(vals, dtype="float64")
    if agg == "avg":
        sums = np.bincount(inv, weights=vals, minlength=uniq.size)
        out = sums / counts
    elif agg == "min":
        out = np.full(uniq.size, np.inf)
        np.minimum.at(out, inv, vals)
    elif agg == "max":
        out = np.full(uniq.size, -np.inf)
        np.maximum.at(out, inv, vals)
    elif agg == "median":
        order = np.argsort(inv, kind="stable")
        sorted_vals = vals[order]
        bounds = np.cumsum(counts)[:-1]
        segments = np.split(sorted_vals, bounds)
        out = np.array([np.median(seg) for seg in segments], dtype="float64")
    else:
        raise ValueError(f"unknown agg {agg!r}; expected one of {_AGGS}")

    return uniq, [float(m) for m in out]


def raster_to_grid(ds, resolution: int, grid: str, agg: str) -> list:
    """Aggregate raster pixel values into discrete-global-grid cells, per band.

    Args:
        ds:         An open rasterio ``DatasetReader``.
        resolution: Grid resolution (H3 0..15; quadbin 0..20).
        grid:       ``"h3"`` or ``"quadbin"``.
        agg:        One of ``"avg"``, ``"count"``, ``"min"``, ``"max"``,
                    ``"median"``.

    Returns:
        One list per band; each is a list of
        ``{"cellID": int, "measure": float|int}`` (``int`` for ``count``).
    """
    resolution = int(resolution)
    _validate_resolution(resolution, grid)
    if agg not in _AGGS:
        raise ValueError(f"unknown agg {agg!r}; expected one of {_AGGS}")

    encode = _h3_cells if grid == "h3" else _quadbin_cells
    gt = ds.transform.to_gdal()  # (c, a, b, f, d, e) == GDAL geotransform

    out = []
    for bi in range(1, ds.count + 1):
        band = ds.read(bi).astype("float64")
        mask = ds.read_masks(bi)  # 0 = invalid (nodata-derived)

        ys, xs = np.nonzero(mask)  # valid pixels only (matches mask==0 skip)
        if ys.size == 0:
            out.append([])
            continue

        x_off = xs + 0.5
        y_off = ys + 0.5
        lon = gt[0] + x_off * gt[1] + y_off * gt[2]
        lat = gt[3] + x_off * gt[4] + y_off * gt[5]
        vals = band[ys, xs]

        cids = encode(lon, lat, resolution)
        uniq, measures = _grouped_measures(cids, vals, agg)
        out.append(
            [
                {"cellID": int(cid), "measure": m}
                for cid, m in zip(uniq.tolist(), measures)
            ]
        )
    return out
