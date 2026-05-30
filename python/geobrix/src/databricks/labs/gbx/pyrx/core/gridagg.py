"""Spark-free raster->discrete-global-grid aggregation (H3 + quadbin).

Mirrors the heavyweight ``RST_{H3,Quadbin}_RasterToGrid`` family exactly:

* Per band, every valid pixel (non-zero mask) is mapped to a grid cell id by
  the pixel-centroid world coordinate (0.5-pixel offset through the
  geotransform). The raster is interpreted as EPSG:4326 lon/lat -- no
  reprojection (callers reproject upstream via ``rst_transform``).
* Pixel values are accumulated per cell, then reduced by the chosen aggregate.

Backed by the best-in-class libs ``h3`` (v4) and ``quadbin`` (CARTO v0).
"""

import h3
import numpy as np
import quadbin

H3_MAX_RES = 15
QUADBIN_MAX_RES = 20

_AGGS = ("avg", "count", "min", "max", "median")


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


def _h3_cell(lon: float, lat: float, resolution: int) -> int:
    # h3 v4 arg order is (lat, lng); str_to_int yields the int64 cell id.
    return h3.str_to_int(h3.latlng_to_cell(lat, lon, resolution))


def _quadbin_cell(lon: float, lat: float, resolution: int) -> int:
    # quadbin arg order is (lon, lat).
    return quadbin.point_to_cell(lon, lat, resolution)


def _reduce(values, agg: str):
    arr = np.asarray(values, dtype="float64")
    if agg == "avg":
        return float(arr.mean())
    if agg == "count":
        return int(arr.size)
    if agg == "min":
        return float(arr.min())
    if agg == "max":
        return float(arr.max())
    if agg == "median":
        return float(np.median(arr))
    raise ValueError(f"unknown agg {agg!r}; expected one of {_AGGS}")


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

    cell_fn = _h3_cell if grid == "h3" else _quadbin_cell
    gt = ds.transform.to_gdal()  # (c, a, b, f, d, e) == GDAL geotransform

    out = []
    for bi in range(1, ds.count + 1):
        band = ds.read(bi).astype("float64")
        mask = ds.read_masks(bi)  # 0 = invalid (nodata-derived)
        height, width = band.shape

        acc = {}
        for y in range(height):
            for x in range(width):
                if mask[y, x] == 0:
                    continue
                x_off = x + 0.5
                y_off = y + 0.5
                lon = gt[0] + x_off * gt[1] + y_off * gt[2]
                lat = gt[3] + x_off * gt[4] + y_off * gt[5]
                cid = cell_fn(lon, lat, resolution)
                acc.setdefault(cid, []).append(band[y, x])

        out.append(
            [
                {"cellID": cid, "measure": _reduce(vals, agg)}
                for cid, vals in acc.items()
            ]
        )
    return out
