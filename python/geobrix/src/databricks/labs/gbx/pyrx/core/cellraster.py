"""Rasterize a set of H3 cells onto a regular grid (pixel-centroid burn).

The inverse of core.gridagg.raster_to_grid: there each pixel centroid is indexed
to an H3 cell; here each output pixel takes the value of the cell containing its
centroid. Pure functions (no Spark); rasterio + h3 + numpy + pyproj only.
"""

import math

import h3
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import Affine

_NODATA = -9999.0
_U64 = 0xFFFFFFFFFFFFFFFF


def _h3_str(cellid) -> str:
    """Canonical h3 string for a (possibly signed) Spark Long cell id."""
    return h3.int_to_str(int(cellid) & _U64)


def _resolution(cell_strs) -> int:
    res = h3.get_resolution(next(iter(cell_strs)))
    for c in cell_strs:
        if h3.get_resolution(c) != res:
            raise ValueError("H3 cell set has mixed resolutions")
    return res


def _reproject(xs, ys, src, dst):
    if src == dst:
        return np.asarray(xs, dtype="float64"), np.asarray(ys, dtype="float64")
    from pyproj import Transformer

    tr = Transformer.from_crs(src, dst, always_xy=True)
    x2, y2 = tr.transform(np.asarray(xs), np.asarray(ys))
    return np.asarray(x2, dtype="float64"), np.asarray(y2, dtype="float64")


def cell_bbox(cellid, srid=4326, mode="centroids"):
    """(xmin, ymin, xmax, ymax) for one cell in `srid`.

    mode='centroids' -> the centroid point (degenerate bbox); 'spatial_envelope'
    -> the hexagon boundary envelope.
    """
    c = _h3_str(cellid)
    if mode == "centroids":
        lat, lon = h3.cell_to_latlng(c)
        lons, lats = [lon], [lat]
    elif mode == "spatial_envelope":
        b = h3.cell_to_boundary(c)  # [(lat, lon), ...]
        lats = [p[0] for p in b]
        lons = [p[1] for p in b]
    else:
        raise ValueError(f"unknown mode {mode!r}")
    xs, ys = _reproject(lons, lats, 4326, srid)
    return float(xs.min()), float(ys.min()), float(xs.max()), float(ys.max())


def snap_bounds(bxmin, bymin, bxmax, bymax, pixel_size):
    """Snap a bounding box outward to the nearest pixel_size lattice.

    Returns (xmin, ymin, xmax, ymax, width, height) where xmin and ymax are
    integer multiples of pixel_size, and the grid is at least 1x1. Independent
    grids built with the same pixel_size and these snapped origins will align.
    Note: pixel_size is NOT included in the return tuple; callers must thread
    it separately (e.g. compute_gridspec inserts it into the returned gridspec).
    """
    xmin = math.floor(bxmin / pixel_size) * pixel_size
    ymax = math.ceil(bymax / pixel_size) * pixel_size
    width = max(1, int(math.ceil((bxmax - xmin) / pixel_size)))
    height = max(1, int(math.ceil((ymax - bymin) / pixel_size)))
    xmax = xmin + width * pixel_size
    ymin = ymax - height * pixel_size
    return xmin, ymin, xmax, ymax, width, height


def compute_gridspec(
    cellids, srid=4326, pixel_size=None, mode="centroids", kring_pad=1
):
    """Snapped, lattice-aligned grid spec for a cell set.

    Returns (xmin, ymin, xmax, ymax, pixel_size, width, height, srid).
    """
    cells = {_h3_str(c) for c in cellids}
    if not cells:
        raise ValueError("empty cell set")
    res = _resolution(cells)
    if kring_pad and kring_pad > 0:
        padded = set()
        for c in cells:
            padded.update(h3.grid_disk(c, kring_pad))
        cells = padded

    if mode == "centroids":
        pts = [h3.cell_to_latlng(c) for c in cells]  # (lat, lon)
        lons = [p[1] for p in pts]
        lats = [p[0] for p in pts]
    elif mode == "spatial_envelope":
        lons, lats = [], []
        for c in cells:
            for la, lo in h3.cell_to_boundary(c):
                lons.append(lo)
                lats.append(la)
    else:
        raise ValueError(f"unknown mode {mode!r}")

    xs, ys = _reproject(lons, lats, 4326, srid)
    bxmin, bxmax = float(xs.min()), float(xs.max())
    bymin, bymax = float(ys.min()), float(ys.max())

    if pixel_size is None:
        edge_m = h3.average_hexagon_edge_length(res, unit="m")
        if srid == 4326:
            midlat = (bymin + bymax) / 2.0
            pixel_size = edge_m / (111320.0 * max(math.cos(math.radians(midlat)), 1e-6))
        else:
            pixel_size = edge_m

    xmin, ymin, xmax, ymax, width, height = snap_bounds(
        bxmin, bymin, bxmax, bymax, pixel_size
    )
    return (xmin, ymin, xmax, ymax, pixel_size, width, height, srid)


def cells_to_raster(
    cell_values, xmin, ymin, xmax, ymax, pixel_size, width, height, srid, resolution
):
    """Burn {cellid:int -> value:float} onto a width x height grid (centroid burn).

    Arg order matches the `compute_gridspec` 8-tuple (so callers splat it:
    `cells_to_raster(cell_values, *gridspec, resolution=res)`). The snapped grid has
    square pixels of `pixel_size`. Returns single-band float64 GTiff bytes; NoData
    where no cell covers a pixel.
    """
    lut = {_h3_str(c): float(v) for c, v in cell_values.items()}
    transform = Affine(pixel_size, 0.0, xmin, 0.0, -pixel_size, ymax)

    cols = np.arange(width) + 0.5
    rows = np.arange(height) + 0.5
    gx, gy = np.meshgrid(xmin + cols * pixel_size, ymax - rows * pixel_size)  # (h, w)
    lon, lat = _reproject(gx.ravel(), gy.ravel(), srid, 4326)

    out = np.full(lon.size, _NODATA, dtype="float64")
    # Scalar h3 index per pixel (no array API). The grid is bounded to the cells'
    # padded bbox, so this is O(pixels-in-footprint). PERF FOLLOW-UP: restrict to
    # pixels within each cell's local window instead of the whole grid.
    for i in range(lon.size):
        v = lut.get(h3.latlng_to_cell(float(lat[i]), float(lon[i]), resolution))
        if v is not None:
            out[i] = v

    data = out.reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float64",
        crs=f"EPSG:{srid}",
        transform=transform,
        nodata=_NODATA,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()
