"""Spark-free H3 raster tessellation (mirrors heavyweight ``RST_H3_Tessellate``
/ ``RasterTessellate``).

For every H3 cell overlapping the raster's bounding box at the requested
resolution, the raster is clipped to that cell's hexagon geometry and one tile
is yielded per cell, carrying the H3 cell id as its ``cellid``. Cells whose
clip is empty / all-nodata are skipped.
"""

import h3
import shapely.wkb
from rasterio.features import geometry_mask
from rasterio.warp import transform_bounds, transform_geom
from shapely.geometry import Polygon, mapping, shape

from databricks.labs.gbx.pyrx.core import edit

H3_MAX_RES = 15

_WGS84 = "EPSG:4326"


def _cell_polygon_lonlat(cell: str) -> Polygon:
    """H3 cell hexagon as a shapely Polygon in (lon, lat) order.

    ``h3.cell_to_boundary`` returns (lat, lng) tuples; shapely expects
    (lon, lat), so the coordinates are flipped.
    """
    boundary = h3.cell_to_boundary(cell)  # list of (lat, lng)
    return Polygon([(lng, lat) for lat, lng in boundary])


def tessellate_h3(ds, resolution: int) -> list:
    """Tessellate a raster into H3 cells; return ``[(cellid_int, gtiff_bytes)]``.

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: H3 resolution in ``[0, 15]``.

    Returns:
        List of ``(cellid, raster_bytes)`` tuples, one per overlapping H3 cell
        whose clip is non-empty. ``cellid`` is the signed int64 H3 cell id.
    """
    resolution = int(resolution)
    if resolution < 0 or resolution > H3_MAX_RES:
        raise ValueError(
            f"rst_h3_tessellate: resolution must be in [0, {H3_MAX_RES}]; "
            f"got {resolution}"
        )

    # Raster bbox in WGS84 lon/lat, enumerate covering H3 cells.
    west, south, east, north = transform_bounds(ds.crs, _WGS84, *ds.bounds)
    bbox_poly = h3.LatLngPoly(
        [(south, west), (north, west), (north, east), (south, east)]
    )
    cells = h3.h3shape_to_cells(bbox_poly, resolution)
    # Buffer by one ring so edge cells overlapping the raster are included.
    covered = set(cells)
    for c in cells:
        covered.update(h3.grid_disk(c, 1))

    dst_epsg = ds.crs.to_epsg() if ds.crs else None
    reproject = dst_epsg != 4326

    out = []
    for cell in covered:
        cell_poly = _cell_polygon_lonlat(cell)
        if reproject:
            geom = transform_geom(_WGS84, ds.crs, mapping(cell_poly))
            cell_poly = shape(geom)
        # Emptiness guard (parity with heavy RasterTessellate.getTile, which
        # drops a cell when ClipToGeom + RasterAccessors.isEmpty find no valid
        # pixels): the one-ring expansion adds fringe candidates whose bounding
        # box clips the raster edge but whose hexagon covers ZERO pixel cells.
        # gdalwarp -crop_to_cutline yields an all-nodata tile for those, which
        # heavy's isEmpty rejects; without this guard the lightweight side keeps
        # the degenerate zero-coverage cell and diverges (6 vs heavy's 5).
        cover = geometry_mask(
            [cell_poly],
            out_shape=(ds.height, ds.width),
            transform=ds.transform,
            invert=True,
            all_touched=True,
        )
        if not cover.any():
            continue
        try:
            clipped = edit.clip_to_geom(ds, shapely.wkb.dumps(cell_poly))
        except ValueError:
            # rasterio.mask raises ValueError when the shape does not overlap.
            continue
        cellid = h3.str_to_int(cell)
        # h3 ids fit in unsigned 64-bit; map to signed int64 for the tile struct.
        if cellid >= 2**63:
            cellid -= 2**64
        out.append((cellid, clipped))
    return out
