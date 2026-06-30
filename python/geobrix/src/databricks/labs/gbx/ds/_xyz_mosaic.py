"""Pure (Spark-free) per-tile XYZ mosaic core for the pmtiles_gbx raster reader.

Enumerate slippy-map tiles for an AOI (morecantile WebMercatorQuad) and render each
tile by compositing the covering source rasters with rio-tiler's mosaic_reader. No
full mosaic is built: each tile reads only its 256x256 window, so memory is bounded
per tile and the work distributes one (z,x,y) at a time.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

BBox = Tuple[float, float, float, float]


def _tms():
    import morecantile

    return morecantile.tms.get("WebMercatorQuad")


def enumerate_tiles(bbox: BBox, min_z: int, max_z: int) -> List[Tuple[int, int, int]]:
    """Every (z, x, y) WebMercatorQuad tile intersecting bbox (EPSG:4326) across z."""
    tms = _tms()
    w, s, e, n = bbox
    out: List[Tuple[int, int, int]] = []
    for z in range(int(min_z), int(max_z) + 1):
        for t in tms.tiles(w, s, e, n, [z]):
            out.append((int(t.z), int(t.x), int(t.y)))
    return out


def source_bounds_union(paths: Sequence[str]) -> BBox:
    """EPSG:4326 union of the source rasters' bounds."""
    import rasterio
    from rasterio.warp import transform_bounds

    ws = ss = es = ns = None
    for p in paths:
        with rasterio.open(p) as ds:
            w, s, e, n = transform_bounds(ds.crs, "EPSG:4326", *ds.bounds)
        ws = w if ws is None else min(ws, w)
        ss = s if ss is None else min(ss, s)
        es = e if es is None else max(es, e)
        ns = n if ns is None else max(ns, n)
    if ws is None:
        raise ValueError("source_bounds_union: no source rasters")
    return (ws, ss, es, ns)


def render_tile(z, x, y, paths: Sequence[str], tile_format: str = "PNG") -> Optional[bytes]:
    """Composite the source rasters at `paths` for tile (z,x,y); PNG bytes or None.

    Opens each source fresh per call via rio-tiler Reader so that concurrent
    executor tasks are isolated.  Uses mosaic_reader serially (threads=0) to avoid
    concurrent reads on shared file handles.  Returns None when no source covers
    the tile (caller skips it).
    """
    from rio_tiler.errors import EmptyMosaicError
    from rio_tiler.io import Reader
    from rio_tiler.mosaic import mosaic_reader

    def _read(path, tx, ty, tz):
        with Reader(path) as cog:
            return cog.tile(tx, ty, tz)

    try:
        img, _ = mosaic_reader(list(paths), _read, int(x), int(y), int(z), threads=0)
    except EmptyMosaicError:
        return None
    return bytes(img.render(img_format=tile_format))
