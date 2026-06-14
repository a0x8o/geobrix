"""Pure-Python quadbin GridX core for the pygx light tier.

Cell math via the `quadbin` package; logic the package lacks (distance, bbox
polyfill) is ported to match the heavy `gridx/grid/Quadbin.scala` exactly.
Geometry outputs are EWKB (SRID 4326), matching heavy's JTS.toEWKB.
"""

import math

import quadbin
from shapely import set_srid, to_wkb, union_all
from shapely.geometry import Point, box

from ._geom import parse_geom

_MAX_RES = 26
_MAX_POLYFILL_RES = 20

# Web-mercator latitude clamp (matches Quadbin.scala LAT_MIN/LAT_MAX).
_LAT_MIN = -85.05112878
_LAT_MAX = 85.05112878


def point_as_cell(lon: float, lat: float, resolution: int) -> int:
    z = int(resolution)
    if z < 0 or z > _MAX_RES:
        raise ValueError(f"quadbin resolution must be in [0,{_MAX_RES}]; got {z}")
    # Match heavy Quadbin.scala pointToCell exactly: derive the (x, y) tile via the
    # heavy lonLatToTile clamp (floor + clamp to [0, n-1]), then pack via the
    # canonical CARTO tile_to_cell. NOTE: we do NOT delegate to quadbin.point_to_cell
    # — it wraps lon at the antimeridian (lon=180 -> xTile 0), whereas heavy floors
    # to n then clamps to n-1 (keeps the easternmost tile). Routing through
    # _lonlat_to_tile makes the antimeridian/pole edges bit-identical to heavy.
    x, y = _lonlat_to_tile(float(lon), float(lat), z)
    return quadbin.tile_to_cell((x, y, z))


def resolution(cell: int) -> int:
    return quadbin.get_resolution(int(cell))


def k_ring(cell: int, k: int) -> list:
    if int(k) < 0:
        raise ValueError(f"k must be >= 0; got {k}")
    return list(quadbin.k_ring(int(cell), int(k)))


def distance(cell_a: int, cell_b: int) -> int:
    if resolution(cell_a) != resolution(cell_b):
        raise ValueError("quadbin_distance: cells must be at same resolution")
    ax, ay = quadbin.cell_to_tile(int(cell_a))[:2]
    bx, by = quadbin.cell_to_tile(int(cell_b))[:2]
    return int(max(abs(ax - bx), abs(ay - by)))


def _ewkb(geom) -> bytes:
    return to_wkb(set_srid(geom, 4326), include_srid=True)


def as_wkb(cell: int) -> bytes:
    w, s, e, n = quadbin.cell_to_bounding_box(int(cell))
    return _ewkb(box(w, s, e, n))


def centroid(cell: int) -> bytes:
    # Match heavy Quadbin.scala cellCenter exactly: the ARITHMETIC MEAN of the
    # cell bbox corners ((lonMin+lonMax)/2, (latMin+latMax)/2). NOTE: we do NOT use
    # quadbin.cell_to_point — it returns the TRUE inverse-mercator cell center,
    # whose latitude differs from the corner-mean (heavy averages the two
    # mercator-projected corner latitudes). The corner-mean keeps light bit-faithful
    # to heavy's centroid.
    w, s, e, n = quadbin.cell_to_bounding_box(int(cell))
    return _ewkb(Point((w + e) / 2.0, (s + n) / 2.0))


def cell_union(cells):
    if not cells:
        return None
    polys = [box(*quadbin.cell_to_bounding_box(int(c))) for c in cells if c is not None]
    if not polys:
        return None
    return _ewkb(union_all(polys))


def _lonlat_to_tile(lon: float, lat: float, z: int):
    """Port of Quadbin.scala lonLatToTile: (lon, lat) -> clamped (xTile, yTile)."""
    lat = max(_LAT_MIN, min(_LAT_MAX, lat))
    lon = max(-180.0, min(180.0, lon))
    n = 1 if z == 0 else (1 << z)
    lat_rad = lat * math.pi / 180.0
    x = math.floor((lon + 180.0) / 360.0 * n)
    y = math.floor(
        (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi)
        / 2.0
        * n
    )
    x = max(0, min(n - 1, x))
    y = max(0, min(n - 1, y))
    return x, y


def polyfill(geom, resolution: int) -> list:
    """Quadbin cells covering the geometry's envelope (bbox) at `resolution`.

    Mirrors heavy Quadbin_Polyfill / Quadbin.polyfillBbox: derive the SW/NE
    tile range from the bbox corners at `resolution` and enumerate every tile.
    """
    z = int(resolution)
    if z < 0 or z > _MAX_POLYFILL_RES:
        raise ValueError(
            f"quadbin_polyfill: resolution must be in [0, {_MAX_POLYFILL_RES}]; got {z}"
        )
    parsed = parse_geom(geom)
    if parsed is None or parsed.is_empty:
        return []
    w, s, e, n = parsed.bounds  # (minx, miny, maxx, maxy)
    x0, y0 = _lonlat_to_tile(w, n, z)  # upper-left
    x1, y1 = _lonlat_to_tile(e, s, z)  # lower-right
    x_lo, x_hi = min(x0, x1), max(x0, x1)
    y_lo, y_hi = min(y0, y1), max(y0, y1)
    count = (x_hi - x_lo + 1) * (y_hi - y_lo + 1)
    if count > 1_000_000:  # mirrors Quadbin.polyfillBbox maxCells
        raise ValueError(
            f"polyfill would produce {count} cells (max=1000000); use a lower zoom"
        )
    return [
        quadbin.tile_to_cell((x, y, z))
        for x in range(x_lo, x_hi + 1)
        for y in range(y_lo, y_hi + 1)
    ]


def tessellate(geom, resolution: int) -> list:
    """Tessellate a geometry into quadbin chips: list of (cell, EWKB(intersection)).

    Polyfill the bbox, then per cell intersect its polygon with the input geom,
    emitting (cell, EWKB) and dropping empty intersections. Mirrors heavy
    Quadbin_Tessellate.execute.
    """
    cells = polyfill(geom, resolution)
    if not cells:
        return []
    parsed = parse_geom(geom)
    chips = []
    for cell in cells:
        cell_box = box(*quadbin.cell_to_bounding_box(int(cell)))
        try:
            inter = cell_box.intersection(parsed)
        except Exception:
            continue
        if inter is None or inter.is_empty:
            continue
        chips.append((int(cell), _ewkb(inter)))
    return chips
