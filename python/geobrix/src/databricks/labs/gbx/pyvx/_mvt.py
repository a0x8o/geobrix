"""Pure-Python MVT encoding + XYZ pyramid tiling (Spark-free, Serverless-safe)."""
import math
from typing import Any, Dict, Iterator, List, Tuple

import mapbox_vector_tile as mvt
from shapely import from_wkb
from shapely.geometry import box
from shapely.ops import transform

from ._serde import to_native_props

MAX_ZOOM = 20
MAX_TILES = 1_000_000
DEFAULT_EXTENT = 4096


def encode_layer(
    features: List[Dict[str, Any]],
    layer_name: str,
    extent: int = DEFAULT_EXTENT,
) -> bytes:
    """Encode features into one MVT layer blob.

    Each feature is ``{'geometry': <WKB bytes or Shapely geom>, 'properties': dict}``.
    Geometry is passed through as-is; callers are responsible for projecting to
    tile-local pixel coordinates [0, extent] before calling (e.g. via ``_to_tile_local``).
    Property values keep their native Python type (bool/int/float/str);
    non-native types are str()-ified via ``to_native_props``.

    Uses ``y_coord_down=True`` so the caller's y-axis (0 = top) is preserved
    as-is without an additional flip.
    """
    layer_feats = []
    for f in features:
        geom = f["geometry"]
        if isinstance(geom, (bytes, bytearray)):
            shp = from_wkb(bytes(geom))
        else:
            shp = geom
        if shp is None or shp.is_empty:
            continue
        layer_feats.append(
            {
                "geometry": shp,
                "properties": to_native_props(f.get("properties")),
            }
        )
    return mvt.encode(
        {"name": layer_name, "features": layer_feats},
        default_options={"extents": extent, "y_coord_down": True},
    )


def _lonlat_to_tile(lon: float, lat: float, z: int) -> Tuple[int, int]:
    n = 2**z
    x = int((lon + 180.0) / 360.0 * n)
    lat_r = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_r)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def _tile_bounds(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    n = 2**z
    lon1 = x / n * 360.0 - 180.0
    lon2 = (x + 1) / n * 360.0 - 180.0
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat2 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon1, min(lat1, lat2), lon2, max(lat1, lat2)


def _to_tile_local(geom, z: int, x: int, y: int, extent: int):
    """Project a WGS-84 geometry into [0, extent] tile-pixel space for tile (z, x, y).

    Origin is NW corner (x=0, y=0), matching the XYZ/WebMercator convention.
    """
    minx, miny, maxx, maxy = _tile_bounds(z, x, y)
    sx = extent / (maxx - minx)
    sy = extent / (maxy - miny)
    return transform(
        lambda xs, ys, zs=None: ((xs - minx) * sx, (maxy - ys) * sy),
        geom,
    )


def pyramid_tiles(
    geom_wkb: Any,
    attrs: Any,
    min_z: int,
    max_z: int,
    layer_name: str,
    extent: int = DEFAULT_EXTENT,
) -> Iterator[Tuple[int, int, int, bytes]]:
    """Yield ``(z, x, y, mvt_bytes)`` for every tile a WGS-84 feature intersects across [min_z, max_z].

    Yields incrementally (no buffering) to keep worker memory flat. Limits:
    - ``max_z`` <= MAX_ZOOM (20).
    - Total intersecting tiles <= MAX_TILES (1,000,000); raises ValueError if exceeded.

    Args:
        geom_wkb: WKB bytes (or Shapely geometry) in EPSG:4326.
        attrs: Mapping or PySpark Row of feature attributes.
        min_z: Minimum zoom level (inclusive).
        max_z: Maximum zoom level (inclusive).
        layer_name: MVT layer name written into each tile blob.
        extent: Tile coordinate extent (default 4096).
    """
    if min_z < 0:
        raise ValueError(f"min_z must be >= 0; got {min_z}")
    if max_z < min_z:
        raise ValueError(f"max_z ({max_z}) must be >= min_z ({min_z})")
    if max_z > MAX_ZOOM:
        raise ValueError(f"max_z {max_z} exceeds MAX_ZOOM {MAX_ZOOM}")
    if isinstance(geom_wkb, (bytes, bytearray)):
        shp = from_wkb(bytes(geom_wkb))
    else:
        shp = geom_wkb
    if shp is None or shp.is_empty:
        return
    props = to_native_props(attrs)
    minx, miny, maxx, maxy = shp.bounds
    # Pre-count tiles to enforce the cap before emitting anything.
    total = 0
    spans: Dict[int, Tuple[range, range]] = {}
    for z in range(min_z, max_z + 1):
        # Tile y increases southward, so SW corner has higher y than NE corner.
        x_sw, y_sw = _lonlat_to_tile(minx, miny, z)
        x_ne, y_ne = _lonlat_to_tile(maxx, maxy, z)
        xr = range(min(x_sw, x_ne), max(x_sw, x_ne) + 1)
        yr = range(min(y_ne, y_sw), max(y_ne, y_sw) + 1)
        spans[z] = (xr, yr)
        total += len(xr) * len(yr)
        if total > MAX_TILES:
            raise ValueError(
                f"pyramid would emit > {MAX_TILES} tiles; narrow the zoom range"
            )
    for z in range(min_z, max_z + 1):
        xr, yr = spans[z]
        for x in xr:
            for y in yr:
                tb = box(*_tile_bounds(z, x, y))
                clipped = shp.intersection(tb)
                if clipped.is_empty:
                    continue
                local = _to_tile_local(clipped, z, x, y, extent)
                blob = encode_layer(
                    [{"geometry": local, "properties": props}],
                    layer_name,
                    extent,
                )
                yield (z, x, y, blob)
