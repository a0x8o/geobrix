"""Spark-free web-mercator XYZ slippy-map tiling (rio-tiler + morecantile).

Mirrors the heavyweight rasterx ``RST_TileXYZ`` / ``RST_XYZPyramid`` semantics:

  * ``render_tile`` renders a single (z, x, y) web-mercator tile to PNG / JPEG /
    WEBP bytes. Out-of-extent or empty tiles return a transparent PNG (RGBA,
    alpha=0) of the requested size — NEVER null — because slippy-map servers
    need a 200-status non-empty body outside source coverage. On ANY hard
    failure we likewise return a transparent PNG.
  * ``pyramid`` enumerates every intersecting (z, x, y) tile across a zoom range
    and renders each, returning a list of ``{"z","x","y","bytes"}`` dicts.

rio-tiler handles on-the-fly reprojection: the source raster may be in any CRS;
``Reader.tile`` warps to the EPSG:3857 tile grid internally.
"""

import morecantile
import numpy as np
from rasterio.warp import transform_bounds
from rio_tiler.errors import TileOutsideBounds
from rio_tiler.io import Reader
from rio_tiler.models import ImageData

# --- constants (mirror heavyweight) -----------------------------------------
MAX_ZOOM = 20
MAX_TILE_COUNT = 1_000_000

ALLOWED_FORMATS = {"PNG", "JPEG", "WEBP"}

# Heavyweight gdalwarp resampling name -> rasterio.enums.Resampling name.
_RESAMPLING_MAP = {
    "near": "nearest",
    "bilinear": "bilinear",
    "cubic": "cubic",
    "cubicspline": "cubic_spline",
    "lanczos": "lanczos",
    "average": "average",
    "mode": "mode",
    "max": "max",
    "min": "min",
    "med": "med",
    "q1": "q1",
    "q3": "q3",
}

_TMS = morecantile.tms.get("WebMercatorQuad")


def transparent_png(size: int) -> bytes:
    """Return a fully transparent RGBA PNG of ``size`` x ``size`` (alpha=0)."""
    s = int(size)
    arr = np.zeros((4, s, s), dtype="uint8")
    return ImageData(arr).render(add_mask=False, img_format="PNG")


def _validate(fmt: str, size: int, resampling: str) -> tuple:
    """Validate + normalize (fmt upper, resampling -> rasterio name). Raises ValueError."""
    fmt_u = str(fmt).upper()
    if fmt_u not in ALLOWED_FORMATS:
        raise ValueError(
            f"rst_tilexyz: format must be one of {', '.join(sorted(ALLOWED_FORMATS))}; "
            f"got '{fmt}'"
        )
    resamp_l = str(resampling).lower()
    if resamp_l not in _RESAMPLING_MAP:
        raise ValueError(
            f"rst_tilexyz: unsupported resampling '{resampling}'; allowed: "
            f"{', '.join(sorted(_RESAMPLING_MAP))}"
        )
    s = int(size)
    if not (0 < s <= 4096):
        raise ValueError(f"rst_tilexyz: size must be in (0, 4096]; got {s}")
    return fmt_u, s, _RESAMPLING_MAP[resamp_l]


def render_tile(ds, z, x, y, fmt="PNG", size=256, resampling="bilinear") -> bytes:
    """Render a single web-mercator (z, x, y) tile from open dataset ``ds``.

    Validates inputs (raises ValueError on bad format/size/resampling). Out-of-
    extent / empty tiles, or any hard render failure, return a transparent PNG
    of ``size`` x ``size`` (mirrors heavyweight: PNG regardless of ``fmt``).
    """
    fmt_u, s, resamp_name = _validate(fmt, size, resampling)
    try:
        with Reader(None, dataset=ds) as cog:
            img = cog.tile(
                int(x), int(y), int(z), tilesize=s, resampling_method=resamp_name
            )
            out = img.render(img_format=fmt_u)
        if not out:
            return transparent_png(s)
        return out
    except TileOutsideBounds:
        return transparent_png(s)
    except Exception:
        # Slippy-map servers need a non-null 200 body even on failure.
        return transparent_png(s)


def _wgs84_bounds(ds) -> tuple:
    """Source extent as (west, south, east, north) in EPSG:4326."""
    return transform_bounds(ds.crs, "EPSG:4326", *ds.bounds)


def _zoom_tile_count(west, south, east, north, z) -> int:
    return sum(1 for _ in _TMS.tiles(west, south, east, north, [int(z)]))


def tile_count(ds, min_z, max_z) -> int:
    """Total intersecting tiles across [min_z, max_z]. Validates the zoom guards."""
    lo, hi = _validate_zoom_range(min_z, max_z)
    west, south, east, north = _wgs84_bounds(ds)
    total = 0
    for z in range(lo, hi + 1):
        total += _zoom_tile_count(west, south, east, north, z)
        if total > MAX_TILE_COUNT:
            _raise_count(lo, hi)
    return total


def intersecting_tiles(ds, min_z, max_z) -> list:
    """List of (z, x, y) tuples intersecting the source extent across the range."""
    lo, hi = _validate_zoom_range(min_z, max_z)
    west, south, east, north = _wgs84_bounds(ds)
    out = []
    for z in range(lo, hi + 1):
        for t in _TMS.tiles(west, south, east, north, [z]):
            out.append((t.z, t.x, t.y))
    return out


def pyramid(ds, min_z, max_z, fmt="PNG", size=256, resampling="bilinear") -> list:
    """Render every intersecting (z, x, y) tile across [min_z, max_z].

    Returns a list of ``{"z","x","y","bytes"}`` dicts. Validates zoom guards and
    the tile-count guard BEFORE rendering any tile.
    """
    lo, hi = _validate_zoom_range(min_z, max_z)
    # Validate render args up front (so bad format/size fails fast, not per-tile).
    _validate(fmt, size, resampling)
    west, south, east, north = _wgs84_bounds(ds)

    # Count guard first — never materialize a giant list to count.
    total = 0
    for z in range(lo, hi + 1):
        total += _zoom_tile_count(west, south, east, north, z)
        if total > MAX_TILE_COUNT:
            _raise_count(lo, hi)

    out = []
    for z in range(lo, hi + 1):
        for t in _TMS.tiles(west, south, east, north, [z]):
            b = render_tile(ds, t.z, t.x, t.y, fmt, size, resampling)
            out.append({"z": t.z, "x": t.x, "y": t.y, "bytes": b})
    return out


def _validate_zoom_range(min_z, max_z) -> tuple:
    lo, hi = int(min_z), int(max_z)
    if lo < 0:
        raise ValueError(f"rst_xyzpyramid: min_z must be >= 0; got {lo}")
    if hi < lo:
        raise ValueError(f"rst_xyzpyramid: max_z ({hi}) must be >= min_z ({lo})")
    if hi > MAX_ZOOM:
        raise ValueError(
            f"rst_xyzpyramid: max_z must be <= {MAX_ZOOM} "
            f"(cell-count explosion at higher zooms); got {hi}"
        )
    return lo, hi


def _raise_count(lo, hi):
    raise ValueError(
        f"rst_xyzpyramid: tile-count across zoom range [{lo}, {hi}] exceeds "
        f"{MAX_TILE_COUNT} (raster extent is too large for that pyramid depth). "
        f"Lower max_z, or upstream-resample the raster before pyramidizing."
    )
