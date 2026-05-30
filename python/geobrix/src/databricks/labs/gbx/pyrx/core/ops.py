"""Spark-free RasterX *Operations* ports: try-open validity check, format
conversion, internal-overview building, and point sampling.

Faithful ports of the heavyweight ``gbx_rst_tryopen`` / ``gbx_rst_asformat`` /
``gbx_rst_buildoverviews`` / ``gbx_rst_sample`` expressions, implemented with
rasterio's bundled GDAL (no JAR)."""

import os
import tempfile

import rasterio
import shapely.wkb
from rasterio.enums import Resampling
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde

# Heavyweight gdaladdo resampling name -> rasterio.enums.Resampling name.
# Mirrors the AllowedResampling set in RST_BuildOverviews.scala; "near" is the
# heavyweight alias for "nearest" and "cubicspline" maps to GDAL's cubic_spline.
_OVERVIEW_RESAMPLING_MAP = {
    "nearest": "nearest",
    "near": "nearest",
    "average": "average",
    "rms": "rms",
    "gauss": "gauss",
    "cubic": "cubic",
    "cubicspline": "cubic_spline",
    "lanczos": "lanczos",
    "bilinear": "bilinear",
    "mode": "mode",
}


def try_open(raster_bytes: bytes) -> bool:
    """Return True if ``raster_bytes`` open as a valid raster, False otherwise.

    Mirrors the heavyweight ``gbx_rst_tryopen``: any failure to open (corrupt
    bytes, unknown format, etc.) yields False rather than raising.
    """
    if raster_bytes is None:
        return False
    try:
        with _serde.open_tile(bytes(raster_bytes)) as ds:
            # Touch a property so a lazily-opened-but-invalid dataset still trips.
            _ = ds.count
        return True
    except Exception:
        return False


def as_format(ds, new_format: str) -> bytes:
    """Re-encode the raster to another GDAL driver (e.g. PNG, GTiff).

    Mirrors the heavyweight ``gbx_rst_asformat``. Validates the requested
    driver is available in rasterio's bundled GDAL build; raises ValueError
    otherwise. Returns the raster bytes encoded in ``new_format``.
    """
    new_format = str(new_format)
    # raster_driver_extensions maps extension -> driver short name; the value
    # set is the writable raster drivers available in this GDAL build.
    available = set(rasterio.drivers.raster_driver_extensions().values())
    if new_format not in available:
        raise ValueError(
            f"rst_asformat: driver '{new_format}' is not available in this "
            f"GDAL build; available: {', '.join(sorted(available))}"
        )
    data = ds.read()
    profile = ds.profile.copy()
    profile.update(driver=new_format)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def build_overviews(ds, levels, resampling: str = "average") -> bytes:
    """Build internal pyramid overviews at ``levels`` and return GTiff bytes.

    Mirrors the heavyweight ``gbx_rst_buildoverviews``:
      * ``levels`` is a non-empty list of integer decimation factors, each >= 2.
      * ``resampling`` defaults to "average"; mapped to rasterio's Resampling
        enum (near->nearest, cubicspline->cubic_spline, ...).

    Overviews are embedded internally in the GTiff (no .ovr sidecar).
    """
    if levels is None or len(levels) == 0:
        raise ValueError(
            "rst_buildoverviews: levels must be a non-empty integer array "
            "(e.g. [2, 4, 8])"
        )
    levels = [int(level) for level in levels]
    for level in levels:
        if level < 2:
            raise ValueError(
                f"rst_buildoverviews: each level must be >= 2; got {level}"
            )
    resampling = (
        "average" if resampling is None or resampling == "" else str(resampling)
    )
    key = resampling.lower()
    if key not in _OVERVIEW_RESAMPLING_MAP:
        allowed = ", ".join(sorted(_OVERVIEW_RESAMPLING_MAP))
        raise ValueError(
            f"rst_buildoverviews: unsupported resampling '{resampling}'; "
            f"allowed: {allowed}"
        )
    resampling_enum = Resampling[_OVERVIEW_RESAMPLING_MAP[key]]

    data = ds.read()
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    # BuildOverviews needs an r+ dataset; reopening a MemoryFile in update mode
    # mints a fresh vsimem path that does not see the just-written bytes. A
    # round-trip through a real temp file keeps the path stable, so overviews
    # embed internally; then read the bytes (with their .ovr) back.
    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.close()
    try:
        with rasterio.open(tmp.name, "w", **profile) as dst:
            dst.write(data)
        with rasterio.open(tmp.name, "r+") as dst:
            dst.build_overviews(levels, resampling_enum)
            dst.update_tags(ns="rio_overview", resampling=key)
        with open(tmp.name, "rb") as fh:
            return fh.read()
    finally:
        os.unlink(tmp.name)


def sample(ds, geom_wkb: bytes) -> list:
    """Sample per-band raster values at a POINT geometry (WKB).

    Mirrors the heavyweight ``gbx_rst_sample``: requires a POINT geometry
    (raises otherwise), takes (geom.x, geom.y) as a world coordinate already
    aligned to the raster CRS, and returns one Double per band in band order.
    Points outside the raster extent return None.
    """
    geom = shapely.wkb.loads(bytes(geom_wkb))
    if geom.geom_type != "Point":
        raise ValueError(f"rst_sample requires a POINT geometry; got {geom.geom_type}")
    values = list(ds.sample([(geom.x, geom.y)]))[0]
    return [float(v) for v in values]
