"""pyrx public API — Arrow-UDF Column wrappers (signatures mirror rasterx).

Swap-compatible with ``databricks.labs.gbx.rasterx.functions``:
    from databricks.labs.gbx.pyrx import functions as prx
    df.select(prx.rst_width("tile"))
"""

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx._udf import (
    ColLike,
    _col,
    _raster_field,
    sql_scalar_udf,
    sql_scalar_udf2,
    tile_scalar_udf,
    tile_scalar_udf2,
)
from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import agg as agg_core
from databricks.labs.gbx.pyrx.core import coords
from databricks.labs.gbx.pyrx.core import derivedband as derivedband_core
from databricks.labs.gbx.pyrx.core import edit, features, focal, gridagg, indices
from databricks.labs.gbx.pyrx.core import mapalgebra as mapalgebra_core
from databricks.labs.gbx.pyrx.core import ops as ops_core
from databricks.labs.gbx.pyrx.core import resample, terrain, tiling, warp, xyz


def register(spark: SparkSession = None) -> None:
    """Explicitly register the pyrx functions as Spark SQL functions.

    Installs the same ``gbx_rst_*`` SQL names the heavyweight rasterx package
    uses, but powered by the pyspark/rasterio implementation (no JAR). Call
    this once, consciously, when you want to use the functions from SQL —
    exactly like heavyweight ``rasterx.functions.register``. The Python Column
    API (``prx.rst_width(col)``) works WITHOUT this call.

    You register the lightweight OR the heavyweight package in a given session;
    they share the ``gbx_rst_*`` names, so the last registration wins.

    Args:
        spark: Spark session (uses the active session if not provided).
    """
    from databricks.labs.gbx.pyrx import _env

    _env.assert_rasterio_available()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    for name, udf_obj in SQL_REGISTRY.items():
        spark.udf.register(name, udf_obj)


# --- Module-level UDF singletons (built once at import) ---------------------
_u_width = tile_scalar_udf(accessors.width, IntegerType())
_u_height = tile_scalar_udf(accessors.height, IntegerType())
_u_numbands = tile_scalar_udf(accessors.numbands, IntegerType())
_u_srid = tile_scalar_udf(accessors.srid, IntegerType())
_u_pixelwidth = tile_scalar_udf(accessors.pixelwidth, DoubleType())
_u_pixelheight = tile_scalar_udf(accessors.pixelheight, DoubleType())
_u_upperleftx = tile_scalar_udf(accessors.upperleftx, DoubleType())
_u_upperlefty = tile_scalar_udf(accessors.upperlefty, DoubleType())
_u_boundingbox = tile_scalar_udf(accessors.boundingbox, BinaryType())
_u_scalex = tile_scalar_udf(accessors.scalex, DoubleType())
_u_scaley = tile_scalar_udf(accessors.scaley, DoubleType())
_u_isempty = tile_scalar_udf(accessors.isempty, BooleanType())
_u_type = tile_scalar_udf(accessors.type, ArrayType(StringType()))
_u_getnodata = tile_scalar_udf(accessors.getnodata, ArrayType(DoubleType()))


# metadata: pandas_udf rejects MapType in some Arrow builds; fall back to
# a regular Python UDF for this one function only.
@f.udf(MapType(StringType(), StringType()))
def _metadata_udf(raster):
    if raster is None:
        return None
    from databricks.labs.gbx.pyrx import _env, _serde

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(raster)) as ds:
        return accessors.metadata(ds)


_u_r2w_x = tile_scalar_udf2(coords.raster_to_world_x, DoubleType())
_u_r2w_y = tile_scalar_udf2(coords.raster_to_world_y, DoubleType())
_u_w2r_x = tile_scalar_udf2(coords.world_to_raster_x, IntegerType())
_u_w2r_y = tile_scalar_udf2(coords.world_to_raster_y, IntegerType())


# --- Group 1: per-band statistics & accessor UDFs ---------------------------
_u_avg = tile_scalar_udf(accessors.avg, ArrayType(DoubleType()))
_u_min = tile_scalar_udf(accessors.minimum, ArrayType(DoubleType()))
_u_max = tile_scalar_udf(accessors.maximum, ArrayType(DoubleType()))
_u_median = tile_scalar_udf(accessors.median, ArrayType(DoubleType()))
_u_pixelcount = tile_scalar_udf(accessors.pixelcount, ArrayType(LongType()))
_u_rotation = tile_scalar_udf(accessors.rotation, DoubleType())
_u_skewx = tile_scalar_udf(accessors.skewx, DoubleType())
_u_skewy = tile_scalar_udf(accessors.skewy, DoubleType())
_u_format = tile_scalar_udf(accessors.format, StringType())


# memsize: works off the raw raster bytes (no rasterio open needed) — mirror
# heavyweight which returns the in-memory buffer length.
@f.udf(LongType())
def _memsize_udf(raster):
    if raster is None:
        return None
    return int(len(bytes(raster)))


# Struct-accepting memsize for SQL registration: reads the raster byte length
# from the tile struct directly (no rasterio open).
@f.udf(LongType())
def _memsize_struct_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    return int(len(bytes(tile["raster"])))


# MapType return paths use plain @f.udf (pandas_udf rejects MapType on some
# Arrow builds), matching the existing _metadata_udf fallback.
@f.udf(MapType(StringType(), DoubleType()))
def _georeference_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.georeference(ds)


@f.udf(MapType(StringType(), StringType()))
def _bandmetadata_udf(tile, band):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.bandmetadata(ds, int(band))


@f.udf(MapType(StringType(), StringType()))
def _subdatasets_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.subdatasets(ds)


@f.udf(StringType())
def _summary_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.summary(ds)


@f.udf(MapType(StringType(), ArrayType(LongType())))
def _histogram_udf(tile, n_buckets, min_val, max_val, include_nodata):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    nb = 256 if n_buckets is None else int(n_buckets)
    lo = None if min_val is None else float(min_val)
    hi = None if max_val is None else float(max_val)
    inc = bool(include_nodata) if include_nodata is not None else False
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.histogram(ds, nb, lo, hi, inc)


@f.udf(_serde.TILE_SCHEMA)
def _getsubdataset_udf(tile, name):
    if tile is None or tile["raster"] is None or name is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = accessors.getsubdataset(ds, str(name))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


# --- Group 2: struct coordinate UDFs ----------------------------------------
_R2W_COORD_SCHEMA = StructType(
    [
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
    ]
)
_W2R_COORD_SCHEMA = StructType(
    [
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
    ]
)


@f.udf(_R2W_COORD_SCHEMA)
def _rastertoworldcoord_udf(tile, x, y):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return coords.raster_to_world_coord(ds, int(x), int(y))


@f.udf(_W2R_COORD_SCHEMA)
def _worldtorastercoord_udf(tile, x, y):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return coords.world_to_raster_coord(ds, float(x), float(y))


# --- Constructor ------------------------------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _fromcontent_udf(raster, drv):
    if raster is None:
        return None
    return _serde.build_tile(bytes(raster), drv or "GTiff")


def rst_fromcontent(content: ColLike, driver: ColLike) -> Column:
    """Build a tile struct from raster BINARY content and GDAL driver name."""
    return _fromcontent_udf(_col(content), _col(driver))


# --- Tier 1b: tile-returning warp UDFs -------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _transform_udf(tile, target_srid):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = warp.reproject_to_srid(ds, int(target_srid))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _to_webmercator_udf(tile, resampling):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = warp.reproject_to_srid(ds, 3857, resampling=str(resampling))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_transform(tile: ColLike, target_srid: ColLike) -> Column:
    """Reproject the raster to the target SRID (EPSG code)."""
    return _transform_udf(_col(tile), _col(target_srid))


def rst_to_webmercator(tile: ColLike, resampling: ColLike = "bilinear") -> Column:
    """Reproject the tile to EPSG:3857 (web mercator). resampling defaults to 'bilinear'."""
    resampling_col = (
        f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    )
    return _to_webmercator_udf(_col(tile), resampling_col)


# --- Tier 1c: tile-returning resample UDFs ----------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _resample_udf(tile, factor, algorithm):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = resample.resample_by_factor(ds, float(factor), str(algorithm))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _resample_to_size_udf(tile, width_px, height_px, algorithm):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = resample.resample_to_size(
            ds, int(width_px), int(height_px), str(algorithm)
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _resample_to_res_udf(tile, x_res, y_res, algorithm):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = resample.resample_to_res(
            ds, float(x_res), float(y_res), str(algorithm)
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_resample(
    tile: ColLike, factor: ColLike, algorithm: ColLike = "bilinear"
) -> Column:
    """Resample a raster tile by a multiplicative factor (>1 upsamples, 0<factor<1 downsamples).

    CRS and geographic extent are preserved; only the pixel grid changes.
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    return _resample_udf(_col(tile), _col(factor), alg)


def rst_resample_to_size(
    tile: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    algorithm: ColLike = "bilinear",
) -> Column:
    """Resample a raster tile to exact pixel dimensions (width_px x height_px).

    CRS and geographic extent are preserved; only the pixel grid changes.
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    return _resample_to_size_udf(_col(tile), _col(width_px), _col(height_px), alg)


def rst_resample_to_res(
    tile: ColLike,
    x_res: ColLike,
    y_res: ColLike,
    algorithm: ColLike = "bilinear",
) -> Column:
    """Resample a raster tile to a target ground resolution in CRS units.

    CRS and geographic extent are preserved; pixel count is derived from extent / resolution.
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    return _resample_to_res_udf(_col(tile), _col(x_res), _col(y_res), alg)


# --- Tier 1d: tile-returning edit UDFs -------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _clip_udf(tile, geom_wkb, all_touched):
    if tile is None or tile["raster"] is None or geom_wkb is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.clip_to_geom(ds, bytes(geom_wkb), bool(all_touched))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _update_type_udf(tile, new_type):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.update_type(ds, str(new_type))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _init_nodata_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.init_nodata(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_clip(tile: ColLike, clip: ColLike, cutline_all_touched: ColLike) -> Column:
    """Clip the raster to a geometry (WKB). cutline_all_touched includes pixels touched by the boundary."""
    return _clip_udf(_col(tile), _col(clip), _col(cutline_all_touched))


def rst_updatetype(tile: ColLike, new_type: ColLike) -> Column:
    """Cast all raster bands to a new GDAL data type (e.g. 'Int32', 'Float64')."""
    return _update_type_udf(_col(tile), _col(new_type))


def rst_initnodata(tile: ColLike) -> Column:
    """Ensure a NoData value is set on the raster tile; uses -9999.0 if not already set."""
    return _init_nodata_udf(_col(tile))


# --- Tier 1d6: operations UDFs (tryopen, setsrid, band, asformat, ----------
# buildoverviews, sample) ----------------------------------------------------
@f.udf(BooleanType())
def _tryopen_udf(tile):
    if tile is None or tile["raster"] is None:
        return False
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    return ops_core.try_open(bytes(tile["raster"]))


@f.udf(_serde.TILE_SCHEMA)
def _setsrid_udf(tile, srid):
    if tile is None or tile["raster"] is None or srid is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.set_srid(ds, int(srid))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _band_udf(tile, band_index):
    if tile is None or tile["raster"] is None or band_index is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.band(ds, int(band_index))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _asformat_udf(tile, new_format):
    if tile is None or tile["raster"] is None or new_format is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = ops_core.as_format(ds, str(new_format))
    # metadata.driver must reflect the requested output format.
    return _serde.build_tile(new_bytes, str(new_format), tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _buildoverviews_udf(tile, levels, resampling):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    resamp = "average" if resampling is None else str(resampling)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = ops_core.build_overviews(ds, list(levels), resamp)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(ArrayType(DoubleType()))
def _sample_udf(tile, geom_wkb):
    if tile is None or tile["raster"] is None or geom_wkb is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return ops_core.sample(ds, bytes(geom_wkb))


def rst_tryopen(tile: ColLike) -> Column:
    """Return BOOLEAN: True if the raster bytes open as a valid dataset.

    Mirrors the heavyweight ``gbx_rst_tryopen`` — any failure to open (corrupt
    bytes, unknown format) yields False rather than raising.
    """
    return _tryopen_udf(_col(tile))


def rst_setsrid(tile: ColLike, srid: ColLike) -> Column:
    """Stamp the CRS as ``EPSG:<srid>`` WITHOUT reprojecting the pixels.

    Equivalent to ``gdal_edit.py -a_srs``: pixel values and the GeoTransform
    are unchanged; only the CRS metadata is rewritten. Use ``rst_transform``
    for an actual reprojecting warp.

    Args:
        tile: Tile struct column.
        srid: Positive EPSG code to stamp.

    Returns:
        Tile with the same pixels/transform but CRS = EPSG:srid.
    """
    return _setsrid_udf(_col(tile), _col(srid))


def rst_band(tile: ColLike, band_index: ColLike) -> Column:
    """Extract a single 1-based band as a new single-band tile.

    Equivalent to ``gdal_translate -b <band_index>``: the extracted tile
    preserves the source CRS, GeoTransform, nodata, and dtype; only the band
    count is reduced to 1. ``band_index`` is 1-based and must be in range.

    Args:
        tile:       Tile struct column.
        band_index: 1-based band index in ``[1 .. numbands]``.

    Returns:
        Single-band tile struct.
    """
    return _band_udf(_col(tile), _col(band_index))


def rst_asformat(tile: ColLike, new_format: ColLike) -> Column:
    """Re-encode the raster to another GDAL driver (e.g. 'PNG', 'GTiff').

    Mirrors the heavyweight ``gbx_rst_asformat``: the output tile's raster
    bytes are encoded in ``new_format`` and the tile metadata ``driver``
    reflects it. Raises if the requested driver is unavailable in this GDAL
    build.

    Args:
        tile:       Tile struct column.
        new_format: GDAL driver short name (e.g. 'GTiff', 'PNG').

    Returns:
        Tile struct whose raster bytes are encoded in ``new_format``.
    """
    fmt = f.lit(new_format) if isinstance(new_format, str) else _col(new_format)
    return _asformat_udf(_col(tile), fmt)


def rst_buildoverviews(
    tile: ColLike, levels: ColLike, resampling: ColLike = "average"
) -> Column:
    """Build internal pyramid overviews at the given decimation ``levels``.

    Mirrors the heavyweight ``gbx_rst_buildoverviews``: ``levels`` is a
    non-empty array of integer decimation factors, each >= 2; ``resampling``
    defaults to "average" (one of near, average, rms, gauss, cubic,
    cubicspline, lanczos, bilinear, mode). Overviews are embedded internally
    in the output GTiff (no .ovr sidecar).

    Args:
        tile:       Tile struct column.
        levels:     ARRAY<INT> of decimation factors (e.g. ``f.array(...)``).
        resampling: Overview resampling algorithm. Defaults to "average".

    Returns:
        Tile struct with internal overviews embedded.
    """
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    return _buildoverviews_udf(_col(tile), _col(levels), resamp)


def rst_sample(tile: ColLike, geom_wkb: ColLike) -> Column:
    """Sample per-band raster values at a POINT geometry (WKB).

    Mirrors the heavyweight ``gbx_rst_sample``: requires a POINT geometry
    (raises otherwise), uses (geom.x, geom.y) as a world coordinate already
    aligned to the raster CRS, and returns ARRAY<DOUBLE> with one value per
    band in band order. Points outside the raster extent return null.

    Args:
        tile:     Tile struct column.
        geom_wkb: POINT geometry as WKB bytes.

    Returns:
        ARRAY<DOUBLE>: one value per band, or null if the point is out of extent.
    """
    return _sample_udf(_col(tile), _col(geom_wkb))


# --- Tier 1d3: band-math / focal UDFs --------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _threshold_udf(tile, op, value):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.threshold(ds, op, value)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _filter_udf(tile, kernel_size, operation):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = focal.filt(ds, int(kernel_size), str(operation))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _convolve_udf(tile, kernel):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = focal.convolve(ds, kernel)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_threshold(tile: ColLike, op: ColLike = None, value: ColLike = None) -> Column:
    """Keep pixels satisfying the comparison; others become NoData.

    Args:
        tile:  Tile struct column.
        op:    Comparison operator: ">", "<", ">=", "<=", "==", "!=".
               Defaults to ">".
        value: Threshold scalar.  Defaults to 0.0.

    Returns:
        Tile with the same dtype and band count; failing pixels set to NoData.
    """
    op_col = (
        f.lit(">") if op is None else (f.lit(op) if isinstance(op, str) else _col(op))
    )
    val_col = (
        f.lit(0.0)
        if value is None
        else (f.lit(value) if isinstance(value, (int, float)) else _col(value))
    )
    return _threshold_udf(_col(tile), op_col, val_col)


def rst_filter(tile: ColLike, kernel_size: ColLike, operation: ColLike) -> Column:
    """Apply a focal filter over a square window per band.

    Args:
        tile:        Tile struct column.
        kernel_size: Side length of the square neighbourhood (odd integer).
        operation:   One of "min", "max", "mean", "median".

    Returns:
        Filtered tile; same band count.  "mean" returns Float32; others
        preserve the input dtype.
    """
    return _filter_udf(_col(tile), _col(kernel_size), _col(operation))


def rst_convolve(tile: ColLike, kernel: ColLike) -> Column:
    """Convolve each band with a 2-D kernel (ARRAY<ARRAY<DOUBLE>>).

    Args:
        tile:   Tile struct column.
        kernel: 2-D array column of floats (e.g. built with ``f.array``).

    Returns:
        Convolved tile with dtype Float64.
    """
    return _convolve_udf(_col(tile), _col(kernel))


# --- Tier 1d4: map algebra UDF ----------------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _mapalgebra_udf(tiles, expression):
    if tiles is None or expression is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [
        bytes(t["raster"]) for t in tiles if t is not None and t["raster"] is not None
    ]
    if not rasters:
        return None
    new_bytes = mapalgebra_core.mapalgebra(rasters, str(expression))
    return _serde.build_tile(new_bytes, "GTiff", tiles[0]["cellid"])


def rst_mapalgebra(tiles: ColLike, expression: ColLike) -> Column:
    """Apply a map-algebra expression across an array of tiles.

    Band 1 of each tile (in array order) binds to A, B, C, …; the expression is
    evaluated with numexpr (safe math only — no arbitrary code execution).
    Output is a single-band Float32 tile on the first input's georeference.

    Args:
        tiles:      Column of ARRAY<tile struct> (e.g. ``f.array("ta", "tb")``).
        expression: Math expression string, e.g. ``"(A - B) / (A + B)"``.

    Returns:
        Single-band Float32 tile struct.
    """
    expr_col = f.lit(expression) if isinstance(expression, str) else _col(expression)
    return _mapalgebra_udf(_col(tiles), expr_col)


# --- Tier 1d2: spectral index UDFs -----------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _ndvi_udf(tile, red_band, nir_band):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.ndvi(ds, int(red_band), int(nir_band))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _ndwi_udf(tile, green_idx, nir_idx):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.ndwi(ds, int(green_idx), int(nir_idx))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _nbr_udf(tile, nir_idx, swir_idx):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.nbr(ds, int(nir_idx), int(swir_idx))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _savi_udf(tile, red_idx, nir_idx, l_val):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.savi(ds, int(red_idx), int(nir_idx), l=float(l_val))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _evi_udf(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.evi(
            ds,
            int(red_idx),
            int(nir_idx),
            int(blue_idx),
            l=float(l_val),
            c1=float(c1),
            c2=float(c2),
            g=float(g),
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_ndvi(tile: ColLike, red_band: ColLike, nir_band: ColLike) -> Column:
    """Compute NDVI = (NIR - Red) / (NIR + Red); single-band Float32 tile."""
    return _ndvi_udf(_col(tile), _col(red_band), _col(nir_band))


def rst_ndwi(tile: ColLike, green_idx: ColLike, nir_idx: ColLike) -> Column:
    """Compute NDWI = (Green - NIR) / (Green + NIR); single-band Float32 tile."""
    return _ndwi_udf(_col(tile), _col(green_idx), _col(nir_idx))


def rst_nbr(tile: ColLike, nir_idx: ColLike, swir_idx: ColLike) -> Column:
    """Compute NBR = (NIR - SWIR) / (NIR + SWIR); single-band Float32 tile."""
    return _nbr_udf(_col(tile), _col(nir_idx), _col(swir_idx))


def rst_savi(
    tile: ColLike, red_idx: ColLike, nir_idx: ColLike, l: ColLike = 0.5  # noqa: E741
) -> Column:
    """Compute SAVI = (NIR - Red) / (NIR + Red + L) * (1 + L); single-band Float32 tile."""
    l_col = f.lit(l) if isinstance(l, (int, float)) else _col(l)
    return _savi_udf(_col(tile), _col(red_idx), _col(nir_idx), l_col)


def rst_evi(  # noqa: E741
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    blue_idx: ColLike,
    l: ColLike = 1.0,
    c1: ColLike = 6.0,
    c2: ColLike = 7.5,
    g: ColLike = 2.5,
) -> Column:
    """Compute EVI = G * (NIR - Red) / (NIR + C1*Red - C2*Blue + L); single-band Float32 tile."""
    l_col = f.lit(l) if isinstance(l, (int, float)) else _col(l)
    c1_col = f.lit(c1) if isinstance(c1, (int, float)) else _col(c1)
    c2_col = f.lit(c2) if isinstance(c2, (int, float)) else _col(c2)
    g_col = f.lit(g) if isinstance(g, (int, float)) else _col(g)
    return _evi_udf(
        _col(tile),
        _col(red_idx),
        _col(nir_idx),
        _col(blue_idx),
        l_col,
        c1_col,
        c2_col,
        g_col,
    )


# --- Tier 1e: constructor + fill UDFs (vector bridge) -----------------------
@f.udf(_serde.TILE_SCHEMA)
def _rasterize_udf(geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, srid):
    if geom_wkb is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    new_bytes = features.rasterize_geom(
        bytes(geom_wkb), value, xmin, ymin, xmax, ymax, width_px, height_px, srid
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_rasterize(
    geom_wkb: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
) -> Column:
    """Burn a geometry (WKB) into a new raster tile at the given extent/size/SRID."""
    return _rasterize_udf(
        _col(geom_wkb),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
    )


@f.udf(_serde.TILE_SCHEMA)
def _fillnodata_udf(tile, max_search_dist, smoothing_iter):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = features.fill_nodata(ds, max_search_dist, smoothing_iter)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_fillnodata(
    tile: ColLike,
    max_search_dist: ColLike = None,
    smoothing_iter: ColLike = None,
) -> Column:
    """Interpolate across NoData gaps in the raster."""
    msd = f.lit(None) if max_search_dist is None else _col(max_search_dist)
    smi = f.lit(None) if smoothing_iter is None else _col(smoothing_iter)
    return _fillnodata_udf(_col(tile), msd, smi)


_POLYGONIZE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("geom_wkb", BinaryType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
        ]
    )
)


@f.udf(_POLYGONIZE_SCHEMA)
def _polygonize_udf(tile, band, connectedness):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        pairs = features.polygonize(ds, int(band), int(connectedness))
    return [{"geom_wkb": g, "value": v} for g, v in pairs]


def rst_polygonize(
    tile: ColLike, band: ColLike = 1, connectedness: ColLike = 4
) -> Column:
    """Extract vector polygons from a raster's contiguous equal-value regions.

    Returns ARRAY<struct(geom_wkb BINARY, value DOUBLE)>; NoData excluded.
    """
    return _polygonize_udf(_col(tile), _col(band), _col(connectedness))


# --- Tier 1e2: tiling UDFs (separatebands, retile, tooverlappingtiles) ------
@f.udf(ArrayType(_serde.TILE_SCHEMA))
def _separatebands_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        parts = tiling.separate_bands(ds)
    return [_serde.build_tile(b, "GTiff", i) for i, b in enumerate(parts)]


@f.udf(ArrayType(_serde.TILE_SCHEMA))
def _retile_udf(tile, tile_width, tile_height):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        parts = tiling.retile(ds, int(tile_width), int(tile_height))
    return [_serde.build_tile(b, "GTiff", i) for i, b in enumerate(parts)]


@f.udf(ArrayType(_serde.TILE_SCHEMA))
def _tooverlappingtiles_udf(tile, tile_width, tile_height, overlap):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        parts = tiling.to_overlapping_tiles(
            ds, int(tile_width), int(tile_height), int(overlap)
        )
    return [_serde.build_tile(b, "GTiff", i) for i, b in enumerate(parts)]


def rst_separatebands(tile: ColLike) -> Column:
    """Split a multi-band tile into an array of single-band tiles.

    Returns ARRAY<tile struct>; explode the result to get one row per band.
    Each output tile carries the same georeferencing and CRS as the input.
    """
    return _separatebands_udf(_col(tile))


def rst_retile(tile: ColLike, tile_width: ColLike, tile_height: ColLike) -> Column:
    """Partition a tile into non-overlapping sub-tiles of the given pixel size.

    Edge tiles are narrower/shorter when the raster dimensions are not exact
    multiples of tile_width/tile_height.  Returns ARRAY<tile struct>; explode
    the result to get one row per sub-tile.  Each output tile carries the
    correct windowed transform and CRS.
    """
    return _retile_udf(_col(tile), _col(tile_width), _col(tile_height))


def rst_tooverlappingtiles(
    tile: ColLike,
    tile_width: ColLike,
    tile_height: ColLike,
    overlap: ColLike,
) -> Column:
    """Partition a tile into overlapping sub-tiles.

    Each tile is tile_width x tile_height pixels; neighboring tiles share
    *overlap* pixels on each shared edge (step = tile_width - overlap).
    Edge tiles are clamped to the raster boundary.  Returns ARRAY<tile struct>;
    explode the result to get one row per sub-tile.
    """
    return _tooverlappingtiles_udf(
        _col(tile), _col(tile_width), _col(tile_height), _col(overlap)
    )


@f.udf(ArrayType(_serde.TILE_SCHEMA))
def _maketiles_udf(tile, size_in_mb):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        parts = tiling.make_tiles(ds, float(size_in_mb))
    return [_serde.build_tile(b, "GTiff", i) for i, b in enumerate(parts)]


def rst_maketiles(tile: ColLike, size_in_mb: ColLike) -> Column:
    """Split a raster into an array of tiles of approximately size_in_mb each.

    Derives a square tile side from the target MB budget and the raster's
    bytes-per-pixel, then partitions the raster into non-overlapping sub-tiles.
    Returns ARRAY<tile struct>; explode the result to get one row per sub-tile.
    Each output tile carries the correct windowed transform and CRS.
    """
    return _maketiles_udf(_col(tile), _col(size_in_mb))


# --- Tier 1f: terrain UDFs (slope, aspect, hillshade) ----------------------
@f.udf(_serde.TILE_SCHEMA)
def _slope_udf(tile, unit, scale):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.slope(ds, unit=str(unit), scale=float(scale))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _aspect_udf(tile, trigonometric, zero_for_flat):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.aspect(
            ds, trigonometric=bool(trigonometric), zero_for_flat=bool(zero_for_flat)
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _hillshade_udf(tile, azimuth, altitude, z_factor):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.hillshade(
            ds,
            azimuth=float(azimuth),
            altitude=float(altitude),
            z_factor=float(z_factor),
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_slope(tile: ColLike, unit: ColLike = "degrees", scale: ColLike = 1.0) -> Column:
    """Compute terrain slope from a single-band DEM tile (Horn's 3x3 method).

    Args:
        tile:  Tile struct column containing a single-band DEM raster.
        unit:  ``"degrees"`` (default) or ``"percent"``.
        scale: Ratio of vertical to horizontal units (default 1.0).
               Use ~111120 for geographic-degree grids.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    unit_col = f.lit(unit) if isinstance(unit, str) else _col(unit)
    scale_col = f.lit(scale) if isinstance(scale, (int, float)) else _col(scale)
    return _slope_udf(_col(tile), unit_col, scale_col)


def rst_aspect(
    tile: ColLike,
    trigonometric: ColLike = False,
    zero_for_flat: ColLike = False,
) -> Column:
    """Compute terrain aspect from a single-band DEM tile (Horn's 3x3 method).

    Default output is compass degrees: 0 = North, increasing clockwise.
    Flat cells are -9999 unless zero_for_flat is True.

    Args:
        tile:           Tile struct column containing a single-band DEM raster.
        trigonometric:  Return math-convention (CCW from east) instead of compass.
        zero_for_flat:  Return 0 for flat cells instead of -9999.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    trig_col = (
        f.lit(trigonometric) if isinstance(trigonometric, bool) else _col(trigonometric)
    )
    zff_col = (
        f.lit(zero_for_flat) if isinstance(zero_for_flat, bool) else _col(zero_for_flat)
    )
    return _aspect_udf(_col(tile), trig_col, zff_col)


def rst_hillshade(
    tile: ColLike,
    azimuth: ColLike = 315.0,
    altitude: ColLike = 45.0,
    z_factor: ColLike = 1.0,
) -> Column:
    """Compute hillshade from a single-band DEM tile (Horn's 3x3 method).

    Args:
        tile:      Tile struct column containing a single-band DEM raster.
        azimuth:   Sun azimuth in degrees (default 315 = NW).
        altitude:  Sun elevation above horizon in degrees (default 45).
        z_factor:  Vertical exaggeration applied to gradients (default 1.0).

    Returns:
        Single-band Byte (uint8) tile; values 0..255.
    """
    az_col = f.lit(azimuth) if isinstance(azimuth, (int, float)) else _col(azimuth)
    alt_col = f.lit(altitude) if isinstance(altitude, (int, float)) else _col(altitude)
    zf_col = f.lit(z_factor) if isinstance(z_factor, (int, float)) else _col(z_factor)
    return _hillshade_udf(_col(tile), az_col, alt_col, zf_col)


# --- Tier 1g: terrain ruggedness UDFs (tri, tpi, roughness) -----------------
@f.udf(_serde.TILE_SCHEMA)
def _tri_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.tri(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _tpi_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.tpi(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _roughness_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.roughness(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_tri(tile: ColLike) -> Column:
    """Compute Terrain Ruggedness Index (TRI) from a single-band DEM tile.

    TRI = mean of the absolute differences between the center cell and each of
    its 8 neighbours (Wilson 2007).  Flat terrain yields 0.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _tri_udf(_col(tile))


def rst_tpi(tile: ColLike) -> Column:
    """Compute Topographic Position Index (TPI) from a single-band DEM tile.

    TPI = center - mean(8 neighbours).  Positive = local high; negative = local
    low; flat terrain yields 0.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _tpi_udf(_col(tile))


def rst_roughness(tile: ColLike) -> Column:
    """Compute terrain roughness from a single-band DEM tile.

    Roughness = max(3x3 window) - min(3x3 window).  Flat terrain yields 0.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _roughness_udf(_col(tile))


@f.udf(_serde.TILE_SCHEMA)
def _color_relief_udf(tile, color_table_path):
    if tile is None or tile["raster"] is None or color_table_path is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.color_relief(ds, str(color_table_path))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_color_relief(tile: ColLike, color_table_path: ColLike) -> Column:
    """Map a single-band DEM through a gdaldem color table to an RGB(A) Byte tile.

    Reads a gdaldem-style color file (elevation R G B [A] per line; ``nv`` for
    NoData pixels; ``<n>%`` for percentage of the band range).  Applies linear
    interpolation per channel.

    Args:
        tile:             Tile struct column containing a single-band DEM raster.
        color_table_path: Column or string path to a gdaldem color file.

    Returns:
        3-band (RGB) or 4-band (RGBA) Byte tile.
    """
    ctp = (
        f.lit(color_table_path)
        if isinstance(color_table_path, str)
        else _col(color_table_path)
    )
    return _color_relief_udf(_col(tile), ctp)


# --- Tier 0: accessors ------------------------------------------------------
def rst_width(tile: ColLike) -> Column:
    return _u_width(_raster_field(_col(tile)))


def rst_height(tile: ColLike) -> Column:
    return _u_height(_raster_field(_col(tile)))


def rst_numbands(tile: ColLike) -> Column:
    return _u_numbands(_raster_field(_col(tile)))


def rst_srid(tile: ColLike) -> Column:
    return _u_srid(_raster_field(_col(tile)))


def rst_pixelwidth(tile: ColLike) -> Column:
    return _u_pixelwidth(_raster_field(_col(tile)))


def rst_pixelheight(tile: ColLike) -> Column:
    return _u_pixelheight(_raster_field(_col(tile)))


def rst_upperleftx(tile: ColLike) -> Column:
    return _u_upperleftx(_raster_field(_col(tile)))


def rst_upperlefty(tile: ColLike) -> Column:
    return _u_upperlefty(_raster_field(_col(tile)))


def rst_boundingbox(tile: ColLike) -> Column:
    return _u_boundingbox(_raster_field(_col(tile)))


def rst_metadata(tile: ColLike) -> Column:
    return _metadata_udf(_raster_field(_col(tile)))


def rst_scalex(tile: ColLike) -> Column:
    return _u_scalex(_raster_field(_col(tile)))


def rst_scaley(tile: ColLike) -> Column:
    return _u_scaley(_raster_field(_col(tile)))


def rst_isempty(tile: ColLike) -> Column:
    return _u_isempty(_raster_field(_col(tile)))


def rst_type(tile: ColLike) -> Column:
    """Return the GDAL data-type name per band (e.g. ['Float32', 'Float32'])."""
    return _u_type(_raster_field(_col(tile)))


def rst_getnodata(tile: ColLike) -> Column:
    """Return the NoData value per band as an array of doubles, or null if not set."""
    return _u_getnodata(_raster_field(_col(tile)))


# --- Tier 1: coordinate transforms -----------------------------------------
def rst_rastertoworldcoordx(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    return _u_r2w_x(_raster_field(_col(tile)), _col(pixel_x), _col(pixel_y))


def rst_rastertoworldcoordy(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    return _u_r2w_y(_raster_field(_col(tile)), _col(pixel_x), _col(pixel_y))


def rst_worldtorastercoordx(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    return _u_w2r_x(_raster_field(_col(tile)), _col(world_x), _col(world_y))


def rst_worldtorastercoordy(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    return _u_w2r_y(_raster_field(_col(tile)), _col(world_x), _col(world_y))


def rst_rastertoworldcoord(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    """World coordinate of pixel (x=col, y=row) as STRUCT<x: DOUBLE, y: DOUBLE>."""
    return _rastertoworldcoord_udf(_col(tile), _col(x), _col(y))


def rst_worldtorastercoord(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    """Pixel (col, row) containing world (x, y) as STRUCT<x: INT, y: INT>."""
    return _worldtorastercoord_udf(_col(tile), _col(x), _col(y))


# --- Group 1: per-band statistics & accessors -------------------------------
def rst_avg(tile: ColLike) -> Column:
    """Per-band mean of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NaN.
    """
    return _u_avg(_raster_field(_col(tile)))


def rst_min(tile: ColLike) -> Column:
    """Per-band minimum of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NaN.
    """
    return _u_min(_raster_field(_col(tile)))


def rst_max(tile: ColLike) -> Column:
    """Per-band maximum of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NaN.
    """
    return _u_max(_raster_field(_col(tile)))


def rst_median(tile: ColLike) -> Column:
    """Per-band median of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NaN.
    """
    return _u_median(_raster_field(_col(tile)))


def rst_pixelcount(tile: ColLike) -> Column:
    """Per-band count of valid (non-NoData) pixels; ARRAY<LONG>.

    Empty / all-invalid bands return 0.
    """
    return _u_pixelcount(_raster_field(_col(tile)))


def rst_memsize(tile: ColLike) -> Column:
    """Serialized size of the raster in bytes (length of the raster buffer); LONG."""
    return _memsize_udf(_raster_field(_col(tile)))


def rst_rotation(tile: ColLike) -> Column:
    """Rotation angle = atan(skewY / scaleX) in radians; DOUBLE."""
    return _u_rotation(_raster_field(_col(tile)))


def rst_skewx(tile: ColLike) -> Column:
    """X skew of the geotransform (gt2); DOUBLE."""
    return _u_skewx(_raster_field(_col(tile)))


def rst_skewy(tile: ColLike) -> Column:
    """Y skew of the geotransform (gt4); DOUBLE."""
    return _u_skewy(_raster_field(_col(tile)))


def rst_format(tile: ColLike) -> Column:
    """GDAL driver short name of the raster (e.g. 'GTiff'); STRING."""
    return _u_format(_raster_field(_col(tile)))


def rst_georeference(tile: ColLike) -> Column:
    """Geotransform as MAP<STRING,DOUBLE>.

    Keys: upperLeftX, upperLeftY, scaleX, scaleY, skewX, skewY.
    """
    return _georeference_udf(_col(tile))


def rst_bandmetadata(tile: ColLike, band: ColLike) -> Column:
    """Metadata tags of the given 1-based band as MAP<STRING,STRING>."""
    return _bandmetadata_udf(_col(tile), _col(band))


def rst_subdatasets(tile: ColLike) -> Column:
    """Subdataset map as MAP<STRING,STRING>; empty for plain single-dataset rasters."""
    return _subdatasets_udf(_col(tile))


def rst_getsubdataset(tile: ColLike, name: ColLike) -> Column:
    """Extract the named subdataset as a new raster tile struct.

    Raises if no subdataset matches ``name`` (mirrors heavyweight).
    """
    nm = f.lit(name) if isinstance(name, str) else _col(name)
    return _getsubdataset_udf(_col(tile), nm)


def rst_summary(tile: ColLike) -> Column:
    """gdalinfo-style JSON summary string with per-band statistics; STRING.

    The JSON shape is GeoBrix-specific (driver, size, crs, geoTransform, bands
    with min/max/mean/stdDev), not a byte-for-byte ``gdalinfo -json`` match.
    """
    return _summary_udf(_col(tile))


def rst_histogram(
    tile: ColLike,
    n_buckets: ColLike = 256,
    min: ColLike = None,  # noqa: A002 - mirrors heavyweight arg name
    max: ColLike = None,  # noqa: A002
    include_nodata: ColLike = False,
) -> Column:
    """Per-band histogram as MAP<STRING, ARRAY<LONG>> keyed by ``band_<i>``.

    Args:
        tile:           Tile struct column.
        n_buckets:      Number of equal-width buckets across [min, max] (default 256).
        min:            Lower bound; defaults to the band's valid-pixel minimum.
        max:            Upper bound; defaults to the band's valid-pixel maximum.
        include_nodata: Include masked pixels in the binning (default False).

    Values outside [min, max] are dropped (no out-of-range bucket).
    """
    nb = f.lit(n_buckets) if isinstance(n_buckets, int) else _col(n_buckets)
    lo = f.lit(None) if min is None else _col(min)
    hi = f.lit(None) if max is None else _col(max)
    inc = (
        f.lit(include_nodata)
        if isinstance(include_nodata, bool)
        else _col(include_nodata)
    )
    return _histogram_udf(_col(tile), nb, lo, hi, inc)


# --- Tier 1d5: derived-band UDF ---------------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _derivedband_udf(tile, pyfunc, func_name):
    if tile is None or tile["raster"] is None or pyfunc is None or func_name is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = derivedband_core.derivedband(ds, str(pyfunc), str(func_name))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_derivedband(tile_expr: ColLike, pyfunc: ColLike, func_name: ColLike) -> Column:
    """Apply a user-provided Python function to the raster's bands.

    pyfunc follows GDAL's VRT pixel-function signature::

        func(in_ar, out_ar, xoff, yoff, xsize, ysize,
             raster_xsize, raster_ysize, buf_radius, gt, **kwargs)

    where ``in_ar`` is a list of 2-D NumPy arrays (one per input band) and
    ``out_ar`` is a preallocated 2-D output array the function fills in-place
    (``out_ar[:] = ...``). This matches GDAL's Python pixel-function contract,
    so a pyfunc authored for the heavyweight ``rst_derivedband`` works here.

    SECURITY: pyfunc is executed in-process without sandboxing — pass only
    trusted (your own) code, the same trust model as any Spark UDF.

    Args:
        tile_expr: Tile struct column.
        pyfunc:    Python source code (string) defining the function.
        func_name: Name of the callable within ``pyfunc``.

    Returns:
        Single-band Float64 tile struct.
    """
    pf = f.lit(pyfunc) if isinstance(pyfunc, str) else _col(pyfunc)
    fn = f.lit(func_name) if isinstance(func_name, str) else _col(func_name)
    return _derivedband_udf(_col(tile_expr), pf, fn)


# --- Tier 1h: web-mercator XYZ tiling UDFs ---------------------------------
@f.udf(BinaryType())
def _tilexyz_udf(tile, z, x, y, format, size, resampling):
    # Mirror heavyweight: rst_tilexyz NEVER returns null — a null/empty tile or
    # any hard failure yields a transparent PNG (slippy-map servers need a 200).
    sz = int(size) if size is not None else 256
    if tile is None or tile["raster"] is None:
        return xyz.transparent_png(sz)
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    fmt = str(format) if format is not None else "PNG"
    resamp = str(resampling) if resampling is not None else "bilinear"
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return xyz.render_tile(ds, int(z), int(x), int(y), fmt, sz, resamp)


_XYZPYRAMID_SCHEMA = ArrayType(
    StructType(
        [
            StructField("z", IntegerType(), False),
            StructField("x", IntegerType(), False),
            StructField("y", IntegerType(), False),
            StructField("bytes", BinaryType(), True),
        ]
    )
)


@f.udf(_XYZPYRAMID_SCHEMA)
def _xyzpyramid_udf(tile, min_z, max_z, format, size, resampling):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    fmt = str(format) if format is not None else "PNG"
    sz = int(size) if size is not None else 256
    resamp = str(resampling) if resampling is not None else "bilinear"
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return xyz.pyramid(ds, int(min_z), int(max_z), fmt, sz, resamp)


def rst_tilexyz(
    tile: ColLike,
    z: ColLike,
    x: ColLike,
    y: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
) -> Column:
    """Render a single web-mercator XYZ slippy-map tile from the raster.

    Warps the source into a ``size`` x ``size`` raster covering exactly the
    EPSG:3857 bbox of (z, x, y) and encodes it to the requested image format.

    Args:
        tile:       Tile struct column.
        z, x, y:    Web-mercator tile coordinates (Y north-down slippy-map).
        format:     "PNG" (default), "JPEG", or "WEBP" (case-insensitive).
        size:       Output tile side in pixels, in (0, 4096]. Default 256.
        resampling: GDAL warp resampling name (near, bilinear (default), cubic,
                    cubicspline, lanczos, average, mode, max, min, med, q1, q3).

    Returns:
        BINARY image bytes. Out-of-extent / empty tiles (and any hard failure)
        return a transparent RGBA PNG of ``size`` x ``size`` — NEVER null — so
        slippy-map servers always get a 200-status body.
    """
    fmt = f.lit(format) if isinstance(format, str) else _col(format)
    sz = f.lit(size) if isinstance(size, int) else _col(size)
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    return _tilexyz_udf(_col(tile), _col(z), _col(x), _col(y), fmt, sz, resamp)


def rst_xyzpyramid(
    tile: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
) -> Column:
    """Render every web-mercator XYZ tile intersecting the raster across a zoom range.

    Computes the source extent in WGS84, enumerates intersecting (z, x, y) tiles
    for each zoom in [min_z, max_z] (WebMercatorQuad TMS, Y north-down), and
    renders each via the same path as :func:`rst_tilexyz`.

    Args:
        tile:       Tile struct column.
        min_z:      Minimum zoom (>= 0).
        max_z:      Maximum zoom (>= min_z, <= 20).
        format:     "PNG" (default), "JPEG", or "WEBP".
        size:       Output tile side in pixels, in (0, 4096]. Default 256.
        resampling: GDAL warp resampling name (default "bilinear").

    Returns:
        ARRAY<struct(z INT, x INT, y INT, bytes BINARY)>, one element per
        intersecting tile. Explode the array to get one row per tile. Raises if
        the candidate tile-count across the range exceeds 1,000,000.
    """
    fmt = f.lit(format) if isinstance(format, str) else _col(format)
    sz = f.lit(size) if isinstance(size, int) else _col(size)
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    return _xyzpyramid_udf(_col(tile), _col(min_z), _col(max_z), fmt, sz, resamp)


# --- Tier 1i: raster->grid aggregation UDFs (h3 + quadbin) -----------------
# Output: ARRAY (per band) of ARRAY of struct(cellID LONG, measure <T>).
# measure is INTEGER for the count variants, DOUBLE for avg/min/max/median.
def _grid_struct_schema(measure_type):
    return ArrayType(
        ArrayType(
            StructType(
                [
                    StructField("cellID", LongType(), True),
                    StructField("measure", measure_type, True),
                ]
            )
        )
    )


_GRID_DOUBLE_SCHEMA = _grid_struct_schema(DoubleType())
_GRID_INT_SCHEMA = _grid_struct_schema(IntegerType())


def _make_rastertogrid_udf(grid, agg, schema):
    @f.udf(schema)
    def _udf(tile, resolution):
        if tile is None or tile["raster"] is None:
            return None
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            return gridagg.raster_to_grid(ds, int(resolution), grid, agg)

    return _udf


_u_h3_rastertogridavg = _make_rastertogrid_udf("h3", "avg", _GRID_DOUBLE_SCHEMA)
_u_h3_rastertogridcount = _make_rastertogrid_udf("h3", "count", _GRID_INT_SCHEMA)
_u_h3_rastertogridmax = _make_rastertogrid_udf("h3", "max", _GRID_DOUBLE_SCHEMA)
_u_h3_rastertogridmin = _make_rastertogrid_udf("h3", "min", _GRID_DOUBLE_SCHEMA)
_u_h3_rastertogridmedian = _make_rastertogrid_udf("h3", "median", _GRID_DOUBLE_SCHEMA)
_u_quadbin_rastertogridavg = _make_rastertogrid_udf(
    "quadbin", "avg", _GRID_DOUBLE_SCHEMA
)
_u_quadbin_rastertogridcount = _make_rastertogrid_udf(
    "quadbin", "count", _GRID_INT_SCHEMA
)
_u_quadbin_rastertogridmax = _make_rastertogrid_udf(
    "quadbin", "max", _GRID_DOUBLE_SCHEMA
)
_u_quadbin_rastertogridmin = _make_rastertogrid_udf(
    "quadbin", "min", _GRID_DOUBLE_SCHEMA
)
_u_quadbin_rastertogridmedian = _make_rastertogrid_udf(
    "quadbin", "median", _GRID_DOUBLE_SCHEMA
)

_RASTERTOGRID_DOC = """{summary}

    Per band, every valid (non-NoData) pixel is mapped to a {grid} cell at the
    given ``resolution`` via its pixel-centroid world coordinate; the pixel
    values falling in each cell are reduced by {agg_desc}. The raster is
    interpreted as EPSG:4326 lon/lat (no reprojection -- reproject upstream with
    ``rst_transform`` if your source CRS differs).

    Args:
        tile:       Tile struct column.
        resolution: {grid} resolution ({res_range}).

    Returns:
        ARRAY (one element per raster band) of ARRAY of
        ``struct(cellID LONG, measure {measure})``. Explode twice (or index)
        to consume: ``explode`` the outer array for per-band rows, then
        ``explode`` the inner array for one row per cell.
    """


def rst_h3_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    return _u_h3_rastertogridavg(_col(tile), _col(resolution))


def rst_h3_rastertogridcount(tile: ColLike, resolution: ColLike) -> Column:
    return _u_h3_rastertogridcount(_col(tile), _col(resolution))


def rst_h3_rastertogridmax(tile: ColLike, resolution: ColLike) -> Column:
    return _u_h3_rastertogridmax(_col(tile), _col(resolution))


def rst_h3_rastertogridmin(tile: ColLike, resolution: ColLike) -> Column:
    return _u_h3_rastertogridmin(_col(tile), _col(resolution))


def rst_h3_rastertogridmedian(tile: ColLike, resolution: ColLike) -> Column:
    return _u_h3_rastertogridmedian(_col(tile), _col(resolution))


def rst_quadbin_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    return _u_quadbin_rastertogridavg(_col(tile), _col(resolution))


def rst_quadbin_rastertogridcount(tile: ColLike, resolution: ColLike) -> Column:
    return _u_quadbin_rastertogridcount(_col(tile), _col(resolution))


def rst_quadbin_rastertogridmax(tile: ColLike, resolution: ColLike) -> Column:
    return _u_quadbin_rastertogridmax(_col(tile), _col(resolution))


def rst_quadbin_rastertogridmin(tile: ColLike, resolution: ColLike) -> Column:
    return _u_quadbin_rastertogridmin(_col(tile), _col(resolution))


def rst_quadbin_rastertogridmedian(tile: ColLike, resolution: ColLike) -> Column:
    return _u_quadbin_rastertogridmedian(_col(tile), _col(resolution))


rst_h3_rastertogridavg.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by mean, per band.",
    grid="H3",
    agg_desc="their mean (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
)
rst_h3_rastertogridcount.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Count raster pixels falling in each H3 cell, per band.",
    grid="H3",
    agg_desc="a pixel count (INTEGER)",
    res_range="0..15",
    measure="INTEGER",
)
rst_h3_rastertogridmax.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by maximum, per band.",
    grid="H3",
    agg_desc="their maximum (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
)
rst_h3_rastertogridmin.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by minimum, per band.",
    grid="H3",
    agg_desc="their minimum (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
)
rst_h3_rastertogridmedian.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by median, per band.",
    grid="H3",
    agg_desc="their median (DOUBLE; even counts average the two middle values)",
    res_range="0..15",
    measure="DOUBLE",
)
rst_quadbin_rastertogridavg.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by mean, per band.",
    grid="quadbin",
    agg_desc="their mean (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
)
rst_quadbin_rastertogridcount.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Count raster pixels falling in each quadbin cell, per band.",
    grid="quadbin",
    agg_desc="a pixel count (INTEGER)",
    res_range="0..20",
    measure="INTEGER",
)
rst_quadbin_rastertogridmax.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by maximum, per band.",
    grid="quadbin",
    agg_desc="their maximum (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
)
rst_quadbin_rastertogridmin.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by minimum, per band.",
    grid="quadbin",
    agg_desc="their minimum (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
)
rst_quadbin_rastertogridmedian.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by median, per band.",
    grid="quadbin",
    agg_desc="their median (DOUBLE; even counts average the two middle values)",
    res_range="0..20",
    measure="DOUBLE",
)


# ---------------------------------------------------------------------------
# Tier 2: grouped aggregators (rst_*_agg)
# ---------------------------------------------------------------------------
# Spark 4.0 forbids a Python aggregate from returning a StructType, but allows
# a grouped-agg pandas_udf to return BINARY and a scalar UDF to wrap that result
# inside .agg(). So each public aggregator COMPOSES a BINARY grouped-agg UDF
# with a scalar ``as_tile`` UDF, yielding a tile struct transparently while
# preserving the heavyweight call pattern: df.groupBy(k).agg(rx.rst_*_agg(...)).
#
# The grouped-agg UDFs accept the tile STRUCT column directly (Arrow delivers a
# struct column to the pandas_udf as a Series of dict-like rows; we extract each
# member's ``raster`` bytes via row["raster"]). VERIFIED to work for both the
# Python .agg() path and SQL GROUP BY — no raster-bytes fallback needed.


def _tile_raster_bytes(row):
    """Extract raster bytes from a tile-struct row delivered by Arrow.

    Arrow hands a StructType column to a pandas_udf as a Series whose elements
    are dict-like (mapping field name -> value). Returns None for null rows.
    """
    if row is None:
        return None
    raster = row["raster"]
    return None if raster is None else bytes(raster)


# --- scalar as_tile UDFs: wrap an aggregated BINARY result into a tile struct
@f.udf(_serde.TILE_SCHEMA)
def _as_tile_udf(raster_bytes):
    if raster_bytes is None:
        return None
    rb = bytes(raster_bytes)
    if len(rb) == 0:
        return None
    return _serde.build_tile(rb, "GTiff", 0)


# combineavg must carry the group's first cellid through to the output tile.
# Spark forbids mixing a grouped-agg pandas_udf with another aggregate (e.g.
# f.first) in the same .agg(), so we cannot pass the cellid as a sibling
# aggregate. Instead the grouped-agg UDF prepends an 8-byte big-endian cellid
# envelope onto the raster bytes; this scalar UDF strips it back off.
def _as_tile_cellid_envelope_udf_fn(raster_bytes):
    if raster_bytes is None:
        return None
    rb = bytes(raster_bytes)
    if len(rb) < 8:
        return None
    cellid = int.from_bytes(rb[:8], "big", signed=True)
    return _serde.build_tile(rb[8:], "GTiff", cellid)


_as_tile_cellid_envelope_udf = f.udf(_serde.TILE_SCHEMA)(
    _as_tile_cellid_envelope_udf_fn
)


# --- grouped-agg pandas_udf(BinaryType()) reducers --------------------------
@pandas_udf(BinaryType())
def _merge_agg_udf(tile: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [b for b in (_tile_raster_bytes(r) for r in tile) if b is not None]
    return agg_core.merge_tiles(rasters)


@pandas_udf(BinaryType())
def _combineavg_agg_udf(tile: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    cellid = 0
    first = True
    for r in tile:
        rb = _tile_raster_bytes(r)
        if rb is None:
            continue
        if first:
            cid = r["cellid"]
            cellid = int(cid) if cid is not None else 0
            first = False
        rasters.append(rb)
    out = agg_core.combineavg_tiles(rasters)
    if out is None:
        return None
    # Prepend an 8-byte big-endian cellid envelope (stripped by the scalar UDF).
    return cellid.to_bytes(8, "big", signed=True) + bytes(out)


@pandas_udf(BinaryType())
def _combineavg_agg_sql_udf(tile: pd.Series) -> bytes:
    # SQL registration variant: returns raw GTiff bytes (no cellid envelope), so
    # SQL callers can wrap it directly with gbx_rst_fromcontent(<agg>, 'GTiff').
    # SQL has no tile cellid concept beyond the struct, and the envelope would
    # corrupt fromcontent — so the SQL aggregate drops the cellid (always 0).
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [b for b in (_tile_raster_bytes(r) for r in tile) if b is not None]
    return agg_core.combineavg_tiles(rasters)


@pandas_udf(BinaryType())
def _frombands_agg_udf(tile: pd.Series, band_index: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    indexed = []
    for r, idx in zip(tile, band_index):
        rb = _tile_raster_bytes(r)
        if rb is not None and idx is not None:
            indexed.append((int(idx), rb))
    return agg_core.frombands_tiles(indexed)


@pandas_udf(BinaryType())
def _rasterize_agg_udf(
    geom_wkb: pd.Series,
    value: pd.Series,
    xmin: pd.Series,
    ymin: pd.Series,
    xmax: pd.Series,
    ymax: pd.Series,
    width_px: pd.Series,
    height_px: pd.Series,
    srid: pd.Series,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    features_list = [
        (bytes(g), float(v))
        for g, v in zip(geom_wkb, value)
        if g is not None and v is not None
    ]
    if not features_list:
        return None
    # Extent/size/srid are per-group constants; read them from the first row.
    return agg_core.rasterize_features(
        features_list,
        xmin.iloc[0],
        ymin.iloc[0],
        xmax.iloc[0],
        ymax.iloc[0],
        width_px.iloc[0],
        height_px.iloc[0],
        srid.iloc[0],
    )


@pandas_udf(BinaryType())
def _derivedband_agg_udf(
    tile: pd.Series, python_func: pd.Series, func_name: pd.Series
) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [b for b in (_tile_raster_bytes(r) for r in tile) if b is not None]
    if not rasters:
        return None
    # pyfunc/func_name are per-group constants; read them from the first row.
    return agg_core.derivedband_tiles(
        rasters, str(python_func.iloc[0]), str(func_name.iloc[0])
    )


# --- public Column wrappers (compose grouped-agg BINARY + scalar as_tile) ----
def rst_merge_agg(tile: ColLike) -> Column:
    """Merge a group's tile rasters into one spatial mosaic tile.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_merge_agg("tile").alias("merged"))

    Each tile carries its own georef/CRS, so the merge is spatial and the output
    spans the union extent. Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(_merge_agg_udf(_col(tile)))


def rst_combineavg_agg(tile: ColLike) -> Column:
    """Per-pixel mean across a group's aligned tiles, ignoring NoData.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_combineavg_agg("tile").alias("avg"))

    Assumes the group's tiles are aligned (same shape/extent/CRS); raises if
    shapes differ. The output cellid is the group's first tile cellid. Returns a
    tile struct.
    """
    return _as_tile_cellid_envelope_udf(_combineavg_agg_udf(_col(tile)))


def rst_frombands_agg(tile: ColLike, band_index: ColLike) -> Column:
    """Stack a group's single-band tiles into one multi-band tile.

    Bands are ordered by ``band_index`` ASCENDING (the ordering guarantee).
    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_frombands_agg("tile", "band_index").alias("stacked"))

    Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(_frombands_agg_udf(_col(tile), _col(band_index)))


def rst_rasterize_agg(
    geom_wkb: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
) -> Column:
    """Burn a group's ``(geom_wkb, value)`` features into ONE tile.

    The extent/size/srid args are per-group constants. Overlap is last-wins.
    Use inside ``.agg()``::

        df.groupBy(k).agg(
            prx.rst_rasterize_agg("g", "v", 0, 0, 4, 4, 256, 256, 4326).alias("burned")
        )

    Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(
        _rasterize_agg_udf(
            _col(geom_wkb),
            _col(value),
            _col(xmin),
            _col(ymin),
            _col(xmax),
            _col(ymax),
            _col(width_px),
            _col(height_px),
            _col(srid),
        )
    )


def rst_derivedband_agg(
    tile: ColLike, python_func: ColLike, func_name: ColLike
) -> Column:
    """Apply a user GDAL VRT pixel function across a group's tiles.

    Each tile contributes one input band; the pyfunc (``func_name`` entry point)
    runs across the N bands to produce a single-band output tile.
    ``python_func``/``func_name`` are per-group constants. Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_derivedband_agg("tile", code, "fn").alias("out"))

    SECURITY: ``python_func`` is exec'd in-process without sandboxing — pass only
    trusted code. Returns a tile struct (cellid 0).
    """
    pf = f.lit(python_func) if isinstance(python_func, str) else _col(python_func)
    fn = f.lit(func_name) if isinstance(func_name, str) else _col(func_name)
    return _as_tile_udf(_derivedband_agg_udf(_col(tile), pf, fn))


# ---------------------------------------------------------------------------
# SQL registration registry
# ---------------------------------------------------------------------------
# Struct-accepting scalar UDFs for SQL registration.  The Python Column API
# still goes through the pandas_udf path above (tile_scalar_udf/2); these are
# separate objects that accept the full tile struct (so SQL can pass the struct
# column directly without callers needing to extract the raster subfield).

_sql_accessors = {
    "gbx_rst_width": sql_scalar_udf(accessors.width, IntegerType()),
    "gbx_rst_height": sql_scalar_udf(accessors.height, IntegerType()),
    "gbx_rst_numbands": sql_scalar_udf(accessors.numbands, IntegerType()),
    "gbx_rst_srid": sql_scalar_udf(accessors.srid, IntegerType()),
    "gbx_rst_pixelwidth": sql_scalar_udf(accessors.pixelwidth, DoubleType()),
    "gbx_rst_pixelheight": sql_scalar_udf(accessors.pixelheight, DoubleType()),
    "gbx_rst_upperleftx": sql_scalar_udf(accessors.upperleftx, DoubleType()),
    "gbx_rst_upperlefty": sql_scalar_udf(accessors.upperlefty, DoubleType()),
    "gbx_rst_scalex": sql_scalar_udf(accessors.scalex, DoubleType()),
    "gbx_rst_scaley": sql_scalar_udf(accessors.scaley, DoubleType()),
    "gbx_rst_isempty": sql_scalar_udf(accessors.isempty, BooleanType()),
    "gbx_rst_boundingbox": sql_scalar_udf(accessors.boundingbox, BinaryType()),
    "gbx_rst_metadata": sql_scalar_udf(
        accessors.metadata, MapType(StringType(), StringType())
    ),
    "gbx_rst_type": sql_scalar_udf(accessors.type, ArrayType(StringType())),
    "gbx_rst_getnodata": sql_scalar_udf(accessors.getnodata, ArrayType(DoubleType())),
    "gbx_rst_rastertoworldcoordx": sql_scalar_udf2(
        coords.raster_to_world_x, DoubleType()
    ),
    "gbx_rst_rastertoworldcoordy": sql_scalar_udf2(
        coords.raster_to_world_y, DoubleType()
    ),
    "gbx_rst_worldtorastercoordx": sql_scalar_udf2(
        coords.world_to_raster_x, IntegerType()
    ),
    "gbx_rst_worldtorastercoordy": sql_scalar_udf2(
        coords.world_to_raster_y, IntegerType()
    ),
    # Group 1 per-band statistics & scalar accessors (struct-accepting).
    "gbx_rst_avg": sql_scalar_udf(accessors.avg, ArrayType(DoubleType())),
    "gbx_rst_min": sql_scalar_udf(accessors.minimum, ArrayType(DoubleType())),
    "gbx_rst_max": sql_scalar_udf(accessors.maximum, ArrayType(DoubleType())),
    "gbx_rst_median": sql_scalar_udf(accessors.median, ArrayType(DoubleType())),
    "gbx_rst_pixelcount": sql_scalar_udf(accessors.pixelcount, ArrayType(LongType())),
    "gbx_rst_rotation": sql_scalar_udf(accessors.rotation, DoubleType()),
    "gbx_rst_skewx": sql_scalar_udf(accessors.skewx, DoubleType()),
    "gbx_rst_skewy": sql_scalar_udf(accessors.skewy, DoubleType()),
    "gbx_rst_format": sql_scalar_udf(accessors.format, StringType()),
    # memsize reads the raster byte length straight off the tile struct.
    "gbx_rst_memsize": _memsize_struct_udf,
    "gbx_rst_georeference": _georeference_udf,
    "gbx_rst_bandmetadata": _bandmetadata_udf,
    "gbx_rst_subdatasets": _subdatasets_udf,
    "gbx_rst_getsubdataset": _getsubdataset_udf,
    "gbx_rst_summary": _summary_udf,
    "gbx_rst_histogram": _histogram_udf,
    "gbx_rst_rastertoworldcoord": _rastertoworldcoord_udf,
    "gbx_rst_worldtorastercoord": _worldtorastercoord_udf,
}

# Tile-returning / constructor / array UDFs already accept the tile struct
# (or raw constructor inputs for fromcontent/rasterize); register the existing
# objects directly — no wrapper needed.
_sql_tile_ops = {
    "gbx_rst_fromcontent": _fromcontent_udf,
    "gbx_rst_transform": _transform_udf,
    "gbx_rst_to_webmercator": _to_webmercator_udf,
    "gbx_rst_resample": _resample_udf,
    "gbx_rst_resample_to_size": _resample_to_size_udf,
    "gbx_rst_resample_to_res": _resample_to_res_udf,
    "gbx_rst_clip": _clip_udf,
    "gbx_rst_updatetype": _update_type_udf,
    "gbx_rst_initnodata": _init_nodata_udf,
    "gbx_rst_tryopen": _tryopen_udf,
    "gbx_rst_setsrid": _setsrid_udf,
    "gbx_rst_band": _band_udf,
    "gbx_rst_asformat": _asformat_udf,
    "gbx_rst_buildoverviews": _buildoverviews_udf,
    "gbx_rst_sample": _sample_udf,
    "gbx_rst_fillnodata": _fillnodata_udf,
    "gbx_rst_rasterize": _rasterize_udf,
    "gbx_rst_polygonize": _polygonize_udf,
    "gbx_rst_ndvi": _ndvi_udf,
    "gbx_rst_ndwi": _ndwi_udf,
    "gbx_rst_nbr": _nbr_udf,
    "gbx_rst_savi": _savi_udf,
    "gbx_rst_evi": _evi_udf,
    "gbx_rst_slope": _slope_udf,
    "gbx_rst_aspect": _aspect_udf,
    "gbx_rst_hillshade": _hillshade_udf,
    "gbx_rst_tri": _tri_udf,
    "gbx_rst_tpi": _tpi_udf,
    "gbx_rst_roughness": _roughness_udf,
    "gbx_rst_color_relief": _color_relief_udf,
    "gbx_rst_threshold": _threshold_udf,
    "gbx_rst_filter": _filter_udf,
    "gbx_rst_convolve": _convolve_udf,
    "gbx_rst_mapalgebra": _mapalgebra_udf,
    "gbx_rst_derivedband": _derivedband_udf,
    "gbx_rst_separatebands": _separatebands_udf,
    "gbx_rst_retile": _retile_udf,
    "gbx_rst_tooverlappingtiles": _tooverlappingtiles_udf,
    "gbx_rst_maketiles": _maketiles_udf,
    "gbx_rst_tilexyz": _tilexyz_udf,
    "gbx_rst_xyzpyramid": _xyzpyramid_udf,
    "gbx_rst_h3_rastertogridavg": _u_h3_rastertogridavg,
    "gbx_rst_h3_rastertogridcount": _u_h3_rastertogridcount,
    "gbx_rst_h3_rastertogridmax": _u_h3_rastertogridmax,
    "gbx_rst_h3_rastertogridmin": _u_h3_rastertogridmin,
    "gbx_rst_h3_rastertogridmedian": _u_h3_rastertogridmedian,
    "gbx_rst_quadbin_rastertogridavg": _u_quadbin_rastertogridavg,
    "gbx_rst_quadbin_rastertogridcount": _u_quadbin_rastertogridcount,
    "gbx_rst_quadbin_rastertogridmax": _u_quadbin_rastertogridmax,
    "gbx_rst_quadbin_rastertogridmin": _u_quadbin_rastertogridmin,
    "gbx_rst_quadbin_rastertogridmedian": _u_quadbin_rastertogridmedian,
}

# Grouped aggregators register the BINARY grouped-agg pandas_udf directly; SQL
# callers use them in GROUP BY and wrap the BINARY result with
# gbx_rst_fromcontent(<agg>, 'GTiff') to recover a tile struct. The grouped-agg
# UDFs accept the tile STRUCT column directly in SQL as well (verified).
_sql_aggregators = {
    "gbx_rst_merge_agg": _merge_agg_udf,
    "gbx_rst_combineavg_agg": _combineavg_agg_sql_udf,
    "gbx_rst_frombands_agg": _frombands_agg_udf,
    "gbx_rst_rasterize_agg": _rasterize_agg_udf,
    "gbx_rst_derivedband_agg": _derivedband_agg_udf,
}

SQL_REGISTRY = {**_sql_accessors, **_sql_tile_ops, **_sql_aggregators}
