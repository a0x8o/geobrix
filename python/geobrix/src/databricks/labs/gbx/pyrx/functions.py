"""pyrx public API — Arrow-UDF Column wrappers (signatures mirror rasterx).

Swap-compatible with ``databricks.labs.gbx.rasterx.functions``:
    from databricks.labs.gbx.pyrx import functions as prx
    df.select(prx.rst_width("tile"))
"""

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
)

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx._udf import (
    ColLike,
    _col,
    _raster_field,
    tile_scalar_udf,
    tile_scalar_udf2,
)
from databricks.labs.gbx.pyrx.core import accessors, coords, resample, warp


def register(_spark: SparkSession = None) -> None:
    """No-op compatibility shim. pyrx needs no SQL registration (Arrow UDFs are
    self-contained), but accepting register(spark) keeps notebooks swap-compatible
    with rasterx, which DOES require registration."""
    from databricks.labs.gbx.pyrx import _env

    _env.assert_rasterio_available()


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
