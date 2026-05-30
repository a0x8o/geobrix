"""pyrx public API — Arrow-UDF Column wrappers (signatures mirror rasterx).

Swap-compatible with ``databricks.labs.gbx.rasterx.functions``:
    from databricks.labs.gbx.pyrx import functions as prx
    df.select(prx.rst_width("tile"))
"""

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
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
from databricks.labs.gbx.pyrx.core import (
    accessors,
    coords,
    edit,
    features,
    indices,
    resample,
    terrain,
    warp,
)


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
}

SQL_REGISTRY = {**_sql_accessors, **_sql_tile_ops}
