"""RasterX Python API.

Thin wrappers around GeoBrix Scala functions (gbx_rst_*). Register with
rx.register(spark) then use the functions on raster tile columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_rst_<name>;

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)`` — so you can write ``rst_clip(tile, geom, True)``
and ``rst_transform(tile, 4326)`` instead of wrapping in ``f.lit``. Strings and
``Column`` values pass through unchanged — pyspark treats a bare string as a
dataframe column reference (``f.col("name")``); wrap in ``f.lit(...)`` to pass
a string literal.
"""

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through.

    Strings stay as strings so pyspark's call_function treats them as column
    references (matching the library's existing idiom, e.g. rx.rst_width("tile")).
    Use f.lit("...") for string literals.
    """
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def register(_spark: SparkSession) -> None:
    """Register RasterX functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_rst_* SQL
    functions are available. Uses the active Spark session if needed.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "rasterx").load().collect()


def rst_avg(tile: ColLike) -> Column:
    """Return the average pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_avg", _col(tile))


def rst_bandmetadata(tile: ColLike, band: ColLike) -> Column:
    """Return metadata for the given band index (e.g. nodata, data type).

    Args:
        tile: Raster tile column.
        band: 1-based band index column.

    Returns:
        Column of map (string -> string).
    """
    return f.call_function("gbx_rst_bandmetadata", _col(tile), _col(band))


def rst_boundingbox(tile: ColLike) -> Column:
    """Return the bounding box of the raster in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_rst_boundingbox", _col(tile))


def rst_format(tile: ColLike) -> Column:
    """Return the GDAL format/driver name of the raster (e.g. GTiff).

    Args:
        tile: Raster tile column.

    Returns:
        Column of format string.
    """
    return f.call_function("gbx_rst_format", _col(tile))


def rst_georeference(tile: ColLike) -> Column:
    """Return the georeference (affine transform) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> double).
    """
    return f.call_function("gbx_rst_georeference", _col(tile))


def rst_getnodata(tile: ColLike) -> Column:
    """Return the NoData value for the raster (or null if not set).

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band), or null.
    """
    return f.call_function("gbx_rst_getnodata", _col(tile))


def rst_getsubdataset(tile: ColLike, subset_name: ColLike) -> Column:
    """Return a sub-dataset (e.g. HDF sublayer) by name.

    Args:
        tile: Raster tile column.
        subset_name: Name of the sub-dataset.

    Returns:
        Column of raster tile (sub-dataset).
    """
    return f.call_function("gbx_rst_getsubdataset", _col(tile), _col(subset_name))


def rst_height(tile: ColLike) -> Column:
    """Return the pixel height (number of rows) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer height.
    """
    return f.call_function("gbx_rst_height", _col(tile))


def rst_max(tile: ColLike) -> Column:
    """Return the maximum pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_max", _col(tile))


def rst_median(tile: ColLike) -> Column:
    """Return the median pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_median", _col(tile))


def rst_memsize(tile: ColLike) -> Column:
    """Return the approximate memory size of the tile in bytes.

    Args:
        tile: Raster tile column.

    Returns:
        Column of long (bytes).
    """
    return f.call_function("gbx_rst_memsize", _col(tile))


def rst_metadata(tile: ColLike) -> Column:
    """Return full metadata of the raster (driver, dimensions, CRS, etc.).

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> string).
    """
    return f.call_function("gbx_rst_metadata", _col(tile))


def rst_min(tile: ColLike) -> Column:
    """Return the minimum pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_min", _col(tile))


def rst_numbands(tile: ColLike) -> Column:
    """Return the number of bands in the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer band count.
    """
    return f.call_function("gbx_rst_numbands", _col(tile))


def rst_pixelcount(tile: ColLike) -> Column:
    """Return the valid pixel count per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of long (one per band).
    """
    return f.call_function("gbx_rst_pixelcount", _col(tile))


def rst_pixelheight(tile: ColLike) -> Column:
    """Return the pixel height (ground size in Y) in CRS units.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double (may be negative).
    """
    return f.call_function("gbx_rst_pixelheight", _col(tile))


def rst_pixelwidth(tile: ColLike) -> Column:
    """Return the pixel width (ground size in X) in CRS units.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_pixelwidth", _col(tile))


def rst_rotation(tile: ColLike) -> Column:
    """Return the rotation component of the georeference (if any).

    Args:
        tile: Raster tile column.

    Returns:
        Column of rotation (double).
    """
    return f.call_function("gbx_rst_rotation", _col(tile))


def rst_scalex(tile: ColLike) -> Column:
    """Return the X scale (pixel size in X) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_scalex", _col(tile))


def rst_scaley(tile: ColLike) -> Column:
    """Return the Y scale (pixel size in Y) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double (often negative).
    """
    return f.call_function("gbx_rst_scaley", _col(tile))


def rst_skewx(tile: ColLike) -> Column:
    """Return the X skew component of the georeference.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_skewx", _col(tile))


def rst_skewy(tile: ColLike) -> Column:
    """Return the Y skew component of the georeference.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_skewy", _col(tile))


def rst_srid(tile: ColLike) -> Column:
    """Return the spatial reference ID (EPSG code) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer SRID.
    """
    return f.call_function("gbx_rst_srid", _col(tile))


def rst_subdatasets(tile: ColLike) -> Column:
    """Return the sub-dataset names and descriptions (e.g. for HDF/NetCDF).

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> string, name to description).
    """
    return f.call_function("gbx_rst_subdatasets", _col(tile))


def rst_summary(tile: ColLike) -> Column:
    """Return a short text summary of the raster (dimensions, CRS, bands).

    Args:
        tile: Raster tile column.

    Returns:
        Column of string summary.
    """
    return f.call_function("gbx_rst_summary", _col(tile))


def rst_type(tile: ColLike) -> Column:
    """Return the raster data type (e.g. Byte, Int16, Float32) per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of strings (one per band).
    """
    return f.call_function("gbx_rst_type", _col(tile))


def rst_upperleftx(tile: ColLike) -> Column:
    """Return the X coordinate of the upper-left corner in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_upperleftx", _col(tile))


def rst_upperlefty(tile: ColLike) -> Column:
    """Return the Y coordinate of the upper-left corner in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_upperlefty", _col(tile))


def rst_width(tile: ColLike) -> Column:
    """Return the pixel width (number of columns) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer width.
    """
    return f.call_function("gbx_rst_width", _col(tile))


# Aggregators


def rst_combineavg_agg(tile: ColLike) -> Column:
    """Aggregate multiple raster tiles by averaging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg_agg", _col(tile))


def rst_derivedband_agg(tile: ColLike, pyfunc: ColLike, func_name: ColLike) -> Column:
    """Aggregate tiles and apply a Python UDF per band (use with groupBy).

    Args:
        tile: Raster tile column.
        pyfunc: Python source code of the UDF (string).
        func_name: Name of the callable in pyfunc.

    Returns:
        Column of derived raster tile.
    """
    return f.call_function(
        "gbx_rst_derivedband_agg", _col(tile), _col(pyfunc), _col(func_name)
    )


def rst_merge_agg(tile: ColLike) -> Column:
    """Aggregate multiple raster tiles by merging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge_agg", _col(tile))


# Constructors


def rst_fromcontent(content: ColLike, driver: ColLike) -> Column:
    """Build a raster tile from binary content and GDAL driver name.

    Args:
        content: Column of binary content (e.g. from binaryFile reader).
        driver: GDAL driver name (e.g. GTiff, COG).

    Returns:
        Column of raster tile.
    """
    return f.call_function("gbx_rst_fromcontent", _col(content), _col(driver))


def rst_fromfile(path: ColLike, driver: ColLike) -> Column:
    """Build a raster tile from a file path and GDAL driver name.

    Args:
        path: Column of file path (string).
        driver: GDAL driver name (e.g. GTiff).

    Returns:
        Column of raster tile.
    """
    return f.call_function("gbx_rst_fromfile", _col(path), _col(driver))


def rst_frombands(bands: ColLike) -> Column:
    """Build a raster tile from a list of band tiles (same dimensions).

    Args:
        bands: Column of array of raster tiles (one per band).

    Returns:
        Column of multi-band raster tile.
    """
    return f.call_function("gbx_rst_frombands", _col(bands))


# Generators


def rst_h3_tessellate(tile: ColLike, resolution: ColLike) -> Column:
    """Tessellate the raster into H3 cells at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of array of (H3 index, tile) or similar.
    """
    return f.call_function("gbx_rst_h3_tessellate", _col(tile), _col(resolution))


def rst_maketiles(tile: ColLike, size_in_mb: ColLike) -> Column:
    """Split the raster into smaller tiles by approximate size in MB.

    Args:
        tile: Raster tile column.
        size_in_mb: Target tile size in megabytes (column).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function("gbx_rst_maketiles", _col(tile), _col(size_in_mb))


def rst_retile(tile: ColLike, tile_width: ColLike, tile_height: ColLike) -> Column:
    """Retile the raster into tiles of the given pixel dimensions.

    Args:
        tile: Raster tile column.
        tile_width: Width of output tiles (pixels).
        tile_height: Height of output tiles (pixels).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function(
        "gbx_rst_retile", _col(tile), _col(tile_width), _col(tile_height)
    )


def rst_separatebands(tile: ColLike) -> Column:
    """Split the raster into one tile per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of single-band raster tiles.
    """
    return f.call_function("gbx_rst_separatebands", _col(tile))


def rst_tooverlappingtiles(
    tile: ColLike, tile_width: ColLike, tile_height: ColLike, overlap: ColLike
) -> Column:
    """Produce overlapping tiles with the given dimensions and overlap.

    Args:
        tile: Raster tile column.
        tile_width: Width of each tile (pixels).
        tile_height: Height of each tile (pixels).
        overlap: Overlap in pixels (e.g. for stitching).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function(
        "gbx_rst_tooverlappingtiles",
        _col(tile),
        _col(tile_width),
        _col(tile_height),
        _col(overlap),
    )


# Grid


def rst_h3_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    """Compute average pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and avg).
    """
    return f.call_function("gbx_rst_h3_rastertogridavg", _col(tile), _col(resolution))


def rst_h3_rastertogridcount(tile: ColLike, resolution: ColLike) -> Column:
    """Compute pixel count per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and count).
    """
    return f.call_function("gbx_rst_h3_rastertogridcount", _col(tile), _col(resolution))


def rst_h3_rastertogridmax(tile: ColLike, resolution: ColLike) -> Column:
    """Compute maximum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and max).
    """
    return f.call_function("gbx_rst_h3_rastertogridmax", _col(tile), _col(resolution))


def rst_h3_rastertogridmin(tile: ColLike, resolution: ColLike) -> Column:
    """Compute minimum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and min).
    """
    return f.call_function("gbx_rst_h3_rastertogridmin", _col(tile), _col(resolution))


def rst_h3_rastertogridmedian(tile: ColLike, resolution: ColLike) -> Column:
    """Compute median pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and median).
    """
    return f.call_function(
        "gbx_rst_h3_rastertogridmedian", _col(tile), _col(resolution)
    )


# Operations


def rst_asformat(tile: ColLike, new_format: ColLike) -> Column:
    """Convert the raster to a different GDAL format (e.g. COG, Zarr).

    Args:
        tile: Raster tile column.
        new_format: Target format/driver name.

    Returns:
        Column of raster tile in the new format.
    """
    return f.call_function("gbx_rst_asformat", _col(tile), _col(new_format))


def rst_clip(tile: ColLike, clip: ColLike, cutline_all_touched: ColLike) -> Column:
    """Clip the raster to a geometry (or mask).

    Args:
        tile: Raster tile column.
        clip: Clipping geometry column (WKT/WKB) or raster mask.
        cutline_all_touched: If True, include pixels touched by the boundary.

    Returns:
        Column of clipped raster tile.
    """
    return f.call_function(
        "gbx_rst_clip", _col(tile), _col(clip), _col(cutline_all_touched)
    )


def rst_combineavg(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by averaging (same extent/cellsize).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg", _col(tiles))


def rst_convolve(tile: ColLike, kernel: ColLike) -> Column:
    """Apply a convolution kernel to the raster.

    Args:
        tile: Raster tile column.
        kernel: Kernel matrix (e.g. 3x3) as column.

    Returns:
        Column of convolved raster tile.
    """
    return f.call_function("gbx_rst_convolve", _col(tile), _col(kernel))


def rst_derivedband(tile_expr: ColLike, pyfunc: ColLike, func_name: ColLike) -> Column:
    """Apply a Python UDF to each pixel (or band) to produce a derived band.

    Args:
        tile_expr: Raster tile column (or expression).
        pyfunc: Python source code of the UDF (string).
        func_name: Name of the callable in pyfunc.

    Returns:
        Column of raster tile with derived band(s).
    """
    return f.call_function(
        "gbx_rst_derivedband", _col(tile_expr), _col(pyfunc), _col(func_name)
    )


def rst_filter(tile: ColLike, kernel_size: ColLike, operation: ColLike) -> Column:
    """Apply a filter (e.g. min, max, mean) over a kernel window.

    Args:
        tile: Raster tile column.
        kernel_size: Size of the kernel (e.g. 3 for 3x3).
        operation: Filter operation name (e.g. min, max, mean).

    Returns:
        Column of filtered raster tile.
    """
    return f.call_function(
        "gbx_rst_filter", _col(tile), _col(kernel_size), _col(operation)
    )


def rst_initnodata(tile: ColLike) -> Column:
    """Initialise or fix NoData values in the raster (e.g. from metadata).

    Args:
        tile: Raster tile column.

    Returns:
        Column of raster tile with NoData set.
    """
    return f.call_function("gbx_rst_initnodata", _col(tile))


def rst_isempty(tile: ColLike) -> Column:
    """Return true if the raster tile is empty or invalid.

    Args:
        tile: Raster tile column.

    Returns:
        Column of boolean.
    """
    return f.call_function("gbx_rst_isempty", _col(tile))


def rst_mapalgebra(tiles: ColLike, expression: ColLike) -> Column:
    """Apply a map algebra expression to one or more tiles.

    Args:
        tiles: Column of array of raster tiles (or single tile).
        expression: Expression string (e.g. A + B, A * 2).

    Returns:
        Column of result raster tile.
    """
    return f.call_function("gbx_rst_mapalgebra", _col(tiles), _col(expression))


def rst_merge(tiles: ColLike) -> Column:
    """Merge multiple raster tiles into one (e.g. mosaic).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge", _col(tiles))


def rst_ndvi(tile: ColLike, red_band: ColLike, nir_band: ColLike) -> Column:
    """Compute NDVI from red and NIR band indices.

    Args:
        tile: Raster tile column.
        red_band: 1-based red band index.
        nir_band: 1-based NIR band index.

    Returns:
        Column of raster tile (single-band NDVI).
    """
    return f.call_function("gbx_rst_ndvi", _col(tile), _col(red_band), _col(nir_band))


def rst_rastertoworldcoord(tile: ColLike, pixel_x: ColLike, pixel_y: ColLike) -> Column:
    """Convert pixel (x, y) to world (x, y) in the CRS of the raster.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of struct (x, y as double) in world coordinates.
    """
    return f.call_function(
        "gbx_rst_rastertoworldcoord", _col(tile), _col(pixel_x), _col(pixel_y)
    )


def rst_rastertoworldcoordx(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    """Convert pixel (x, y) to world X coordinate.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of double.
    """
    return f.call_function(
        "gbx_rst_rastertoworldcoordx", _col(tile), _col(pixel_x), _col(pixel_y)
    )


def rst_rastertoworldcoordy(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    """Convert pixel (x, y) to world Y coordinate.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of double.
    """
    return f.call_function(
        "gbx_rst_rastertoworldcoordy", _col(tile), _col(pixel_x), _col(pixel_y)
    )


def rst_transform(tile: ColLike, target_srid: ColLike) -> Column:
    """Reproject the raster to the target SRID (EPSG code).

    Args:
        tile: Raster tile column.
        target_srid: Target spatial reference ID (e.g. 4326 for WGS84).

    Returns:
        Column of reprojected raster tile.
    """
    return f.call_function("gbx_rst_transform", _col(tile), _col(target_srid))


def rst_tryopen(tile: ColLike) -> Column:
    """Attempt to open/validate the raster; return true if successful.

    Args:
        tile: Raster tile column.

    Returns:
        Column of boolean.
    """
    return f.call_function("gbx_rst_tryopen", _col(tile))


def rst_updatetype(tile: ColLike, new_type: ColLike) -> Column:
    """Update the declared data type of the raster (e.g. after conversion).

    Args:
        tile: Raster tile column.
        new_type: New GDAL data type name (e.g. Byte, Float32).

    Returns:
        Column of raster tile with updated type metadata.
    """
    return f.call_function("gbx_rst_updatetype", _col(tile), _col(new_type))


def rst_worldtorastercoord(tile: ColLike, world_x: ColLike, world_y: ColLike) -> Column:
    """Convert world (x, y) to pixel (x, y) in the raster.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of struct (x, y as integer) in pixel coordinates.
    """
    return f.call_function(
        "gbx_rst_worldtorastercoord", _col(tile), _col(world_x), _col(world_y)
    )


def rst_worldtorastercoordx(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    """Convert world (x, y) to pixel column index.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of integer (pixel column index).
    """
    return f.call_function(
        "gbx_rst_worldtorastercoordx", _col(tile), _col(world_x), _col(world_y)
    )


def rst_worldtorastercoordy(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    """Convert world (x, y) to pixel row index.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of integer (pixel row index).
    """
    return f.call_function(
        "gbx_rst_worldtorastercoordy", _col(tile), _col(world_x), _col(world_y)
    )
