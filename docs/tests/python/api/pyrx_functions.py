"""
Python code examples for pyrx (lightweight raster API) documentation.
Single source of truth for docs/docs/api/pyrx-functions.mdx

All examples are self-contained and JAR-free: they build a synthetic in-memory
GeoTIFF using rasterio + numpy rather than reading from /Volumes sample data.
No path_config import is needed.
"""

try:
    from databricks.labs.gbx.pyrx import functions as prx
except ImportError:
    prx = None


# ---------------------------------------------------------------------------
# Shared helper — builds a small in-memory GeoTIFF (used by every example)
# ---------------------------------------------------------------------------

def _make_geotiff_bytes(width=4, height=3, count=2, epsg=4326):
    """Return in-memory 2-band float32 GTiff bytes (4 x 3, EPSG:4326, origin (10, 50), 0.5 px)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    from pyspark.sql import functions as f

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# Setup example
# ---------------------------------------------------------------------------

def pyrx_setup_example(spark):
    """Import pyrx, build an in-memory GeoTIFF, wrap it into a tile DataFrame."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    # Build a 4 x 3, 2-band float32 GTiff in memory (origin 10.0, 50.0; 0.5 px; EPSG:4326).
    raster_bytes = _make_geotiff_bytes(width=4, height=3, count=2, epsg=4326)

    df = spark.createDataFrame([(raster_bytes,)], ["raster"])
    tile_df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    tile_df.createOrReplaceTempView("rasters")
    return tile_df


pyrx_setup_example_output = """
One-row DataFrame with a tile column (struct<cellid, raster, metadata>).
Temp view `rasters` available for SQL examples.
"""


# ---------------------------------------------------------------------------
# Accessor example
# ---------------------------------------------------------------------------

def pyrx_accessors_example(spark):
    """Read basic raster properties from the tile struct."""
    from databricks.labs.gbx.pyrx import functions as prx

    tile_df = _tile_df(spark, width=4, height=3, count=2, epsg=4326)
    row = tile_df.select(
        prx.rst_width("tile").alias("width"),
        prx.rst_height("tile").alias("height"),
        prx.rst_srid("tile").alias("srid"),
        prx.rst_numbands("tile").alias("bands"),
    ).first()
    return row


pyrx_accessors_example_output = """
Row(width=4, height=3, srid=4326, bands=2)
"""


# ---------------------------------------------------------------------------
# Transform example
# ---------------------------------------------------------------------------

def pyrx_transform_example(spark):
    """Reproject the raster tile to a target CRS (EPSG:3857)."""
    from databricks.labs.gbx.pyrx import functions as prx

    tile_df = _tile_df(spark, epsg=4326)
    out = tile_df.select(prx.rst_transform("tile", 3857).alias("t"))
    srid = out.select(prx.rst_srid("t").alias("s")).first()["s"]
    return srid


pyrx_transform_example_output = """
3857
"""


# ---------------------------------------------------------------------------
# Clip example
# ---------------------------------------------------------------------------

def pyrx_clip_example(spark):
    """Clip the raster to a smaller bounding box geometry (WKB)."""
    import shapely.wkb
    from pyspark.sql import functions as f
    from shapely.geometry import box

    from databricks.labs.gbx.pyrx import functions as prx

    tile_df = _tile_df(spark, width=4, height=3, epsg=4326)
    # Clip to a 1 x 0.5 degree box — smaller than the full 2 x 1.5 degree extent.
    clip_geom = shapely.wkb.dumps(box(10.5, 49.0, 11.5, 49.5))
    df = tile_df.withColumn("clip_geom", f.lit(clip_geom))
    out = df.select(prx.rst_clip("tile", "clip_geom", False).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
    ).first()
    return row


pyrx_clip_example_output = """
Clipped tile is smaller than the original 4 x 3 (e.g. Row(w=2, h=1)).
"""


# ---------------------------------------------------------------------------
# Polygonize example
# ---------------------------------------------------------------------------

def pyrx_polygonize_example(spark):
    """Extract vector polygons from contiguous equal-value regions in the raster."""
    import numpy as np
    from pyspark.sql import functions as f
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx import functions as prx

    # Build a 4 x 4 raster with a 2 x 2 block of value 5.0 in the centre;
    # all other pixels are NoData so polygonize traces only the filled region.
    data = np.full((4, 4), -9999.0, dtype="float32")
    data[1:3, 1:3] = 5.0
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        raster_bytes = mf.read()

    df = spark.createDataFrame([(raster_bytes,)], ["raster"])
    tile_df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))

    rows = (
        tile_df.select(f.explode(prx.rst_polygonize("tile")).alias("p"))
        .select(f.col("p.value").alias("value"))
        .collect()
    )
    return rows


pyrx_polygonize_example_output = """
[Row(value=5.0)]
"""


# ---------------------------------------------------------------------------
# SQL example
# ---------------------------------------------------------------------------

def pyrx_sql_example(spark):
    """Register pyrx SQL functions and query them from Spark SQL."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    raster_bytes = _make_geotiff_bytes(width=4, height=3, count=2, epsg=4326)
    df = spark.createDataFrame([(raster_bytes,)], ["raster"])
    tile_df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    tile_df.createOrReplaceTempView("rasters_sql")

    result = spark.sql(
        "SELECT gbx_rst_width(tile) AS width, gbx_rst_srid(tile) AS srid FROM rasters_sql"
    )
    row = result.first()
    return row


pyrx_sql_example_output = """
Row(width=4, srid=4326)
"""
