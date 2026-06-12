"""Executable doc examples for the lightweight vector writers (Docker).

Each function is a self-contained round-trip: read source → write via *_gbx →
read back → assert feature-count parity. Used as the single source of truth for
the per-format writer pages in docs/docs/writers/.
"""

import os
import tempfile

from path_config import SAMPLE_DATA_BASE

# ---------------------------------------------------------------------------
# Snippet strings shown in the MDX pages (CodeFromTest functionName=...)
# ---------------------------------------------------------------------------

WRITE_OGR_GBX = """# Generic lightweight OGR vector writer (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
df = spark.read.format("geojson_gbx").load(src)
out = "/tmp/gbx_ogr_write_example"
(df.coalesce(1).write.format("ogr_gbx")
   .option("driverName", "GeoJSON")
   .mode("overwrite").save(out))
back = spark.read.format("ogr_gbx").option("driverName", "GeoJSON").load(out)
assert back.count() == df.count()"""

WRITE_SHAPEFILE_GBX = """# Lightweight Shapefile writer (pyogrio; OGR driver preset to "ESRI Shapefile")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"
df = spark.read.format("shapefile_gbx").load(src)
out = "/tmp/gbx_shapefile_write_example"
df.coalesce(1).write.format("shapefile_gbx").mode("overwrite").save(out)
back = spark.read.format("shapefile_gbx").load(out)
assert back.count() == df.count()"""

WRITE_GEOJSON_GBX = """# Lightweight GeoJSON writer (pyogrio; OGR driver preset to "GeoJSON")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
df = spark.read.format("geojson_gbx").load(src)
out = "/tmp/gbx_geojson_write_example"
df.coalesce(1).write.format("geojson_gbx").mode("overwrite").save(out)
back = spark.read.format("geojson_gbx").load(out)
assert back.count() == df.count()"""

WRITE_GPKG_GBX = """# Lightweight GeoPackage writer (pyogrio; OGR driver preset to "GPKG")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
df = spark.read.format("gpkg_gbx").load(src)
out = "/tmp/gbx_gpkg_write_example"
df.coalesce(1).write.format("gpkg_gbx").mode("overwrite").save(out)
back = spark.read.format("gpkg_gbx").load(out)
assert back.count() == df.count()"""

WRITE_FILE_GDB_GBX = """# Lightweight GeoDatabase writer (pyogrio; OGR driver preset to "OpenFileGDB")
from databricks.labs.gbx.ds.register import register
register(spark)
src = f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"
df = spark.read.format("file_gdb_gbx").load(src)
out = "/tmp/gbx_filegdb_write_example"
df.coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
back = spark.read.format("file_gdb_gbx").load(out)
assert back.count() == df.count()"""


# ---------------------------------------------------------------------------
# Executable round-trip functions (called by the test file)
# ---------------------------------------------------------------------------


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def write_ogr_gbx(spark):
    """WRITE_OGR_GBX: generic ogr_gbx round-trip (GeoJSON driver)."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
    df = spark.read.format("geojson_gbx").load(src)
    with tempfile.TemporaryDirectory() as out:
        (
            df.coalesce(1)
            .write.format("ogr_gbx")
            .option("driverName", "GeoJSON")
            .mode("overwrite")
            .save(out)
        )
        back = spark.read.format("ogr_gbx").option("driverName", "GeoJSON").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
    return out


def write_shapefile_gbx(spark):
    """WRITE_SHAPEFILE_GBX: shapefile_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"
    df = spark.read.format("shapefile_gbx").load(src)
    with tempfile.TemporaryDirectory() as out:
        df.coalesce(1).write.format("shapefile_gbx").mode("overwrite").save(out)
        back = spark.read.format("shapefile_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
    return out


def write_geojson_gbx(spark):
    """WRITE_GEOJSON_GBX: geojson_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
    df = spark.read.format("geojson_gbx").load(src)
    with tempfile.TemporaryDirectory() as out:
        df.coalesce(1).write.format("geojson_gbx").mode("overwrite").save(out)
        back = spark.read.format("geojson_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
    return out


def write_gpkg_gbx(spark):
    """WRITE_GPKG_GBX: gpkg_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
    df = spark.read.format("gpkg_gbx").load(src)
    with tempfile.TemporaryDirectory() as out:
        df.coalesce(1).write.format("gpkg_gbx").mode("overwrite").save(out)
        back = spark.read.format("gpkg_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
    return out


def write_file_gdb_gbx(spark):
    """WRITE_FILE_GDB_GBX: file_gdb_gbx round-trip."""
    _register(spark)
    src = f"{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip"
    df = spark.read.format("file_gdb_gbx").load(src)
    with tempfile.TemporaryDirectory() as out:
        df.coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
        back = spark.read.format("file_gdb_gbx").load(out)
        assert back.count() == df.count(), f"count mismatch: {back.count()} != {df.count()}"
    return out
