"""Register the light DataSources with a Spark session.

Mirrors pyrx.functions.register: call once, consciously. The format strings
raster_gbx / gtiff_gbx / pmtiles_gbx do not collide with the Scala-registered
gdal / gtiff_gdal, so both tiers coexist.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.ds.vector import (
    FileGdbGbxDataSource,
    GeoJSONGbxDataSource,
    GpkgGbxDataSource,
    VectorGbxDataSource,
    ShapefileGbxDataSource,
)

_SOURCES = (
    RasterGbxDataSource,
    GTiffGbxDataSource,
    PMTilesGbxDataSource,
    VectorGbxDataSource,
    ShapefileGbxDataSource,
    GeoJSONGbxDataSource,
    GpkgGbxDataSource,
    FileGdbGbxDataSource,
)


def register(spark: Optional[SparkSession] = None) -> None:
    """Register raster_gbx + gtiff_gbx + pmtiles_gbx. Uses the active session if not given."""
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    for source in _SOURCES:
        spark.dataSource.register(source)


def _try_register_on_import() -> None:
    """Best-effort register if a session is already live (no-op otherwise)."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            register(spark)
    except Exception:
        pass
