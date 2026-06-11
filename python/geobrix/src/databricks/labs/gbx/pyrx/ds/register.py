"""Register the light raster DataSources with a Spark session.

Mirrors pyrx.functions.register: call once, consciously. The format strings
raster_gbx / gtiff_gbx do not collide with the Scala-registered gdal /
gtiff_gdal, so both tiers coexist.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from databricks.labs.gbx.pyrx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource

_SOURCES = (RasterGbxDataSource, GTiffGbxDataSource)


def register(spark: SparkSession = None) -> None:
    """Register raster_gbx + gtiff_gbx. Uses the active session if not given."""
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
