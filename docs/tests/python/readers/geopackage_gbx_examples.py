"""Executable doc example for the lightweight gpkg_gbx reader (Docker)."""

from path_config import SAMPLE_DATA_BASE

SAMPLE = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"

READ_GPKG_GBX = """# Lightweight GeoPackage reader (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("gpkg_gbx").load(SAMPLE)   # (attrs..., <geom>, <geom>_srid, <geom>_srid_proj)
df.show()"""


def read_gpkg_gbx(spark, path=None):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    df = spark.read.format("gpkg_gbx").load(path or SAMPLE)
    # gpkg geometry column name matches the source layer column (e.g. shape/SHAPE)
    srid_cols = [c for c in df.columns if c.endswith("_srid") and not c.endswith("_srid_proj")]
    assert len(srid_cols) > 0, f"Expected a *_srid column, got: {df.columns}"
    assert df.count() > 0
