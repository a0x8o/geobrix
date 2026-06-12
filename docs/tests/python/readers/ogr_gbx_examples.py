"""Executable doc example for the lightweight ogr_gbx reader (Docker)."""

from path_config import SAMPLE_DATA_BASE

SAMPLE = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"

READ_OGR_GBX = """# Lightweight generic OGR reader (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("ogr_gbx").load(SAMPLE)   # (attrs..., geom_0, geom_0_srid, geom_0_srid_proj)
df.show()"""


def read_ogr_gbx(spark, path=None):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    df = spark.read.format("ogr_gbx").load(path or SAMPLE)
    assert "geom_0" in df.columns and "geom_0_srid" in df.columns
    assert df.count() > 0
