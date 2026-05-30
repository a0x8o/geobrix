"""Tests for explicit SQL registration of gbx_rst_* via prx.register(spark).

register(spark) should install the same SQL names the heavyweight rasterx
package uses, but bound to the pyspark/rasterio UDFs — so
``spark.sql("SELECT gbx_rst_width(tile) ...")`` works identically whether
the user registered the heavyweight (Scala) or lightweight (pyrx) package.
"""

from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import functions as prx

from .conftest import make_geotiff_bytes


def _tile_view(spark, name="t", **kw):
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    df.createOrReplaceTempView(name)


def test_register_enables_scalar_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    row = spark.sql(
        "SELECT gbx_rst_width(tile) AS w, gbx_rst_height(tile) AS h, gbx_rst_srid(tile) AS s FROM t"
    ).first()
    assert (row["w"], row["h"], row["s"]) == (4, 3, 4326)


def test_register_enables_coord_sql(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    row = spark.sql(
        "SELECT gbx_rst_worldtorastercoordx(tile, 10.6, 49.9) AS c FROM t"
    ).first()
    assert row["c"] == 1


def test_register_enables_tile_returning_sql_composition(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    # reproject in SQL, then read its SRID in SQL — composes like heavyweight.
    row = spark.sql(
        "SELECT gbx_rst_srid(gbx_rst_transform(tile, 3857)) AS s FROM t"
    ).first()
    assert row["s"] == 3857


def test_register_enables_polygonize_sql(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    # polygonize SQL UDF requires explicit band and connectedness args (no SQL defaults).
    n = spark.sql("SELECT size(gbx_rst_polygonize(tile, 1, 4)) AS n FROM t").first()[
        "n"
    ]
    assert n >= 1
