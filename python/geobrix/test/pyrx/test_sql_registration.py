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


def _rgb_tile_view(spark, name="t"):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.03125, 0.03125),
    )
    data = (np.arange(64 * 64) % 256).astype("uint8").reshape(64, 64)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for b in range(1, 4):
                dst.write(data, b)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView(name)


def test_register_enables_tilexyz_sql(spark):
    prx.register(spark)
    _rgb_tile_view(spark)
    # SQL callers must pass all args (no Python defaults in SQL UDFs).
    png = spark.sql(
        "SELECT gbx_rst_tilexyz(tile, 5, 16, 10, 'PNG', 256, 'bilinear') AS b FROM t"
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_register_enables_xyzpyramid_sql(spark):
    prx.register(spark)
    _rgb_tile_view(spark)
    n = spark.sql(
        "SELECT size(gbx_rst_xyzpyramid(tile, 1, 2, 'PNG', 64, 'bilinear')) AS n FROM t"
    ).first()["n"]
    assert n >= 1


def test_register_enables_h3_rastertogrid_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    # nested ARRAY<ARRAY<struct>>: outer size == band count (1).
    n = spark.sql(
        "SELECT size(gbx_rst_h3_rastertogridcount(tile, 6)) AS n FROM t"
    ).first()["n"]
    assert n == 1


def test_register_enables_quadbin_rastertogrid_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    # explode both levels and confirm counts sum to the 12 valid pixels.
    total = spark.sql(
        "SELECT sum(c.measure) AS total FROM t "
        "LATERAL VIEW explode(gbx_rst_quadbin_rastertogridcount(tile, 10)) AS b "
        "LATERAL VIEW explode(b) AS c"
    ).first()["total"]
    assert total == 12
