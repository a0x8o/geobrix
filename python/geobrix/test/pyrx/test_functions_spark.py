from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import functions as prx

from .conftest import make_geotiff_bytes


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_no_jar_in_session(spark):
    # Guardrail: pyrx must work without the GeoBrix JAR.
    assert not spark.conf.get("spark.jars", "")


def test_rst_width_height(spark):
    df = _tile_df(spark, width=4, height=3)
    row = df.select(
        prx.rst_width("tile").alias("w"),
        prx.rst_height("tile").alias("h"),
    ).first()
    assert row["w"] == 4
    assert row["h"] == 3


def test_rst_srid(spark):
    df = _tile_df(spark, epsg=4326)
    assert df.select(prx.rst_srid("tile").alias("s")).first()["s"] == 4326


def test_rst_rastertoworldcoordx(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_rastertoworldcoordx("tile", f.lit(0), f.lit(0)).alias("x")
    ).first()
    assert row["x"] == 10.25


def test_rst_worldtorastercoordx(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_worldtorastercoordx("tile", f.lit(10.6), f.lit(49.9)).alias("c")
    ).first()
    assert row["c"] == 1


def test_rst_metadata_maptype_roundtrip(spark):
    # Exercises the MapType return path (non-pandas @f.udf fallback).
    df = _tile_df(spark, width=4, height=3)
    meta = df.select(prx.rst_metadata("tile").alias("m")).first()["m"]
    assert meta["width"] == "4"
    assert meta["height"] == "3"
    assert meta["driver"] == "GTiff"


def test_rst_boundingbox_binarytype_roundtrip(spark):
    # Exercises the BinaryType (WKB) return path end-to-end through Spark.
    import shapely.wkb

    df = _tile_df(spark, width=4, height=3)
    wkb = df.select(prx.rst_boundingbox("tile").alias("b")).first()["b"]
    assert shapely.wkb.loads(bytes(wkb)).bounds == (10.0, 48.5, 12.0, 50.0)


def test_register_is_noop(spark):
    # swap-compat: rx.register(spark) -> prx.register(spark) must not error.
    prx.register(spark)


def test_rst_transform_to_3857(spark):
    df = _tile_df(spark, epsg=4326, count=2)
    out = df.select(prx.rst_transform("tile", 3857).alias("t"))
    row = out.select(
        prx.rst_srid("t").alias("s"), prx.rst_numbands("t").alias("n")
    ).first()
    assert row["s"] == 3857
    assert row["n"] == 2


def test_rst_to_webmercator(spark):
    df = _tile_df(spark, epsg=4326)
    out = df.select(prx.rst_to_webmercator("tile").alias("t"))
    assert out.select(prx.rst_srid("t").alias("s")).first()["s"] == 3857
