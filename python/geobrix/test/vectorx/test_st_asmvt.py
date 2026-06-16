"""Python tests for gbx_st_asmvt — mirrors Scala ST_AsMvtTest."""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.appName("gbx-vectorx-tests")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    from databricks.labs.gbx.vectorx import functions as vx

    vx.register(s)
    yield s


def test_st_asmvt_single_point(spark):
    from databricks.labs.gbx.vectorx import functions as vx

    # WKB for POINT(0.5 0.5): 01 01 00 00 00 + 8 bytes x + 8 bytes y (little-endian double).
    pt_wkb = bytes.fromhex("0101000000000000000000E03F000000000000E03F")
    df = spark.createDataFrame([(pt_wkb, "alpha", 1)], ["geom_wkb", "name", "id"])
    mvt = df.agg(
        vx.st_asmvt(
            col("geom_wkb"), struct(col("name"), col("id")), lit("layer1")
        ).alias("mvt")
    ).collect()[0]["mvt"]
    assert mvt is not None and len(mvt) > 0
    assert mvt[0] == 0x1A


def test_st_asmvt_multiple_features(spark):
    from databricks.labs.gbx.vectorx import functions as vx

    # WKBs for POINT(0.1,0.1), POINT(0.5,0.5), POINT(0.9,0.9).
    pts = [
        (bytes.fromhex("01010000009A9999999999B93F9A9999999999B93F"), "a", 1),
        (bytes.fromhex("0101000000000000000000E03F000000000000E03F"), "b", 2),
        (bytes.fromhex("0101000000CDCCCCCCCCCCEC3FCDCCCCCCCCCCEC3F"), "c", 3),
    ]
    df = spark.createDataFrame(pts, ["geom_wkb", "name", "id"])
    mvt = df.agg(
        vx.st_asmvt(
            col("geom_wkb"), struct(col("name"), col("id")), lit("points")
        ).alias("mvt")
    ).collect()[0]["mvt"]
    assert mvt is not None and len(mvt) > 0
    assert b"points" in mvt
