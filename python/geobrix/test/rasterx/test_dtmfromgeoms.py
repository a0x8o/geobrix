"""End-to-end Python tests for rst_dtmfromgeoms and rst_dtmfromgeoms_agg.

Exercises the full PySpark call_function -> registered UDF -> Scala execute path.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    return spark


def test_rst_dtmfromgeoms_returns_tile(spark):
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as F

    # Four Z-valued corner points of a 100x100 extent, as WKT (z = 2x+3y+5). Z MUST be preserved.
    pts = [
        "POINT Z (0 0 5)",
        "POINT Z (100 0 205)",
        "POINT Z (0 100 305)",
        "POINT Z (100 100 505)",
    ]
    df = spark.createDataFrame([(pts,)], ["points"])
    out = df.select(
        F.rst_dtmfromgeoms(
            f.col("points"),
            f.array().cast("array<string>"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(100.0),
            f.lit(100.0),
            f.lit(10),
            f.lit(10),
            f.lit(32633),
        ).alias("dtm")
    ).collect()
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None


def test_rst_dtmfromgeoms_wkb_points(spark):
    """Regression: WKB (binary) point arrays via PySpark must not NPE on UnsafeArrayData."""
    from pyspark.sql import functions as f
    from shapely import Point

    from databricks.labs.gbx.rasterx import functions as F

    pts = [
        Point(0.0, 0.0, 5.0).wkb,
        Point(100.0, 0.0, 205.0).wkb,
        Point(0.0, 100.0, 305.0).wkb,
        Point(100.0, 100.0, 505.0).wkb,
    ]
    df = spark.createDataFrame([(pts,)], "points: array<binary>")
    out = df.select(
        F.rst_dtmfromgeoms(
            f.col("points"),
            f.array().cast("array<binary>"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(100.0),
            f.lit(100.0),
            f.lit(10),
            f.lit(10),
            f.lit(32633),
        ).alias("dtm")
    ).collect()
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None


def test_rst_dtmfromgeoms_agg_wkb_breaklines(spark):
    """Regression: agg decodes a non-empty WKB breakline array (the untested decode path)."""
    from pyspark.sql import functions as f
    from shapely import LineString

    from databricks.labs.gbx.rasterx import functions as F

    bl = LineString([(0.0, 50.0, 0.0), (100.0, 50.0, 0.0)]).wkb
    rows = [
        (1, "POINT Z (0 0 5)"),
        (1, "POINT Z (100 0 205)"),
        (1, "POINT Z (0 100 305)"),
        (1, "POINT Z (100 100 505)"),
    ]
    df = spark.createDataFrame(rows, ["region", "pt"])
    out = (
        df.groupBy("region")
        .agg(
            F.rst_dtmfromgeoms_agg(
                f.col("pt"),
                f.array(
                    f.lit(bl).cast("binary")
                ),  # non-empty WKB breakline array (constant)
                f.lit(0.0),
                f.lit(0.01),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(100.0),
                f.lit(100.0),
                f.lit(10),
                f.lit(10),
                f.lit(32633),
            ).alias("dtm")
        )
        .collect()
    )
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None


def test_rst_dtmfromgeoms_agg_returns_tile(spark):
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as F

    rows = [
        (1, "POINT Z (0 0 5)"),
        (1, "POINT Z (100 0 205)"),
        (1, "POINT Z (0 100 305)"),
        (1, "POINT Z (100 100 505)"),
    ]
    df = spark.createDataFrame(rows, ["region", "pt"])
    out = (
        df.groupBy("region")
        .agg(
            F.rst_dtmfromgeoms_agg(
                f.col("pt"),
                f.array().cast("array<string>"),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(100.0),
                f.lit(100.0),
                f.lit(10),
                f.lit(10),
                f.lit(32633),
            ).alias("dtm")
        )
        .collect()
    )
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None
