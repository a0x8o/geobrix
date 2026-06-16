"""End-to-end Python tests for rst_rasterize_agg.

Streams (geom_wkb, value) rows into the aggregator with constant extent
parameters, and asserts a non-null tile with non-null raster is returned.
"""

import logging
import struct
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()


def _polygon_wkb(x0: float, y0: float, x1: float, y1: float) -> bytes:
    """WKB (little-endian) for a closed rectangular polygon from two corners."""
    # WKB layout: byte_order(1B) + wkb_type(4B) + ring_count(4B) + point_count(4B) + 5*point(16B each)
    coords = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]  # closed ring
    # byte_order=1 (little-endian), wkb_type=3 (Polygon), num_rings=1, num_points=5
    header = struct.pack("<BII I", 1, 3, 1, 5)
    points = b"".join(struct.pack("<dd", x, y) for x, y in coords)
    return header + points


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


def test_rst_rasterize_agg_returns_tile(spark):
    """rst_rasterize_agg streams WKB polygon rows and returns a non-null tile."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as F

    # Two non-overlapping polygons within a 100x100 extent (EPSG:32633).
    #   Polygon A: lower-left quadrant (0,0)-(50,50), burn value 10.0
    #   Polygon B: upper-right quadrant (50,50)-(100,100), burn value 20.0
    wkb_a = _polygon_wkb(0.0, 0.0, 50.0, 50.0)
    wkb_b = _polygon_wkb(50.0, 50.0, 100.0, 100.0)

    rows = [
        (1, wkb_a, 10.0),
        (1, wkb_b, 20.0),
    ]
    df = spark.createDataFrame(rows, ["key", "geom_wkb", "value"])

    out = (
        df.groupBy("key")
        .agg(
            F.rst_rasterize_agg(
                f.col("geom_wkb"),
                f.col("value"),
                f.lit(0.0),  # xmin
                f.lit(0.0),  # ymin
                f.lit(100.0),  # xmax
                f.lit(100.0),  # ymax
                f.lit(100),  # width_px
                f.lit(100),  # height_px
                f.lit(32633),  # srid
            ).alias("tile")
        )
        .collect()
    )
    assert len(out) == 1
    assert out[0]["tile"] is not None
    assert out[0]["tile"]["raster"] is not None
    assert len(out[0]["tile"]["raster"]) > 0
