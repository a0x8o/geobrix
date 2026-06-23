"""End-to-end Python tests for rst_frombands_agg.

Streams (tile, band_index) rows into the aggregator, asserts a non-null
multi-band tile is returned. Uses gbx_rst_fromcontent on a known test TIF to
produce real single-band input tiles (two copies = band 1 and band 2).
"""

import logging
from pathlib import Path
from test.rasterx._helpers import read_bytes

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

# MODIS single-band GeoTIFF used by several rasterx python tests.
MODIS_B01 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
).resolve()


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


def test_rst_frombands_agg_returns_tile(spark):
    """rst_frombands_agg stacks two single-band tiles into a non-null tile."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as F

    # Load the same single-band MODIS TIF twice with different band indices.
    # The aggregator sorts by band_index and stacks, so the result should have >= 1 band.
    # rst_fromfile is lightweight-only (issue #34); carry the tile bytes and decode
    # via the heavy-native gbx_rst_fromcontent.
    modis_content = read_bytes(MODIS_B01)
    rows = [
        (1, modis_content, 1),
        (1, modis_content, 2),
    ]
    df = spark.createDataFrame(rows, ["key", "content", "band_index"])

    # Materialise tile column via rst_fromcontent, then aggregate.
    df_tiles = df.select(
        f.col("key"),
        F.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile"),
        f.col("band_index"),
    )

    out = (
        df_tiles.groupBy("key")
        .agg(
            F.rst_frombands_agg(
                f.col("tile"),
                f.col("band_index"),
            ).alias("result")
        )
        .collect()
    )
    assert len(out) == 1
    assert out[0]["result"] is not None
    assert out[0]["result"]["raster"] is not None
    assert len(out[0]["result"]["raster"]) > 0
