"""End-to-end Python tests for raster->quadbin aggregator functions.

Mirrors the Scala suite (RST_Quadbin_RasterToGridTest) at the PySpark API
boundary — confirms the Long-overload eval entry points fire correctly when
PySpark sends Python ints as LongType.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

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


def _collect_first(spark, fn):
    """Apply `fn(tile_col)` over a single MODIS row and return the first cell tuple."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        fn(
            rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")),
            f.lit(4),
        ).alias("grid")
    )
    row = df.collect()[0]
    assert row["grid"] is not None
    bands = row["grid"]
    assert isinstance(bands, (list, tuple))
    assert len(bands) >= 1
    first_band = bands[0]
    assert isinstance(first_band, (list, tuple))
    assert len(first_band) > 0
    return first_band[0], bands


def test_rst_quadbin_rastertogridavg(spark):
    from databricks.labs.gbx.rasterx import functions as rx

    cell, _ = _collect_first(spark, rx.rst_quadbin_rastertogridavg)
    assert cell["cellID"] is not None
    assert isinstance(cell["cellID"], int)
    assert cell["measure"] is not None
    assert isinstance(cell["measure"], float)


def test_rst_quadbin_rastertogridcount(spark):
    from databricks.labs.gbx.rasterx import functions as rx

    cell, _ = _collect_first(spark, rx.rst_quadbin_rastertogridcount)
    assert cell["cellID"] is not None
    assert isinstance(cell["cellID"], int)
    # count is LongType so it round-trips to Python int
    assert isinstance(cell["measure"], int)
    assert cell["measure"] > 0


def test_rst_quadbin_rastertogridmax(spark):
    from databricks.labs.gbx.rasterx import functions as rx

    cell, _ = _collect_first(spark, rx.rst_quadbin_rastertogridmax)
    assert cell["cellID"] is not None
    assert isinstance(cell["measure"], float)


def test_rst_quadbin_rastertogridmin(spark):
    from databricks.labs.gbx.rasterx import functions as rx

    cell, _ = _collect_first(spark, rx.rst_quadbin_rastertogridmin)
    assert cell["cellID"] is not None
    assert isinstance(cell["measure"], float)


def test_rst_quadbin_rastertogridmedian(spark):
    from databricks.labs.gbx.rasterx import functions as rx

    cell, _ = _collect_first(spark, rx.rst_quadbin_rastertogridmedian)
    assert cell["cellID"] is not None
    assert isinstance(cell["measure"], float)
