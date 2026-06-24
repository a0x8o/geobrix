import h3

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx


def test_rst_h3_rasterize_agg_presence_mask(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = (
        df.groupBy("tx").agg(rx.rst_h3_rasterize_agg("cellid").alias("tile")).collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        # presence mask -> covered pixels are 1.0, count matches >=1 per cell
        assert (arr == 1.0).sum() >= len(cells)
        assert ds.nodata == -9999.0


def test_rst_h3_rasterize_agg_burns_value(spark):
    res = 9
    c = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, res))
    df = spark.createDataFrame([(int(c), 42.0, "TX1")], ["cellid", "val", "tx"])
    out = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        assert (arr == 42.0).sum() >= 1


def test_rst_h3_rasterize_agg_null_typed_value_column_is_presence(spark):
    """A null in a TYPED (Double) value column must burn presence 1.0, not NaN.

    Regression: pandas delivers a typed null as np.nan, and `np.nan is not None`
    is True, so the presence guard burned float(np.nan)=NaN. The cluster benchmark
    surfaced this as a heavy(1.0)-vs-light(NaN) divergence.
    """
    import numpy as np
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("val", DoubleType(), True),  # nullable; all null -> presence
            StructField("tx", StringType(), False),
        ]
    )
    df = spark.createDataFrame([(int(c), None, "TX1") for c in cells], schema)
    out = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= len(cells)
        assert not np.isnan(covered).any(), "null value column burned NaN, not presence"
        assert np.all(covered == 1.0), f"expected all 1.0, got {np.unique(covered)}"
