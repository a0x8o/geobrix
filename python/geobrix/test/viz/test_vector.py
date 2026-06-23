import logging
import warnings

import pytest


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-vector-tests")
        .getOrCreate()
    )
    yield s


def test_as_gdf_crs_geometry_and_columns(spark):
    from databricks.labs.gbx.viz import as_gdf

    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = as_gdf(df)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"]
    assert "wkt" not in gdf.columns
    assert all(gdf.geometry.is_valid)


def test_as_gdf_truncates_and_warns_over_max_rows(spark):
    from databricks.labs.gbx.viz import as_gdf

    df = spark.range(5).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gdf = as_gdf(df, max_rows=2)
    assert len(gdf) == 2
    assert any("truncated" in str(w.message).lower() for w in caught)


def test_cells_as_gdf_boundary_from_h3_lib(spark):
    import h3

    from databricks.labs.gbx.viz import cells_as_gdf

    cell_int = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 5))
    df = spark.createDataFrame([(cell_int, 7)], ["cellid", "count"])
    gdf = cells_as_gdf(df, extra_cols=["count"])
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["count"]) == [7]
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
