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
    from databricks.labs.gbx.vizx import as_gdf

    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = as_gdf(df)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"]
    assert "wkt" not in gdf.columns
    assert all(gdf.geometry.is_valid)


def test_as_gdf_truncates_and_warns_over_max_rows(spark):
    from databricks.labs.gbx.vizx import as_gdf

    df = spark.range(5).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gdf = as_gdf(df, max_rows=2)
    assert len(gdf) == 2
    assert any("truncated" in str(w.message).lower() for w in caught)


def test_cells_as_gdf_boundary_from_h3_lib(spark):
    import h3

    from databricks.labs.gbx.vizx import cells_as_gdf

    cell_int = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 5))
    df = spark.createDataFrame([(cell_int, 7)], ["cellid", "count"])
    gdf = cells_as_gdf(df, extra_cols=["count"])
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["count"]) == [7]
    assert gdf.geometry.iloc[0].geom_type == "Polygon"


# ---------------------------------------------------------------------------
# sample_seed: reproducible sampling on the collection adapters
# ---------------------------------------------------------------------------


class _FakeSparkDF:
    """Minimal Spark-DataFrame stand-in for sampling-path unit assertions.

    Records the args passed to ``.sample`` and returns a pandas frame from
    ``.toPandas`` so the adapter can build a gdf without a real Spark session.
    """

    def __init__(self, pdf, *, count, columns=None):
        self._pdf = pdf
        self._count = count
        self.columns = columns if columns is not None else list(pdf.columns)
        self.sample_calls = []

    def count(self):
        return self._count

    def select(self, *cols):
        return self

    def sample(self, withReplacement, fraction, seed):
        self.sample_calls.append(
            {"withReplacement": withReplacement, "fraction": fraction, "seed": seed}
        )
        return self

    def limit(self, n):
        self._pdf = self._pdf.iloc[:n]
        return self

    def toPandas(self):
        return self._pdf


def test_as_gdf_sample_seed_calls_sample_with_seed_and_fraction():
    """sample_seed=int -> df.sample(seed=..., fraction=max_rows*1.3/count)."""
    import pandas as pd

    from databricks.labs.gbx.vizx import as_gdf

    pdf = pd.DataFrame(
        {"name": list("abcd"), "wkt": [f"POINT ({i} 0)" for i in range(4)]}
    )
    fake = _FakeSparkDF(pdf, count=1000)
    gdf = as_gdf(fake, max_rows=10, sample_seed=42)
    assert len(fake.sample_calls) == 1
    call = fake.sample_calls[0]
    assert call["seed"] == 42
    assert call["withReplacement"] is False
    assert abs(call["fraction"] - (10 * 1.3 / 1000)) < 1e-12
    assert gdf.crs.to_epsg() == 4326


def test_as_gdf_sample_seed_none_uses_limit_not_sample(spark):
    """sample_seed=None keeps the first-N .limit path (no .sample, no count)."""
    from databricks.labs.gbx.vizx import as_gdf

    df = spark.range(5).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    pdf = df.toPandas()
    fake = _FakeSparkDF(pdf, count=5)
    fake.count = lambda: (_ for _ in ()).throw(
        AssertionError("count() must not be called on the None path")
    )
    gdf = as_gdf(fake, max_rows=2)
    assert fake.sample_calls == []
    assert len(gdf) == 2


def test_as_gdf_sample_seed_reproducible_same_seed_identical(spark):
    """Same seed twice -> identical collected rows; a different seed differs."""
    from databricks.labs.gbx.vizx import as_gdf

    df = spark.range(2000).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    a = as_gdf(df, max_rows=50, sample_seed=7)
    b = as_gdf(df, max_rows=50, sample_seed=7)
    c = as_gdf(df, max_rows=50, sample_seed=99)
    assert list(a["id"]) == list(b["id"])
    # different seed: overwhelmingly likely to differ on 50-of-2000 samples
    assert list(a["id"]) != list(c["id"])


def test_cells_as_gdf_sample_seed_calls_sample_with_seed():
    """cells_as_gdf threads sample_seed into df.sample (seed + fraction)."""
    import h3
    import pandas as pd

    from databricks.labs.gbx.vizx import cells_as_gdf

    cell = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 5))
    pdf = pd.DataFrame({"cellid": [cell, cell], "count": [1, 2]})
    fake = _FakeSparkDF(pdf, count=500, columns=["cellid", "count"])
    cells_as_gdf(fake, extra_cols=["count"], max_rows=10, sample_seed=11)
    assert len(fake.sample_calls) == 1
    call = fake.sample_calls[0]
    assert call["seed"] == 11
    assert abs(call["fraction"] - (10 * 1.3 / 500)) < 1e-12


def test_cells_as_gdf_sample_seed_none_uses_limit(spark):
    """cells_as_gdf sample_seed=None keeps first-N .limit (no .sample)."""
    import h3

    from databricks.labs.gbx.vizx import cells_as_gdf

    cells = [(h3.str_to_int(c), 1) for c in h3.grid_disk(h3.latlng_to_cell(0, 0, 5), 2)]
    df = spark.createDataFrame(cells, ["cellid", "count"])
    gdf = cells_as_gdf(df, extra_cols=["count"], max_rows=3)
    assert len(gdf) == 3


# ---------------------------------------------------------------------------
# grid_as_gdf tests
# ---------------------------------------------------------------------------


def test_grid_as_gdf_from_dict_4326():
    """Dict with srid=4326 -> 1-row GDF with correct bounds and crs."""
    from databricks.labs.gbx.vizx import grid_as_gdf

    grid = {
        "xmin": -73.0,
        "ymin": 40.0,
        "xmax": -72.0,
        "ymax": 41.0,
        "srid": 4326,
        "pixel_size": 0.01,
        "width": 100,
        "height": 100,
    }
    gdf = grid_as_gdf(grid)
    assert len(gdf) == 1
    assert gdf.crs.to_epsg() == 4326
    minx, miny, maxx, maxy = gdf.geometry.iloc[0].bounds
    assert abs(minx - (-73.0)) < 1e-6
    assert abs(miny - 40.0) < 1e-6
    assert abs(maxx - (-72.0)) < 1e-6
    assert abs(maxy - 41.0) < 1e-6
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
    # Optional metadata columns carried through
    assert gdf["pixel_size"].iloc[0] == 0.01
    assert gdf["width"].iloc[0] == 100
    assert gdf["height"].iloc[0] == 100


def test_grid_as_gdf_projected_srid_reprojects_to_4326():
    """UTM-27700 bounding box reprojects to sensible lon/lat range."""
    from databricks.labs.gbx.vizx import grid_as_gdf

    # London area in EPSG:27700 (British National Grid)
    grid = {
        "xmin": 525000.0,
        "ymin": 175000.0,
        "xmax": 535000.0,
        "ymax": 185000.0,
        "srid": 27700,
    }
    gdf = grid_as_gdf(grid)
    assert gdf.crs.to_epsg() == 4326
    minx, miny, maxx, maxy = gdf.geometry.iloc[0].bounds
    # Reprojected bounds should be in lon/lat range for London
    assert -1.0 < minx < 0.0, f"xmin longitude out of expected London range: {minx}"
    assert 51.0 < miny < 52.0, f"ymin latitude out of expected London range: {miny}"
    assert maxx > minx
    assert maxy > miny


def test_grid_as_gdf_srid_override():
    """Explicit srid kwarg overrides any srid stored in the grid."""
    from databricks.labs.gbx.vizx import grid_as_gdf

    # Supply grid without srid field; pass srid explicitly as 4326
    grid = {"xmin": -73.0, "ymin": 40.0, "xmax": -72.0, "ymax": 41.0}
    gdf = grid_as_gdf(grid, srid=4326)
    assert gdf.crs.to_epsg() == 4326
    assert len(gdf) == 1


def test_grid_as_gdf_from_row(spark):
    """Spark Row input works the same as a plain dict."""
    from pyspark.sql import Row

    from databricks.labs.gbx.vizx import grid_as_gdf

    row = Row(xmin=-73.0, ymin=40.0, xmax=-72.0, ymax=41.0, srid=4326)
    gdf = grid_as_gdf(row)
    assert gdf.crs.to_epsg() == 4326
    assert len(gdf) == 1
    minx, _, maxx, _ = gdf.geometry.iloc[0].bounds
    assert abs(minx - (-73.0)) < 1e-6
    assert abs(maxx - (-72.0)) < 1e-6


# ---------------------------------------------------------------------------
# cells_as_gdf dissolve_by tests
# ---------------------------------------------------------------------------


def test_cells_as_gdf_dissolve_by_merges_per_group(spark):
    """dissolve_by=band_level returns one merged polygon per distinct value."""
    import h3

    from databricks.labs.gbx.vizx import cells_as_gdf

    # Two groups: band_level 1 and 2, each with 3 adjacent cells around
    # different lat/lng origins so geometries are distinct.
    def _cell_ints(lat, lng, res=5):
        centre = h3.latlng_to_cell(lat, lng, res)
        return [h3.str_to_int(c) for c in h3.grid_disk(centre, 1)]

    group1 = [(c, 1) for c in _cell_ints(0.0, 0.0)]
    group2 = [(c, 2) for c in _cell_ints(10.0, 10.0)]
    rows = group1 + group2

    df = spark.createDataFrame(rows, ["cellid", "band_level"])
    gdf = cells_as_gdf(df, extra_cols=["band_level"], dissolve_by="band_level")

    assert len(gdf) == 2
    assert set(gdf["band_level"]) == {1, 2}
    assert all(gdf.geometry.is_valid)
    # Each dissolved geometry should cover more area than a single cell
    single_cell = cells_as_gdf(
        spark.createDataFrame(group1[:1], ["cellid", "band_level"]),
        extra_cols=["band_level"],
    )
    assert gdf.geometry.iloc[0].area > single_cell.geometry.iloc[0].area


def test_cells_as_gdf_dissolve_by_not_in_extra_cols_raises(spark):
    """dissolve_by not in extra_cols raises ValueError."""
    import h3

    from databricks.labs.gbx.vizx import cells_as_gdf

    cell_int = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 5))
    df = spark.createDataFrame([(cell_int, 7)], ["cellid", "count"])
    with pytest.raises(ValueError, match="dissolve_by"):
        cells_as_gdf(df, extra_cols=["count"], dissolve_by="band_level")
