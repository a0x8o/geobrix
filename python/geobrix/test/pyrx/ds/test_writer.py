"""Round-trip: raster_gbx read -> gtiff_gbx write -> re-read; + strict schema."""

import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_round_trip(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)

    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))

    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_strict_schema_rejects_extra_columns(spark, tmp_path):
    import pytest
    from pyspark.sql import functions as F

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src)).withColumn("extra", F.lit(1))
    with pytest.raises(Exception):
        df.write.format("gtiff_gbx").mode("overwrite").save(str(tmp_path / "o2"))


def test_overwrite_replaces_not_accumulates(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    first = {f for f in os.listdir(out_dir) if f.endswith(".tif")}
    assert len(first) == 1
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    second = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(second) == 1, f"overwrite accumulated stale tiles: {second}"


def test_namecol_controls_filenames(spark, tmp_path):
    from pyspark.sql import functions as F

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_named"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .load(str(src))
        .withColumn("source", F.lit("mytile"))
    )
    df.write.format("gtiff_gbx").mode("overwrite").option("nameCol", "source").save(
        str(out_dir)
    )
    files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert files == ["mytile.tif"]


def test_ext_option_controls_suffix(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_ext"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").option("ext", "tiff").save(
        str(out_dir)
    )
    assert all(f.endswith(".tiff") for f in os.listdir(out_dir))


def test_raster_gbx_catch_all_writer_round_trips(spark, tmp_path):
    import numpy as np
    import rasterio

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_catchall"
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("raster_gbx").mode("overwrite").save(str(out_dir))
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )
