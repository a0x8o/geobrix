"""Integration tests for the raster_gbx DataSource (uses local Spark)."""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource

EXPECTED_METADATA_KEYS = {
    "path",
    "sourcePath",
    "driver",
    "format",
    "last_command",
    "last_error",
    "all_parents",
    "size",
    "compression",
    "isZipped",
    "isSubset",
}


def _write_sample(path, width=4, height=3):
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_schema_matches_tile_schema():
    from databricks.labs.gbx.pyrx import _serde

    ds = RasterGbxDataSource(options={"path": "/tmp/none"})
    schema = ds.schema()
    assert [f.name for f in schema.fields] == ["source", "tile"]
    assert schema["tile"].dataType == _serde.TILE_SCHEMA


def test_read_single_file_yields_one_row(spark, tmp_path):
    f = tmp_path / "sample.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(f))
    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["source"] == str(f)
    assert row["tile"]["cellid"] == -1
    assert set(row["tile"]["metadata"].keys()) == EXPECTED_METADATA_KEYS
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_read_directory_one_partition_per_file(spark, tmp_path):
    for i in range(3):
        _write_sample(str(tmp_path / f"s{i}.tif"))
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("filterRegex", r".*\.tif$")
        .load(str(tmp_path))
    )
    assert df.rdd.getNumPartitions() == 3
    assert df.count() == 3


def test_corrupt_file_fails_fast(spark, tmp_path):
    import pytest

    bad = tmp_path / "bad.tif"
    bad.write_bytes(b"not a raster")
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(bad))
    with pytest.raises(Exception):
        df.collect()
