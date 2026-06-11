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


def test_multi_tile_split_matches_core_tiling(spark, tmp_path):
    import os

    import rasterio

    from databricks.labs.gbx.pyrx.core import tiling as core_tiling

    # Incompressible noise so the on-disk file is genuinely large -> forces a split.
    f = tmp_path / "big.tif"
    rng = np.random.default_rng(0)
    data = rng.integers(0, 255, size=(3, 2048, 2048), dtype="uint8")
    profile = dict(
        driver="GTiff",
        width=2048,
        height=2048,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    )
    with rasterio.open(str(f), "w", **profile) as ds:
        ds.write(data)

    size_bytes = os.path.getsize(str(f))
    with rasterio.open(str(f)) as ds:
        expected = len(core_tiling.make_tiles(ds, size_in_mb=1, size_bytes=size_bytes))
    assert expected > 1, "test setup failed to force a multi-tile split"

    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("sizeInMB", "1").load(str(f))
    assert df.count() == expected
    # every emitted tile is a fresh, un-tessellated tile
    assert all(r["tile"]["cellid"] == -1 for r in df.select("tile").collect())


def test_whole_file_gtiff_is_passthrough(spark, tmp_path):
    # A single whole-raster GTiff tile must emit the ORIGINAL file bytes verbatim
    # (no decode/re-encode) — the fast path. Still cellid=-1 + 11 metadata keys,
    # and the decoded pixels equal the source.
    f = tmp_path / "whole.tif"
    _write_sample(str(f))
    raw = f.read_bytes()

    spark.dataSource.register(RasterGbxDataSource)
    rows = spark.read.format("raster_gbx").load(str(f)).collect()
    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert (
        bytes(tile["raster"]) == raw
    ), "whole-file GTiff should pass through unchanged"
    assert tile["cellid"] == -1
    assert set(tile["metadata"].keys()) == EXPECTED_METADATA_KEYS
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_multi_tile_subwindows_are_reencoded(spark, tmp_path):
    # When the source splits into sub-tiles, those CANNOT pass through (each is a
    # window of the source), so they must be re-encoded -> bytes differ from source.
    f = tmp_path / "big.tif"
    rng = np.random.default_rng(1)
    data = rng.integers(0, 255, size=(3, 2048, 2048), dtype="uint8")
    profile = dict(
        driver="GTiff",
        width=2048,
        height=2048,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    )
    with rasterio.open(str(f), "w", **profile) as ds:
        ds.write(data)
    raw = f.read_bytes()

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx").option("sizeInMB", "1").load(str(f)).collect()
    )
    assert len(rows) > 1  # split happened
    assert all(
        bytes(r["tile"]["raster"]) != raw for r in rows
    ), "sub-tiles must be re-encoded"
