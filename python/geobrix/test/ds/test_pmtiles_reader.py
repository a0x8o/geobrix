"""Integration tests for the pmtiles_gbx raster reader (source="raster").

Task 2: per-tile mosaic pyramid reader — enumerate tiles, fan out into
InputPartitions, render via _xyz_mosaic core, emit (z, x, y, bytes) rows.
"""

import io

import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_bounds

from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource


def _write_cog(path, w, s, e, n, px=256, val=200):
    data = np.full((3, px, px), val, dtype="uint8")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=px,
        height=px,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(w, s, e, n, px, px),
    ) as ds:
        ds.write(data)


def test_raster_reader_schema_and_fanout(spark, tmp_path):
    # two adjacent quads over a small AOI
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("bbox", "-122.50,37.74,-122.40,37.79")
        .option("minZoom", "14")
        .option("maxZoom", "16")
        .option("tilesPerPartition", "20")
        .load()
    )
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]
    rows = df.collect()
    assert len(rows) > 0
    assert df.rdd.getNumPartitions() >= 2  # fans out via InputPartitions
    # no (z,x,y) duplicates (each tile produced once)
    keys = [(r["z"], r["x"], r["y"]) for r in rows]
    assert len(keys) == len(set(keys))


def test_raster_reader_composites_seam_tile(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    spark.dataSource.register(PMTilesGbxDataSource)
    rows = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("bbox", "-122.50,37.74,-122.40,37.79")
        .option("minZoom", "16")
        .option("maxZoom", "16")
        .load()
        .collect()
    )
    # find a fully-covered tile; assert it carries BOTH source values (composited)
    both = 0
    for r in rows:
        arr = np.asarray(Image.open(io.BytesIO(bytes(r["bytes"]))).convert("RGBA"))
        if float(np.mean(arr[:, :, 3] == 255)) > 0.99:
            lo = (arr[:, :, 0] > 90) & (arr[:, :, 0] < 150)
            hi = arr[:, :, 0] > 190
            if lo.any() and hi.any():
                both += 1
    assert both >= 1, "no tile composited both quads -> the western-quad bug"


def test_raster_reader_bbox_defaults_to_source_union(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("minZoom", "15")
        .option("maxZoom", "15")
        .load()
    )  # no bbox
    assert len(df.collect()) > 0
