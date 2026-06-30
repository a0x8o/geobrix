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
from databricks.labs.gbx.ds._pmtiles_read import PMtilesRasterReader


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


def _png_bytes(i):
    import io

    import numpy as np
    from PIL import Image

    a = np.full((256, 256, 3), 40 + i * 20, dtype="uint8")
    buf = io.BytesIO()
    Image.fromarray(a).save(buf, format="PNG")
    return buf.getvalue()


def test_archive_reader_roundtrip(spark, tmp_path):
    from pyspark.sql import Row

    spark.dataSource.register(PMTilesGbxDataSource)
    # build a small archive via the writer
    tiles = [Row(z=14, x=2615 + i, y=6330, bytes=_png_bytes(i)) for i in range(3)]
    out = str(tmp_path / "rt.pmtiles")
    spark.createDataFrame(tiles).write.format("pmtiles_gbx").option(
        "shardZoom", "0"
    ).mode("overwrite").save(out)
    # read it back
    back = (
        spark.read.format("pmtiles_gbx")
        .option("source", "archive")
        .option("path", out)
        .load()
        .collect()
    )
    got = {(r["z"], r["x"], r["y"]): bytes(r["bytes"]) for r in back}
    # both the (z,x,y) set AND the tile bytes must round-trip identically
    assert got == {(14, 2615 + i, 6330): _png_bytes(i) for i in range(3)}


def test_raster_reader_rejects_bad_pixel_selection(tmp_path):
    with pytest.raises(ValueError):
        PMtilesRasterReader({"path": str(tmp_path), "pixelSelection": "max"})


def test_reader_rejects_unknown_source(spark, tmp_path):
    spark.dataSource.register(PMTilesGbxDataSource)
    with pytest.raises(Exception):
        spark.read.format("pmtiles_gbx").option("source", "bogus").option(
            "path", str(tmp_path)
        ).load().collect()


def test_raster_reader_bbox_outside_sources_is_empty(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (
        spark.read.format("pmtiles_gbx")
        .option("source", "raster")
        .option("path", str(tmp_path))
        .option("bbox", "10,50,10.1,50.1")
        .option("minZoom", "15")
        .option("maxZoom", "15")
        .load()
    )
    assert df.collect() == []
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]
