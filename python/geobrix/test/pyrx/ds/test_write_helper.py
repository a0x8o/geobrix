"""Unit tests for the writer's per-tile byte production (no Spark)."""

import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.ds import _write


def _gtiff_bytes(width=4, height=3, dtype="float32"):
    from rasterio.transform import from_origin

    data = np.arange(width * height, dtype=dtype).reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


def test_gtiff_target_is_verbatim():
    raw = _gtiff_bytes()
    meta = {"driver": "GTiff", "format": "GTiff", "compression": "DEFLATE"}
    out = _write.tile_to_bytes(
        cellid=-1, raster_bytes=raw, metadata=meta, force_driver=None
    )
    assert out == raw


def test_force_gtiff_is_verbatim_even_if_metadata_says_otherwise():
    raw = _gtiff_bytes()
    meta = {"driver": "COG", "format": "COG"}
    out = _write.tile_to_bytes(
        cellid=-1, raster_bytes=raw, metadata=meta, force_driver="GTiff"
    )
    assert out == raw


def test_non_gtiff_target_reencodes_same_pixels_with_tags():
    raw = _gtiff_bytes()
    meta = {"driver": "COG", "format": "COG", "compression": "DEFLATE"}
    out = _write.tile_to_bytes(
        cellid=7, raster_bytes=raw, metadata=meta, force_driver=None
    )
    assert out != raw
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.driver in ("COG", "GTiff")
        arr = ds.read(1)
        tags = ds.tags()
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )
    assert tags.get("RASTERX_CELL") == "7"
    assert tags.get("RASTERX_driver") == "COG"
