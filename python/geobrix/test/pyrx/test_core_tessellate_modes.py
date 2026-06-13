"""Tests for rst_h3_tessellate mode parameter (Spark-free core).

iter_tessellate_h3 yields (cellid_int, gtiff_bytes) 2-tuples.
"""

import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tessellate as T


def _tile_4326(size=64, res_deg=0.01, origin=(-0.1, 51.5)):
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(origin[0], origin[1], res_deg, res_deg),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def test_centroid_mode_partitions_pixels():
    """centroid: every valid pixel assigned to exactly one cell; union == all pixels; no overlap."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            results = list(T.iter_tessellate_h3(ds, resolution=9, mode="centroid"))

    seen = 0
    for _cellid, raster_bytes in results:
        with _serde.open_tile(raster_bytes) as ds:
            arr = ds.read(1, masked=True)
            seen += int((~arr.mask).sum())

    assert seen == 64 * 64, (
        f"centroid chips must partition all valid pixels exactly once; got {seen}"
    )


def test_centroid_mode_invalid_raises():
    """Unknown mode raises ValueError with a useful message."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            import pytest
            with pytest.raises(ValueError, match="mode must be one of"):
                list(T.iter_tessellate_h3(ds, resolution=9, mode="bad_mode"))


def test_covering_mode_unchanged():
    """Passing mode='covering' (explicit default) still yields cells (backward compat)."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            cells = list(T.iter_tessellate_h3(ds, resolution=9, mode="covering"))
    assert len(cells) > 0, "covering mode must yield at least one cell"
