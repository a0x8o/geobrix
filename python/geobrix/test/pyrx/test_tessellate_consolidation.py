"""Parity test: iter_tessellate generic reproduces per-grid behavior exactly.

Step 2 (RED): ``iter_tessellate`` absent → ImportError on collection.
Step 4 (GREEN): per-grid delegates resolve to the same code path → identical output.

For each grid (h3, quadbin, bng) and each mode (covering, centroid),
``iter_tessellate(ds, resolution, grid, mode)`` must yield identical cell-id
lists to the pre-existing ``iter_tessellate_{grid}`` functions.  Both old and
new calls are driven against the same tile bytes (opened as separate contexts so
neither generator state affects the other).
"""

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core.tessellate import (
    iter_tessellate,
    iter_tessellate_bng,
    iter_tessellate_h3,
    iter_tessellate_quadbin,
)

# ---- Resolution used per grid -----------------------------------------------
_RES = {"h3": 9, "quadbin": 12, "bng": "1km"}

# ---- Per-grid old (thin-delegate) functions ---------------------------------
_OLD_FN = {
    "h3": iter_tessellate_h3,
    "quadbin": iter_tessellate_quadbin,
    "bng": iter_tessellate_bng,
}


# ---- Tile helpers -----------------------------------------------------------


def _tile_4326(size: int = 32) -> bytes:
    """Small EPSG:4326 tile (London-area) for H3 and quadbin parity."""
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(-0.1, 51.5, 0.01, 0.01),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def _tile_27700(size: int = 32) -> bytes:
    """Small EPSG:27700 tile (London-area) for BNG parity."""
    minx, miny, maxx, maxy = 529500, 179500, 531500, 181500
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    xres = (maxx - minx) / size
    yres = (maxy - miny) / size
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=rasterio.transform.from_origin(minx, maxy, xres, yres),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


_TILE_FN = {"h3": _tile_4326, "quadbin": _tile_4326, "bng": _tile_27700}


# ---- Parity tests -----------------------------------------------------------


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_iter_tessellate_matches_per_grid(grid, mode):
    """Generic iter_tessellate yields identical cell-id list and chip count as
    the old per-grid delegate function."""
    tile = _TILE_FN[grid]()
    resolution = _RES[grid]
    old_fn = _OLD_FN[grid]

    with MemoryFile(bytes(tile)) as mf1:
        with mf1.open() as ds:
            new_ids = [c for c, _ in iter_tessellate(ds, resolution, grid, mode)]

    with MemoryFile(bytes(tile)) as mf2:
        with mf2.open() as ds:
            old_ids = [c for c, _ in old_fn(ds, resolution, mode)]

    assert len(new_ids) > 0, f"{grid}/{mode}: no chips yielded"
    assert new_ids == old_ids, (
        f"{grid}/{mode}: new yielded {len(new_ids)} cell-ids, "
        f"old yielded {len(old_ids)}"
    )


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
def test_iter_tessellate_invalid_mode_raises(grid):
    """iter_tessellate raises ValueError for an unrecognised mode string."""
    tile = _TILE_FN[grid]()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            with pytest.raises(ValueError, match="mode must be one of"):
                list(iter_tessellate(ds, _RES[grid], grid, mode="bad"))


def test_iter_tessellate_invalid_grid_raises():
    """iter_tessellate raises ValueError for an unrecognised grid string."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            with pytest.raises(ValueError, match="grid must be one of"):
                list(iter_tessellate(ds, 9, "xyz"))
