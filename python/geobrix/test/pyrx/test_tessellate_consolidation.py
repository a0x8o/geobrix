"""Golden regression guard for iter_tessellate (all grids × modes).

Freezes the SHA-256 digest of the sorted cell-id list and the chip count for
each of the six (grid × mode) combinations, captured from the verified-correct
post-consolidation output.  Any future change to iter_tessellate's cell-id
set or count is caught here before it can silently break coverage/assignment
semantics downstream.

These are NOT live old-vs-new comparisons (the per-grid delegates are now thin
``yield from iter_tessellate(...)`` wrappers, so comparing them would be
comparing iter_tessellate to itself).  They are frozen constants, updated only
when a deliberate behavioral change is made and the new baseline verified
correct.

Behavioral correctness is covered by the 44 pre-existing tests in:
  test_core_tessellate.py, test_tessellate_{bng,quadbin}.py,
  test_core_tessellate_modes.py, test_tessellate_gridsystem.py.
"""

import hashlib

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

# ---------------------------------------------------------------------------
# Frozen golden baselines
# Captured 2026-09-09 from the verified-correct consolidated implementation.
# Update only when a deliberate behavioral change is made and re-verified.
# ---------------------------------------------------------------------------

_GOLDEN = {
    # (grid, mode): {"count": int, "digest": str}
    # digest = sha256(sorted(str(c) for c in cell_ids).join(","))[:16]
    ("h3", "covering"): {"count": 8623, "digest": "ed9af0e9521dba67"},
    ("h3", "centroid"): {"count": 1024, "digest": "2029ccd9fdd1a68d"},
    ("quadbin", "covering"): {"count": 30, "digest": "ab5d473d48ab0f7d"},
    ("quadbin", "centroid"): {"count": 30, "digest": "ab5d473d48ab0f7d"},
    ("bng", "covering"): {"count": 9, "digest": "3ad7e348dd33093d"},
    ("bng", "centroid"): {"count": 9, "digest": "3ad7e348dd33093d"},
}

# Resolution used per grid (same as captured baseline)
_RES = {"h3": 9, "quadbin": 12, "bng": "1km"}


# ---------------------------------------------------------------------------
# Tile helpers (same fixtures used to capture the baseline)
# ---------------------------------------------------------------------------


def _tile_4326(size: int = 32) -> bytes:
    """Small EPSG:4326 tile (London-area, 32×32 px, 0.01 deg) for H3 and quadbin."""
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
    """Small EPSG:27700 tile (London-area, 32×32 px, 62.5 m) for BNG."""
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


def _digest(cell_ids) -> str:
    """SHA-256 of comma-joined sorted string cell-ids, truncated to 16 hex chars."""
    sorted_strs = sorted(str(c) for c in cell_ids)
    return hashlib.sha256(",".join(sorted_strs).encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Golden regression tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_iter_tessellate_golden(grid, mode):
    """iter_tessellate must reproduce the frozen (count, cell-id digest) baseline.

    Catches any change to which cells are selected or their encoding.
    """
    tile = _TILE_FN[grid]()
    resolution = _RES[grid]
    golden = _GOLDEN[(grid, mode)]

    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            ids = [c for c, _ in iter_tessellate(ds, resolution, grid, mode)]

    assert (
        len(ids) == golden["count"]
    ), f"{grid}/{mode}: expected {golden['count']} chips, got {len(ids)}"
    assert _digest(ids) == golden["digest"], (
        f"{grid}/{mode}: cell-id set changed (count {len(ids)} matches but "
        f"digest {_digest(ids)!r} != frozen {golden['digest']!r})"
    )


# ---------------------------------------------------------------------------
# Error-path guards (behaviour, not golden)
# ---------------------------------------------------------------------------


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
