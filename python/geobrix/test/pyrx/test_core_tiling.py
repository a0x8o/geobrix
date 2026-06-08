from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tiling

from .conftest import make_geotiff_bytes


def test_separate_bands():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=3)) as ds:
        parts = tiling.separate_bands(ds)
    assert len(parts) == 3
    for b in parts:
        with _serde.open_tile(b) as o:
            assert o.count == 1
            assert (o.width, o.height) == (4, 3)


def test_retile_even():
    # 4x4 into 2x2 -> 4 tiles, each 2x2, CRS preserved
    with _serde.open_tile(make_geotiff_bytes(width=4, height=4, epsg=4326)) as ds:
        parts = tiling.retile(ds, 2, 2)
    assert len(parts) == 4
    for b in parts:
        with _serde.open_tile(b) as o:
            assert (o.width, o.height) == (2, 2)
            assert o.crs.to_epsg() == 4326


def test_retile_uneven_edges():
    # 5x5 into 2x2 -> 3x3 = 9 tiles; edge tiles are 1 wide/tall
    with _serde.open_tile(make_geotiff_bytes(width=5, height=5)) as ds:
        parts = tiling.retile(ds, 2, 2)
    assert len(parts) == 9
    sizes = sorted({(o_w, o_h) for o_w, o_h in _dims(parts)})
    # contains full 2x2 and edge 1x* / *x1 tiles
    assert (2, 2) in sizes and (1, 1) in sizes


def _dims(parts):
    out = []
    for b in parts:
        with _serde.open_tile(b) as o:
            out.append((o.width, o.height))
    return out


def test_overlapping_tiles():
    # 4x4, tile 2x2, overlap 1 -> step 1 -> 3x3 windows clamped = 9 (corner windows smaller)
    with _serde.open_tile(make_geotiff_bytes(width=4, height=4)) as ds:
        parts = tiling.to_overlapping_tiles(ds, 2, 2, 1)
    assert len(parts) >= 4
    # at least one full 2x2 tile exists
    assert (2, 2) in set(_dims(parts))


def _square_geotiff_bytes(side, count, dtype="float32"):
    """Square multi-band GTiff with arange data (uncompressed, like the corpus)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.arange(side * side, dtype=dtype).reshape(side, side)
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=count,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(0, side, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for b in range(1, count + 1):
                dst.write(data + (b - 1) * 100, b)
        return mf.read()


def _heavy_count(width, height, size_bytes, dest_mib):
    """Reference port of BalancedSubdivision.getTileSize + ReTile tile count.

    Power-of-4 split: smallest k (capped at 4^(k+1) <= 512) such that
    size_bytes >> (2*k) <= limit; nx = ny = 2**k; tiles via ceil-div.
    """
    import math

    limit = dest_mib * 1024 * 1024
    k = 0
    while k < 9 and (size_bytes >> (2 * k)) > limit and (1 << (2 * (k + 1))) <= 512:
        k += 1
    nx = 1 << k
    tile_x = (width + nx - 1) // nx
    tile_y = (height + nx - 1) // nx
    return math.ceil(width / tile_x) * math.ceil(height / tile_y)


def test_make_tiles_matches_heavy_power_of_4():
    # Heavy keys on the encoded (vsimem) byte length, not raw pixel size, and
    # splits power-of-4. For uncompressed arange GTiffs (encoded ~= raw):
    #   512^2 x 2 float32 ~= 2 MiB, mib=1  -> k=1 -> 4 tiles
    #   256^2 x 2 float32 ~= 0.5 MiB, mib=1 -> k=0 -> 1 tile
    #   1024^2 x 2 float32 ~= 8 MiB, mib=1 -> k=2 -> 16 tiles (sqrt heuristic gave 9)
    for side, mib in [(256, 1), (512, 1), (1024, 1), (512, 4), (1024, 4)]:
        src = _square_geotiff_bytes(side, count=2)
        expected = _heavy_count(side, side, len(src), mib)
        with _serde.open_tile(src) as ds:
            parts = tiling.make_tiles(ds, mib)
        assert (
            len(parts) == expected
        ), f"side={side} mib={mib}: light={len(parts)} expected(heavy)={expected}"


def test_make_tiles_tile_dims_are_power_of_2_grid():
    # 512^2 x 2 @ mib=1 -> 2x2 grid of 256x256 tiles, CRS preserved.
    src = _square_geotiff_bytes(512, count=2)
    with _serde.open_tile(src) as ds:
        parts = tiling.make_tiles(ds, 1)
    assert len(parts) == 4
    for b in parts:
        with _serde.open_tile(b) as o:
            assert (o.width, o.height) == (256, 256)
            assert o.crs.to_epsg() == 4326


def test_make_tiles_single_when_budget_large():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=4)) as ds:
        parts = tiling.make_tiles(ds, 100.0)  # huge budget -> one tile
    assert len(parts) == 1
