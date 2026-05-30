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
