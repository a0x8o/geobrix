"""Pure-function tests for H3 raster tessellation (rst_h3_tessellate)."""

import h3
import numpy as np
import pytest
from rasterio.features import geometry_mask
from shapely.geometry import Polygon

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tessellate

from .conftest import make_geotiff_bytes


def _src_bounds():
    # make_geotiff_bytes origin (10.0, 50.0), 0.5 deg pixels.
    with _serde.open_tile(make_geotiff_bytes(width=8, height=8, epsg=4326)) as ds:
        return ds.bounds


def test_tessellate_returns_nonempty_array():
    with _serde.open_tile(make_geotiff_bytes(width=8, height=8, epsg=4326)) as ds:
        tiles = tessellate.tessellate_h3(ds, 4)
    assert len(tiles) > 0
    # each entry is (cellid_int, raster_bytes)
    for cellid, raster in tiles:
        assert isinstance(cellid, int)
        assert isinstance(raster, (bytes, bytearray))


def test_tessellate_cellids_are_valid_h3():
    with _serde.open_tile(make_geotiff_bytes(width=8, height=8, epsg=4326)) as ds:
        tiles = tessellate.tessellate_h3(ds, 4)
    for cellid, _ in tiles:
        assert h3.is_valid_cell(h3.int_to_str(cellid))


def test_tessellate_clipped_tiles_within_source_extent():
    src = _src_bounds()
    with _serde.open_tile(make_geotiff_bytes(width=8, height=8, epsg=4326)) as ds:
        tiles = tessellate.tessellate_h3(ds, 4)
    assert tiles
    for _, raster in tiles:
        with _serde.open_tile(raster) as o:
            b = o.bounds
            # clipped tile must lie (approximately) within the source extent
            assert b.left >= src.left - 1e-6
            assert b.right <= src.right + 1e-6
            assert b.bottom >= src.bottom - 1e-6
            assert b.top <= src.top + 1e-6


def test_tessellate_resolution_out_of_range_raises():
    with _serde.open_tile(make_geotiff_bytes(width=8, height=8, epsg=4326)) as ds:
        with pytest.raises(ValueError):
            tessellate.tessellate_h3(ds, 16)
        with pytest.raises(ValueError):
            tessellate.tessellate_h3(ds, -1)


def _cell_covers_any_pixel(ds, cellid):
    """True iff the H3 cell hexagon covers >=1 source pixel (all-touched)."""
    u = cellid + 2**64 if cellid < 0 else cellid
    boundary = h3.cell_to_boundary(h3.int_to_str(u))  # (lat, lng)
    poly = Polygon([(lng, lat) for lat, lng in boundary])
    cover = geometry_mask(
        [poly],
        out_shape=(ds.height, ds.width),
        transform=ds.transform,
        invert=True,
        all_touched=True,
    )
    return bool(cover.any())


def test_tessellate_drops_zero_coverage_fringe_cells():
    # polygon_to_cells_experimental(contain="overlap") returns exactly the cells
    # whose hexagon intersects the bbox, so no zero-coverage fringe cells can
    # appear.  This test confirms that invariant holds end-to-end.
    src = make_geotiff_bytes(width=4, height=4, epsg=4326)
    with _serde.open_tile(src) as ds:
        tiles = tessellate.tessellate_h3(ds, 3)
        # Every returned cell must genuinely cover at least one source pixel.
        zero = [cid for cid, _ in tiles if not _cell_covers_any_pixel(ds, cid)]
    assert not zero, f"tessellate returned zero-coverage fringe cells: {zero}"


def test_tessellate_reprojects_cell_for_non_4326_raster():
    # a UTM raster (EPSG:32633) should still tessellate by reprojecting the
    # cell polygons from 4326 into the raster CRS.
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.arange(16 * 16, dtype="float32").reshape(16, 16)
    profile = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(500000, 5400000, 1000, 1000),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        tiles = tessellate.tessellate_h3(ds, 6)
    assert len(tiles) > 0
    for cellid, raster in tiles:
        assert h3.is_valid_cell(h3.int_to_str(cellid))
        with _serde.open_tile(raster) as o:
            assert o.crs.to_epsg() == 32633
