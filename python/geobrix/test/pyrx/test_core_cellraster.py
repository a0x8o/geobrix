import h3
import numpy as np

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import cellraster as cr


def _cell(lat, lon, res=9):
    return h3.str_to_int(h3.latlng_to_cell(lat, lon, res))


def test_h3_str_normalizes_signed_long():
    cid_unsigned = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 9))
    signed = cid_unsigned - (1 << 64) if cid_unsigned >= (1 << 63) else cid_unsigned
    assert cr._h3_str(signed) == cr._h3_str(cid_unsigned) == h3.int_to_str(cid_unsigned)


def test_compute_gridspec_single_cell_centroids_uses_kring1():
    cid = _cell(0.0, 0.0, 9)
    # kring_pad=1 (default): a single cell is non-degenerate (neighbor centroids)
    xmin, ymin, xmax, ymax, px, w, h, srid = cr.compute_gridspec([cid])
    assert w >= 3 and h >= 3 and srid == 4326
    assert xmax > xmin and ymax > ymin
    # kring_pad=0: degenerate -> 1x1 (centroid only)
    g0 = cr.compute_gridspec([cid], kring_pad=0)
    assert g0[5] == 1 and g0[6] == 1


def test_compute_gridspec_origin_snapped_to_lattice():
    cid = _cell(10.0, 20.0, 9)
    xmin, ymin, xmax, ymax, px, w, h, srid = cr.compute_gridspec([cid], pixel_size=0.01)
    # origin is an integer multiple of pixel_size -> independently-built grids align
    assert abs((xmin / 0.01) - round(xmin / 0.01)) < 1e-9
    assert abs((ymax / 0.01) - round(ymax / 0.01)) < 1e-9


def test_compute_gridspec_rejects_mixed_resolution():
    import pytest

    a = _cell(0.0, 0.0, 9)
    b = _cell(0.0, 0.0, 8)
    with pytest.raises(ValueError, match="resolution"):
        cr.compute_gridspec([a, b])


def test_cells_to_raster_partition_property():
    # polyfill a small area -> cells; rasterize as presence mask; every burned
    # pixel centroid must re-index to a cell IN the set, every NoData pixel must not.
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = {h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)}
    cell_values = {c: 1.0 for c in cells}
    g = cr.compute_gridspec(list(cells), kring_pad=1)
    raster = cr.cells_to_raster(cell_values, *g, resolution=res)
    with _serde.open_tile(raster) as ds:
        arr = ds.read(1)
        t = ds.transform
        nod = ds.nodata
        cellset = {cr._h3_str(c) for c in cells}
        for row in range(ds.height):
            for col in range(ds.width):
                lon, lat = t * (col + 0.5, row + 0.5)
                idx = h3.latlng_to_cell(lat, lon, res)
                burned = arr[row, col] != nod
                assert burned == (idx in cellset)
