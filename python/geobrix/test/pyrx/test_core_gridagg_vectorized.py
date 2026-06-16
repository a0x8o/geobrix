"""Identity tests for the vectorized pyrx raster->grid aggregation.

These pin the vectorized ``gridagg`` against (a) the upstream ``quadbin`` lib
(bit-exact cell ids) and (b) a faithful reference copy of the original
per-pixel double-loop implementation (identical output for every grid x agg).

The downstream bench gates these functions on a SORTED cell-id hash plus
order-independent measure stats (``fingerprint_dggs_grid``), so cell ORDER does
not matter -- only the cell-id SET and the per-cell measures.
"""

import h3
import numpy as np
import pytest
import quadbin
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import gridagg


def _open(raster_bytes):
    return MemoryFile(raster_bytes).open()


def _custom_raster(data, nodata=-9999.0, epsg=4326, origin=(10.0, 50.0), px=0.5):
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# --- reference copy of the ORIGINAL per-pixel loop (the thing we replaced) ---
def _reduce_ref(values, agg):
    arr = np.asarray(values, dtype="float64")
    if agg == "avg":
        return float(arr.mean())
    if agg == "count":
        return int(arr.size)
    if agg == "min":
        return float(arr.min())
    if agg == "max":
        return float(arr.max())
    if agg == "median":
        return float(np.median(arr))
    raise ValueError(agg)


def _raster_to_grid_ref(ds, resolution, grid, agg):
    resolution = int(resolution)
    if grid == "h3":

        def cell_fn(lon, lat, res):
            return h3.str_to_int(h3.latlng_to_cell(lat, lon, res))

    else:

        def cell_fn(lon, lat, res):
            return quadbin.point_to_cell(lon, lat, res)

    gt = ds.transform.to_gdal()
    out = []
    for bi in range(1, ds.count + 1):
        band = ds.read(bi).astype("float64")
        mask = ds.read_masks(bi)
        height, width = band.shape
        acc = {}
        for y in range(height):
            for x in range(width):
                if mask[y, x] == 0:
                    continue
                x_off = x + 0.5
                y_off = y + 0.5
                lon = gt[0] + x_off * gt[1] + y_off * gt[2]
                lat = gt[3] + x_off * gt[4] + y_off * gt[5]
                cid = cell_fn(lon, lat, resolution)
                acc.setdefault(cid, []).append(band[y, x])
        out.append(
            [
                {"cellID": cid, "measure": _reduce_ref(vals, agg)}
                for cid, vals in acc.items()
            ]
        )
    return out


def _as_map(band):
    """{cellID: measure} -- order-independent comparison (matches the bench)."""
    return {c["cellID"]: c["measure"] for c in band}


def _assert_band_equal(new_band, old_band, agg):
    new_map, old_map = _as_map(new_band), _as_map(old_band)
    # No duplicate cell ids (the group-reduce must collapse them).
    assert len(new_band) == len(new_map)
    assert set(new_map) == set(old_map)
    for cid, old_v in old_map.items():
        new_v = new_map[cid]
        assert type(new_v) is type(old_v), (agg, cid, type(new_v), type(old_v))
        assert np.isclose(new_v, old_v, rtol=0, atol=0) or np.isclose(new_v, old_v)


# --- 1. vectorized quadbin encoder is bit-exact vs the lib -------------------
def test_quadbin_encoder_bit_exact_dense():
    lons = np.linspace(-179.0, 179.0, 50)
    lats = np.linspace(-85.0, 85.0, 50)
    lon_g, lat_g = np.meshgrid(lons, lats)
    lon_f = lon_g.ravel()
    lat_f = lat_g.ravel()
    mismatches = 0
    total = 0
    for res in (0, 1, 5, 10, 14, 20):
        got = gridagg._quadbin_cells(lon_f, lat_f, res)
        want = np.array(
            [quadbin.point_to_cell(lo, la, res) for lo, la in zip(lon_f, lat_f)],
            dtype="uint64",
        )
        total += got.size
        mismatches += int(np.count_nonzero(got.astype("uint64") != want))
    assert mismatches == 0, f"{mismatches}/{total} quadbin encoder mismatches"


def test_quadbin_encoder_clipping_extremes():
    # Latitudes beyond the web-mercator bound get clipped to +/-89 by the lib.
    lon = np.array([-200.0, 200.0, 0.0, 0.0], dtype="float64")
    lat = np.array([95.0, -95.0, 89.0, -89.0], dtype="float64")
    for res in (3, 12, 20):
        got = gridagg._quadbin_cells(lon, lat, res)
        want = np.array(
            [quadbin.point_to_cell(lo, la, res) for lo, la in zip(lon, lat)],
            dtype="uint64",
        )
        assert np.array_equal(got.astype("uint64"), want), res


# --- 2. raster_to_grid identity: new == old, every grid x agg ----------------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "count", "min", "max", "median"])
def test_raster_to_grid_identity_synthetic(grid, agg):
    rng = np.random.default_rng(7)
    data = rng.normal(100.0, 20.0, size=(13, 17)).astype("float32")
    data[2, 3] = -9999.0  # nodata pixels exercising the mask==0 skip
    data[7, 11] = -9999.0
    data[0, 0] = -9999.0
    raster = _custom_raster(data, px=0.25)
    for res in (3, 6, 9):
        with _open(raster) as ds:
            new = gridagg.raster_to_grid(ds, res, grid, agg)
        with _open(raster) as ds:
            old = _raster_to_grid_ref(ds, res, grid, agg)
        assert len(new) == len(old) == 1
        _assert_band_equal(new[0], old[0], agg)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "count", "min", "max", "median"])
def test_raster_to_grid_identity_multiband(grid, agg):
    rng = np.random.default_rng(11)
    b1 = rng.uniform(0, 50, size=(9, 9)).astype("float32")
    b2 = rng.uniform(50, 99, size=(9, 9)).astype("float32")
    b1[1, 1] = -9999.0
    profile = dict(
        driver="GTiff",
        width=9,
        height=9,
        count=2,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.3, 0.3),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(b1, 1)
            dst.write(b2, 2)
        raster = mf.read()
    with _open(raster) as ds:
        new = gridagg.raster_to_grid(ds, 7, grid, agg)
    with _open(raster) as ds:
        old = _raster_to_grid_ref(ds, 7, grid, agg)
    assert len(new) == len(old) == 2
    for nb, ob in zip(new, old):
        _assert_band_equal(nb, ob, agg)
