import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import terrain


def _dem(data):
    data = np.asarray(data, dtype="float32")
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def test_slope_flat_is_zero():
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.slope(ds)
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "float32"
        # Interior is flat (slope 0); the 1-px kernel border is NoData.
        assert np.allclose(o.read(1)[1:-1, 1:-1], 0.0, atol=1e-4)


def test_slope_45deg_ramp():
    # ramp rising 1 unit per 1 unit in +x (projected, 1m px) -> 45 degrees slope interior
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))  # value == column index
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds, unit="degrees", scale=1.0)
    with _serde.open_tile(out) as o:
        interior = o.read(1)[1:-1, 1:-1]
        assert np.allclose(interior, 45.0, atol=1.0)


def test_aspect_ramp_faces_west_or_known():
    # value increases with column (slopes up toward +x); downslope faces -x (west=270 compass)
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.aspect(ds)
    with _serde.open_tile(out) as o:
        interior = o.read(1)[1:-1, 1:-1]
        # all interior cells share one aspect; assert it's a finite compass value in [0,360)
        assert np.all((interior >= 0) & (interior < 360))
        assert np.allclose(interior, interior[0, 0], atol=1.0)


def test_aspect_flat_is_nodata():
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.aspect(ds)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # flat surface: all cells should be nodata (-9999)
        assert np.allclose(arr, -9999.0, atol=1e-3)


def test_aspect_flat_zero_for_flat():
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.aspect(ds, zero_for_flat=True)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # Interior flat cells -> 0; the 1-px kernel border is NoData.
        assert np.allclose(arr[1:-1, 1:-1], 0.0, atol=1e-3)


def test_aspect_trigonometric():
    # N-facing slope: value decreases with row (higher values at top, lower at bottom)
    # -> dzdy > 0 (Horn: g+2h+i - a+2b+c)
    # In trig mode, arctan2(dzdy, -dzdx) with dzdx=0 -> 90 degrees (east)
    # But for a slope increasing toward bottom (south), dzdy<0 → trig gives -90
    col_ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(col_ramp)) as ds:
        out_trig = terrain.aspect(ds, trigonometric=True)
        out_compass = terrain.aspect(ds, trigonometric=False)
    with _serde.open_tile(out_trig) as o:
        trig_interior = o.read(1)[1:-1, 1:-1]
    with _serde.open_tile(out_compass) as o:
        compass_interior = o.read(1)[1:-1, 1:-1]
    # trig and compass should differ (different conventions)
    assert not np.allclose(trig_interior, compass_interior, atol=0.1)


def test_hillshade_byte_range():
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "uint8"
        arr = o.read(1)
        assert arr.min() >= 0 and arr.max() <= 255


def test_hillshade_flat_uniform():
    # A flat DEM should produce uniform hillshade (no slope variation)
    flat = np.full((5, 5), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # Interior is uniform; the 1-px kernel border is NoData (0).
        assert np.all(arr[1:-1, 1:-1] == arr[1, 1])


def test_slope_percent():
    # 45-degree slope -> 100% grade
    ramp = np.tile(np.arange(5, dtype="float32"), (5, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds, unit="percent")
    with _serde.open_tile(out) as o:
        interior = o.read(1)[1:-1, 1:-1]
        assert np.allclose(interior, 100.0, atol=2.0)


def test_tri_tpi_roughness_flat_zero():
    flat = np.full((5, 5), 50.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        tri_b, tpi_b, rough_b = terrain.tri(ds), terrain.tpi(ds), terrain.roughness(ds)
    for out in (tri_b, tpi_b, rough_b):
        with _serde.open_tile(out) as o:
            assert o.count == 1 and o.dtypes[0] == "float32"
            # Interior flat -> 0; the 1-px kernel border is NoData.
            assert np.allclose(o.read(1)[1:-1, 1:-1], 0.0, atol=1e-4)


def test_roughness_known_step():
    # a single high cell in a flat field -> roughness around it = the step height
    data = np.full((5, 5), 0.0, dtype="float32")
    data[2, 2] = 10.0
    with _serde.open_tile(_dem(data)) as ds:
        out = terrain.roughness(ds)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # the peak cell and its neighbors see a 10-unit max-min spread
        assert abs(arr[2, 2] - 10.0) < 1e-4
        assert (
            abs(arr[1, 1] - 10.0) < 1e-4
        )  # diagonal neighbor's window includes the peak


def test_tpi_peak_positive():
    data = np.full((5, 5), 0.0, dtype="float32")
    data[2, 2] = 9.0
    with _serde.open_tile(_dem(data)) as ds:
        out = terrain.tpi(ds)
    with _serde.open_tile(out) as o:
        # center is higher than its (zero) neighbors -> positive TPI
        assert o.read(1)[2, 2] > 0


def test_slope_border_is_nodata():
    ramp = np.tile(np.arange(6, dtype="float32"), (6, 1))
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds, unit="degrees", scale=1.0)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert o.nodata == -9999.0
        assert (r[0, :] == -9999.0).all() and (r[-1, :] == -9999.0).all()
        assert (r[:, 0] == -9999.0).all() and (r[:, -1] == -9999.0).all()
        assert r[2, 2] != -9999.0


def test_slope_input_nodata_propagates_to_neighbors():
    ramp = np.tile(np.arange(6, dtype="float32"), (6, 1))
    ramp[3, 3] = -9999.0
    with _serde.open_tile(_dem(ramp)) as ds:
        out = terrain.slope(ds)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert (r[2:5, 2:5] == -9999.0).all()


def test_hillshade_border_is_nodata_zero():
    flat = np.full((6, 6), 100.0, dtype="float32")
    with _serde.open_tile(_dem(flat)) as ds:
        out = terrain.hillshade(ds)
    with _serde.open_tile(out) as o:
        assert o.nodata == 0.0
        r = o.read(1)
        assert (r[0, :] == 0).all()
        assert r[2, 2] != 0
