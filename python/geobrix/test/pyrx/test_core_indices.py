import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import indices

from .conftest import make_geotiff_bytes


def _two_band(red, nir, nodata=-9999.0):
    arr = np.stack([red, nir]).astype("float32")
    profile = dict(
        driver="GTiff",
        width=arr.shape[2],
        height=arr.shape[1],
        count=2,
        dtype="float32",
        crs=rasterio.crs.CRS.from_epsg(4326),
        transform=rasterio.transform.from_origin(0, 0, 1, 1),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr)
        return mf.read()


def test_ndvi_masks_input_nodata_no_spread():
    red = np.full((4, 4), 10.0, dtype="float32")
    nir = np.full((4, 4), 30.0, dtype="float32")
    red[1, 1] = -9999.0
    with _serde.open_tile(_two_band(red, nir)) as ds:
        out = indices.ndvi(ds, 1, 2)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert o.nodata == -9999.0
        assert r[1, 1] == -9999.0  # masked at the sentinel
        assert r[0, 0] != -9999.0  # no neighborhood spread (per-pixel)
        assert r[2, 2] != -9999.0


def test_ndvi_no_declared_nodata_no_masking():
    red = np.full((3, 3), 10.0, dtype="float32")
    red[1, 1] = -9999.0
    nir = np.full((3, 3), 30.0, dtype="float32")
    with _serde.open_tile(_two_band(red, nir, nodata=None)) as ds:
        out = indices.ndvi(ds, 1, 2)
    with _serde.open_tile(out) as o:
        r = o.read(1)
        # no declared nodata -> our logic masks nothing at [1,1]
        assert r[1, 1] != -9999.0


def test_ndvi_values():
    # band1 (red) pixels 0..11, band2 (nir) 100..111. NDVI=(nir-red)/(nir+red).
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        out = indices.ndvi(ds, 1, 2)
    with _serde.open_tile(out) as o:
        assert o.count == 1
        assert o.dtypes[0] == "float32"
        arr = o.read(1)
        # pixel (0,0): red=0, nir=100 -> (100-0)/(100+0)=1.0
        assert abs(arr[0, 0] - 1.0) < 1e-5
        # pixel with red=10,nir=110 -> 100/120 = 0.8333
        assert abs(float(arr.flatten()[10]) - (100.0 / 120.0)) < 1e-4


def test_ndwi_and_nbr_shapes():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        ndwi_out = indices.ndwi(ds, 1, 2)
        nbr_out = indices.nbr(ds, 2, 1)
    for out in (ndwi_out, nbr_out):
        with _serde.open_tile(out) as o:
            assert o.count == 1 and o.dtypes[0] == "float32"


def test_savi_reduces_to_ndvi_when_l_zero():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        savi0 = indices.savi(ds, 1, 2, l=0.0)
        ndvi = indices.ndvi(ds, 1, 2)
    with _serde.open_tile(savi0) as a, _serde.open_tile(ndvi) as b:
        assert np.allclose(a.read(1), b.read(1), atol=1e-5)


def test_evi_runs_and_is_single_band():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=3)) as ds:
        out = indices.evi(ds, 1, 2, 3)
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "float32"
