import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import derivedband

PYFUNC_DOUBLE = """
def double(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    out_ar[:] = in_ar[0] * 2
"""

PYFUNC_SUM2 = """
def addbands(in_ar, out_ar, *args, **kwargs):
    out_ar[:] = in_ar[0] + in_ar[1]
"""


def _ras(data):
    data = np.asarray(data, dtype="float32")
    if data.ndim == 2:
        data = data[None, :, :]
    bands, h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=bands,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, h, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def test_derivedband_double():
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    with _serde.open_tile(_ras(data)) as ds:
        out = derivedband.derivedband(ds, PYFUNC_DOUBLE, "double")
    with _serde.open_tile(out) as o:
        assert o.count == 1
        assert np.allclose(o.read(1), data * 2)


def test_derivedband_sum_two_bands():
    b1 = np.full((2, 2), 5.0, dtype="float32")
    b2 = np.full((2, 2), 7.0, dtype="float32")
    with _serde.open_tile(_ras(np.stack([b1, b2]))) as ds:
        out = derivedband.derivedband(ds, PYFUNC_SUM2, "addbands")
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 12.0)
