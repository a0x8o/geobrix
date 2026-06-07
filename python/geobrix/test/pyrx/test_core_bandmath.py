import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import edit, focal


def _ras(data, nodata=-9999.0):
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
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def test_threshold_masks_below():
    data = np.array([[1.0, 5.0], [10.0, 20.0]], dtype="float32")
    with _serde.open_tile(_ras(data)) as ds:
        out = edit.threshold(ds, op=">", value=5.0)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # >5 kept, others -> nodata (-9999)
        assert arr[0, 0] == -9999.0  # 1.0 fails
        assert arr[0, 1] == -9999.0  # 5.0 fails (strict >)
        assert arr[1, 0] == 10.0
        assert arr[1, 1] == 20.0


def test_filter_mean_smooths():
    data = np.zeros((5, 5), dtype="float32")
    data[2, 2] = 9.0
    with _serde.open_tile(_ras(data)) as ds:
        out = focal.filt(ds, 3, "mean")
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # 3x3 mean around the spike: center = 9/9 = 1.0
        assert abs(arr[2, 2] - 1.0) < 1e-4


def test_filter_max():
    data = np.zeros((5, 5), dtype="float32")
    data[2, 2] = 7.0
    with _serde.open_tile(_ras(data)) as ds:
        out = focal.filt(ds, 3, "max")
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert arr[1, 1] == 7.0  # neighbor's window includes the peak


def test_convolve_identity():
    data = np.arange(25, dtype="float32").reshape(5, 5)
    identity = [[0, 0, 0], [0, 1, 0], [0, 0, 0]]
    with _serde.open_tile(_ras(data)) as ds:
        out = focal.convolve(ds, identity)
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), data, atol=1e-5)
