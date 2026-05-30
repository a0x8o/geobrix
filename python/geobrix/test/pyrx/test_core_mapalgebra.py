import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import mapalgebra


def _ras(value, w=2, h=2):
    data = np.full((h, w), float(value), dtype="float32")
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


def test_mapalgebra_add_two():
    out = mapalgebra.mapalgebra([_ras(1.0), _ras(2.0)], "A + B")
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "float32"
        assert np.allclose(o.read(1), 3.0)


def test_mapalgebra_scalar_on_one():
    out = mapalgebra.mapalgebra([_ras(4.0)], "A * 2")
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 8.0)


def test_mapalgebra_normalized_diff():
    out = mapalgebra.mapalgebra([_ras(10.0), _ras(4.0)], "(A - B) / (A + B)")
    with _serde.open_tile(out) as o:
        assert np.allclose(o.read(1), 6.0 / 14.0, atol=1e-5)
