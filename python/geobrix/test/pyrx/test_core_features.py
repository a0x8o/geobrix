import numpy as np
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import features


def test_rasterize_burns_value_inside_nodata_outside():
    # extent x:[0,4] y:[0,4], 4x4 px (1 unit/px). Box covers x:[1,3] y:[1,3].
    geom = shapely.wkb.dumps(box(1.0, 1.0, 3.0, 3.0))
    out = features.rasterize_geom(geom, 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326)
    with _serde.open_tile(out) as ds:
        assert ds.crs.to_epsg() == 4326
        assert (ds.width, ds.height) == (4, 4)
        assert ds.nodata == -9999.0
        arr = ds.read(1)
        # center pixels burned to 5.0, corners remain nodata
        assert 5.0 in np.unique(arr)
        assert -9999.0 in np.unique(arr)


def test_fillnodata_fills_hole():
    # Build a 1-band 5x5 float32 raster, all 1.0 except a nodata hole in the middle.
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.ones((5, 5), dtype="float32")
    data[2, 2] = -9999.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 5, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src_bytes = mf.read()
    with _serde.open_tile(src_bytes) as ds:
        out = features.fill_nodata(ds)
    with _serde.open_tile(out) as o:
        filled = o.read(1)
        assert filled[2, 2] != -9999.0  # the hole was interpolated
        assert filled[2, 2] == 1.0  # surrounded by 1.0
