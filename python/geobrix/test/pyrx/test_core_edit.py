import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import edit

from .conftest import make_geotiff_bytes


def test_clip_crops_to_geometry():
    # raster extent x:[10,12] y:[48.5,50], 0.5px. Box over col1..2 / row1 -> 2x1.
    geom_wkb = shapely.wkb.dumps(box(10.5, 49.0, 11.5, 49.5))
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = edit.clip_to_geom(ds, geom_wkb, all_touched=False)
    with _serde.open_tile(out) as o:
        assert o.crs.to_epsg() == 4326
        assert o.width < 4 and o.height < 3
        assert o.width > 0 and o.height > 0


def test_update_type_casts_dtype():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3)) as ds:
        out = edit.update_type(ds, "Int32")
    with _serde.open_tile(out) as o:
        assert o.dtypes[0] == "int32"
        assert (o.width, o.height) == (4, 3)


def test_init_nodata_sets_when_missing():
    with _serde.open_tile(make_geotiff_bytes(nodata=None)) as ds:
        assert ds.nodata is None
        out = edit.init_nodata(ds)
    with _serde.open_tile(out) as o:
        assert o.nodata == -9999.0


def test_init_nodata_preserves_existing():
    with _serde.open_tile(make_geotiff_bytes(nodata=-1.0)) as ds:
        out = edit.init_nodata(ds)
    with _serde.open_tile(out) as o:
        assert o.nodata == -1.0
