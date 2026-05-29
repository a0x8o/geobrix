import shapely.wkb

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors

from .conftest import make_geotiff_bytes


def _ds(**kw):
    return _serde.open_tile(make_geotiff_bytes(**kw))


def test_width_height_numbands():
    with _ds(width=4, height=3, count=2) as ds:
        assert accessors.width(ds) == 4
        assert accessors.height(ds) == 3
        assert accessors.numbands(ds) == 2


def test_srid():
    with _ds(epsg=4326) as ds:
        assert accessors.srid(ds) == 4326


def test_pixel_size():
    with _ds() as ds:
        assert accessors.pixelwidth(ds) == 0.5
        assert accessors.pixelheight(ds) == -0.5


def test_boundingbox_wkb():
    with _ds(width=4, height=3) as ds:
        geom = shapely.wkb.loads(accessors.boundingbox(ds))
    # origin (10,50), 0.5 px → (10, 48.5) .. (12, 50)
    assert geom.bounds == (10.0, 48.5, 12.0, 50.0)


def test_metadata_contains_dimensions():
    with _ds(width=4, height=3) as ds:
        meta = accessors.metadata(ds)
    assert meta["width"] == "4"
    assert meta["height"] == "3"
    assert meta["driver"] == "GTiff"
