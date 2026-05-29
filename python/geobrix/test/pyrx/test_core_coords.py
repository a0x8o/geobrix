from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import coords

from .conftest import make_geotiff_bytes


def _ds(**kw):
    return _serde.open_tile(make_geotiff_bytes(**kw))


def test_raster_to_world_x_y():
    # pixel (0,0) center = origin + half a pixel: (10 + 0.25, 50 - 0.25)
    with _ds() as ds:
        assert coords.raster_to_world_x(ds, 0, 0) == 10.25
        assert coords.raster_to_world_y(ds, 0, 0) == 49.75


def test_world_to_raster_x_y():
    # a world point inside pixel (col=0,row=0)
    with _ds() as ds:
        assert coords.world_to_raster_x(ds, 10.1, 49.9) == 0
        assert coords.world_to_raster_y(ds, 10.1, 49.9) == 0
        # one pixel to the right / down
        assert coords.world_to_raster_x(ds, 10.6, 49.9) == 1
        assert coords.world_to_raster_y(ds, 10.1, 49.4) == 1
