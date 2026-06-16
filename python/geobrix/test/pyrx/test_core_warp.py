from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import warp

from .conftest import make_geotiff_bytes


def test_reproject_to_srid_changes_crs_preserves_bands():
    with _serde.open_tile(make_geotiff_bytes(epsg=4326, count=2)) as ds:
        out = warp.reproject_to_srid(ds, 3857)
    with _serde.open_tile(out) as ds2:
        assert ds2.crs.to_epsg() == 3857
        assert ds2.count == 2
        assert ds2.width > 0 and ds2.height > 0


def test_reproject_to_srid_resampling_arg_accepted():
    with _serde.open_tile(make_geotiff_bytes(epsg=4326)) as ds:
        out = warp.reproject_to_srid(ds, 3857, resampling="bilinear")
    with _serde.open_tile(out) as ds2:
        assert ds2.crs.to_epsg() == 3857
