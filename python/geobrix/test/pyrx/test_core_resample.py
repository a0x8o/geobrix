from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import resample

from .conftest import make_geotiff_bytes


def _open(out):
    return _serde.open_tile(out)


def test_resample_by_factor_upsamples_preserves_crs_and_extent():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        src_bounds = ds.bounds
        out = resample.resample_by_factor(ds, 2.0)
    with _open(out) as o:
        assert (o.width, o.height) == (8, 6)
        assert o.crs.to_epsg() == 4326
        # extent preserved (origin + full coverage)
        assert tuple(round(b, 6) for b in o.bounds) == tuple(round(b, 6) for b in src_bounds)


def test_resample_by_factor_downsamples():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=4, epsg=4326)) as ds:
        out = resample.resample_by_factor(ds, 0.5)
    with _open(out) as o:
        assert (o.width, o.height) == (2, 2)


def test_resample_to_size():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3)) as ds:
        out = resample.resample_to_size(ds, 10, 7)
    with _open(out) as o:
        assert (o.width, o.height) == (10, 7)


def test_resample_to_res():
    # extent 2.0 x 1.5 at 0.25 res -> 8 x 6
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = resample.resample_to_res(ds, 0.25, 0.25)
    with _open(out) as o:
        assert (o.width, o.height) == (8, 6)
