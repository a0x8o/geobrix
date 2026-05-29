import os

from databricks.labs.gbx.pyrx import _env


def test_configure_gdal_env_sets_gdal_data():
    # Clear to prove configure_gdal_env populates from the rasterio wheel.
    os.environ.pop("GDAL_DATA", None)
    _env.configure_gdal_env()
    assert os.environ.get(
        "GDAL_DATA"
    ), "GDAL_DATA should be set from rasterio's bundled data"
    assert os.path.isdir(os.environ["GDAL_DATA"])


def test_configure_gdal_env_is_idempotent_and_respects_existing():
    os.environ["GDAL_DATA"] = "/tmp/preset-gdal-data"
    _env.configure_gdal_env()
    assert os.environ["GDAL_DATA"] == "/tmp/preset-gdal-data"


def test_assert_rasterio_available_returns_versions():
    gdal_ver, rio_ver = _env.assert_rasterio_available()
    assert gdal_ver and rio_ver
