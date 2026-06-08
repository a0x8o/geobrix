import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import features


def _raster_with_block():
    # 4x4 float32, all NoData (-9999) except a 2x2 block of 5.0 in the middle.
    data = np.full((4, 4), -9999.0, dtype="float32")
    data[1:3, 1:3] = 5.0
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def _continuous_ramp_raster(n=32):
    # n x n float32 ramp with a distinct value per pixel in [0, 1).
    # Truncated to int32 (GDALPolygonize semantics) every pixel -> class 0,
    # so it must collapse to a single region, NOT one polygon per pixel.
    idx = np.arange(n * n, dtype="float32").reshape(n, n)
    data = (idx / (n * n)).astype("float32")  # all values in [0, 1) -> int 0
    profile = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, n, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def test_polygonize_groups_integer_regions_on_continuous_band():
    # GDALPolygonize truncates pixel values to int before grouping contiguous
    # equal-value regions. A [0, 1) ramp must collapse to a single class-0
    # region -- NOT one polygon per pixel (the float-equality bug).
    n = 32
    with _serde.open_tile(_continuous_ramp_raster(n)) as ds:
        results = features.polygonize(ds, band=1, connectedness=4)
    assert len(results) <= 4, f"expected a handful of regions, got {len(results)}"
    assert len(results) < n * n  # definitely not one-per-pixel
    # all pixels truncate to int 0
    assert {v for _, v in results} == {0.0}


def test_polygonize_returns_geom_value_excluding_nodata():
    with _serde.open_tile(_raster_with_block()) as ds:
        results = features.polygonize(ds, band=1, connectedness=4)
    # at least one polygon, the 5.0 block; nodata excluded
    values = [v for _, v in results]
    assert 5.0 in values
    assert -9999.0 not in values
    # geom_wkb entries load as valid shapely geometries
    geom_wkb, value = next((g, v) for g, v in results if v == 5.0)
    poly = shapely.wkb.loads(geom_wkb)
    assert poly.is_valid and poly.area > 0
