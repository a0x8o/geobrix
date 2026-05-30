"""Pure-function tests for pyrx Analysis ports: proximity (scipy EDT) and
cog_convert (rio-cogeo)."""

import math

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import analysis

from .conftest import make_geotiff_bytes


def _single_source_bytes(width=5, height=5, src_row=0, src_col=0, pixel=1.0):
    """A raster that is all-zero except one source pixel (value != 0).

    Pixel size 1x1 so PIXEL and GEO distances coincide; CRS EPSG:32633.
    """
    data = np.zeros((height, width), dtype="float32")
    data[src_row, src_col] = pixel
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


# --- proximity --------------------------------------------------------------
def test_proximity_distance_increases_with_pixel_distance():
    src = _single_source_bytes(width=5, height=5, src_row=0, src_col=0)
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # source pixel -> 0 distance
        assert arr[0, 0] == pytest.approx(0.0)
        # one pixel right / down -> 1.0
        assert arr[0, 1] == pytest.approx(1.0)
        assert arr[1, 0] == pytest.approx(1.0)
        # diagonal corner -> sqrt(2)
        assert arr[1, 1] == pytest.approx(math.sqrt(2.0), rel=1e-5)


def test_proximity_output_is_float32_nodata_minus1():
    src = _single_source_bytes()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        assert o.dtypes[0] == "float32"
        assert o.nodata == -1.0


def test_proximity_geo_units_scale_by_pixel_size():
    # pixel size 0.5 (make_geotiff_bytes); single source at (0,0).
    width, height = 5, 5
    data = np.zeros((height, width), dtype="float32")
    data[0, 0] = 1.0
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        geo = analysis.proximity(ds, None, "GEO", None)
    with _serde.open_tile(geo) as o:
        arr = o.read(1)
        # one pixel right in GEO = 0.5 ground units (pixel width)
        assert arr[0, 1] == pytest.approx(0.5)
        assert arr[1, 0] == pytest.approx(0.5)


def test_proximity_preserves_georef():
    src = _single_source_bytes(width=5, height=5)
    with _serde.open_tile(src) as ds:
        src_transform = ds.transform
        src_crs = ds.crs
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        assert o.transform == src_transform
        assert o.crs == src_crs
        assert (o.width, o.height) == (5, 5)


def test_proximity_max_distance_marks_far_pixels_nodata():
    src = _single_source_bytes(width=5, height=5, src_row=0, src_col=0)
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", 1.5)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # within 1.5: source (0), adjacent (1), diagonal sqrt2 ~1.41 kept
        assert arr[0, 0] == pytest.approx(0.0)
        assert arr[0, 1] == pytest.approx(1.0)
        assert arr[1, 1] == pytest.approx(math.sqrt(2.0), rel=1e-5)
        # far corner (4,4) dist ~5.66 > 1.5 -> nodata
        assert arr[4, 4] == pytest.approx(-1.0)


def test_proximity_default_source_rule_nonzero():
    # default (target_values=None): source = pixels with value != 0.
    width, height = 4, 1
    data = np.array([[0.0, 7.0, 0.0, 0.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, None, "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert list(arr[0]) == pytest.approx([1.0, 0.0, 1.0, 2.0])


def test_proximity_target_values_selects_specific_values():
    # source pixels = only those whose value is in target_values.
    width, height = 4, 1
    data = np.array([[0.0, 7.0, 9.0, 0.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    # only value 9 is a source.
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, "9", "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # distances to col=2 (the only 9): [2,1,0,1]
        assert list(arr[0]) == pytest.approx([2.0, 1.0, 0.0, 1.0])


def test_proximity_target_values_comma_separated():
    width, height = 5, 1
    data = np.array([[0.0, 7.0, 0.0, 9.0, 0.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    with _serde.open_tile(src) as ds:
        out = analysis.proximity(ds, "7,9", "PIXEL", None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        # sources at col 1 and 3: distances [1,0,1,0,1]
        assert list(arr[0]) == pytest.approx([1.0, 0.0, 1.0, 0.0, 1.0])


def test_proximity_bad_distunits_raises():
    with _serde.open_tile(_single_source_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.proximity(ds, None, "BOGUS", None)


def test_proximity_bad_max_distance_raises():
    with _serde.open_tile(_single_source_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.proximity(ds, None, "PIXEL", 0.0)
        with pytest.raises(ValueError):
            analysis.proximity(ds, None, "PIXEL", -5.0)


# --- cog_convert ------------------------------------------------------------
def test_cog_convert_roundtrips_and_validates():
    src = make_geotiff_bytes(width=64, height=64)
    with _serde.open_tile(src) as ds:
        out = analysis.cog_convert(ds, "DEFLATE", 512, "AVERAGE")
    # reopens with rasterio
    with _serde.open_tile(out) as o:
        assert (o.width, o.height) == (64, 64)
    # validates as a COG
    try:
        from rio_cogeo.cogeo import cog_validate

        with MemoryFile(out) as mf:
            is_valid, errors, warnings = cog_validate(mf.name)
        assert is_valid, f"COG invalid: {errors}"
    except ImportError:
        pytest.skip("rio-cogeo not installed")


def test_cog_convert_honors_blocksize_and_compression():
    from rio_cogeo.cogeo import cog_info

    src = make_geotiff_bytes(width=256, height=256)
    with _serde.open_tile(src) as ds:
        out = analysis.cog_convert(ds, "LZW", 128, "NEAREST")
    with MemoryFile(out) as mf:
        info = cog_info(mf.name)
    assert tuple(info.IFD[0].Blocksize) == (128, 128)
    assert info.Compression.upper() == "LZW"


def test_cog_convert_bad_blocksize_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.cog_convert(ds, "DEFLATE", 0, "AVERAGE")


def test_cog_convert_empty_compression_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.cog_convert(ds, "", 512, "AVERAGE")


def test_cog_convert_unknown_compression_raises():
    with _serde.open_tile(make_geotiff_bytes()) as ds:
        with pytest.raises(ValueError):
            analysis.cog_convert(ds, "NOT_A_PROFILE", 512, "AVERAGE")
