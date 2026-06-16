"""Pure-function tests for pyrx web-mercator XYZ tiling (core/xyz.py).

These exercise the Spark-free render/intersection logic directly on an open
rasterio DatasetReader — no Spark, no UDFs.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import xyz


def _make_rgb(width=64, height=64, epsg=4326, ulx=10.0, uly=50.0, res=0.03125):
    """A small RGB GTiff over a European extent (default ~lon 10..12, lat 48..50)."""
    transform = from_origin(ulx, uly, res, res)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=3,
        dtype="uint8",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    data = (np.arange(width * height) % 256).astype("uint8").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, 4):
                ds.write(data, b)
        return mf.read()


def _make_world(epsg=4326):
    """A coarse raster covering the whole WGS84 world (for count-guard tests)."""
    transform = from_origin(-180.0, 90.0, 10.0, 10.0)
    profile = dict(
        driver="GTiff",
        width=36,
        height=18,
        count=1,
        dtype="uint8",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((18, 36), dtype="uint8"), 1)
        return mf.read()


def _open(raster_bytes):
    mf = MemoryFile(raster_bytes)
    return mf, mf.open()


def _decode(png_bytes):
    with MemoryFile(png_bytes) as mf, mf.open() as ds:
        return ds.read()


# --- render_tile: in-extent -------------------------------------------------
def test_render_tile_in_extent_png():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        # z=5 tile (16, 10) covers lon~11.25, lat~48.9 — inside the fixture.
        out = xyz.render_tile(ds, 5, 16, 10, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    assert len(out) > 0


def test_render_tile_jpeg_webp():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        jpeg = xyz.render_tile(ds, 5, 16, 10, "JPEG", 256, "bilinear")
        webp = xyz.render_tile(ds, 5, 16, 10, "WEBP", 256, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert jpeg[:3] == b"\xff\xd8\xff"
    assert webp[:4] == b"RIFF"


# --- render_tile: out-of-extent -> transparent PNG --------------------------
def test_render_tile_out_of_extent_transparent_png():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        # z=2 (0,0) is the NW quadrant of the world — opposite a European fixture.
        out = xyz.render_tile(ds, 2, 0, 0, "PNG", 128, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 128, 128)  # RGBA
    assert arr[3].max() == 0  # alpha fully transparent


def test_render_tile_out_of_extent_returns_png_even_for_jpeg_request():
    # Mirror heavyweight: out-of-extent always yields a transparent PNG regardless
    # of the requested format.
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        out = xyz.render_tile(ds, 2, 0, 0, "JPEG", 64, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"


# --- validation -------------------------------------------------------------
def test_render_tile_bad_format_raises():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "GIF", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_render_tile_bad_size_raises():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "PNG", 5000, "bilinear")
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "PNG", 0, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_render_tile_bad_resampling_raises():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "PNG", 256, "wobble")
    finally:
        ds.close()
        mf.close()


def test_render_tile_format_resampling_case_insensitive():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        out = xyz.render_tile(ds, 5, 16, 10, "png", 256, "BILINEAR")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"


def test_transparent_png_size():
    out = xyz.transparent_png(200)
    arr = _decode(out)
    assert arr.shape == (4, 200, 200)
    assert arr[3].max() == 0


# --- intersecting_tiles / pyramid -------------------------------------------
def test_intersecting_tiles_and_pyramid_lengths_match():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        tiles = xyz.intersecting_tiles(ds, 1, 3)
        result = xyz.pyramid(ds, 1, 3, "PNG", 64, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert len(result) == len(tiles)
    assert len(result) > 0
    for el in result:
        assert el["z"] in (1, 2, 3)
        assert el["bytes"][:4] == b"\x89PNG"
        assert {"z", "x", "y", "bytes"} <= set(el.keys())


def test_tile_count_matches_intersecting():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        assert xyz.tile_count(ds, 1, 3) == len(xyz.intersecting_tiles(ds, 1, 3))
    finally:
        ds.close()
        mf.close()


# --- pyramid guards ---------------------------------------------------------
def test_pyramid_guard_min_z_negative():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, -1, 2, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_pyramid_guard_max_lt_min():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, 3, 2, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_pyramid_guard_max_zoom():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, 0, 21, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_pyramid_guard_tile_count_before_rendering():
    # A world-extent raster at z=20 implies ~1.1e12 tiles — must raise the
    # count guard WITHOUT rendering anything (fast).
    raster = _make_world()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, 0, 20, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()
