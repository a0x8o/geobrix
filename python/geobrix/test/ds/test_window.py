"""Tests for the window_for_bbox clip-safe AOI windowing primitive."""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from rasterio.warp import transform_bounds

from databricks.labs.gbx.ds._window import window_for_bbox


def make_geotiff_bytes(width=4, height=3, count=1, epsg=4326, nodata=-9999.0):
    """Return in-memory single/multi-band GTiff bytes with a known georeference.

    Origin (ulx, uly) = (10.0, 50.0); pixel size 0.5 x 0.5 (north-up).
    So extent = (10.0, 50.0 - 0.5*height) .. (10.0 + 0.5*width, 50.0).
    """
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=nodata,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def _open(width=4, height=3, epsg=4326):
    # Fixture extent: origin (10.0, 50.0), 0.5 px -> x[10,12], y[48.5,50] in EPSG:4326.
    mf = MemoryFile(make_geotiff_bytes(width=width, height=height, epsg=epsg))
    return mf, mf.open()


def test_fully_inside_window_matches_bbox():
    mf, ds = _open()
    try:
        win = window_for_bbox(
            ds, (10.5, 49.0, 11.5, 50.0)
        )  # inside x[10,12], y[48.5,50]
        assert win is not None
        b = rasterio.windows.bounds(win, ds.transform)  # (left, bottom, right, top)
        assert b == (10.5, 49.0, 11.5, 50.0)
    finally:
        ds.close()
        mf.close()


def test_north_overhang_is_clipped_not_shifted():
    # Regression for the NB-02 georef bug: a bbox whose top (51.0) is north of the
    # dataset top (50.0) must clip to the dataset top, NOT report row 0 at 51.0.
    mf, ds = _open()
    try:
        win = window_for_bbox(ds, (10.5, 49.0, 11.5, 51.0))
        assert win is not None
        top = rasterio.windows.bounds(win, ds.transform)[3]
        assert top == 50.0, f"top should clip to dataset top 50.0, got {top}"
        assert win.row_off == 0
    finally:
        ds.close()
        mf.close()


def test_no_overlap_returns_none():
    mf, ds = _open()
    try:
        assert window_for_bbox(ds, (20.0, 20.0, 21.0, 21.0)) is None
    finally:
        ds.close()
        mf.close()


def test_bbox_crs_is_reprojected():
    # Source in EPSG:3857 over a known SF extent; a WGS84 bbox inside it must be
    # transformed to 3857 before windowing (proves bbox_crs is applied).
    w, s, e, n = transform_bounds("EPSG:4326", "EPSG:3857", -122.5, 37.7, -122.4, 37.8)
    from rasterio.transform import from_bounds as _affine_from_bounds

    profile = dict(
        driver="GTiff",
        width=100,
        height=100,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=_affine_from_bounds(w, s, e, n, 100, 100),
    )
    with MemoryFile() as src_mf:
        with src_mf.open(**profile) as out:
            out.write(np.zeros((1, 100, 100), dtype="uint8"))
        data = src_mf.read()
    mf = MemoryFile(data)
    ds = mf.open()
    try:
        win = window_for_bbox(
            ds, (-122.47, 37.72, -122.43, 37.78), bbox_crs="EPSG:4326"
        )
        assert win is not None
        b = rasterio.windows.bounds(win, ds.transform)  # in source CRS (3857)
        exp = transform_bounds("EPSG:4326", "EPSG:3857", -122.47, 37.72, -122.43, 37.78)
        # within one source pixel (rounding to whole pixels)
        px = abs(ds.transform.a)
        assert all(abs(a - c) <= px for a, c in zip(b, exp))
    finally:
        ds.close()
        mf.close()
