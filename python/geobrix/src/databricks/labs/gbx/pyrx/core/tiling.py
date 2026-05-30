"""Spark-free tiling ops. Each returns a list of GTiff byte strings (one per
output tile); the Spark layer wraps each into a tile struct."""

import math

import numpy as np
from rasterio.io import MemoryFile
from rasterio.windows import Window


def _write(profile, data) -> bytes:
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def separate_bands(ds) -> list:
    out = []
    for i in range(1, ds.count + 1):
        profile = ds.profile.copy()
        profile.update(driver="GTiff", count=1)
        out.append(_write(profile, ds.read(i)[np.newaxis, :, :]))
    return out


def _window_tiles(ds, tile_width, tile_height, step_x, step_y) -> list:
    tw, th = int(tile_width), int(tile_height)
    out = []
    row = 0
    while row < ds.height:
        col = 0
        while col < ds.width:
            w = min(tw, ds.width - col)
            h = min(th, ds.height - row)
            if w > 0 and h > 0:
                win = Window(col, row, w, h)
                data = ds.read(window=win)
                profile = ds.profile.copy()
                profile.update(
                    driver="GTiff",
                    width=w,
                    height=h,
                    transform=ds.window_transform(win),
                )
                out.append(_write(profile, data))
            col += step_x
        row += step_y
    return out


def retile(ds, tile_width, tile_height) -> list:
    tw, th = int(tile_width), int(tile_height)
    return _window_tiles(ds, tw, th, tw, th)


def to_overlapping_tiles(ds, tile_width, tile_height, overlap) -> list:
    tw, th, ov = int(tile_width), int(tile_height), int(overlap)
    return _window_tiles(ds, tw, th, max(1, tw - ov), max(1, th - ov))


def make_tiles(ds, size_in_mb) -> list:
    """Split a raster into tiles sized to fit approximately size_in_mb each.

    Derives a square tile side from the target MB budget and the raster's
    bytes-per-pixel, then delegates to retile.  Returns one tile when the
    budget exceeds the full raster extent.
    """
    bytes_per_pixel = max(1, np.dtype(ds.dtypes[0]).itemsize * ds.count)
    budget = max(1.0, float(size_in_mb) * 1024 * 1024)
    pixels = budget / bytes_per_pixel
    side = max(1, int(math.sqrt(pixels)))
    return retile(ds, side, side)
