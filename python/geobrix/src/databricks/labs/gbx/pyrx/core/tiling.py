"""Spark-free tiling ops. Each returns a list of GTiff byte strings (one per
output tile); the Spark layer wraps each into a tile struct."""

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


def _get_tile_size(width, height, size_bytes, size_in_mb):
    """Power-of-4 tile dimensions, ported from heavy BalancedSubdivision.getTileSize.

    Finds the smallest number of quad-split rounds ``k`` such that the per-tile
    byte size ``size_bytes >> (2*k)`` no longer exceeds the MB limit, capped so
    the split count ``4**(k+1)`` stays within 512.  The raster is then split
    into a ``2**k x 2**k`` grid via ceil-div tile dimensions.

    ``size_bytes`` is the *encoded* raster byte length (heavy keys on GDAL's
    in-memory file size, i.e. the serialized GTiff buffer length), NOT the raw
    width*height*bands*itemsize pixel-array size.
    """
    limit = int(size_in_mb) * 1024 * 1024
    k = 0
    while k < 9 and (size_bytes >> (2 * k)) > limit and (1 << (2 * (k + 1))) <= 512:
        k += 1
    nx = ny = 1 << k
    tile_x = (width + nx - 1) // nx  # ceil-div
    tile_y = (height + ny - 1) // ny
    return tile_x, tile_y


def _encoded_size_bytes(ds) -> int:
    """Serialized GTiff byte length of ``ds`` -- the analog of heavy's memSize.

    Used when the caller did not supply ``size_bytes``; re-encodes the open
    dataset to an in-memory GTiff and measures the buffer length, matching the
    vsimem buffer size heavy reads via GetMemFileBuffer.
    """
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    return len(_write(profile, ds.read()))


def make_tiles(ds, size_in_mb, size_bytes=None) -> list:
    """Split a raster into a power-of-4 grid of tiles, matching heavy rst_maketiles.

    Aligned to heavy BalancedSubdivision: keyed on the encoded raster byte size
    versus the MB limit, the raster is quad-split ``k`` times into a
    ``2**k x 2**k`` grid (so 1, 4, 16, 64, ... tiles).  Returns one tile when the
    full raster already fits the budget.

    ``size_in_mb`` is truncated to an integer to mirror heavy, whose Catalyst
    cast to Int drops the fraction (so e.g. 0.7 -> 0 -> a single tile).

    ``size_bytes`` is the encoded raster byte length; callers that already hold
    the raster bytes (the Spark UDF) should pass it so the split count matches
    heavy exactly.  When omitted it is derived by re-encoding ``ds`` to GTiff.
    """
    size_in_mb = int(size_in_mb)  # heavy casts sizeInMB to Int (truncates)
    if size_in_mb <= 0:
        return retile(ds, ds.width, ds.height)
    if size_bytes is None:
        size_bytes = _encoded_size_bytes(ds)
    tile_x, tile_y = _get_tile_size(ds.width, ds.height, size_bytes, size_in_mb)
    return retile(ds, tile_x, tile_y)
