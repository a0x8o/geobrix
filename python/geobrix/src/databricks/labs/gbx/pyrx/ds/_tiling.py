"""Port of Scala BalancedSubdivision tile-grid math (power-of-4 split).

Pure integer math so the light reader emits the SAME number of tiles per
raster as the heavy reader (row-count parity). Mirrors
``BalancedSubdivision.getTileSize`` in
src/main/scala/.../rasterx/operations/BalancedSubdivision.scala.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np


def _mem_size_bytes(width: int, height: int, bands: int, dtype: str) -> int:
    """In-memory size of the raster, matching RasterAccessors.memSize."""
    return width * height * bands * int(np.dtype(dtype).itemsize)


def _num_splits_k(width: int, height: int, bands: int, dtype: str, size_mib: int) -> int:
    """Number of quad-split rounds k (nx=ny=2^k, tiles=4^k). Mirrors the Scala while-loop."""
    size_bytes = _mem_size_bytes(width, height, bands, dtype)
    limit = size_mib * 1024 * 1024
    k = 0
    while k < 9 and (size_bytes >> (2 * k)) > limit and (1 << (2 * (k + 1))) <= 512:
        k += 1
    return k


def tile_grid(width: int, height: int, bands: int, dtype: str, size_mib: int) -> Tuple[int, int, int, int]:
    """Return (nx, ny, tile_x, tile_y): grid divisions and per-tile pixel dims (ceil-div)."""
    k = _num_splits_k(width, height, bands, dtype, size_mib)
    nx = 1 << k
    ny = 1 << k
    tile_x = -(-width // nx)
    tile_y = -(-height // ny)
    return nx, ny, tile_x, tile_y


def plan_windows(width: int, height: int, bands: int, dtype: str, size_mib: int) -> List[Tuple[int, int, int, int]]:
    """List of (col_off, row_off, win_w, win_h) windows tiling the raster, no gaps/overlap."""
    _nx, _ny, tile_x, tile_y = tile_grid(width, height, bands, dtype, size_mib)
    windows: List[Tuple[int, int, int, int]] = []
    row_off = 0
    while row_off < height:
        win_h = min(tile_y, height - row_off)
        col_off = 0
        while col_off < width:
            win_w = min(tile_x, width - col_off)
            windows.append((col_off, row_off, win_w, win_h))
            col_off += tile_x
        row_off += tile_y
    return windows
