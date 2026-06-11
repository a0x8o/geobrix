"""Windowed GTiff(DEFLATE) re-encode + 11-key metadata, matching the heavy reader.

Mirrors RasterDriver.writeToBytes (always GTiff/DEFLATE on the wire) and
WindowedExtract metadata. tile.raster is NOT raw source bytes.
"""
from __future__ import annotations

from typing import Dict, Tuple

import rasterio
from rasterio.io import MemoryFile
from rasterio.windows import Window

CELLID_FRESH = -1  # GDAL_Reader.scala:30 writes -1L for un-tessellated tiles


def encode_tile(
    ds: "rasterio.DatasetReader",
    window: Tuple[int, int, int, int],
    source_path: str,
    all_parents: str,
    compression: str = "DEFLATE",
) -> Tuple[int, bytes, Dict[str, str]]:
    """Read one window, re-encode it as an in-memory GTiff, return (cellid, bytes, metadata)."""
    col_off, row_off, win_w, win_h = window
    rio_window = Window(col_off, row_off, win_w, win_h)
    data = ds.read(window=rio_window)

    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        width=win_w,
        height=win_h,
        compress=compression.lower(),
        transform=ds.window_transform(rio_window),
    )

    with MemoryFile() as mf:
        with mf.open(**profile) as out:
            out.write(data)
        raster_bytes = mf.read()

    metadata = {
        "path": f"/vsimem/light_{abs(hash((source_path, col_off, row_off))) & 0xffffffff}.tif",
        "sourcePath": source_path,
        "driver": "GTiff",
        "format": "GTiff",
        "last_command": f"windowed_extract -srcwin {col_off} {row_off} {win_w} {win_h}",
        "last_error": "",
        "all_parents": f"{source_path};{all_parents}",
        "size": "-1",
        "compression": compression,
        "isZipped": "false",
        "isSubset": "false",
    }
    return CELLID_FRESH, raster_bytes, metadata
