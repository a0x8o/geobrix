"""Per-tile byte production for the raster writer.

Hybrid, mirroring the heavy gdal writer's intent (encoding from tile.metadata):
- target driver GTiff (the common case; raster_gbx/gtiff_gbx tiles are already
  GTiff) -> pass tile.raster bytes through VERBATIM. Pixel-identical to heavy;
  heavy's specific creation options differ but our contract is decoded-pixel.
- target driver COG -> rasterio re-encode applying metadata-derived
  compression/blocksize/zlevel/zstd, and stamp RASTERX_<key> + RASTERX_CELL.

Only GTiff (verbatim pass-through) and COG (re-encode, GTiff-structured) are
validated end-to-end. Other drivers (PNG, PNM, Zarr, etc.) are passed
best-effort to rasterio with the metadata-derived options and are NOT
guaranteed to match heavy (heavy special-cases dtype coercion for PNG, etc.).

Writer .option()s never carry encoding; only tile.metadata does (like heavy).
"""

from __future__ import annotations

from typing import Dict, Optional


def _is_float(dtype: str) -> bool:
    return str(dtype).startswith("float")


def _creation_opts(
    driver: str, meta: Dict[str, str], dtype: str, width: int, height: int
) -> Dict[str, str]:
    """GTiff/COG creation options from tile metadata, mirroring OperatorOptions.appendOptions."""
    compression = str(meta.get("compression", "DEFLATE")).upper()
    opts: Dict[str, str] = {"compress": compression}
    if compression == "DEFLATE":
        opts["zlevel"] = str(meta.get("zlevel", "6"))
        opts["predictor"] = "3" if _is_float(dtype) else "2"
    elif compression == "ZSTD":
        opts["zstd_level"] = str(meta.get("zstd_level", "9"))
    elif compression == "LZW":
        opts["predictor"] = "3" if _is_float(dtype) else "2"
    try:
        blk = int(meta.get("blocksize", "512"))
    except ValueError:
        blk = 512
    blk = max(64, (min(blk, min(width, height)) // 16) * 16)
    blk = max(16, blk)  # never 0 for tiny rasters
    if driver.upper() == "COG":
        opts["blocksize"] = str(blk)
    return opts


def tile_to_bytes(
    cellid: int,
    raster_bytes: bytes,
    metadata: Dict[str, str],
    force_driver: Optional[str] = None,
) -> bytes:
    """Return the on-disk bytes for one tile (verbatim GTiff, else re-encode)."""
    driver = force_driver or metadata.get("driver") or metadata.get("format") or "GTiff"
    if str(driver).upper() == "GTIFF":
        return raster_bytes

    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as src_mf, src_mf.open() as src:
        data = src.read()
        profile = src.profile.copy()
        profile["driver"] = driver
        profile.update(
            _creation_opts(driver, metadata, src.dtypes[0], src.width, src.height)
        )
        with MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                dst.write(data)
                tags = {f"RASTERX_{k}": str(v) for k, v in (metadata or {}).items()}
                tags["RASTERX_CELL"] = str(cellid)
                dst.update_tags(**tags)
            return out_mf.read()
