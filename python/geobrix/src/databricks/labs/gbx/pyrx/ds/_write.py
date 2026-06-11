"""Per-tile byte production for the raster writer.

Hybrid, mirroring the heavy gdal writer's intent (encoding from tile.metadata):
- target driver GTiff (the common case; raster_gbx/gtiff_gbx tiles are already
  GTiff) -> pass tile.raster bytes through VERBATIM. Pixel-identical to heavy;
  heavy's specific creation options differ but our contract is decoded-pixel.
- non-GTiff target -> rasterio re-encode to that driver applying the
  metadata-derived compression/blocksize/zlevel/zstd, and stamp RASTERX_<key>
  (each metadata entry) + RASTERX_CELL (cellid), matching heavy SetMetadataItem.

Writer .option()s never carry encoding; only tile.metadata does (like heavy).
"""
from __future__ import annotations

from typing import Dict


def _is_float(dtype: str) -> bool:
    return str(dtype).startswith("float")


def _creation_opts(driver: str, meta: Dict[str, str], dtype: str) -> Dict[str, str]:
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
    blk = max(64, (blk // 16) * 16)
    if driver.upper() == "COG":
        opts["blocksize"] = str(blk)
    return opts


def tile_to_bytes(
    cellid: int,
    raster_bytes: bytes,
    metadata: Dict[str, str],
    force_driver: str = None,
) -> bytes:
    """Return the on-disk bytes for one tile (verbatim GTiff, else re-encode)."""
    driver = force_driver or metadata.get("driver") or metadata.get("format") or "GTiff"
    if str(driver).upper() == "GTIFF":
        return raster_bytes

    import rasterio
    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as src_mf, src_mf.open() as src:
        data = src.read()
        profile = src.profile.copy()
        profile["driver"] = driver
        profile.update(_creation_opts(driver, metadata, src.dtypes[0]))
        with MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                dst.write(data)
                tags = {f"RASTERX_{k}": str(v) for k, v in (metadata or {}).items()}
                tags["RASTERX_CELL"] = str(cellid)
                dst.update_tags(**tags)
            return out_mf.read()
