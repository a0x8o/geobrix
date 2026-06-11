"""PMTiles header assembly + tile-type sniffing from magic bytes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

from pmtiles.tile import Compression, TileType

from databricks.labs.gbx.ds.tiles.grid import BBox, Grid, TileKey


def sniff_tile_type(data: bytes) -> TileType:
    """Detect tile encoding from magic bytes; default MVT for vector payloads."""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return TileType.PNG
    if data[:3] == b"\xff\xd8\xff":
        return TileType.JPEG
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return TileType.WEBP
    if data[4:8] == b"ftyp" and b"avif" in data[8:20]:
        return TileType.AVIF
    # MVT is protobuf (often gzipped) with no reliable magic.
    return TileType.MVT


def _e7(v: float) -> int:
    return int(round(v * 1e7))


@dataclass
class HeaderInfo:
    tile_type: TileType
    tile_compression: Compression
    min_zoom: int
    max_zoom: int
    bbox: BBox
    metadata: Dict[str, object]

    def header_dict(self) -> Dict[str, object]:
        minlon, minlat, maxlon, maxlat = self.bbox
        clon = (minlon + maxlon) / 2.0
        clat = (minlat + maxlat) / 2.0
        return {
            "tile_type": self.tile_type,
            "tile_compression": self.tile_compression,
            "min_zoom": self.min_zoom,
            "max_zoom": self.max_zoom,
            "min_lon_e7": _e7(minlon),
            "min_lat_e7": _e7(minlat),
            "max_lon_e7": _e7(maxlon),
            "max_lat_e7": _e7(maxlat),
            "center_zoom": self.min_zoom,
            "center_lon_e7": _e7(clon),
            "center_lat_e7": _e7(clat),
        }


def build_header_info(
    tiles: Iterable[TileKey],
    grid: Grid,
    tile_type: TileType,
    tile_compression: Compression,
    metadata: Dict[str, object],
) -> HeaderInfo:
    """Compute min/max zoom + union bbox over a set of (z,x,y) tiles."""
    tiles = list(tiles)
    if not tiles:
        raise ValueError("build_header_info requires at least one tile")
    zs = [z for z, _, _ in tiles]
    minlon = minlat = float("inf")
    maxlon = maxlat = float("-inf")
    for z, x, y in tiles:
        bb = grid.tile_bbox(z, x, y)
        minlon, minlat = min(minlon, bb[0]), min(minlat, bb[1])
        maxlon, maxlat = max(maxlon, bb[2]), max(maxlat, bb[3])
    return HeaderInfo(
        tile_type=tile_type,
        tile_compression=tile_compression,
        min_zoom=min(zs),
        max_zoom=max(zs),
        bbox=(minlon, minlat, maxlon, maxlat),
        metadata=metadata,
    )
