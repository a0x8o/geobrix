from pmtiles.tile import Compression, TileType

from databricks.labs.gbx.ds.tiles._header import build_header_info, sniff_tile_type
from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 8
WEBP = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 4
GZIP_MVT = b"\x1f\x8b\x08\x00" + b"\x00" * 8


def test_sniff_known_types():
    assert sniff_tile_type(PNG) == TileType.PNG
    assert sniff_tile_type(JPEG) == TileType.JPEG
    assert sniff_tile_type(WEBP) == TileType.WEBP
    assert sniff_tile_type(GZIP_MVT) == TileType.MVT


def test_build_header_info_zoom_and_bbox():
    g = SlippyGrid()
    tiles = [(6, 32, 21), (6, 33, 21), (7, 64, 42)]
    info = build_header_info(tiles, g, TileType.PNG, Compression.NONE, {"name": "demo"})
    assert info.min_zoom == 6
    assert info.max_zoom == 7
    minlon, minlat, maxlon, maxlat = info.bbox
    assert minlon < maxlon and minlat < maxlat
    hd = info.header_dict()
    assert hd["min_zoom"] == 6 and hd["max_zoom"] == 7
    assert hd["tile_type"] == TileType.PNG
    assert hd["center_zoom"] == 6
    assert isinstance(hd["min_lon_e7"], int)
