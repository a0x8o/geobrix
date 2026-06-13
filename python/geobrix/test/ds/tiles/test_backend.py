import os
import tempfile

from pmtiles.reader import MmapSource, Reader
from pmtiles.tile import Compression, TileType, zxy_to_tileid

from databricks.labs.gbx.ds.tiles._header import build_header_info
from databricks.labs.gbx.ds.tiles.backend import PMTilesBackend
from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

PNG = b"\x89PNG\r\n\x1a\n"


def test_pmtiles_backend_round_trip():
    g = SlippyGrid()
    tiles = [(6, 32, 21), (6, 33, 21)]
    # sorted-by-tileid stream of (tileid, bytes)
    payload = {t: PNG + bytes([i]) for i, t in enumerate(tiles)}
    stream = sorted(
        ((zxy_to_tileid(z, x, y), payload[(z, x, y)]) for (z, x, y) in tiles)
    )
    info = build_header_info(tiles, g, TileType.PNG, Compression.NONE, {"name": "t"})

    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "shard.pmtiles")
        PMTilesBackend().assemble(iter(stream), info, out)
        assert os.path.getsize(out) > 0
        with open(out, "rb") as f:
            r = Reader(MmapSource(f))
            assert r.get(6, 32, 21) == payload[(6, 32, 21)]
            assert r.get(6, 33, 21) == payload[(6, 33, 21)]
            assert r.header()["min_zoom"] == 6
            assert r.metadata()["name"] == "t"
