"""Spark-free tests for the light PMTiles archive assembler."""

import pytest
from pmtiles.reader import MmapSource, Reader

from databricks.labs.gbx.pmtiles._agg_light import _MAX_ARCHIVE_BYTES, _assemble_archive

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16  # sniffs as PNG


def _mvt(i):  # arbitrary non-magic bytes => sniffs as MVT
    return b"mvt-payload-" + bytes([i % 256]) + b"\x00\x01\x02"


def _decode(blob, tmp_path):
    p = tmp_path / "a.pmtiles"
    p.write_bytes(blob)
    out = {}
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        for z in range(0, 6):
            n = 2**z
            for x in range(n):
                for y in range(n):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out


def test_single_tile_roundtrip(tmp_path):
    blob = _assemble_archive([_mvt(1)], [3], [2], [4], {})
    assert blob is not None
    assert _decode(blob, tmp_path) == {(3, 2, 4): _mvt(1)}


def test_multi_zoom_roundtrip(tmp_path):
    data = [_mvt(1), _mvt(2), _mvt(3)]
    zs, xs, ys = [2, 3, 3], [1, 2, 5], [1, 4, 6]
    got = _decode(_assemble_archive(data, zs, xs, ys, {}), tmp_path)
    assert got == {(2, 1, 1): _mvt(1), (3, 2, 4): _mvt(2), (3, 5, 6): _mvt(3)}


def test_png_payload_roundtrip(tmp_path):
    got = _decode(_assemble_archive([_PNG], [1], [0], [0], {}), tmp_path)
    assert got == {(1, 0, 0): _PNG}


def test_metadata_roundtrip(tmp_path):
    blob = _assemble_archive([_mvt(1)], [0], [0], [0], {"name": "demo", "n": 1})
    p = tmp_path / "m.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        assert r.metadata().get("name") == "demo"


def test_null_payloads_skipped(tmp_path):
    got = _decode(
        _assemble_archive([None, _mvt(2), None], [0, 1, 0], [0, 1, 0], [0, 1, 0], {}),
        tmp_path,
    )
    assert got == {(1, 1, 1): _mvt(2)}


def test_empty_group_returns_none():
    assert _assemble_archive([], [], [], [], {}) is None
    assert _assemble_archive([None], [0], [0], [0], {}) is None


def test_duplicate_tileid_dropped(tmp_path):
    # two rows for the same (z,x,y): keep first, no Writer error
    got = _decode(
        _assemble_archive([_mvt(1), _mvt(9)], [2, 2], [1, 1], [1, 1], {}), tmp_path
    )
    assert got == {(2, 1, 1): _mvt(1)}


def test_cap_exceeded_raises():
    big = b"\x00" * (_MAX_ARCHIVE_BYTES + 1)
    with pytest.raises(ValueError, match="exceeds"):
        _assemble_archive([big], [0], [0], [0], {})
