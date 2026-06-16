import tempfile

from pmtiles.tile import zxy_to_tileid

from databricks.labs.gbx.ds.tiles.grid import SlippyGrid
from databricks.labs.gbx.ds.tiles.shard import (
    OVERVIEW,
    ScratchWriter,
    assign_shards,
    read_entries,
    stream_sorted,
)


def _write_scratch(scratch_dir, rows):
    w = ScratchWriter(scratch_dir)
    for z, x, y, data in rows:
        w.add(z, x, y, zxy_to_tileid(z, x, y), data)
    return w.close()  # (bin_path, idx_path)


def test_scratch_round_trip_and_stream_sorted():
    with tempfile.TemporaryDirectory() as d:
        rows = [
            (6, 33, 21, b"B"),
            (6, 32, 21, b"A"),
            (7, 64, 42, b"C"),
        ]
        _bin, idx = _write_scratch(d, rows)
        entries = read_entries(idx, d)
        assert len(entries) == 3
        streamed = list(stream_sorted(entries))
        # ascending tileid
        ids = [tid for tid, _ in streamed]
        assert ids == sorted(ids)
        assert {data for _, data in streamed} == {b"A", b"B", b"C"}


def test_fixed_assignment_and_overview_split():
    g = SlippyGrid()
    with tempfile.TemporaryDirectory() as d:
        rows = [
            (6, 32, 21, b"a"),  # body shard (6,32,21)
            (8, 130, 85, b"b"),  # body, parent (6, 32, 21)
            (3, 4, 2, b"o"),  # overview (z<6)
        ]
        _bin, idx = _write_scratch(d, rows)
        entries = read_entries(idx, d)
        groups = assign_shards(entries, shard_zoom=6, grid=g)
        assert OVERVIEW in groups
        assert len(groups[OVERVIEW]) == 1
        body_keys = [k for k in groups if k != OVERVIEW]
        # both body tiles share parent (6,32,21)
        assert body_keys == [(6, 32, 21)]
        assert len(groups[(6, 32, 21)]) == 2


def test_adaptive_subdivides_dense_cells():
    g = SlippyGrid()
    with tempfile.TemporaryDirectory() as d:
        # 4 z8 tiles under (6,32,21) but in two distinct z7 children
        rows = [
            (8, 128, 84, b"1"),
            (8, 129, 84, b"2"),
            (8, 130, 86, b"3"),
            (8, 131, 86, b"4"),
        ]
        _bin, idx = _write_scratch(d, rows)
        entries = read_entries(idx, d)
        # target 2 per shard -> base z6 cell (4 tiles) must subdivide
        groups = assign_shards(entries, shard_zoom=6, grid=g, target_tiles_per_shard=2)
        assert all(len(v) <= 2 for v in groups.values())
        # variable zoom: at least one shard deeper than 6
        assert any(k[0] > 6 for k in groups)
