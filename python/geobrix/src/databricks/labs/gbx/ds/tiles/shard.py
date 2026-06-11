"""Entries-driven sharding. ``write()`` (executor) appends tile bytes to a
per-partition indexed scratch (bytes file + entries index) with NO shard
assignment. ``commit()`` (driver) reads only the entries metadata, assigns each
tile to a shard (fixed or adaptive), then streams each shard's bytes back in
tileid order — tile bytes never load on the driver in bulk."""

from __future__ import annotations

import json
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

from databricks.labs.gbx.ds.tiles.grid import Grid, TileKey

OVERVIEW = "overview"
_MAX_SHARD_ZOOM = 14  # cap adaptive subdivision depth


@dataclass
class Entry:
    z: int
    x: int
    y: int
    tileid: int
    offset: int
    length: int
    bin_path: str


class ScratchWriter:
    """Append tile bytes to one partition's scratch bin + collect an index."""

    def __init__(self, scratch_dir: str):
        os.makedirs(scratch_dir, exist_ok=True)
        uid = uuid.uuid4().hex
        self.bin_path = os.path.join(scratch_dir, f"part-{uid}.bin")
        self.idx_path = os.path.join(scratch_dir, f"part-{uid}.idx")
        self._f = open(self.bin_path, "wb")
        self._entries: List[Tuple[int, int, int, int, int, int]] = []
        self._offset = 0

    def add(self, z: int, x: int, y: int, tileid: int, data: bytes) -> None:
        self._f.write(data)
        self._entries.append((z, x, y, tileid, self._offset, len(data)))
        self._offset += len(data)

    def close(self) -> Tuple[str, str]:
        self._f.close()
        with open(self.idx_path, "w") as fh:
            json.dump(
                {"bin": os.path.basename(self.bin_path), "entries": self._entries},
                fh,
            )
        return self.bin_path, self.idx_path


def read_entries(idx_path: str, scratch_dir: str) -> List[Entry]:
    with open(idx_path) as fh:
        doc = json.load(fh)
    bin_path = os.path.join(scratch_dir, doc["bin"])
    return [
        Entry(z, x, y, tid, off, length, bin_path)
        for (z, x, y, tid, off, length) in doc["entries"]
    ]


def assign_shards(
    entries: List[Entry],
    shard_zoom: int,
    grid: Grid,
    target_tiles_per_shard: Optional[int] = None,
) -> Dict[object, List[Entry]]:
    """Group entries into shards. ``z < shard_zoom`` go to OVERVIEW; the rest are
    keyed by fixed parent or adaptively subdivided. Keys are (sz,sx,sy) tuples."""
    overview = [e for e in entries if e.z < shard_zoom]
    body = [e for e in entries if e.z >= shard_zoom]

    if target_tiles_per_shard is None:
        groups: Dict[object, List[Entry]] = defaultdict(list)
        for e in body:
            groups[grid.parent(e.z, e.x, e.y, shard_zoom)].append(e)
        result: Dict[object, List[Entry]] = dict(groups)
    else:
        result = _adaptive(body, shard_zoom, grid, target_tiles_per_shard)

    if overview:
        result[OVERVIEW] = overview
    return result


def _adaptive(
    entries: List[Entry], base_zoom: int, grid: Grid, target: int
) -> Dict[object, List[Entry]]:
    result: Dict[object, List[Entry]] = {}

    def recurse(zoom: int, cell_entries: List[Entry]) -> None:
        if len(cell_entries) <= target or zoom >= _MAX_SHARD_ZOOM:
            e0 = cell_entries[0]
            result[grid.parent(e0.z, e0.x, e0.y, zoom)] = cell_entries
            return
        buckets: Dict[TileKey, List[Entry]] = defaultdict(list)
        for e in cell_entries:
            buckets[grid.parent(e.z, e.x, e.y, zoom + 1)].append(e)
        # If subdivision did not actually split (all entries clamp to <= zoom),
        # keep them here to avoid infinite recursion.
        if len(buckets) == 1 and zoom + 1 > max(e.z for e in cell_entries):
            e0 = cell_entries[0]
            result[grid.parent(e0.z, e0.x, e0.y, zoom)] = cell_entries
            return
        for sub in buckets.values():
            recurse(zoom + 1, sub)

    base: Dict[TileKey, List[Entry]] = defaultdict(list)
    for e in entries:
        base[grid.parent(e.z, e.x, e.y, base_zoom)].append(e)
    for cell in base.values():
        recurse(base_zoom, cell)
    return result


def stream_sorted(entries: List[Entry]) -> Iterator[Tuple[int, bytes]]:
    """Yield (tileid, bytes) in ascending tileid order, reading from scratch bins.
    Duplicate tileids (should not occur in non-overlapping shards) are dropped."""
    ordered = sorted(entries, key=lambda e: e.tileid)
    handles: Dict[str, object] = {}
    seen = set()
    try:
        for e in ordered:
            if e.tileid in seen:
                continue
            seen.add(e.tileid)
            fh = handles.get(e.bin_path)
            if fh is None:
                fh = handles[e.bin_path] = open(e.bin_path, "rb")
            fh.seek(e.offset)
            yield e.tileid, fh.read(e.length)
    finally:
        for fh in handles.values():
            fh.close()
