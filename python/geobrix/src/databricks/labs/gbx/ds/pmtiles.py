"""``pmtiles_gbx`` — pure-Python DataSource V2 PMTiles writer on the shared
tiled-output framework. Write-only. Default = sharded (shardZoom=6) with a
separate overview.pmtiles + a STAC manifest; shardZoom=0 = single archive."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

from databricks.labs.gbx.ds.tiles import shard as _shard
from databricks.labs.gbx.ds.tiles._header import build_header_info, sniff_tile_type
from databricks.labs.gbx.ds.tiles.backend import PMTilesBackend
from databricks.labs.gbx.ds.tiles.catalog import (
    ShardInfo,
    STACManifestCatalog,
    TileJSONCatalog,
)
from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

INPUT_SCHEMA = StructType(
    [
        StructField("z", IntegerType(), nullable=False),
        StructField("x", IntegerType(), nullable=False),
        StructField("y", IntegerType(), nullable=False),
        StructField("bytes", BinaryType(), nullable=False),
    ]
)

_COMPRESSION = {
    "none": Compression.NONE,
    "gzip": Compression.GZIP,
    "brotli": Compression.BROTLI,
    "zstd": Compression.ZSTD,
}
_TILETYPE = {
    "png": TileType.PNG,
    "jpeg": TileType.JPEG,
    "jpg": TileType.JPEG,
    "webp": TileType.WEBP,
    "avif": TileType.AVIF,
    "mvt": TileType.MVT,
}
_CATALOGS = {"stac": STACManifestCatalog, "tilejson": TileJSONCatalog}


def assert_input_schema(schema: StructType) -> None:
    names = [f.name for f in schema.fields]
    if names != ["z", "x", "y", "bytes"]:
        raise ValueError(
            "pmtiles_gbx requires exactly columns (z:int, x:int, y:int, "
            f"bytes:binary); got {names}"
        )


@dataclass
class PMTilesCommitMessage(WriterCommitMessage):
    bin_path: str
    idx_path: str


class PMTilesGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "pmtiles_gbx"

    def schema(self) -> StructType:
        return INPUT_SCHEMA

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        assert_input_schema(schema)
        path = self.options.get("path")
        if not path:
            raise ValueError(
                "pmtiles_gbx writer requires an output path (.save(path))."
            )
        return PMTilesGbxWriter(path, dict(self.options), overwrite)


class PMTilesGbxWriter(DataSourceWriter):
    def __init__(self, path: str, options: Dict[str, str], overwrite: bool):
        # PySpark DataSource V2 lowercases all option keys (e.g. shardZoom → shardzoom).
        # Normalise once so the rest of the class uses consistent names.
        opts = {k.lower(): v for k, v in options.items()}
        self.path = path
        self.overwrite = overwrite
        self.shard_zoom = int(opts.get("shardzoom", "6"))
        tps = opts.get("targettilespershard")
        self.target_tiles_per_shard = int(tps) if tps else None
        self.catalog_kind = opts.get("catalog", "stac").lower()
        if self.catalog_kind not in _CATALOGS and self.catalog_kind != "none":
            raise ValueError(f"unknown catalog {self.catalog_kind!r}")
        tt = opts.get("tiletype")
        self.tile_type_override = _TILETYPE[tt.lower()] if tt else None
        self.tile_compression = _COMPRESSION[
            opts.get("tilecompression", "none").lower()
        ]
        self.metadata = json.loads(opts["metadata"]) if opts.get("metadata") else {}
        self.grid = SlippyGrid()
        # For single-archive mode path is a .pmtiles file; scratch must live
        # beside it (in parent dir), not inside it.
        _scratch_base = (
            os.path.dirname(self.path) or "." if self.shard_zoom == 0 else self.path
        )
        self.scratch_dir = os.path.join(_scratch_base, "_scratch")

        if not self.overwrite and self._target_exists():
            raise ValueError(
                "pmtiles_gbx does not support append; a finalized archive cannot be "
                "appended to. Use .mode('overwrite')."
            )
        if self.overwrite:
            self._clear_target()

    # ---- driver-side path helpers (no Spark internals; pure os) ----
    def _is_single(self) -> bool:
        return self.shard_zoom == 0

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    def _clear_target(self) -> None:
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path)

    # ---- executor: stream bytes to indexed scratch ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        writer = _shard.ScratchWriter(self.scratch_dir)
        for row in iterator:
            z, x, y, data = int(row[0]), int(row[1]), int(row[2]), bytes(row[3])
            writer.add(z, x, y, zxy_to_tileid(z, x, y), data)
        bin_path, idx_path = writer.close()
        return PMTilesCommitMessage(bin_path=bin_path, idx_path=idx_path)

    # ---- driver: assemble shards + catalog from entries ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        entries: List[_shard.Entry] = []
        for msg in messages:
            if isinstance(msg, PMTilesCommitMessage):
                entries.extend(_shard.read_entries(msg.idx_path, self.scratch_dir))
        try:
            if not entries:
                return
            if self._is_single():
                self._assemble_single(entries)
            else:
                self._assemble_sharded(entries)
        finally:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)

    def _tile_type(self, sample: bytes) -> TileType:
        return self.tile_type_override or sniff_tile_type(sample)

    def _assemble_single(self, entries: List[_shard.Entry]) -> None:
        # PySpark DataSource V2 always creates self.path as a directory before
        # commit() runs. For single-archive mode the user expects self.path to
        # be a file. Write to a sibling temp file then atomically replace.
        parent = os.path.dirname(self.path) or "."
        os.makedirs(parent, exist_ok=True)
        tmp_path = self.path + "._tmp"
        tiles = [(e.z, e.x, e.y) for e in entries]
        sample = next(iter(_shard.stream_sorted(entries[:1])))[1]
        info = build_header_info(
            tiles,
            self.grid,
            self._tile_type(sample),
            self.tile_compression,
            self.metadata,
        )
        PMTilesBackend().assemble(_shard.stream_sorted(entries), info, tmp_path)
        # Remove the directory Spark created, then put the file in its place.
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)
        elif os.path.isfile(self.path):
            os.remove(self.path)
        os.rename(tmp_path, self.path)

    def _assemble_sharded(self, entries: List[_shard.Entry]) -> None:
        tileset = os.path.join(self.path, "tileset")
        os.makedirs(tileset, exist_ok=True)
        groups = _shard.assign_shards(
            entries, self.shard_zoom, self.grid, self.target_tiles_per_shard
        )
        shard_infos: List[ShardInfo] = []
        for key, group in groups.items():
            sample = next(iter(_shard.stream_sorted(group[:1])))[1]
            tiles = [(e.z, e.x, e.y) for e in group]
            info = build_header_info(
                tiles,
                self.grid,
                self._tile_type(sample),
                self.tile_compression,
                self.metadata,
            )
            if key == _shard.OVERVIEW:
                rel = "overview.pmtiles"
            else:
                sz, sx, sy = key
                rel = os.path.join(str(sz), str(sx), f"{sy}.pmtiles")
            out_path = os.path.join(tileset, rel)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            PMTilesBackend().assemble(_shard.stream_sorted(group), info, out_path)
            shard_infos.append(ShardInfo(rel, info.min_zoom, info.max_zoom, info.bbox))
        if self.catalog_kind != "none":
            _CATALOGS[self.catalog_kind]().write(shard_infos, tileset)

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        shutil.rmtree(self.scratch_dir, ignore_errors=True)
        self._clear_target()
