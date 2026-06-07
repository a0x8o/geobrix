"""Corpus manifest model: tiles + scale metadata, JSON (de)serialized."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class TileEntry:
    path: str  # path relative to the corpus root
    cellid: int
    srid: int
    dtype: str  # "uint8" | "int16" | "float32"
    bands: int
    tile_px: int  # square tile edge in pixels
    nodata_frac: float


@dataclass(frozen=True)
class RowPool:
    tile_px: int
    bands: int
    dtype: str
    tiles: List[TileEntry]  # ordered; runner takes the first N for each row count


@dataclass(frozen=True)
class Corpus:
    seed: int
    size_sweep: List[
        TileEntry
    ]  # one tile per (tile_px, bands, dtype, srid, nodata_frac) point
    row_pool: RowPool  # tiles for the spark-path row-count sweep

    def write(self, path) -> None:
        Path(path).write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def read(cls, path) -> "Corpus":
        d = json.loads(Path(path).read_text())
        return cls(
            seed=d["seed"],
            size_sweep=[TileEntry(**t) for t in d["size_sweep"]],
            row_pool=RowPool(
                tile_px=d["row_pool"]["tile_px"],
                bands=d["row_pool"]["bands"],
                dtype=d["row_pool"]["dtype"],
                tiles=[TileEntry(**t) for t in d["row_pool"]["tiles"]],
            ),
        )
