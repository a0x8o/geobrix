"""raster_gbx — catch-all pure-Python DataSource V2 raster reader.

1:1 swap-out for the Scala ``gdal`` reader: recursively lists files, splits each
into BalancedSubdivision tiles, re-encodes each tile as GTiff, emits
(source, tile) rows matching pyrx._serde.TILE_SCHEMA. Pure Python (Serverless).

Fast path: when a source is a single whole-raster GTiff tile (the common
"directory of GeoTIFFs" case), the original file bytes are passed through
unchanged instead of being decoded + re-encoded — pixels are identical, so it
is parity-safe (decoded-pixel, not byte) and ~80x cheaper per tile.

Limitation: per-band masks/alpha and source colormaps are not yet propagated to
the re-encoded tiles (band data + nodata/dtype/crs/transform are). Sources that
rely on a colormap or per-band mask will differ structurally from the heavy
reader; tracked as a follow-up.
"""

from __future__ import annotations

from typing import Dict, Iterator, Sequence

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.ds import _encode, _listing


def reader_schema() -> StructType:
    """(source, tile) — tile from the single-source TILE_SCHEMA."""
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("tile", _serde.TILE_SCHEMA, nullable=False),
        ]
    )


# Max tile rows accumulated per emitted Arrow batch (bounds executor memory;
# the boundary win comes from amortizing the Python->JVM crossing over many rows).
_BATCH_ROWS = 64


class _FilePartition(InputPartition):
    """A group of source files = one partition (picklable).

    Defaults to one file per partition (max parallelism); ``maxFilesPerPartition``
    groups several so each ``read()`` can emit larger Arrow batches.
    """

    def __init__(self, file_paths: Sequence[str], size_mib: int):
        self.file_paths = list(file_paths)
        self.size_mib = size_mib


def _file_tiles(file_path: str, size_mib: int):
    """Yield (source, cellid, raster_bytes, metadata) tiles for one file.

    Whole-file GTiff -> pass through original bytes; otherwise split + re-encode.
    """
    import os

    import rasterio

    from databricks.labs.gbx.pyrx.core import tiling as core_tiling

    size_bytes = os.path.getsize(file_path)
    with rasterio.open(file_path) as ds:
        tile_x, tile_y = core_tiling._get_tile_size(
            ds.width, ds.height, size_bytes, size_mib
        )
        if tile_x >= ds.width and tile_y >= ds.height and ds.driver == "GTiff":
            compression = str(ds.profile.get("compress") or "DEFLATE").upper()
            cellid, raster_bytes, meta = _encode.passthrough_tile(
                file_path,
                ds.width,
                ds.height,
                source_path=file_path,
                all_parents="",
                compression=compression,
            )
            yield (file_path, cellid, raster_bytes, meta)
            return
        row_off = 0
        while row_off < ds.height:
            win_h = min(tile_y, ds.height - row_off)
            col_off = 0
            while col_off < ds.width:
                win_w = min(tile_x, ds.width - col_off)
                cellid, raster_bytes, meta = _encode.encode_tile(
                    ds,
                    window=(col_off, row_off, win_w, win_h),
                    source_path=file_path,
                    all_parents="",
                )
                yield (file_path, cellid, raster_bytes, meta)
                col_off += tile_x
            row_off += tile_y


def _to_record_batch(rows):
    """Build an Arrow RecordBatch (source, tile<cellid,raster,metadata>) from buffered rows.

    Arrow columnar output avoids the per-row Python-object ser/de of the tuple
    path on the Python->JVM boundary — the dominant cost at scale for the
    byte-heavy tile payloads.
    """
    import pyarrow as pa

    sources = [r[0] for r in rows]
    cellids = [r[1] for r in rows]
    rasters = [r[2] for r in rows]
    metas = [list(r[3].items()) for r in rows]  # map<string,string> as (k,v) lists

    meta_type = pa.map_(pa.string(), pa.string())
    tile = pa.StructArray.from_arrays(
        [
            pa.array(cellids, pa.int64()),
            pa.array(rasters, pa.binary()),
            pa.array(metas, meta_type),
        ],
        names=["cellid", "raster", "metadata"],
    )
    return pa.RecordBatch.from_arrays(
        [pa.array(sources, pa.string()), tile], names=["source", "tile"]
    )


class RasterGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("raster_gbx requires a 'path' (e.g. .load(path)).")
        self.size_mib = int(options.get("sizeInMB", "16"))
        self.filter_regex = options.get("filterRegex", ".*")
        self.max_files_per_partition = max(
            1, int(options.get("maxFilesPerPartition", "1"))
        )

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        step = self.max_files_per_partition
        return [
            _FilePartition(files[i : i + step], self.size_mib)
            for i in range(0, len(files), step)
        ]

    def read(self, partition: "_FilePartition") -> Iterator["object"]:
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()

        buf = []
        for file_path in partition.file_paths:
            for tile_row in _file_tiles(file_path, partition.size_mib):
                buf.append(tile_row)
                if len(buf) >= _BATCH_ROWS:
                    yield _to_record_batch(buf)
                    buf = []
        if buf:
            yield _to_record_batch(buf)


class RasterGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "raster_gbx"

    def schema(self) -> StructType:
        return reader_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return RasterGbxReader(self.options)
