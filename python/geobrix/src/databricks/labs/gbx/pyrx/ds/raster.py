"""raster_gbx — catch-all pure-Python DataSource V2 raster reader.

1:1 swap-out for the Scala ``gdal`` reader: recursively lists files, splits each
into BalancedSubdivision tiles, re-encodes each tile as GTiff, emits
(source, tile) rows matching pyrx._serde.TILE_SCHEMA. Pure Python (Serverless).
"""

from __future__ import annotations

from typing import Dict, Iterator, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.ds import _encode, _listing, _tiling


def reader_schema() -> StructType:
    """(source, tile) — tile from the single-source TILE_SCHEMA."""
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("tile", _serde.TILE_SCHEMA, nullable=False),
        ]
    )


class _FilePartition(InputPartition):
    """One source file = one partition (picklable)."""

    def __init__(self, file_path: str, size_mib: int):
        self.file_path = file_path
        self.size_mib = size_mib


class RasterGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("raster_gbx requires a 'path' (e.g. .load(path)).")
        self.size_mib = int(options.get("sizeInMB", "16"))
        self.filter_regex = options.get("filterRegex", ".*")

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        return [_FilePartition(f, self.size_mib) for f in files]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        import rasterio

        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()

        with rasterio.open(partition.file_path) as ds:
            windows = _tiling.plan_windows(
                width=ds.width,
                height=ds.height,
                bands=ds.count,
                dtype=ds.dtypes[0],
                size_mib=partition.size_mib,
            )
            for win in windows:
                cellid, raster_bytes, meta = _encode.encode_tile(
                    ds,
                    window=win,
                    source_path=partition.file_path,
                    all_parents="",
                )
                yield (partition.file_path, (cellid, raster_bytes, meta))


class RasterGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "raster_gbx"

    def schema(self) -> StructType:
        return reader_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return RasterGbxReader(self.options)
