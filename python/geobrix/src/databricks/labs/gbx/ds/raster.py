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

from typing import Dict, Iterator, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.gbx.ds import _encode, _listing
from databricks.labs.gbx.pyrx import _serde

# Spark BinaryType cells are bounded by the JVM 2 GiB array limit; a single
# whole-image tile larger than this cannot be materialized and would otherwise
# fail deep in the writer with an opaque error. Guard the no-split path against
# it (conservative ~1.9 GiB) so users get an actionable "set sizeInMB" message.
_MAX_TILE_BYTES = 1932735283  # ~1.8 GiB


def _numpy_itemsize(dtype: str) -> int:
    import numpy as np

    try:
        return np.dtype(dtype).itemsize
    except TypeError:
        return 1


def _estimate_tile_bytes(
    width: int, height: int, count: int, dtype, file_size: int
) -> int:
    """Conservative encoded-size estimate for a single whole-image tile.

    Uses the larger of the raw pixel-array size (width*height*bands*itemsize) and
    the on-disk encoded size, so neither a highly compressed source nor an
    expanded re-encode slips past the cell-limit guard.
    """
    raw = width * height * max(count, 1) * _numpy_itemsize(str(dtype))
    return max(raw, file_size)


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
        self.size_mib = int(options.get("sizeInMB", "-1"))
        self.filter_regex = options.get("filterRegex", ".*")

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        return [_FilePartition(f, self.size_mib) for f in files]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        import os

        import rasterio

        from databricks.labs.gbx.pyrx import _env
        from databricks.labs.gbx.pyrx.core import tiling as core_tiling

        _env.configure_gdal_env()

        # rasterio reads the BARE FUSE path; only the emitted source column is
        # scheme-qualified to match binaryFile / heavy gdal (dbfs:/Volumes/...),
        # so a light-produced DataFrame joins cleanly against that convention.
        source = _listing.to_spark_uri(partition.file_path)

        # Heavy keys the BalancedSubdivision split on RasterAccessors.memSize,
        # which for an on-disk source is the file's encoded byte size. Reuse the
        # shared, tested split math in core.tiling rather than re-deriving it.
        size_bytes = os.path.getsize(partition.file_path)
        with rasterio.open(partition.file_path) as ds:
            tile_x, tile_y = core_tiling._get_tile_size(
                ds.width, ds.height, size_bytes, partition.size_mib
            )
            # Large-raster safety: a single whole-image tile that would exceed
            # Spark's ~2 GiB BinaryType cell limit must fail with an actionable
            # message rather than producing a giant (or unmaterializable) cell.
            if tile_x >= ds.width and tile_y >= ds.height:
                est = _estimate_tile_bytes(
                    ds.width, ds.height, ds.count, ds.dtypes[0], size_bytes
                )
                if est > _MAX_TILE_BYTES:
                    raise ValueError(
                        f"raster {partition.file_path} is ~{est // (1024 * 1024)} MB "
                        f"as a single tile, which exceeds the ~2 GB Spark cell limit; "
                        f"set the reader option sizeInMB=<n> (a positive MB value) to "
                        f"tile it into smaller pieces."
                    )
            # Fast path: when the split is a single tile spanning the whole raster
            # AND the source is already a GTiff, emit the original file bytes
            # instead of decoding + re-encoding (the re-encode is ~95% of per-tile
            # cost). Pixels are identical, so this is parity-safe (decoded-pixel,
            # not byte). Sub-tiles or non-GTiff sources fall through to encode_tile.
            if tile_x >= ds.width and tile_y >= ds.height and ds.driver == "GTiff":
                compression = str(ds.profile.get("compress") or "DEFLATE").upper()
                cellid, raster_bytes, meta = _encode.passthrough_tile(
                    partition.file_path,
                    ds.width,
                    ds.height,
                    source_path=partition.file_path,
                    all_parents="",
                    compression=compression,
                )
                yield (source, (cellid, raster_bytes, meta))
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
                        source_path=partition.file_path,
                        all_parents="",
                    )
                    yield (source, (cellid, raster_bytes, meta))
                    col_off += tile_x
                row_off += tile_y


class RasterGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "raster_gbx"

    def schema(self) -> StructType:
        return reader_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return RasterGbxReader(self.options)

    def writer(
        self, schema: StructType, overwrite: bool
    ) -> "DataSourceWriter":  # noqa: F821
        from pyspark.sql.datasource import DataSourceWriter  # noqa: F401

        from databricks.labs.gbx.ds.writer import RasterGbxWriter

        path = self.options.get("path")
        if not path:
            raise ValueError("raster_gbx writer requires an output path (.save(path)).")
        return RasterGbxWriter(
            path,
            schema,
            overwrite,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            force_driver=None,
        )
