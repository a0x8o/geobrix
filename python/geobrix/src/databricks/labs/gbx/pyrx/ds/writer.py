"""gtiff_gbx writer (DataSource V2 write path).

Enforces the exact (source, tile) schema like the heavy GDAL writer, writes each
row's tile.raster GTiff bytes to a file under the output path. Pure Python.
"""

from __future__ import annotations

import glob
import os
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.pyrx.ds.raster import reader_schema


@dataclass
class RasterCommitMessage(WriterCommitMessage):
    paths: List[str]


def assert_write_schema(schema: StructType) -> None:
    """Exact (source, tile) — extras OR missing both fail (matches GDAL writer)."""
    expected = reader_schema()
    if [f.name for f in schema.fields] != [f.name for f in expected.fields]:
        raise ValueError(
            f"gtiff_gbx writer requires exactly columns "
            f"{[f.name for f in expected.fields]}, got {[f.name for f in schema.fields]}"
        )


class RasterGbxWriter(DataSourceWriter):
    def __init__(self, path: str, schema: StructType, overwrite: bool):
        assert_write_schema(schema)
        self.path = path
        self.overwrite = overwrite
        if overwrite and os.path.isdir(path):
            for stale in glob.glob(os.path.join(path, "*.tif")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        os.makedirs(self.path, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            raster_bytes = bytes(row["tile"]["raster"])
            out = os.path.join(self.path, f"raster_{uuid.uuid4().hex}.tif")
            with open(out, "wb") as fh:
                fh.write(raster_bytes)
            written.append(out)
        return RasterCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        for msg in messages:
            if isinstance(msg, RasterCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
