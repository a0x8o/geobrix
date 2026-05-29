"""Tile struct schema and rasterio MemoryFile (de)serialization.

The tile struct mirrors the heavyweight rasterx tile exactly:
    struct<cellid: bigint, raster: binary, metadata: map<string,string>>
pyrx always uses the BINARY raster variant (never executor-only file paths).
"""

from contextlib import contextmanager
from typing import Dict, Iterator

from pyspark.sql.types import (
    BinaryType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from rasterio.io import DatasetReader, MemoryFile

TILE_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=False),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


@contextmanager
def open_tile(raster_bytes: bytes) -> Iterator[DatasetReader]:
    """Open raster BINARY content as a rasterio DatasetReader (in-memory)."""
    with MemoryFile(bytes(raster_bytes)) as mf:
        with mf.open() as ds:
            yield ds


def build_tile(raster_bytes: bytes, driver: str, cellid: int = 0) -> Dict:
    """Construct a tile struct dict from raster BINARY content."""
    raster = bytes(raster_bytes)
    with open_tile(raster) as ds:
        meta = {
            "driver": driver or ds.driver,
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
    return {"cellid": int(cellid), "raster": raster, "metadata": meta}
