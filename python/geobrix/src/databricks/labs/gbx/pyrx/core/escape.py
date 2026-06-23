"""Python-only escape-hatches for users whose needs fall outside the rst_* surface.

NOT SQL-registered (tile_to_numpy returns a host object; rst_apply takes a Python
callable), so neither appears in registered_functions.txt / function-info.json.
"""

from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import DataType, DoubleType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx._udf import _col


def tile_to_numpy(tile_or_bytes):
    """Read a tile's raster into a numpy ndarray (all bands).

    Accepts a tile struct (a Row/dict with a 'raster' field) or raw bytes. The
    "drop to numpy" hatch: call on a collected tile, or inside your own UDF.
    """
    if isinstance(tile_or_bytes, (bytes, bytearray)):
        raw = bytes(tile_or_bytes)
    else:
        raw = bytes(tile_or_bytes["raster"])
    with _serde.open_tile(raw) as ds:
        return ds.read()


def rst_apply(tile_col, fn, returnType: DataType = DoubleType()) -> Column:
    """Apply an arbitrary rasterio function to each tile, returning one scalar/row.

    fn receives an open rasterio DatasetReader and returns a value of returnType
    (default DoubleType; any Spark DataType). The escape-hatch for "GeoBrix lacks
    function X — run your own rasterio per tile". Scalar return only. Null/empty
    tile -> null.
    """

    @udf(returnType=returnType)
    def _apply(tile):
        if tile is None or tile["raster"] is None:
            return None
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            return fn(ds)

    return _apply(_col(tile_col))
