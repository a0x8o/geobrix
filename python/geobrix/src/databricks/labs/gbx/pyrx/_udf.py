"""Arrow-vectorized UDF harness lowering Spark-free core fns to Columns.

Each public rst_* function extracts the BINARY ``raster`` subfield from the tile
struct column and feeds it (plus any scalar args) to a pandas_udf that opens the
bytes with rasterio and calls a core function. Returning a Column preserves the
one-line swap contract with the heavyweight rasterx wrappers.
"""

from typing import Callable, Union

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DataType

from databricks.labs.gbx.pyrx import _env, _serde

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Mirror rasterx: auto-wrap bool/int/float/bytes; pass str/Column through."""
    if isinstance(x, (Column, str)):
        return x
    return f.lit(x)


def _raster_field(tile: ColLike) -> Column:
    """Resolve a tile arg to its BINARY ``raster`` subfield Column."""
    c = f.col(tile) if isinstance(tile, str) else tile
    return c.getField("raster")


def tile_scalar_udf(core_fn: Callable, return_type: DataType):
    """Build a pandas_udf: (raster: Series[bytes]) -> Series, calling core_fn(ds)."""

    @pandas_udf(return_type)
    def _udf(raster: pd.Series) -> pd.Series:
        _env.configure_gdal_env()  # runs on the worker process
        out = []
        for b in raster:
            if b is None:
                out.append(None)
                continue
            with _serde.open_tile(bytes(b)) as ds:
                out.append(core_fn(ds))
        return pd.Series(out, dtype="object")

    return _udf


def tile_scalar_udf2(core_fn: Callable, return_type: DataType):
    """Build a pandas_udf: (raster, a, b) -> Series, calling core_fn(ds, a, b)."""

    @pandas_udf(return_type)
    def _udf(raster: pd.Series, a: pd.Series, b: pd.Series) -> pd.Series:
        _env.configure_gdal_env()
        out = []
        for rb, av, bv in zip(raster, a, b):
            if rb is None:
                out.append(None)
                continue
            with _serde.open_tile(bytes(rb)) as ds:
                out.append(core_fn(ds, av, bv))
        return pd.Series(out, dtype="object")

    return _udf
