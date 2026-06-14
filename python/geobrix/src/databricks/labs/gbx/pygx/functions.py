"""pygx light GridX API — quadbin SQL functions (Serverless-safe).

Registers the gbx_quadbin_* SQL functions (point->cell, resolution, k-ring,
distance, polyfill, geometry/centroid EWKB, cell-union, tessellate) plus the
gbx_quadbin_cellunion_agg grouped aggregator, and exposes Column wrappers so
light <-> heavy is a one-line import swap.

Signatures mirror databricks.labs.gbx.gridx.grid.functions. Register once with
gx.register(spark), then use on columns. Serverless-safe: spark.udf.register
plus Column expressions only — no _jvm / spark.conf / RDD access.
"""

from typing import Union

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, BinaryType, IntegerType, LongType

from . import _env, _quadbin
from ._serde import QUADBIN_CELL_SCHEMA

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


# --- scalar UDF implementations -------------------------------------------------------------


def _pointascell(lon, lat, res):
    return _quadbin.point_as_cell(lon, lat, res)


def _resolution(cell):
    return _quadbin.resolution(cell)


def _kring(cell, k):
    return _quadbin.k_ring(cell, k)


def _distance(a, b):
    return _quadbin.distance(a, b)


def _polyfill(geom, res):
    return _quadbin.polyfill(geom, res)


def _aswkb(cell):
    return _quadbin.as_wkb(cell)


def _centroid(cell):
    return _quadbin.centroid(cell)


def _cellunion(cells):
    return _quadbin.cell_union(list(cells) if cells else cells)


def _tessellate(geom, res):
    # ARRAY<STRUCT<cell,geom>> — return dicts so the struct fields bind by name.
    return [{"cell": int(c), "geom": g} for (c, g) in _quadbin.tessellate(geom, res)]


# --- grouped-aggregate pandas UDF -----------------------------------------------------------
# (pd.Series) -> bytes is detected as GROUPED_AGG (Series-to-Scalar) by PySpark 3+.
# Returns the unioned cell-coverage geometry as BINARY (atomic) directly.


@pandas_udf(BinaryType())
def _cellunion_agg_udf(cell: pd.Series) -> bytes:
    return _quadbin.cell_union([int(c) for c in cell if c is not None])


def register(spark: SparkSession = None) -> None:
    """Register the pygx quadbin SQL functions (Serverless-safe: udf only)."""
    _env.assert_quadbin_available()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.udf.register("gbx_quadbin_pointascell", _pointascell, LongType())
    spark.udf.register("gbx_quadbin_resolution", _resolution, IntegerType())
    spark.udf.register("gbx_quadbin_kring", _kring, ArrayType(LongType()))
    spark.udf.register("gbx_quadbin_distance", _distance, IntegerType())
    spark.udf.register("gbx_quadbin_polyfill", _polyfill, ArrayType(LongType()))
    spark.udf.register("gbx_quadbin_aswkb", _aswkb, BinaryType())
    spark.udf.register("gbx_quadbin_centroid", _centroid, BinaryType())
    spark.udf.register("gbx_quadbin_cellunion", _cellunion, BinaryType())
    spark.udf.register(
        "gbx_quadbin_tessellate", _tessellate, ArrayType(QUADBIN_CELL_SCHEMA)
    )
    spark.udf.register("gbx_quadbin_cellunion_agg", _cellunion_agg_udf)


# --- Column wrappers (mirror heavy gridx.grid.functions) ------------------------------------


def quadbin_pointascell(lon: ColLike, lat: ColLike, res: ColLike) -> Column:
    """Quadbin cell (LONG) containing the WGS84 lon/lat point at `res`."""
    return f.call_function("gbx_quadbin_pointascell", _col(lon), _col(lat), _col(res))


def quadbin_resolution(cell: ColLike) -> Column:
    """Resolution (INT) of a quadbin cell."""
    return f.call_function("gbx_quadbin_resolution", _col(cell))


def quadbin_kring(cell: ColLike, k: ColLike) -> Column:
    """ARRAY<LONG> of cells within ring distance `k` of `cell` (includes center)."""
    return f.call_function("gbx_quadbin_kring", _col(cell), _col(k))


def quadbin_distance(a: ColLike, b: ColLike) -> Column:
    """Chebyshev grid distance (INT) between two same-resolution cells."""
    return f.call_function("gbx_quadbin_distance", _col(a), _col(b))


def quadbin_polyfill(geom: ColLike, res: ColLike) -> Column:
    """ARRAY<LONG> of cells covering the geometry's envelope at `res`."""
    return f.call_function("gbx_quadbin_polyfill", _col(geom), _col(res))


def quadbin_aswkb(cell: ColLike) -> Column:
    """Cell boundary polygon as EWKB (SRID 4326) BINARY."""
    return f.call_function("gbx_quadbin_aswkb", _col(cell))


def quadbin_centroid(cell: ColLike) -> Column:
    """Cell centroid point as EWKB (SRID 4326) BINARY."""
    return f.call_function("gbx_quadbin_centroid", _col(cell))


def quadbin_cellunion(cells: ColLike) -> Column:
    """Union of an ARRAY<LONG> of cell boundaries as EWKB (SRID 4326) BINARY."""
    return f.call_function("gbx_quadbin_cellunion", _col(cells))


def quadbin_tessellate(geom: ColLike, res: ColLike) -> Column:
    """ARRAY<STRUCT<cell:LONG, geom:BINARY>> chips clipping the geometry per cell."""
    return f.call_function("gbx_quadbin_tessellate", _col(geom), _col(res))


def quadbin_cellunion_agg(cell: ColLike) -> Column:
    """Aggregator: union a group's cell boundaries into one EWKB (SRID 4326) BINARY."""
    return _cellunion_agg_udf(_col(cell))
