"""pygx light GridX API — quadbin SQL functions (Serverless-safe).

Registers the gbx_quadbin_* SQL functions (point->cell, resolution, k-ring,
distance, polyfill, geometry/centroid EWKB, cell-union, tessellate) plus the
gbx_quadbin_cellunion_agg grouped aggregator, and exposes Column wrappers so
light <-> heavy is a one-line import swap.

Signatures mirror databricks.labs.gbx.gridx.grid.functions. Register once with
gx.register(spark), then use on columns. Serverless-safe: spark.udf.register
plus Column expressions only — no _jvm / spark.conf / RDD access.
"""

from typing import Optional, Union

import numpy as np
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


# --- impl rule: scalar-output -> pandas_udf ; array-output -> plain @udf --------------------
# Scalar/bounded-output functions (pointascell/resolution/distance + the single-
# geometry aswkb/centroid/cellunion) use pandas_udf: pointascell/resolution are
# numpy-vectorized (a real win), the rest amortize the Arrow batch transfer, and a
# batch of scalars is bounded in memory. ARRAY-returning functions (kring/polyfill/
# tessellate) use a PLAIN @udf instead: a scalar pandas_udf buffers a whole Arrow
# batch (~10k rows) of output at once, so variable-length array outputs (polyfill
# can emit up to ~1M cells/row; tessellate many chips/row) risk worker OOM at scale
# — a plain @udf streams row-by-row (peak memory ~ one row's output). All wrap the
# same scalar _quadbin oracle, so cross-tier parity holds either way.


@pandas_udf(LongType())
def _pointascell_udf(lon: pd.Series, lat: pd.Series, res: pd.Series) -> pd.Series:
    lons = lon.to_numpy(dtype=np.float64)
    lats = lat.to_numpy(dtype=np.float64)
    ress = res.to_numpy()
    out = np.empty(len(lons), dtype=np.int64)
    # point_as_cell_vec needs a fixed z per call; group rows by their resolution
    # (typically a single literal => one pass). Bit-identical to the scalar path.
    for z in np.unique(ress):
        mask = ress == z
        out[mask] = _quadbin.point_as_cell_vec(lons[mask], lats[mask], int(z))
    return pd.Series(out)


@pandas_udf(IntegerType())
def _resolution_udf(cell: pd.Series) -> pd.Series:
    cells = cell.to_numpy(dtype=np.int64)
    return pd.Series(_quadbin.resolution_vec(cells).astype(np.int32))


@pandas_udf(IntegerType())
def _distance_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    # Same-resolution-or-error; Chebyshev on cell_to_tile coords. Looped per
    # element (cell_to_tile is scalar in the lib); batched Arrow transfer is the win.
    return pd.Series(
        [int(_quadbin.distance(int(x), int(y))) for x, y in zip(a, b)],
        dtype="int32",
    )


# --- single-geometry UDFs (pandas_udf, bounded scalar output) --------------------------------
# aswkb/centroid/cellunion return ONE geometry per row (bounded output), so a
# pandas_udf batch is memory-safe; they loop over the batch calling the scalar
# _quadbin oracle (the win is the batched Arrow transfer). NULL preserved per row.


@pandas_udf(BinaryType())
def _aswkb_udf(cell: pd.Series) -> pd.Series:
    return pd.Series([_quadbin.as_wkb(int(c)) if c is not None else None for c in cell])


@pandas_udf(BinaryType())
def _centroid_udf(cell: pd.Series) -> pd.Series:
    return pd.Series(
        [_quadbin.centroid(int(c)) if c is not None else None for c in cell]
    )


@pandas_udf(BinaryType())
def _cellunion_udf(cells: pd.Series) -> pd.Series:
    # cells is a Series of arrays (ARRAY<LONG>); per-row union. None/empty -> None.
    return pd.Series(
        [
            (_quadbin.cell_union(list(cs)) if cs is not None and len(cs) else None)
            for cs in cells
        ]
    )


# --- array-returning UDFs (PLAIN @udf, row-by-row for scale safety) --------------------------
# kring/polyfill/tessellate emit variable-length arrays per row; a scalar pandas_udf
# would buffer a whole Arrow batch of these at once (OOM risk at scale), so they are
# plain row-at-a-time UDFs. NULL geom -> NULL (heavy propagateNull).


def _kring(cell, k):
    if cell is None or k is None:
        return None
    return _quadbin.k_ring(int(cell), int(k))


def _polyfill(geom, res):
    if geom is None:
        return None
    return _quadbin.polyfill(geom, int(res))


def _tessellate(geom, res):
    if geom is None:
        return None
    return [
        {"cell": int(c), "geom": gm} for (c, gm) in _quadbin.tessellate(geom, int(res))
    ]


# --- grouped-aggregate pandas UDF -----------------------------------------------------------
# (pd.Series) -> bytes is detected as GROUPED_AGG (Series-to-Scalar) by PySpark 3+.
# Returns the unioned cell-coverage geometry as BINARY (atomic) directly.


@pandas_udf(BinaryType())
def _cellunion_agg_udf(cell: pd.Series) -> Optional[bytes]:
    return _quadbin.cell_union([int(c) for c in cell if c is not None])


def register(spark: SparkSession = None) -> None:
    """Register the pygx quadbin SQL functions (Serverless-safe: udf only)."""
    _env.assert_quadbin_available()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    # scalar-output -> pandas_udf (vectorized / bounded batch)
    spark.udf.register("gbx_quadbin_pointascell", _pointascell_udf)
    spark.udf.register("gbx_quadbin_resolution", _resolution_udf)
    spark.udf.register("gbx_quadbin_distance", _distance_udf)
    spark.udf.register("gbx_quadbin_aswkb", _aswkb_udf)
    spark.udf.register("gbx_quadbin_centroid", _centroid_udf)
    spark.udf.register("gbx_quadbin_cellunion", _cellunion_udf)
    # array-output -> plain @udf (row-by-row, scale-safe)
    spark.udf.register("gbx_quadbin_kring", _kring, ArrayType(LongType()))
    spark.udf.register("gbx_quadbin_polyfill", _polyfill, ArrayType(LongType()))
    spark.udf.register(
        "gbx_quadbin_tessellate", _tessellate, ArrayType(QUADBIN_CELL_SCHEMA)
    )
    # grouped aggregate
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


def quadbin_distance(cell_a: ColLike, cell_b: ColLike) -> Column:
    """Chebyshev grid distance (INT) between two same-resolution cells."""
    return f.call_function("gbx_quadbin_distance", _col(cell_a), _col(cell_b))


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
