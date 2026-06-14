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
from pyspark.sql.functions import pandas_udf, udtf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)

from . import _bng, _env, _quadbin
from ._geom import parse_geom
from ._serde import BNG_CHIP_SCHEMA, QUADBIN_CELL_SCHEMA

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


# ============================================================================
# BNG (British National Grid) — pure-Python port of gridx/grid/BNG.scala.
# Cell ids are STRING in the public surface; geometry outputs are plain WKB
# (EPSG:27700 coordinates, NO SRID — heavy uses JTS.toWKB, not toEWKB).
#
# Same impl-by-shape rule as quadbin: scalar/bounded -> pandas_udf (batched
# Arrow transfer is the win); ARRAY-returning -> plain @udf (row-by-row,
# OOM-safe at scale); explode -> @udtf (SQL-LATERAL only); grouped-agg ->
# grouped-aggregate pandas_udf (returns the dissolved chip as BINARY — PySpark
# grouped-agg cannot return a StructType; see the agg note below).
# ============================================================================


# --- scalar / bounded-output -> pandas_udf ----------------------------------


@pandas_udf(StringType())
def _bng_pointascell_udf(geom: pd.Series, res: pd.Series) -> pd.Series:
    # pointascell takes a GEOMETRY (WKB/EWKB/WKT/EWKT) and uses its centroid as
    # the EPSG:27700 easting/northing (heavy BNG_PointAsCell semantics).
    out = []
    for g, r in zip(geom, res):
        if g is None or r is None:
            out.append(None)
            continue
        pg = parse_geom(g)
        if pg is None or pg.is_empty:
            out.append(None)
            continue
        c = pg.centroid
        out.append(_bng.point_as_cell(c.x, c.y, _norm_res(r)))
    return pd.Series(out)


@pandas_udf(StringType())
def _bng_eastnorthasbng_udf(e: pd.Series, n: pd.Series, res: pd.Series) -> pd.Series:
    # eastnorthasbng takes SCALAR EPSG:27700 eastings/northings (not a geometry).
    out = []
    for ee, nn, r in zip(e, n, res):
        if ee is None or nn is None or r is None:
            out.append(None)
            continue
        out.append(_bng.point_as_cell(float(ee), float(nn), _norm_res(r)))
    return pd.Series(out)


@pandas_udf(DoubleType())
def _bng_cellarea_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.area(_bng.parse(c)) if c is not None else None for c in cellid]
    )


@pandas_udf(LongType())
def _bng_distance_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                int(_bng.distance(_bng.parse(x), _bng.parse(y)))
                if x is not None and y is not None
                else None
            )
            for x, y in zip(a, b)
        ]
    )


@pandas_udf(LongType())
def _bng_euclideandistance_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                int(_bng.euclidean_distance(_bng.parse(x), _bng.parse(y)))
                if x is not None and y is not None
                else None
            )
            for x, y in zip(a, b)
        ]
    )


@pandas_udf(BinaryType())
def _bng_aswkb_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.cell_aswkb(_bng.parse(c)) if c is not None else None for c in cellid]
    )


@pandas_udf(StringType())
def _bng_aswkt_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.cell_aswkt(_bng.parse(c)) if c is not None else None for c in cellid]
    )


@pandas_udf(BinaryType())
def _bng_centroid_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.cell_centroid(_bng.parse(c)) if c is not None else None for c in cellid]
    )


# --- chip-pair scalar ops -> pandas_udf returning BNG_CHIP_SCHEMA ------------
# cellintersection/cellunion take two chip structs {cellid, core, chip}; the
# _bng ops operate on (cellid_str, core, shapely|None) tuples and apply the
# left-hand rule (different cellid -> empty; either core -> that chip).


def _chip_tuple(struct_row):
    """Map a chip struct row (positional (id, core, chip)) to (cellid, core, shapely|None).

    Fields are read POSITIONALLY — the documented chip-struct contract — so
    callers may name the struct fields anything (e.g. SQL ``struct(c, isCore,
    geom)``), matching the heavy reader's by-position/type field detection.
    """
    cellid, core, raw = struct_row[0], struct_row[1], struct_row[2]
    chip = parse_geom(raw) if raw is not None else None
    return (cellid, bool(core), chip)


def _to_positional(rec):
    """Coerce one struct element to a positional (id, core, chip) tuple.

    A struct element may arrive as a dict (grouped-agg Series), a Spark Row, or
    a plain sequence (scalar DataFrame rows). Dicts/Rows preserve struct-field
    declaration order in their values.
    """
    if isinstance(rec, dict):
        return tuple(rec.values())
    if hasattr(rec, "__fields__"):  # pyspark Row
        return tuple(rec)
    return tuple(rec)


def _chip_records(struct_col):
    """Iterate per-row positional (id, core, chip) tuples from a struct column.

    A StructType column arrives as a pandas DataFrame (scalar pandas_udf: one
    column per struct field) or as a Series of dict/Row elements (grouped-agg).
    """
    if isinstance(struct_col, pd.DataFrame):
        return [tuple(rec) for rec in struct_col.itertuples(index=False, name=None)]
    return [_to_positional(rec) for rec in struct_col]


def _chip_to_row(result):
    cellid, core, chip = result
    chip_wkb = None
    if chip is not None and not chip.is_empty:
        from shapely import to_wkb as _to_wkb

        chip_wkb = _to_wkb(chip)
    return {"cellid": cellid, "core": bool(core), "chip": chip_wkb}


@pandas_udf(BNG_CHIP_SCHEMA)
def _bng_cellunion_udf(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    rows = [
        _chip_to_row(_bng.cell_union(_chip_tuple(lt), _chip_tuple(rt)))
        for lt, rt in zip(_chip_records(left), _chip_records(right))
    ]
    return pd.DataFrame(rows, columns=["cellid", "core", "chip"])


@pandas_udf(BNG_CHIP_SCHEMA)
def _bng_cellintersection_udf(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    rows = [
        _chip_to_row(_bng.cell_intersection(_chip_tuple(lt), _chip_tuple(rt)))
        for lt, rt in zip(_chip_records(left), _chip_records(right))
    ]
    return pd.DataFrame(rows, columns=["cellid", "core", "chip"])


# --- array-output -> plain @udf (row-by-row, scale-safe) --------------------


def _bng_kring(cellid, k):
    if cellid is None or k is None:
        return None
    return _bng.k_ring_str(cellid, int(k))


def _bng_kloop(cellid, k):
    if cellid is None or k is None:
        return None
    return _bng.k_loop_str(cellid, int(k))


def _bng_polyfill(geom, res):
    if geom is None or res is None:
        return None
    return _bng.polyfill_str(geom, _norm_res(res))


def _bng_geomkring(geom, res, k):
    if geom is None or res is None or k is None:
        return None
    return sorted(_bng.geometry_k_ring_str(geom, _norm_res(res), int(k)))


def _bng_geomkloop(geom, res, k):
    if geom is None or res is None or k is None:
        return None
    return sorted(_bng.geometry_k_loop_str(geom, _norm_res(res), int(k)))


def _bng_tessellate(geom, res):
    if geom is None or res is None:
        return None
    return [
        {"cellid": c, "core": bool(core), "chip": chip}
        for (c, core, chip) in _bng.tessellate_str(geom, _norm_res(res))
    ]


# --- explode UDTFs (SQL-LATERAL only) ---------------------------------------


@udtf(returnType="cellid: string")
class _BngKRingExplode:
    def eval(self, cellid, k):
        if cellid is None or k is None:
            return
        for c in _bng.k_ring_str(cellid, int(k)):
            yield (c,)


@udtf(returnType="cellid: string")
class _BngKLoopExplode:
    def eval(self, cellid, k):
        if cellid is None or k is None:
            return
        for c in _bng.k_loop_str(cellid, int(k)):
            yield (c,)


@udtf(returnType="cellid: string")
class _BngGeomKRingExplode:
    def eval(self, geom, res, k):
        if geom is None or res is None or k is None:
            return
        for c in sorted(_bng.geometry_k_ring_str(geom, _norm_res(res), int(k))):
            yield (c,)


@udtf(returnType="cellid: string")
class _BngGeomKLoopExplode:
    def eval(self, geom, res, k):
        if geom is None or res is None or k is None:
            return
        for c in sorted(_bng.geometry_k_loop_str(geom, _norm_res(res), int(k))):
            yield (c,)


@udtf(returnType="cellid: string, core: boolean, chip: binary")
class _BngTessellateExplode:
    def eval(self, geom, res):
        if geom is None or res is None:
            return
        for c, core, chip in _bng.tessellate_str(geom, _norm_res(res)):
            yield (c, bool(core), chip)


# --- grouped-agg pandas_udf returning BNG_CHIP_SCHEMA ------------------------
# Fold a group's chips (same cellid) into one dissolved chip via the chip op.


# NOTE: PySpark grouped-aggregate pandas UDFs cannot return a StructType
# (NOT_IMPLEMENTED for struct return). Heavy BNG_CellUnionAgg returns a chip
# STRUCT<cellid, core, chip>, but the light grouped-agg can only emit an atomic
# type — so it returns the dissolved chip geometry as plain WKB BINARY (the
# load-bearing field), exactly like the quadbin cellunion_agg. The group key is
# the cellid (callers GROUP BY cellid), and core is recoverable from whether the
# chip equals the full cell, so no information is lost for the swap. Cross-tier
# parity (Task 9) compares the decoded chip geometry.


def _fold_chip_geom(chip, op):
    acc = None
    for row in _chip_records(chip):
        if row is None:
            continue
        cur = _chip_tuple(row)
        acc = cur if acc is None else op(acc, cur)
    if acc is None:
        return None
    cellid, core, geom = acc
    # A core chip carries None geometry in the array form; materialize the full
    # cell polygon so the dissolved output is always a real geometry.
    if (geom is None or geom.is_empty) and core and cellid is not None:
        geom = _bng.cell_id_to_geometry(_bng.parse(cellid))
    if geom is None or geom.is_empty:
        return None
    from shapely import to_wkb as _to_wkb

    return _to_wkb(geom)


@pandas_udf(BinaryType())
def _bng_cellunion_agg_udf(chip: pd.DataFrame) -> Optional[bytes]:
    return _fold_chip_geom(chip, _bng.cell_union)


@pandas_udf(BinaryType())
def _bng_cellintersection_agg_udf(chip: pd.DataFrame) -> Optional[bytes]:
    return _fold_chip_geom(chip, _bng.cell_intersection)


def _norm_res(res):
    """Normalize a SQL resolution arg (Int index or resolutionMap string).

    _bng.get_resolution validates Int ±1..±6 / resolutionMap keys and rejects
    metres-as-Int; the _bng wrapper fns call it again, so pass the raw value
    through with only numpy-scalar unwrapping.
    """
    if isinstance(res, (np.integer,)):
        return int(res)
    return res


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

    # --- BNG (gridx.bng) — all 23 gbx_bng_* names ---------------------------
    _env.assert_bng_available()
    # scalar / bounded-output -> pandas_udf
    spark.udf.register("gbx_bng_pointascell", _bng_pointascell_udf)
    spark.udf.register("gbx_bng_eastnorthasbng", _bng_eastnorthasbng_udf)
    spark.udf.register("gbx_bng_cellarea", _bng_cellarea_udf)
    spark.udf.register("gbx_bng_distance", _bng_distance_udf)
    spark.udf.register("gbx_bng_euclideandistance", _bng_euclideandistance_udf)
    spark.udf.register("gbx_bng_aswkb", _bng_aswkb_udf)
    spark.udf.register("gbx_bng_aswkt", _bng_aswkt_udf)
    spark.udf.register("gbx_bng_centroid", _bng_centroid_udf)
    spark.udf.register("gbx_bng_cellintersection", _bng_cellintersection_udf)
    spark.udf.register("gbx_bng_cellunion", _bng_cellunion_udf)
    # array-output -> plain @udf (row-by-row, scale-safe)
    spark.udf.register("gbx_bng_kring", _bng_kring, ArrayType(StringType()))
    spark.udf.register("gbx_bng_kloop", _bng_kloop, ArrayType(StringType()))
    spark.udf.register("gbx_bng_polyfill", _bng_polyfill, ArrayType(StringType()))
    spark.udf.register("gbx_bng_geomkring", _bng_geomkring, ArrayType(StringType()))
    spark.udf.register("gbx_bng_geomkloop", _bng_geomkloop, ArrayType(StringType()))
    spark.udf.register(
        "gbx_bng_tessellate", _bng_tessellate, ArrayType(BNG_CHIP_SCHEMA)
    )
    # explode -> UDTF (SQL-LATERAL only)
    spark.udtf.register("gbx_bng_kringexplode", _BngKRingExplode)
    spark.udtf.register("gbx_bng_kloopexplode", _BngKLoopExplode)
    spark.udtf.register("gbx_bng_geomkringexplode", _BngGeomKRingExplode)
    spark.udtf.register("gbx_bng_geomkloopexplode", _BngGeomKLoopExplode)
    spark.udtf.register("gbx_bng_tessellateexplode", _BngTessellateExplode)
    # grouped aggregate
    spark.udf.register("gbx_bng_cellunion_agg", _bng_cellunion_agg_udf)
    spark.udf.register("gbx_bng_cellintersection_agg", _bng_cellintersection_agg_udf)


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


# --- BNG Column wrappers (mirror heavy gridx.bng.functions) ----------------------------------
# Cell ids are STRING; geometry outputs are plain WKB (EPSG:27700, no SRID).


def bng_pointascell(geom: ColLike, res: ColLike) -> Column:
    """BNG cell id (STRING) for the centroid of a geometry (EPSG:27700)."""
    return f.call_function("gbx_bng_pointascell", _col(geom), _col(res))


def bng_eastnorthasbng(e: ColLike, n: ColLike, res: ColLike) -> Column:
    """BNG cell id (STRING) for scalar EPSG:27700 eastings/northings at `res`."""
    return f.call_function("gbx_bng_eastnorthasbng", _col(e), _col(n), _col(res))


def bng_cellarea(cellid: ColLike) -> Column:
    """Cell area in square KILOMETRES (DOUBLE)."""
    return f.call_function("gbx_bng_cellarea", _col(cellid))


def bng_distance(cell_a: ColLike, cell_b: ColLike) -> Column:
    """Manhattan grid distance (LONG) between two BNG cells (edge-size units)."""
    return f.call_function("gbx_bng_distance", _col(cell_a), _col(cell_b))


def bng_euclideandistance(cell_a: ColLike, cell_b: ColLike) -> Column:
    """Chebyshev grid distance (LONG) between two BNG cells (edge-size units)."""
    return f.call_function("gbx_bng_euclideandistance", _col(cell_a), _col(cell_b))


def bng_aswkb(cellid: ColLike) -> Column:
    """Cell boundary polygon as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_bng_aswkb", _col(cellid))


def bng_aswkt(cellid: ColLike) -> Column:
    """Cell boundary polygon as WKT (STRING)."""
    return f.call_function("gbx_bng_aswkt", _col(cellid))


def bng_centroid(cellid: ColLike) -> Column:
    """Cell centroid point as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_bng_centroid", _col(cellid))


def bng_cellintersection(left: ColLike, right: ColLike) -> Column:
    """Per-cell chip intersection STRUCT<cellid, core, chip> (left-hand rule)."""
    return f.call_function("gbx_bng_cellintersection", _col(left), _col(right))


def bng_cellunion(left: ColLike, right: ColLike) -> Column:
    """Per-cell chip union STRUCT<cellid, core, chip> (left-hand rule)."""
    return f.call_function("gbx_bng_cellunion", _col(left), _col(right))


def bng_kring(cellid: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> of cells within ring distance `k` (includes center)."""
    return f.call_function("gbx_bng_kring", _col(cellid), _col(k))


def bng_kloop(cellid: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> of the hollow ring of cells at exact distance `k`."""
    return f.call_function("gbx_bng_kloop", _col(cellid), _col(k))


def bng_polyfill(geom: ColLike, res: ColLike) -> Column:
    """ARRAY<STRING> of cells whose centroid is contained by the geometry."""
    return f.call_function("gbx_bng_polyfill", _col(geom), _col(res))


def bng_geomkring(geom: ColLike, res: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> k-ring around a geometry's covering chips."""
    return f.call_function("gbx_bng_geomkring", _col(geom), _col(res), _col(k))


def bng_geomkloop(geom: ColLike, res: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> k-loop around a geometry's covering chips."""
    return f.call_function("gbx_bng_geomkloop", _col(geom), _col(res), _col(k))


def bng_tessellate(geom: ColLike, res: ColLike) -> Column:
    """ARRAY<STRUCT<cellid:STRING, core:BOOL, chip:BINARY>> chips per cell."""
    return f.call_function("gbx_bng_tessellate", _col(geom), _col(res))


def bng_cellunion_agg(chip: ColLike) -> Column:
    """Aggregator: union a group's same-cell chips into one dissolved-chip WKB BINARY.

    (Light grouped-agg cannot return a STRUCT; emits the dissolved chip geometry,
    keyed by the group's cellid — see the registration note.)
    """
    return f.call_function("gbx_bng_cellunion_agg", _col(chip))


def bng_cellintersection_agg(chip: ColLike) -> Column:
    """Aggregator: intersect a group's same-cell chips into one dissolved-chip WKB BINARY."""
    return f.call_function("gbx_bng_cellintersection_agg", _col(chip))


# The five *explode functions are SQL-LATERAL-only table functions in the light
# tier — they have no Python DataFrame Column form (unlike the heavy tier). They
# exist here as importable names for binding parity, but raise NotImplementedError
# pointing to the registered UDTF; invoke them via SQL LATERAL instead.

_EXPLODE_HINT = (
    "Light BNG {name} has no Python Column form; invoke the registered UDTF via "
    "SQL LATERAL, e.g. SELECT t.* FROM <df>, LATERAL {udtf}(...) t"
)


def bng_kringexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT cellid FROM gbx_bng_kringexplode(cellid, k)."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(name="bng_kringexplode", udtf="gbx_bng_kringexplode")
    )


def bng_kloopexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT cellid FROM gbx_bng_kloopexplode(cellid, k)."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(name="bng_kloopexplode", udtf="gbx_bng_kloopexplode")
    )


def bng_geomkringexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT t.* FROM <df>, LATERAL gbx_bng_geomkringexplode(geom, res, k) t."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(
            name="bng_geomkringexplode", udtf="gbx_bng_geomkringexplode"
        )
    )


def bng_geomkloopexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT t.* FROM <df>, LATERAL gbx_bng_geomkloopexplode(geom, res, k) t."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(
            name="bng_geomkloopexplode", udtf="gbx_bng_geomkloopexplode"
        )
    )


def bng_tessellateexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT t.* FROM <df>, LATERAL gbx_bng_tessellateexplode(geom, res) t."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(
            name="bng_tessellateexplode", udtf="gbx_bng_tessellateexplode"
        )
    )
