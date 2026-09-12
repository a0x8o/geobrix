"""H3 light-tier geometry-aware kring/kloop — PySpark composition.

This module provides the public Python API for H3 geometry-aware kring/kloop
on Databricks. It is light-tier ONLY (no Scala/heavy equivalent) and requires
the `h3` Python package plus Databricks product `h3_*` SQL functions.

Architecture (spec §6):
  1. Geometry work (cover/core cell arrays) -> product h3_* columnar SQL
     (h3_coverash3/h3_polyfillash3 on the raw geometry column). These are
     Databricks-native columnar functions that run efficiently in the executor
     without Python UDF overhead.
  2. Topological expansion -> _h3_geomkring/_h3_geomkloop UDFs in pygx/functions.py
     (runs the shared _dilate engine over the precomputed h3 cell arrays, using
     h3.grid_disk for neighbor lookup — pure Python, works anywhere).

Usage:
    from databricks.labs.gbx.pygx.functions import register
    from databricks.labs.gbx.gridx.h3.functions import geomkring, geomkloop

    register(spark)
    df = df.withColumn("kring", geomkring("geom_col", 9, 1))

SQL surface:
    The registered SQL function `gbx_h3_geomkring` takes the cover/core arrays
    directly (from product h3_* calls). For a single SQL expression see the
    SQL-body example in the doc tests.

VERIFY: Product function names (h3_coverash3, h3_polyfillash3) and their
containment semantics must be confirmed at integration time on a Databricks
cluster. The names used here match the expected Databricks H3 API conventions
but have not been run against a live cluster.
"""

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Column:
    """Coerce a ColLike to a Column: column name str -> F.col, scalars -> F.lit."""
    if isinstance(x, Column):
        return x
    if isinstance(x, str):
        # String arguments at this layer are always column NAMES, not literals.
        # Callers that want a literal string must wrap it in F.lit() themselves.
        return F.col(x)
    return F.lit(x)


def _cover_col(geom_col: Column, resolution: int) -> Column:
    """H3 cells overlapping the geometry at `resolution` (product h3_coverash3).

    Uses F.call_function for Spark Connect / Serverless compatibility — no
    F.expr(f-string), no ._jc references.

    VERIFY: exact product function name (h3_coverash3) and overlap-vs-contained
    semantics on a Databricks cluster at integration time.
    h3_coverash3 is expected to return cells whose hexagon intersects the geometry
    (overlap semantics = _dilate p_cover).
    """
    # VERIFY exact product function name: h3_coverash3
    return F.call_function("h3_coverash3", geom_col, F.lit(int(resolution)))


def _core_col(geom_col: Column, resolution: int) -> Column:
    """H3 cells fully inside the geometry at `resolution` (product h3_polyfillash3).

    Uses F.call_function for Spark Connect / Serverless compatibility.

    VERIFY: exact product function name (h3_polyfillash3) and containment
    semantics on a Databricks cluster at integration time.
    h3_polyfillash3 is expected to return cells fully contained by the geometry
    (containment semantics = _dilate p_core).
    """
    # VERIFY exact product function name: h3_polyfillash3
    return F.call_function("h3_polyfillash3", geom_col, F.lit(int(resolution)))


def _holes_arrays(resolution: int):
    """Return (holes_cover_col, holes_core_col) for interior ring cells.

    Hole extraction is DEFERRED to the Databricks integration step.
    For geometries without holes these empty-array columns are safe and do not
    affect boundary-out / boundary-in behavior.

    For geometries WITH holes the following modes degrade when holes are empty:
    - hole-in / hole-out / hole-out-ignore-geom: return empty results
      (h_cover=h_core={} → h_border empty → no frontier). Correct emptiness.
    - boundary-in-ignore-holes: silently degrades to boundary-in (s_core==p_core),
      returns non-empty but INCORRECT results on holed geometries.
      NOTE: this is a correctness issue, not just a missing feature.
    VERIFY and fix all hole modes when hole extraction is wired at integration.

    A full implementation would iterate interior rings using product ST_* and
    polyfill each one, e.g.:
      holes_cover = h3_coverash3(ST_MakePolygon(ST_InteriorRingN(geom, i)), res)
      holes_core  = h3_polyfillash3(ST_MakePolygon(ST_InteriorRingN(geom, i)), res)
    VERIFY ST_NumInteriorRings, ST_InteriorRingN, h3_coverash3 availability on
    the target Databricks runtime before wiring.
    """
    empty = F.lit(None).cast("array<bigint>")
    return empty, empty


def geomkring(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-ring from a geometry column.

    Composes product h3_coverash3/h3_polyfillash3 (columnar geometry → cell arrays)
    with the _h3_geomkring UDF (dilation engine over cell arrays). Connect-safe:
    uses F.call_function throughout — no F.expr(f-string), no ._jc references.

    Args:
        geom_col:   Column name (str) or Column expression holding the geometry
                    (WKB BINARY or WKT STRING accepted by the product h3_* functions).
        resolution: H3 resolution (0..15).
        k:          Ring distance (0 = covering set only).
        mode:       Dilation mode (default "boundary-out"). One of the 6 modes
                    in _dilate.MODES.

    Returns:
        Column of ARRAY<BIGINT> h3 cell ids.

    VERIFY: product function names (h3_coverash3, h3_polyfillash3) at integration.
    """
    geom = _col(geom_col)
    cover = _cover_col(geom, resolution)
    core = _core_col(geom, resolution)
    holes_cover, holes_core = _holes_arrays(resolution)
    return F.call_function(
        "gbx_h3_geomkring",
        cover,
        core,
        holes_cover,
        holes_core,
        _col(k),
        F.lit(mode),
    )


def geomkloop(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-loop (hollow shell at exactly k steps).

    See :func:`geomkring` for parameter details. Connect-safe: uses F.call_function
    throughout — no F.expr(f-string), no ._jc references.

    VERIFY: product function names (h3_coverash3, h3_polyfillash3) at integration.
    """
    geom = _col(geom_col)
    cover = _cover_col(geom, resolution)
    core = _core_col(geom, resolution)
    holes_cover, holes_core = _holes_arrays(resolution)
    return F.call_function(
        "gbx_h3_geomkloop",
        cover,
        core,
        holes_cover,
        holes_core,
        _col(k),
        F.lit(mode),
    )
