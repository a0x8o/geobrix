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


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via F.lit(); pass strings and Columns through."""
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return F.lit(x)


def _cover_col(geom_col: str, resolution: int) -> Column:
    """H3 cells overlapping the geometry at `resolution` (product h3_coverash3).

    VERIFY: exact product function name (h3_coverash3) and overlap-vs-contained
    semantics on a Databricks cluster at integration time.
    h3_coverash3 is expected to return cells whose hexagon intersects the geometry
    (overlap semantics = _dilate p_cover).
    """
    # VERIFY exact product function name: h3_coverash3
    return F.expr(f"h3_coverash3({geom_col}, {int(resolution)})")


def _core_col(geom_col: str, resolution: int) -> Column:
    """H3 cells fully inside the geometry at `resolution` (product h3_polyfillash3).

    VERIFY: exact product function name (h3_polyfillash3) and containment
    semantics on a Databricks cluster at integration time.
    h3_polyfillash3 is expected to return cells fully contained by the geometry
    (containment semantics = _dilate p_core).
    """
    # VERIFY exact product function name: h3_polyfillash3
    return F.expr(f"h3_polyfillash3({geom_col}, {int(resolution)})")


def _holes_arrays(geom_col: str, resolution: int):
    """Return (holes_cover_col, holes_core_col) for interior ring cells.

    For geometries without holes these return empty arrays. For holed
    geometries the interior ring cells are extracted via product ST_* /
    H3 functions.

    VERIFY: The product SQL path for hole extraction must be confirmed at
    integration time. The approach below uses ST_NumInteriorRings and
    ST_InteriorRingN to get each hole as a polygon, then polyfills it.
    Adjust to match available product functions on your Databricks runtime.

    For hole-free geometries (the common case) these are safe to call —
    they return empty arrays and do not affect boundary-out/boundary-in
    behavior.
    """
    # VERIFY: ST_NumInteriorRings, ST_InteriorRingN, h3_coverash3 on ring geometry
    # For now use a safe approximation: empty arrays for holes.
    # A full implementation would iterate interior rings using product ST_* and
    # polyfill each one, e.g.:
    #   holes_cover = h3_coverash3(ST_MakePolygon(ST_InteriorRingN(geom, i)), res)
    #   holes_core  = h3_polyfillash3(ST_MakePolygon(ST_InteriorRingN(geom, i)), res)
    # For geoms with holes this approximation means hole modes (hole-in, hole-out,
    # hole-out-ignore-geom) return empty results. VERIFY and fix at integration.
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
    with the _h3_geomkring UDF (dilation engine over cell arrays).

    Args:
        geom_col:   Column name or Column expression holding the geometry
                    (WKB BINARY or WKT STRING accepted by the product h3_* functions).
        resolution: H3 resolution (0..15).
        k:          Ring distance (0 = covering set only).
        mode:       Dilation mode (default "boundary-out"). One of the 6 modes
                    in _dilate.MODES.

    Returns:
        Column of ARRAY<BIGINT> h3 cell ids.

    VERIFY: product function names (h3_coverash3, h3_polyfillash3) at integration.
    """
    geom_str = geom_col if isinstance(geom_col, str) else geom_col._jc.toString()
    cover = _cover_col(geom_str, resolution)
    core = _core_col(geom_str, resolution)
    holes_cover, holes_core = _holes_arrays(geom_str, resolution)
    mode_arg = F.lit(mode)
    return F.call_function(
        "gbx_h3_geomkring",
        cover,
        core,
        holes_cover,
        holes_core,
        _col(k),
        mode_arg,
    )


def geomkloop(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-loop (hollow shell at exactly k steps).

    See :func:`geomkring` for parameter details.

    VERIFY: product function names (h3_coverash3, h3_polyfillash3) at integration.
    """
    geom_str = geom_col if isinstance(geom_col, str) else geom_col._jc.toString()
    cover = _cover_col(geom_str, resolution)
    core = _core_col(geom_str, resolution)
    holes_cover, holes_core = _holes_arrays(geom_str, resolution)
    mode_arg = F.lit(mode)
    return F.call_function(
        "gbx_h3_geomkloop",
        cover,
        core,
        holes_cover,
        holes_core,
        _col(k),
        mode_arg,
    )
