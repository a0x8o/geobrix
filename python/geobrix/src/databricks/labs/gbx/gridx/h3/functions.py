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

Product function names/semantics verified on e2-demo-field-eng 2026-09-12:
h3_coverash3(STRING|BINARY, res) = overlap (p_cover); h3_polyfillash3 =
contained (p_core). Both reject a GEOMETRY value, so ST outputs are wrapped in
ST_AsBinary. Geometry input: WKB BINARY (canonical) for every mode; WKT STRING
also works for boundary-out / boundary-in (which skip the ST/hole chain).

Because these wrappers delegate the geometry work to Databricks product
functions, geomkring/geomkloop fail fast with a clear RuntimeError when invoked
off Databricks (no product h3_*/ST_*) — see _require_databricks_product. The
pre-computed-array SQL UDFs and the dilation engine have no such dependency.
"""

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

ColLike = Union[Column, str, bool, int, float, bytes]

# Databricks product functions the composition relies on (columnar geometry
# work). Presence is probed once per session to fail fast off-Databricks.
_PRODUCT_SENTINEL = "h3_coverash3"
_checked_sessions: set = set()


def _require_databricks_product() -> None:
    """Fail fast with a clear message when the Databricks product H3/ST functions
    the composition needs aren't available (i.e. invoked off Databricks).

    These geometry-aware H3 functions delegate the geometry→cell work to
    Databricks-native ``h3_coverash3``/``h3_polyfillash3`` (and ``ST_*`` for hole
    extraction), so the Python column API only runs on Databricks (Serverless or
    a Databricks cluster). Off-Databricks the columns would otherwise fail at
    execution with an opaque ``UNRESOLVED_ROUTINE``; probe once per session
    (``DESCRIBE FUNCTION``, which resolves built-ins too) and raise a clear
    RuntimeError instead. If there is no active session, or the probe itself is
    unavailable, defer to lazy execution rather than block.
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415

    spark = SparkSession.getActiveSession()
    if spark is None:
        return
    key = id(spark)
    if key in _checked_sessions:
        return
    try:
        spark.sql(f"DESCRIBE FUNCTION {_PRODUCT_SENTINEL}").collect()
    except Exception as exc:  # product function not resolvable -> not on Databricks
        raise RuntimeError(
            "GeoBrix h3 geometry-aware functions (geomkring/geomkloop) require the "
            "Databricks product H3/ST SQL functions (h3_coverash3, h3_polyfillash3, "
            "ST_*), which are not available in this Spark session. These light-tier "
            "functions must run on Databricks (Serverless or a Databricks cluster). "
            "The pre-computed-array SQL UDFs (gbx_h3_geomkring etc.) and the dilation "
            "engine run anywhere; only this geometry composition needs Databricks."
        ) from exc
    _checked_sessions.add(key)


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

    h3_coverash3 returns cells whose hexagon intersects the geometry (overlap
    semantics = _dilate p_cover). Verified 2026-09-12 (see module docstring).
    """
    return F.call_function("h3_coverash3", geom_col, F.lit(int(resolution)))


def _core_col(geom_col: Column, resolution: int) -> Column:
    """H3 cells fully inside the geometry at `resolution` (product h3_polyfillash3).

    Uses F.call_function for Spark Connect / Serverless compatibility.

    h3_polyfillash3 returns cells fully contained by the geometry (containment
    semantics = _dilate p_core). Verified 2026-09-12 (see module docstring).
    """
    return F.call_function("h3_polyfillash3", geom_col, F.lit(int(resolution)))


# Modes whose traversal reads the holes classification (h_cover/h_core):
# expand from the holes. boundary-in-ignore-holes reads s_core instead (see
# _solid_core below), not the holes. boundary-out/boundary-in touch neither, so
# they skip the ST chain entirely — which also keeps WKT-string input working.
_HOLE_MODES = frozenset({"hole-in", "hole-out", "hole-out-ignore-geom"})

# The one mode that reads the solid-fill core (s_core).
_SOLID_CORE_MODE = "boundary-in-ignore-holes"


def _solid(geom: Column) -> Column:
    """The solid (outer ring, holes filled) as a GEOMETRY: ST_MakePolygon(ST_ExteriorRing(g))."""
    g = F.call_function("ST_GeomFromWKB", geom)
    return F.call_function("ST_MakePolygon", F.call_function("ST_ExteriorRing", g))


def _solid_core_col(geom: Column, resolution: int, mode: str) -> Column:
    """ARRAY<BIGINT> cells fully inside the solid — the faithful s_core.

    Only boundary-in-ignore-holes reads s_core, so this emits the ST/product
    chain for that mode alone and returns an empty column otherwise. Polyfilling
    the solid directly (h3_polyfillash3) yields the exact solid core, avoiding
    the hole-rim-straddle under-count of reconstructing s_core = core|holes_core.
    Requires WKB BINARY `geom` (ST_GeomFromWKB parses it).
    """
    if mode != _SOLID_CORE_MODE:
        return F.lit(None).cast("array<bigint>")
    solid_wkb = F.call_function("ST_AsBinary", _solid(geom))
    return F.call_function("h3_polyfillash3", solid_wkb, F.lit(int(resolution)))


def _holes_arrays(geom: Column, resolution: int, mode: str):
    """Return (holes_cover_col, holes_core_col) for the geometry's holes union.

    For modes that don't read holes (boundary-out, boundary-in) this returns
    empty-array columns and emits no ST/product calls — so those modes also keep
    accepting WKT-string geometry. For hole-reading modes it extracts the holes
    union in a single columnar expression that handles any number of interior
    rings without a per-row loop:

        holes = ST_Difference(ST_MakePolygon(ST_ExteriorRing(g)), g)

    i.e. (outer ring filled) minus (the donut) = exactly the holes. Then the
    product h3_* functions polyfill it. All function names/semantics verified on
    e2-demo-field-eng 2026-09-12 (see the SDD ledger):
      - holes_cover = h3_coverash3(ST_AsBinary(holes), res)   → h_cover (overlap)
      - holes_core  = h3_polyfillash3(ST_AsBinary(holes), res) → h_core (contained)

    Holeless geometries: ST_Difference → POLYGON EMPTY → h3_coverash3 returns an
    empty (size-0) array, so hole modes correctly no-op. h3_* reject a GEOMETRY
    value, hence the ST_AsBinary wrap.

    For a hole-reading mode, `geom` must be a WKB BINARY geometry column (the
    GeoBrix canonical form); ST_GeomFromWKB parses it for the ST chain.
    """
    empty = F.lit(None).cast("array<bigint>")
    if mode not in _HOLE_MODES:
        return empty, empty
    g = F.call_function("ST_GeomFromWKB", geom)
    solid = F.call_function("ST_MakePolygon", F.call_function("ST_ExteriorRing", g))
    holes_wkb = F.call_function(
        "ST_AsBinary", F.call_function("ST_Difference", solid, g)
    )
    res_lit = F.lit(int(resolution))
    holes_cover = F.call_function("h3_coverash3", holes_wkb, res_lit)
    holes_core = F.call_function("h3_polyfillash3", holes_wkb, res_lit)
    return holes_cover, holes_core


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
        geom_col:   Column name (str) or Column expression holding the geometry.
                    WKB BINARY is canonical and required for hole-reading modes
                    (boundary-in-ignore-holes, hole-*); WKT STRING also works for
                    boundary-out / boundary-in.
        resolution: H3 resolution (0..15).
        k:          Ring distance (0 = covering set only).
        mode:       Dilation mode (default "boundary-out"). One of the 6 modes
                    in _dilate.MODES.

    Returns:
        Column of ARRAY<BIGINT> h3 cell ids.
    """
    _require_databricks_product()
    geom = _col(geom_col)
    cover = _cover_col(geom, resolution)
    core = _core_col(geom, resolution)
    holes_cover, holes_core = _holes_arrays(geom, resolution, mode)
    solid_core = _solid_core_col(geom, resolution, mode)
    return F.call_function(
        "gbx_h3_geomkring",
        cover,
        core,
        holes_cover,
        holes_core,
        solid_core,
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
    """
    _require_databricks_product()
    geom = _col(geom_col)
    cover = _cover_col(geom, resolution)
    core = _core_col(geom, resolution)
    holes_cover, holes_core = _holes_arrays(geom, resolution, mode)
    solid_core = _solid_core_col(geom, resolution, mode)
    return F.call_function(
        "gbx_h3_geomkloop",
        cover,
        core,
        holes_cover,
        holes_core,
        solid_core,
        _col(k),
        F.lit(mode),
    )
