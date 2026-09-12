"""H3 geometry-aware kring/kloop — light-tier public API.

h3 is light-tier only (no Scala/heavy equivalent), but it now behaves exactly
like the other grids: a single geom-taking function does the whole job in pure
Python via the ``h3`` library (polyfill + neighbour walk), so it needs no
Databricks product functions and runs anywhere (local + Serverless + classic).

    from databricks.labs.gbx.pygx.functions import register
    from databricks.labs.gbx.gridx.h3.functions import geomkring, geomkloop

    register(spark)
    df.withColumn("kring", geomkring("geom_col", resolution=9, k=1))

These are thin aliases of the registered ``gbx_h3_geomkring`` / ``gbx_h3_geomkloop``
UDFs — the same functions SQL calls — so the Python and SQL surfaces are one and
the same. The explode variants are SQL-``LATERAL`` table functions with no Column
form (see pygx.functions.h3_geomkringexplode).
"""

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

from databricks.labs.gbx.pygx import functions as _pygx

ColLike = Union[Column, str, bool, int, float, bytes]


def _geom(x: Union[str, Column]) -> Column:
    """A bare string is a column NAME here (per the documented usage); wrap it."""
    return F.col(x) if isinstance(x, str) else x


def geomkring(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-ring (filled disk) from a geometry column.

    Args:
        geom_col:   Geometry column (WKB BINARY or WKT STRING).
        resolution: H3 resolution (0..15).
        k:          Ring distance (0 = covering set only).
        mode:       Dilation mode (default "boundary-out"); one of the 6 modes.

    Returns:
        Column of ARRAY<BIGINT> H3 cell ids.
    """
    return _pygx.h3_geomkring(_geom(geom_col), resolution, k, mode)


def geomkloop(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-loop (hollow shell at exactly k steps).

    See :func:`geomkring` for parameters.
    """
    return _pygx.h3_geomkloop(_geom(geom_col), resolution, k, mode)
