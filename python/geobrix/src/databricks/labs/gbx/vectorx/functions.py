"""VectorX Python API.

Thin wrappers around GeoBrix Scala functions (``gbx_st_*``). Register with
``vx.register(spark)`` then use the functions on Spark columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_st_<name>;

As of v0.4.0 this package exposes the ``gbx_st_asmvt`` MVT aggregator and
``gbx_st_asmvt_pyramid`` MVT pyramid generator. Subsequent waves add more.

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)``. Strings and ``Column`` values pass through
unchanged — pyspark treats a bare string as a dataframe column reference
(``f.col("name")``); wrap in ``f.lit(...)`` to pass a string literal
(e.g. ``vx.st_asmvt(geom, attrs, f.lit("roads"))``).
"""

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through.

    Strings stay as strings so pyspark's call_function treats them as column
    references. Use f.lit("...") for string literals.
    """
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def register(spark: SparkSession) -> None:
    """Register VectorX expression-level SQL functions with the Spark session.

    Call once (e.g. after creating the session) so that ``gbx_st_*``
    expression-level functions are available. Delegates to the JVM
    ``com.databricks.labs.gbx.vectorx.functions.register`` entry point —
    this is independent from the data-source registry used by other GeoBrix
    packages because VectorX expression-level functions are new in v0.4.0.

    Args:
        spark: Spark session (uses active session if not provided).
    """
    spark = spark or SparkSession.builder.getOrCreate()
    spark._jvm.com.databricks.labs.gbx.vectorx.functions.register(spark._jsparkSession)


def st_asmvt(geom_wkb: ColLike, attrs: ColLike, layer_name: ColLike) -> Column:
    """Aggregator: encode a group of features into a Mapbox Vector Tile (MVT) protobuf blob.

    Args:
        geom_wkb: Per-row geometry in WKB (BINARY) column, in tile-local coordinates.
        attrs:    Per-row attribute struct column (all fields stringified in v0.4.0).
        layer_name: Constant MVT layer name. Pass a plain ``str`` for a literal layer
                    name (auto-wrapped with ``f.lit``), or a ``Column`` to reference
                    a column. To reference a column by name, use ``f.col("...")``.

    Returns:
        Aggregate Column producing the MVT protobuf bytes (``BINARY``) for one tile layer.
    """
    if isinstance(layer_name, str):
        layer_name = f.lit(layer_name)
    return f.call_function(
        "gbx_st_asmvt", _col(geom_wkb), _col(attrs), _col(layer_name)
    )


def st_asmvt_pyramid(
    geom_wkb: ColLike,
    attrs: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    layer_name: Union[ColLike, None] = None,
    extent: Union[ColLike, None] = None,
) -> Column:
    """Generator: emit one row per intersecting ``(z, x, y)`` tile across ``[min_z, max_z]``.

    Per-row output column is a struct
    ``tile: STRUCT<z INT, x INT, y INT, mvt_bytes BINARY>``. Invoke directly in
    ``select(...)`` (top-level generator, do not wrap in ``F.explode``).

    Inputs are assumed in EPSG:4326 lon/lat. Per-tile clip + MVT encode happen
    in the helper; the row output is ready to feed into ``gbx_pmtiles_agg`` for
    end-to-end vector publishing. ``max_z`` capped at 20; total tile-count
    across the requested zoom range capped at 10^6.

    Args:
        geom_wkb:   Per-feature geometry in WKB (BINARY) column.
        attrs:      Per-feature attribute struct column (all fields stringified in v0.4.0).
        min_z:      Inclusive minimum zoom level.
        max_z:      Inclusive maximum zoom level (<= 20).
        layer_name: Constant MVT layer name. Pass a plain ``str`` for a literal
                    layer name (auto-wrapped with ``f.lit``).
        extent:     MVT tile extent in pixels (default 4096).

    Returns:
        Generator Column producing one row per intersecting tile.
    """
    layer_name_col = (
        f.lit("layer")
        if layer_name is None
        else (f.lit(layer_name) if isinstance(layer_name, str) else _col(layer_name))
    )
    extent_col = f.lit(4096) if extent is None else _col(extent)
    return f.call_function(
        "gbx_st_asmvt_pyramid",
        _col(geom_wkb),
        _col(attrs),
        _col(min_z),
        _col(max_z),
        layer_name_col,
        extent_col,
    )
