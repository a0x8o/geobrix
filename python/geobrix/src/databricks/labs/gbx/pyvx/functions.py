"""pyvx light VectorX API — MVT functions (Serverless-safe).

Signatures mirror databricks.labs.gbx.vectorx.functions so light <-> heavy is a
one-line import swap. Register once with vx.register(spark), then use on columns.
"""
from typing import Union

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType

from . import _env, _mvt

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


# --- st_asmvt: grouped-aggregate pandas UDF -------------------------------------------------
# Type hints (pd.Series, pd.Series) -> bytes are detected as GROUPED_AGG (Series-to-Scalar)
# by PySpark 3+.  Each series element for a struct column arrives as a plain dict.


@pandas_udf(BinaryType())
def _asmvt_udf(geom: pd.Series, attrs: pd.Series, layer: pd.Series) -> bytes:
    """Grouped-agg: encode one group's features into a single MVT layer blob."""
    layer_name = "layer"
    if layer is not None and len(layer) > 0 and layer.iloc[0] is not None:
        layer_name = str(layer.iloc[0])
    feats = [
        {"geometry": bytes(g), "properties": a}
        for g, a in zip(geom, attrs)
        if g is not None and len(bytes(g)) > 0
    ]
    return _mvt.encode_layer(feats, layer_name=layer_name)


def register(spark: SparkSession = None) -> None:
    """Register the pyvx MVT SQL functions (Serverless-safe: udf/udtf only)."""
    _env.assert_mvt_available()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.udf.register("gbx_st_asmvt", _asmvt_udf)
    # st_asmvt_pyramid registration is added in Task 5.


def st_asmvt(geom_wkb: ColLike, attrs: ColLike, layer_name: ColLike) -> Column:
    """Aggregator: encode a group of features into an MVT protobuf blob (BINARY).

    geom_wkb: per-row WKB geometry in tile-local coordinates.
    attrs:    per-row attribute struct (native-typed in the output tile).
    layer_name: constant MVT layer name (plain str -> literal).
    """
    if isinstance(layer_name, str):
        layer_name = f.lit(layer_name)
    return _asmvt_udf(_col(geom_wkb), _col(attrs), _col(layer_name))
