"""Light vector readers (*_gbx) — pyogrio-backed PySpark DataSource V2, emitting
the same schema as the heavyweight Scala OGR readers (geom_j WKB + srid + proj4 +
typed attributes). Pure-Python / Serverless-safe (no JVM)."""

from __future__ import annotations

from typing import Dict, List, Tuple

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# OGR field type (+ subtype) -> Spark type, matching heavy OGR_SchemaInference.getType.
_OGR_TO_SPARK = {
    "OFTInteger": IntegerType,
    "OFTInteger64": LongType,
    "OFTReal": DoubleType,
    "OFTString": StringType,
    "OFTWideString": StringType,
    "OFTDate": DateType,
    "OFTTime": TimestampType,
    "OFTDateTime": TimestampType,
    "OFTBinary": BinaryType,
}
_OGR_LIST_TO_SPARK = {
    "OFTIntegerList": IntegerType,
    "OFTRealList": DoubleType,
    "OFTStringList": StringType,
    "OFTWideStringList": StringType,
}


def _ogr_to_spark(ogr_type: str, subtype: str):
    if subtype == "OFSTBoolean":
        return BooleanType()
    if ogr_type in _OGR_LIST_TO_SPARK:
        return ArrayType(_OGR_LIST_TO_SPARK[ogr_type]())
    return _OGR_TO_SPARK.get(ogr_type, StringType)()


def _geom_name(info: Dict) -> str:
    # Heavy uses the OGR geom field name if present, else geom_0 (single-geom v1).
    return info.get("geometry_name") or "geom_0"


def _vector_schema(info: Dict, as_wkb: bool) -> StructType:
    fields: List[StructField] = []
    names = list(info.get("fields", []))
    ogr_types = list(info.get("ogr_types", []))
    subtypes = list(info.get("ogr_subtypes", []))
    for j, name in enumerate(names):
        col = name if name else f"field_{j}"
        ot = ogr_types[j] if j < len(ogr_types) else "OFTString"
        st = subtypes[j] if j < len(subtypes) else "OFSTNone"
        fields.append(StructField(col, _ogr_to_spark(ot, st), True))
    gname = _geom_name(info)
    geom_type = BinaryType() if as_wkb else StringType()
    fields.append(StructField(gname, geom_type, True))
    fields.append(StructField(gname + "_srid", StringType(), True))
    fields.append(StructField(gname + "_srid_proj", StringType(), True))
    return StructType(fields)


def _crs_to_srid_proj(crs) -> Tuple[str, str]:
    """(authority code string e.g. '4326' or '0', PROJ4 string or '')."""
    if not crs:
        return "0", ""
    try:
        from pyproj import CRS

        c = CRS.from_user_input(crs)
        auth = c.to_authority()
        srid = auth[1] if auth else "0"
        try:
            proj4 = c.to_proj4() or ""
        except Exception:
            proj4 = ""
        return srid, proj4
    except Exception:
        return "0", ""
