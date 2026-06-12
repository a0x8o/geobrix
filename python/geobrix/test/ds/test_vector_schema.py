from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from databricks.labs.gbx.ds.vector import (
    _crs_to_srid_proj,
    _ogr_to_spark,
    _vector_schema,
)


def test_ogr_to_spark_map():
    assert isinstance(_ogr_to_spark("OFTString", "OFSTNone"), StringType)
    assert isinstance(_ogr_to_spark("OFTInteger", "OFSTNone"), IntegerType)
    assert isinstance(_ogr_to_spark("OFTInteger", "OFSTBoolean"), BooleanType)
    assert isinstance(_ogr_to_spark("OFTInteger64", "OFSTNone"), LongType)
    assert isinstance(_ogr_to_spark("OFTReal", "OFSTNone"), DoubleType)
    assert isinstance(_ogr_to_spark("OFTUnknownFuture", "OFSTNone"), StringType)


def test_vector_schema_matches_heavy_layout():
    info = {
        "fields": ["name", "pop", "area"],
        "ogr_types": ["OFTString", "OFTInteger", "OFTReal"],
        "ogr_subtypes": ["OFSTNone", "OFSTNone", "OFSTNone"],
        "geometry_name": "",
    }
    schema = _vector_schema(info, as_wkb=True)
    names = [f.name for f in schema.fields]
    assert names == ["name", "pop", "area", "geom_0", "geom_0_srid", "geom_0_srid_proj"]
    by = {f.name: f for f in schema.fields}
    assert isinstance(by["pop"].dataType, IntegerType)
    assert isinstance(by["area"].dataType, DoubleType)
    assert isinstance(by["geom_0"].dataType, BinaryType)
    assert isinstance(by["geom_0_srid"].dataType, StringType)
    assert all(f.nullable for f in schema.fields)


def test_vector_schema_wkt_is_string():
    info = {"fields": [], "ogr_types": [], "ogr_subtypes": [], "geometry_name": ""}
    schema = _vector_schema(info, as_wkb=False)
    assert isinstance({f.name: f for f in schema.fields}["geom_0"].dataType, StringType)


def test_crs_to_srid_proj():
    srid, proj4 = _crs_to_srid_proj("EPSG:4326")
    assert srid == "4326"
    assert "+proj=longlat" in proj4
    assert _crs_to_srid_proj(None) == ("0", "")
