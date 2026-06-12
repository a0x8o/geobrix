import pytest
from shapely import Point, LineString, Polygon, to_wkb
from shapely import from_wkb as _from_wkb

from databricks.labs.gbx.ds.vector import (
    _geometry_type_of,
    _srid_to_crs,
    _writer_col_roles,
)
from databricks.labs.gbx.ds.register import register
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def test_geometry_type_of_point_and_line():
    assert _geometry_type_of(to_wkb(Point(1, 2))) == "Point"
    assert _geometry_type_of(to_wkb(LineString([(0, 0), (1, 1)]))) == "LineString"


def test_srid_to_crs():
    assert _srid_to_crs("4326", "") == "EPSG:4326"
    assert _srid_to_crs("0", "+proj=longlat +datum=WGS84 +no_defs") == (
        "+proj=longlat +datum=WGS84 +no_defs"
    )
    assert _srid_to_crs("0", "") is None
    assert _srid_to_crs("", "") is None


def test_writer_col_roles_named_geom():
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("pop", IntegerType()),
            StructField("SHAPE", BinaryType()),
            StructField("SHAPE_srid", StringType()),
            StructField("SHAPE_srid_proj", StringType()),
        ]
    )
    geom, srid, proj, attrs = _writer_col_roles(schema)
    assert (geom, srid, proj) == ("SHAPE", "SHAPE_srid", "SHAPE_srid_proj")
    assert attrs == ["name", "pop"]


def _wkb_df(spark):
    rows = [
        ("a", 10, bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", 20, bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    return spark.createDataFrame(
        rows, schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string"
    )


def test_geojson_roundtrip_single_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "out.geojson")
    _wkb_df(spark).coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)

    back = spark.read.format("ogr_gbx").load(out)
    assert back.count() == 2
    got = {r["name"]: r["pop"] for r in back.collect()}
    assert got == {"a": 10, "b": 20}
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}


def test_multi_partition_merge(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "multi.geojson")
    rows = [
        (str(i), i, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(50)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(4)
    assert df.rdd.getNumPartitions() == 4
    df.write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("ogr_gbx").load(out)
    assert back.count() == 50
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(50)}


def test_crs_roundtrips(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "crs.geojson")
    _wkb_df(spark).coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("ogr_gbx").load(out)
    scol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0]
    assert {r[scol] for r in back.select(scol).collect()} == {"4326"}


def test_geometry_type_override(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "poly.geojson")
    poly = to_wkb(Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]))
    df = spark.createDataFrame(
        [("p", bytearray(poly), "4326", "")],
        schema="name string, geom_0 binary, geom_0_srid string, "
        "geom_0_srid_proj string",
    )
    df.coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).option("geometryType", "Polygon").save(out)
    back = spark.read.format("ogr_gbx").load(out)
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert _from_wkb(bytes(back.collect()[0][gcol])).geom_type == "Polygon"


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "exists.geojson")
    _wkb_df(spark).coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    with pytest.raises(Exception) as ei:
        _wkb_df(spark).write.format("ogr_gbx").mode("append").option(
            "driverName", "GeoJSON"
        ).save(out)
    assert "append" in str(ei.value).lower()
