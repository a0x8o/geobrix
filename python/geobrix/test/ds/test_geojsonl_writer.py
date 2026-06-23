"""Tests for the multi-file GeoJSONL writer (geojsonl_gbx).

Unlike the single-file vector writers, geojsonl_gbx writes a DIRECTORY of
newline-delimited GeoJSONL shards -- one per non-empty partition, NO driver
merge. An optional maxRecordsPerFile splits a large partition into multiple
shards. Round-trips with the geojson_gbx (multi=true) directory reader.
"""

import os

import pytest
from pyspark.sql import functions as F
from shapely import Point
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.ds.register import register


def _wkb_df(spark, n=6):
    rows = [
        (str(i), i, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(n)
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    )


def _shards(out):
    return [n for n in os.listdir(out) if n.endswith((".geojsonl", ".geojsons"))]


def test_multi_file_one_shard_per_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "shards")
    df = _wkb_df(spark, 6).repartition(3, F.col("geom_0"))
    nparts = df.rdd.getNumPartitions()
    df.write.format("geojsonl_gbx").mode("overwrite").save(out)

    # Output is a DIRECTORY with EXACTLY one .geojsonl shard per (non-empty) partition.
    assert os.path.isdir(out)
    shards = _shards(out)
    assert (
        len(shards) == nparts
    ), f"expected exactly {nparts} shards (one per partition), got {len(shards)}: {shards}"

    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 6
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(6)}
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}


def test_max_records_per_file_splits_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "split")
    n, k = 10, 3  # ceil(10/3) == 4 shards
    _wkb_df(spark, n).repartition(1, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).option("maxRecordsPerFile", str(k)).save(out)

    shards = _shards(out)
    expected = -(-n // k)  # ceil
    assert len(shards) == expected, f"expected {expected} shards, got {shards}"

    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == n


def test_output_is_directory_not_single_file(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "dir")
    _wkb_df(spark, 4).repartition(2, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    assert os.path.isdir(out)
    assert not os.path.isfile(out)
    # every published member is a shard (plus an optional _SUCCESS marker)
    members = set(os.listdir(out)) - {"_SUCCESS"}
    assert members and all(m.endswith((".geojsonl", ".geojsons")) for m in members)


def test_wkt_input_roundtrips(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "wkt")
    rows = [
        ("a", "POINT (-73.9 40.7)", "4326", ""),
        ("b", "POINT (-0.1 51.5)", "4326", ""),
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 string, geom_0_srid string, geom_0_srid_proj string",
    )
    df.repartition(2, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 2
    assert {r["name"] for r in back.collect()} == {"a", "b"}


def test_overwrite_clears_previous_shards(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "ow")
    _wkb_df(spark, 8).repartition(4, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    first = set(_shards(out))
    # rewrite with fewer rows/partitions -> stale shards must be gone
    _wkb_df(spark, 2).repartition(1, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    second = set(_shards(out))
    assert first.isdisjoint(second), "overwrite left stale shards behind"
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 2


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "exists")
    _wkb_df(spark, 4).repartition(2, F.col("geom_0")).write.format("geojsonl_gbx").mode(
        "overwrite"
    ).save(out)
    with pytest.raises(Exception) as ei:
        _wkb_df(spark, 4).write.format("geojsonl_gbx").mode("append").save(out)
    assert "append" in str(ei.value).lower()


def test_serverless_safe_no_jvm_or_conf():
    """The writer code must not touch the JVM, sparkContext, spark.conf, or RDDs."""
    import inspect

    from databricks.labs.gbx.ds import vector

    src = inspect.getsource(vector.GeoJSONLGbxWriter)
    for forbidden in ("_jvm", "sparkContext", "spark.conf.set", ".rdd"):
        assert forbidden not in src, f"{forbidden} found in GeoJSONLGbxWriter"
