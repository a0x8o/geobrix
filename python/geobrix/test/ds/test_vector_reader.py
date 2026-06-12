import json
import os

from shapely import from_wkb

from databricks.labs.gbx.ds.register import register

_GJ = {
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "a", "pop": 10},
            "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
        },
        {
            "type": "Feature",
            "properties": {"name": "b", "pop": 20},
            "geometry": {"type": "Point", "coordinates": [-0.1, 51.5]},
        },
    ],
}


def _gj_path(tmp):
    p = os.path.join(tmp, "pts.geojson")
    with open(p, "w") as f:
        json.dump(_GJ, f)
    return p


def test_vector_gbx_reads_wkb_schema(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("vector_gbx").load(p)
    assert df.columns == ["name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj"]
    rows = df.orderBy("name").collect()
    assert rows[0]["name"] == "a" and rows[0]["pop"] == 10
    assert rows[0]["geom_0_srid"] == "4326"
    assert from_wkb(bytes(rows[0]["geom_0"])).geom_type == "Point"
    assert df.count() == 2


def test_vector_gbx_wkt_option(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("vector_gbx").option("asWKB", "false").load(p)
    g = df.orderBy("name").collect()[0]["geom_0"]
    assert isinstance(g, str) and g.upper().startswith("POINT")


def test_vector_gbx_chunksize_reads_all(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("vector_gbx").option("chunkSize", "1").load(p)
    # chunkSize=1 over 2 features -> multiple partitions; union still 2 rows
    assert df.rdd.getNumPartitions() >= 2
    assert df.count() == 2


def test_ogr_gbx_reads_directory(spark, tmp_path):
    register(spark)
    d = os.path.join(str(tmp_path), "many")
    os.makedirs(d)
    for k in range(3):
        with open(os.path.join(d, f"p{k}.geojson"), "w") as f:
            json.dump(_GJ, f)
    df = spark.read.format("geojson_gbx").load(d)
    assert df.count() == 6  # 3 files x 2 features


def test_vector_gbx_read_yields_recordbatch_wkb(spark, tmp_path):
    """read() must be Arrow-native: yield pyarrow.RecordBatch (not Python tuples),
    with WKB geometry that round-trips."""
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import VectorGbxReader

    p = _gj_path(str(tmp_path))
    rdr = VectorGbxReader({"path": p})
    parts = list(rdr.partitions())
    batches = []
    for part in parts:
        for b in rdr.read(part):
            assert isinstance(b, pa.RecordBatch)
            batches.append(b)
    tbl = pa.Table.from_batches(batches)
    assert tbl.column_names == [
        "name",
        "pop",
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
    ]
    names = tbl.column("name").to_pylist()
    srids = set(tbl.column("geom_0_srid").to_pylist())
    geoms = tbl.column("geom_0").to_pylist()
    assert set(names) == {"a", "b"}
    assert srids == {"4326"}
    assert all(from_wkb(bytes(g)).geom_type == "Point" for g in geoms)


def test_vector_gbx_read_yields_recordbatch_wkt(spark, tmp_path):
    """asWKB=false: vectorized WKB->WKT in the Arrow output."""
    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import _GeoJSONReader

    p = _gj_path(str(tmp_path))
    rdr = _GeoJSONReader({"path": p, "asWKB": "false"})
    batches = [b for part in rdr.partitions() for b in rdr.read(part)]
    tbl = pa.Table.from_batches(batches)
    assert pa.types.is_string(tbl.schema.field("geom_0").type)
    for g in tbl.column("geom_0").to_pylist():
        assert isinstance(g, str) and g.upper().startswith("POINT")


def test_shapefile_gbx_reads_directory_of_shp_zip(spark, tmp_path):
    """A directory of copy_*.shp.zip files is enumerated and read by shapefile_gbx.
    Each .shp.zip contains a small shapefile written by the shapefile_gbx writer so
    the round-trip exercises the actual path the scaled bench uses."""
    import zipfile

    from databricks.labs.gbx.bench.corpus_vector import (
        generate_polygon_seed,
        transcode_vector_seed,
    )

    register(spark)
    n_features = 10
    seed_df = generate_polygon_seed(spark, n_features)
    seeds = transcode_vector_seed(spark, seed_df, ["shapefile_gbx"], str(tmp_path / "seeds"))
    shp_zip = seeds["shapefile_gbx"]
    assert shp_zip.endswith(".shp.zip")

    # Build a copies directory with 2 copies of the seed .shp.zip
    copies_dir = str(tmp_path / "copies")
    os.makedirs(copies_dir)
    import shutil

    for i in range(2):
        shutil.copy(shp_zip, os.path.join(copies_dir, f"copy_{i}.shp.zip"))

    df = spark.read.format("shapefile_gbx").load(copies_dir)
    assert df.count() == n_features * 2  # 2 copies × n_features features each
