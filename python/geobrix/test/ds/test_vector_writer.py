import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from shapely import LineString, Point, Polygon
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds.vector import (
    VectorGbxWriter,
    _geometry_type_of,
    _srid_to_crs,
    _writer_col_roles,
)


def test_default_gpkg_layer_avoids_reserved_prefix():
    from databricks.labs.gbx.ds.vector import _default_gpkg_layer

    assert _default_gpkg_layer("/a/b/roads") == "roads"
    assert _default_gpkg_layer("/a/b/roads.gpkg") == "roads"
    assert _default_gpkg_layer("/a/b/gpkg") == "layer"  # reserved prefix
    assert _default_gpkg_layer("/a/b/gpkgdata") == "layer"
    assert _default_gpkg_layer("/") == "layer"  # empty stem


def test_gpkg_write_to_path_named_gpkg(spark, tmp_path):
    # Saving to a path whose stem is 'gpkg' must not fail with the GeoPackage
    # reserved-prefix error (the layer name falls back to 'layer').
    register(spark)
    out = str(tmp_path / "gpkg")  # stem == "gpkg"
    rows = [("a", bytearray(to_wkb(Point(1.0, 2.0))), "4326", "")]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    )
    df.write.format("gpkg_gbx").mode("overwrite").save(out)  # must not raise
    back = spark.read.format("gpkg_gbx").load(out)
    assert back.count() == 1


def test_non_geojson_commit_streams_without_concat(spark, tmp_path, monkeypatch):
    # GPKG/Shapefile/FileGDB must append fragments per-partition (bounded driver
    # memory), NOT concat every partition into one in-memory table (which OOMs a
    # single-node driver on large inputs). Make concat_tables raise; a 2-partition
    # GPKG write must still succeed.
    import pyarrow as pa

    monkeypatch.setattr(
        pa,
        "concat_tables",
        lambda *a, **k: pytest.fail("commit must not concat for GPKG"),
    )
    register(spark)
    out = str(tmp_path / "streamed.gpkg")
    rows = [
        ("a", bytearray(to_wkb(Point(1.0, 2.0))), "4326", ""),
        ("b", bytearray(to_wkb(Point(3.0, 4.0))), "4326", ""),
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    ).repartition(2, F.col("geom_0"))
    df.write.format("gpkg_gbx").mode("overwrite").save(out)  # must not raise / concat
    assert spark.read.format("gpkg_gbx").load(out).count() == 2


def test_should_stream_policy():
    # All pyogrio single-file writers (GeoJSON, Shapefile, GPKG) stream fragments
    # into one write (bounded driver memory, single pass). OpenFileGDB uses the
    # native osgeo path and is excluded.
    from databricks.labs.gbx.ds.vector import _should_stream

    assert _should_stream("GeoJSON") is True
    assert _should_stream("ESRI Shapefile") is True
    assert _should_stream("GPKG") is True
    assert _should_stream("OpenFileGDB") is False


def test_stream_record_batches_chains_and_drops_meta(tmp_path):
    import pyarrow as pa
    import pyarrow.feather as feather

    from databricks.labs.gbx.ds.vector import _stream_record_batches

    schema = pa.schema(
        [
            ("name", pa.string()),
            ("geom_0", pa.binary()),
            ("geom_0_srid", pa.string()),
            ("geom_0_srid_proj", pa.string()),
        ]
    )
    f1 = str(tmp_path / "frag1.arrow")
    f2 = str(tmp_path / "frag2.arrow")
    feather.write_feather(
        pa.table(
            {
                "name": ["a"],
                "geom_0": [b"x"],
                "geom_0_srid": ["4326"],
                "geom_0_srid_proj": [""],
            },
            schema=schema,
        ),
        f1,
    )
    feather.write_feather(
        pa.table(
            {
                "name": ["b", "c"],
                "geom_0": [b"y", b"z"],
                "geom_0_srid": ["4326", "4326"],
                "geom_0_srid_proj": ["", ""],
            },
            schema=schema,
        ),
        f2,
    )
    drop = {"geom_0_srid", "geom_0_srid_proj"}
    batches = list(_stream_record_batches([f1, f2], drop))
    assert sum(b.num_rows for b in batches) == 3  # chained across fragments
    for b in batches:
        assert b.schema.names == ["name", "geom_0"]  # meta cols dropped


def _roundtrip_streamed(spark, tmp_path, driver, name):
    register(spark)
    out = str(tmp_path / name)
    rows = [
        (str(i), None, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(20)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, note string, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(4, F.col("geom_0"))
    df.write.format("vector_gbx").mode("overwrite").option("driverName", driver).save(
        out
    )
    back = spark.read.format("vector_gbx").load(out)
    assert back.count() == 20
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(20)}
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}


def test_geojson_streaming_multipartition_roundtrip(spark, tmp_path):
    # Streamed GeoJSON write round-trips all rows/geoms (incl. an all-null col).
    _roundtrip_streamed(spark, tmp_path, "GeoJSON", "streamed.geojson")


def test_shapefile_streaming_multipartition_roundtrip(spark, tmp_path):
    # Streamed Shapefile write round-trips all rows/geoms (incl. an all-null col).
    _roundtrip_streamed(spark, tmp_path, "ESRI Shapefile", "streamed_shp")


def test_gpkg_streaming_multipartition_roundtrip(spark, tmp_path):
    # Streamed GPKG write (geom column renamed to 'geom' per batch) round-trips.
    _roundtrip_streamed(spark, tmp_path, "GPKG", "streamed.gpkg")


def test_scratch_dir_unique_per_write(tmp_path):
    # Two concurrent writes to the same parent directory must NOT share a scratch
    # dir, or one write's commit cleanup (rmtree) would delete the other's
    # in-flight Arrow fragments.
    schema = StructType(
        [
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    parent = str(tmp_path)
    w1 = VectorGbxWriter(f"{parent}/a.geojson", schema, "GeoJSON", {}, overwrite=True)
    w2 = VectorGbxWriter(f"{parent}/b.geojson", schema, "GeoJSON", {}, overwrite=True)
    assert w1.scratch_dir != w2.scratch_dir
    assert os.path.dirname(w1.scratch_dir) == parent
    assert os.path.dirname(w2.scratch_dir) == parent


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
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    )


def test_geojson_roundtrip_single_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "out.geojson")
    _wkb_df(spark).coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)

    back = spark.read.format("vector_gbx").load(out)
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
    ).repartition(4, F.col("geom_0"))
    assert df.rdd.getNumPartitions() == 4
    df.write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("vector_gbx").load(out)
    assert back.count() == 50
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(50)}


def test_crs_roundtrips(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "crs.geojson")
    _wkb_df(spark).coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("vector_gbx").load(out)
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
    df.coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).option("geometryType", "Polygon").save(out)
    back = spark.read.format("vector_gbx").load(out)
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert _from_wkb(bytes(back.collect()[0][gcol])).geom_type == "Polygon"


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "exists.geojson")
    _wkb_df(spark).coalesce(1).write.format("vector_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    with pytest.raises(Exception) as ei:
        _wkb_df(spark).write.format("vector_gbx").mode("append").option(
            "driverName", "GeoJSON"
        ).save(out)
    assert "append" in str(ei.value).lower()


@pytest.mark.parametrize(
    "fmt,target",
    [
        ("geojson_gbx", "named.geojson"),
        ("gpkg_gbx", "named.gpkg"),
        ("shapefile_gbx", "named.shp"),
    ],
)
def test_named_writer_roundtrip(spark, tmp_path, fmt, target):
    register(spark)
    out = str(tmp_path / target)
    _wkb_df(spark).coalesce(1).write.format(fmt).mode("overwrite").save(out)
    back = spark.read.format(fmt).load(out)
    assert back.count() == 2
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert {
        _from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()
    } == {"Point"}


def _ogr_can_create(driver: str) -> bool:
    try:
        import tempfile

        import pyarrow as pa
        import pyogrio
        from shapely import Point as _P
        from shapely import to_wkb as _twkb

        d = tempfile.mkdtemp()
        path = d + ("/t.gdb" if driver == "OpenFileGDB" else "/t.out")
        tbl = pa.table({"g": [_twkb(_P(0, 0))]})
        pyogrio.write_arrow(
            tbl,
            path,
            driver=driver,
            geometry_name="g",
            geometry_type="Point",
            crs="EPSG:4326",
        )
        return True
    except Exception:
        return False


def test_file_gdb_writer_roundtrip(spark, tmp_path):
    import importlib.util

    register(spark)
    if importlib.util.find_spec("osgeo") is None:
        pytest.skip(
            "native osgeo (heavy GDAL natives) not present; file_gdb_gbx write requires osgeo"
        )
    out = str(tmp_path / "out.gdb")
    _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
    back = spark.read.format("file_gdb_gbx").load(out)
    assert back.count() == 2


def test_gpkg_reads_from_readonly_dir(spark, tmp_path):
    # GPKG written by the writer must read back even when the file/dir are
    # read-only (simulating a read-only Volume) -- i.e. no WAL that forces a
    # reader-side checkpoint write.
    import os
    import stat

    register(spark)
    out = str(tmp_path / "ro.gpkg")
    _wkb_df(spark).coalesce(1).write.format("gpkg_gbx").mode("overwrite").save(out)
    # make the output file + its dir read-only
    os.chmod(out, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    os.chmod(str(tmp_path), stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
    try:
        back = spark.read.format("gpkg_gbx").load(out)
        assert back.count() == 2
    finally:
        os.chmod(str(tmp_path), 0o755)
        os.chmod(out, 0o644)


def test_file_gdb_osgeo_hybrid_roundtrip(spark, tmp_path):
    import pytest

    pytest.importorskip("osgeo", reason="native osgeo (heavy GDAL natives) not present")
    register(spark)
    out = str(tmp_path / "hybrid.gdb")
    _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
    back = spark.read.format("file_gdb_gbx").load(out)
    assert back.count() == 2


def test_file_gdb_clear_error_without_osgeo(spark, tmp_path):
    import importlib.util

    import pytest

    if importlib.util.find_spec("osgeo") is not None:
        pytest.skip("osgeo present -- the no-natives error path is not exercised")
    register(spark)
    out = str(tmp_path / "noosgeo.gdb")
    with pytest.raises(Exception) as ei:
        _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(
            out
        )
    assert "osgeo" in str(ei.value).lower() or "native" in str(ei.value).lower()


def test_gpkg_output_uses_format_default_geom_name(spark, tmp_path):
    # GPKG output should use the format-default geometry column name `geom`,
    # not the input column name, so an arbitrary input name doesn't leak out.
    register(spark)
    out = str(tmp_path / "out.gpkg")
    rows = [("a", bytearray(to_wkb(Point(1.0, 2.0))), "4326", "")]
    df = spark.createDataFrame(
        rows, schema="name string, the_geom binary, epsg string, p4 string"
    )
    (
        df.write.format("gpkg_gbx")
        .mode("overwrite")
        .option("geomCol", "the_geom")
        .option("sridCol", "epsg")
        .save(out)
    )
    import pyogrio

    info = pyogrio.read_info(out)
    assert info["geometry_name"] == "geom"


def test_classic_write_path_roundtrip(spark, tmp_path):
    # Exercise the classic pyogrio.raw.write fallback path directly (write_arrow
    # works locally so the auto-fallback won't trigger here) to prove the
    # arrow-table -> (geometry, field_data, fields) conversion round-trips.
    register(spark)
    import os
    import tempfile

    import pyarrow as pa

    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    out = str(tmp_path / "classic.gpkg")
    w = VectorGbxWriter(
        out, _wkb_df(spark).schema, "GPKG", {"driverName": "GPKG"}, True
    )
    tbl = pa.table(
        {
            "name": ["a", "b"],
            "pop": [10, 20],
            "geom_0": [
                bytearray(to_wkb(Point(-73.9, 40.7))),
                bytearray(to_wkb(Point(-0.1, 51.5))),
            ],
            "geom_0_srid": ["4326", "4326"],
            "geom_0_srid_proj": ["", ""],
        }
    )
    d = tempfile.mkdtemp()
    local_out = os.path.join(d, "classic.gpkg")
    w._write_local_classic([tbl], local_out, "Point", "EPSG:4326")
    back = spark.read.format("gpkg_gbx").load(local_out)
    assert back.count() == 2
