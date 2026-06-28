import os

import pytest

pyspark = pytest.importorskip("pyspark")

from test.sample._fake_overture_catalog import open_fake_overture  # noqa: E402

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.gbx.sample.overture import (  # noqa: E402
    _META_COLS,
    OvertureClient,
    _meta_dataframe,
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("overture-sp1-test")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_discover_columns_and_filter(spark):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    df = client.discover((-122.45, 37.74, -122.40, 37.78), themes=["buildings"])
    assert df.columns == ["theme", "type", "href", "asset_bbox", "release"]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["release"] == "2024-07-01"
    assert rows[0]["asset_bbox"] == [-122.52, 37.70, -122.36, 37.83]


def test_discover_all_themes_when_none(spark):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    df = client.discover((-180, -90, 180, 90), themes=None)
    # fake catalog has a building + a place; both fall inside the world bbox
    assert {r["type"] for r in df.collect()} == {"building", "place"}


def _write_fake_overture_parquet(spark, path, bbox_struct=True):
    """Write a tiny GeoParquet-ish parquet with a bbox struct so pushdown can fire."""
    if bbox_struct:
        rows = [
            Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76)),
            Row(
                id=2, bbox=Row(xmin=10.0, ymin=50.0, xmax=10.1, ymax=50.1)
            ),  # outside SF
        ]
    else:
        rows = [Row(id=1), Row(id=2)]
    spark.createDataFrame(rows).write.mode("overwrite").parquet(path)


def test_download_distributed_writes_aoi_subset(spark, tmp_path):
    src = str(tmp_path / "src.parquet")
    _write_fake_overture_parquet(spark, src)
    out_dir = str(tmp_path / "out")

    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            )
        ],
        schema,
    )
    meta = client._download_distributed(
        assets,
        out_dir,
        bbox=(-122.45, 37.74, -122.40, 37.78),
        validate=True,
        partitions=4,
    )
    # Serverless-safety: hash-by-column repartition is NOT AQE-coalesced to 1.
    assert meta.rdd.getNumPartitions() > 1
    mrows = meta.collect()
    assert len(mrows) == 1
    written = mrows[0]["source"]
    assert written == mrows[0]["path"]  # source aliased as path
    assert os.path.isdir(written) or os.path.exists(written)
    # only the in-AOI row survived the bbox-struct pushdown
    subset = spark.read.parquet(written)
    assert subset.count() == 1
    assert subset.collect()[0]["id"] == 1


def test_empty_meta_dataframe_matches_meta_cols(spark):
    """Empty-result meta DataFrame must have the exact _META_COLS schema.

    Tasks 7/8 union meta DataFrames; a schema mismatch on the empty branch
    (missing path/last_update) causes a union error at runtime.
    """
    empty_meta = _meta_dataframe(spark, [], partitions=2)

    # Column names must match _META_COLS exactly (order included).
    assert empty_meta.columns == _META_COLS

    # path and last_update must carry the right types (not missing or string-only).
    schema_map = {f.name: f.dataType for f in empty_meta.schema}
    assert "path" in schema_map, "path column missing from empty meta DataFrame"
    assert isinstance(
        schema_map["path"], StringType
    ), f"path should be StringType, got {schema_map['path']}"
    assert (
        "last_update" in schema_map
    ), "last_update column missing from empty meta DataFrame"
    assert isinstance(
        schema_map["last_update"], TimestampType
    ), f"last_update should be TimestampType, got {schema_map['last_update']}"

    # DataFrame must be truly empty.
    assert empty_meta.count() == 0


def test_download_fallback_injected_fetcher(spark, tmp_path):
    # build a real source parquet, then serve its bytes through a fake _get_fn
    src = str(tmp_path / "asset.parquet")

    spark.createDataFrame([Row(id=1), Row(id=2)]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(src)
    # the "asset" is a single part file inside src
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    def fake_get(href, timeout=None, stream=False):
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def iter_content(self, n):
                with open(src_file, "rb") as fh:
                    while True:
                        chunk = fh.read(n)
                        if not chunk:
                            break
                        yield chunk

        return _Resp()

    client = OvertureClient(
        release="2024-07-01", _catalog_opener=open_fake_overture, _get_fn=fake_get
    )
    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    out_dir = str(tmp_path / "out_fb")
    assets = spark.createDataFrame(
        [
            (
                "places",
                "place",
                "http://fake/place.parquet",
                [0.0, 0.0, 1.0, 1.0],
                "2024-07-01",
            )
        ],
        schema,
    )
    meta = client._download_fallback(
        assets, out_dir, validate=True, max_tries=2, partitions=4
    )
    assert meta.rdd.getNumPartitions() > 1
    row = meta.collect()[0]
    assert row["is_out_file_valid"] is True
    assert os.path.exists(row["source"])
    assert row["source"] == row["path"]
    assert spark.read.parquet(row["source"]).count() == 2


def test_download_routes_cloud_to_distributed(spark, tmp_path):
    src = str(tmp_path / "cloudish.parquet")

    # The href is an absolute path (starts with "/"), so download()'s cloud
    # predicate treats it as a FUSE-mounted Volume path and routes to
    # _download_distributed (not the HTTP fallback).
    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            )
        ],
        schema,
    )
    out_dir = str(tmp_path / "dl")
    meta = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta.columns == _META_COLS
    first = meta.collect()
    assert len(first) == 1 and first[0]["is_out_file_valid"] is True
    # idempotent re-run: same target, still valid, no error
    meta2 = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta2.collect()[0]["is_out_file_valid"] is True


def test_download_table_merge_idempotent(spark, tmp_path):
    pytest.importorskip("delta")
    # a SparkSession with Delta configured is required; skip if not available
    try:
        spark.sql("SELECT 1")  # sanity
        spark.range(1).write.format("delta").mode("overwrite").save(
            str(tmp_path / "_delta_probe")
        )
    except Exception:
        pytest.skip("Delta not enabled on this local SparkSession")

    src = str(tmp_path / "m.parquet")

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            )
        ],
        schema,
    )
    table = "overture_meta_test"
    out_dir = str(tmp_path / "dl2")
    client.download(
        assets,
        out_dir,
        bbox=(-122.45, 37.74, -122.40, 37.78),
        table=table,
        partitions=2,
    )
    client.download(
        assets,
        out_dir,
        bbox=(-122.45, 37.74, -122.40, 37.78),
        table=table,
        partitions=2,
    )
    # MERGE keyed by (theme, type, source) -> still exactly one row, not two
    assert spark.table(table).count() == 1
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read_from_volume_dir(spark, tmp_path):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    out_dir = str(tmp_path / "rd")
    target = os.path.join(out_dir, "buildings", "building")

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(target)
    df = client.read(out_dir)
    assert df.count() == 1
    # bbox filter retains the in-AOI row
    df2 = client.read(out_dir, bbox=(-122.45, 37.74, -122.40, 37.78))
    assert df2.count() == 1
    df3 = client.read(out_dir, bbox=(0, 0, 1, 1))  # disjoint
    assert df3.count() == 0


def test_read_from_metadata_dataframe(spark, tmp_path):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    target = str(tmp_path / "assetdir")

    spark.createDataFrame([Row(id=7)]).write.mode("overwrite").parquet(target)
    meta = spark.createDataFrame([(target,)], ["source"]).withColumn(
        "path", F.col("source")
    )
    df = client.read(meta)
    assert df.collect()[0]["id"] == 7


def test_download_routes_http_to_fallback(spark, tmp_path):
    """download() with an http:// href routes to _download_fallback (not distributed).

    The cloud predicate requires all hrefs to start with a cloud scheme or "/";
    an http:// URL matches neither, so the fallback path is taken. This test
    proves the routing — _download_fallback is not called directly.
    """
    # Build a real parquet file to serve via the injected fake _get_fn.
    src = str(tmp_path / "http_asset.parquet")
    spark.createDataFrame([Row(id=1), Row(id=2)]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(src)
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    def fake_get(href, timeout=None, stream=False):
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def iter_content(self, n):
                with open(src_file, "rb") as fh:
                    while True:
                        chunk = fh.read(n)
                        if not chunk:
                            break
                        yield chunk

        return _Resp()

    client = OvertureClient(
        release="2024-07-01", _catalog_opener=open_fake_overture, _get_fn=fake_get
    )
    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    # http:// href — fails the cloud predicate → _download_fallback is called
    assets = spark.createDataFrame(
        [
            (
                "places",
                "place",
                "http://fake-overture.example.com/place.parquet",
                [0.0, 0.0, 1.0, 1.0],
                "2024-07-01",
            )
        ],
        schema,
    )
    out_dir = str(tmp_path / "out_http")
    meta = client.download(assets, out_dir, partitions=4)
    # Result must carry the full _META_COLS schema in order.
    assert meta.columns == _META_COLS
    row = meta.collect()[0]
    assert row["is_out_file_valid"] is True
    assert os.path.exists(row["source"])
    assert row["source"] == row["path"]
    assert spark.read.parquet(row["source"]).count() == 2


def test_read_empty_metadata_raises(spark, tmp_path):
    """read() with an empty metadata DataFrame raises a clear ValueError.

    _read_paths([]) would IndexError without the guard; a ValueError with an
    actionable message is required (offline, no Delta).
    """
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    empty_meta = spark.createDataFrame(
        [], StructType([StructField("source", StringType())])
    )
    with pytest.raises(ValueError, match="no asset paths found"):
        client.read(empty_meta)


def test_read_from_table_name(spark, tmp_path):
    """read() with a Delta table name reads the underlying asset parquet rows.

    Gated on delta-spark being importable (skips cleanly offline).
    """
    pytest.importorskip("delta")
    # Verify Delta write actually works on this local SparkSession; skip if not.
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(
            str(tmp_path / "_delta_probe")
        )
    except Exception:
        pytest.skip("Delta not enabled on this local SparkSession")

    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    # Write a tiny parquet asset that client.read() will load.
    asset_dir = str(tmp_path / "tbl_asset")
    spark.createDataFrame([Row(id=42)]).write.mode("overwrite").parquet(asset_dir)

    # Build a minimal metadata DataFrame and persist it as a Delta table.
    table_name = "overture_read_test_tbl"
    meta_df = spark.createDataFrame([(asset_dir, asset_dir)], ["source", "path"])
    meta_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    try:
        df = client.read(table_name)
        rows = df.collect()
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]["id"] == 42
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_download_overture_aoi_one_shot(spark, tmp_path, monkeypatch):
    # Force the convenience fn's default client to use the fake opener (offline).
    import databricks.labs.gbx.sample.overture as ov

    src = str(tmp_path / "aoi.parquet")

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)

    orig_init = ov.OvertureClient.__init__

    def patched_init(self, *a, **k):
        k["_catalog_opener"] = open_fake_overture
        orig_init(self, *a, **k)

    monkeypatch.setattr(ov.OvertureClient, "__init__", patched_init)

    # The fake catalog's SF building href points at s3://...; rewrite discover to our local src
    orig_discover = ov.OvertureClient.discover

    def patched_discover(self, bbox, themes=None, release=None):
        df = orig_discover(self, bbox, themes=themes, release=release)

        return df.withColumn("href", F.lit(src))

    monkeypatch.setattr(ov.OvertureClient, "discover", patched_discover)

    out_dir = str(tmp_path / "oneshot")
    meta = ov.download_overture_aoi(
        (-122.45, 37.74, -122.40, 37.78),
        out_dir,
        themes=["buildings"],
        release="2024-07-01",
    )
    rows = meta.collect()
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["is_out_file_valid"] is True
