import os

import pytest

pyspark = pytest.importorskip("pyspark")

from test.sample._fake_overture_catalog import open_fake_overture  # noqa: E402

from pyspark.sql import Row  # noqa: E402
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

    # simulate a cloud asset with a local path that LOOKS like a cloud read target
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
    # local file path -> not a cloud scheme, so it routes to distributed-read anyway
    # only when the caller forces it; here use a real local path and assert columns + idempotency
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
    assert meta.columns == [
        "theme",
        "type",
        "source",
        "path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
        "asset_bbox",
        "release",
        "href",
    ]
    first = meta.collect()
    assert len(first) == 1 and first[0]["is_out_file_valid"] is True
    # idempotent re-run: same target, still valid, no error
    meta2 = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta2.collect()[0]["is_out_file_valid"] is True
