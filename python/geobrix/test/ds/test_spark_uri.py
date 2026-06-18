"""Unit tests for to_spark_uri() — scheme-qualify a listed path to the Hadoop convention.

Spark-free. Mirrors what Hadoop's FileSystem.listFiles ultimately produces on
Databricks, so a light-reader ``source`` column joins cleanly against the
binaryFile / heavy ``gdal`` ``path`` column (both emit ``dbfs:/Volumes/...``).
"""

import pytest

from databricks.labs.gbx.ds._listing import to_local_path, to_spark_uri


@pytest.mark.parametrize(
    "raw,expected",
    [
        # UC Volumes (the xView case) -> dbfs:/Volumes/...
        ("/Volumes/a/b.tif", "dbfs:/Volumes/a/b.tif"),
        ("/Volumes/xview/d/e/10.tif", "dbfs:/Volumes/xview/d/e/10.tif"),
        # DBFS FUSE -> dbfs:/
        ("/dbfs/x", "dbfs:/x"),
        ("/dbfs/tmp/y.tif", "dbfs:/tmp/y.tif"),
        # already-qualified dbfs:/ — idempotent
        ("dbfs:/Volumes/x", "dbfs:/Volumes/x"),
        ("dbfs:/x/y.tif", "dbfs:/x/y.tif"),
        # file:/ — unchanged (local filesystem)
        ("file:/tmp/x", "file:/tmp/x"),
        # other object-store schemes — unchanged
        ("s3://b/k", "s3://b/k"),
        (
            "abfss://c@acct.dfs.core.windows.net/k",
            "abfss://c@acct.dfs.core.windows.net/k",
        ),
        ("gs://b/k", "gs://b/k"),
        (
            "wasbs://c@acct.blob.core.windows.net/k",
            "wasbs://c@acct.blob.core.windows.net/k",
        ),
        ("https://host/k", "https://host/k"),
        ("http://host/k", "http://host/k"),
        # bare local absolute path (local dev/test) — unchanged, NOT mangled
        ("/tmp/x", "/tmp/x"),
        ("/Users/me/data/r.tif", "/Users/me/data/r.tif"),
        # relative path (no leading slash) — unchanged
        ("a/b.tif", "a/b.tif"),
        ("rel.tif", "rel.tif"),
    ],
)
def test_to_spark_uri_mapping(raw, expected):
    assert to_spark_uri(raw) == expected


def test_to_spark_uri_idempotent_on_qualified():
    once = to_spark_uri("/Volumes/a/b.tif")
    assert to_spark_uri(once) == once


@pytest.mark.parametrize(
    "raw,expected",
    [
        # dbfs:/Volumes -> bare FUSE /Volumes (the xView open case)
        ("dbfs:/Volumes/a/b.tif", "/Volumes/a/b.tif"),
        ("dbfs:/Volumes/xview/d/e/10.tif", "/Volumes/xview/d/e/10.tif"),
        # dbfs:/foo -> /foo (DBFS FUSE)
        ("dbfs:/foo", "/foo"),
        ("dbfs:/tmp/y.tif", "/tmp/y.tif"),
        # file:/ -> bare local
        ("file:/tmp/x", "/tmp/x"),
        ("file:/Users/me/r.tif", "/Users/me/r.tif"),
        # object-store / remote schemes — UNCHANGED (GDAL/rasterio read natively)
        ("s3://b/k", "s3://b/k"),
        ("s3a://b/k", "s3a://b/k"),
        (
            "abfss://c@acct.dfs.core.windows.net/k",
            "abfss://c@acct.dfs.core.windows.net/k",
        ),
        ("gs://b/k", "gs://b/k"),
        (
            "wasbs://c@acct.blob.core.windows.net/k",
            "wasbs://c@acct.blob.core.windows.net/k",
        ),
        ("https://host/k", "https://host/k"),
        ("http://host/k", "http://host/k"),
        # GDAL virtual filesystem — UNCHANGED (incl. internal /vsimem/ temp paths)
        ("/vsizip/x.zip/inner.shp", "/vsizip/x.zip/inner.shp"),
        ("/vsimem/temp_raster_abc.tif", "/vsimem/temp_raster_abc.tif"),
        # bare local absolute + relative — UNCHANGED
        ("/Volumes/a/b.tif", "/Volumes/a/b.tif"),
        ("/tmp/x", "/tmp/x"),
        ("a/b.tif", "a/b.tif"),
    ],
)
def test_to_local_path_mapping(raw, expected):
    assert to_local_path(raw) == expected


def test_to_local_path_idempotent():
    once = to_local_path("dbfs:/Volumes/a/b.tif")
    assert to_local_path(once) == once  # "/Volumes/a/b.tif" stays put


@pytest.mark.parametrize("p", ["/Volumes/x/y.tif", "/Volumes/main/geobrix/a/b.tif"])
def test_round_trip_local_to_spark_to_local(p):
    """to_local_path is the exact inverse of to_spark_uri for the /Volumes case
    (the operational one: a Volume listed bare -> stored dbfs: in a column ->
    stripped back to the bare FUSE path for the native open)."""
    assert to_local_path(to_spark_uri(p)) == p


def test_dbfs_qualified_column_strips_to_fuse():
    """The column-stored form (what to_spark_uri emits for a /dbfs FUSE path)
    strips back to a bare absolute FUSE path. Note /dbfs/x and dbfs:/x are the
    SAME DBFS location on Databricks (dbfs:/x is FUSE-mounted at /dbfs/x), so the
    bare-input round trip is exact only for /Volumes; what matters operationally
    is that a stored dbfs: column opens via a bare path."""
    assert to_local_path(to_spark_uri("/dbfs/data/r.tif")) == "/data/r.tif"
    assert to_local_path("dbfs:/dbfs/data/r.tif") == "/dbfs/data/r.tif"
