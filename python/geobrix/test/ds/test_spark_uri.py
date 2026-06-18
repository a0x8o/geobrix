"""Unit tests for to_spark_uri() — scheme-qualify a listed path to the Hadoop convention.

Spark-free. Mirrors what Hadoop's FileSystem.listFiles ultimately produces on
Databricks, so a light-reader ``source`` column joins cleanly against the
binaryFile / heavy ``gdal`` ``path`` column (both emit ``dbfs:/Volumes/...``).
"""

import pytest

from databricks.labs.gbx.ds._listing import to_spark_uri


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
