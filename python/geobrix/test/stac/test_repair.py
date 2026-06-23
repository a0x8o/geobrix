"""Unit tests for StacClient.repair().

repair() reads the invalid rows (where is_out_file_valid = false), re-downloads them
via self.download on the (item_id, asset_name, href) columns, and (for a table target)
MERGEs the updated rows back. These tests call repair() directly and stub self.download,
so no network/Delta is needed: they verify the filter-to-invalid + column contract and
the fail-loud guard when href is absent (e.g. a band table).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.labs.gbx.stac import StacClient


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("stac-repair-test")
        .getOrCreate()
    )
    yield s


def test_repair_filters_invalid_and_downloads_href_columns(spark, monkeypatch):
    client = StacClient()
    captured = {}

    def fake_download(input_df, out_dir, **kwargs):
        captured["cols"] = list(input_df.columns)
        captured["n"] = input_df.count()
        captured["out_dir"] = out_dir
        return (
            input_df.withColumn("out_file_path", F.lit("/repaired.tif"))
            .withColumn("out_file_sz", F.lit(123))
            .withColumn("is_out_file_valid", F.lit(True))
            .withColumn("last_update", F.current_timestamp())
        )

    monkeypatch.setattr(client, "download", fake_download)

    df = spark.createDataFrame(
        [
            ("i1", "B02", "http://h/1.tif", None, False),
            ("i2", "B02", "http://h/2.tif", "/ok.tif", True),
        ],
        ["item_id", "asset_name", "href", "out_file_path", "is_out_file_valid"],
    )

    out = client.repair(df, out_dir="/tmp/repairtest")

    # only the invalid row is re-downloaded, carrying item_id/asset_name/href
    assert {"item_id", "asset_name", "href"}.issubset(set(captured["cols"]))
    assert captured["n"] == 1
    assert captured["out_dir"] == "/tmp/repairtest"
    rows = out.collect()
    assert len(rows) == 1 and rows[0]["is_out_file_valid"] is True


def test_repair_without_href_raises_actionable_error(spark):
    """A band table (band_name, no asset_name/href) cannot be repaired — clear error."""
    client = StacClient()
    bandlike = spark.createDataFrame(
        [("i1", "B02", "/x.tif")],
        ["item_id", "band_name", "out_file_path"],
    ).withColumn("is_out_file_valid", F.lit(False))

    with pytest.raises(ValueError) as ei:
        client.repair(bandlike)
    msg = str(ei.value)
    assert "href" in msg and "asset_name" in msg
