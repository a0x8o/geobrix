import json
import pytest
from pyspark.sql import SparkSession
from databricks.labs.gbx.stac import StacClient

_ITEM = {
    "id": "S2_X", "collection": "sentinel-2-l2a", "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z"},
    "assets": {"B02": {"href": "http://x/B02.tif"}, "B03": {"href": "http://x/B03.tif"}},
}


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("stac-test").getOrCreate()
    yield s
    s.stop()


class _FakeItem:
    def __init__(self, d): self._d = d
    def to_dict(self): return self._d


class _FakeSearch:
    def item_collection(self): return [_FakeItem(_ITEM)]


class _FakeCatalog:
    def search(self, collections, intersects, datetime): return _FakeSearch()


def test_search_explodes_items_to_asset_rows(spark):
    client = StacClient(_catalog_opener=lambda: _FakeCatalog())
    df = spark.createDataFrame([("cellA", '{"type":"Point","coordinates":[1,2]}')], ["cellid", "geojson"])
    out = client.search(df, geojson_col="geojson", collections=["sentinel-2-l2a"], datetime="2022-06-01", partitions=2)
    rows = {(r["cellid"], r["item_id"], r["asset_name"]) for r in out.collect()}
    assert rows == {("cellA", "S2_X", "B02"), ("cellA", "S2_X", "B03")}
    one = out.filter("asset_name = 'B02'").first()
    assert one["date"] == "2022-06-01" and one["href"] == "http://x/B02.tif"


@pytest.mark.integration
def test_pc_search_and_download_one_asset(spark, tmp_path):
    client = StacClient()  # real Planetary Computer + sign_inplace
    df = spark.createDataFrame(
        [('{"type":"Point","coordinates":[-131.6,55.3]}',)], ["geojson"]
    )
    assets = client.search(df, geojson_col="geojson", collections=["sentinel-2-l2a"],
                           datetime="2022-06-01/2022-06-05", partitions=1)
    assets = assets.filter("asset_name = 'B02'").limit(1)
    assert assets.count() == 1
    files = client.download(assets, str(tmp_path), asset_names=["B02"], max_tries=3)
    row = files.first()
    assert row["is_out_file_valid"] is True
