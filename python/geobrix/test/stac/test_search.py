import json
from databricks.labs.gbx.stac._search import parse_item, extract_assets, search_one

_ITEM = json.dumps({
    "id": "S2_X",
    "collection": "sentinel-2-l2a",
    "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z", "eo:cloud_cover": 5},
    "assets": {
        "B02": {"href": "http://x/B02.tif", "type": "image/tiff"},
        "B03": {"href": "http://x/B03.tif", "type": "image/tiff"},
    },
})


def test_parse_item_fields():
    p = parse_item(_ITEM)
    assert p["item_id"] == "S2_X"
    assert p["date"] == "2022-06-01"
    assert p["item_bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert p["item_properties"]["eo:cloud_cover"] == 5


def test_extract_assets():
    a = extract_assets(_ITEM)
    names = sorted(x["asset_name"] for x in a)
    assert names == ["B02", "B03"]
    b02 = next(x for x in a if x["asset_name"] == "B02")
    assert b02["href"] == "http://x/B02.tif"


def test_search_one_uses_catalog_and_retries(monkeypatch):
    calls = {"n": 0}

    class FakeItem:
        def __init__(self, d): self._d = d
        def to_dict(self): return self._d

    class FakeSearch:
        def item_collection(self): return [FakeItem(json.loads(_ITEM))]

    class FakeCatalog:
        def search(self, collections, intersects, datetime):
            calls["n"] += 1
            assert collections == ["sentinel-2-l2a"]
            return FakeSearch()

    out = search_one(FakeCatalog(), ["sentinel-2-l2a"], "2022-06-01", '{"type":"Point","coordinates":[1,2]}')
    assert calls["n"] == 1
    assert json.loads(out[0])["id"] == "S2_X"
