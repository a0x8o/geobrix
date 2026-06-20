"""Module-level importable fake STAC catalog for UDF-path tests.

Must be a top-level importable module (not a test-local class/closure) so
cloudpickle can resolve it on Spark worker processes. The test/stac/ package
deliberately has an __init__.py so Python treats it as a package — the
cloudpickle serialization uses the fully qualified module name
``test.stac._fake_catalog``.

NOTE: if UDF pickling ever fails because of the package __init__.py, you would
need to remove __init__.py and convert this to a standalone script on sys.path —
but with cloudpickle >=2.x and a proper importable package this should not occur.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard fake STAC item (used by all UDF-path tests)
# ---------------------------------------------------------------------------
FAKE_ITEM_DICT = {
    "id": "S2_X",
    "collection": "sentinel-2-l2a",
    "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z", "eo:cloud_cover": 5},
    "assets": {
        "B02": {"href": "http://fake/B02.tif"},
        "B03": {"href": "http://fake/B03.tif"},
    },
}

# Item with a path-traversal item_id for I5 tests
TRAVERSAL_ITEM_DICT = {
    "id": "../evil/item",
    "collection": "sentinel-2-l2a",
    "bbox": [0.0, 0.0, 1.0, 1.0],
    "properties": {"datetime": "2022-06-02T00:00:00Z"},
    "assets": {"B02": {"href": "http://fake/evil.tif"}},
}


class _FakeItem:
    def __init__(self, d: dict):
        self._d = d

    def to_dict(self) -> dict:
        return self._d


class _FakeSearch:
    def __init__(self, items):
        self._items = items

    def item_collection(self):
        return [_FakeItem(d) for d in self._items]


class FakeCatalog:
    """One item returned per search — importable for cloudpickle."""

    def search(self, collections, intersects, datetime):
        return _FakeSearch([FAKE_ITEM_DICT])


class FakeCatalogMultiAOI:
    """Two identical items returned so dedup logic (distinct) is tested."""

    def search(self, collections, intersects, datetime):
        return _FakeSearch([FAKE_ITEM_DICT, FAKE_ITEM_DICT])


class FakeEmptyCatalog:
    """Returns no items — empty result path."""

    def search(self, collections, intersects, datetime):
        return _FakeSearch([])


def open_fake(url, modifier=None):
    """Drop-in replacement for pystac_client.Client.open that returns FakeCatalog."""
    return FakeCatalog()


def open_fake_multi(url, modifier=None):
    """Drop-in for pystac_client.Client.open returning FakeCatalogMultiAOI."""
    return FakeCatalogMultiAOI()
