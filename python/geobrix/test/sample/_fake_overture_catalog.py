"""Top-level importable fake Overture static STAC catalog for offline tests.

Mirrors the pystac.Catalog surface that traverse_catalog walks: a root with
get_children() -> collections, each with get_items() -> items, each item with
.bbox, .properties (theme/type), and .assets (name -> obj with .href).
Importable (not a closure) so it can be injected as _catalog_opener and, if ever
used on a worker, resolved by cloudpickle via the module name.
"""

from __future__ import annotations


class _Asset:
    def __init__(self, href):
        self.href = href


class _Item:
    def __init__(self, bbox, theme, type_, href):
        self.bbox = bbox
        self.properties = {"theme": theme, "type": type_}
        self.assets = {"data": _Asset(href)}


class _Collection:
    def __init__(self, items):
        self._items = items

    def get_items(self):
        return list(self._items)


class FakeOvertureCatalog:
    """Two collections: SF buildings (intersects) + a faraway places item (disjoint)."""

    def get_children(self):
        sf = _Collection(
            [
                _Item(
                    [-122.52, 37.70, -122.36, 37.83],
                    "buildings",
                    "building",
                    "s3://overturemaps-us-west-2/release/buildings/building/sf.parquet",
                )
            ]
        )
        faraway = _Collection(
            [
                _Item(
                    [10.0, 50.0, 11.0, 51.0],
                    "places",
                    "place",
                    "s3://overturemaps-us-west-2/release/places/place/eu.parquet",
                )
            ]
        )
        return [sf, faraway]


def open_fake_overture():
    return FakeOvertureCatalog()
