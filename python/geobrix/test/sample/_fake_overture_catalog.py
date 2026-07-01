"""Top-level importable fake Overture static STAC catalog for offline tests.

Mirrors the nested pystac.Catalog structure that the fixed traverse_catalog walks:
root → release child (extra_fields={"latest": True}) → theme Catalog →
type Catalog → links with rel="item" (each with get_absolute_href()).

Importable (not a closure) so it can be injected as _catalog_opener and, if ever
used on a worker, resolved by cloudpickle via the module name.
"""

from __future__ import annotations


class _Link:
    def __init__(self, href, rel="item"):
        self.rel = rel
        self._href = href

    def get_absolute_href(self):
        return self._href


class _TypeCatalog:
    def __init__(self, links):
        self._links = links  # list of _Link with rel="item"

    @property
    def links(self):
        return self._links


class _ThemeCatalog:
    def __init__(self, type_catalogs):
        self._children = type_catalogs  # dict: type_id -> _TypeCatalog

    def get_child(self, type_id):
        return self._children.get(type_id)


class _ReleaseCatalog:
    def __init__(self, id_, theme_catalogs):
        self.id = id_
        self.extra_fields = {"latest": True}
        self._children = theme_catalogs  # dict: theme_id -> _ThemeCatalog

    def get_child(self, theme_id):
        return self._children.get(theme_id)


class FakeOvertureCatalog:
    """Two themes: SF buildings (intersects SF) + EU places (disjoint from SF)."""

    def get_children(self):
        building_type_cat = _TypeCatalog(
            [
                _Link("fake://sf-building-item.json"),
            ]
        )
        buildings_theme_cat = _ThemeCatalog({"building": building_type_cat})

        place_type_cat = _TypeCatalog(
            [
                _Link("fake://eu-place-item.json"),
            ]
        )
        places_theme_cat = _ThemeCatalog({"place": place_type_cat})

        release = _ReleaseCatalog(
            "2026-06-17.0",
            {
                "buildings": buildings_theme_cat,
                "places": places_theme_cat,
            },
        )
        return [release]


def open_fake_overture():
    return FakeOvertureCatalog()
