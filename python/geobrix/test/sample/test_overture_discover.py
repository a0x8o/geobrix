from test.sample._fake_overture_catalog import open_fake_overture

import pytest

from databricks.labs.gbx.sample._overture_discover import (
    OVERTURE_THEMES,
    bbox_intersects,
    expand_themes,
    normalize_bbox,
    resolve_release,
    traverse_catalog,
)


def test_bbox_intersects_overlap():
    assert bbox_intersects((0, 0, 10, 10), (5, 5, 15, 15)) is True


def test_bbox_intersects_touching_edge():
    # touching edges count as intersecting (inclusive)
    assert bbox_intersects((0, 0, 10, 10), (10, 0, 20, 10)) is True


def test_bbox_intersects_disjoint():
    assert bbox_intersects((0, 0, 1, 1), (5, 5, 6, 6)) is False


def test_normalize_bbox_returns_floats():
    assert normalize_bbox([1, 2, 3, 4]) == (1.0, 2.0, 3.0, 4.0)


def test_normalize_bbox_rejects_inverted():
    with pytest.raises(ValueError):
        normalize_bbox((10, 0, 0, 10))


def test_overture_themes_complete():
    assert set(OVERTURE_THEMES) == {
        "addresses",
        "base",
        "buildings",
        "divisions",
        "places",
        "transportation",
    }
    assert OVERTURE_THEMES["buildings"] == ["building", "building_part"]


def test_expand_themes_none_is_all_pairs():
    pairs = expand_themes(None)
    assert ("buildings", "building") in pairs
    assert ("transportation", "segment") in pairs
    # one pair per (theme, type)
    assert len(pairs) == sum(len(v) for v in OVERTURE_THEMES.values())


def test_expand_themes_subset():
    assert expand_themes(["places"]) == [("places", "place")]


def test_expand_themes_unknown_raises():
    with pytest.raises(ValueError):
        expand_themes(["weather"])


def _sf_building_loader(href):
    if "sf-building" in href:

        class _A:
            href = "s3://overturemaps-us-west-2/release/buildings/building/sf.parquet"

        class _FakeItem:
            bbox = [-122.52, 37.70, -122.36, 37.83]
            assets = {"data": _A()}

        return _FakeItem()
    raise FileNotFoundError(href)


def _eu_place_loader(href):
    if "eu-place" in href:

        class _A:
            href = "s3://overturemaps-us-west-2/release/places/place/eu.parquet"

        class _FakeItem:
            bbox = [10.0, 50.0, 11.0, 51.0]
            assets = {"data": _A()}

        return _FakeItem()
    raise FileNotFoundError(href)


def _both_loader(href):
    if "sf-building" in href:
        return _sf_building_loader(href)
    if "eu-place" in href:
        return _eu_place_loader(href)
    raise FileNotFoundError(href)


def test_traverse_catalog_bbox_filters_disjoint():
    sf_bbox = (-122.45, 37.74, -122.40, 37.78)
    rows = traverse_catalog(
        open_fake_overture,
        sf_bbox,
        [("buildings", "building")],
        _item_loader=_sf_building_loader,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["theme"] == "buildings"
    assert r["type"] == "building"
    assert r["href"].endswith("sf.parquet")
    assert r["asset_bbox"] == [-122.52, 37.70, -122.36, 37.83]


def test_traverse_catalog_skips_unrequested_pairs():
    # AOI covers the whole world, but we only ask for places -> the SF building drops out
    rows = traverse_catalog(
        open_fake_overture,
        (-180, -90, 180, 90),
        [("places", "place")],
        _item_loader=_eu_place_loader,
    )
    assert [r["type"] for r in rows] == ["place"]


class _RelChild:
    """Fake release child with latest=True."""

    id = "2024-07-01"
    extra_fields = {"latest": True}


class _RelCatalog:
    def get_children(self):
        return [_RelChild()]


class _EmptyCatalog:
    def get_children(self):
        return []


def test_resolve_release_explicit_passthrough():
    assert resolve_release(lambda: _RelCatalog(), "2023-12-12") == "2023-12-12"


def test_resolve_release_latest():
    assert resolve_release(lambda: _RelCatalog(), None) == "2024-07-01"


def test_resolve_release_missing_raises():
    with pytest.raises(ValueError):
        resolve_release(lambda: _EmptyCatalog(), None)


def test_traverse_catalog_is_the_discovery_path():
    """discover() now calls traverse_catalog directly (cli_discover was removed).

    Verify traverse_catalog returns the correct shape and filters by bbox — the
    same contract OvertureClient.discover() relies on.
    """
    rows = traverse_catalog(
        open_fake_overture,
        (-122.5, 37.7, -122.3, 37.8),
        [("buildings", "building")],
        _item_loader=_sf_building_loader,
    )
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["type"] == "building"
    assert rows[0]["href"].endswith("sf.parquet")
    assert "asset_bbox" in rows[0]
