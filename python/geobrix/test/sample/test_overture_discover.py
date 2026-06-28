import pytest

from databricks.labs.gbx.sample._overture_discover import (
    bbox_intersects,
    normalize_bbox,
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


from databricks.labs.gbx.sample._overture_discover import (
    OVERTURE_THEMES,
    expand_themes,
)


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


from databricks.labs.gbx.sample._overture_discover import traverse_catalog
from test.sample._fake_overture_catalog import open_fake_overture


def test_traverse_catalog_bbox_filters_disjoint():
    sf_bbox = (-122.45, 37.74, -122.40, 37.78)
    rows = traverse_catalog(open_fake_overture, sf_bbox, [("buildings", "building")])
    assert len(rows) == 1
    r = rows[0]
    assert r["theme"] == "buildings"
    assert r["type"] == "building"
    assert r["href"].endswith("sf.parquet")
    assert r["asset_bbox"] == [-122.52, 37.70, -122.36, 37.83]


def test_traverse_catalog_skips_unrequested_pairs():
    # AOI covers the whole world, but we only ask for places -> the SF building drops out
    rows = traverse_catalog(open_fake_overture, (-180, -90, 180, 90), [("places", "place")])
    assert [r["type"] for r in rows] == ["place"]
