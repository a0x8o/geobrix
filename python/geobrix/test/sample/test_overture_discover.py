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
