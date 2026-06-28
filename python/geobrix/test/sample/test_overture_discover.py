from test.sample._fake_overture_catalog import open_fake_overture

import pytest

from databricks.labs.gbx.sample._overture_discover import (
    OVERTURE_THEMES,
    bbox_intersects,
    cli_discover,
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
    rows = traverse_catalog(
        open_fake_overture, (-180, -90, 180, 90), [("places", "place")]
    )
    assert [r["type"] for r in rows] == ["place"]


class _RelCatalog:
    extra_fields = {"overture:releases": ["2024-01-01", "2024-07-01"]}


class _NoRelCatalog:
    extra_fields = {}
    id = None


def test_resolve_release_explicit_passthrough():
    assert resolve_release(lambda: _RelCatalog(), "2023-12-12") == "2023-12-12"


def test_resolve_release_latest():
    assert resolve_release(lambda: _RelCatalog(), None) == "2024-07-01"


def test_resolve_release_missing_raises():
    with pytest.raises(ValueError):
        resolve_release(lambda: _NoRelCatalog(), None)


def test_cli_discover_absent_returns_none(monkeypatch):
    # No overturemaps on PATH -> None so the caller falls back to traversal.
    monkeypatch.setattr(
        "databricks.labs.gbx.sample._overture_discover.shutil.which",
        lambda name: None,
    )
    assert (
        cli_discover(
            (-122.5, 37.7, -122.3, 37.8), [("buildings", "building")], "2024-07-01"
        )
        is None
    )


def test_cli_discover_present_parses_runner(monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.gbx.sample._overture_discover.shutil.which",
        lambda name: "/usr/bin/overturemaps",
    )

    class _Completed:
        returncode = 0
        stdout = (
            "s3://overturemaps-us-west-2/2024-07-01/buildings/building/part-0.parquet\n"
        )

    rows = cli_discover(
        (-122.5, 37.7, -122.3, 37.8),
        [("buildings", "building")],
        "2024-07-01",
        runner=lambda *a, **k: _Completed(),
    )
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["type"] == "building"
    assert rows[0]["href"].endswith("part-0.parquet")
    assert rows[0]["asset_bbox"] == [-122.5, 37.7, -122.3, 37.8]
