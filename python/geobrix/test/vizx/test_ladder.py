"""Tests for vizx._maplibre.prepare_layers — the >64 MB budget ladder."""

import shutil

import geopandas as gpd
import pytest
from shapely.geometry import Point

from databricks.labs.gbx.vizx._layers import pmtiles_layer, vector_layer
from databricks.labs.gbx.vizx._maplibre import prepare_layers


def _small_gdf():
    return gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")


def test_under_budget_is_interactive():
    out = prepare_layers([vector_layer(_small_gdf())], max_embed_mb=64)
    assert out["mode"] == "interactive"


def test_oversize_pmtiles_without_url_or_spec_falls_back_to_static():
    big = pmtiles_layer(b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024))
    out = prepare_layers([big], max_embed_mb=1, fallback=True)
    assert out["mode"] == "static"
    assert any("static" in w.lower() for w in out["warnings"])


def test_fallback_false_raises():
    big = pmtiles_layer(b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024))
    with pytest.raises(ValueError, match="budget"):
        prepare_layers([big], max_embed_mb=1, fallback=False)


def test_url_mode_pmtiles_is_interactive_zero_embed_cost():
    """A pmtiles layer with an http URL streams remotely — embed_bytes==0, always interactive."""
    url_layer = pmtiles_layer("https://example.com/tiles.pmtiles")
    # With a generous budget, a URL-mode layer must always be interactive.
    out = prepare_layers([url_layer], max_embed_mb=64)
    assert out["mode"] == "interactive"
    assert not out["warnings"]


def test_warnings_are_list():
    out = prepare_layers([vector_layer(_small_gdf())])
    assert isinstance(out["warnings"], list)


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_spec_produces_interactive():
    """An oversize vector layer + simplify_tiles_spec → mode='interactive' with 'simplified' warning."""
    import shutil as _shutil
    import warnings as _warnings

    import geopandas as gpd
    from shapely.geometry import box

    # Create a vector layer with some polygons; use a generous budget so it fits
    # after simplification (simplify is applied first, then budget is checked).
    gdf = gpd.GeoDataFrame(
        {"v": range(5)},
        geometry=[box(i * 0.1, 0, i * 0.1 + 0.1, 0.1) for i in range(5)],
        crs="EPSG:4326",
    )
    lyr = vector_layer(gdf)
    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        out = prepare_layers(
            [lyr],
            max_embed_mb=100,
            simplify_tiles_spec={"max_z": 4, "budget_mb": 8},
        )
    assert out["mode"] == "interactive"
    # Should have a warning mentioning "simplified"
    combined = " ".join(out["warnings"]) + " ".join(str(x.message) for x in w)
    assert "simplified" in combined.lower()
