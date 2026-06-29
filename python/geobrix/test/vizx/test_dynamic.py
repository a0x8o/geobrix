"""Tests for vizx._dynamic (Phase-1.5 dynamic zoom cut-over widget).

Logic-level tests: widget construction, ESM content, and the seam-gate helper.
Browser comm round-trip is proven by Spike B (model.send -> on_msg -> trait ->
change handler); that path is not unit-testable headlessly.
"""

import pytest

anywidget = pytest.importorskip("anywidget")

from databricks.labs.gbx.vizx._dynamic import _viewport_payload, plot_interactive_dynamic
from databricks.labs.gbx.vizx._layers import vector_layer
import geopandas as gpd
from shapely.geometry import Point


def test_builds_widget_with_overview_and_esm():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    assert isinstance(w, anywidget.AnyWidget)
    assert "moveend" in w._esm and "model.send" in w._esm


def test_viewport_payload_only_fires_above_seam():
    assert _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=12, max_z=10) is not None
    assert _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=9, max_z=10) is None


def test_viewport_payload_at_seam_boundary():
    # zoom == max_z is NOT above the seam — must return None.
    assert _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=10, max_z=10) is None
    # zoom == max_z + 1 is above the seam — must return a payload.
    assert _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=11, max_z=10) is not None


def test_viewport_payload_structure():
    result = _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=12, max_z=10)
    assert result is not None
    assert result["bbox"] == [-122.5, 37.7, -122.4, 37.8]
    assert result["zoom"] == 12


def test_widget_has_detail_trait():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    # detail must be a synced traitlets.Unicode (checked via trait metadata)
    assert hasattr(w, "detail")
    # Synced traits have 'sync' in their metadata
    trait_meta = w.traits()["detail"].metadata
    assert trait_meta.get("sync") is True


def test_widget_esm_registers_change_detail_handler():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    # The ESM must listen on 'change:detail' to refresh the map source.
    assert "change:detail" in w._esm


def test_widget_esm_references_maplibre():
    """The ESM should use MapLibre (the SRI-pinned CDN URL from _maplibre.py)."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    assert "maplibre" in w._esm.lower() or "maplibregl" in w._esm.lower()


def test_custom_on_viewport_called():
    """A user-supplied on_viewport callback replaces the default tiler."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    calls = []

    def my_callback(bbox, zoom):
        calls.append((bbox, zoom))
        return b"FAKE_PMTILES_BYTES"

    w = plot_interactive_dynamic(
        [vector_layer(gdf)],
        simplify_tiles_spec={"max_z": 8},
        on_viewport=my_callback,
    )
    assert isinstance(w, anywidget.AnyWidget)

    # Simulate a JS message (the on_msg handler path, not the live comm).
    # Trigger the internal handler directly by calling send-equivalent internals.
    # We access the registered on_msg handler via a private test hook:
    w._gbx_handle_msg({"bbox": [-122.5, 37.7, -122.4, 37.8], "zoom": 12})
    assert len(calls) == 1
    assert calls[0] == ([-122.5, 37.7, -122.4, 37.8], 12)


def test_custom_on_viewport_below_seam_not_called():
    """on_viewport must NOT be called when zoom <= max_z (seam gate)."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    calls = []

    def my_callback(bbox, zoom):
        calls.append((bbox, zoom))
        return b"FAKE_PMTILES_BYTES"

    w = plot_interactive_dynamic(
        [vector_layer(gdf)],
        simplify_tiles_spec={"max_z": 8},
        on_viewport=my_callback,
    )
    w._gbx_handle_msg({"bbox": [-122.5, 37.7, -122.4, 37.8], "zoom": 7})
    assert calls == [], "on_viewport must not fire below the seam"
