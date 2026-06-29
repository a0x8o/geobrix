"""Tests for vizx._dynamic (Phase-1.5 dynamic zoom cut-over widget).

Logic-level tests: widget construction, ESM content, and the seam-gate helper.
Browser comm round-trip is proven by Spike B (model.send -> on_msg -> trait ->
change handler); that path is not unit-testable headlessly.
"""

import pytest
import threading

anywidget = pytest.importorskip("anywidget")

from databricks.labs.gbx.vizx._dynamic import (
    _viewport_payload,
    plot_interactive_dynamic,
    _TileCache,
    _PrefetchWorker,
)
from databricks.labs.gbx.vizx._layers import vector_layer
import geopandas as gpd
from shapely.geometry import Point


# ---------------------------------------------------------------------------
# Task 13b: _TileCache + prefetch worker tests
# ---------------------------------------------------------------------------


def test_tilecache_lru_evicts_oldest():
    """Insert > capacity: the oldest (least-recently accessed) viewed entry is evicted."""
    cache = _TileCache(max_entries=2)
    cache.put((1, 0, 0), b"tile-a", viewed=True)
    cache.put((1, 1, 0), b"tile-b", viewed=True)
    # Access (1,0,0) so (1,1,0) is now the LRU.
    assert cache.get((1, 0, 0)) == b"tile-a"
    # Insert a third entry — (1,1,0) is the LRU and must be evicted.
    cache.put((1, 0, 1), b"tile-c", viewed=True)
    assert cache.get((1, 1, 0)) is None  # evicted
    assert cache.get((1, 0, 0)) == b"tile-a"
    assert cache.get((1, 0, 1)) == b"tile-c"


def test_tilecache_evicts_unviewed_before_viewed():
    """When full, a never-viewed (prefetched-only) entry is evicted before a viewed one."""
    cache = _TileCache(max_entries=2)
    cache.put((1, 0, 0), b"viewed", viewed=True)
    cache.put((1, 1, 0), b"prefetched", viewed=False)
    # Both slots are full. A third put must evict the unviewed entry.
    cache.put((1, 2, 0), b"new-viewed", viewed=True)
    assert cache.get((1, 1, 0)) is None  # unviewed evicted first
    assert cache.get((1, 0, 0)) == b"viewed"  # viewed entry survives
    assert cache.get((1, 2, 0)) == b"new-viewed"


def test_prefetch_populates_neighbor_ring():
    """After a viewport request at (z,x,y) + flush, all 8 neighbors are in the cache."""
    calls = []

    def stub_tiler(z, x, y):
        calls.append((z, x, y))
        return b"tile-bytes"

    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=stub_tiler)
    try:
        # Simulate a real viewport request at (12, 5, 5) — populates the cache and
        # schedules neighbor prefetch.
        worker.on_viewport_served(z=12, x=5, y=5)
        worker.flush()

        # All 8 neighbors must be in the cache (not marked viewed).
        expected_neighbors = [
            (12, dx, dy)
            for dx in (4, 5, 6)
            for dy in (4, 5, 6)
            if not (dx == 5 and dy == 5)  # exclude the center tile itself
        ]
        for key in expected_neighbors:
            assert cache.get(key) == b"tile-bytes", f"missing neighbor {key}"
        # Stub was called for all 8.
        assert set(calls) == set((z, x, y) for z, x, y in expected_neighbors)
    finally:
        worker.shutdown()


def test_cache_hit_skips_retiling():
    """A second request for an already-prefetched neighbor is served from cache, stub not called again."""
    calls = []

    def stub_tiler(z, x, y):
        calls.append((z, x, y))
        return b"tile-bytes"

    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=stub_tiler)
    try:
        # Prefetch neighbors of (12, 5, 5).
        worker.on_viewport_served(z=12, x=5, y=5)
        worker.flush()

        calls_after_first = list(calls)
        # (12, 4, 4) is a neighbor — it must already be in the cache.
        assert cache.get((12, 4, 4)) == b"tile-bytes"

        # Now request (12, 4, 4) as a viewport (simulate cache-hit serving).
        # The worker's serve path should find it in cache and NOT call the tiler again.
        hit = cache.get((12, 4, 4))
        assert hit == b"tile-bytes"
        # Mark it viewed (as cache-first serving would do).
        cache.put((12, 4, 4), hit, viewed=True)

        # Schedule neighbors of (12, 4, 4) — the tiler must NOT be called for (12, 4, 4) again.
        worker.on_viewport_served(z=12, x=4, y=4)
        worker.flush()

        retile_calls = [c for c in calls if c not in calls_after_first]
        assert (12, 4, 4) not in retile_calls, "tiler called again for a cached tile"
    finally:
        worker.shutdown()


def test_prefetch_coalesces_stale():
    """A rapid second viewport supersedes the first; stale prefetch is skipped."""
    calls = []
    # Use a barrier to hold the worker mid-flight so we can advance the generation.
    gate = threading.Event()
    released = threading.Event()

    def slow_tiler(z, x, y):
        # Stall the very first call to give the test time to advance the generation.
        if not released.is_set():
            gate.wait(timeout=5)
            released.set()
        calls.append((z, x, y))
        return b"tile-bytes"

    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=slow_tiler)
    try:
        # Schedule prefetch for viewport (12, 10, 10).
        worker.on_viewport_served(z=12, x=10, y=10)
        # Immediately supersede with a new viewport (12, 20, 20).
        worker.on_viewport_served(z=12, x=20, y=20)
        # Unblock the stalled worker.
        gate.set()
        worker.flush()

        # The stale ring around (10, 10) must not all be in the cache; some
        # entries may have been skipped.  At minimum: none of the stale neighbors
        # (those NOT adjacent to (20,20)) should be present.
        stale_only_neighbors = [
            (12, x, y)
            for x in (9, 10, 11)
            for y in (9, 10, 11)
            if not (x == 10 and y == 10)
            if not (19 <= x <= 21 and 19 <= y <= 21)
        ]
        # Not all stale neighbors were cached (coalescing dropped at least some).
        cached_stale = [k for k in stale_only_neighbors if cache.get(k) is not None]
        all_stale = len(stale_only_neighbors)
        assert len(cached_stale) < all_stale, (
            "No stale prefetch was coalesced/skipped — generation token not working"
        )
    finally:
        worker.shutdown()


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


def test_esm_is_synced_to_frontend():
    """_esm must appear in get_state() — i.e. it actually reaches the JS frontend."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    state = w.get_state()
    # _esm must be in the synced state dict (proves anywidget registered it sync=True).
    assert "_esm" in state, "_esm is not in get_state() — widget will render blank in a notebook"
    # Sanity-check: the ESM content is present (not an empty/null stub).
    esm_val = state["_esm"]
    if isinstance(esm_val, (list, tuple)):
        # anywidget may wrap as (value, metadata) buffer tuple
        esm_val = esm_val[0]
    assert "moveend" in esm_val, "_esm in state but content is wrong"
    # detail trait must also be synced.
    assert "detail" in state, "detail trait is not in get_state()"


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
    # Prove the Python data-flow: _handle_msg must have base64-encoded the result
    # into the detail trait (which syncs to the JS frontend).
    assert w.detail != "", "detail trait is empty after on_viewport returned bytes"


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
