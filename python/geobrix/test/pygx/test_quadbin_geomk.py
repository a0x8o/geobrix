"""Unit tests for pygx quadbin geometry-aware kring/kloop.

Tests mirror the brief Step 1 fixture exactly.
"""

import pytest
from shapely import to_wkb
from shapely.geometry import box

from databricks.labs.gbx.pygx import _quadbin


def _wkb():
    return to_wkb(box(-73.99, 40.71, -73.95, 40.75))  # NYC lon/lat


def test_quadbin_geomkring_k0_is_covering_set():
    g, res = _wkb(), 12
    k0 = set(_quadbin.geometry_k_ring(g, res, 0))
    fill = set(_quadbin.polyfill(g, res))
    assert k0 == fill


def test_quadbin_geomkring_filled_superset_of_polyfill():
    g, res = _wkb(), 12
    assert set(_quadbin.polyfill(g, res)) <= set(_quadbin.geometry_k_ring(g, res, 1))


def test_quadbin_geomkloop_is_ring_diff():
    g, res = _wkb(), 12
    r2 = set(_quadbin.geometry_k_ring(g, res, 2))
    r1 = set(_quadbin.geometry_k_ring(g, res, 1))
    assert set(_quadbin.geometry_k_loop(g, res, 2)) == (r2 - r1)


def test_quadbin_geomkring_returns_bigint():
    assert all(isinstance(c, int) for c in _quadbin.geometry_k_ring(_wkb(), 12, 1))


def test_quadbin_geomkring_k0_equals_polyfill_boundary_modes():
    """k=0 == polyfill for boundary modes (no-hole polygon; hole modes start from h_border=empty)."""
    # Only boundary-* modes have k0 == p_cover for a simple no-hole polygon.
    boundary_modes = ("boundary-out", "boundary-in", "boundary-in-ignore-holes")
    g, res = _wkb(), 12
    fill = set(_quadbin.polyfill(g, res))
    for mode in boundary_modes:
        k0 = set(_quadbin.geometry_k_ring(g, res, 0, mode=mode))
        assert k0 == fill, f"mode={mode}: k0={len(k0)} fill={len(fill)}"


def test_quadbin_geomkloop_k0_returns_polyfill():
    """k=0 loop == polyfill."""
    g, res = _wkb(), 12
    fill = set(_quadbin.polyfill(g, res))
    assert set(_quadbin.geometry_k_loop(g, res, 0)) == fill


def test_quadbin_geomkring_all_modes_return_bigints():
    """All 6 modes return int (bigint) cell ids."""
    from databricks.labs.gbx.pygx._dilate import MODES

    g, res = _wkb(), 12
    for mode in MODES:
        cells = _quadbin.geometry_k_ring(g, res, 1, mode=mode)
        assert all(isinstance(c, int) for c in cells), f"mode={mode}: non-int cell"


def test_quadbin_geomkring_invalid_mode_raises():
    """Unknown mode raises ValueError."""
    with pytest.raises(ValueError, match="unknown mode"):
        _quadbin.geometry_k_ring(_wkb(), 12, 1, mode="BOGUS")
