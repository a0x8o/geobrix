"""Unit tests for pygx quadbin geometry-aware kring/kloop.

Tests mirror the brief Step 1 fixture exactly, plus a fixture-correctness gate
for the holed polygon used in parity tests (verifying hCore is non-empty).
"""

import pytest
from shapely import to_wkb
from shapely.geometry import box
from shapely.geometry.polygon import Polygon

from databricks.labs.gbx.pygx import _quadbin


def _wkb():
    return to_wkb(box(-73.99, 40.71, -73.95, 40.75))  # NYC lon/lat


# Holed fixture mirroring the Scala and parity tests:
# east-US box (-76,38)→(-72,43) with 2°×3° interior hole (-75,39)→(-73,42) at res 10.
# At res 10 (cells ≈0.35°) the hole spans ~6×9 cells → hCore non-empty.
_HOLED_OUTER = [(-76.0, 38.0), (-72.0, 38.0), (-72.0, 43.0), (-76.0, 43.0)]
_HOLED_HOLE = [(-75.0, 39.0), (-73.0, 39.0), (-73.0, 42.0), (-75.0, 42.0)]
_HOLED_POLY = Polygon(_HOLED_OUTER, [_HOLED_HOLE])
_HOLED_RES = 10


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


def test_quadbin_geomkring_k0_boundary_out_is_polyfill():
    """boundary-out k=0 == polyfill (the full covering set; k0 = p_cover, unchanged)."""
    g, res = _wkb(), 12
    fill = set(_quadbin.polyfill(g, res))
    k0 = set(_quadbin.geometry_k_ring(g, res, 0, mode="boundary-out"))
    assert k0 == fill, f"boundary-out k=0={len(k0)} fill={len(fill)}"


def test_quadbin_geomkring_k0_boundary_in_is_outer_perimeter():
    """boundary-in/ignore-holes k=0 == outer_perimeter(s_cover) under the perimeter fix.

    Layer-2 fix: boundary-in and boundary-in-ignore-holes seed from the covering-set
    perimeter (outer ring of s_cover), not the full polyfill.  This makes them
    alignment-robust: a grid-aligned polygon has zero straddling cells but always has
    a non-empty outer perimeter.
    """
    g, res = _wkb(), 12
    fill = set(_quadbin.polyfill(g, res))
    for mode in ("boundary-in", "boundary-in-ignore-holes"):
        k0 = set(_quadbin.geometry_k_ring(g, res, 0, mode=mode))
        # k0 is the outer perimeter — a non-empty subset of the covering set.
        assert k0, f"mode={mode}: k0 must be non-empty"
        assert k0 <= fill, f"mode={mode}: k0 must be a subset of polyfill"


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


def test_holed_fixture_h_core_nonempty():
    """Fixture correctness gate: large hole at res 10 must populate hCore.

    The 2°×3° hole (-75,39)→(-73,42) at res 10 spans ~6×9 cells; cells
    well inside the hole are fully contained by the hole polygon → hCore
    must be non-empty so hole-in/hole-out exercise genuine inward fill.
    """
    from shapely.geometry import shape

    cls = _quadbin.classify(shape(_HOLED_POLY), _HOLED_RES)
    assert len(cls.h_core) > 0, (
        f"hCore is empty — hole is too small for res {_HOLED_RES}; "
        f"hCover={len(cls.h_cover)}, hBorder={len(cls.h_border)}"
    )


def test_hole_in_mode_reaches_hcore():
    """hole-in k=3 expansion reaches hCore cells and stays within hCover."""
    from shapely.geometry import shape

    cls = _quadbin.classify(shape(_HOLED_POLY), _HOLED_RES)
    expanded = set(
        _quadbin.geometry_k_ring(to_wkb(_HOLED_POLY), _HOLED_RES, 3, mode="hole-in")
    )
    # All cells must be within the hole region.
    assert expanded <= cls.h_cover, "hole-in result contains cells outside hCover"
    # After 3 steps inward, must reach some hCore cells.
    assert expanded & cls.h_core, "hole-in k=3 did not reach any hCore cells"
    # Must not contain cells that are only in the solid interior (pCore \ hCover).
    solid_only = cls.p_core - cls.h_cover
    assert not (expanded & solid_only), "hole-in result leaked into solid pCore"
