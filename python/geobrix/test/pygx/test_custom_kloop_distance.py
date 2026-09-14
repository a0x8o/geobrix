"""Tests for _custom.k_loop and _custom.distance:
hollow-ring semantics, set-difference identity, k==0 / k<0 edge cases,
and Chebyshev (max(|dx|,|dy|)) distance semantics."""

import pytest

shapely = pytest.importorskip("shapely")  # _custom imports shapely at module load

from databricks.labs.gbx.pygx import _custom  # noqa: E402


def _conf():
    """0..1,000,000 grid with 1000-unit root cells, splits=2."""
    return _custom.CustomGridConf(
        bound_x_min=0,
        bound_x_max=1_000_000,
        bound_y_min=0,
        bound_y_max=1_000_000,
        cell_splits=2,
        root_cell_size_x=1000,
        root_cell_size_y=1000,
        srid=-1,
    )


# --- k_loop -----------------------------------------------------------------


def test_kloop_k0_returns_center():
    conf = _conf()
    # At res=0, cell_width=1000; pick an interior point.
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    assert _custom.k_loop(conf, cell, 0) == [cell]


def test_kloop_negative_k_raises():
    conf = _conf()
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    with pytest.raises(ValueError):
        _custom.k_loop(conf, cell, -1)


def test_kloop_k1_is_hollow_ring():
    """k_loop(k) == sorted(k_ring(k) - k_ring(k-1))."""
    conf = _conf()
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    ring1 = set(_custom.k_ring(conf, cell, 1))
    ring0 = set(_custom.k_ring(conf, cell, 0))
    expected = sorted(ring1 - ring0)
    assert _custom.k_loop(conf, cell, 1) == expected


def test_kloop_k2_is_hollow_ring():
    conf = _conf()
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    ring2 = set(_custom.k_ring(conf, cell, 2))
    ring1 = set(_custom.k_ring(conf, cell, 1))
    expected = sorted(ring2 - ring1)
    assert _custom.k_loop(conf, cell, 2) == expected


def test_kring_equals_union_of_kloops():
    """k_ring(k) == union of k_loop(0..k)."""
    conf = _conf()
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    k = 3
    kring_k = set(_custom.k_ring(conf, cell, k))
    kloop_union = set()
    for i in range(k + 1):
        kloop_union.update(_custom.k_loop(conf, cell, i))
    assert kring_k == kloop_union


def test_kloop_does_not_include_center_for_k1():
    conf = _conf()
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    assert cell not in _custom.k_loop(conf, cell, 1)


# --- distance (Chebyshev) ---------------------------------------------------
# At res=0 with root_cell_size=1000: cell_width=1000, so cell_pos_x = x//1000.
# cell at x=500_000 -> pos_x=500; at x=501_000 -> pos_x=501 (dx=1).


def test_distance_same_cell_is_zero():
    conf = _conf()
    cell = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    assert _custom.distance(conf, cell, cell) == 0


def test_distance_diagonal_dx1_dy1_chebyshev_is_1():
    """Diagonal: dx=1, dy=1 -> Chebyshev=1 (Manhattan would be 2)."""
    conf = _conf()
    cell_a = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    cell_b = _custom.point_to_cell_id(conf, 501_000.0, 501_000.0, 0)
    assert _custom.distance(conf, cell_a, cell_b) == 1


def test_distance_diagonal_dx2_dy1_chebyshev_is_2():
    """Diagonal: dx=2, dy=1 -> Chebyshev=2 (Manhattan would be 3)."""
    conf = _conf()
    cell_a = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    cell_b = _custom.point_to_cell_id(conf, 502_000.0, 501_000.0, 0)
    assert _custom.distance(conf, cell_a, cell_b) == 2


def test_distance_pure_horizontal():
    """dx=3, dy=0 -> Chebyshev=3."""
    conf = _conf()
    cell_a = _custom.point_to_cell_id(conf, 500_000.0, 500_000.0, 0)
    cell_b = _custom.point_to_cell_id(conf, 503_000.0, 500_000.0, 0)
    assert _custom.distance(conf, cell_a, cell_b) == 3
