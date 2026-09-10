"""Tests for _quadbin.k_loop: hollow ring semantics, set-difference identity,
k==0 and k<0 edge cases."""

import pytest

pytest.importorskip("quadbin")
import quadbin  # noqa: E402

from databricks.labs.gbx.pygx import _quadbin  # noqa: E402


def test_kloop_k0_returns_center():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    assert _quadbin.k_loop(cell, 0) == [cell]


def test_kloop_negative_k_raises():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    with pytest.raises(ValueError):
        _quadbin.k_loop(cell, -1)


def test_kloop_k1_is_hollow_ring():
    """k_loop(k) == sorted(k_ring(k) - k_ring(k-1))."""
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    ring1 = set(_quadbin.k_ring(cell, 1))
    ring0 = set(_quadbin.k_ring(cell, 0))
    expected = sorted(ring1 - ring0)
    assert _quadbin.k_loop(cell, 1) == expected


def test_kloop_k2_is_hollow_ring():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    ring2 = set(_quadbin.k_ring(cell, 2))
    ring1 = set(_quadbin.k_ring(cell, 1))
    expected = sorted(ring2 - ring1)
    assert _quadbin.k_loop(cell, 2) == expected


def test_kring_equals_union_of_kloops():
    """k_ring(k) == union of k_loop(0..k)."""
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    k = 3
    kring_k = set(_quadbin.k_ring(cell, k))
    kloop_union = set()
    for i in range(k + 1):
        kloop_union.update(_quadbin.k_loop(cell, i))
    assert kring_k == kloop_union


def test_kloop_does_not_include_center_for_k1():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    assert cell not in _quadbin.k_loop(cell, 1)


def test_kloop_k1_size_is_8_for_interior_cell():
    """Interior cell at zoom 10: k=1 ring has 8 cells (3x3 - 1 center)."""
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    assert len(_quadbin.k_loop(cell, 1)) == 8
