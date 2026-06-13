import numpy as np
import pytest

pytest.importorskip("scipy")
from databricks.labs.gbx.pyvx import _tin


def test_triangulate_square_gives_two_triangles():
    pts = np.array([[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [1.0, 1.0, 0.0], [0.0, 1.0, 0.0]])
    tris = _tin.triangulate(pts, breaklines=[], merge_tolerance=0.0, snap_tolerance=0.0)
    assert len(tris) == 2
    assert all(t.shape == (3, 3) for t in tris)


def test_merge_tolerance_dedups_near_coincident():
    pts = np.array([[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0], [1e-9, 1e-9, 0]], dtype=float)
    tris = _tin.triangulate(pts, breaklines=[], merge_tolerance=1e-6, snap_tolerance=0.0)
    assert len(tris) == 2


def test_empty_or_too_few_points():
    assert _tin.triangulate(np.zeros((0, 3)), [], 0.0, 0.0) == []
    assert _tin.triangulate(np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 0.0]]), [], 0.0, 0.0) == []
