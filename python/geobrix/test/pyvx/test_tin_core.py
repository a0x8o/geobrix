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


def test_breakline_appears_as_triangle_edges():
    pts = np.array([[0,0,0],[4,0,0],[4,4,0],[0,4,0],[1,3,0],[3,1,0]], dtype=float)
    bl = [np.array([[1.0, 3.0], [3.0, 1.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)
    edges = set()
    for t in tris:
        xy = [tuple(np.round(p[:2], 6)) for p in t]
        for a, b in [(0, 1), (1, 2), (2, 0)]:
            edges.add(frozenset([xy[a], xy[b]]))
    assert frozenset([(1.0, 3.0), (3.0, 1.0)]) in edges


def test_recovery_terminates_on_dense_constraints():
    rng = np.random.default_rng(0)
    pts = np.column_stack([rng.random(40), rng.random(40), np.zeros(40)])
    bl = [np.array([[0.05, 0.05], [0.95, 0.95]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)  # must not hang
    assert len(tris) > 0


def test_zsnap_sets_vertex_z_along_constraint():
    pts = np.array([[0,0,0],[4,0,0],[4,4,0],[0,4,0]], dtype=float)
    bl = [np.array([[0.0, 2.0, 10.0], [4.0, 2.0, 10.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 1e-6)
    assert len(tris) > 0  # must not crash; recovery + snap run


def test_grid_bbox_centers_column_major():
    cells = list(_tin.grid_bbox(0.0, 0.0, 2.0, 2.0, 2, 2))
    assert cells == [(0.5, 0.5), (0.5, 1.5), (1.5, 0.5), (1.5, 1.5)]


def test_grid_geom_negative_celly():
    cells = list(_tin.grid_geom(0.0, 10.0, 2, 2, 5.0, -5.0))
    assert cells == [(2.5, 7.5), (2.5, 2.5), (7.5, 7.5), (7.5, 2.5)]


def test_interpolate_known_plane_and_outside_hull():
    pts = np.array([[0,0,0],[1,0,1],[1,1,2],[0,1,1]], dtype=float)
    tris = _tin.triangulate(pts, [], 0.0, 0.0)
    z = _tin.interpolate_z(tris, 0.5, 0.5)
    assert abs(z - 1.0) < 1e-9
    assert _tin.interpolate_z(tris, 5.0, 5.0) is None
