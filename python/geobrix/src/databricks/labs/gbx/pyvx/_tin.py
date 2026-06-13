"""Pure-Python TIN engine for the pyvx light tier (Serverless-safe).

scipy Delaunay + Sloan constraint recovery (constrained, no Steiner points),
Z-snap to breaklines, barycentric Z interpolation, and grid generators.
Heavy parity target: vectorx.jts.InterpolateElevation (mode="constrained").
"""
from typing import List, Sequence

import numpy as np
from scipy.spatial import Delaunay


def _merge_vertices(pts: np.ndarray, tol: float) -> np.ndarray:
    """Snap near-coincident XY vertices (within tol) to a single representative."""
    if tol <= 0.0 or len(pts) == 0:
        return pts
    keys = np.round(pts[:, :2] / tol).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def triangulate(
    points: np.ndarray,
    breaklines: Sequence[np.ndarray],
    merge_tolerance: float,
    snap_tolerance: float,
) -> List[np.ndarray]:
    """Constrained Delaunay over XYZ points. Returns a list of (3,3) XYZ triangles.

    breaklines: sequence of (N,2|3) constraint polylines whose segments are forced
    as triangle edges (Sloan recovery). Empty -> plain Delaunay.
    """
    pts = _merge_vertices(np.asarray(points, dtype=float), merge_tolerance)
    if len(pts) < 3:
        return []
    tri = Delaunay(pts[:, :2])
    simplices = tri.simplices.copy()
    if breaklines:
        simplices = _recover_constraints(pts[:, :2], simplices, tri, breaklines)
    z = pts[:, 2]
    out = [np.column_stack([pts[s, 0], pts[s, 1], z[s]]) for s in simplices]
    if snap_tolerance > 0.0 and breaklines:
        out = _zsnap(out, breaklines, snap_tolerance)
    return out


def _recover_constraints(xy, simplices, tri, breaklines):
    return simplices  # real Sloan recovery added in the next task


def _zsnap(triangles, breaklines, tol):
    return triangles  # real Z-snap added in the next task
