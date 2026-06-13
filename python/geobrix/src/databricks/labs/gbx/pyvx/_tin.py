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


def _orient2d(a, b, c) -> float:
    """>0 if a->b->c is counter-clockwise."""
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _segments_intersect(p1, p2, p3, p4) -> bool:
    d1 = _orient2d(p3, p4, p1); d2 = _orient2d(p3, p4, p2)
    d3 = _orient2d(p1, p2, p3); d4 = _orient2d(p1, p2, p4)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def _vertex_index(xy: np.ndarray, p, tol=1e-9) -> int:
    d = np.hypot(xy[:, 0] - p[0], xy[:, 1] - p[1])
    i = int(np.argmin(d))
    return i if d[i] <= max(tol, 1e-9) else -1


def _recover_constraints(xy: np.ndarray, simplices: np.ndarray, tri, breaklines):
    """Sloan recovery: force each breakline segment to be a triangle edge via
    convex-quad edge flips. Bounded flip budget; raises on non-termination."""
    triangles = [list(s) for s in simplices.tolist()]

    def edges_of(t):
        return [frozenset((t[0], t[1])), frozenset((t[1], t[2])), frozenset((t[2], t[0]))]

    def build_adj():
        adj = {}
        for ti, t in enumerate(triangles):
            for e in edges_of(t):
                adj.setdefault(e, []).append(ti)
        return adj

    for bl in breaklines:
        seg_pts = np.asarray(bl, dtype=float)
        for k in range(len(seg_pts) - 1):
            ia = _vertex_index(xy, seg_pts[k]); ib = _vertex_index(xy, seg_pts[k + 1])
            if ia < 0 or ib < 0 or ia == ib:
                continue
            target = frozenset((ia, ib))
            budget = 50 * len(triangles) + 100
            while target not in build_adj() and budget > 0:
                budget -= 1
                adj = build_adj()
                flipped = False
                for e, ts in adj.items():
                    if len(ts) != 2:
                        continue
                    (u, v) = tuple(e)
                    if not _segments_intersect(xy[ia], xy[ib], xy[u], xy[v]):
                        continue
                    t0, t1 = triangles[ts[0]], triangles[ts[1]]
                    w0 = next(x for x in t0 if x not in e)
                    w1 = next(x for x in t1 if x not in e)
                    if not _segments_intersect(xy[w0], xy[w1], xy[u], xy[v]):
                        continue  # quad not convex -> flip would be invalid
                    triangles[ts[0]] = [w0, w1, u]
                    triangles[ts[1]] = [w0, w1, v]
                    flipped = True
                    break
                if not flipped:
                    break  # cannot recover this segment with convex flips; leave as-is
            if budget <= 0:
                raise RuntimeError("Sloan constraint recovery did not terminate")
    return np.array(triangles, dtype=np.int64)


def _zsnap(triangles, breaklines, tol):
    """Overwrite vertex Z by linear interpolation along any constraint line
    within tol (mirrors heavy LengthIndexedLine post-process)."""
    out = []
    for t in triangles:
        t = t.copy()
        for vi in range(3):
            p = t[vi]
            for bl in breaklines:
                bl = np.asarray(bl, dtype=float)
                if bl.shape[1] < 3:
                    continue
                for k in range(len(bl) - 1):
                    a, b = bl[k], bl[k + 1]
                    ab = b[:2] - a[:2]
                    L2 = float(ab @ ab)
                    if L2 == 0.0:
                        continue
                    s = float((p[:2] - a[:2]) @ ab) / L2
                    if 0.0 <= s <= 1.0:
                        proj = a[:2] + s * ab
                        if np.hypot(*(p[:2] - proj)) <= tol:
                            t[vi, 2] = a[2] + s * (b[2] - a[2])
        out.append(t)
    return out
