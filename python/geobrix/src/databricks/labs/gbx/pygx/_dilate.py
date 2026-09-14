"""Grid-agnostic geometry dilation engine (frontier BFS) + covering classifier.

Shared by every light-tier geom-aware kring/kloop (quadbin, custom, BNG-light, h3).
See .superpowers/specs/2026-09-11-geom-aware-kring-kloop-design.md §4/§5.
"""

from dataclasses import dataclass, field

from shapely.geometry import Polygon

MODES = (
    "boundary-out",
    "boundary-in",
    "boundary-in-ignore-holes",
    "hole-in",
    "hole-out",
    "hole-out-ignore-geom",
)
DEFAULT_MODE = "boundary-out"

# Coverage basis selector (LOCKED design, "COVERAGE PARAM").  Three nested
# predicates decide which cells "belong to" a region P/S/H:
#   coveras  -> cover    (*_cover)    : cell overlaps the region
#   polyfill -> centroid (*_centroid) : cell CENTROID is inside the region
#   core     -> core     (*_core)     : cell is fully contained by the region
# Nested: cover ⊇ centroid ⊇ core.  The chosen basis drives the seed
# (perimeter), the admit bound, and boundary-ring inclusion.
COVERAGE = ("coveras", "polyfill", "core")
DEFAULT_COVERAGE = "coveras"
_COVERAGE_BASIS = {"coveras": "cover", "polyfill": "centroid", "core": "core"}


def _basis_sets(cls, coverage):
    """Return (P_X, S_X, H_X) — the belongs-to sets for the chosen coverage basis.

    Validates ``coverage`` (raises ValueError on an unknown value).
    """
    if coverage not in _COVERAGE_BASIS:
        raise ValueError(f"unknown coverage {coverage!r}; expected one of {COVERAGE}")
    b = _COVERAGE_BASIS[coverage]
    return (
        getattr(cls, f"p_{b}"),
        getattr(cls, f"s_{b}"),
        getattr(cls, f"h_{b}"),
    )


def outer_perimeter(s_cover, neighbors):
    """Covering-set perimeter: cells in s_cover with at least one neighbor outside s_cover.

    outer_perimeter(S) = { c ∈ s_cover : ∃ n ∈ neighbors(c) with n ∉ s_cover }

    Properties:
    - Alignment-robust: any non-empty covering set has a perimeter, regardless of whether
      any cell straddles the geometry boundary (fixes the grid-aligned empty-seed bug).
    - Excludes hole-rim cells: for a holed polygon, hole-rim cells lie inside the filled
      solid S, so all their neighbors are in s_cover → not on the perimeter.  Subsumes
      Layer-1's s_border approach while fixing the aligned-solid case.
    - Non-empty whenever s_cover is non-empty.
    """
    return frozenset(c for c in s_cover if any(n not in s_cover for n in neighbors(c)))


def dilate(frontier0, visited0, neighbors, admit):
    """Yield (k, shell) for k=1,2,...; shell = cells first reached at step k.

    visited0 blocks re-entry (direction); admit filters+prunes (region bound).
    Each cell is recorded once → total work O(|output|).
    """
    visited = set(visited0)
    frontier = set(frontier0)
    k = 0
    while frontier:
        k += 1
        nxt = {
            n for c in frontier for n in neighbors(c) if n not in visited and admit(n)
        }
        if not nxt:
            return
        visited |= nxt
        frontier = nxt
        yield k, nxt


@dataclass
class Classification:
    p_cover: set  # overlaps P (geom with holes)
    p_core: set  # fully inside P
    s_cover: set  # overlaps S (outer ring, holes filled)
    s_core: set  # fully inside S
    h_cover: set  # overlaps holes union H
    h_core: set  # fully inside H
    # Centroid basis (cell CENTROID inside the region) — the "polyfill" coverage.
    # Nested strictly between core and cover: core ⊆ centroid ⊆ cover.  Defaulted
    # so legacy 6-arg positional construction (e.g. an empty Classification) works.
    p_centroid: set = field(default_factory=set)  # centroid inside P
    s_centroid: set = field(default_factory=set)  # centroid inside S
    h_centroid: set = field(default_factory=set)  # centroid inside H

    @property
    def p_border(self):
        return self.p_cover - self.p_core

    @property
    def s_border(self):
        """Outer-boundary cells only: overlaps S but not fully inside S.

        For a polygon with no holes, S = P so s_border == p_border.
        For a holed polygon, s_border excludes hole-rim cells (which ARE in
        p_border but are fully inside S and therefore in s_core, not s_border).
        This is the correct frontier seed for boundary-* modes.
        """
        return self.s_cover - self.s_core

    @property
    def h_border(self):
        return self.h_cover - self.h_core


def _solid_and_holes(geom):
    """Return (S, H) shapely geoms: S = outer ring filled; H = union of holes."""
    from shapely.geometry import MultiPolygon
    from shapely.ops import unary_union

    polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    solids, holes = [], []
    for p in polys:
        if p.geom_type != "Polygon":
            continue
        solids.append(Polygon(p.exterior))
        holes.extend(Polygon(r) for r in p.interiors)
    S = unary_union(solids) if solids else geom
    H = unary_union(holes) if holes else None
    return S, H


def _geom_dimension(geom) -> int:
    """Topological dimension of a geometry: 0=point, 1=line/ring, 2=surface/other."""
    t = geom.geom_type
    if t in ("Point", "MultiPoint"):
        return 0
    if t in ("LineString", "LinearRing", "MultiLineString"):
        return 1
    return 2  # Polygon, MultiPolygon, GeometryCollection, etc.


def _sample_coords(geom, n_samples: int = 16):
    """Yield (x, y) coordinate pairs sampled from a point or line geometry.

    Used for generating candidate cells from a grid's point_to_cell hook when
    polyfill_fn returns nothing for non-polygon geometries (e.g. BNG centroid-BFS
    returns empty for a point or line input).

    - Point / MultiPoint: yield the point coordinate(s).
    - LineString / LinearRing: yield endpoints + n_samples interior samples + centroid.
    - Multi-geometries: recurse into each part.
    """
    if hasattr(geom, "geoms"):
        for part in geom.geoms:
            yield from _sample_coords(part, n_samples)
        return
    # Single geometry: centroid first (always available)
    c = geom.centroid
    yield (c.x, c.y)
    # Explicit vertex coordinates (Point has .coords; LineString has .coords)
    if hasattr(geom, "coords"):
        for xy in geom.coords:
            yield (xy[0], xy[1])
    # Densify lines with n_samples-1 interior fractions
    if geom.geom_type in ("LineString", "LinearRing") and n_samples > 0:
        ln = geom.length
        if ln > 0:
            for i in range(1, n_samples):
                pt = geom.interpolate(i / n_samples, normalized=True)
                yield (pt.x, pt.y)


def classify(geom, res, polyfill_fn, cell_geom_fn, point_to_cell_fn=None):
    """Partition polyfill candidate cells vs P (geom), S (solid), H (holes).

    Parameters
    ----------
    geom : shapely geometry
    res : resolution (grid-specific)
    polyfill_fn : callable(g, res) -> list[cell_id]
        Grid's polygon polyfill.  Must be called with the SOLID (hole-filled)
        geometry so that hole-interior cells become candidates for hole-* modes.
        For non-polygon geoms (point/line) this may return an empty list (e.g.
        BNG centroid-BFS, custom centroid-containment).
    cell_geom_fn : callable(cell_id) -> shapely polygon
        Inverse map: grid cell → its bounding polygon.
    point_to_cell_fn : callable(x, y) -> cell_id | None, optional
        Per-grid hook that returns the cell containing a coordinate pair.
        Required for grids whose polyfill_fn cannot handle point/line inputs
        (BNG, custom).  When provided, it is used as a fallback only when
        polyfill_fn returns an empty candidate set for a non-polygon geometry.
        Quadbin (bbox-based polyfill) does not need this hook.

    Coverage semantics (dimension-aware, replaces the old uniform area>0 test):
    - Polygon (dim 2): cell ∈ cover iff intersection area > 0.
    - Line    (dim 1): cell ∈ cover iff intersection length > 0.
    - Point   (dim 0): cell ∈ cover iff they intersect (any shared point suffices).
    Core sets: cell ∈ core iff geom.contains(cell) — always empty for point/line
    because no polygon cell can be contained by a 0D or 1D geometry.

    Centroid sets (the "polyfill" coverage basis): cell ∈ centroid iff the cell's
    centroid lies inside the region (region.contains(cell.centroid)).  Nested
    strictly between core and cover.  Always empty for point/line (a cell centroid
    almost never lies exactly on a 0D/1D geometry) — so polyfill/core coverage of a
    point or line is naturally empty, which is the intended behaviour.
    """
    S, H = _solid_and_holes(geom)
    dim = _geom_dimension(geom)

    if dim == 2:
        # Polygon path: unchanged.  Polyfill the filled SOLID so hole-interior
        # cells are candidates for hole-* mode classification.
        cands = set(polyfill_fn(S, res))
    else:
        # Non-polygon (point or line): try polyfill first.
        # Quadbin uses a bbox-based polyfill that returns cells for any input;
        # BNG/custom use centroid-containment and return nothing for points/lines.
        cands = set(polyfill_fn(S, res))
        if not cands and point_to_cell_fn is not None:
            # Fallback: sample representative coordinates along the geometry and
            # map each to its containing cell via the per-grid hook.
            seen: set = set()
            for x, y in _sample_coords(geom):
                if (x, y) in seen:
                    continue
                seen.add((x, y))
                try:
                    c = point_to_cell_fn(x, y)
                    if c is not None:
                        cands.add(c)
                except Exception:
                    pass

    p_cover, p_core, s_cover, s_core, h_cover, h_core = (set() for _ in range(6))
    p_centroid, s_centroid, h_centroid = set(), set(), set()

    for c in cands:
        g = cell_geom_fn(c)
        cen = g.centroid  # cell centroid — the "polyfill" (centroid) basis probe

        # Dimension-aware coverage test for P (the original geometry, may have holes)
        # and S (the hole-filled solid, always a polygon when dim==2).
        if dim == 0:
            p_in_cover = geom.intersects(g)
            s_in_cover = S.intersects(g)  # S == geom for non-polygon
        elif dim == 1:
            ix_p = geom.intersection(g)
            p_in_cover = geom.intersects(g) and ix_p.length > 0
            ix_s = S.intersection(g)
            s_in_cover = S.intersects(g) and ix_s.length > 0
        else:
            p_in_cover = geom.intersects(g) and geom.intersection(g).area > 0
            s_in_cover = S.intersects(g) and S.intersection(g).area > 0

        # The centroid basis (and core basis) is 2D-only.  For a point/line region a
        # cell centroid is "inside" only by measure-zero coincidence (e.g. a point
        # placed exactly at a cell centre) — which is not a meaningful polyfill hit,
        # so polyfill/core coverage of a 0/1-dim geom is empty by construction.
        is_2d = dim == 2
        if p_in_cover:
            p_cover.add(c)
            if is_2d and geom.contains(cen):
                p_centroid.add(c)
            if is_2d and geom.contains(g):
                p_core.add(c)
        if s_in_cover:
            s_cover.add(c)
            if is_2d and S.contains(cen):
                s_centroid.add(c)
            if is_2d and S.contains(g):
                s_core.add(c)
        if H is not None and H.intersects(g) and H.intersection(g).area > 0:
            h_cover.add(c)
            if is_2d and H.contains(cen):
                h_centroid.add(c)
            if is_2d and H.contains(g):
                h_core.add(c)

    return Classification(
        p_cover,
        p_core,
        s_cover,
        s_core,
        h_cover,
        h_core,
        p_centroid=p_centroid,
        s_centroid=s_centroid,
        h_centroid=h_centroid,
    )


def mode_setup(mode, cls, neighbors=None, coverage=DEFAULT_COVERAGE):
    """Return (frontier0, visited0, admit, k0) for a traversal mode + coverage.

    ``coverage`` ∈ {"coveras","polyfill","core"} selects the belongs-to basis X
    (cover/centroid/core).  Seeds (perimeters), admit bounds and boundary-ring
    inclusion are all computed against the region sets P_X / S_X / H_X.

    For boundary-* modes `neighbors` must be provided so that outer_perimeter can
    be computed.  hole-* modes also require `neighbors` for alignment-robust
    hole-edge perimeter seeds (void_hole_edge / solid_hole_edge).
    """
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {MODES}")
    # Select the belongs-to basis (validates `coverage`).
    p_x, s_x, h_x = _basis_sets(cls, coverage)
    # boundary-* modes seed from the region-X perimeter (topological outer ring).
    # outer_perimeter is alignment-robust: a grid-aligned polygon has no straddling
    # cells but always has perimeter cells.  For a holed polygon, hole-rim cells lie
    # inside S (all their S-neighbours are in S_X) → excluded from the perimeter →
    # boundary-* never seeds from the hole rim.
    if mode.startswith("boundary-"):
        if neighbors is None:
            raise ValueError(
                f"mode {mode!r} requires `neighbors` to compute outer_perimeter"
            )
        op = outer_perimeter(s_x, neighbors)

    if mode == "boundary-out":
        # OUTWARD from the outer perimeter; visited (P_X ∪ op) blocks any inward
        # path (the hole is never reached — op is outer-only).  admit: True.
        # k0 = ∅: the geom / covering set is EXCLUDED — boundary-out returns ONLY
        # the outward k-step band (LOCKED design correction).
        return op, frozenset(p_x) | op, (lambda n: True), frozenset()
    if mode == "boundary-in":
        # frontier/visited/k0: op; admit: P_X only (respect holes)
        return op, op, (lambda n: n in p_x), op
    if mode == "boundary-in-ignore-holes":
        # frontier/visited/k0: op; admit: S_X (marches across hole interior)
        return op, op, (lambda n: n in s_x), op

    # ------------------------------------------------------------------
    # hole-* modes — alignment-robust perimeter seeds
    #
    # Old approach (h_border = h_cover - h_core) fails for grid-ALIGNED holes:
    # when the hole boundary exactly coincides with cell boundaries every cell is
    # either fully inside or fully outside → h_cover == h_core → h_border empty
    # → frontier empty → all three hole modes return nothing.
    #
    # Fix: replace h_border with topological perimeter seeds derived from
    # neighbor-adjacency, symmetric to outer_perimeter for boundary-* modes:
    #
    #   void_hole_edge  = { c ∈ h_cover : ∃ n ∉ h_cover }
    #       (hole-side cells adjacent to non-hole-region cells — always non-empty
    #        when h_core is non-empty, whether or not the hole is grid-aligned)
    #
    #   solid_hole_edge = { c ∈ p_core : ∃ n ∈ h_cover }
    #       (solid cells adjacent to the hole region — always non-empty when h_core
    #        is non-empty and there is solid material around the hole)
    #
    # Direction / admit semantics are UNCHANGED; only the seed changes.
    # ------------------------------------------------------------------
    if neighbors is None:
        raise ValueError(
            f"mode {mode!r} requires `neighbors` to compute hole-edge perimeters"
        )

    # Hole-edge perimeters computed on the chosen basis X (symmetric to
    # outer_perimeter for boundary-*).  Both are alignment-robust: non-empty for a
    # non-empty hole region regardless of grid alignment.
    #
    #   void_edge  = { c ∈ H_X : ∃ n ∉ H_X }  (hole-side cells adjacent to non-hole)
    #   solid_edge = { c ∈ P_X : ∃ n ∈ H_X }  (solid cells adjacent to the hole)
    void_edge = frozenset(c for c in h_x if any(n not in h_x for n in neighbors(c)))
    solid_edge = frozenset(c for c in p_x if any(n in h_x for n in neighbors(c)))

    if mode == "hole-in":
        # Seed from void-side (VOID-side hole edge).
        # Expand INTO the hole interior (admit H_X); visited = seed (blocks exit).
        return void_edge, void_edge, (lambda n: n in h_x), void_edge
    if mode == "hole-out":
        # Seed from solid-side (P_X cells adjacent to the hole).
        # Expand outward into solid (admit P_X); visited = H_X ∪ solid_edge so
        # the seed cannot reappear in shell 1 (disjoint-loop invariant).
        return solid_edge, frozenset(h_x) | frozenset(solid_edge), (lambda n: n in p_x), solid_edge
    # hole-out-ignore-geom: solid-side seed, expand unbounded (admit not-in-H_X).
    # Same visited0 fix: include solid_edge so shell 1 is disjoint from the seed.
    return solid_edge, frozenset(h_x) | frozenset(solid_edge), (lambda n: n not in h_x), solid_edge


def geom_expand(kind, k, mode, cls, neighbors, coverage=DEFAULT_COVERAGE):
    """kind='ring' (filled <=k) or 'loop' (shell at exactly k). k>=0.

    ``coverage`` selects the belongs-to basis (see :func:`mode_setup`).
    """
    if kind not in ("ring", "loop"):
        raise ValueError(f"kind must be 'ring' or 'loop'; got {kind!r}")
    frontier0, visited0, admit, k0 = mode_setup(mode, cls, neighbors, coverage)
    if k == 0:
        return set(k0)
    acc = set(k0) if kind == "ring" else set()
    shell_k = set()
    for kk, shell in dilate(frontier0, visited0, neighbors, admit):
        if kk > k:
            break
        if kind == "ring":
            acc |= shell
        if kk == k:
            shell_k = shell
            break
    return acc if kind == "ring" else shell_k
