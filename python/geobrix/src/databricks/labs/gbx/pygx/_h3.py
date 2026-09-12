"""h3 geometry-aware expansion: runs the shared dilation engine over precomputed
h3 cell-id arrays (cover/core/holes from product h3_* columnar functions).

Neighbors come from the h3 package (grid_disk) — topology only, no geometry.
The geometry work (polyfill / coverage of the geom and its holes) is supplied
columnar via F.expr by the PySpark composition wrapper in gridx/h3/functions.py.

Spec §6: h3 is light-tier ONLY (no Scala/heavy equivalent).
"""

import h3

from databricks.labs.gbx.pygx import _dilate


def _neighbors(c: int) -> list:
    """Return the 6 H3 neighbors of integer cell id c (grid_disk topology only).

    h3 v4 uses hex-string cell ids internally; we convert int → str → h3 → int.
    This mirrors the _h3_k_loop adapter in pygx/functions.py (cellfill agg).
    """
    c_str = h3.int_to_str(c)
    return [int(n, 16) for n in h3.grid_disk(c_str, 1) if n != c_str]


def _to_int_set(cells) -> set:
    """Convert an iterable of h3 cell ids (int or hex string) to a set of ints."""
    result = set()
    for c in cells:
        if isinstance(c, int):
            result.add(c)
        else:
            result.add(int(c, 16))
    return result


def geom_expand_cells(kind, k, mode, *, cover, core, holes_cover, holes_core):
    """Run the shared dilation engine over precomputed h3 cell-id arrays.

    Args:
        kind:        "ring" (filled <=k) or "loop" (shell at exactly k).
        k:           Ring distance (int >= 0).
        mode:        Dilation mode string; one of _dilate.MODES.
        cover:       Cells that overlap the geometry P (iterable of int or hex str).
        core:        Cells fully inside P (iterable of int or hex str).
        holes_cover: Cells that overlap the holes union H (iterable of int or hex str).
        holes_core:  Cells fully inside H (iterable of int or hex str).

    Returns:
        set of int cell ids.

    Notes on s_cover / s_core (solid = outer ring with holes filled):
        holes_cover/holes_core are now supplied columnar by the PySpark wrapper
        (gridx/h3/functions.py) for hole-reading modes, so hole-in / hole-out /
        hole-out-ignore-geom operate on real hole cells. We reconstruct
        s_cover = cover | holes_cover (exact: S = P ∪ H) and
        s_core = core | holes_core.

        s_core here UNDER-COUNTS the true solid core by the "rim-straddle" cells:
        a cell fully inside the solid that straddles a hole boundary (partly in
        the donut, partly in the hole) is in neither p_core nor h_core but IS in
        s_core. Only boundary-in-ignore-holes reads s_core, so on holed
        geometries its inward band can have small notches at hole rims. The
        faithful fix is to pass a solid-polyfill core array (h3_polyfillash3 of
        the solid) instead of reconstructing — a deliberate follow-up (see the
        SDD ledger "Phase B"); rim-straddle was 0 for a typical box+hole probe.
    """
    p_cover = _to_int_set(cover)
    p_core = _to_int_set(core)
    h_cover = _to_int_set(holes_cover)
    h_core = _to_int_set(holes_core)
    # Solid = outer ring filled = geom union holes (approximation for h3 arrays)
    s_cover = p_cover | h_cover
    s_core = p_core | h_core

    cls = _dilate.Classification(
        p_cover=p_cover,
        p_core=p_core,
        s_cover=s_cover,
        s_core=s_core,
        h_cover=h_cover,
        h_core=h_core,
    )
    return _dilate.geom_expand(kind, int(k), mode, cls, _neighbors)
