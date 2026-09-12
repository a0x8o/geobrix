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


def geom_expand_cells(
    kind, k, mode, *, cover, core, holes_cover, holes_core, solid_core=None
):
    """Run the shared dilation engine over precomputed h3 cell-id arrays.

    Args:
        kind:        "ring" (filled <=k) or "loop" (shell at exactly k).
        k:           Ring distance (int >= 0).
        mode:        Dilation mode string; one of _dilate.MODES.
        cover:       Cells that overlap the geometry P (iterable of int or hex str).
        core:        Cells fully inside P (iterable of int or hex str).
        holes_cover: Cells that overlap the holes union H (iterable of int or hex str).
        holes_core:  Cells fully inside H (iterable of int or hex str).
        solid_core:  Cells fully inside the SOLID S (outer ring, holes filled) —
                     the faithful s_core, supplied only for boundary-in-ignore-holes.
                     When empty/None, s_core falls back to core | holes_core.

    Returns:
        set of int cell ids.

    s_core (solid = outer ring with holes filled):
        boundary-in-ignore-holes is the only mode that reads s_core. When the
        wrapper supplies solid_core = h3_polyfillash3(solid), s_core is exact.
        Otherwise s_core = core | holes_core, which UNDER-COUNTS by the
        "rim-straddle" cells (fully inside the solid but straddling a hole
        boundary, so in neither p_core nor h_core), leaving small notches at
        hole rims — the reason boundary-in-ignore-holes passes solid_core.

        s_cover = cover | holes_cover is exact (S = P ∪ H) and unused by the
        engine's mode table, so it is not supplied separately.
    """
    p_cover = _to_int_set(cover)
    p_core = _to_int_set(core)
    h_cover = _to_int_set(holes_cover)
    h_core = _to_int_set(holes_core)
    s_cover = p_cover | h_cover  # exact; unused by the engine mode table
    # Faithful solid core when supplied (boundary-in-ignore-holes); else reconstruct.
    s_core = _to_int_set(solid_core) if solid_core else (p_core | h_core)

    cls = _dilate.Classification(
        p_cover=p_cover,
        p_core=p_core,
        s_cover=s_cover,
        s_core=s_core,
        h_cover=h_cover,
        h_core=h_core,
    )
    return _dilate.geom_expand(kind, int(k), mode, cls, _neighbors)
