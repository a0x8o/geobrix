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
        For h3 we approximate s_cover = cover | holes_cover and
        s_core = core | holes_core.  When holes_cover/holes_core are both empty
        (deferred extraction), s_core == p_core. This means:
        - boundary-in-ignore-holes silently degrades to boundary-in on holed
          geometries (s_core = p_core, not the solid-fill core): non-empty but
          INCORRECT, not empty. NOTE: this is a correctness concern, not just
          a missing feature — boundary-in-ignore-holes is supposed to grow
          inward ignoring the holes, but with s_core == p_core it's blocked by
          the same holes as boundary-in. VERIFY and fix when hole extraction is
          wired at integration.
        - hole-in / hole-out / hole-out-ignore-geom correctly return empty
          results (h_cover=h_core={} → h_border empty → no frontier).
        VERIFY the full s_cover/s_core approximation vs product classification
        at integration time (Step 5).
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
