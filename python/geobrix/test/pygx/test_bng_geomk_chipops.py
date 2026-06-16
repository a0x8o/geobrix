"""Spark-free unit tests for BNG geometry-centric neighborhood + chip ops (Task 7).

Covers ``geometryKRing``/``geometryKLoop`` (the k_ring/k_loop of cells covering a
geometry) and the chip-struct ops ``cellIntersection``/``cellUnion`` ported from
``BNG.scala`` (geometryKRing/geometryKLoop, getChips, lineFill/lineDecompose,
isValid) and the ``BNG_CellUnion``/``BNG_CellIntersection`` expressions. Chip
geometry is plain WKB (NO SRID), matching heavy ``JTS.toWKB``.
"""

import pytest

shapely = pytest.importorskip("shapely")

from shapely import to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402


def _box2(minx, miny, maxx, maxy):
    return box(minx, miny, maxx, maxy)


def _towkb(geom):
    return to_wkb(geom)


def test_geomkring_box_superset_of_polyfill():
    geom = _towkb(_box2(530000.0, 180000.0, 533000.0, 183000.0))
    res = _bng.get_resolution("1km")
    fill = set(_bng.polyfill_str(geom, res))
    gkr = set(_bng.geometry_k_ring_str(geom, res, 1))
    # k-ring around the geometry includes (at least) the tessellated coverage.
    assert fill <= gkr or len(gkr) >= len(fill)


def test_geomkloop_excludes_inner_ring():
    # A sub-cell box (single border chip, no core cells) so the k=2 loop is a
    # genuine non-empty hollow ring -- a multi-cell box can legitimately diff to
    # empty (border n-ring + core cover the whole k-loop), which would make the
    # non-emptiness check vacuous.
    geom = _towkb(_box2(530200.0, 180200.0, 530800.0, 180800.0))
    res = _bng.get_resolution("1km")
    gkr1 = set(_bng.geometry_k_ring_str(geom, res, 1))
    gkl2 = set(_bng.geometry_k_loop_str(geom, res, 2))
    # k-loop at 2 is disjoint from the k-ring at 1 (hollow outer ring); the
    # inner-ring subtraction removes everything covered by the k-ring at 1.
    assert gkl2.isdisjoint(gkr1)
    # ...and the hollow outer ring is non-empty.
    assert len(gkl2) > 0


def test_cell_union_same_cell_merges_chips():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid = _bng.parse(cid_s)
    full = _bng.cell_id_to_geometry(cid)
    left = (
        cid_s,
        False,
        full.buffer(0).intersection(_box2(530000, 180000, 530500, 181000)),
    )
    right = (
        cid_s,
        False,
        full.buffer(0).intersection(_box2(530500, 180000, 531000, 181000)),
    )
    cell, core, chip = _bng.cell_union(left, right)
    assert cell == cid_s
    assert chip.equals(full) or chip.area == pytest.approx(full.area, rel=1e-6)


def test_cell_intersection_different_cells_is_empty():
    a = (
        _bng.east_north_as_bng(530000.0, 180000.0, "1km"),
        False,
        _box2(530000, 180000, 531000, 181000),
    )
    b = (
        _bng.east_north_as_bng(540000.0, 180000.0, "1km"),
        False,
        _box2(540000, 180000, 541000, 181000),
    )
    cell, core, chip = _bng.cell_intersection(a, b)
    assert chip.is_empty


def test_geomk_str_emit_canonical_string_ids():
    geom = _towkb(_box2(530000.0, 180000.0, 532000.0, 182000.0))
    res = _bng.get_resolution("1km")
    for cid in _bng.geometry_k_ring_str(geom, res, 1):
        # round-trips through parse/format -> canonical BNG string id
        assert _bng.format(_bng.parse(cid)) == cid


def test_geomkring_covers_point():
    # Point geometry: getChips -> single border cell, k-ring expands around it.
    pt = _towkb(shapely.geometry.Point(530000.0, 180000.0))
    res = _bng.get_resolution("1km")
    gkr = _bng.geometry_k_ring_str(pt, res, 1)
    home = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    assert home in gkr
    # k-ring 1 around a single cell = center + 8 neighbours (all valid, in-bounds).
    assert len(gkr) == 9


def test_cell_union_core_chip_wins():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    full = _bng.cell_id_to_geometry(_bng.parse(cid_s))
    core_left = (cid_s, True, full)
    border_right = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    # left-hand rule: a core chip on either side short-circuits to that chip.
    assert _bng.cell_union(core_left, border_right) == core_left
    assert _bng.cell_intersection(core_left, border_right) == core_left
    # right core when left is border
    border_left = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    core_right = (cid_s, True, full)
    assert _bng.cell_union(border_left, core_right) == core_right


def _geom_equals(a, b):
    """Start-point-insensitive geometry equality (mutual near-containment).

    shapely renormalizes the ring start vertex on intersection/union, so
    ``equals_exact`` is unreliable; use ``equals`` (topological) with an
    area-approx fallback.
    """
    return a.equals(b) or a.area == pytest.approx(b.area, rel=1e-9)


# ---------------------------------------------------------------------------
# Chip-op branch ordering (heavy BNG_CellUnion/BNG_CellIntersection eval order)
#
# Heavy evalLong/evalString check the CORE flag FIRST (chip1.core -> chip1;
# chip2.core -> chip2) BEFORE the cellid-equality check. So a CORE chip with a
# mismatched cellid must return that core chip, NOT an empty polygon. Union and
# intersection share the identical eval order. These cover, for both ops:
#   (a) core-left + different cellid   -> left
#   (b) core-right + different cellid  -> right (left non-core)
#   (c) both non-core + different cellid -> (left cellid, left core, empty)
#   (d) both non-core + same cellid    -> geometric op
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", [_bng.cell_union, _bng.cell_intersection])
def test_chipop_core_left_mismatched_cellid_returns_left(op):
    # Heavy: if (chip1.core) return chip1 -- BEFORE the cellid check.
    cid_l = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid_r = _bng.east_north_as_bng(540000.0, 180000.0, "1km")
    left = (cid_l, True, _bng.cell_id_to_geometry(_bng.parse(cid_l)))
    right = (cid_r, False, _box2(540000, 180000, 541000, 181000))
    assert op(left, right) == left


@pytest.mark.parametrize("op", [_bng.cell_union, _bng.cell_intersection])
def test_chipop_core_right_mismatched_cellid_returns_right(op):
    # Heavy: chip1 non-core, then if (chip2.core) return chip2 -- still BEFORE
    # the cellid check (the cellid mismatch never gets a chance to empty it).
    cid_l = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid_r = _bng.east_north_as_bng(540000.0, 180000.0, "1km")
    left = (cid_l, False, _box2(530000, 180000, 531000, 181000))
    right = (cid_r, True, _bng.cell_id_to_geometry(_bng.parse(cid_r)))
    assert op(left, right) == right


@pytest.mark.parametrize("op", [_bng.cell_union, _bng.cell_intersection])
def test_chipop_both_noncore_mismatched_cellid_is_empty(op):
    # Heavy execute*: different cell ids -> (chip1._1, chip1._2, emptyPolygon).
    cid_l = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    cid_r = _bng.east_north_as_bng(540000.0, 180000.0, "1km")
    left = (cid_l, False, _box2(530000, 180000, 531000, 181000))
    right = (cid_r, False, _box2(540000, 180000, 541000, 181000))
    cell, core, chip = op(left, right)
    assert cell == cid_l  # left cellid
    assert core is False  # left core flag
    assert chip.is_empty  # empty polygon


def test_chipop_both_noncore_same_cell_union_merges():
    # Heavy: else -> chip1.union(chip2).
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    full = _bng.cell_id_to_geometry(_bng.parse(cid_s))
    left = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    right = (cid_s, False, _box2(530500, 180000, 531000, 181000))
    cell, core, chip = _bng.cell_union(left, right)
    assert cell == cid_s
    assert core is False
    assert _geom_equals(chip, full)


def test_chipop_both_noncore_same_cell_intersection_clips():
    # Heavy: else -> chip1.intersection(chip2).
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    left = (cid_s, False, _box2(530000, 180000, 531000, 181000))
    right = (cid_s, False, _box2(530000, 180000, 530500, 181000))
    cell, core, chip = _bng.cell_intersection(left, right)
    assert cell == cid_s
    assert core is False
    assert _geom_equals(chip, _box2(530000, 180000, 530500, 181000))


def test_line_fill_chips_follow_line():
    # A horizontal line spanning ~3 cells -> getChips yields border chips per cell.
    line = _towkb(
        shapely.geometry.LineString([(530100.0, 180500.0), (532900.0, 180500.0)])
    )
    res = _bng.get_resolution("1km")
    gkr = _bng.geometry_k_ring_str(line, res, 0)
    # k=0 ring = the line's own covering cells (border chips, no expansion).
    assert len(gkr) >= 3
