"""Spark-free unit tests for BNG tessellation (Task 6).

Tests the core/border split and the mosaic#423 input-geometry-type chip filter
ported from ``BNG.tessellate`` (gridx/grid/BNG.scala line 754). Chips are plain
WKB (NO SRID), matching heavy ``JTS.toWKB``.
"""

import pytest

shapely = pytest.importorskip("shapely")

from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402


def test_tessellate_box_has_core_and_border():
    # Box whose top/right edges cut cells in half (4.5km span on the 1km grid)
    # -> interior full cells become core, the half/quarter edge cells stay
    # clipped border chips. A fully grid-aligned box would (correctly) promote
    # every cell to core, so the edges must straddle a grid line.
    geom = to_wkb(box(530000.0, 180000.0, 534500.0, 184500.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    assert len(chips) > 0
    cores = [c for c in chips if c[1]]
    borders = [c for c in chips if not c[1]]
    assert cores and borders
    # core chip geom is None (keep_core_geom False default for the array form);
    # border chips carry a WKB polygon.
    for cell, core, chip in cores:
        assert chip is None
    for cell, core, chip in borders:
        assert chip is not None
        g = from_wkb(chip)
        assert g.geom_type in ("Polygon", "MultiPolygon")


def test_tessellate_chip_wkb_has_no_srid():
    # Heavy uses JTS.toWKB (no SRID), unlike quadbin's EWKB SRID 4326.
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    for cell, core, chip in chips:
        if chip is not None:
            assert get_srid(from_wkb(chip)) == 0


def test_tessellate_grid_aligned_no_degenerate_chips():
    # mosaic#423: a polygon aligned exactly to the 1km grid must NOT emit
    # POINT/LINESTRING chips at shared edges. Box edges land on 1km lines.
    geom = to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    assert len(chips) > 0
    for cell, core, chip in chips:
        if chip is not None:
            g = from_wkb(chip)
            assert g.geom_type in (
                "Polygon",
                "MultiPolygon",
            ), f"degenerate chip {g.geom_type} for {cell} (mosaic#423)"


def test_tessellate_fully_covered_cell_promoted_to_core():
    # A small grid-aligned polygon that fully covers one 1km cell. Because the
    # box width equals the cell edge, carved = buffer(-radius) is EMPTY, so the
    # fully-covered cell goes through the BORDER path. Heavy promotes a chip that
    # equals the whole cell (within 0.1m) to core=True / chip=None. shapely's
    # intersection renormalizes the ring start vertex, so the old equals_exact
    # check never fired and the cell came back core=False with a full-cell chip.
    res = _bng.get_resolution("1km")
    cell_box = box(530000.0, 180000.0, 531000.0, 181000.0)
    assert cell_box.buffer(-_bng.get_buffer_radius(res)).is_empty  # border path
    chips = _bng.tessellate_str(to_wkb(cell_box), res)
    # The fully-covered cell must be promoted to core (chip None), not a chip.
    cores = [c for c in chips if c[1]]
    assert cores, "fully-covered cell should be promoted to core=True"
    for cell, core, chip in cores:
        assert chip is None, f"promoted core cell {cell} must have chip=None"


def test_tessellate_empty_geom_is_empty():
    assert _bng.tessellate_str(None, 3) == []


def test_tessellate_returns_string_cellids():
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
    chips = _bng.tessellate_str(geom, _bng.get_resolution("1km"))
    for cell, core, chip in chips:
        assert isinstance(cell, str)
        assert isinstance(core, bool)
        assert cell.startswith("TQ")


def test_tessellate_core_set_subset_of_polyfill():
    # Every cell tessellate emits must also be a polyfill cell of the geometry
    # (tessellate = polyfill(carved) core + polyfill(border) clipped chips).
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
    res = _bng.get_resolution("1km")
    fill = set(_bng.polyfill_str(geom, res))
    cores = {c for (c, core, chip) in _bng.tessellate_str(geom, res) if core}
    assert cores <= fill
