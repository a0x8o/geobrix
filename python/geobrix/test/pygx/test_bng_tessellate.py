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
    # 5km box on the 1km grid -> interior core cells + clipped border cells.
    geom = to_wkb(box(530000.0, 180000.0, 535000.0, 185000.0))
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
