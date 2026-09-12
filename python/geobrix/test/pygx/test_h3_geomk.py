"""Unit tests for pygx h3 geometry-aware kring/kloop.

Tests the _h3.geom_expand_cells engine driver and the array-expansion UDF
wrappers directly, constructing cover/core arrays via the h3 library.

Cell ID convention: h3 v4 returns hex strings internally; we convert to integers
(BIGINT) since that is the UDF/SQL type.  Use h3.str_to_int(h3.latlng_to_cell(...)).

Cover vs core fixtures:
    For a real polygon, h3_coverash3(geom) returns cells that OVERLAP (cover),
    h3_polyfillash3(geom) returns cells FULLY inside (core).  The p_border =
    cover − core is NON-EMPTY for any real polygon, providing the frontier for
    boundary-out dilation.  Tests use core ≠ cover to reflect real product behavior
    (cover==core → p_border empty → boundary-out frontier empty → no dilation).

Run locally (no Databricks):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_geomk.py

Integration (product h3_coverash3/h3_polyfillash3 cross-check) requires a
Databricks session — skip it locally and run via:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_geomk.py \\
        --with-integration
"""

import h3
import pytest

from databricks.labs.gbx.pygx import _h3

# ---------------------------------------------------------------------------
# Helpers: convert h3 hex strings to ints (BIGINT as used by UDFs)
# ---------------------------------------------------------------------------


def _cell_int(lat, lng, res) -> int:
    """Integer H3 cell id for (lat, lng) at resolution."""
    return int(h3.latlng_to_cell(lat, lng, res), 16)


def _disk_ints(c_int: int, k: int) -> set:
    """h3.grid_disk(c, k) as a set of ints."""
    c_str = h3.int_to_str(c_int)
    return {int(n, 16) for n in h3.grid_disk(c_str, k)}


def _ring_ints(c_int: int, k: int) -> set:
    """h3.grid_ring(c, k) as a set of ints (hollow ring at exactly k)."""
    c_str = h3.int_to_str(c_int)
    return {int(n, 16) for n in h3.grid_ring(c_str, k)}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _nyc_cell(res=9) -> int:
    """NYC reference cell (single int) at the given resolution."""
    return _cell_int(40.71, -73.99, res)


def _border_cover_core(center: int, res=9):
    """Fixture simulating a 'disk-1' polygon:
        cover = disk(center, 1) — cells that OVERLAP a ~7-cell polygon
        core  = {center}       — cells FULLY inside (just the center)
    p_border = outer 6 cells of the disk. This is a realistic product-like
    fixture where cover != core and the frontier is non-empty.
    """
    cover = _disk_ints(center, 1)  # 7 cells (center + 6 neighbors)
    core = {center}  # center only is "fully inside"
    return cover, core


# ---------------------------------------------------------------------------
# _h3.geom_expand_cells — engine driver tests
# ---------------------------------------------------------------------------


def test_h3_geom_expand_cells_boundary_out_single_border_cell():
    """boundary-out k=1 from a single border cell (cover={c}, core={}) == grid_disk(c,1).

    When a tiny polygon covers exactly one cell at its border (overlaps but
    doesn't fully contain it), cover={c} and core={}.  The dilation frontier
    is {c} (p_border = cover − core = {c}), and boundary-out k=1 grows to all
    6 neighbours plus the center → equals h3.grid_disk(c, 1).

    This is the analogue of the brief's Step 1 fixture with the correct
    cover != core semantics for real product behavior.
    """
    c = _nyc_cell()
    cover = {c}
    core = set()  # point-like: overlaps the cell but doesn't fill it → no core
    result = _h3.geom_expand_cells(
        "ring",
        1,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    expected = _disk_ints(c, 1)
    assert result == expected, (
        f"boundary-out k=1 from single border cell should equal grid_disk(c,1); "
        f"got {len(result)}, expected {len(expected)}"
    )


def test_h3_geom_expand_cells_ring_k0_is_covering_set():
    """ring k=0 returns the initial k0 set == p_cover (the covering set)."""
    c = _nyc_cell()
    cover = {c}
    core = set()
    result = _h3.geom_expand_cells(
        "ring",
        0,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    assert result == cover


def test_h3_geom_expand_cells_loop_is_ring_diff():
    """geomkloop(k) == ring(k) − ring(k−1): hollow shell at exactly k.

    Uses a disk-1 fixture (cover=7 cells, core={center}) where the 6 outer cells
    are p_border and form the frontier. Dilation grows outward from that frontier.
    """
    c = _nyc_cell()
    cover, core = _border_cover_core(c)

    r2 = _h3.geom_expand_cells(
        "ring",
        2,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    r1 = _h3.geom_expand_cells(
        "ring",
        1,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    loop2 = _h3.geom_expand_cells(
        "loop",
        2,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    assert loop2 == (r2 - r1), (
        f"loop(2) should == ring(2) − ring(1); "
        f"loop2={len(loop2)}, r2-r1={len(r2 - r1)}"
    )


def test_h3_geom_expand_cells_ring_superset_of_cover():
    """ring k>=1 is a superset of the covering set."""
    c = _nyc_cell()
    cover, core = _border_cover_core(c)

    r1 = _h3.geom_expand_cells(
        "ring",
        1,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    assert cover <= r1, "ring(k>=1) must be a superset of the cover"


def test_h3_geom_expand_cells_returns_integers():
    """All returned cell ids are Python ints (BIGINT-compatible)."""
    c = _nyc_cell()
    cover, core = _border_cover_core(c)
    result = _h3.geom_expand_cells(
        "ring",
        1,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=set(),
        holes_core=set(),
    )
    assert all(
        isinstance(cell, int) for cell in result
    ), f"All cell ids should be int; got types: {set(type(x) for x in result)}"


def test_h3_geom_expand_cells_holed_holes_core_nonempty():
    """Holed polygon: holes_core non-empty passes correctly to the engine.

    Fixture: donut-shaped coverage.
        cover       = disk(c, 2) — 19 cells overlapping the polygon
        core        = disk(c, 1) — 7 cells FULLY inside (not on outer border)
        holes_cover = {c}        — center cell overlaps the hole
        holes_core  = {c}        — center cell is fully inside the hole

    p_border = cover − core = ring(c, 2) — 12 outer cells (the actual outer frontier).
    boundary-out k=1 from ring(c, 2) grows to ring(c, 3) → result > 19 cells.
    """
    c = _nyc_cell(res=5)
    cover = _disk_ints(c, 2)  # 19 cells
    core = _disk_ints(c, 1)  # 7 cells (inner disk = "fully inside" the polygon)
    holes_cover = {c}  # center cell is in the hole
    holes_core = {c}  # center cell is fully inside the hole

    result = _h3.geom_expand_cells(
        "ring",
        1,
        "boundary-out",
        cover=cover,
        core=core,
        holes_cover=holes_cover,
        holes_core=holes_core,
    )
    # p_border = ring(c, 2) is the outer frontier; boundary-out k=1 adds ring(c,3)
    assert len(result) > len(cover), (
        f"boundary-out k=1 should grow beyond the covering set; "
        f"result={len(result)}, cover={len(cover)}"
    )
    # holes_core was non-empty — verify it was wired in (engine didn't crash)
    assert isinstance(result, set) and all(isinstance(x, int) for x in result)


def test_h3_geom_expand_cells_hole_in_fills_hole():
    """hole-in k=1 from h_border grows INWARD through hole cells.

    Constructs a genuine holes_cover/holes_core from the h3 lib:
        center cell c = the hole's single interior cell (h_core = {c})
        h_border = cells that overlap the hole but aren't fully inside
                 = holes_cover - holes_core = ring(c, 1) (the 6 outer hole cells)

    With mode="hole-in", frontier = h_border = ring(c, 1); admit = n in h_core = {c}.
    k=1 dilation: from ring(c,1) look for neighbors in h_core → center c → result = {c}.

    Asserts:
      - result contains h_core cells (hole was filled)
      - result is DISJOINT from p_core (inward fill stays inside the hole)
    """
    c = _nyc_cell(res=7)
    # Hole: center cell is h_core, surrounding ring is h_border
    holes_core = {c}
    holes_cover = _disk_ints(c, 1)  # 7 cells; h_border = ring(c,1)

    # Outer polygon covers disk(c, 2); core = ring(c, 2) (doesn't include hole area)
    cover = _disk_ints(c, 2)  # 19 cells
    p_core = _ring_ints(c, 2)  # 12 cells: ring-2 only (outer ring is "fully inside")
    # p_core is disjoint from holes_core by construction (ring-2 vs center)
    assert c not in p_core, "Fixture: p_core must not contain the hole cell"

    result = _h3.geom_expand_cells(
        "ring",
        1,
        "hole-in",
        cover=cover,
        core=p_core,
        holes_cover=holes_cover,
        holes_core=holes_core,
    )
    # The hole-in result should contain the h_core cell (filled into the hole)
    assert (
        holes_core <= result
    ), f"hole-in should fill into h_core; result={result}, h_core={holes_core}"
    # Result must be DISJOINT from p_core (inward fill stays inside the hole)
    assert result.isdisjoint(
        p_core
    ), f"hole-in result must not bleed into p_core; overlap={result & p_core}"
    # All results are ints
    assert all(isinstance(x, int) for x in result)


def test_h3_geom_expand_cells_invalid_mode_raises():
    """Unknown mode raises ValueError (delegated from _dilate.mode_setup)."""
    c = _nyc_cell()
    with pytest.raises(ValueError, match="unknown mode"):
        _h3.geom_expand_cells(
            "ring",
            1,
            "BOGUS",
            cover={c},
            core=set(),
            holes_cover=set(),
            holes_core=set(),
        )


# ---------------------------------------------------------------------------
# _h3_geomkring / _h3_geomkloop UDF wrappers
# ---------------------------------------------------------------------------


def _cover_list(c_int: int, k_ring: int = 1) -> list:
    """cover = grid_disk(c, k_ring); core = [c] (realistic fixture)."""
    return sorted(_disk_ints(c_int, k_ring))


def test_h3_geomkring_udf_boundary_out_k1():
    """_h3_geomkring UDF boundary-out k=1 from single-border-cell cover grows to disk(c,1).

    cover=[c] (no core → border cell) → frontier={c} → k=1 grows to all neighbors.
    Expected result: h3.grid_disk(c, 1) as sorted ints.
    """
    from databricks.labs.gbx.pygx.functions import _h3_geomkring

    c = _nyc_cell()
    cover = [c]
    core: list = []  # no core → c is a border cell
    result = _h3_geomkring(cover, core, [], [], 1, "boundary-out")
    expected = sorted(_disk_ints(c, 1))
    assert result == expected


def test_h3_geomkloop_udf_is_ring_diff():
    """_h3_geomkloop UDF: loop(k) == ring(k) − ring(k−1)."""
    from databricks.labs.gbx.pygx.functions import _h3_geomkloop, _h3_geomkring

    c = _nyc_cell()
    cover = sorted(_disk_ints(c, 1))  # 7-cell disk
    core = [c]  # center only

    r2 = set(_h3_geomkring(cover, core, [], [], 2))
    r1 = set(_h3_geomkring(cover, core, [], [], 1))
    loop2 = set(_h3_geomkloop(cover, core, [], [], 2))
    assert loop2 == (r2 - r1)


def test_h3_geomkring_udf_none_cover_returns_none():
    """None cover returns None (NULL propagation)."""
    from databricks.labs.gbx.pygx.functions import _h3_geomkring

    assert _h3_geomkring(None, [], [], [], 1) is None


def test_h3_geomkring_udf_none_k_returns_none():
    """None k returns None (NULL propagation)."""
    from databricks.labs.gbx.pygx.functions import _h3_geomkring

    c = _nyc_cell()
    assert _h3_geomkring([c], [], [], [], None) is None


def test_h3_geomkring_udf_bad_mode_raises():
    """Unknown mode raises ValueError from _dilate_check_mode."""
    from databricks.labs.gbx.pygx.functions import _h3_geomkring

    c = _nyc_cell()
    with pytest.raises(ValueError, match="unknown mode"):
        _h3_geomkring([c], [], [], [], 1, "BOGUS")


def test_h3_geomkring_udf_returns_sorted_ints():
    """UDF returns sorted list of ints."""
    from databricks.labs.gbx.pygx.functions import _h3_geomkring

    c = _nyc_cell()
    cover = [c]
    core: list = []
    result = _h3_geomkring(cover, core, [], [], 1)
    assert result == sorted(result)
    assert all(isinstance(x, int) for x in result)


# ---------------------------------------------------------------------------
# Integration tests (require Databricks + product h3_* SQL functions)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_h3_product_polyfillash3_matches_h3_lib(spark):
    """Cross-check: product h3_polyfillash3 cell ids == h3.polygon_to_cells on same geom.

    VERIFY: exact product function name (h3_polyfillash3) and containment
    semantics (contained vs overlap) must be confirmed at integration time.
    Adjust if product uses h3_coverash3 or a different name.
    """
    pytest.skip(
        "Integration test: requires Databricks session with product h3_polyfillash3. "
        "Run with --with-integration on a Databricks cluster. "
        "VERIFY product function name h3_polyfillash3 and containment semantics."
    )

    from shapely import to_wkb
    from shapely.geometry import box

    geom_wkb = to_wkb(box(-73.99, 40.71, -73.95, 40.75))
    res = 9

    # Product polyfill via SQL
    # VERIFY: h3_polyfillash3 vs h3_coverash3 — adjust to match actual Databricks SQL
    row = spark.sql(
        f"SELECT h3_polyfillash3(ST_GeomFromWKB(unhex('{geom_wkb.hex()}')), {res}) AS cells"
    ).collect()[0]
    product_cells = set(row["cells"] or [])

    # h3 lib polygon_to_cells (contained semantics)
    geom_shp = box(-73.99, 40.71, -73.95, 40.75)
    # VERIFY: h3_polyfillash3 uses contained semantics → h3.polygon_to_cells
    lib_cells_ints = {
        int(c, 16)
        for c in h3.polygon_to_cells(h3.H3Polygon(list(geom_shp.exterior.coords)), res)
    }
    assert (
        product_cells == lib_cells_ints
    ), f"Product polyfill ({len(product_cells)}) != h3 lib ({len(lib_cells_ints)})"


@pytest.mark.integration
def test_h3_geomkring_boundary_out_expands_beyond_cover(spark):
    """Cross-check: gbx_h3_geomkring boundary-out k=1 genuinely expands beyond cover.

    Uses h3_coverash3 (overlap → p_cover) and h3_polyfillash3 (contained → p_core)
    for a REAL frontier: p_border = cover − core is non-empty for any real polygon,
    so boundary-out dilation actually adds cells beyond the covering set.

    Asserts:
      1. gbx_set >= cover_set (result contains the original covering set)
      2. gbx_set > cover_set (expansion actually happened — new cells added)

    VERIFY: product function names h3_coverash3, h3_polyfillash3, h3_kring,
    and containment semantics must be confirmed at integration time.
    """
    pytest.skip(
        "Integration test: requires Databricks session with product h3_* functions and "
        "gbx_h3_geomkring registered. Run with --with-integration on a Databricks cluster. "
        "VERIFY product function names: h3_coverash3 (overlap), h3_polyfillash3 (contained)."
    )

    from shapely import to_wkb
    from shapely.geometry import box

    from databricks.labs.gbx.pygx.functions import register

    register(spark)

    geom_wkb = to_wkb(box(-73.99, 40.71, -73.95, 40.75)).hex()
    res = 9

    # Product cover (overlap) and core (contained) — real frontier between them
    # VERIFY: h3_coverash3 = overlap semantics; h3_polyfillash3 = contained semantics
    cover_result = spark.sql(f"""
        SELECT h3_coverash3(ST_GeomFromWKB(unhex('{geom_wkb}')), {res}) AS cover,
               h3_polyfillash3(ST_GeomFromWKB(unhex('{geom_wkb}')), {res}) AS core
    """).collect()[0]
    cover_set = set(cover_result["cover"] or [])
    core_set = set(cover_result["core"] or [])

    # Sanity: frontier must be non-empty for expansion to happen
    border = cover_set - core_set
    assert len(border) > 0, (
        "VERIFY product names — cover and core are identical; "
        "h3_coverash3 may have different semantics than expected"
    )

    # GeoBrix boundary-out k=1: cover=h3_coverash3, core=h3_polyfillash3
    # (real frontier → real expansion)
    gbx_result = spark.sql(f"""
        SELECT gbx_h3_geomkring(
            h3_coverash3(ST_GeomFromWKB(unhex('{geom_wkb}')), {res}),
            h3_polyfillash3(ST_GeomFromWKB(unhex('{geom_wkb}')), {res}),
            array(),
            array(),
            1,
            'boundary-out'
        ) AS cells
        -- VERIFY exact product function names h3_coverash3, h3_polyfillash3
    """).collect()[0]["cells"]
    gbx_set = set(gbx_result or [])

    # 1. Result contains the original covering set (no cells lost)
    assert gbx_set >= cover_set, (
        f"boundary-out result must contain all cover cells; "
        f"missing: {cover_set - gbx_set}"
    )
    # 2. Result is strictly larger (expansion actually happened)
    assert len(gbx_set) > len(cover_set), (
        f"boundary-out k=1 must add cells beyond the cover; "
        f"gbx={len(gbx_set)}, cover={len(cover_set)}"
    )
