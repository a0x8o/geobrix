"""Coverage-param engine tests (0.5.1 re-cut) — the LIGHT-ENGINE correctness heart.

Locked design: .superpowers/specs/2026-09-14-geom-aware-boundary-perimeter-fix.md
("COVERAGE PARAM — LOCKED").

Three classify bases per region P/S/H:
  - cover    (``*_cover``)    — overlap                       -> coverage="coveras" (DEFAULT)
  - centroid (``*_centroid``) — cell centroid inside region   -> coverage="polyfill"
  - core     (``*_core``)     — cell fully contained          -> coverage="core"
Nested: cover ⊇ centroid ⊇ core.

The ``coverage`` value selects which basis drives the seed (perimeter), the admit
bound, and boundary-ring inclusion.  boundary-out has k0 = ∅ (the geom/covering set
is EXCLUDED — only the outward band is returned).

Exercised at the engine level (synthetic grid) and on two real grids (custom,
quadbin).  h3 is covered by the geom_expand signature; heavy parity diverges until
the heavy mirror lands (out of scope for this pass).
"""

import pytest
from shapely.geometry import LineString, Point, Polygon, box

from databricks.labs.gbx.pygx import _dilate as D

# ---------------------------------------------------------------------------
# Synthetic 8-connected grid: cell id = (x<<20)|y; cell = unit box at (x,y).
# ---------------------------------------------------------------------------


def _cid(x, y):
    return (x << 20) | y


def _xy(c):
    return (c >> 20, c & 0xFFFFF)


def _neighbors(c):
    x, y = _xy(c)
    return [
        _cid(x + dx, y + dy)
        for dx in (-1, 0, 1)
        for dy in (-1, 0, 1)
        if not (dx == 0 and dy == 0) and 0 <= x + dx and 0 <= y + dy
    ]


def _cell_geom(c):
    x, y = _xy(c)
    return box(x, y, x + 1, y + 1)


def _wide_polyfill(g, res):
    return [_cid(x, y) for x in range(-3, 14) for y in range(-3, 14)]


@pytest.fixture
def holed_cls():
    """Holed solid offset 0.3 from the grid → strict cover ⊃ centroid ⊃ core.

    outer=box(0.3,0.3,9.7,9.7), hole=box(3.3,3.3,6.7,6.7).  The 0.3 inset makes the
    exterior + hole boundaries cut cells such that the three bases are strictly
    nested (verified: |p_core|=48 < |p_centroid|=84 < |p_cover|=96).
    """
    outer = box(0.3, 0.3, 9.7, 9.7)
    hole = box(3.3, 3.3, 6.7, 6.7)
    geom = Polygon(outer.exterior.coords, [list(hole.exterior.coords)])
    return D.classify(geom, 1, _wide_polyfill, _cell_geom)


@pytest.fixture
def point_cls():
    """Point strictly inside cell (5,5); coveras → 1 cover cell, centroid/core ∅."""
    geom = Point(5.5, 5.5)
    return D.classify(geom, 1, lambda g, r: [_cid(5, 5)], _cell_geom)


@pytest.fixture
def line_cls():
    """Horizontal line crossing cells (2,5)..(5,5); coveras → 4 cover cells."""
    geom = LineString([(2.5, 5.5), (5.5, 5.5)])

    def polyfill(g, res):
        minx, miny, maxx, maxy = g.bounds
        return [
            _cid(x, y)
            for x in range(int(minx) - 1, int(maxx) + 2)
            for y in range(int(miny) - 1, int(maxy) + 2)
        ]

    return D.classify(geom, 1, polyfill, _cell_geom)


def _region(cls, coverage, letter):
    """Belongs-to set for region letter ('p'/'s'/'h') under a coverage value."""
    basis = {"coveras": "cover", "polyfill": "centroid", "core": "core"}[coverage]
    return getattr(cls, f"{letter}_{basis}")


# ---------------------------------------------------------------------------
# classify — three bases and their nesting
# ---------------------------------------------------------------------------


def test_classification_exposes_centroid_basis(holed_cls):
    for attr in ("p_centroid", "s_centroid", "h_centroid"):
        assert hasattr(holed_cls, attr), f"Classification missing {attr}"


def test_three_basis_nesting_p_s_h(holed_cls):
    c = holed_cls
    assert c.p_core <= c.p_centroid <= c.p_cover
    assert c.s_core <= c.s_centroid <= c.s_cover
    assert c.h_core <= c.h_centroid <= c.h_cover
    # P has a genuine three-way gap for this fixture.
    assert c.p_core < c.p_centroid < c.p_cover, "expected strict cover⊃centroid⊃core"


# ---------------------------------------------------------------------------
# coverage validation + default
# ---------------------------------------------------------------------------


def test_coverage_default_is_coveras(holed_cls):
    for mode in D.MODES:
        default = D.geom_expand("ring", 2, mode, holed_cls, _neighbors)
        coveras = D.geom_expand("ring", 2, mode, holed_cls, _neighbors, "coveras")
        assert default == coveras, f"mode={mode}: default must equal coveras"


def test_coverage_invalid_raises(holed_cls):
    with pytest.raises(ValueError):
        D.geom_expand("ring", 1, "boundary-out", holed_cls, _neighbors, "nonsense")
    with pytest.raises(ValueError):
        D.mode_setup("boundary-out", holed_cls, _neighbors, coverage="nonsense")


# ---------------------------------------------------------------------------
# boundary-out — k0 = ∅ (geom EXCLUDED) under every coverage
# ---------------------------------------------------------------------------


def test_boundary_out_k0_empty_every_coverage(holed_cls):
    for cov in D.COVERAGE:
        assert (
            D.geom_expand("loop", 0, "boundary-out", holed_cls, _neighbors, cov)
            == set()
        ), f"boundary-out k0 must be ∅ under coverage={cov}"
        assert (
            D.geom_expand("ring", 0, "boundary-out", holed_cls, _neighbors, cov)
            == set()
        ), f"boundary-out ring(0) must be ∅ under coverage={cov}"


def test_boundary_out_excludes_geom_every_coverage(holed_cls):
    for cov in D.COVERAGE:
        r = D.geom_expand("ring", 1, "boundary-out", holed_cls, _neighbors, cov)
        region_p = _region(holed_cls, cov, "p")
        assert r.isdisjoint(region_p), (
            f"boundary-out must exclude the geom (region P) under coverage={cov}; "
            f"overlap={sorted(r & region_p)[:5]}"
        )
        # coveras: outward band is non-empty for a real solid.
        if cov == "coveras":
            assert r, "boundary-out k=1 under coveras must be a non-empty outward band"


# ---------------------------------------------------------------------------
# boundary-in / ignore-holes — seed = outer perimeter of region S under coverage
# ---------------------------------------------------------------------------


def test_boundary_in_k0_is_region_perimeter_every_coverage(holed_cls):
    for cov in D.COVERAGE:
        region_s = _region(holed_cls, cov, "s")
        expected = D.outer_perimeter(region_s, _neighbors)
        for mode in ("boundary-in", "boundary-in-ignore-holes"):
            k0 = D.geom_expand("loop", 0, mode, holed_cls, _neighbors, cov)
            assert k0 == expected, f"{mode} k0 must equal outer_perimeter(S_{cov})"


def test_boundary_in_respects_holes_coveras(holed_cls):
    r = D.geom_expand("ring", 3, "boundary-in", holed_cls, _neighbors, "coveras")
    assert r.isdisjoint(holed_cls.h_core), "boundary-in must not enter the hole core"


# ---------------------------------------------------------------------------
# 3 coverage × 6 modes matrix — no crash + result within region envelope
# ---------------------------------------------------------------------------


def test_coverage_mode_matrix_envelopes(holed_cls):
    for cov in D.COVERAGE:
        region_p = _region(holed_cls, cov, "p")
        region_s = _region(holed_cls, cov, "s")
        region_h = _region(holed_cls, cov, "h")
        for mode in D.MODES:
            r = D.geom_expand("ring", 2, mode, holed_cls, _neighbors, cov)
            # loop(k) == ring(k) − ring(k−1) invariant holds for every cell/mode.
            r3 = D.geom_expand("ring", 3, mode, holed_cls, _neighbors, cov)
            r2 = D.geom_expand("ring", 2, mode, holed_cls, _neighbors, cov)
            loop3 = D.geom_expand("loop", 3, mode, holed_cls, _neighbors, cov)
            assert loop3 == (r3 - r2), f"cov={cov} mode={mode}: loop != ring diff"
            # per-mode envelope
            if mode == "boundary-out":
                assert r.isdisjoint(region_p)
            elif mode == "boundary-in":
                assert r <= region_p, f"cov={cov} boundary-in escaped region P"
            elif mode == "boundary-in-ignore-holes":
                assert r <= region_s, f"cov={cov} ignore-holes escaped region S"
            elif mode == "hole-in":
                assert r <= region_h, f"cov={cov} hole-in escaped region H"
            elif mode == "hole-out":
                assert r <= region_p, f"cov={cov} hole-out escaped region P"


# ---------------------------------------------------------------------------
# Point / line under each coverage
# ---------------------------------------------------------------------------


def test_point_coveras_supported(point_cls):
    # cover basis: exactly the containing cell
    assert len(point_cls.p_cover) == 1
    # boundary-in k0 = perimeter of s_cover = the single covering cell (it has
    # outside neighbours) → the covering cell is retrievable under coveras.
    k0_in = D.geom_expand("loop", 0, "boundary-in", point_cls, _neighbors, "coveras")
    assert (
        k0_in == point_cls.p_cover
    ), "point covering cell must appear (boundary-in k0)"
    # boundary-out k=1 = outward ring, EXCLUDING the covering cell (k0=∅).
    r1 = D.geom_expand("ring", 1, "boundary-out", point_cls, _neighbors, "coveras")
    assert r1, "boundary-out k=1 on a point must be a non-empty outward ring"
    assert r1.isdisjoint(point_cls.p_cover), "boundary-out excludes the point's cell"


def test_point_polyfill_core_empty(point_cls):
    assert point_cls.p_centroid == set()
    assert point_cls.p_core == set()
    for cov in ("polyfill", "core"):
        for mode in D.MODES:
            r = D.geom_expand("ring", 2, mode, point_cls, _neighbors, cov)
            assert r == set(), f"point coverage={cov} mode={mode} must be empty"


def test_line_coveras_supported(line_cls):
    assert (
        len(line_cls.p_cover) == 4
    ), f"line should cross 4 cells; got {len(line_cls.p_cover)}"
    k0_in = D.geom_expand("loop", 0, "boundary-in", line_cls, _neighbors, "coveras")
    assert k0_in == line_cls.p_cover, "all crossed cells must appear (boundary-in k0)"
    r1 = D.geom_expand("ring", 1, "boundary-out", line_cls, _neighbors, "coveras")
    assert r1, "boundary-out k=1 on a line must be a non-empty outward band"
    assert r1.isdisjoint(line_cls.p_cover), "boundary-out excludes the line's cells"


def test_line_polyfill_core_empty(line_cls):
    assert line_cls.p_centroid == set()
    assert line_cls.p_core == set()
    for cov in ("polyfill", "core"):
        for mode in D.MODES:
            r = D.geom_expand("ring", 2, mode, line_cls, _neighbors, cov)
            assert r == set(), f"line coverage={cov} mode={mode} must be empty"


# ===========================================================================
# Grid-level: custom + quadbin (2 real grids)
# ===========================================================================

from shapely import to_wkb  # noqa: E402

from databricks.labs.gbx.pygx import _custom, _quadbin  # noqa: E402
from databricks.labs.gbx.pygx._custom import CustomGridConf  # noqa: E402


def _custom_conf():
    return CustomGridConf(
        bound_x_min=0,
        bound_x_max=1_000_000,
        bound_y_min=0,
        bound_y_max=1_000_000,
        cell_splits=2,
        root_cell_size_x=1000,
        root_cell_size_y=1000,
        srid=-1,
    )


_CUSTOM_GEOM = box(530000, 180000, 535000, 185000)
_CUSTOM_RES = 0
_QUADBIN_GEOM = box(-73.99, 40.71, -73.95, 40.75)
_QUADBIN_RES = 12


# ---- custom ----------------------------------------------------------------


def test_custom_coverage_default_is_coveras():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    for mode in D.MODES:
        default = set(_custom.geometry_k_ring(conf, g, _CUSTOM_RES, 1, mode=mode))
        coveras = set(
            _custom.geometry_k_ring(
                conf, g, _CUSTOM_RES, 1, mode=mode, coverage="coveras"
            )
        )
        assert default == coveras, f"custom mode={mode}: default != coveras"


def test_custom_boundary_out_k0_empty_every_coverage():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    for cov in D.COVERAGE:
        assert (
            _custom.geometry_k_ring(
                conf, g, _CUSTOM_RES, 0, mode="boundary-out", coverage=cov
            )
            == []
        ), f"custom boundary-out k0 must be empty under coverage={cov}"


def test_custom_boundary_out_excludes_cover():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    cover = set(_custom.polyfill(conf, _CUSTOM_GEOM, _CUSTOM_RES))
    r1 = set(_custom.geometry_k_ring(conf, g, _CUSTOM_RES, 1, mode="boundary-out"))
    assert r1, "boundary-out k=1 must be a non-empty outward band"
    assert r1.isdisjoint(cover), "boundary-out must exclude the covering set"


def test_custom_matrix_no_crash_all_coverage():
    conf, g = _custom_conf(), bytes(to_wkb(_CUSTOM_GEOM))
    for cov in D.COVERAGE:
        for mode in D.MODES:
            cells = _custom.geometry_k_ring(
                conf, g, _CUSTOM_RES, 2, mode=mode, coverage=cov
            )
            assert all(isinstance(c, int) for c in cells)


def test_custom_point_coveras_vs_polyfill_core():
    conf = _custom_conf()
    pt = bytes(to_wkb(Point(530500, 180500)))
    # coveras: boundary-out k=1 outward ring is non-empty
    r_cov = _custom.geometry_k_ring(
        conf, pt, _CUSTOM_RES, 1, mode="boundary-out", coverage="coveras"
    )
    assert r_cov, "custom point under coveras (boundary-out k=1) must be non-empty"
    # polyfill/core: point has no centroid-in / contained cell → empty
    for cov in ("polyfill", "core"):
        for mode in D.MODES:
            assert (
                _custom.geometry_k_ring(
                    conf, pt, _CUSTOM_RES, 2, mode=mode, coverage=cov
                )
                == []
            ), f"custom point coverage={cov} mode={mode} must be empty"


# ---- quadbin ---------------------------------------------------------------


def test_quadbin_coverage_default_is_coveras():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    for mode in D.MODES:
        default = set(_quadbin.geometry_k_ring(g, _QUADBIN_RES, 1, mode=mode))
        coveras = set(
            _quadbin.geometry_k_ring(g, _QUADBIN_RES, 1, mode=mode, coverage="coveras")
        )
        assert default == coveras, f"quadbin mode={mode}: default != coveras"


def test_quadbin_boundary_out_k0_empty_every_coverage():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    for cov in D.COVERAGE:
        assert (
            _quadbin.geometry_k_ring(
                g, _QUADBIN_RES, 0, mode="boundary-out", coverage=cov
            )
            == []
        ), f"quadbin boundary-out k0 must be empty under coverage={cov}"


def test_quadbin_boundary_out_excludes_cover():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    cover = set(_quadbin.polyfill(g, _QUADBIN_RES))
    r1 = set(_quadbin.geometry_k_ring(g, _QUADBIN_RES, 1, mode="boundary-out"))
    assert r1, "quadbin boundary-out k=1 must be a non-empty outward band"
    assert r1.isdisjoint(cover), "quadbin boundary-out must exclude the covering set"


def test_quadbin_matrix_no_crash_all_coverage():
    g = bytes(to_wkb(_QUADBIN_GEOM))
    for cov in D.COVERAGE:
        for mode in D.MODES:
            cells = _quadbin.geometry_k_ring(
                g, _QUADBIN_RES, 2, mode=mode, coverage=cov
            )
            assert all(isinstance(c, int) for c in cells)
