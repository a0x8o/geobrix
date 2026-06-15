"""Light (pygx) vs heavy (gridx.custom) EXACT cross-tier parity for the 7 custom-grid functions.

The bar (per the pygx custom-gridding spec) is EXACT, not tolerant, for cell ids / sets:

- **Cell IDs / sets are bit-exact** (custom ids are BIGINT): ``pointascell``
  (same BIGINT), ``polyfill`` (sorted cell-set equality), ``kring`` (sorted set
  equality). The cell math is a verbatim port of ``CustomGridSystem.scala`` /
  ``GridConf.scala``, so light and heavy must produce bit-identical ids.
- **Geometry WKB within 1e-6**: ``cellaswkb`` / ``cellaswkt`` / ``centroid`` —
  decode both tiers (shapely ``from_wkb`` / ``from_wkt``). Custom carries **NO
  SRID** (heavy ``JTS.toWKB``, the 2D no-SRID variant; the grid's ``srid`` field
  is metadata only and is NEVER stamped into output geometry), so assert
  ``get_srid == 0`` in BOTH tiers and compare the normalized geometries within
  1e-6 via ``equals_exact``.

Both tiers register the SAME ``gbx_custom_*`` SQL names. Light registers PySpark
UDF/pandas-UDFs; heavy registers JVM expressions. We collect EVERY light result
first (via SQL on the shared names), then register heavy (which OVERWRITES the
``gbx_custom_*`` catalog entries) and collect the heavy results (same pattern as
``test_parity_bng.py`` / ``test_parity_quadbin.py``). The grid spec is built with
``gbx_custom_grid(...)`` so the SAME validated STRUCT flows into both tiers.

Coverage (Task 9):
  * EXACT cell id (``pointascell``) and cell set (``polyfill``, ``kring``).
  * Edge cells: the origin cell ``(100, 100)`` and a max-corner cell near
    ``(999900, 999900)`` — the latter exercises the ``kring`` upper clamp
    ``min(pos+k, totalCells)`` that the unit tests never reached. If light and
    heavy DIVERGE there, the verbatim port and heavy disagree — that is a real
    FINDING, not a test bug.
  * Multi-resolution: ``cell_splits`` 2 (res 0 + a deeper res) and 4 (res 0).
  * srid-independence: a grid WITH (``srid=27700``) and WITHOUT (``srid=-1``) a
    CRS must yield identical cell ids (srid is metadata only).
  * All-4-encodings geom input (WKB/EWKB/WKT/EWKT) for ``pointascell`` and
    ``polyfill`` — identical results within each tier AND light == heavy.
  * Y-NaN lock-in (Resolved decision 3): light ``point_to_cell_id`` rejects a
    NaN Y, and heavy ``gbx_custom_pointascell`` rejects a NaN-Y point — locking
    the CG-T8 fix cross-tier.

Heavy requires the geobrix JAR (Scala/JTS). The JAR is present in the geobrix-dev
Docker container; this test auto-skips when the JAR is not staged under
``python/geobrix/lib/``. The staged JAR MUST contain the CG-T8 NaN-Y fix
(rebuild + restage if stale) for the Y-NaN heavy lock-in to pass.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_parity_custom.py \\
        --with-integration --log parity-custom.log
"""

import logging
from pathlib import Path

import pytest
from shapely import equals_exact, from_wkb, from_wkt, get_srid, set_srid
from shapely import to_wkb as _to_wkb
from shapely.geometry import Point as _Point
from shapely.geometry import box as _box

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pygx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: it has no effect if a JVM (and therefore
    # a Spark session) is already live in this process. Skip instead of producing a
    # misleading failure when another test suite already created a JAR-free session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run this test in isolation: "
                "gbx:test:python --path python/geobrix/test/pygx/test_parity_custom.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-custom-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


# --- geometry helper --------------------------------------------------------------------------


def _assert_geom_parity(light_blob, heavy_blob, ctx="", decoder=from_wkb):
    """Decode both blobs; assert SRID 0 (custom has none) in both + coords equal 1e-6."""
    assert light_blob is not None, f"{ctx}: light geometry is None"
    assert heavy_blob is not None, f"{ctx}: heavy geometry is None"
    lg = decoder(bytes(light_blob)) if decoder is from_wkb else decoder(light_blob)
    hg = decoder(bytes(heavy_blob)) if decoder is from_wkb else decoder(heavy_blob)
    # Custom WKB carries NO SRID (heavy JTS.toWKB is 2D, no SRID stamp).
    assert (
        get_srid(lg) == 0
    ), f"{ctx}: light SRID {get_srid(lg)} != 0 (custom has no SRID)"
    assert (
        get_srid(hg) == 0
    ), f"{ctx}: heavy SRID {get_srid(hg)} != 0 (custom has no SRID)"
    # normalize() canonicalizes vertex/ring order so equals_exact compares shape,
    # not winding/start-vertex. 1e-6 from the spec.
    assert equals_exact(
        lg.normalize(), hg.normalize(), 1e-6
    ), f"{ctx}: geometry mismatch beyond 1e-6\n  light={lg.wkt}\n  heavy={hg.wkt}"


# Deterministic fixtures -----------------------------------------------------------------------
# A 0..1,000,000 grid (the doc SQL example grid): cell_splits=2, root cells 1000x1000.
# srid=27700 mirrors a real BNG-extent CRS, but the cell math is srid-independent.
_GRID_27700 = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)"
_GRID_NOSRID = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, -1)"
# cell_splits=4 -> max_resolution 14, root cells 1000x1000 (250 of them, ceil(1e6/4000)).
_GRID_SPLIT4 = "gbx_custom_grid(0, 1000000, 0, 1000000, 4, 4000, 4000, 0)"

# London-ish interior point (EPSG:27700-style easting/northing inside the grid).
_PX, _PY = 530000.0, 180000.0
# Origin cell (posX=posY=0) and a max-corner cell near totalCells-1 (posX=posY=999 at
# res 0). The max-corner cell drives the kring UPPER clamp min(pos+k, totalCells).
_ORIGIN_PT = (100.0, 100.0)  # cell (0, 0)
_MAXCORNER_PT = (999900.0, 999900.0)  # cell (999, 999) at res 0 (totalCells = 1000)
# A 3000x3000 box aligned to the grid -> 9 cell centers fall inside at res 0.
_BOX = _box(530000.0, 180000.0, 533000.0, 183000.0)


def _wkb(geom):
    return bytearray(_to_wkb(geom))


# --- the full custom-grid parity sweep --------------------------------------------------------


def test_custom_full_parity(spark_with_jar):
    """All 7 custom functions, light vs heavy, in one session (light first, then heavy).

    Covers pointascell (exact BIGINT), polyfill/kring (exact set incl. the kring
    UPPER-edge clamp), and cellaswkb/cellaswkt/centroid (geometry + SRID==0), at
    multiple resolutions and cell_splits, on a grid WITH and WITHOUT a srid.
    """
    spark = spark_with_jar

    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    box_wkb = _wkb(_BOX)
    df = spark.createDataFrame([(box_wkb,)], "geom binary")
    df.createOrReplaceTempView("custom_src")

    def collect_all():
        """Collect every parity-relevant result against the currently-registered
        gbx_custom_* SQL names (light first, then heavy overwrites)."""
        out = {}

        # --- pointascell: interior + edge cells, multi-res, multi-split, srid-indep ---
        # (grid_sql, x, y, res) probes.
        pac_probes = [
            (_GRID_27700, _PX, _PY, 0),
            (_GRID_27700, _PX, _PY, 5),  # deeper res, split=2
            (_GRID_27700, *_ORIGIN_PT, 0),  # origin cell
            (_GRID_27700, *_MAXCORNER_PT, 0),  # max-corner cell
            (_GRID_NOSRID, _PX, _PY, 0),  # same grid, no srid -> same id
            (_GRID_SPLIT4, _PX, _PY, 0),  # cell_splits=4
            (_GRID_SPLIT4, _PX, _PY, 3),  # cell_splits=4, deeper res
        ]
        pac = []
        for grid_sql, x, y, res in pac_probes:
            r = spark.sql(
                f"SELECT gbx_custom_pointascell('POINT({x} {y})', {grid_sql}, {res}) AS c"
            ).collect()[0]
            pac.append(r["c"])
        out["pac"] = pac

        # --- cell geometry (cellaswkb / cellaswkt / centroid), no SRID ---
        seed = pac[0]  # interior cell at res 0
        geo = spark.sql(
            f"SELECT gbx_custom_cellaswkb({seed}L, {_GRID_27700}) AS wkb, "
            f"gbx_custom_cellaswkt({seed}L, {_GRID_27700}) AS wkt, "
            f"gbx_custom_centroid({seed}L, {_GRID_27700}) AS cen"
        ).collect()[0]
        out["aswkb"] = geo["wkb"]
        out["aswkt"] = geo["wkt"]
        out["centroid"] = geo["cen"]

        # --- polyfill: exact cell-set, multi-res / multi-split ---
        pf = []
        for grid_sql, res in [(_GRID_27700, 0), (_GRID_SPLIT4, 0)]:
            r = spark.sql(
                f"SELECT gbx_custom_polyfill(geom, {grid_sql}, {res}) AS pf FROM custom_src"
            ).collect()[0]
            pf.append(sorted(r["pf"]))
        out["pf"] = pf

        # --- kring: interior, origin (lower clamp), max-corner (UPPER clamp) ---
        # Build the three seed cells via pointascell so inputs are tier-derived,
        # but the SQL pointascell already produced them in `pac`: interior=pac[0],
        # origin=pac[2], max-corner=pac[3].
        kr = {}
        for label, seed_cell, k in [
            ("interior", pac[0], 1),
            ("origin", pac[2], 1),
            ("maxcorner", pac[3], 1),
            ("maxcorner_k2", pac[3], 2),
        ]:
            r = spark.sql(
                f"SELECT gbx_custom_kring({seed_cell}L, {_GRID_27700}, {k}) AS r"
            ).collect()[0]
            kr[label] = sorted(r["r"])
        out["kr"] = kr
        return out

    # ---- LIGHT first (heavy register OVERWRITES the gbx_custom_* SQL names) ----
    gx.register(spark)
    light = collect_all()

    # ---- HEAVY (overwrites the catalog names) ----
    hx.register(spark)
    heavy = collect_all()

    # === cell-ID parity (EXACT) ===
    assert light["pac"] == heavy["pac"], (
        "pointascell BIGINT mismatch (interior/edge/multi-res/multi-split/srid):\n"
        f"  light={light['pac']}\n  heavy={heavy['pac']}"
    )
    # srid-independence: probe 0 (srid=27700) and probe 4 (srid=-1) are the same grid
    # geometry -> identical cell id within each tier.
    assert light["pac"][0] == light["pac"][4], (
        "srid must be metadata only: srid=27700 vs srid=-1 gave different light cell ids "
        f"({light['pac'][0]} vs {light['pac'][4]})"
    )
    assert heavy["pac"][0] == heavy["pac"][4], (
        "srid must be metadata only: srid=27700 vs srid=-1 gave different heavy cell ids "
        f"({heavy['pac'][0]} vs {heavy['pac'][4]})"
    )

    # === polyfill cell-set parity (EXACT) ===
    for i, (lpf, hpf) in enumerate(zip(light["pf"], heavy["pf"])):
        assert lpf == hpf, (
            f"polyfill cell-set mismatch (probe {i}):\n  light={lpf}\n  heavy={hpf}\n"
            f"  light_only={sorted(set(lpf) - set(hpf))} "
            f"heavy_only={sorted(set(hpf) - set(lpf))}"
        )

    # === kring cell-set parity (EXACT) — incl. the UPPER-edge clamp ===
    for label in ("interior", "origin", "maxcorner", "maxcorner_k2"):
        lkr, hkr = light["kr"][label], heavy["kr"][label]
        assert lkr == hkr, (
            f"kring cell-set mismatch ({label}) — the verbatim port and heavy DISAGREE; "
            f"INVESTIGATE (do not weaken the test):\n  light={lkr}\n  heavy={hkr}\n"
            f"  light_only={sorted(set(lkr) - set(hkr))} "
            f"heavy_only={sorted(set(hkr) - set(lkr))}"
        )

    # === geometry WKB parity (within 1e-6, SRID 0 both tiers) ===
    _assert_geom_parity(light["aswkb"], heavy["aswkb"], "cellaswkb")
    _assert_geom_parity(light["aswkt"], heavy["aswkt"], "cellaswkt", decoder=from_wkt)
    _assert_geom_parity(light["centroid"], heavy["centroid"], "centroid")


# --- all-4-encodings geom input -------------------------------------------------------------


def test_custom_pointascell_polyfill_all_four_encodings(spark_with_jar):
    """Geom-input consistency (Resolved decision 2): WKB/EWKB/WKT/EWKT of the same
    geometry yield identical pointascell id AND polyfill cell-set, in BOTH tiers."""
    spark = spark_with_jar

    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    # A point for pointascell and a box for polyfill, each in all four encodings.
    pt = _Point(_PX, _PY)
    pt_srid = set_srid(_Point(_PX, _PY), 27700)
    box_geom = _BOX
    box_srid = set_srid(_box(530000.0, 180000.0, 533000.0, 183000.0), 27700)

    df = spark.createDataFrame(
        [
            (
                bytearray(_to_wkb(pt)),  # pt_wkb
                bytearray(_to_wkb(pt_srid, include_srid=True)),  # pt_ewkb
                pt.wkt,  # pt_wkt
                f"SRID=27700;{pt.wkt}",  # pt_ewkt
                bytearray(_to_wkb(box_geom)),  # box_wkb
                bytearray(_to_wkb(box_srid, include_srid=True)),  # box_ewkb
                box_geom.wkt,  # box_wkt
                f"SRID=27700;{box_geom.wkt}",  # box_ewkt
            )
        ],
        "pt_wkb binary, pt_ewkb binary, pt_wkt string, pt_ewkt string, "
        "box_wkb binary, box_ewkb binary, box_wkt string, box_ewkt string",
    )
    df.createOrReplaceTempView("enc_src")

    def collect_enc():
        pac = spark.sql(
            f"SELECT "
            f"gbx_custom_pointascell(pt_wkb, {_GRID_27700}, 0) AS a, "
            f"gbx_custom_pointascell(pt_ewkb, {_GRID_27700}, 0) AS b, "
            f"gbx_custom_pointascell(pt_wkt, {_GRID_27700}, 0) AS c, "
            f"gbx_custom_pointascell(pt_ewkt, {_GRID_27700}, 0) AS d "
            f"FROM enc_src"
        ).collect()[0]
        pf = spark.sql(
            f"SELECT "
            f"gbx_custom_polyfill(box_wkb, {_GRID_27700}, 0) AS a, "
            f"gbx_custom_polyfill(box_ewkb, {_GRID_27700}, 0) AS b, "
            f"gbx_custom_polyfill(box_wkt, {_GRID_27700}, 0) AS c, "
            f"gbx_custom_polyfill(box_ewkt, {_GRID_27700}, 0) AS d "
            f"FROM enc_src"
        ).collect()[0]
        return pac, pf

    gx.register(spark)
    light_pac, light_pf = collect_enc()
    hx.register(spark)  # overwrites the gbx_custom_* names
    heavy_pac, heavy_pf = collect_enc()

    # All four encodings agree within each tier (pointascell).
    for tier, row in (("light", light_pac), ("heavy", heavy_pac)):
        assert row["a"] == row["b"] == row["c"] == row["d"], (
            f"{tier} pointascell encoding inconsistency: "
            f"wkb={row['a']} ewkb={row['b']} wkt={row['c']} ewkt={row['d']}"
        )
    # And light == heavy.
    assert (
        light_pac["a"] == heavy_pac["a"]
    ), f"pointascell cross-tier mismatch: light={light_pac['a']} heavy={heavy_pac['a']}"

    # All four encodings agree within each tier (polyfill cell-set).
    for tier, row in (("light", light_pf), ("heavy", heavy_pf)):
        sets = {k: sorted(row[k]) for k in ("a", "b", "c", "d")}
        assert (
            sets["a"] == sets["b"] == sets["c"] == sets["d"]
        ), f"{tier} polyfill encoding inconsistency: {sets}"
    assert sorted(light_pf["a"]) == sorted(heavy_pf["a"]), (
        f"polyfill cross-tier cell-set mismatch:\n"
        f"  light={sorted(light_pf['a'])}\n  heavy={sorted(heavy_pf['a'])}"
    )


# --- Y-NaN lock-in (Resolved decision 3) ----------------------------------------------------


def test_custom_y_nan_rejected_both_tiers(spark_with_jar):
    """Lock the CG-T8 fix cross-tier: a NaN Y must be REJECTED in BOTH tiers.

    The heavy ``CustomGridSystem.pointToCellID`` previously had a
    ``require(!x.isNaN && !x.isNaN, ...)`` typo that left a NaN Y unguarded; the
    light port guards both and the heavy fix (CG-T8) corrects the typo.

    A NaN Y cannot round-trip through standard WKB/WKT cleanly across tiers, so we
    lock it at each tier's natural boundary:
      * LIGHT: call the pure-Python core ``_custom.point_to_cell_id`` with a NaN Y
        and assert it raises ``ValueError`` (the same guard the UDF invokes).
      * HEAVY: feed a NaN-Y point as a WKT literal (``POINT(530000 NaN)``, which JTS
        parses to a NaN ordinate) through ``gbx_custom_pointascell`` and assert the
        query raises (the heavy ``require`` throws ``IllegalStateException``, which
        Spark surfaces as an analysis/execution exception).
    """
    spark = spark_with_jar

    from databricks.labs.gbx.gridx.custom import functions as hx
    from databricks.labs.gbx.pygx import _custom
    from databricks.labs.gbx.pygx import functions as gx

    # LIGHT: the pure-Python guard (the UDF calls straight into this).
    conf = _custom.CustomGridConf(
        bound_x_min=0,
        bound_x_max=1_000_000,
        bound_y_min=0,
        bound_y_max=1_000_000,
        cell_splits=2,
        root_cell_size_x=1000,
        root_cell_size_y=1000,
        srid=27700,
    )
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(conf, _PX, float("nan"), 0)
    # Sanity: a NaN X is also rejected (the original guard already covered X).
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(conf, float("nan"), _PY, 0)

    # HEAVY: a NaN-Y point through the registered gbx_custom_pointascell must raise.
    hx.register(spark)  # overwrites the catalog with heavy expressions
    with pytest.raises(Exception):
        spark.sql(
            f"SELECT gbx_custom_pointascell('POINT(530000 NaN)', {_GRID_27700}, 0)"
        ).collect()

    # Re-register light afterwards so the session's gbx_custom_* names are restored
    # for any subsequent test in this module (defensive; module fixture is shared).
    gx.register(spark)
    # And confirm the light tier also rejects NaN-Y via the registered UDF path.
    with pytest.raises(Exception):
        spark.sql(
            f"SELECT gbx_custom_pointascell('POINT(530000 NaN)', {_GRID_27700}, 0)"
        ).collect()
