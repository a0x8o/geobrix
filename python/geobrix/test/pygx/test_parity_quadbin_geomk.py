"""Cross-tier parity: light (pygx) vs heavy (gridx.quadbin) for quadbin geometry-aware kring/kloop.

Tests all 6 dilation modes on both a simple polygon and a holed polygon, comparing
light ``_quadbin.geometry_k_ring/loop`` against heavy ``gbx_quadbin_geomkring/geomkloop``
registered functions.

Fixture: NYC lon/lat box (-73.99, 40.71 → -73.95, 40.75) at resolution 12.
Holed polygon: same outer box with inner hole (-73.975, 40.725 → -73.965, 40.735) at res 13.

Heavy requires the geobrix JAR. Mark integration; skips when no JAR under python/geobrix/lib/.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_parity_quadbin_geomk.py \\
        --with-integration --log parity-quadbin-geomk.log
"""

import logging
from pathlib import Path

import pytest
from shapely import to_wkb
from shapely.geometry import box
from shapely.geometry.polygon import Polygon

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# --- Fixtures ---

# Simple NYC box at res 12
_NYC_BOX = box(-73.99, 40.71, -73.95, 40.75)
_RES_SIMPLE = 12

# Holed polygon: NYC outer with small interior hole, at res 13
_OUTER = [(-73.99, 40.71), (-73.95, 40.71), (-73.95, 40.75), (-73.99, 40.75)]
_HOLE = [(-73.975, 40.725), (-73.965, 40.725), (-73.965, 40.735), (-73.975, 40.735)]
_HOLED_POLY = Polygon(_OUTER, [_HOLE])
_RES_HOLED = 13

_MODES = (
    "boundary-out",
    "boundary-in",
    "boundary-in-ignore-holes",
    "hole-in",
    "hole-out",
    "hole-out-ignore-geom",
)


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "PRECONDITION MISSING: staged JAR for heavy parity; "
            "run in geobrix-dev Docker (no geobrix JAR under python/geobrix/lib/)"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: skip if a JAR-free session is already live.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run in isolation: "
                "gbx:test:python --path python/geobrix/test/pygx/test_parity_quadbin_geomk.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-quadbin-geomk-parity")
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


def _wkb(geom) -> bytes:
    return bytes(to_wkb(geom))


def _collect_light(geom, res, k, mode):
    """Collect light result for one (geom, res, k, mode) combination."""
    from databricks.labs.gbx.pygx import _quadbin

    return set(_quadbin.geometry_k_ring(to_wkb(geom), res, k, mode))


def _collect_light_loop(geom, res, k, mode):
    from databricks.labs.gbx.pygx import _quadbin

    return set(_quadbin.geometry_k_loop(to_wkb(geom), res, k, mode))


def _collect_heavy(spark, geom, res, k, mode, fn_name):
    """Collect heavy result for one (geom, res, k, mode) combination.

    Uses SQL call_function routed through the registered gbx_quadbin_* name.
    Calls heavy with registered SQL UDF names; returns a set of ints.
    """
    from pyspark.sql import functions as f

    geom_wkb = _wkb(geom)
    df = spark.createDataFrame(
        [(geom_wkb, res, k, mode)], "geom binary, res int, k int, mode string"
    )
    row = df.select(
        f.call_function(
            fn_name, f.col("geom"), f.col("res"), f.col("k"), f.col("mode")
        ).alias("cells")
    ).collect()[0]
    if row["cells"] is None:
        return set()
    return set(int(c) for c in row["cells"])


def test_parity_quadbin_geomkring_simple_all_modes(spark_with_jar):
    """Light vs heavy geomkring over all 6 modes, simple polygon."""
    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # Register light FIRST, collect all light results, then register heavy.
    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light(_NYC_BOX, _RES_SIMPLE, 1, mode)

    # Now register heavy (overwrites light SQL names).
    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _NYC_BOX, _RES_SIMPLE, 1, mode, "gbx_quadbin_geomkring"
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkring mode={mode}: light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


def test_parity_quadbin_geomkloop_simple_all_modes(spark_with_jar):
    """Light vs heavy geomkloop over all 6 modes, simple polygon."""
    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light_loop(_NYC_BOX, _RES_SIMPLE, 1, mode)

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _NYC_BOX, _RES_SIMPLE, 1, mode, "gbx_quadbin_geomkloop"
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkloop mode={mode}: light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


def test_parity_quadbin_geomkring_holed_all_modes(spark_with_jar):
    """Light vs heavy geomkring over all 6 modes, holed polygon."""
    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light(_HOLED_POLY, _RES_HOLED, 1, mode)

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _HOLED_POLY, _RES_HOLED, 1, mode, "gbx_quadbin_geomkring"
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkring holed mode={mode}: light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )


def test_parity_quadbin_geomkloop_holed_all_modes(spark_with_jar):
    """Light vs heavy geomkloop over all 6 modes, holed polygon."""
    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_results = {}
    for mode in _MODES:
        light_results[mode] = _collect_light_loop(_HOLED_POLY, _RES_HOLED, 1, mode)

    hx.register(spark)
    for mode in _MODES:
        heavy = _collect_heavy(
            spark, _HOLED_POLY, _RES_HOLED, 1, mode, "gbx_quadbin_geomkloop"
        )
        light = light_results[mode]
        assert light == heavy, (
            f"geomkloop holed mode={mode}: light={sorted(light)[:5]}... heavy={sorted(heavy)[:5]}... "
            f"diff={sorted(light.symmetric_difference(heavy))[:5]}"
        )
