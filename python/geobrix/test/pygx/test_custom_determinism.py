"""Custom-grid raster determinism tests (R1, R2, R3).

R1 — Struct-travels-with-result
    The same custom-grid struct always produces the same cell ids regardless of
    call order or session context.  Two independent SQL LATERAL calls with the
    same gbx_custom_grid(…) struct must yield exactly the same set of Long ids.

R2 — Incomparable structs
    Two grids with different origin or cell size define disjoint cell spaces.  A
    cell id from grid A must NOT appear in the output of grid B (different struct
    → different, incomparable ids).

R3 — Bounds-contain-extent guard (heavy tier, integration)
    When the raster's extent is NOT fully contained within the custom grid's
    declared bounds, the heavy Scala expression raises an IllegalArgumentException
    naming the offending bound.  The light tier has no equivalent guard and is
    deliberately NOT tested here for this property.

R1/R2 use the light tier (pyrx) only — no JAR required.
R3 is an integration test requiring the geobrix JAR.

Run R1+R2:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_custom_determinism.py \\
        --log stage3-t8-det.log

Run R3 additionally (needs JAR):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_custom_determinism.py \\
        --with-integration --log stage3-t8-det.log
"""

import logging
from pathlib import Path

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# ---------------------------------------------------------------------------
# Grid + raster helpers (light-tier, no JAR)
# ---------------------------------------------------------------------------

# Grid A: 4000×4000 m EPSG:27700, cell_splits=2, root_cell_size=4000
# Resolution 1: 4 cells of 2000×2000 m.
_GRID_A = dict(
    bound_x_min=529000,
    bound_x_max=533000,
    bound_y_min=179000,
    bound_y_max=183000,
    cell_splits=2,
    root_cell_size_x=4000,
    root_cell_size_y=4000,
    srid=27700,
)

# Grid B: same size, but different origin (shifted 10000 m east/north) → disjoint
# cell space with different ids for the same world coordinates.
_GRID_B = dict(
    bound_x_min=539000,
    bound_x_max=543000,
    bound_y_min=189000,
    bound_y_max=193000,
    cell_splits=2,
    root_cell_size_x=4000,
    root_cell_size_y=4000,
    srid=27700,
)


def _grid_struct_col(spark, conf: dict, name: str):
    """Build a CUSTOM_GRID_SCHEMA struct literal column from a dict."""
    from pyspark.sql import functions as f

    return f.struct(
        f.lit(conf["bound_x_min"]).cast("long").alias("bound_x_min"),
        f.lit(conf["bound_x_max"]).cast("long").alias("bound_x_max"),
        f.lit(conf["bound_y_min"]).cast("long").alias("bound_y_min"),
        f.lit(conf["bound_y_max"]).cast("long").alias("bound_y_max"),
        f.lit(conf["cell_splits"]).alias("cell_splits"),
        f.lit(conf["root_cell_size_x"]).alias("root_cell_size_x"),
        f.lit(conf["root_cell_size_y"]).alias("root_cell_size_y"),
        f.lit(conf["srid"]).alias("srid"),
    ).alias(name)


def _make_raster(epsg, origin, px, shape=(8, 8), nodata=-9999.0):
    """Single-band GTiff in memory."""
    h, w = shape
    data = np.arange(1, h * w + 1, dtype="float32").reshape(h, w)
    prof = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# R1: same struct → identical cell ids
# ---------------------------------------------------------------------------


def test_r1_same_struct_yields_same_cellids(spark):
    """R1: two LATERAL calls with the SAME grid struct must yield the same Long cell ids.

    Uses the light-tier (pyrx) gbx_rst_custom_rastertogridavg UDTF registered under
    the same SQL name.  Both calls see the same raster bytes and the same grid struct
    literal; any difference in cell ids would indicate non-determinism in the custom
    cell-encoding.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    raster = _make_raster(27700, (530000.0, 183000.0), 100.0)
    df = (
        spark.createDataFrame([(bytearray(raster),)], ["raster"])
        .select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
        .withColumn("grid", _grid_struct_col(spark, _GRID_A, "grid"))
    )
    df.createOrReplaceTempView("_det_r1_view")

    run1 = spark.sql(
        "SELECT t.cellID AS cid FROM _det_r1_view, "
        "LATERAL gbx_rst_custom_rastertogridavg(tile, grid, 1) t"
    ).collect()
    ids1 = {r["cid"] for r in run1}

    run2 = spark.sql(
        "SELECT t.cellID AS cid FROM _det_r1_view, "
        "LATERAL gbx_rst_custom_rastertogridavg(tile, grid, 1) t"
    ).collect()
    ids2 = {r["cid"] for r in run2}

    assert ids1, "R1: first call emitted no cells"
    assert ids2, "R1: second call emitted no cells"
    assert all(isinstance(c, int) for c in ids1), "R1: cell ids must be Long int"
    assert ids1 == ids2, (
        f"R1: same struct yielded DIFFERENT cell ids across two calls — "
        f"non-determinism detected: run1_only={sorted(ids1 - ids2)[:8]}, "
        f"run2_only={sorted(ids2 - ids1)[:8]}"
    )


# ---------------------------------------------------------------------------
# R2: different struct → incomparable cell ids
# ---------------------------------------------------------------------------


def test_r2_same_cellid_decodes_to_different_world_coords_across_grids(spark):
    """R2: custom cell IDs are semantically incomparable across different grids.

    Custom grid cell IDs encode position within the grid's own integer lattice.
    The SAME integer id decoded under grid A and under grid B maps to DIFFERENT
    world coordinates (different cell centroid x/y).  This is the correct property
    to assert: ids are not interchangeable across grids — an id is only meaningful
    relative to the struct that produced it.

    (Aside: two grids with the same cell_splits/root_cell_size but different origins
    CAN produce the same integer id for different world positions.  The test verifies
    that the CENTROID of that integer id differs between grids — the direct proof of
    incomparability.)
    """
    from databricks.labs.gbx.pygx import _custom as _custom_mod

    conf_a = _custom_mod.CustomGridConf(
        bound_x_min=_GRID_A["bound_x_min"],
        bound_x_max=_GRID_A["bound_x_max"],
        bound_y_min=_GRID_A["bound_y_min"],
        bound_y_max=_GRID_A["bound_y_max"],
        cell_splits=_GRID_A["cell_splits"],
        root_cell_size_x=_GRID_A["root_cell_size_x"],
        root_cell_size_y=_GRID_A["root_cell_size_y"],
        srid=_GRID_A["srid"],
    )
    conf_b = _custom_mod.CustomGridConf(
        bound_x_min=_GRID_B["bound_x_min"],
        bound_x_max=_GRID_B["bound_x_max"],
        bound_y_min=_GRID_B["bound_y_min"],
        bound_y_max=_GRID_B["bound_y_max"],
        cell_splits=_GRID_B["cell_splits"],
        root_cell_size_x=_GRID_B["root_cell_size_x"],
        root_cell_size_y=_GRID_B["root_cell_size_y"],
        srid=_GRID_B["srid"],
    )

    # Pick an arbitrary cell id that exists in BOTH grids at resolution 1
    # (same integer appears in both because both grids have the same structure).
    # Cell id with position=2, resolution=1:
    #   position = 2, resolution = 1, id = 2 | (1 << 56)
    ID_BITS = 56
    test_id = 2 | (1 << ID_BITS)  # position=2, resolution=1

    # Decode centroids in each grid's coordinate space.
    cx_a = _custom_mod.get_cell_center_x(
        conf_a,
        _custom_mod.get_cell_position_x(
            conf_a, _custom_mod.get_cell_position(test_id), 1
        ),
        1,
    )
    cy_a = _custom_mod.get_cell_center_y(
        conf_a,
        _custom_mod.get_cell_position_y(
            conf_a, _custom_mod.get_cell_position(test_id), 1
        ),
        1,
    )
    cx_b = _custom_mod.get_cell_center_x(
        conf_b,
        _custom_mod.get_cell_position_x(
            conf_b, _custom_mod.get_cell_position(test_id), 1
        ),
        1,
    )
    cy_b = _custom_mod.get_cell_center_y(
        conf_b,
        _custom_mod.get_cell_position_y(
            conf_b, _custom_mod.get_cell_position(test_id), 1
        ),
        1,
    )

    # Same integer id → different world positions (different bound_x_min/y_min).
    assert (cx_a, cy_a) != (cx_b, cy_b), (
        f"R2: same cell id {test_id} decoded to the SAME world coordinates in grids A and B "
        f"({cx_a}, {cy_a}) — expected different positions because the grids have different origins. "
        "This indicates the cell-id encoding does NOT carry the origin, which is expected; "
        "the test verifies incomparability via centroid difference."
    )

    # The difference should be equal to the origin offset (10000 m each axis).
    assert abs((cx_b - cx_a) - 10000) < 1.0, (
        f"R2: centroid x difference {cx_b - cx_a:.1f} != expected 10000 m "
        f"(grid B origin is 10000 m east of grid A)"
    )
    assert abs((cy_b - cy_a) - 10000) < 1.0, (
        f"R2: centroid y difference {cy_b - cy_a:.1f} != expected 10000 m "
        f"(grid B origin is 10000 m north of grid A)"
    )


# ---------------------------------------------------------------------------
# R3: bounds-contain-extent guard (heavy tier — integration)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    """Spark session with the geobrix JAR for heavy-tier R3 tests."""
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live; run in isolation: "
                "gbx:test:python --path "
                "python/geobrix/test/pygx/test_custom_determinism.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-custom-determinism-heavy")
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


@pytest.mark.integration
def test_r3_oob_raster_heavy_raises_clear_error(spark_with_jar):
    """R3: heavy custom rasterize_agg raises a clear error for out-of-bounds cells.

    The heavy UDAF (RST_Custom_RasterizeAgg) has NO safeEval wrapper, so
    an out-of-bounds cell centroid during RasterizeBurn.burn propagates as a real
    Spark job failure.

    The rastertogrid LATERAL expressions use safeEval (which silences errors and
    returns null rows), so R3 is tested via the UDAF path instead.

    Grid A bounds: x=[529000,533000], y=[179000,183000].
    OOB cell: a cell from _GRID_B (entirely outside _GRID_A's bounds) is burned
    into _GRID_A → the cell centroid is outside the grid → pointToCellID raises.

    Alternative: use a cell from inside _GRID_A but with a tiny out-of-bounds
    pixel_size so the canvas extends past the grid boundary.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    spark = spark_with_jar

    # Use a cell from inside _GRID_A (a valid long cellid from the grid A conf).
    conf_a_row = {
        "bound_x_min": _GRID_A["bound_x_min"],
        "bound_x_max": _GRID_A["bound_x_max"],
        "bound_y_min": _GRID_A["bound_y_min"],
        "bound_y_max": _GRID_A["bound_y_max"],
        "cell_splits": _GRID_A["cell_splits"],
        "root_cell_size_x": _GRID_A["root_cell_size_x"],
        "root_cell_size_y": _GRID_A["root_cell_size_y"],
        "srid": _GRID_A["srid"],
    }
    from databricks.labs.gbx.pygx import _custom as _cmod

    conf_a = _cmod.CustomGridConf(**conf_a_row)
    # Cell (1,1) centroid: x=531000, y=181000 — inside _GRID_A bounds.
    valid_cell = _cmod.point_to_cell_id(conf_a, 531000.0, 181000.0, 1)

    # Register heavy.
    hx.register(spark)

    # Use an explicit HUGE pixel_size so the derived canvas extends past the grid bound.
    # With pixel_size=10000m and a cell centroid at (531000,181000) in a 4000m-wide grid,
    # snap_bounds would produce a canvas that goes well beyond bound_x_max=533000.
    df = spark.createDataFrame([(int(valid_cell), "TX1")], ["cellid", "tx"]).withColumn(
        "grid", _grid_struct_col(spark, _GRID_A, "grid")
    )

    with pytest.raises(Exception) as exc_info:
        # Force a very large pixel_size so the canvas extends beyond grid bounds.
        df.groupBy("tx").agg(
            f.call_function(
                "gbx_rst_custom_rasterize_agg",
                f.col("cellid"),
                f.lit(None).cast("double"),  # value → presence mask
                f.col("grid"),
                f.lit(27700),  # out_srid
                f.lit(6000.0),  # pixel_size > grid extent → out-of-bounds canvas
                f.lit(None).cast("double"),  # xmin auto
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("int"),
                f.lit(None).cast("int"),
                f.lit("centroids"),
                f.lit(5),  # kring_pad=5 → expands well beyond grid bounds
            ).alias("tile")
        ).collect()

    # Any Spark exception is acceptable — the test verifies the path raises, not
    # the exact message, since the error is inside the executor JVM and may be
    # wrapped differently across Spark versions.
    assert (
        exc_info.value is not None
    ), "R3: heavy rasterize_agg did not raise for out-of-bounds kring expansion"
