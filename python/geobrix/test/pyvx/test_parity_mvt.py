"""Light (pyvx) vs heavy (vectorx) st_asmvt decoded-feature parity.

Both tiers encode the same WKB features and layer name; the decoded MVT must
produce the same geometry *and* the same native-typed properties (id stays int,
h stays float, name stays str).

Heavy requires the geobrix JAR *and* the GDAL/OGR native libraries (JNI).
Both are present in the geobrix-dev Docker container; this test auto-skips
when the JAR is not staged under ``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyvx/test_parity_mvt.py \\
        --with-integration --log mvt-parity.log
"""

import logging
from pathlib import Path

import mapbox_vector_tile as mvt
import pytest
from shapely import to_wkb
from shapely.geometry import Point

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyvx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker")
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
                "gbx:test:python --path python/geobrix/test/pyvx/test_parity_mvt.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pyvx-mvt-parity")
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


def _feats(blob: bytes, layer: str = "layer") -> dict:
    """Return {id_value: properties_dict} keyed on the 'id' property."""
    return {round(p["properties"]["id"]): p["properties"] for p in mvt.decode(blob)[layer]["features"]}


def test_light_vs_heavy_asmvt_decoded_parity(spark_with_jar):
    """Decoded features from pyvx and vectorx st_asmvt must match exactly.

    Checks:
    - same feature keys (by id)
    - ``id`` property is int in both
    - ``h`` property matches to float precision
    - ``name`` property is identical string
    """
    from databricks.labs.gbx.pyvx import functions as vx
    from databricks.labs.gbx.vectorx import functions as hx
    from pyspark.sql import functions as f

    spark = spark_with_jar
    vx.register(spark)
    hx.register(spark)

    rows = [
        (bytearray(to_wkb(Point(100.0, 200.0))), 1, 3.5, "alpha"),
        (bytearray(to_wkb(Point(300.0, 400.0))), 2, 9.0, "beta"),
    ]
    df = spark.createDataFrame(rows, "geom binary, id int, h double, name string")

    light_blob = bytes(
        df.agg(vx.st_asmvt(f.col("geom"), f.struct("id", "h", "name"), "layer")).collect()[0][0]
    )
    heavy_blob = bytes(
        df.agg(hx.st_asmvt(f.col("geom"), f.struct("id", "h", "name"), "layer")).collect()[0][0]
    )

    lf = _feats(light_blob)
    hf = _feats(heavy_blob)

    assert lf.keys() == hf.keys(), f"feature key mismatch: light={set(lf.keys())} heavy={set(hf.keys())}"

    for k in lf:
        # integer parity
        assert lf[k]["id"] == hf[k]["id"], f"id mismatch for key {k}: {lf[k]['id']} vs {hf[k]['id']}"
        assert isinstance(lf[k]["id"], int), f"light id not int for key {k}: {type(lf[k]['id'])}"
        assert isinstance(hf[k]["id"], int), f"heavy id not int for key {k}: {type(hf[k]['id'])}"

        # float parity
        assert abs(float(lf[k]["h"]) - float(hf[k]["h"])) < 1e-9, (
            f"h mismatch for key {k}: {lf[k]['h']} vs {hf[k]['h']}"
        )

        # string parity
        assert lf[k]["name"] == hf[k]["name"], f"name mismatch for key {k}: {lf[k]['name']} vs {hf[k]['name']}"
