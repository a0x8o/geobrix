import pytest

pytest.importorskip("quadbin")
shapely = pytest.importorskip("shapely")
from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.pygx import functions as gx  # noqa: E402


def test_pointascell_and_resolution(spark):
    gx.register(spark)
    import quadbin

    c = spark.sql(
        "SELECT gbx_quadbin_pointascell(-122.4194, 37.7749, 10) AS c"
    ).collect()[0]["c"]
    assert c == quadbin.point_to_cell(-122.4194, 37.7749, 10)
    r = spark.sql(f"SELECT gbx_quadbin_resolution({c}) AS r").collect()[0]["r"]
    assert r == 10


def test_kring_and_distance(spark):
    gx.register(spark)
    import quadbin

    c = quadbin.point_to_cell(0.0, 0.0, 10)
    ring = spark.sql(f"SELECT gbx_quadbin_kring({c}, 1) AS k").collect()[0]["k"]
    assert len(ring) == 9
    d = spark.sql(f"SELECT gbx_quadbin_distance({c}, {c}) AS d").collect()[0]["d"]
    assert d == 0


def test_aswkb_centroid_ewkb(spark):
    gx.register(spark)
    import quadbin

    c = quadbin.point_to_cell(0.0, 0.0, 10)
    w = spark.sql(f"SELECT gbx_quadbin_aswkb({c}) AS w").collect()[0]["w"]
    g = from_wkb(bytes(w))
    assert g.geom_type == "Polygon" and get_srid(g) == 4326
    p = spark.sql(f"SELECT gbx_quadbin_centroid({c}) AS p").collect()[0]["p"]
    assert from_wkb(bytes(p)).geom_type == "Point"


def test_polyfill_and_tessellate(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(-0.05, -0.05, 0.05, 0.05))),)], "g binary"
    )
    df.createOrReplaceTempView("v")
    cells = spark.sql("SELECT gbx_quadbin_polyfill(g, 12) AS c FROM v").collect()[0][
        "c"
    ]
    assert len(cells) > 0
    chips = spark.sql(
        "SELECT t.cell, t.geom FROM v LATERAL VIEW explode(gbx_quadbin_tessellate(g, 12)) AS t"
    ).collect()
    assert len(chips) > 0 and isinstance(chips[0]["cell"], int)


def test_cellunion_and_agg(spark):
    gx.register(spark)
    import quadbin

    cells = list(quadbin.k_ring(quadbin.point_to_cell(0.0, 0.0, 8), 1))
    u = spark.sql(
        f"SELECT gbx_quadbin_cellunion(array({','.join(str(c) for c in cells)})) AS u"
    ).collect()[0]["u"]
    assert from_wkb(bytes(u)).geom_type in ("Polygon", "MultiPolygon")
    df = spark.createDataFrame([(c,) for c in cells], "cell long")
    a = df.agg(gx.quadbin_cellunion_agg("cell").alias("u")).collect()[0]["u"]
    assert from_wkb(bytes(a)).geom_type in ("Polygon", "MultiPolygon")
