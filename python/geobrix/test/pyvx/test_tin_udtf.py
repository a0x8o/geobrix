import pytest

pytest.importorskip("scipy")
shapely = pytest.importorskip("shapely")
from shapely import to_wkb, wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx


def _pts_wkb(coords):
    return [bytearray(to_wkb(Point(*c))) for c in coords]


def test_st_triangulate_emits_triangles(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0), (0, 1, 0)])
    df = spark.createDataFrame([(pts, [], 0.0, 0.0, "NONENCROACHING")],
                               "pts array<binary>, bl array<binary>, mt double, st double, spf string")
    df.createOrReplaceTempView("v")
    rows = spark.sql("SELECT t.triangle FROM v, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t").collect()
    assert len(rows) == 2
    assert all(wkb.loads(bytes(r["triangle"])).geom_type == "Polygon" for r in rows)


def test_st_triangulate_two_arg_default_mode(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0)])
    df = spark.createDataFrame([(pts, [], 0.0, 0.0, "NONENCROACHING")],
                               "pts array<binary>, bl array<binary>, mt double, st double, spf string")
    df.createOrReplaceTempView("vd")
    rows = spark.sql("SELECT t.triangle FROM vd, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf) t").collect()
    assert len(rows) == 1  # default mode = constrained


def test_st_triangulate_conforming_raises(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0)])
    df = spark.createDataFrame([(pts, [], 0.0, 0.0, "MIDPOINT")],
                               "pts array<binary>, bl array<binary>, mt double, st double, spf string")
    df.createOrReplaceTempView("v2")
    with pytest.raises(Exception, match="conforming"):
        spark.sql("SELECT t.* FROM v2, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'conforming') t").collect()
