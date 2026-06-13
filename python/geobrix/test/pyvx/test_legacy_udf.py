import pytest

shapely = pytest.importorskip("shapely")
from shapely import wkb  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx


def test_st_legacyaswkb_roundtrips_polygon_with_hole(spark):
    vx.register(spark)
    outer = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
    hole = [[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]
    schema = "g struct<typeId:int,srid:int,boundaries:array<array<array<double>>>,holes:array<array<array<array<double>>>>>"
    df = spark.createDataFrame([({"typeId": 5, "srid": 0, "boundaries": [outer], "holes": [[hole]]},)], schema)
    out = df.selectExpr("gbx_st_legacyaswkb(g) AS wkb").collect()
    geom = wkb.loads(bytes(out[0]["wkb"]))
    assert len(geom.interiors) == 1
    assert abs(geom.area - 96.0) < 1e-9
