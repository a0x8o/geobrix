import pytest

shapely = pytest.importorskip("shapely")
from shapely import from_wkb, from_wkt, get_srid  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402


def test_aswkb_is_wkb_polygon_no_srid():
    cid = _bng.parse("TQ3080")  # 1km cell
    g = from_wkb(_bng.cell_aswkb(cid))
    assert g.geom_type == "Polygon"
    assert get_srid(g) == 0  # BNG WKB carries NO SRID (heavy uses toWKB, not toEWKB)
    minx, miny, maxx, maxy = g.bounds
    # 1km cell, easting bin 30 -> 530000, northing bin 80 -> 180000.
    assert (minx, miny, maxx, maxy) == (530000.0, 180000.0, 531000.0, 181000.0)


def test_aswkt_is_polygon_text():
    cid = _bng.parse("TQ3080")
    g = from_wkt(_bng.cell_aswkt(cid))
    assert g.geom_type == "Polygon"


def test_centroid_is_wkb_point_no_srid():
    cid = _bng.parse("TQ3080")
    g = from_wkb(_bng.cell_centroid(cid))
    assert g.geom_type == "Point" and get_srid(g) == 0
    assert (g.x, g.y) == (530500.0, 180500.0)  # cell centre
