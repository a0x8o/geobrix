import pytest

shapely = pytest.importorskip("shapely")
from shapely import to_wkb, set_srid, get_srid  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pygx import _geom


def test_parse_wkb_wkt_ewkt_ewkb_none():
    assert _geom.parse_geom(None) is None
    assert _geom.parse_geom("") is None
    assert _geom.parse_geom(to_wkb(Point(1, 2))).equals(Point(1, 2))
    assert _geom.parse_geom("POINT (1 2)").equals(Point(1, 2))
    g = _geom.parse_geom("SRID=4326;POINT (1 2)")
    assert g.equals(Point(1, 2)) and get_srid(g) == 4326
    e = _geom.parse_geom(to_wkb(set_srid(Point(1, 2), 4326), include_srid=True))
    assert get_srid(e) == 4326
