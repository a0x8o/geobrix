import pytest

shapely = pytest.importorskip("shapely")
from shapely import wkb  # noqa: E402
from shapely.geometry import LineString, MultiPoint, MultiPolygon, Point  # noqa: E402

from databricks.labs.gbx.pyvx import _legacy  # noqa: E402


def _row(type_id, boundaries, holes=None, srid=0):
    return {
        "typeId": type_id,
        "srid": srid,
        "boundaries": boundaries,
        "holes": holes or [],
    }


def test_point_xy():
    g = _legacy.legacy_to_geom(_row(1, [[[30.0, 10.0]]]))
    assert g.equals(Point(30.0, 10.0))


def test_point_xyz_preserves_z():
    g = _legacy.legacy_to_geom(_row(1, [[[30.0, 10.0, 5.0]]]))
    assert g.has_z and abs(g.z - 5.0) < 1e-9


def test_multipoint_xy():
    # boundaries[0] is the single ring holding all point coords.
    g = _legacy.legacy_to_geom(_row(2, [[[30.0, 10.0], [40.0, 20.0]]]))
    assert g.equals(MultiPoint([(30.0, 10.0), (40.0, 20.0)]))


def test_multipoint_xyz():
    g = _legacy.legacy_to_geom(_row(2, [[[30.0, 10.0, 5.0], [40.0, 20.0, 6.0]]]))
    assert isinstance(g, MultiPoint)
    assert g.has_z
    zs = sorted(pt.z for pt in g.geoms)
    assert zs == [5.0, 6.0]


def test_linestring():
    g = _legacy.legacy_to_geom(_row(3, [[[0.0, 0.0], [1.0, 1.0]]]))
    assert g.equals(LineString([(0, 0), (1, 1)]))


def test_polygon_preserves_holes():
    outer = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
    hole = [[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]
    g = _legacy.legacy_to_geom(_row(5, [outer], holes=[[hole]]))
    assert len(g.interiors) == 1
    assert abs(g.area - (100.0 - 4.0)) < 1e-9


def test_multipolygon_preserves_holes():
    def sq(o, s):
        return [[o, o], [o + s, o], [o + s, o + s], [o, o + s], [o, o]]

    poly0 = sq(0.0, 10.0)
    hole0 = sq(2.0, 2.0)
    poly1 = sq(20.0, 5.0)
    g = _legacy.legacy_to_geom(_row(6, [poly0, poly1], holes=[[hole0], []]))
    assert isinstance(g, MultiPolygon)
    assert sum(len(p.interiors) for p in g.geoms) == 1


def test_geometrycollection_raises():
    with pytest.raises(ValueError, match="GeometryCollection"):
        _legacy.legacy_to_geom(_row(8, []))


def test_aswkb_preserves_z_iso():
    out = _legacy.legacy_to_wkb(_row(1, [[[30.0, 10.0, 5.0]]]))
    assert wkb.loads(out).has_z


def test_null_input():
    assert _legacy.legacy_to_wkb(None) is None
