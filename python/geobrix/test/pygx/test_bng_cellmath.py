import pytest

shapely = pytest.importorskip("shapely")
from databricks.labs.gbx.pygx import _bng  # noqa: E402  (after importorskip guard)


def test_cellarea_100km_is_10000_sqkm():
    # 100km cell -> (100000/1000)^2 = 10000 km^2.
    cid = _bng.parse("NE")  # a 100km grid square
    assert _bng.area(cid) == pytest.approx(10000.0)


def test_cellarea_1km_is_1_sqkm():
    cid = _bng.parse("TQ3080")  # a 1km cell
    assert _bng.area(cid) == pytest.approx(1.0)


def test_east_north_as_bng_string_and_int_res_agree():
    s_int = _bng.east_north_as_bng(530000.0, 180000.0, 3)
    s_str = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    assert s_int == s_str == "TQ3080"


def test_distance_manhattan_one_cell_east():
    a = _bng.east_north_as_bng(530000.0, 180000.0, "1km")  # TQ3080
    b = _bng.east_north_as_bng(531000.0, 180000.0, "1km")  # one cell east
    assert _bng.distance(_bng.parse(a), _bng.parse(b)) == 1


def test_euclidean_distance_diagonal_is_one():
    a = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    b = _bng.east_north_as_bng(531000.0, 181000.0, "1km")  # diagonal neighbour
    # Manhattan would be 2; Chebyshev (max) is 1.
    assert _bng.euclidean_distance(_bng.parse(a), _bng.parse(b)) == 1
    assert _bng.distance(_bng.parse(a), _bng.parse(b)) == 2
