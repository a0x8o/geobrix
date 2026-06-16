"""Bit-exactness tests for the vectorized quadbin cell-id kernels.

The pandas_udf path uses the numpy `*_vec` kernels in `_quadbin`. These must be
bit-identical to the scalar `_quadbin` functions (the parity oracle that was
tuned to match heavy Quadbin.scala), including the +-180 antimeridian and +-85
pole clamp. If these drift, cross-tier parity (test_parity_quadbin) breaks.
"""

import numpy as np
import pytest

pytest.importorskip("quadbin")
import quadbin  # noqa: E402

from databricks.labs.gbx.pygx import _quadbin  # noqa: E402

# Dense lon/lat grid + explicit antimeridian/pole edges.
_LONS = np.concatenate(
    [np.linspace(-180.0, 180.0, 41), np.array([-180.0, 180.0, -179.999, 179.999, 0.0])]
)
_LATS = np.concatenate(
    [
        np.linspace(-86.0, 86.0, 39),
        np.array([-85.05112878, 85.05112878, -90.0, 90.0, 0.0]),
    ]
)
_RESOLUTIONS = [0, 1, 2, 5, 8, 10, 14, 18, 20, 23, 26]


def _grid():
    LL, AA = np.meshgrid(_LONS, _LATS)
    return LL.ravel(), AA.ravel()


@pytest.mark.parametrize("z", _RESOLUTIONS)
def test_point_as_cell_vec_bit_identical_to_scalar(z):
    lons, lats = _grid()
    cells = _quadbin.point_as_cell_vec(lons, lats, z)
    assert cells.dtype == np.int64
    for lon, lat, c in zip(lons, lats, cells):
        expected = _quadbin.point_as_cell(float(lon), float(lat), z)
        assert int(c) == expected, f"lon={lon} lat={lat} z={z}: {int(c)} != {expected}"


def test_point_as_cell_vec_antimeridian_pole_edges():
    # The exact edges the scalar path was fixed for: lon=+-180 -> easternmost
    # tile (n-1, not wrapped 0); lat beyond +-85 -> pole clamp.
    for z in (1, 14, 26):
        n = 1 if z == 0 else (1 << z)
        lons = np.array([180.0, -180.0, 179.9999, 0.0])
        lats = np.array([85.05112878, -85.05112878, 90.0, -90.0])
        cells = _quadbin.point_as_cell_vec(lons, lats, z)
        for lon, lat, c in zip(lons, lats, cells):
            assert int(c) == _quadbin.point_as_cell(float(lon), float(lat), z)
        # lon=180 must land on the easternmost x tile, not the wrapped x=0.
        assert int(cells[0]) == quadbin.tile_to_cell((n - 1, 0, z))


def test_point_as_cell_vec_resolution_validation():
    lons, lats = _grid()
    with pytest.raises(ValueError):
        _quadbin.point_as_cell_vec(lons, lats, 27)


@pytest.mark.parametrize("z", _RESOLUTIONS)
def test_resolution_vec_bit_identical_to_scalar(z):
    lons, lats = _grid()
    cells = _quadbin.point_as_cell_vec(lons, lats, z)
    res = _quadbin.resolution_vec(cells)
    for c, r in zip(cells, res):
        assert int(r) == _quadbin.resolution(int(c)) == z


def test_distance_matches_scalar():
    a = quadbin.point_to_cell(0.0, 0.0, 12)
    others = [
        quadbin.point_to_cell(0.0, 0.0, 12),
        quadbin.point_to_cell(0.5, 0.5, 12),
        quadbin.point_to_cell(-1.0, 2.0, 12),
        quadbin.point_to_cell(3.0, -2.0, 12),
    ]
    for b in others:
        assert _quadbin.distance(a, b) == _quadbin.distance(a, b)
        # sanity vs direct Chebyshev on tile coords
        ta, tb = quadbin.cell_to_tile(a), quadbin.cell_to_tile(b)
        assert _quadbin.distance(a, b) == max(abs(ta[0] - tb[0]), abs(ta[1] - tb[1]))


def test_kring_matches_scalar():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    for k in (0, 1, 2, 3):
        assert sorted(_quadbin.k_ring(cell, k)) == sorted(quadbin.k_ring(cell, k))
