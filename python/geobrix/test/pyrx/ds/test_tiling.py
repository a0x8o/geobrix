"""Unit tests for the BalancedSubdivision port (pure integer math)."""

from databricks.labs.gbx.pyrx.ds import _tiling


def test_small_raster_is_single_tile():
    windows = _tiling.plan_windows(
        width=4, height=3, bands=1, dtype="float32", size_mib=16
    )
    assert windows == [(0, 0, 4, 3)]


def test_tile_count_is_power_of_four_when_split():
    windows = _tiling.plan_windows(
        width=4096, height=4096, bands=4, dtype="float64", size_mib=16
    )
    n = len(windows)
    side = int(round(n**0.5))
    assert side * side == n, f"{n} tiles is not a square grid"
    assert (side & (side - 1)) == 0, f"side {side} is not a power of two"


def test_windows_tile_the_full_raster_without_gaps_or_overlap():
    width, height = 1000, 700
    windows = _tiling.plan_windows(
        width=width, height=height, bands=2, dtype="uint8", size_mib=1
    )
    covered = 0
    for col_off, row_off, win_w, win_h in windows:
        assert col_off + win_w <= width
        assert row_off + win_h <= height
        covered += win_w * win_h
    assert covered == width * height


def test_get_tile_size_matches_scala_ceil_div():
    nx, ny, tile_x, tile_y = _tiling.tile_grid(
        width=1000, height=700, bands=2, dtype="uint8", size_mib=1
    )
    assert tile_x == -(-1000 // nx)
    assert tile_y == -(-700 // ny)
