import math

from databricks.labs.gbx.ds.tiles.grid import SlippyGrid


def test_tile_bbox_world_at_zoom0():
    g = SlippyGrid()
    minlon, minlat, maxlon, maxlat = g.tile_bbox(0, 0, 0)
    assert minlon == -180.0
    assert maxlon == 180.0
    # web-mercator clamps latitude near +/-85.0511
    assert math.isclose(maxlat, 85.0511, abs_tol=1e-3)
    assert math.isclose(minlat, -85.0511, abs_tol=1e-3)


def test_tile_bbox_ordering_and_quadrant():
    g = SlippyGrid()
    # z1 tile (1,0,0) is the NW quadrant: lon [-180,0], lat [0, ~85]
    minlon, minlat, maxlon, maxlat = g.tile_bbox(1, 0, 0)
    assert (minlon, maxlon) == (-180.0, 0.0)
    assert minlat >= -0.001 and maxlat > minlat


def test_parent_clamps_and_shifts():
    g = SlippyGrid()
    # a z8 tile's parent at shard zoom 6 drops 2 bits
    assert g.parent(8, 130, 85, 6) == (6, 130 >> 2, 85 >> 2)
    # parent at a zoom deeper than the tile clamps to the tile itself
    assert g.parent(4, 3, 5, 6) == (4, 3, 5)
    # parent at the same zoom is identity
    assert g.parent(6, 12, 7, 6) == (6, 12, 7)


def test_tiles_for_bbox_covers_point():
    g = SlippyGrid()
    # London ~ (-0.12, 51.5) at zoom 6 -> a single covering tile
    tiles = list(g.tiles_for_bbox((-0.13, 51.49, -0.11, 51.51), 6))
    assert len(tiles) >= 1
    for z, x, y in tiles:
        bb = g.tile_bbox(z, x, y)
        assert bb[0] <= -0.12 <= bb[2]


def test_buffered_bbox_expands():
    g = SlippyGrid()
    base = g.tile_bbox(6, 32, 21)
    buf = g.buffered_bbox(6, 32, 21, 0.25)
    assert buf[0] < base[0] and buf[2] > base[2]
    assert buf[1] < base[1] and buf[3] > base[3]
