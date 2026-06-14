"""Pure-Python quadbin GridX core for the pygx light tier.

Cell math via the `quadbin` package; logic the package lacks (distance, bbox
polyfill) is ported to match the heavy `gridx/grid/Quadbin.scala` exactly.
Geometry outputs are EWKB (SRID 4326), matching heavy's JTS.toEWKB.
"""
import quadbin

_MAX_RES = 26
_MAX_POLYFILL_RES = 20


def point_as_cell(lon: float, lat: float, resolution: int) -> int:
    z = int(resolution)
    if z < 0 or z > _MAX_RES:
        raise ValueError(f"quadbin resolution must be in [0,{_MAX_RES}]; got {z}")
    return quadbin.point_to_cell(float(lon), float(lat), z)


def resolution(cell: int) -> int:
    return quadbin.get_resolution(int(cell))


def k_ring(cell: int, k: int) -> list:
    if int(k) < 0:
        raise ValueError(f"k must be >= 0; got {k}")
    return list(quadbin.k_ring(int(cell), int(k)))


def distance(cell_a: int, cell_b: int) -> int:
    if resolution(cell_a) != resolution(cell_b):
        raise ValueError("quadbin_distance: cells must be at same resolution")
    ax, ay = quadbin.cell_to_tile(int(cell_a))[:2]
    bx, by = quadbin.cell_to_tile(int(cell_b))[:2]
    return int(max(abs(ax - bx), abs(ay - by)))
