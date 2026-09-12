"""Synthetic "notional building" polygon generator for the geometry-aware
grid-expansion stress benchmark (geomkring / geomkloop).

Pure ``shapely`` — no geobrix or Spark imports, so it runs anywhere (local
unit tests and inside a Serverless notebook alike). Produces WKB footprints
across a mix of shapes and sizes, scattered deterministically over a target
extent so each building lands on a distinct set of grid cells.

Shapes exercise the range the expansion engine sees in practice:
  rect       — simple axis-aligned footprint
  lshape     — re-entrant corner (concave)
  ushape     — deeper concavity / thin arms
  courtyard  — outer ring with a rectangular hole (drives the hole modes)
  irregular  — jittered convex-ish n-gon (non-axis-aligned edges)

Sizes are nominal footprint spans in metres; ``deg_per_m`` converts metres to
the target CRS units (1.0 for a projected metre CRS such as EPSG:27700,
~1/111320 for WGS84 degrees) so the same generator serves lon/lat grids
(h3, quadbin, custom) and BNG (EPSG:27700).
"""

from __future__ import annotations

import math
import random

from shapely import to_wkb
from shapely.geometry import Polygon

SHAPES = ("rect", "lshape", "ushape", "courtyard", "irregular")
# Nominal footprint span in metres.
SIZES = {"small": 15.0, "medium": 50.0, "large": 200.0}

# 1 degree of latitude ~= 111320 m (good enough for a synthetic corpus).
_M_PER_DEG = 111_320.0


def _rect(hw: float, hh: float) -> Polygon:
    """Axis-aligned rectangle centred at the origin, half-width/half-height hw/hh."""
    return Polygon([(-hw, -hh), (hw, -hh), (hw, hh), (-hw, hh)])


def _lshape(s: float) -> Polygon:
    """L-shaped footprint spanning [-s, s]; a quadrant is cut out (concave)."""
    return Polygon([(-s, -s), (s, -s), (s, 0.0), (0.0, 0.0), (0.0, s), (-s, s)])


def _ushape(s: float) -> Polygon:
    """U-shaped footprint spanning [-s, s] with a central notch cut from the top."""
    n = s * 0.4  # half-width of the notch
    return Polygon(
        [
            (-s, -s),
            (s, -s),
            (s, s),
            (n, s),
            (n, -s * 0.2),
            (-n, -s * 0.2),
            (-n, s),
            (-s, s),
        ]
    )


def _courtyard(s: float) -> Polygon:
    """Rectangle spanning [-s, s] with a central rectangular hole (interior ring)."""
    outer = [(-s, -s), (s, -s), (s, s), (-s, s)]
    h = s * 0.45
    hole = [(-h, -h), (h, -h), (h, h), (-h, h)]  # interior ring
    return Polygon(outer, [hole])


def _irregular(s: float, rng: random.Random) -> Polygon:
    """Jittered convex-ish n-gon inscribed in radius s (non-axis-aligned edges)."""
    npts = 7
    pts = []
    for i in range(npts):
        ang = (2.0 * math.pi * i) / npts + rng.uniform(-0.15, 0.15)
        r = s * rng.uniform(0.7, 1.0)
        pts.append((r * math.cos(ang), r * math.sin(ang)))
    return Polygon(pts)


def building_polygon(
    shape: str,
    size_m: float,
    cx: float,
    cy: float,
    deg_per_m: float,
    rng: random.Random,
) -> Polygon:
    """One building footprint of ``shape`` and ``size_m`` centred at (cx, cy).

    Coordinates are in the target CRS units (metres scaled by ``deg_per_m``).
    """
    span = size_m * deg_per_m  # full nominal span in CRS units
    half = span / 2.0
    if shape == "rect":
        geom = _rect(half, half * 0.6)
    elif shape == "lshape":
        geom = _lshape(half)
    elif shape == "ushape":
        geom = _ushape(half)
    elif shape == "courtyard":
        geom = _courtyard(half)
    elif shape == "irregular":
        geom = _irregular(half, rng)
    else:
        raise ValueError(f"unknown shape {shape!r}; expected one of {SHAPES}")
    # Translate to (cx, cy).
    return Polygon(
        [(x + cx, y + cy) for x, y in geom.exterior.coords],
        [[(x + cx, y + cy) for x, y in ring.coords] for ring in geom.interiors],
    )


def deg_per_m(is_degrees: bool) -> float:
    """CRS-unit-per-metre factor: WGS84 degrees vs a projected metre CRS."""
    return (1.0 / _M_PER_DEG) if is_degrees else 1.0


def building_wkb_rows(
    n: int,
    *,
    is_degrees: bool,
    cx0: float,
    cy0: float,
    spread_m: float = 20_000.0,
    seed: int = 0,
    shapes: tuple = SHAPES,
    sizes: tuple = ("small", "medium", "large"),
) -> list:
    """Return ``n`` rows ``(wkb_bytes, shape, size)`` of scattered buildings.

    Buildings are placed uniformly at random within a ``spread_m`` × ``spread_m``
    block centred at (cx0, cy0), cycling ``shapes`` × ``sizes`` deterministically
    for a fixed ``seed`` so runs are reproducible. ``is_degrees`` selects the
    metre→CRS-unit scaling (True for WGS84 lon/lat, False for EPSG:27700).
    """
    if n <= 0:
        return []
    rng = random.Random(seed)
    dpm = deg_per_m(is_degrees)
    spread = spread_m * dpm
    combos = [(sh, sz) for sh in shapes for sz in sizes]
    rows = []
    for i in range(n):
        shape, size = combos[i % len(combos)]
        cx = cx0 + rng.uniform(-spread / 2.0, spread / 2.0)
        cy = cy0 + rng.uniform(-spread / 2.0, spread / 2.0)
        geom = building_polygon(shape, SIZES[size], cx, cy, dpm, rng)
        rows.append((to_wkb(geom), shape, size))
    return rows
