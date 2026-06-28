"""Overture static-STAC discovery helpers (driver-side, network-free when injected).

Kept separate from overture.py so the catalog traversal / bbox-intersect / CLI
fast-path logic is unit-testable in isolation, with no Spark and no network. The
catalog opener is injected by OvertureClient (_catalog_opener) for offline tests,
exactly like StacClient's seam.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

Bbox = Tuple[float, float, float, float]


def normalize_bbox(bbox) -> Bbox:
    """Validate and float-cast a (minx, miny, maxx, maxy) bbox."""
    if bbox is None or len(bbox) != 4:
        raise ValueError(f"bbox must be (minx, miny, maxx, maxy); got {bbox!r}")
    minx, miny, maxx, maxy = (float(v) for v in bbox)
    if minx > maxx or miny > maxy:
        raise ValueError(f"bbox is inverted (min > max): {bbox!r}")
    return (minx, miny, maxx, maxy)


def bbox_intersects(a, b) -> bool:
    """Axis-aligned overlap test; touching edges count as intersecting."""
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    return ax0 <= bx1 and bx0 <= ax1 and ay0 <= by1 and by0 <= ay1


OVERTURE_THEMES = {
    "addresses": ["address"],
    "base": ["infrastructure", "land", "land_cover", "land_use", "water", "bathymetry"],
    "buildings": ["building", "building_part"],
    "divisions": ["division", "division_area", "division_boundary"],
    "places": ["place"],
    "transportation": ["connector", "segment"],
}


def expand_themes(themes: Optional[List[str]]) -> List[Tuple[str, str]]:
    """themes=None -> every (theme, type) pair; a list -> that subset's pairs."""
    names = list(OVERTURE_THEMES) if themes is None else list(themes)
    pairs: List[Tuple[str, str]] = []
    for name in names:
        if name not in OVERTURE_THEMES:
            raise ValueError(
                f"unknown Overture theme {name!r}; valid: {sorted(OVERTURE_THEMES)}"
            )
        pairs.extend((name, t) for t in OVERTURE_THEMES[name])
    return pairs
