"""Overture static-STAC discovery helpers (driver-side, network-free when injected).

Kept separate from overture.py so the catalog traversal / bbox-intersect / CLI
fast-path logic is unit-testable in isolation, with no Spark and no network. The
catalog opener is injected by OvertureClient (_catalog_opener) for offline tests,
exactly like StacClient's seam.
"""

from __future__ import annotations

import shutil
import subprocess
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


def traverse_catalog(opener, bbox, theme_pairs):
    """Walk a static STAC catalog and return one dict per intersecting GeoParquet asset.

    opener() returns a pystac.Catalog-shaped object. Items are filtered by AOI
    bbox intersection and restricted to the requested (theme, type) pairs.
    """
    aoi = normalize_bbox(bbox)
    wanted = set(theme_pairs)
    rows = []
    catalog = opener()
    for collection in catalog.get_children():
        for item in collection.get_items():
            props = item.properties or {}
            pair = (props.get("theme"), props.get("type"))
            if pair not in wanted:
                continue
            item_bbox = list(item.bbox)
            if not bbox_intersects(aoi, tuple(item_bbox)):
                continue
            for asset in item.assets.values():
                rows.append(
                    {
                        "theme": pair[0],
                        "type": pair[1],
                        "href": asset.href,
                        "asset_bbox": [float(v) for v in item_bbox],
                    }
                )
    return rows


def resolve_release(opener, release: Optional[str] = None) -> str:
    """release=None -> latest release id from the catalog; an explicit string passes through."""
    if release is not None:
        return release
    catalog = opener()
    releases = getattr(catalog, "extra_fields", {}).get("overture:releases")
    if releases:
        return sorted(releases)[-1]
    cat_id = getattr(catalog, "id", None)
    if cat_id:
        return cat_id
    raise ValueError(
        "could not resolve latest Overture release from the catalog; pass release=... explicitly"
    )


def cli_discover(bbox, theme_pairs, release, runner=subprocess.run):
    """Fast-path via the `overturemaps` CLI when present; None otherwise.

    Returns rows shaped like traverse_catalog (theme/type/href/asset_bbox). The
    asset_bbox is the AOI bbox (the CLI lists paths intersecting the bbox, not
    per-file extents), which is sufficient for downstream pushdown bookkeeping.
    """
    if shutil.which("overturemaps") is None:
        return None
    aoi = normalize_bbox(bbox)
    bbox_arg = ",".join(str(v) for v in aoi)
    rows = []
    for theme, type_ in theme_pairs:
        completed = runner(
            [
                "overturemaps",
                "download",
                "--bbox",
                bbox_arg,
                "--release",
                release,
                "--type",
                type_,
                "--list-paths",
            ],
            capture_output=True,
            text=True,
        )
        if getattr(completed, "returncode", 1) != 0:
            continue
        for line in (completed.stdout or "").splitlines():
            href = line.strip()
            if href:
                rows.append(
                    {
                        "theme": theme,
                        "type": type_,
                        "href": href,
                        "asset_bbox": list(aoi),
                    }
                )
    return rows
