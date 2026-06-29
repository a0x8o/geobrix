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


def traverse_catalog(opener, bbox, theme_pairs, _item_loader=None):
    """Walk a nested static STAC catalog and return one dict per intersecting GeoParquet asset.

    The real Overture catalog is nested: root → release child (extra_fields={"latest":True})
    → theme Catalog → type Catalog → item links (rel="item"). Items are loaded via
    _item_loader (default: pystac.Item.from_file) and filtered by AOI bbox intersection
    and the requested (theme, type) pairs.

    _item_loader is an injectable seam for offline tests (avoids network calls to pystac).
    """
    import pystac as _pystac

    if _item_loader is None:
        _item_loader = _pystac.Item.from_file

    aoi = normalize_bbox(bbox)
    wanted = set(theme_pairs)
    rows = []
    catalog = opener()

    # Navigate to the latest release child.
    children = list(catalog.get_children())
    release_cat = next(
        (c for c in children if (getattr(c, "extra_fields", None) or {}).get("latest")),
        children[0] if children else None,
    )
    if release_cat is None:
        return rows

    for (theme, type_) in wanted:
        theme_cat = release_cat.get_child(theme)
        if theme_cat is None:
            continue
        type_cat = theme_cat.get_child(type_)
        if type_cat is None:
            continue
        for link in type_cat.links:
            if link.rel != "item":
                continue
            item_href = (
                link.get_absolute_href()
                if hasattr(link, "get_absolute_href")
                else getattr(link, "href", None)
            )
            if not item_href:
                continue
            try:
                item = _item_loader(item_href)
            except Exception:  # noqa: BLE001
                continue
            if item.bbox is None:
                continue
            item_bbox = [float(v) for v in item.bbox]
            if not bbox_intersects(aoi, tuple(item_bbox)):
                continue
            for asset in item.assets.values():
                rows.append(
                    {
                        "theme": theme,
                        "type": type_,
                        "href": asset.href,
                        "asset_bbox": item_bbox,
                    }
                )
    return rows


def resolve_release(opener, release: Optional[str] = None) -> str:
    """release=None -> latest release id from the catalog; an explicit string passes through."""
    if release is not None:
        return release
    catalog = opener()
    children = list(catalog.get_children())
    latest = next(
        (c for c in children if (getattr(c, "extra_fields", None) or {}).get("latest")),
        None,
    )
    if latest is None and children:
        latest = children[0]
    if latest is not None and getattr(latest, "id", None) is not None:
        return latest.id
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
