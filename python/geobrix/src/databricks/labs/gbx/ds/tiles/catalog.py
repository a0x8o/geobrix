"""Catalog writers over a set of shards. Default = a GeoJSON/STAC-style manifest
(one feature per shard with bbox + relative URL); TileJSON is an option. VRT and
full STAC-spec catalogs slot in later (see spec)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List, Protocol, Tuple

BBox = Tuple[float, float, float, float]


@dataclass
class ShardInfo:
    rel_path: str  # path relative to the catalog (e.g. "6/32/21.pmtiles")
    min_zoom: int
    max_zoom: int
    bbox: BBox


class CatalogWriter(Protocol):
    def write(self, shards: List[ShardInfo], out_dir: str) -> str: ...


def _bbox_polygon(bbox: BBox) -> dict:
    minlon, minlat, maxlon, maxlat = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [minlon, minlat],
                [maxlon, minlat],
                [maxlon, maxlat],
                [minlon, maxlat],
                [minlon, minlat],
            ]
        ],
    }


def _union(shards: List[ShardInfo]) -> BBox:
    minlon = min(s.bbox[0] for s in shards)
    minlat = min(s.bbox[1] for s in shards)
    maxlon = max(s.bbox[2] for s in shards)
    maxlat = max(s.bbox[3] for s in shards)
    return (minlon, minlat, maxlon, maxlat)


class STACManifestCatalog:
    """GeoJSON FeatureCollection (STAC-style): one feature per shard."""

    def write(self, shards: List[ShardInfo], out_dir: str) -> str:
        features = [
            {
                "type": "Feature",
                "bbox": list(s.bbox),
                "geometry": _bbox_polygon(s.bbox),
                "properties": {
                    "pmtiles": s.rel_path,
                    "minzoom": s.min_zoom,
                    "maxzoom": s.max_zoom,
                },
            }
            for s in shards
        ]
        doc = {"type": "FeatureCollection", "features": features}
        path = os.path.join(out_dir, "catalog.json")
        with open(path, "w") as f:
            json.dump(doc, f)
        return path


class TileJSONCatalog:
    """Minimal TileJSON 3.0.0 over the shards (union bounds + a shards array)."""

    def write(self, shards: List[ShardInfo], out_dir: str) -> str:
        bounds = _union(shards)
        doc = {
            "tilejson": "3.0.0",
            "minzoom": min(s.min_zoom for s in shards),
            "maxzoom": max(s.max_zoom for s in shards),
            "bounds": list(bounds),
            "shards": [
                {
                    "pmtiles": s.rel_path,
                    "bounds": list(s.bbox),
                    "minzoom": s.min_zoom,
                    "maxzoom": s.max_zoom,
                }
                for s in shards
            ],
        }
        path = os.path.join(out_dir, "catalog.json")
        with open(path, "w") as f:
            json.dump(doc, f)
        return path
