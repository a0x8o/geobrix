"""Benchmark function registry.

Each FnSpec binds a pyrx function to: its SQL name, category, supported timing
modes, default args, and two call adapters:
  core_fn(ds, args) -> Any           # pure-core: rasterio DatasetReader in
  col_fn(tile_col, args) -> Column   # spark-path: build a Spark Column

Only standard ds-in functions live here for the representative set. Special-
shaped functions (rasterize_geom, aggregators, multi-output tiling) get their
own adapters in a later task and are added with the appropriate `modes`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List

from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import accessors, indices, terrain, warp


@dataclass(frozen=True)
class FnSpec:
    name: str
    sql_name: str
    category: str
    modes: tuple
    args: dict = field(default_factory=dict)
    core_fn: Callable = None  # (ds, args) -> Any
    col_fn: Callable = None  # (tile_col, args) -> Column


_BOTH = ("pure-core", "spark-path")

REGISTRY: Dict[str, FnSpec] = {
    "rst_width": FnSpec(
        "rst_width",
        "gbx_rst_width",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.width(ds),
        col_fn=lambda t, a: prx.rst_width(t),
    ),
    "rst_avg": FnSpec(
        "rst_avg",
        "gbx_rst_avg",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.avg(ds),
        col_fn=lambda t, a: prx.rst_avg(t),
    ),
    "rst_slope": FnSpec(
        "rst_slope",
        "gbx_rst_slope",
        "terrain",
        _BOTH,
        {"unit": "degrees", "scale": 1.0},
        core_fn=lambda ds, a: terrain.slope(ds, a["unit"], a["scale"]),
        col_fn=lambda t, a: prx.rst_slope(t, a["unit"], a["scale"]),
    ),
    "rst_ndvi": FnSpec(
        "rst_ndvi",
        "gbx_rst_ndvi",
        "band-math",
        _BOTH,
        {"red_band": 1, "nir_band": 2},
        core_fn=lambda ds, a: indices.ndvi(ds, a["red_band"], a["nir_band"]),
        col_fn=lambda t, a: prx.rst_ndvi(t, a["red_band"], a["nir_band"]),
    ),
    "rst_transform": FnSpec(
        "rst_transform",
        "gbx_rst_transform",
        "warp",
        _BOTH,
        {"target_srid": 3857},
        core_fn=lambda ds, a: warp.reproject_to_srid(ds, a["target_srid"]),
        col_fn=lambda t, a: prx.rst_transform(t, a["target_srid"]),
    ),
    # --- accessors (scalar; accessors.py; empty args) ---
    "rst_height": FnSpec(
        "rst_height",
        "gbx_rst_height",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.height(ds),
        col_fn=lambda t, a: prx.rst_height(t),
    ),
    "rst_numbands": FnSpec(
        "rst_numbands",
        "gbx_rst_numbands",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.numbands(ds),
        col_fn=lambda t, a: prx.rst_numbands(t),
    ),
    "rst_min": FnSpec(
        "rst_min",
        "gbx_rst_min",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.minimum(ds),
        col_fn=lambda t, a: prx.rst_min(t),
    ),
    "rst_max": FnSpec(
        "rst_max",
        "gbx_rst_max",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.maximum(ds),
        col_fn=lambda t, a: prx.rst_max(t),
    ),
    "rst_median": FnSpec(
        "rst_median",
        "gbx_rst_median",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.median(ds),
        col_fn=lambda t, a: prx.rst_median(t),
    ),
    "rst_pixelcount": FnSpec(
        "rst_pixelcount",
        "gbx_rst_pixelcount",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.pixelcount(ds),
        col_fn=lambda t, a: prx.rst_pixelcount(t),
    ),
    # --- terrain (bytes; terrain.py) ---
    "rst_aspect": FnSpec(
        "rst_aspect",
        "gbx_rst_aspect",
        "terrain",
        _BOTH,
        {"trigonometric": False, "zero_for_flat": False},
        core_fn=lambda ds, a: terrain.aspect(
            ds, a["trigonometric"], a["zero_for_flat"]
        ),
        col_fn=lambda t, a: prx.rst_aspect(t, a["trigonometric"], a["zero_for_flat"]),
    ),
    "rst_hillshade": FnSpec(
        "rst_hillshade",
        "gbx_rst_hillshade",
        "terrain",
        _BOTH,
        {"azimuth": 315.0, "altitude": 45.0, "z_factor": 1.0},
        core_fn=lambda ds, a: terrain.hillshade(
            ds, a["azimuth"], a["altitude"], a["z_factor"]
        ),
        col_fn=lambda t, a: prx.rst_hillshade(
            t, a["azimuth"], a["altitude"], a["z_factor"]
        ),
    ),
    "rst_tri": FnSpec(
        "rst_tri",
        "gbx_rst_tri",
        "terrain",
        _BOTH,
        {},
        core_fn=lambda ds, a: terrain.tri(ds),
        col_fn=lambda t, a: prx.rst_tri(t),
    ),
    "rst_tpi": FnSpec(
        "rst_tpi",
        "gbx_rst_tpi",
        "terrain",
        _BOTH,
        {},
        core_fn=lambda ds, a: terrain.tpi(ds),
        col_fn=lambda t, a: prx.rst_tpi(t),
    ),
    "rst_roughness": FnSpec(
        "rst_roughness",
        "gbx_rst_roughness",
        "terrain",
        _BOTH,
        {},
        core_fn=lambda ds, a: terrain.roughness(ds),
        col_fn=lambda t, a: prx.rst_roughness(t),
    ),
    # --- band-math (bytes; indices.py) ---
    "rst_ndwi": FnSpec(
        "rst_ndwi",
        "gbx_rst_ndwi",
        "band-math",
        _BOTH,
        {"green_idx": 1, "nir_idx": 2},
        core_fn=lambda ds, a: indices.ndwi(ds, a["green_idx"], a["nir_idx"]),
        col_fn=lambda t, a: prx.rst_ndwi(t, a["green_idx"], a["nir_idx"]),
    ),
    "rst_nbr": FnSpec(
        "rst_nbr",
        "gbx_rst_nbr",
        "band-math",
        _BOTH,
        {"nir_idx": 1, "swir_idx": 2},
        core_fn=lambda ds, a: indices.nbr(ds, a["nir_idx"], a["swir_idx"]),
        col_fn=lambda t, a: prx.rst_nbr(t, a["nir_idx"], a["swir_idx"]),
    ),
    # --- warp (bytes; warp.py) ---
    "rst_to_webmercator": FnSpec(
        "rst_to_webmercator",
        "gbx_rst_to_webmercator",
        "warp",
        _BOTH,
        {"resampling": "bilinear"},
        core_fn=lambda ds, a: warp.reproject_to_srid(
            ds, 3857, resampling=a["resampling"]
        ),
        col_fn=lambda t, a: prx.rst_to_webmercator(t, a["resampling"]),
    ),
}


def select(functions: List[str] = None, categories: List[str] = None) -> List[FnSpec]:
    out = list(REGISTRY.values())
    if functions:
        out = [f for f in out if f.name in set(functions)]
    if categories:
        out = [f for f in out if f.category in set(categories)]
    return out


def dump_functions_json(path) -> None:
    """Write the language-neutral function list (no callables) for the Scala runner."""
    data = [
        {
            "name": f.name,
            "sql_name": f.sql_name,
            "category": f.category,
            "modes": list(f.modes),
            "args": f.args,
        }
        for f in REGISTRY.values()
    ]
    Path(path).write_text(json.dumps(data, indent=2))
