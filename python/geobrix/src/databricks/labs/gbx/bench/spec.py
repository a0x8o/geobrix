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
    core_fn: Callable = None   # (ds, args) -> Any
    col_fn: Callable = None    # (tile_col, args) -> Column


_BOTH = ("pure-core", "spark-path")

REGISTRY: Dict[str, FnSpec] = {
    "rst_width": FnSpec(
        "rst_width", "gbx_rst_width", "accessor", _BOTH, {},
        core_fn=lambda ds, a: accessors.width(ds),
        col_fn=lambda t, a: prx.rst_width(t),
    ),
    "rst_avg": FnSpec(
        "rst_avg", "gbx_rst_avg", "accessor", _BOTH, {},
        core_fn=lambda ds, a: accessors.avg(ds),
        col_fn=lambda t, a: prx.rst_avg(t),
    ),
    "rst_slope": FnSpec(
        "rst_slope", "gbx_rst_slope", "terrain", _BOTH,
        {"unit": "degrees", "scale": 1.0},
        core_fn=lambda ds, a: terrain.slope(ds, a["unit"], a["scale"]),
        col_fn=lambda t, a: prx.rst_slope(t, a["unit"], a["scale"]),
    ),
    "rst_ndvi": FnSpec(
        "rst_ndvi", "gbx_rst_ndvi", "band-math", _BOTH,
        {"red_band": 1, "nir_band": 2},
        core_fn=lambda ds, a: indices.ndvi(ds, a["red_band"], a["nir_band"]),
        col_fn=lambda t, a: prx.rst_ndvi(t, a["red_band"], a["nir_band"]),
    ),
    "rst_transform": FnSpec(
        "rst_transform", "gbx_rst_transform", "warp", _BOTH,
        {"target_srid": 3857},
        core_fn=lambda ds, a: warp.reproject_to_srid(ds, a["target_srid"]),
        col_fn=lambda t, a: prx.rst_transform(t, a["target_srid"]),
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
        {"name": f.name, "sql_name": f.sql_name, "category": f.category,
         "modes": list(f.modes), "args": f.args}
        for f in REGISTRY.values()
    ]
    Path(path).write_text(json.dumps(data, indent=2))
