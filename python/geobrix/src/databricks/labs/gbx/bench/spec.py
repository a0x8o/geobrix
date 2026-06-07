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
    min_bands: int = 1
    core_fn: Callable = None  # (ds, args) -> Any
    col_fn: Callable = None  # (tile_col, args) -> Column
    core: bool = False  # in the fast "core" benchmark set


_BOTH = ("pure-core", "spark-path")

REGISTRY: Dict[str, FnSpec] = {
    "rst_width": FnSpec(
        "rst_width",
        "gbx_rst_width",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.width(ds),
        col_fn=lambda t, a: prx.rst_width(t),
    ),
    "rst_avg": FnSpec(
        "rst_avg",
        "gbx_rst_avg",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.avg(ds),
        col_fn=lambda t, a: prx.rst_avg(t),
    ),
    "rst_slope": FnSpec(
        "rst_slope",
        "gbx_rst_slope",
        "terrain",
        _BOTH,
        {"unit": "degrees"},
        core=True,
        core_fn=lambda ds, a: terrain.slope(ds, a["unit"]),
        col_fn=lambda t, a: prx.rst_slope(t, a["unit"]),
    ),
    "rst_ndvi": FnSpec(
        "rst_ndvi",
        "gbx_rst_ndvi",
        "band-math",
        _BOTH,
        {"red_band": 1, "nir_band": 2},
        min_bands=2,
        core=True,
        core_fn=lambda ds, a: indices.ndvi(ds, a["red_band"], a["nir_band"]),
        col_fn=lambda t, a: prx.rst_ndvi(t, a["red_band"], a["nir_band"]),
    ),
    "rst_transform": FnSpec(
        "rst_transform",
        "gbx_rst_transform",
        "warp",
        _BOTH,
        {"target_srid": 3857},
        core=True,
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
        core=True,
        core_fn=lambda ds, a: accessors.height(ds),
        col_fn=lambda t, a: prx.rst_height(t),
    ),
    "rst_numbands": FnSpec(
        "rst_numbands",
        "gbx_rst_numbands",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.numbands(ds),
        col_fn=lambda t, a: prx.rst_numbands(t),
    ),
    "rst_min": FnSpec(
        "rst_min",
        "gbx_rst_min",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.minimum(ds),
        col_fn=lambda t, a: prx.rst_min(t),
    ),
    "rst_max": FnSpec(
        "rst_max",
        "gbx_rst_max",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.maximum(ds),
        col_fn=lambda t, a: prx.rst_max(t),
    ),
    "rst_median": FnSpec(
        "rst_median",
        "gbx_rst_median",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.median(ds),
        col_fn=lambda t, a: prx.rst_median(t),
    ),
    "rst_pixelcount": FnSpec(
        "rst_pixelcount",
        "gbx_rst_pixelcount",
        "accessor",
        _BOTH,
        {},
        core=True,
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
        core=True,
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
        core=True,
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
        core=True,
        core_fn=lambda ds, a: terrain.tri(ds),
        col_fn=lambda t, a: prx.rst_tri(t),
    ),
    "rst_tpi": FnSpec(
        "rst_tpi",
        "gbx_rst_tpi",
        "terrain",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: terrain.tpi(ds),
        col_fn=lambda t, a: prx.rst_tpi(t),
    ),
    "rst_roughness": FnSpec(
        "rst_roughness",
        "gbx_rst_roughness",
        "terrain",
        _BOTH,
        {},
        core=True,
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
        min_bands=2,
        core=True,
        core_fn=lambda ds, a: indices.ndwi(ds, a["green_idx"], a["nir_idx"]),
        col_fn=lambda t, a: prx.rst_ndwi(t, a["green_idx"], a["nir_idx"]),
    ),
    "rst_nbr": FnSpec(
        "rst_nbr",
        "gbx_rst_nbr",
        "band-math",
        _BOTH,
        {"nir_idx": 1, "swir_idx": 2},
        min_bands=2,
        core=True,
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
        core=True,
        core_fn=lambda ds, a: warp.reproject_to_srid(
            ds, 3857, resampling=a["resampling"]
        ),
        col_fn=lambda t, a: prx.rst_to_webmercator(t, a["resampling"]),
    ),
    # --- scalar accessors (no args; accessors.py) -------------------------------
    # All core=False. Most produce a cross-engine-identical scalar/array
    # fingerprint, so they run both modes. Two exceptions run pure-core-only:
    #   - rst_memsize: heavy returns the on-disk file size while the lightweight
    #     side opens a vsimem MemoryFile (no file size), so the values cannot be
    #     made identical; its fingerprint is suppressed in the scorecard.
    #   - rst_type: a per-band string array; BenchFingerprint has no
    #     array-of-strings constructor to match on the heavy side.
    "rst_srid": FnSpec(
        "rst_srid",
        "gbx_rst_srid",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.srid(ds),
        col_fn=lambda t, a: prx.rst_srid(t),
        core=False,
    ),
    "rst_pixelwidth": FnSpec(
        "rst_pixelwidth",
        "gbx_rst_pixelwidth",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.pixelwidth(ds),
        col_fn=lambda t, a: prx.rst_pixelwidth(t),
        core=False,
    ),
    "rst_pixelheight": FnSpec(
        "rst_pixelheight",
        "gbx_rst_pixelheight",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.pixelheight(ds),
        col_fn=lambda t, a: prx.rst_pixelheight(t),
        core=False,
    ),
    "rst_upperleftx": FnSpec(
        "rst_upperleftx",
        "gbx_rst_upperleftx",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.upperleftx(ds),
        col_fn=lambda t, a: prx.rst_upperleftx(t),
        core=False,
    ),
    "rst_upperlefty": FnSpec(
        "rst_upperlefty",
        "gbx_rst_upperlefty",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.upperlefty(ds),
        col_fn=lambda t, a: prx.rst_upperlefty(t),
        core=False,
    ),
    "rst_scalex": FnSpec(
        "rst_scalex",
        "gbx_rst_scalex",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.scalex(ds),
        col_fn=lambda t, a: prx.rst_scalex(t),
        core=False,
    ),
    "rst_scaley": FnSpec(
        "rst_scaley",
        "gbx_rst_scaley",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.scaley(ds),
        col_fn=lambda t, a: prx.rst_scaley(t),
        core=False,
    ),
    "rst_skewx": FnSpec(
        "rst_skewx",
        "gbx_rst_skewx",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.skewx(ds),
        col_fn=lambda t, a: prx.rst_skewx(t),
        core=False,
    ),
    "rst_skewy": FnSpec(
        "rst_skewy",
        "gbx_rst_skewy",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.skewy(ds),
        col_fn=lambda t, a: prx.rst_skewy(t),
        core=False,
    ),
    "rst_rotation": FnSpec(
        "rst_rotation",
        "gbx_rst_rotation",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.rotation(ds),
        col_fn=lambda t, a: prx.rst_rotation(t),
        core=False,
    ),
    "rst_isempty": FnSpec(
        "rst_isempty",
        "gbx_rst_isempty",
        "accessor",
        _BOTH,
        {},
        # Coerce the bool to 1.0/0.0 so the scalar fingerprint matches the heavy
        # side, which serializes RST_IsEmpty as ofScalar(1.0/0.0).
        core_fn=lambda ds, a: 1.0 if accessors.isempty(ds) else 0.0,
        col_fn=lambda t, a: prx.rst_isempty(t),
        core=False,
    ),
    "rst_getnodata": FnSpec(
        "rst_getnodata",
        "gbx_rst_getnodata",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.getnodata(ds),
        col_fn=lambda t, a: prx.rst_getnodata(t),
        core=False,
    ),
    "rst_format": FnSpec(
        "rst_format",
        "gbx_rst_format",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.format(ds),
        col_fn=lambda t, a: prx.rst_format(t),
        core=False,
    ),
    "rst_type": FnSpec(
        "rst_type",
        "gbx_rst_type",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.type(ds),
        col_fn=lambda t, a: prx.rst_type(t),
        core=False,
    ),
    "rst_memsize": FnSpec(
        "rst_memsize",
        "gbx_rst_memsize",
        "accessor",
        ("pure-core",),
        {},
        # No accessors.memsize: use the in-memory raster buffer length from the
        # open dataset (deterministic; fingerprint suppressed downstream).
        core_fn=lambda ds, a: int(ds.read().nbytes),
        col_fn=lambda t, a: prx.rst_memsize(t),
        core=False,
    ),
}


def select(
    functions: List[str] = None,
    categories: List[str] = None,
    set: str = "core",
) -> List[FnSpec]:
    """Select FnSpecs from the registry.

    ``set`` chooses the tier: ``"core"`` (the fast default, only ``core=True``
    entries) or ``"full"`` (every registered FnSpec). An explicit ``functions``
    list selects by name and ignores the tier. Note: the ``set`` parameter
    shadows the builtin within this function, so use ``frozenset(...)`` for the
    membership sets below.
    """
    out = list(REGISTRY.values())
    if set == "core":
        out = [f for f in out if f.core]
    # set == "full": no core filter
    if functions:
        names = frozenset(functions)  # frozenset, NOT set(...) -- `set` is a param name
        out = [f for f in out if f.name in names]
    if categories:
        cats = frozenset(categories)
        out = [f for f in out if f.category in cats]
    return out


def registered_rst() -> "frozenset[str]":
    """Canonical registered rst_* function names (the 107) from registered_functions.txt."""
    from pathlib import Path

    # resolve repo root robustly from this file's location
    root = Path(__file__).resolve()
    cand = None
    for _ in range(12):
        c = root / "docs" / "tests-function-info" / "registered_functions.txt"
        if c.exists():
            cand = c
            break
        root = root.parent
    if cand is None:
        raise FileNotFoundError("registered_functions.txt not found above " + __file__)
    names = set()
    for line in cand.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = s[4:] if s.startswith("gbx_") else s
        if s.startswith("rst_"):
            names.add(s)
    return frozenset(names)


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
