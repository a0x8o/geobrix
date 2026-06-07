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

from pyspark.sql import functions as F

from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import analysis as analysis_core
from databricks.labs.gbx.pyrx.core import (
    coords,
    edit,
    features,
    focal,
    indices,
    ops,
    resample,
    terrain,
    warp,
    xyz,
)

# Fixed 3x3 normalised mean kernel for rst_convolve. Hardcoded identically here
# (Python core_fn + col_fn) and in the Scala BenchDispatch case so the two engines
# convolve with the same coefficients; the bench args carry only `kernel_size` for
# documentation (a 2-D kernel cannot ride the stringly-typed Scala args map).
_CONVOLVE_KERNEL = [[1.0 / 9.0] * 3 for _ in range(3)]


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
    fingerprint: bool = (
        True  # emit a comparable output fingerprint; False = timing-only
    )


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
        fingerprint=False,
    ),
    "rst_memsize": FnSpec(
        "rst_memsize",
        "gbx_rst_memsize",
        "accessor",
        ("pure-core",),
        {},
        # No accessors.memsize: use the in-memory raster buffer length from the
        # open dataset (deterministic; timing-only, fingerprint suppressed).
        core_fn=lambda ds, a: int(ds.read().nbytes),
        col_fn=lambda t, a: prx.rst_memsize(t),
        core=False,
        fingerprint=False,
    ),
    # --- coordinate / index accessors (Task 3) ----------------------------------
    # raster->world is the forward geotransform (pure affine): rasterio.xy and
    # GDAL.toWorldCoord agree for any pixel index in any CRS, so these compare in
    # both modes. The pixel index {x:64, y:64} is inside every corpus tile (256px
    # and 512px). The non-suffixed `...coord` returns the pair [x, y] (a list, not
    # a dict) so the fingerprint is a scalar_list matching the heavy ofArray(x, y).
    "rst_rastertoworldcoordx": FnSpec(
        "rst_rastertoworldcoordx",
        "gbx_rst_rastertoworldcoordx",
        "accessor",
        _BOTH,
        {"x": 64, "y": 64},
        core_fn=lambda ds, a: coords.raster_to_world_x(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_rastertoworldcoordx(t, a["x"], a["y"]),
        core=False,
    ),
    "rst_rastertoworldcoordy": FnSpec(
        "rst_rastertoworldcoordy",
        "gbx_rst_rastertoworldcoordy",
        "accessor",
        _BOTH,
        {"x": 64, "y": 64},
        core_fn=lambda ds, a: coords.raster_to_world_y(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_rastertoworldcoordy(t, a["x"], a["y"]),
        core=False,
    ),
    "rst_rastertoworldcoord": FnSpec(
        "rst_rastertoworldcoord",
        "gbx_rst_rastertoworldcoord",
        "accessor",
        _BOTH,
        {"x": 64, "y": 64},
        # Return the pair as a LIST [x, y] (not the {x, y} dict the binding emits)
        # so fingerprint_output yields a scalar_list, matching the heavy side's
        # ofArray(Array(pair._1, pair._2)). Order: x (easting) then y (northing).
        core_fn=lambda ds, a: [
            coords.raster_to_world_x(ds, a["x"], a["y"]),
            coords.raster_to_world_y(ds, a["x"], a["y"]),
        ],
        col_fn=lambda t, a: prx.rst_rastertoworldcoord(t, a["x"], a["y"]),
        core=False,
    ),
    # world->raster is the INVERSE geotransform. It is exact per-CRS, but a single
    # fixed world literal is in-extent for only one of the corpus CRSs; for the
    # others the index is huge/negative (the EPSG:4326 0.0001-deg grid overflows
    # int32, where rasterio.index floor-casts and GDAL .toInt truncate differently).
    # So these run pure-core-only and their fingerprints are suppressed in the
    # scorecard, exactly like rst_memsize / rst_type. The world point (-73.985,
    # 40.745) is in-extent for the EPSG:4326 (NYC) corpus tiles.
    "rst_worldtorastercoordx": FnSpec(
        "rst_worldtorastercoordx",
        "gbx_rst_worldtorastercoordx",
        "accessor",
        ("pure-core",),
        {"x": -73.985, "y": 40.745},
        core_fn=lambda ds, a: coords.world_to_raster_x(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_worldtorastercoordx(t, a["x"], a["y"]),
        core=False,
        fingerprint=False,
    ),
    "rst_worldtorastercoordy": FnSpec(
        "rst_worldtorastercoordy",
        "gbx_rst_worldtorastercoordy",
        "accessor",
        ("pure-core",),
        {"x": -73.985, "y": 40.745},
        core_fn=lambda ds, a: coords.world_to_raster_y(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_worldtorastercoordy(t, a["x"], a["y"]),
        core=False,
        fingerprint=False,
    ),
    "rst_worldtorastercoord": FnSpec(
        "rst_worldtorastercoord",
        "gbx_rst_worldtorastercoord",
        "accessor",
        ("pure-core",),
        {"x": -73.985, "y": 40.745},
        # Pair as a LIST [col, row] to mirror the heavy ofArray(pair._1, pair._2).
        core_fn=lambda ds, a: [
            coords.world_to_raster_x(ds, a["x"], a["y"]),
            coords.world_to_raster_y(ds, a["x"], a["y"]),
        ],
        col_fn=lambda t, a: prx.rst_worldtorastercoord(t, a["x"], a["y"]),
        core=False,
        fingerprint=False,
    ),
    # rst_tilexyz renders a warped+encoded slippy-map tile. The output bytes depend
    # on the warp/encode stack (GDAL vs rasterio/PIL) and the source CRS, so it is
    # pure-core-only with its fingerprint suppressed in the scorecard. (z, x, y)
    # cover NYC at zoom 12 so the EPSG:4326 corpus tiles intersect the tile bbox.
    "rst_tilexyz": FnSpec(
        "rst_tilexyz",
        "gbx_rst_tilexyz",
        "accessor",
        ("pure-core",),
        {"z": 12, "x": 1205, "y": 1539},
        core_fn=lambda ds, a: xyz.render_tile(
            ds, a["z"], a["x"], a["y"], "PNG", 256, "bilinear"
        ),
        col_fn=lambda t, a: prx.rst_tilexyz(t, a["z"], a["x"], a["y"]),
        core=False,
        fingerprint=False,
    ),
    # --- map / struct accessors (Task 4): timing-only ---------------------------
    # These return maps (metadata, bandmetadata, histogram), structs/dicts
    # (georeference), CRS/encoding-dependent bytes (boundingbox WKB) or
    # gdalinfo-style JSON (summary). None can be made byte- or value-identical
    # cross-engine, so they are TIMED but not compared: fingerprint=False emits an
    # empty fingerprint on both engines and the comparator marks the cell `na`.
    "rst_metadata": FnSpec(
        "rst_metadata",
        "gbx_rst_metadata",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.metadata(ds),
        col_fn=lambda t, a: prx.rst_metadata(t),
        core=False,
        fingerprint=False,
    ),
    "rst_bandmetadata": FnSpec(
        "rst_bandmetadata",
        "gbx_rst_bandmetadata",
        "accessor",
        ("pure-core",),
        {},
        # bandmetadata needs a 1-based band index; band 1 exists in every tile.
        core_fn=lambda ds, a: accessors.bandmetadata(ds, 1),
        col_fn=lambda t, a: prx.rst_bandmetadata(t, 1),
        core=False,
        fingerprint=False,
    ),
    "rst_georeference": FnSpec(
        "rst_georeference",
        "gbx_rst_georeference",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.georeference(ds),
        col_fn=lambda t, a: prx.rst_georeference(t),
        core=False,
        fingerprint=False,
    ),
    "rst_boundingbox": FnSpec(
        "rst_boundingbox",
        "gbx_rst_boundingbox",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.boundingbox(ds),
        col_fn=lambda t, a: prx.rst_boundingbox(t),
        core=False,
        fingerprint=False,
    ),
    "rst_summary": FnSpec(
        "rst_summary",
        "gbx_rst_summary",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.summary(ds),
        col_fn=lambda t, a: prx.rst_summary(t),
        core=False,
        fingerprint=False,
    ),
    "rst_histogram": FnSpec(
        "rst_histogram",
        "gbx_rst_histogram",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.histogram(ds),
        col_fn=lambda t, a: prx.rst_histogram(t),
        core=False,
        fingerprint=False,
    ),
    # --- Task 5: tile-out transforms with scalar / fixed args (13) -----------
    # Each returns a raster tile, so the runner fingerprints the output as a
    # raster (same path as terrain). All run both modes and are compared, EXCEPT
    # rst_resample_to_res (see below). Fixed scalar args are identical across the
    # core_fn, col_fn, and the Scala BenchDispatch case.
    # --- edit (edit.py) ---
    "rst_band": FnSpec(
        "rst_band",
        "gbx_rst_band",
        "edit",
        _BOTH,
        {"band_index": 2},
        min_bands=2,
        core_fn=lambda ds, a: edit.band(ds, a["band_index"]),
        col_fn=lambda t, a: prx.rst_band(t, a["band_index"]),
        core=False,
    ),
    "rst_threshold": FnSpec(
        "rst_threshold",
        "gbx_rst_threshold",
        "edit",
        _BOTH,
        {"op": ">", "value": 0.5},
        core_fn=lambda ds, a: edit.threshold(ds, a["op"], a["value"]),
        col_fn=lambda t, a: prx.rst_threshold(t, a["op"], a["value"]),
        core=False,
    ),
    "rst_initnodata": FnSpec(
        "rst_initnodata",
        "gbx_rst_initnodata",
        "edit",
        _BOTH,
        {},
        core_fn=lambda ds, a: edit.init_nodata(ds),
        col_fn=lambda t, a: prx.rst_initnodata(t),
        core=False,
    ),
    "rst_setsrid": FnSpec(
        "rst_setsrid",
        "gbx_rst_setsrid",
        "edit",
        _BOTH,
        {"srid": 4326},
        core_fn=lambda ds, a: edit.set_srid(ds, a["srid"]),
        col_fn=lambda t, a: prx.rst_setsrid(t, a["srid"]),
        core=False,
    ),
    "rst_updatetype": FnSpec(
        "rst_updatetype",
        "gbx_rst_updatetype",
        "edit",
        _BOTH,
        {"new_type": "Float64"},
        core_fn=lambda ds, a: edit.update_type(ds, a["new_type"]),
        col_fn=lambda t, a: prx.rst_updatetype(t, a["new_type"]),
        core=False,
    ),
    # --- features (features.py) ---
    "rst_fillnodata": FnSpec(
        "rst_fillnodata",
        "gbx_rst_fillnodata",
        "features",
        _BOTH,
        {"max_search_dist": 10.0, "smoothing_iter": 0},
        core_fn=lambda ds, a: features.fill_nodata(
            ds, a["max_search_dist"], a["smoothing_iter"]
        ),
        col_fn=lambda t, a: prx.rst_fillnodata(
            t, a["max_search_dist"], a["smoothing_iter"]
        ),
        core=False,
    ),
    # --- focal (focal.py) ---
    "rst_filter": FnSpec(
        "rst_filter",
        "gbx_rst_filter",
        "focal",
        _BOTH,
        {"kernel_size": 3, "operation": "mean"},
        core_fn=lambda ds, a: focal.filt(ds, a["kernel_size"], a["operation"]),
        col_fn=lambda t, a: prx.rst_filter(t, a["kernel_size"], a["operation"]),
        core=False,
    ),
    "rst_convolve": FnSpec(
        "rst_convolve",
        "gbx_rst_convolve",
        "focal",
        _BOTH,
        # kernel_size documents the fixed 3x3 mean kernel (_CONVOLVE_KERNEL); the
        # actual coefficients are hardcoded identically on both engines.
        {"kernel_size": 3},
        core_fn=lambda ds, a: focal.convolve(ds, _CONVOLVE_KERNEL),
        col_fn=lambda t, a: prx.rst_convolve(
            t, F.array(*[F.array(*[F.lit(c) for c in row]) for row in _CONVOLVE_KERNEL])
        ),
        core=False,
    ),
    # --- format (ops.py / analysis.py) ---
    "rst_asformat": FnSpec(
        "rst_asformat",
        "gbx_rst_asformat",
        "format",
        _BOTH,
        {"new_format": "GTiff"},
        core_fn=lambda ds, a: ops.as_format(ds, a["new_format"]),
        col_fn=lambda t, a: prx.rst_asformat(t, a["new_format"]),
        core=False,
    ),
    "rst_cog_convert": FnSpec(
        "rst_cog_convert",
        "gbx_rst_cog_convert",
        "format",
        _BOTH,
        {
            "compression": "DEFLATE",
            "blocksize": 512,
            "overview_resampling": "AVERAGE",
        },
        core_fn=lambda ds, a: analysis_core.cog_convert(
            ds, a["compression"], a["blocksize"], a["overview_resampling"]
        ),
        col_fn=lambda t, a: prx.rst_cog_convert(
            t, a["compression"], a["blocksize"], a["overview_resampling"]
        ),
        core=False,
    ),
    # --- resample (resample.py) ---
    "rst_resample": FnSpec(
        "rst_resample",
        "gbx_rst_resample",
        "resample",
        _BOTH,
        {"factor": 2.0, "algorithm": "bilinear"},
        core_fn=lambda ds, a: resample.resample_by_factor(
            ds, a["factor"], a["algorithm"]
        ),
        col_fn=lambda t, a: prx.rst_resample(t, a["factor"], a["algorithm"]),
        core=False,
    ),
    "rst_resample_to_size": FnSpec(
        "rst_resample_to_size",
        "gbx_rst_resample_to_size",
        "resample",
        _BOTH,
        {"width_px": 128, "height_px": 128, "algorithm": "bilinear"},
        core_fn=lambda ds, a: resample.resample_to_size(
            ds, a["width_px"], a["height_px"], a["algorithm"]
        ),
        col_fn=lambda t, a: prx.rst_resample_to_size(
            t, a["width_px"], a["height_px"], a["algorithm"]
        ),
        core=False,
    ),
    # rst_resample_to_res takes an absolute ground resolution in CRS units. A
    # single fixed res cannot be sane across the multi-CRS corpus (a 0.0001-deg
    # grid and a 10-m grid share no common absolute resolution; one CRS would
    # produce a degenerate 1x1 raster while another blows up), exactly like the
    # world->raster coord functions. So it runs pure-core-only and its
    # fingerprint is suppressed in the scorecard.
    "rst_resample_to_res": FnSpec(
        "rst_resample_to_res",
        "gbx_rst_resample_to_res",
        "resample",
        ("pure-core",),
        {"x_res": 5.0, "y_res": 5.0, "algorithm": "bilinear"},
        core_fn=lambda ds, a: resample.resample_to_res(
            ds, a["x_res"], a["y_res"], a["algorithm"]
        ),
        col_fn=lambda t, a: prx.rst_resample_to_res(
            t, a["x_res"], a["y_res"], a["algorithm"]
        ),
        core=False,
        fingerprint=False,
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
