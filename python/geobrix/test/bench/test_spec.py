import json

from databricks.labs.gbx.bench import spec as s


def test_registry_has_representative_functions():
    names = set(s.REGISTRY)
    assert {"rst_width", "rst_avg", "rst_slope", "rst_ndvi", "rst_transform"} <= names


def test_fnspec_fields():
    fs = s.REGISTRY["rst_slope"]
    assert fs.sql_name == "gbx_rst_slope"
    assert fs.category == "terrain"
    assert "pure-core" in fs.modes and "spark-path" in fs.modes
    assert callable(fs.core_fn) and callable(fs.col_fn)


def test_select_filters_by_category_and_name():
    only_acc = s.select(categories=["accessor"])
    assert all(f.category == "accessor" for f in only_acc)
    one = s.select(functions=["rst_slope"])
    assert [f.name for f in one] == ["rst_slope"]


def test_select_by_name_ignores_core_tier():
    # An explicit functions list must ignore the tier: requesting core=False
    # functions by name under the default set="core" must still return them
    # (regression: the core filter previously ran first and yielded zero specs).
    names = ["rst_threshold", "rst_derivedband"]
    for n in names:
        assert s.REGISTRY[n].core is False
    picked = {f.name for f in s.select(functions=names, set="core")}
    assert picked == set(names)


def test_dump_functions_json(tmp_path):
    p = tmp_path / "functions.json"
    s.dump_functions_json(p)
    data = json.loads(p.read_text())
    by_name = {d["name"]: d for d in data}
    assert by_name["rst_slope"]["sql_name"] == "gbx_rst_slope"
    assert by_name["rst_slope"]["args"] == {"unit": "degrees"}
    assert "core_fn" not in by_name["rst_slope"]  # callables are not serialized


def test_registry_covers_core_accessor_and_terrain_families():
    names = set(s.REGISTRY)
    expected_subset = {
        "rst_width",
        "rst_height",
        "rst_numbands",
        "rst_avg",
        "rst_min",
        "rst_max",
        "rst_median",
        "rst_pixelcount",
        "rst_slope",
        "rst_aspect",
        "rst_hillshade",
        "rst_tri",
        "rst_tpi",
        "rst_roughness",
        "rst_ndvi",
        "rst_ndwi",
        "rst_nbr",
        "rst_transform",
        "rst_to_webmercator",
    }
    missing = expected_subset - names
    assert not missing, f"registry missing: {sorted(missing)}"


def test_every_spec_has_valid_bindings_and_modes():
    for name, fs in s.REGISTRY.items():
        assert fs.sql_name.startswith("gbx_")
        assert fs.modes  # non-empty
        assert callable(fs.core_fn) and callable(fs.col_fn)


def test_select_core_is_subset_of_full():
    core = {f.name for f in s.select(set="core")}
    full = {f.name for f in s.select(set="full")}
    assert core
    assert core <= full
    assert len(core) == 19  # core == all current representative functions
    assert "rst_slope" in core and "rst_ndvi" in core


def test_select_defaults_to_core():
    assert {f.name for f in s.select()} == {f.name for f in s.select(set="core")}


def test_full_has_only_registered_names():
    reg = s.registered_rst()
    assert {f.name for f in s.select(set="full")} <= reg
    assert len(reg) == 107  # canonical registered rst_ set


def test_every_full_spec_is_wellformed():
    for f in s.select(set="full"):
        assert f.core_fn is not None and f.col_fn is not None
        assert set(f.modes) <= {"pure-core", "spark-path"}


def test_explicit_functions_filter_ignores_set():
    # naming a function selects it even if not core
    got = {f.name for f in s.select(functions=["rst_width"], set="full")}
    assert got == {"rst_width"}


# --- Task 2: scalar-accessor coverage (15 no-arg accessors) -----------------
_SCALAR_ACCESSORS = {
    "rst_format",
    "rst_getnodata",
    "rst_isempty",
    "rst_memsize",
    "rst_pixelheight",
    "rst_pixelwidth",
    "rst_rotation",
    "rst_scalex",
    "rst_scaley",
    "rst_skewx",
    "rst_skewy",
    "rst_srid",
    "rst_type",
    "rst_upperleftx",
    "rst_upperlefty",
}

# rst_memsize and rst_type cannot produce a cross-engine-identical fingerprint
# (memsize differs file-size vs in-memory; type is a string array with no heavy
# array-of-strings fingerprint constructor), so they run pure-core-only.
_PURE_CORE_ONLY = {"rst_memsize", "rst_type"}


def test_scalar_accessors_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _SCALAR_ACCESSORS - full
    assert not missing, f"registry missing scalar accessors: {sorted(missing)}"


def test_scalar_accessors_wellformed():
    for name in _SCALAR_ACCESSORS:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "accessor"
        assert fs.args == {}
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_scalar_accessors_modes():
    for name in _SCALAR_ACCESSORS:
        fs = s.REGISTRY[name]
        if name in _PURE_CORE_ONLY:
            assert fs.modes == ("pure-core",), name
        else:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_scalar_accessors_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_SCALAR_ACCESSORS & core)


# --- Task 3: coordinate / index accessors (7) -------------------------------
_COORD_ACCESSORS = {
    "rst_rastertoworldcoord",
    "rst_rastertoworldcoordx",
    "rst_rastertoworldcoordy",
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_tilexyz",
}

# raster->world is pure affine (forward geotransform), so rasterio.xy and
# GDAL.toWorldCoord agree for any pixel index in any CRS -> both modes, compared.
_COORD_BOTH = {
    "rst_rastertoworldcoord",
    "rst_rastertoworldcoordx",
    "rst_rastertoworldcoordy",
}
# world->raster cannot be made cross-engine-consistent over the multi-CRS corpus:
# a single fixed world literal is in-extent for only one CRS, and for the others
# the inverse-affine index is huge/negative (the EPSG:4326 0.0001-deg grid even
# overflows int32, where rasterio.index floor-casts and GDAL .toInt truncate
# differently). rst_tilexyz renders a warped+encoded image whose bytes depend on
# the warp/encode stack (GDAL vs rasterio/PIL) and the source CRS. All four run
# pure-core-only; their fingerprints are suppressed in the scorecard, exactly as
# rst_memsize / rst_type are.
_COORD_PURE_CORE_ONLY = {
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_tilexyz",
}


def test_coord_accessors_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _COORD_ACCESSORS - full
    assert not missing, f"registry missing coord accessors: {sorted(missing)}"


def test_coord_accessors_wellformed():
    for name in _COORD_ACCESSORS:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "accessor"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_coord_accessors_modes():
    for name in _COORD_ACCESSORS:
        fs = s.REGISTRY[name]
        if name in _COORD_PURE_CORE_ONLY:
            assert fs.modes == ("pure-core",), name
        else:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_coord_accessors_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_COORD_ACCESSORS & core)


# --- Task 4: timing-only fingerprint flag + map/struct coverage (6) ---------
# Map/struct outputs (metadata maps, georeference dicts, bbox WKB, gdalinfo JSON,
# per-band histograms) cannot be made byte- or value-identical cross-engine, so
# they are timed but never compared: fingerprint=False suppresses the fingerprint
# on BOTH sides, and an empty fingerprint compares as `na` (not divergent).
_MAP_STRUCT = {
    "rst_metadata",
    "rst_bandmetadata",
    "rst_georeference",
    "rst_boundingbox",
    "rst_summary",
    "rst_histogram",
}

# The 6 prior ad-hoc downgrades (Task 2/3) now ride the same flag instead of
# emitting a real-but-suppressed fingerprint.
_RETROFITTED = {
    "rst_memsize",
    "rst_type",
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_tilexyz",
}


def test_fnspec_fingerprint_defaults_true():
    fs = s.FnSpec("x", "gbx_x", "accessor", ("pure-core",))
    assert fs.fingerprint is True


def test_map_struct_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _MAP_STRUCT - full
    assert not missing, f"registry missing map/struct fns: {sorted(missing)}"


def test_map_struct_wellformed_timing_only():
    for name in _MAP_STRUCT:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "accessor"
        assert fs.core is False
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name
        assert callable(fs.core_fn) and callable(fs.col_fn)


def test_map_struct_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_MAP_STRUCT & core)


def test_retrofitted_downgrades_are_timing_only():
    for name in _RETROFITTED:
        fs = s.REGISTRY[name]
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name


# --- Task 5: tile-out transforms with scalar / fixed args (13) ---------------
# Each produces a raster tile, so its output is compared via the raster
# fingerprint (same path as terrain). All are core=False and fingerprint=True
# (both modes, compared) EXCEPT rst_resample_to_res: a single fixed ground
# resolution cannot be sane across the multi-CRS corpus (a 0.0001-deg grid and a
# 10-m grid have no common absolute resolution), exactly like the world->raster
# coord functions, so it runs pure-core-only with its fingerprint suppressed.
_TILE_OUT_SCALAR = {
    "rst_band",
    "rst_threshold",
    "rst_initnodata",
    "rst_setsrid",
    "rst_updatetype",
    "rst_fillnodata",
    "rst_filter",
    "rst_convolve",
    "rst_asformat",
    "rst_cog_convert",
    "rst_resample",
    "rst_resample_to_res",
    "rst_resample_to_size",
}

_TILE_OUT_PURE_CORE_ONLY = {"rst_resample_to_res"}

# rst_threshold runs BOTH modes (timed) but is never fingerprint-compared: the
# two tiers implement different, documented output contracts (heavy binarises to a
# single-band 0/1 mask; light keeps passing pixels and preserves all bands). A
# by-design contract difference, not a bug — so it is timing-only.
_TILE_OUT_TIMING_ONLY = {"rst_threshold"}


def test_tile_out_scalar_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _TILE_OUT_SCALAR - full
    assert not missing, f"registry missing tile-out fns: {sorted(missing)}"


def test_tile_out_scalar_wellformed():
    for name in _TILE_OUT_SCALAR:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_tile_out_scalar_modes_and_fingerprint():
    for name in _TILE_OUT_SCALAR:
        fs = s.REGISTRY[name]
        if name in _TILE_OUT_PURE_CORE_ONLY:
            assert fs.modes == ("pure-core",), name
            assert fs.fingerprint is False, name
        elif name in _TILE_OUT_TIMING_ONLY:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
            assert fs.fingerprint is False, name
        else:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
            assert fs.fingerprint is True, name


def test_tile_out_scalar_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_TILE_OUT_SCALAR & core)


def test_rst_band_requires_two_bands():
    assert s.REGISTRY["rst_band"].min_bands == 2


# --- Task 6: tile-out transforms with geometry/expression/band-map/func (10) -
# All produce a raster tile, all core=False. SIX run both modes and are compared
# (fingerprint=True): the args are CRS-/band-count-independent and the two engines
# run the same algorithm. FOUR run pure-core-only (fingerprint=False) because no
# single literal geometry/observer/point is in-extent across the multi-CRS corpus
# (clip, sample, viewshed) and/or the engines use different interpolation/scan
# algorithms (color_relief np.interp vs GDAL DEMProcessing; viewshed xrspatial vs
# GDAL sweep).
_COMPLEX_ARG = {
    "rst_evi",
    "rst_savi",
    "rst_index",
    "rst_mapalgebra",
    "rst_derivedband",
    "rst_clip",
    "rst_color_relief",
    "rst_proximity",
    "rst_viewshed",
    "rst_sample",
}

_COMPLEX_FULL = {
    "rst_evi",
    "rst_savi",
    "rst_index",
    "rst_mapalgebra",
    "rst_derivedband",
    "rst_proximity",
}

_COMPLEX_TIMING_ONLY = {
    "rst_clip",
    "rst_color_relief",
    "rst_viewshed",
    "rst_sample",
}


def test_complex_arg_partition_is_total():
    # the full + timing-only sets exactly partition the 10 Task-6 functions
    assert _COMPLEX_FULL | _COMPLEX_TIMING_ONLY == _COMPLEX_ARG
    assert not (_COMPLEX_FULL & _COMPLEX_TIMING_ONLY)
    assert len(_COMPLEX_ARG) == 10
    assert len(_COMPLEX_FULL) == 6
    assert len(_COMPLEX_TIMING_ONLY) == 4


def test_complex_arg_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _COMPLEX_ARG - full
    assert not missing, f"registry missing complex-arg fns: {sorted(missing)}"


def test_complex_arg_wellformed():
    for name in _COMPLEX_ARG:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_complex_arg_full_comparison_modes_and_fingerprint():
    for name in _COMPLEX_FULL:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
        assert fs.fingerprint is True, name


def test_complex_arg_timing_only_modes_and_fingerprint():
    for name in _COMPLEX_TIMING_ONLY:
        fs = s.REGISTRY[name]
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name


def test_complex_arg_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_COMPLEX_ARG & core)


def test_complex_arg_band_index_specs_require_two_bands():
    # evi/savi/index all read a 2nd band on the 2-band corpus
    for name in ("rst_evi", "rst_savi", "rst_index"):
        assert s.REGISTRY[name].min_bands == 2, name


def test_full_set_count_is_eighty_four():
    # 19 representative + 15 Task2 + 7 Task3 + 6 Task4 + 13 Task5 + 10 Task6
    # + 6 bucket-C group C1/C2 (4 readers/overviews + 2 subdataset)
    # + 3 bucket-C group C3 (multi-tile: frombands/combineavg/merge)
    # + 5 bucket-C group C4 (tiling: maketiles/retile/tooverlappingtiles/
    #   separatebands/xyzpyramid -> raster_collection fingerprint)
    assert len(s.select(set="full")) == 84


# --- bucket C, group C1/C2: readers + buildoverviews + subdataset fns (6) ----
# C1 (compared): rst_tryopen (bytes->scalar), rst_fromcontent (bytes->raster),
# rst_fromfile (path->raster), rst_buildoverviews (tile->raster). C2 (timing-
# only): rst_subdatasets, rst_getsubdataset — a plain GTiff corpus tile has no
# subdatasets (empty map / no match), so they are timed but never compared.
#
# These introduce the `input_kind` adapter on FnSpec: the bytes/path readers do
# NOT receive an opened dataset — the runner hands core_fn the raw raster bytes
# ("bytes") or the corpus file path ("path") instead of an open ds ("tile",
# the default that preserves every pre-existing function).
_C1 = {"rst_tryopen", "rst_fromcontent", "rst_fromfile", "rst_buildoverviews"}
_C2 = {"rst_subdatasets", "rst_getsubdataset"}

_C1_INPUT_KIND = {
    "rst_tryopen": "bytes",
    "rst_fromcontent": "bytes",
    "rst_fromfile": "path",
    "rst_buildoverviews": "tile",
}


def test_fnspec_input_kind_defaults_tile():
    fs = s.FnSpec("x", "gbx_x", "accessor", ("pure-core",))
    assert fs.input_kind == "tile"


def test_bucket_c_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = (_C1 | _C2) - full
    assert not missing, f"registry missing bucket-C fns: {sorted(missing)}"


def test_c1_wellformed_input_kind_and_fingerprint():
    for name in _C1:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert fs.input_kind == _C1_INPUT_KIND[name], name
        assert fs.fingerprint is True, name
        assert "pure-core" in fs.modes, name


def test_c1_compared_modes():
    # tryopen, fromcontent, buildoverviews run both modes; fromfile is pure-core
    # only because the spark-path tile DataFrame carries no file-path column.
    for name in ("rst_tryopen", "rst_fromcontent", "rst_buildoverviews"):
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
    assert s.REGISTRY["rst_fromfile"].modes == ("pure-core",)


def test_c2_timing_only():
    for name in _C2:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert fs.fingerprint is False, name
        assert fs.modes == ("pure-core",), name
        assert fs.input_kind == "tile", name


def test_bucket_c_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not ((_C1 | _C2) & core)


# --- bucket C, group C3: multi-tile-input functions (3) ----------------------
# rst_frombands / rst_combineavg / rst_merge each consume an ARRAY of tiles. The
# corpus row gives ONE tile, so the runner SYNTHESIZES the multi-tile input from
# it (bench.synth) and writes it to disk ONCE, so both engines read byte-identical
# files. These introduce input_kind == "tile_array": the runner synthesizes, opens
# each into a ds list, and feeds core_fn(ds_list, args); the col_fn builds a Spark
# ARRAY<tile> column from the same synthesized tiles. All produce a raster tile, so
# the output is compared via the raster fingerprint, and all run both modes.
_C3 = {"rst_frombands", "rst_combineavg", "rst_merge"}

_C3_SYNTH = {
    "rst_frombands": "frombands",
    "rst_combineavg": "combineavg",
    "rst_merge": "merge",
}


def test_c3_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _C3 - full
    assert not missing, f"registry missing C3 fns: {sorted(missing)}"


def test_c3_wellformed_tile_array_and_fingerprint():
    for name in _C3:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert fs.input_kind == "tile_array", name
        assert fs.fingerprint is True, name
        assert fs.sources, name


def test_c3_both_modes():
    for name in _C3:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_c3_synth_recipe_maps_to_known_recipe():
    from databricks.labs.gbx.bench import synth

    for name, recipe in _C3_SYNTH.items():
        assert s.synth_recipe(name) == recipe, name
        # the recipe is one the synthesizer actually implements
        assert recipe in synth._RECIPES


def test_c3_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_C3 & core)


# --- bucket C, group C4: tiling functions (5) --------------------------------
# rst_maketiles / rst_retile / rst_tooverlappingtiles / rst_separatebands /
# rst_xyzpyramid each take ONE tile and emit a COLLECTION of tiles. They run on
# the default input_kind == "tile" (a single open dataset), but their core_fn
# returns a LIST of tile bytes, so the runner fingerprints the list with the new
# `raster_collection` fingerprint (tile count + pooled, order-independent agg).
# All run both modes (the spark-path col_fn yields an ARRAY column).
_C4 = {
    "rst_maketiles",
    "rst_retile",
    "rst_tooverlappingtiles",
    "rst_separatebands",
    "rst_xyzpyramid",
}

# rst_xyzpyramid is a tile-in / collection-out function like the rest of C4, but
# its emitted tiles are slippy-map XYZ renders: each is one rst_tilexyz render
# (the pyramid just loops RST_TileXYZ.execute over the intersecting (z,x,y) set).
# Like rst_tilexyz, the rendered bytes are render-engine-specific (heavy GDAL
# gdal_translate -of PNG vs light rio-tiler/PIL RGBA), so they cannot be made
# pooled-pixel-identical cross-engine. The intersecting tile COUNT already agrees,
# so the cell is TIMED but never pixel-compared: timing-only (pure-core,
# fingerprint=False), exactly like rst_tilexyz.
#
# Canonical-render note (follow-up, not fixed here): "XYZ pyramid" is the
# web-mercator slippy-map DISPLAY convention -> a rescaled RGBA PNG (0-255) is the
# canonical output, which is what the lightweight rio-tiler tier emits. The
# heavyweight RST_TileXYZ pipes raw source values through gdal_translate -of PNG
# (no rescale to RGBA), which is the NON-canonical render. A heavy-tier fix to
# emit a true RGBA web-map tile is deferred (Scala change; tracked separately).
_C4_TIMING_ONLY = {"rst_xyzpyramid"}
_C4_COMPARED = _C4 - _C4_TIMING_ONLY

_C4_ARGS = {
    "rst_maketiles": {"size_in_mb"},
    "rst_retile": {"tile_width", "tile_height"},
    "rst_tooverlappingtiles": {"tile_width", "tile_height", "overlap"},
    "rst_separatebands": set(),
    "rst_xyzpyramid": {"min_z", "max_z"},
}


def test_c4_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _C4 - full
    assert not missing, f"registry missing C4 fns: {sorted(missing)}"


def test_c4_wellformed_tile_in_collection_out():
    for name in _C4:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        # C4 takes a single open dataset (default input_kind), not a tile_array.
        assert fs.input_kind == "tile", name
        assert fs.sources, name


def test_c4_compared_fns_fingerprint_true():
    for name in _C4_COMPARED:
        fs = s.REGISTRY[name]
        assert fs.fingerprint is True, name


def test_c4_xyzpyramid_is_timing_only():
    # Render-engine-specific bytes (GDAL vs rio-tiler/PIL) -> count agrees,
    # pixels cannot match; timed but not compared, like rst_tilexyz.
    for name in _C4_TIMING_ONLY:
        fs = s.REGISTRY[name]
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name


def test_c4_compared_both_modes():
    for name in _C4_COMPARED:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_c4_args_present():
    for name, keys in _C4_ARGS.items():
        fs = s.REGISTRY[name]
        assert set(fs.args.keys()) == keys, name


def test_c4_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_C4 & core)


def test_every_fnspec_declares_existing_sources():
    from pathlib import Path

    root = Path(__file__).resolve()
    for _ in range(12):
        if (root / "pom.xml").exists():
            break
        root = root.parent
    for f in s.select(set="full"):
        assert f.sources, f"{f.name} has no sources"
        for p in f.sources:
            assert (root / p).exists(), f"{f.name}: missing source {p}"
