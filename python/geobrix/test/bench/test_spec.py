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


def test_full_set_count_is_seventy():
    # 19 representative + 15 Task2 + 7 Task3 + 6 Task4 + 13 Task5 + 10 Task6
    assert len(s.select(set="full")) == 70
