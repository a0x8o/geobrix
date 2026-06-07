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
