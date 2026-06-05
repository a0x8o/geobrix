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
    assert by_name["rst_slope"]["args"] == {"unit": "degrees", "scale": 1.0}
    assert "core_fn" not in by_name["rst_slope"]  # callables are not serialized


def test_registry_covers_core_accessor_and_terrain_families():
    names = set(s.REGISTRY)
    expected_subset = {
        "rst_width", "rst_height", "rst_numbands", "rst_avg", "rst_min", "rst_max",
        "rst_median", "rst_pixelcount", "rst_slope", "rst_aspect", "rst_hillshade",
        "rst_tri", "rst_tpi", "rst_roughness", "rst_ndvi", "rst_ndwi", "rst_nbr",
        "rst_transform", "rst_to_webmercator",
    }
    missing = expected_subset - names
    assert not missing, f"registry missing: {sorted(missing)}"


def test_every_spec_has_valid_bindings_and_modes():
    for name, fs in s.REGISTRY.items():
        assert fs.sql_name.startswith("gbx_")
        assert fs.modes  # non-empty
        assert callable(fs.core_fn) and callable(fs.col_fn)
