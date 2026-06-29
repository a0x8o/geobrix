"""Tests for gbx.vizx public API exports (Task 12)."""

import databricks.labs.gbx.vizx as vizx


def test_new_public_symbols_exported():
    """Layer constructors, simplify helpers, and audit are accessible from the package."""
    for name in (
        "vector_layer",
        "raster_layer",
        "grid_layer",
        "pmtiles_layer",
        "simplify_tiles_from_source",
        "simplify_tiles_from_archive",
        "audit_layers",
    ):
        assert hasattr(vizx, name), f"missing: {name}"
        assert name in vizx.__all__, f"not in __all__: {name}"


def test_sri_hashes_are_real():
    """SRI hash constants are real sha384 values — no placeholder remains."""
    from databricks.labs.gbx.vizx import _maplibre as m

    assert m._MAPLIBRE_JS_SRI.startswith("sha384-"), (
        f"_MAPLIBRE_JS_SRI is not a real hash: {m._MAPLIBRE_JS_SRI!r}"
    )
    assert "REPLACE" not in m._MAPLIBRE_JS_SRI, (
        f"_MAPLIBRE_JS_SRI still contains placeholder: {m._MAPLIBRE_JS_SRI!r}"
    )
    assert m._PMTILES_JS_SRI.startswith("sha384-"), (
        f"_PMTILES_JS_SRI is not a real hash: {m._PMTILES_JS_SRI!r}"
    )
    assert "REPLACE" not in m._PMTILES_JS_SRI, (
        f"_PMTILES_JS_SRI still contains placeholder: {m._PMTILES_JS_SRI!r}"
    )


def test_build_html_contains_pinned_version_and_sri():
    """build_html output references the pinned maplibre + pmtiles versions with SRI."""
    from databricks.labs.gbx.vizx._maplibre import build_html

    html = build_html([])
    assert "maplibre-gl@4.7.1" in html
    assert "pmtiles@3.2.0" in html
    assert "REPLACE" not in html
    # integrity attributes are present (real SRI hashes)
    assert 'integrity="sha384-' in html


def test_layer_constructors_return_layer_objects():
    """Layer constructors from _layers are importable and return Layer instances."""
    from databricks.labs.gbx.vizx._layers import Layer

    assert isinstance(vizx.vector_layer(None), Layer)
    assert isinstance(vizx.raster_layer(None), Layer)
    assert isinstance(vizx.grid_layer(None, grid_system="h3"), Layer)
    assert isinstance(vizx.pmtiles_layer(b""), Layer)


def test_build_pmtiles_html_is_gone():
    """_build_pmtiles_html and duplicate CDN constants were removed from _pmtiles."""
    import databricks.labs.gbx.vizx._pmtiles as p

    assert not hasattr(p, "_build_pmtiles_html"), (
        "_build_pmtiles_html was not removed from _pmtiles.py"
    )
    assert not hasattr(p, "_MAPLIBRE_JS"), (
        "duplicate _MAPLIBRE_JS constant was not removed from _pmtiles.py"
    )
    assert not hasattr(p, "_PMTILES_JS"), (
        "duplicate _PMTILES_JS constant was not removed from _pmtiles.py"
    )
    assert not hasattr(p, "_MAPLIBRE_CSS"), (
        "duplicate _MAPLIBRE_CSS constant was not removed from _pmtiles.py"
    )
