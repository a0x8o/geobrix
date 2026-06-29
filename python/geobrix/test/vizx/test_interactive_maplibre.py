"""Task 7 tests: plot_interactive on MapLibre GL; folium retired."""

import builtins

import geopandas as gpd
from shapely.geometry import Point

from databricks.labs.gbx.vizx._interactive import plot_interactive
from databricks.labs.gbx.vizx._layers import vector_layer


def _block_display_channels(monkeypatch):
    """Force both notebook-display channels off so plot_interactive returns the HTML string."""
    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    real_import = builtins.__import__

    def _no_ipython_display(name, *args, **kwargs):
        if name == "IPython.display":
            raise ImportError("IPython.display disabled for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_ipython_display)


def test_interactive_returns_maplibre_html_for_layers(monkeypatch):
    _block_display_channels(monkeypatch)
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    html = plot_interactive([vector_layer(gdf)])
    assert "maplibregl.Map" in html


def test_no_folium_import_in_vizx():
    import importlib
    import os
    import pkgutil
    import subprocess

    import databricks.labs.gbx.vizx as v

    for m in pkgutil.iter_modules(v.__path__):
        src = importlib.import_module(f"databricks.labs.gbx.vizx.{m.name}")
        assert "folium" not in (
            getattr(src, "__file__", "") or ""
        )  # sanity; real check below
    # grep-style: no module imports folium
    root = os.path.dirname(v.__file__)
    out = subprocess.run(
        ["grep", "-rl", "import folium", root], capture_output=True, text=True
    )
    assert out.stdout.strip() == "", f"folium still imported in: {out.stdout}"
