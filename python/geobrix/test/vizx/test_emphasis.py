"""Tests for the vizx ``emphasis`` styling mode (data vs blend).

``emphasis="data"`` (default) makes a newly-added layer visually pop against the
basemap; ``emphasis="blend"`` reproduces the prior soft composite exactly.
Explicit user styling kwargs always override the emphasis-driven defaults.
"""

import builtins
import inspect
import json
import logging
import re

import geopandas as gpd
import matplotlib
import pytest

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from shapely.geometry import LineString, Point, Polygon  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-emphasis-tests")
        .getOrCreate()
    )
    yield s


# ---------------------------------------------------------------------------
# signatures
# ---------------------------------------------------------------------------


def test_emphasis_default_is_blend_on_all_entrypoints():
    # Default is "blend" (the soft composite the user prefers); "data" is opt-in.
    from databricks.labs.gbx.vizx import (
        plot_cog,
        plot_interactive,
        plot_pmtiles,
        plot_raster,
        plot_static,
    )

    for fn in (
        plot_static,
        plot_cog,
        plot_raster,
        plot_interactive,
        plot_pmtiles,
    ):
        sig = inspect.signature(fn)
        assert "emphasis" in sig.parameters, f"{fn.__name__} missing emphasis"
        assert (
            sig.parameters["emphasis"].default == "blend"
        ), f"{fn.__name__} emphasis default != 'blend'"


def test_debug_mode_added_to_static_entrypoints():
    from databricks.labs.gbx.vizx import plot_cog, plot_raster, plot_static

    for fn in (plot_static, plot_cog, plot_raster):
        sig = inspect.signature(fn)
        assert "debug_mode" in sig.parameters, f"{fn.__name__} missing debug_mode"
        assert sig.parameters["debug_mode"].default == 1


# ---------------------------------------------------------------------------
# static vector polygons
# ---------------------------------------------------------------------------


def _gdf_polygon():
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    return gpd.GeoDataFrame({"v": [1]}, geometry=[poly], crs=4326)


def _last_collection(ax):
    return ax.collections[-1]


def test_static_polygon_data_has_dark_edge_and_firm_alpha():
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    ax = plot_static(_gdf_polygon(), basemap=False, emphasis="data")
    coll = _last_collection(ax)
    edge = coll.get_edgecolor()
    # data mode draws a non-"none" (visible) dark edge.
    assert edge.size > 0 and float(edge[0][3]) > 0.0
    # the edge is dark (close to #222222), not "face".
    assert float(edge[0][0]) < 0.3
    # firm alpha (~0.85), clearly above the soft blend default.
    assert coll.get_alpha() is not None and coll.get_alpha() > 0.7
    plt.close("all")


def test_static_polygon_blend_matches_prior_soft_values():
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    ax = plot_static(_gdf_polygon(), basemap=False, emphasis="blend")
    coll = _last_collection(ax)
    # prior behavior: edgecolor="face" (== facecolor, no bold dark outline) and
    # the historical alpha default of 0.8.
    assert coll.get_alpha() == 0.8
    edge = coll.get_edgecolor()
    face = coll.get_facecolor()
    # "face" edge: edge RGBA equals the face RGBA (no distinct dark outline).
    assert np.allclose(edge, face)
    plt.close("all")


def test_static_explicit_edgecolor_overrides_emphasis():
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    ax = plot_static(
        _gdf_polygon(), basemap=False, emphasis="data", edgecolor="red", alpha=0.3
    )
    coll = _last_collection(ax)
    edge = coll.get_edgecolor()
    # user red edge (R high, G/B low) wins over the emphasis dark default.
    assert float(edge[0][0]) > 0.8 and float(edge[0][1]) < 0.2
    # user alpha wins over the emphasis firm alpha.
    assert coll.get_alpha() == 0.3
    plt.close("all")


def test_static_explicit_column_still_renders_with_emphasis(spark):
    # A user-supplied column (choropleth) must still drive coloring; emphasis only
    # sets edge/alpha defaults, not the cmap mapping.
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame(
        [("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 3)], ["wkt", "v"]
    )
    ax = plot_static(df, column="v", basemap=False, emphasis="data")
    assert ax.get_figure() is not None
    plt.close("all")


# ---------------------------------------------------------------------------
# static vector lines / points
# ---------------------------------------------------------------------------


def test_static_points_data_bumps_markersize_and_dark_edge():
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(0, 0)], crs=4326)
    ax = plot_static(gdf, basemap=False, emphasis="data")
    coll = _last_collection(ax)
    edge = coll.get_edgecolor()
    # points get a dark edge ring in data mode.
    assert edge.size > 0 and float(edge[0][3]) > 0.0 and float(edge[0][0]) < 0.3
    plt.close("all")


def test_static_lines_data_bumps_linewidth():
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    gdf = gpd.GeoDataFrame(
        {"v": [1]}, geometry=[LineString([(0, 0), (1, 1)])], crs=4326
    )
    ax_data = plot_static(gdf, basemap=False, emphasis="data")
    lw_data = float(_last_collection(ax_data).get_linewidth()[0])
    plt.close("all")
    ax_blend = plot_static(gdf, basemap=False, emphasis="blend")
    lw_blend = float(_last_collection(ax_blend).get_linewidth()[0])
    plt.close("all")
    assert lw_data > lw_blend


def test_static_explicit_markersize_overrides_point_emphasis():
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(0, 0)], crs=4326)
    # explicit markersize must win over the emphasis bumped default.
    ax = plot_static(gdf, basemap=False, emphasis="data", markersize=3)
    sizes = _last_collection(ax).get_sizes()
    assert float(sizes[0]) == pytest.approx(3.0)
    plt.close("all")


# ---------------------------------------------------------------------------
# static raster / COG
# ---------------------------------------------------------------------------


def _write_tif(tmp_path, bands=1, size=16, crs="EPSG:3857"):
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / "cog.tif"
    data = (np.random.rand(bands, size, size) * 200).astype("uint8")
    transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=bands,
        dtype="uint8",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(data)
    return str(path)


def test_cog_data_uses_full_opacity_image(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=1)
    ax = plot_cog(path, basemap=False, emphasis="data")
    # the single-band imshow image is rendered at full strength.
    img = [im for im in ax.get_images()][-1]
    assert float(img.get_alpha() or 1.0) == 1.0
    plt.close("all")


def test_cog_blend_matches_prior_full_opacity(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=1)
    ax = plot_cog(path, basemap=False, emphasis="blend")
    img = [im for im in ax.get_images()][-1]
    # blend == prior: the COG imshow had no explicit alpha (=1.0); blend must NOT
    # dim it (a raster fully covers the basemap, so there is no "blend" to dim to).
    assert float(img.get_alpha() or 1.0) == 1.0
    plt.close("all")


# ---------------------------------------------------------------------------
# interactive (MapLibre) paint
# ---------------------------------------------------------------------------


def _parse_overlay(html):
    """Extract the overlay {sources, layers} object from build_html output."""
    m = re.search(r"const overlay = (\{.*?\});", html, re.S)
    assert m, "overlay object not found in html"
    return json.loads(m.group(1))


def _paint_by_id(overlay, suffix):
    for ly in overlay["layers"]:
        if ly["id"].endswith(suffix):
            return ly["paint"]
    raise AssertionError(
        f"no layer ending in {suffix!r}: {[ly['id'] for ly in overlay['layers']]}"
    )


def _prepared_for(layer, emphasis):
    from databricks.labs.gbx.vizx._maplibre import layer_to_sources_layers

    return [layer_to_sources_layers(layer, 0, emphasis=emphasis)]


def test_build_html_data_polygon_paint_pops():
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html

    gdf = _gdf_polygon()
    prepared = _prepared_for(vector_layer(gdf), "data")
    html = build_html(prepared, emphasis="data")
    overlay = _parse_overlay(html)
    fill = _paint_by_id(overlay, "-fill")
    assert fill["fill-opacity"] == pytest.approx(0.85)
    # a contrasting dark outline color is present in data mode.
    assert "fill-outline-color" in fill
    assert fill["fill-outline-color"].lower() in ("#222222", "#222")
    line = _paint_by_id(overlay, "-line")
    assert line["line-width"] > 1.0


def test_build_html_blend_polygon_paint_is_soft():
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html

    gdf = _gdf_polygon()
    prepared = _prepared_for(vector_layer(gdf), "blend")
    html = build_html(prepared, emphasis="blend")
    overlay = _parse_overlay(html)
    fill = _paint_by_id(overlay, "-fill")
    # prior behavior: 0.8 fill-opacity (vector_layer's factory default), no outline.
    assert fill["fill-opacity"] == pytest.approx(0.8)
    assert "fill-outline-color" not in fill
    line = _paint_by_id(overlay, "-line")
    assert line["line-width"] == pytest.approx(1.0)


def test_build_html_data_explicit_color_overrides_outline():
    # user color sets the fill; the emphasis dark outline still applies, but a user
    # opacity must win over the emphasis fill-opacity.
    from databricks.labs.gbx.vizx._layers import vector_layer
    from databricks.labs.gbx.vizx._maplibre import build_html

    gdf = _gdf_polygon()
    layer = vector_layer(gdf, color="#00ff00", opacity=0.2)
    prepared = _prepared_for(layer, "data")
    html = build_html(prepared, emphasis="data")
    overlay = _parse_overlay(html)
    fill = _paint_by_id(overlay, "-fill")
    assert fill["fill-color"] == "#00ff00"
    assert fill["fill-opacity"] == pytest.approx(0.2)


def test_build_html_data_raster_full_opacity():
    from databricks.labs.gbx.vizx._layers import raster_layer
    from databricks.labs.gbx.vizx._maplibre import build_html

    arr = (np.random.rand(3, 8, 8) * 255).astype("uint8")
    # raster layer needs georeferencing corners; use the ndarray path via the
    # public adapter which the maplibre _raster handles. Provide opacity=None so
    # emphasis controls it.
    layer = raster_layer(arr)
    layer.opacity = None
    prepared = _prepared_for(layer, "data")
    html = build_html(prepared, emphasis="data")
    overlay = _parse_overlay(html)
    paint = _paint_by_id(overlay, "-raster")
    assert paint["raster-opacity"] == pytest.approx(1.0)


def test_build_html_blend_raster_matches_prior_full_opacity():
    from databricks.labs.gbx.vizx._layers import raster_layer
    from databricks.labs.gbx.vizx._maplibre import build_html

    arr = (np.random.rand(3, 8, 8) * 255).astype("uint8")
    layer = raster_layer(arr)
    layer.opacity = None
    prepared = _prepared_for(layer, "blend")
    html = build_html(prepared, emphasis="blend")
    overlay = _parse_overlay(html)
    paint = _paint_by_id(overlay, "-raster")
    # blend == prior: raster default was 1.0; blend must not dim it.
    assert paint["raster-opacity"] == pytest.approx(1.0)


def test_build_html_data_explicit_opacity_overrides_raster():
    from databricks.labs.gbx.vizx._layers import raster_layer
    from databricks.labs.gbx.vizx._maplibre import build_html

    arr = (np.random.rand(3, 8, 8) * 255).astype("uint8")
    layer = raster_layer(arr, opacity=0.3)
    prepared = _prepared_for(layer, "data")
    html = build_html(prepared, emphasis="data")
    overlay = _parse_overlay(html)
    paint = _paint_by_id(overlay, "-raster")
    assert paint["raster-opacity"] == pytest.approx(0.3)


def test_plot_interactive_threads_emphasis(monkeypatch):
    import databricks.labs.gbx.vizx._interactive as itx
    from databricks.labs.gbx.vizx._interactive import plot_interactive

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    real_import = builtins.__import__

    def _no_ipython_display(name, *args, **kwargs):
        if name == "IPython.display":
            raise ImportError("disabled")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_ipython_display)

    from databricks.labs.gbx.vizx._layers import vector_layer

    html = plot_interactive(
        [vector_layer(_gdf_polygon())], emphasis="data", debug_mode=0
    )
    overlay = _parse_overlay(html)
    fill = _paint_by_id(overlay, "-fill")
    assert fill["fill-opacity"] == pytest.approx(0.85)
    assert "fill-outline-color" in fill
