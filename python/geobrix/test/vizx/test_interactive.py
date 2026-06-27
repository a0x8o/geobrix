import logging

import pytest


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-interactive-tests")
        .getOrCreate()
    )
    yield s


def _square_gdf(column=None):
    """Tiny 2-polygon GeoDataFrame fixture (EPSG:4326)."""
    import geopandas as gpd
    from shapely.geometry import Polygon

    polys = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
        Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]),
    ]
    data = {}
    if column == "num":
        data["num"] = [1.0, 5.0]
    elif column == "cat":
        data["cat"] = ["a", "b"]
    return gpd.GeoDataFrame(data, geometry=polys, crs=4326)


def _clear_displayhtml(monkeypatch):
    """Force both notebook-display channels off (plain-return / Jupyter path).

    ``displayHTML`` is reached through the IPython user namespace, and the
    ``IPython.display`` fallback would actually render in a test runner that
    happens to run under IPython. Stub both so ``_render`` lands on the plain
    ``return m`` last resort.
    """
    import builtins

    from databricks.labs.gbx.vizx import _interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    real_import = builtins.__import__

    def _no_ipython_display(name, *args, **kwargs):
        if name == "IPython.display":
            raise ImportError("IPython.display disabled for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_ipython_display)


def _inject_displayhtml(monkeypatch):
    """Mock the notebook ``displayHTML`` helper; return the captured call list."""
    from databricks.labs.gbx.vizx import _interactive as itx

    calls = []
    monkeypatch.setattr(itx, "_notebook_display_html", lambda: calls.append)
    return calls


# --- _raster_overlay (fast path internals) ---


def test_raster_overlay_returns_folium_map_with_imageoverlay(monkeypatch):
    import folium

    from databricks.labs.gbx.vizx import _interactive as itx

    m = itx._raster_overlay(_square_gdf())
    assert isinstance(m, folium.Map)
    # The map must contain an ImageOverlay child (every polygon burned).
    overlays = [
        c
        for c in m._children.values()
        if isinstance(c, folium.raster_layers.ImageOverlay)
    ]
    assert len(overlays) == 1


def test_raster_overlay_numeric_column(monkeypatch):
    from databricks.labs.gbx.vizx import _interactive as itx

    m = itx._raster_overlay(_square_gdf("num"), column="num")
    overlays = [c for c in m._children.values() if type(c).__name__ == "ImageOverlay"]
    assert len(overlays) == 1


def test_raster_overlay_categorical_column(monkeypatch):
    from databricks.labs.gbx.vizx import _interactive as itx

    m = itx._raster_overlay(_square_gdf("cat"), column="cat")
    overlays = [c for c in m._children.values() if type(c).__name__ == "ImageOverlay"]
    assert len(overlays) == 1


# --- mode validation ---


def test_invalid_mode_raises():
    from databricks.labs.gbx.vizx import plot_interactive

    with pytest.raises(ValueError) as exc:
        plot_interactive(_square_gdf(), mode="bogus")
    assert "auto" in str(exc.value) and "detailed" in str(exc.value)


# --- Jupyter (NameError) path: returns the map ---


def test_jupyter_path_returns_map_fast(monkeypatch):
    import folium

    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    m = plot_interactive(_square_gdf(), mode="fast", debug_level=0)
    assert isinstance(m, folium.Map)


def test_jupyter_path_returns_map_detailed(monkeypatch):
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    m = plot_interactive(_square_gdf(), mode="detailed", debug_level=0)
    # geopandas .explore() returns a folium.Map
    assert m is not None
    assert hasattr(m, "_repr_html_")


# --- Databricks (displayHTML) path: side-effect render, returns None ---


def test_databricks_path_calls_displayhtml_and_returns_none(monkeypatch):
    from databricks.labs.gbx.vizx import plot_interactive

    calls = _inject_displayhtml(monkeypatch)
    result = plot_interactive(_square_gdf(), mode="fast", debug_level=0)
    assert result is None
    assert len(calls) == 1
    assert isinstance(calls[0], str) and len(calls[0]) > 0


def test_databricks_path_via_ipython_user_ns(monkeypatch):
    """_render finds displayHTML in the IPython user namespace and returns None."""
    from unittest.mock import Mock

    import IPython

    from databricks.labs.gbx.vizx import _interactive as itx

    dh = Mock()
    fake_ip = Mock()
    fake_ip.user_ns = {"displayHTML": dh}
    monkeypatch.setattr(IPython, "get_ipython", lambda: fake_ip, raising=False)

    m = itx._raster_overlay(_square_gdf())
    result = itx._render(m)
    assert result is None
    dh.assert_called_once()
    (html_arg,) = dh.call_args.args
    assert isinstance(html_arg, str) and len(html_arg) > 0


# --- IPython.display fallback: helper absent, display() used, returns None ---


def test_ipython_display_fallback(monkeypatch):
    """No notebook displayHTML -> IPython.display.display(HTML(...)); returns None."""
    import IPython.display

    from databricks.labs.gbx.vizx import _interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    calls = []
    monkeypatch.setattr(
        IPython.display, "display", lambda obj: calls.append(obj), raising=False
    )

    m = itx._raster_overlay(_square_gdf())
    result = itx._render(m)
    assert result is None
    assert len(calls) == 1


# --- plain-return path: both channels unavailable -> returns the map ---


def test_render_plain_return_when_both_unavailable(monkeypatch):
    import folium

    from databricks.labs.gbx.vizx import _interactive as itx

    _clear_displayhtml(monkeypatch)
    m = itx._raster_overlay(_square_gdf())
    result = itx._render(m)
    assert result is m
    assert isinstance(result, folium.Map)


# --- mode="auto" crossover ---


def test_auto_picks_detailed_under_threshold(monkeypatch):
    from databricks.labs.gbx.vizx import _interactive as itx

    _clear_displayhtml(monkeypatch)
    real_overlay = itx._raster_overlay
    called = {}

    def _fake_overlay(gdf, column=None, opacity=0.65, max_px=1400):
        called["overlay"] = True
        return real_overlay(gdf, column, opacity, max_px)

    monkeypatch.setattr(itx, "_raster_overlay", _fake_overlay)
    # small gdf, high max_vertices -> detailed path; overlay not called
    itx.plot_interactive(
        _square_gdf(), mode="auto", max_vertices=1_000_000, debug_level=0
    )
    assert "overlay" not in called


def test_auto_picks_fast_over_threshold(monkeypatch):
    from databricks.labs.gbx.vizx import _interactive as itx

    _clear_displayhtml(monkeypatch)
    real_overlay = itx._raster_overlay
    called = {}

    def _fake_overlay(gdf, column=None, opacity=0.65, max_px=1400):
        called["overlay"] = True
        return real_overlay(gdf, column, opacity, max_px)

    monkeypatch.setattr(itx, "_raster_overlay", _fake_overlay)
    # force fast: threshold below the fixture's vertex count
    itx.plot_interactive(_square_gdf(), mode="auto", max_vertices=1, debug_level=0)
    assert called.get("overlay") is True


# --- debug_level output gating ---


def test_auto_debug_level_1_announces_path(monkeypatch, capsys):
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    plot_interactive(_square_gdf(), mode="auto", max_vertices=1, debug_level=1)
    out = capsys.readouterr().out
    assert "fast" in out.lower()
    assert "(set debug_level=0 to silence)" in out


def test_debug_level_0_is_silent(monkeypatch, capsys):
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    plot_interactive(_square_gdf(), mode="auto", max_vertices=1, debug_level=0)
    out = capsys.readouterr().out
    assert out == ""


def test_debug_level_2_is_verbose(monkeypatch, capsys):
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    plot_interactive(_square_gdf(), mode="fast", max_vertices=1, debug_level=2)
    out = capsys.readouterr().out
    # verbose: vertex counts shown even when not a decision point
    assert "vert" in out.lower()


# --- detailed over threshold: warn at level 1, silent at level 0, proceeds ---


def test_detailed_over_threshold_warns_at_level_1(monkeypatch, capsys):
    import folium

    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    m = plot_interactive(_square_gdf(), mode="detailed", max_vertices=1, debug_level=1)
    out = capsys.readouterr().out
    assert "may be slow" in out.lower()
    assert "(set debug_level=0 to silence)" in out
    # proceeds: still produces a detailed (.explore) map
    assert isinstance(m, folium.Map)


def test_detailed_over_threshold_silent_at_level_0(monkeypatch, capsys):
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    plot_interactive(_square_gdf(), mode="detailed", max_vertices=1, debug_level=0)
    out = capsys.readouterr().out
    assert out == ""


# --- Spark DataFrame input path ---


def test_spark_dataframe_geometry_input(spark, monkeypatch):
    import folium

    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    df = spark.createDataFrame([("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",)], ["wkt"])
    m = plot_interactive(df, mode="fast", debug_level=0)
    assert isinstance(m, folium.Map)


def test_spark_dataframe_grid_system_input(spark, monkeypatch):
    import folium

    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    import h3

    s = h3.latlng_to_cell(40.7, -74.0, 9)
    df = spark.createDataFrame([(s,)], ["cellid"])
    m = plot_interactive(df, grid_system="h3", mode="fast", debug_level=0)
    assert isinstance(m, folium.Map)


# --- sample_seed threading + fast-path truncation warning ---


def test_sample_seed_threaded_to_resolve_gdf(monkeypatch):
    """plot_interactive forwards sample_seed to _resolve_gdf."""
    # plot_interactive imports _resolve_gdf from _static_map inside the function,
    # so the patch must target the _static_map module (the lookup site).
    from databricks.labs.gbx.vizx import _interactive as itx
    from databricks.labs.gbx.vizx import _static_map as sm

    _clear_displayhtml(monkeypatch)
    captured = {}
    real = sm._resolve_gdf

    def _spy(*args, **kw):
        # plot_interactive passes sample_seed as the 7th positional arg.
        captured["sample_seed"] = args[6] if len(args) > 6 else kw.get("sample_seed")
        return real(*args, **kw)

    monkeypatch.setattr(sm, "_resolve_gdf", _spy)
    itx.plot_interactive(_square_gdf(), mode="fast", debug_level=0, sample_seed=123)
    assert captured["sample_seed"] == 123


def test_sample_seed_threaded_through_plot_static(monkeypatch):
    """plot_static forwards sample_seed to _resolve_gdf."""
    from databricks.labs.gbx.vizx import _static_map as sm

    captured = {}
    real = sm._resolve_gdf

    def _spy(*args, **kw):
        # plot_static passes sample_seed as the 7th positional arg.
        captured["sample_seed"] = args[6] if len(args) > 6 else kw.get("sample_seed")
        return real(*args, **kw)

    monkeypatch.setattr(sm, "_resolve_gdf", _spy)
    sm.plot_static(_square_gdf(), basemap=False, sample_seed=55)
    assert captured["sample_seed"] == 55


def test_fast_truncation_warning_fires_when_capped(monkeypatch, capsys):
    """fast + len(gdf) >= max_rows -> truncation advice at debug_level=1."""
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    # 2-row fixture, max_rows=2 -> len(gdf) >= max_rows -> warning
    plot_interactive(_square_gdf(), mode="fast", max_rows=2, debug_level=1)
    out = capsys.readouterr().out
    assert "pre-aggregate" in out
    assert "of (>= max_rows)" in out
    assert "(set debug_level=0 to silence)" in out


def test_fast_truncation_warning_silent_at_level_0(monkeypatch, capsys):
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    plot_interactive(_square_gdf(), mode="fast", max_rows=2, debug_level=0)
    out = capsys.readouterr().out
    assert out == ""


def test_fast_truncation_warning_not_fired_under_cap(monkeypatch, capsys):
    """Under the cap (len(gdf) < max_rows) -> no truncation advice."""
    from databricks.labs.gbx.vizx import plot_interactive

    _clear_displayhtml(monkeypatch)
    # 2-row fixture, max_rows=100 -> len(gdf) < max_rows -> no warning
    plot_interactive(_square_gdf(), mode="fast", max_rows=100, debug_level=1)
    out = capsys.readouterr().out
    assert "pre-aggregate" not in out


# --- wiring ---


def test_plot_interactive_exported():
    from databricks.labs.gbx import vizx

    assert "plot_interactive" in vizx.__all__
    assert hasattr(vizx, "plot_interactive")
