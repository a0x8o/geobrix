"""Offline tests for plot_pmtiles (interactive HTML + static fallback)."""

import io

import matplotlib
import numpy as np
import pytest
from matplotlib.image import imsave
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

matplotlib.use("Agg")

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


def _build_archive(tiles, tile_type, *, name="demo"):
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": min(zs),
        "max_zoom": max(zs),
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": min(zs),
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    for z, x, y, payload in sorted(
        tiles, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])
    ):
        w.write_tile(zxy_to_tileid(z, x, y), payload)
    w.finalize(header, {"name": name, "vector_layers": [{"id": "demo"}]})
    return buf.getvalue()


def test_is_raster_type():
    from databricks.labs.gbx.vizx import _pmtiles as p

    assert p._is_raster_type("png") is True
    assert p._is_raster_type("jpeg") is True
    assert p._is_raster_type("webp") is True
    assert p._is_raster_type("avif") is True
    assert p._is_raster_type("mvt") is False
    assert p._is_raster_type("unknown") is False


def test_archive_bytes_passthrough_and_path(tmp_path):
    from databricks.labs.gbx.vizx import _pmtiles as p

    raw = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    assert p._archive_bytes(raw) == raw
    f = tmp_path / "a.pmtiles"
    f.write_bytes(raw)
    assert p._archive_bytes(str(f)) == raw
    assert p._archive_bytes("dbfs:" + str(f)) == raw


def _info(tile_type, *, min_zoom=0, max_zoom=2):
    return {
        "tile_type": tile_type,
        "tile_compression": "none",
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "bounds": (-122.52, 37.70, -122.35, 37.83),
        "center": (-122.44, 37.76, min_zoom),
        "tile_count": 3,
        "metadata": {"vector_layers": [{"id": "demo"}]},
    }


def test_build_html_pins_cdn_versions():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("png"))
    assert "maplibre-gl@4.7.1/dist/maplibre-gl.js" in html
    assert "maplibre-gl@4.7.1/dist/maplibre-gl.css" in html
    assert "pmtiles@3.2.1/dist/pmtiles.js" in html


def test_build_html_embeds_base64_and_registers_protocol():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJDREVG", _info("png"))
    assert "QUJDREVG" in html  # the base64 archive is embedded inline
    assert "new pmtiles.Protocol" in html
    assert "addProtocol" in html
    assert "pmtiles.FileSource" in html or "FileSource" in html
    assert "pmtiles://" in html


def test_build_html_raster_layer_for_png():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("png"))
    assert (
        '"type": "raster"' in html
        or "'type': 'raster'" in html
        or 'type: "raster"' in html
    )


def test_build_html_vector_layer_for_mvt():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("mvt"))
    assert '"type": "vector"' in html or 'type: "vector"' in html
    # the source-layer id from the metadata vector_layers drives the fill layer
    assert "demo" in html


def test_build_html_honors_custom_style():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html(
        "QUJD", _info("mvt"), style={"version": 8, "layers": []}
    )
    assert '"version": 8' in html


def test_build_html_script_injection_escaped():
    """I1: </script> in style JSON must be escaped so the HTML parser cannot close
    the script element early."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    evil_style = {
        "version": 8,
        "sources": {},
        "layers": [{"id": "x", "type": "fill", "evil": "</script><script>alert(1)"}],
    }
    html = p._build_pmtiles_html("QUJD", _info("mvt"), style=evil_style)
    # Locate the script block that contains the map init (style JSON is there).
    # The raw substring </script> must NOT appear inside the JS block.
    script_start = html.index("<script>\nconst b64")
    script_end = html.index("</script>", script_start)
    script_body = html[script_start:script_end]
    assert "</script>" not in script_body
    # The escaped form must be present so the value round-trips correctly.
    assert "<\\/script>" in script_body


def test_build_html_b64_newline_stripped():
    """I2: archive_b64 with embedded newlines (e.g. from base64.encodebytes) must
    produce a single-line const b64 = "..." literal."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    b64_with_newlines = "QUJD\nREVG\r\nR0hJ"
    html = p._build_pmtiles_html(b64_with_newlines, _info("png"))
    # Find the b64 literal line and confirm no newline inside the quotes.
    for line in html.splitlines():
        if line.strip().startswith("const b64"):
            # Everything between the first " and last " on this line is the value.
            inner = line.split('"', 1)[1].rsplit('"', 1)[0]
            assert "\n" not in inner and "\r" not in inner
            break
    else:
        raise AssertionError("const b64 line not found in HTML")


def test_build_html_raster_layer_zoom_range():
    """M1: raster layer must carry minzoom/maxzoom from info so archives with
    min_zoom > 0 don't show a blank map before the user zooms in."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("png", min_zoom=2, max_zoom=5))
    assert '"minzoom": 2' in html
    assert '"maxzoom": 5' in html


def _real_png_tile():
    # A real 8x8 RGB PNG so plot_raster's rasterio MemoryFile can decode it.
    buf = io.BytesIO()
    imsave(buf, (np.random.rand(8, 8, 3)), format="png")
    return buf.getvalue()


def test_static_raster_fallback_calls_plot_raster(monkeypatch):
    from databricks.labs.gbx.vizx import _pmtiles as p

    png = _real_png_tile()
    archive = _build_archive([(0, 0, 0, png)], TileType.PNG)
    captured = {}

    def _fake_plot_raster(raster_bytes, **kw):
        captured["n"] = len(raster_bytes)
        captured["kw"] = kw

    monkeypatch.setattr(
        "databricks.labs.gbx.vizx.plot_raster",
        _fake_plot_raster,
    )
    info = __import__(
        "databricks.labs.gbx.pmtiles", fromlist=["pmtiles_info"]
    ).pmtiles_info(archive)
    p._static_raster_fallback(archive, info, basemap=False)
    assert captured["n"] == len(png)  # the decoded lowest-zoom tile bytes
    # basemap must be stripped before forwarding to plot_raster (which rejects it)
    assert "basemap" not in captured["kw"]


def _real_mvt_tile(z, x, y):
    # Encode a polygon in tile-local pixel space for tile (z,x,y) (origin NW),
    # the same convention pyvx writes, so the fallback reprojects it back to 4326.
    import mapbox_vector_tile as mvt
    from shapely.geometry import box

    return mvt.encode(
        {
            "name": "demo",
            "features": [
                {"geometry": box(1000, 1000, 3000, 3000), "properties": {"v": 1}}
            ],
        },
        default_options={"extents": 4096, "y_coord_down": True},
    )


def test_static_vector_fallback_builds_gdf_and_plots(monkeypatch):
    import geopandas as gpd

    from databricks.labs.gbx.vizx import _pmtiles as p

    z, x, y = 10, 163, 395  # an SF-area tile
    blob = _real_mvt_tile(z, x, y)
    archive = _build_archive([(z, x, y, blob)], TileType.MVT)
    info = __import__(
        "databricks.labs.gbx.pmtiles", fromlist=["pmtiles_info"]
    ).pmtiles_info(archive)

    captured = {}

    def _fake_plot_static(gdf, **kw):
        captured["gdf"] = gdf
        captured["kw"] = kw
        return "AX"

    monkeypatch.setattr("databricks.labs.gbx.vizx.plot_static", _fake_plot_static)
    out = p._static_vector_fallback(archive, info, basemap=False)
    assert out == "AX"
    gdf = captured["gdf"]
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) >= 1
    assert gdf.crs.to_epsg() == 4326
    # geometry reprojected into the SF tile's lon/lat extent
    minx, miny, maxx, maxy = gdf.total_bounds
    assert -123 < minx < maxx < -121 and 37 < miny < maxy < 39


def test_plot_pmtiles_interactive_routes_through_displayhtml(monkeypatch):
    """plot_pmtiles delegates to plot_interactive which uses _notebook_display_html."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    captured = {}
    # plot_pmtiles now delegates to plot_interactive, which calls _notebook_display_html
    # from _interactive module.
    monkeypatch.setattr(
        "databricks.labs.gbx.vizx._interactive._notebook_display_html",
        lambda: (lambda html: captured.update(html=html)),
    )
    out = p.plot_pmtiles(archive)  # small -> interactive
    assert out is None  # displayHTML render returns None
    html = captured["html"]
    # HTML comes from _maplibre.build_html (pinned at maplibre-gl@4.7.1 / pmtiles@3.2.0)
    assert "maplibre-gl@4.7.1" in html and "pmtiles@3.2.0" in html
    assert "pmtiles://" in html


def test_plot_pmtiles_size_guard_uses_static_fallback(monkeypatch):
    """When the archive exceeds the budget, plot_pmtiles falls back to the static path.

    Task 7: plot_pmtiles delegates entirely to plot_interactive → prepare_layers.
    When the budget is exceeded, prepare_layers returns mode='static' and
    plot_interactive calls plot_static. We verify plot_static is called and a
    warning is issued by prepare_layers.
    """
    import warnings

    from databricks.labs.gbx.vizx import _pmtiles as p

    png = _real_png_tile()
    archive = _build_archive([(0, 0, 0, png)], TileType.PNG)

    # Stub plot_static to avoid actual rendering (raw tile bytes aren't a full GeoTIFF).
    called = {}
    import databricks.labs.gbx.vizx._interactive as itx
    import databricks.labs.gbx.vizx._static_map as sm

    monkeypatch.setattr(sm, "plot_static", lambda *a, **kw: called.update(static=True))
    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)

    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        # tiny max_embed_mb forces static path
        p.plot_pmtiles(archive, max_embed_mb=1e-9)
    # A prepare_layers warning must have been issued.
    assert any("fallback" in str(w.message).lower() or "exceed" in str(w.message).lower() for w in ws)
    # plot_static was called.
    assert called.get("static") is True


def test_plot_pmtiles_size_guard_uses_vector_static_fallback(monkeypatch):
    """Vector pmtiles over budget delegates to prepare_layers static path."""
    import builtins
    import warnings

    from databricks.labs.gbx.vizx import _pmtiles as p

    blob = _real_mvt_tile(10, 163, 395)
    archive = _build_archive([(10, 163, 395, blob)], TileType.MVT)

    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython(name, *a, **kw):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)

    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        p.plot_pmtiles(archive, max_embed_mb=1e-9)
    # A fallback warning must have been issued by prepare_layers.
    assert any("fallback" in str(w.message).lower() or "exceed" in str(w.message).lower() for w in ws)


def test_plot_pmtiles_oversized_without_fallback_raises():
    """fallback=False on an oversized archive raises ValueError via prepare_layers."""
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    with pytest.raises(ValueError, match="exceeds budget"):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, fallback=False)


def test_plot_pmtiles_unknown_tile_type_treated_as_vector(monkeypatch):
    """Task 7: unknown tile types are treated as vector (no validation error).

    The old plot_pmtiles explicitly validated tile type and raised ValueError.
    After Task 7, plot_pmtiles delegates to plot_interactive → prepare_layers
    → layer_to_sources_layers, which treats unknown tile types as vector
    (no raise). This documents the new expected behavior.
    """
    import builtins

    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)

    def _fake_pmtiles_info(data):
        return {
            "tile_type": "unknown",
            "tile_compression": "none",
            "min_zoom": 0,
            "max_zoom": 0,
            "bounds": (-122.52, 37.70, -122.35, 37.83),
            "center": (-122.44, 37.76, 0),
            "tile_count": 1,
            "metadata": {},
        }

    monkeypatch.setattr(
        "databricks.labs.gbx.pmtiles.pmtiles_info",
        _fake_pmtiles_info,
    )

    # Block display channels so we get the HTML string back.
    import databricks.labs.gbx.vizx._interactive as itx

    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython(name, *a, **kw):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)

    # Should NOT raise; unknown tile_type is treated as vector by _maplibre._pmtiles.
    html = p.plot_pmtiles(archive)
    assert html is not None
    assert "maplibregl.Map" in html


def test_public_exports():
    import databricks.labs.gbx.vizx as vizx

    assert hasattr(vizx, "plot_pmtiles")
    assert hasattr(vizx, "plot_cog")
    assert "plot_pmtiles" in vizx.__all__
    assert "plot_cog" in vizx.__all__
