"""Offline tests for plot_pmtiles (interactive HTML + static fallback)."""

import io

from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

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
