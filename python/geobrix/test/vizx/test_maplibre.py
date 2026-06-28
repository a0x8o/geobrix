"""Tests for vizx._maplibre — per-layer MapLibre GL adapter."""

import base64
import io

import geopandas as gpd
import matplotlib
import numpy as np
import pytest
from shapely.geometry import LineString, Point, Polygon

matplotlib.use("Agg")

from databricks.labs.gbx.vizx._layers import pmtiles_layer, raster_layer, vector_layer
from databricks.labs.gbx.vizx._maplibre import layer_to_sources_layers


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _tiny_geotiff_bytes(width=8, height=8, crs="EPSG:4326"):
    """Build a tiny in-memory GeoTIFF (1 band, float32) using rasterio."""
    import rasterio
    from rasterio.transform import from_bounds

    buf = io.BytesIO()
    transform = from_bounds(-122.5, 37.7, -122.4, 37.8, width, height)
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(np.random.rand(height, width).astype("float32"), 1)
    return buf.getvalue()


def _tiny_geotiff_path(tmp_path):
    p = tmp_path / "tiny.tif"
    p.write_bytes(_tiny_geotiff_bytes())
    return str(p)


def _build_pmtiles_archive(tile_type_str="mvt", layer_name="buildings"):
    """Build a minimal PMTiles archive with one dummy tile.

    Args:
        tile_type_str: ``"mvt"`` or ``"png"``.
        layer_name:    The vector-layer id embedded in TileJSON metadata
                       (ignored for raster archives but harmless to pass).
    """
    from pmtiles.tile import Compression, TileType, zxy_to_tileid
    from pmtiles.writer import Writer

    _TILE_TYPES = {
        "mvt": TileType.MVT,
        "png": TileType.PNG,
    }
    tile_type = _TILE_TYPES[tile_type_str]
    payload = b"DUMMY"
    buf = io.BytesIO()
    w = Writer(buf)
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": 0,
        "max_zoom": 2,
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": 0,
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    w.write_tile(zxy_to_tileid(0, 0, 0), payload)
    w.finalize(header, {"name": "test", "vector_layers": [{"id": layer_name}]})
    return buf.getvalue()


# ---------------------------------------------------------------------------
# vector layer (the brief's required test)
# ---------------------------------------------------------------------------


def test_vector_layer_becomes_geojson_source_and_fill_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf, column="v"), 0)
    assert "gbx0" in sources and sources["gbx0"]["type"] == "geojson"
    assert any(l["type"] in ("fill", "line", "circle") for l in layers)
    assert embed > 0


def test_vector_layer_polygon_gets_fill_and_line_layers():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf), 1)
    types = {l["type"] for l in layers}
    assert "fill" in types
    assert "line" in types
    # no circle for polygon-only gdf
    assert "circle" not in types
    assert sources["gbx1"]["type"] == "geojson"
    assert embed > 0


def test_vector_layer_point_gets_circle_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Point(-122.4, 37.7)],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf), 2)
    types = {l["type"] for l in layers}
    assert "circle" in types
    assert embed > 0


def test_vector_layer_line_gets_line_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[LineString([(-122.5, 37.7), (-122.4, 37.8)])],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf), 3)
    types = {l["type"] for l in layers}
    assert "line" in types
    assert "fill" not in types
    assert embed > 0


def test_vector_layer_source_data_is_valid_geojson():
    """The 'data' key in the source must be a GeoJSON FeatureCollection dict."""
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])],
        crs="EPSG:4326",
    )
    sources, _, _ = layer_to_sources_layers(vector_layer(gdf), 0)
    gj = sources["gbx0"]["data"]
    assert gj["type"] == "FeatureCollection"
    assert len(gj["features"]) == 1


def test_vector_layer_reprojected_to_4326():
    """A EPSG:27700 (BNG) input must be reprojected to EPSG:4326 in the source."""
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[box(530000, 179000, 531000, 180000)],
        crs="EPSG:27700",
    )
    sources, _, _ = layer_to_sources_layers(vector_layer(gdf), 0)
    gj = sources["gbx0"]["data"]
    coords = gj["features"][0]["geometry"]["coordinates"][0]
    # London area — longitude ~-0.1 to 0.0, latitude ~51.4 to 51.6
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    assert all(-1 < lon < 1 for lon in lons), f"Unexpected longitudes: {lons}"
    assert all(51 < lat < 52 for lat in lats), f"Unexpected latitudes: {lats}"


def test_vector_layer_idx_keys_source_and_layers():
    """idx drives the source key and layer ids."""
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])],
        crs="EPSG:4326",
    )
    sources, layers, _ = layer_to_sources_layers(vector_layer(gdf), 7)
    assert "gbx7" in sources
    for l in layers:
        assert l["source"] == "gbx7"
        assert l["id"].startswith("gbx7-")


# ---------------------------------------------------------------------------
# raster layer
# ---------------------------------------------------------------------------


def test_raster_layer_from_path_produces_image_source(tmp_path):
    """raster_layer(path) -> image source with 4-corner coordinates + raster layer."""
    path = _tiny_geotiff_path(tmp_path)
    sources, layers, embed = layer_to_sources_layers(raster_layer(path), 0)
    src = sources["gbx0"]
    assert src["type"] == "image"
    assert src["url"].startswith("data:image/png;base64,")
    assert len(src["coordinates"]) == 4  # [ul, ur, lr, ll] lon/lat pairs
    for corner in src["coordinates"]:
        assert len(corner) == 2
    assert len(layers) == 1
    assert layers[0]["type"] == "raster"
    assert embed > 0


def test_raster_layer_from_bytes_produces_image_source():
    """raster_layer(bytes) -> image source (accepts in-memory GeoTIFF bytes)."""
    tif_bytes = _tiny_geotiff_bytes()
    sources, layers, embed = layer_to_sources_layers(raster_layer(tif_bytes), 0)
    src = sources["gbx0"]
    assert src["type"] == "image"
    assert src["url"].startswith("data:image/png;base64,")
    assert len(src["coordinates"]) == 4
    assert embed > 0


def test_raster_layer_from_ndarray_produces_image_source():
    """raster_layer(ndarray) -> image source (no geo metadata, unit-square coords)."""
    arr = np.random.rand(4, 8).astype("float32")
    sources, layers, embed = layer_to_sources_layers(raster_layer(arr), 0)
    src = sources["gbx0"]
    assert src["type"] == "image"
    assert src["url"].startswith("data:image/png;base64,")
    assert len(src["coordinates"]) == 4
    assert embed > 0


def test_raster_layer_corners_are_lon_lat(tmp_path):
    """Corner coordinates are in lon/lat ([-180..180], [-90..90])."""
    path = _tiny_geotiff_path(tmp_path)
    sources, _, _ = layer_to_sources_layers(raster_layer(path), 0)
    for lon, lat in sources["gbx0"]["coordinates"]:
        assert -180 <= lon <= 180, f"lon out of range: {lon}"
        assert -90 <= lat <= 90, f"lat out of range: {lat}"


def test_raster_layer_b64_is_valid_png(tmp_path):
    """The embedded base64 decodes to a valid PNG magic-bytes sequence."""
    path = _tiny_geotiff_path(tmp_path)
    sources, _, _ = layer_to_sources_layers(raster_layer(path), 0)
    url = sources["gbx0"]["url"]
    b64_part = url.split(",", 1)[1]
    raw = base64.b64decode(b64_part)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n", "Embedded image is not a PNG"


def test_raster_layer_decimation(tmp_path):
    """A large raster should be decimated to <= raster_max_px on its longest side."""
    import rasterio
    from rasterio.transform import from_bounds

    buf = io.BytesIO()
    w, h = 4096, 2048
    transform = from_bounds(-122.5, 37.7, -122.4, 37.8, w, h)
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=h,
        width=w,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(np.random.rand(h, w).astype("float32"), 1)
    big_bytes = buf.getvalue()

    from databricks.labs.gbx.vizx._maplibre import _raster_to_image

    png_b64, corners = _raster_to_image(raster_layer(big_bytes), raster_max_px=512)
    raw = base64.b64decode(png_b64)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n"
    # The decimated PNG should be much smaller than the raw 4096x2048 raster bytes
    assert len(raw) < len(big_bytes) // 10


# ---------------------------------------------------------------------------
# pmtiles layer — url mode (no real archive needed)
# ---------------------------------------------------------------------------


def test_pmtiles_layer_url_mode():
    """An http(s) URL -> url mode sidecar, embed_bytes=0."""
    url = "https://example.com/tiles.pmtiles"
    layer = pmtiles_layer(url)
    sources, layers, embed = layer_to_sources_layers(layer, 0)
    src = sources["gbx0"]
    assert src["url"] == "pmtiles://gbx0"
    assert "_gbx_pmtiles" in src
    info = src["_gbx_pmtiles"]
    assert info["mode"] == "url"
    assert info["url"] == url
    assert embed == 0
    assert len(layers) == 1


def test_pmtiles_layer_embed_mode_from_bytes():
    """bytes archive -> embed mode, embed_bytes == len(bytes)."""
    archive = _build_pmtiles_archive("mvt")
    layer = pmtiles_layer(archive)
    sources, layers, embed = layer_to_sources_layers(layer, 0)
    src = sources["gbx0"]
    assert src["url"] == "pmtiles://gbx0"
    info = src["_gbx_pmtiles"]
    assert info["mode"] == "embed"
    assert "bytes" in info
    assert embed == len(info["bytes"])
    assert embed > 0
    assert len(layers) == 1


def test_pmtiles_layer_embed_mode_from_path(tmp_path):
    """path archive -> embed mode, bytes read from disk."""
    archive = _build_pmtiles_archive("mvt")
    p = tmp_path / "test.pmtiles"
    p.write_bytes(archive)
    layer = pmtiles_layer(str(p))
    sources, layers, embed = layer_to_sources_layers(layer, 0)
    src = sources["gbx0"]
    info = src["_gbx_pmtiles"]
    assert info["mode"] == "embed"
    assert info["bytes"] == archive
    assert embed == len(archive)


def test_pmtiles_layer_vector_source_type():
    """MVT pmtiles -> source type 'vector'."""
    archive = _build_pmtiles_archive("mvt")
    sources, _, _ = layer_to_sources_layers(pmtiles_layer(archive), 0)
    assert sources["gbx0"]["type"] == "vector"


def test_pmtiles_layer_raster_source_type():
    """PNG pmtiles -> source type 'raster'."""
    archive = _build_pmtiles_archive("png")
    sources, _, _ = layer_to_sources_layers(pmtiles_layer(archive), 0)
    assert sources["gbx0"]["type"] == "raster"


def test_pmtiles_layer_idx_drives_key():
    url = "https://example.com/tiles.pmtiles"
    sources, layers, _ = layer_to_sources_layers(pmtiles_layer(url), 5)
    assert "gbx5" in sources
    assert sources["gbx5"]["url"] == "pmtiles://gbx5"
    for l in layers:
        assert l["source"] == "gbx5"


# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------


def test_unknown_kind_raises():
    from databricks.labs.gbx.vizx._layers import Layer

    layer = object.__new__(Layer)
    object.__setattr__(layer, "kind", "unknown_kind")
    with pytest.raises(ValueError):
        layer_to_sources_layers(layer, 0)


# ---------------------------------------------------------------------------
# pmtiles — source-layer derived from archive metadata (Fix 1)
# ---------------------------------------------------------------------------


def test_pmtiles_vector_source_layer_from_metadata():
    """source-layer must be derived from the archive's vector_layers metadata,
    not hardcoded.  Build an archive whose layer is named 'roads' and assert
    the MapLibre fill layer carries source-layer='roads'."""
    archive = _build_pmtiles_archive("mvt", layer_name="roads")
    sources, layers, _ = layer_to_sources_layers(pmtiles_layer(archive), 0)
    fill_layers = [l for l in layers if l.get("type") == "fill"]
    assert fill_layers, "expected at least one fill layer for a vector PMTiles"
    assert fill_layers[0]["source-layer"] == "roads", (
        f"source-layer should be 'roads' (from archive metadata), "
        f"got {fill_layers[0]['source-layer']!r}"
    )


def test_extract_vector_layer_names_unit():
    """Unit test for _extract_vector_layer_names with a TileJSON-shaped dict."""
    from databricks.labs.gbx.vizx._maplibre import _extract_vector_layer_names

    assert _extract_vector_layer_names({}) == []
    assert _extract_vector_layer_names({"vector_layers": []}) == []
    assert _extract_vector_layer_names(
        {"vector_layers": [{"id": "parks"}, {"id": "water"}]}
    ) == ["parks", "water"]
    # Malformed entries (no 'id') are skipped.
    assert _extract_vector_layer_names(
        {"vector_layers": [{"name": "bad"}, {"id": "good"}]}
    ) == ["good"]
