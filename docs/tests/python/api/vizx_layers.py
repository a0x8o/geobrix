"""Executable doc examples for the VizX multi-layer compositor (Docker)."""

import os
import shutil

import matplotlib

matplotlib.use("Agg")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _small_polygon_gdf():
    """Return a tiny GeoDataFrame with one polygon."""
    import geopandas as gpd
    from shapely.geometry import Polygon

    poly = Polygon(
        [(-122.42, 37.76), (-122.40, 37.76), (-122.40, 37.78), (-122.42, 37.78)]
    )
    return gpd.GeoDataFrame({"value": [1]}, geometry=[poly], crs="EPSG:4326")


def _small_point_gdf():
    """Return a tiny GeoDataFrame with two points."""
    import geopandas as gpd
    from shapely.geometry import Point

    pts = [Point(-122.42, 37.77), Point(-122.40, 37.76)]
    return gpd.GeoDataFrame({"count": [10, 20]}, geometry=pts, crs="EPSG:4326")


# ---------------------------------------------------------------------------
# Doc-test functions
# ---------------------------------------------------------------------------


def multilayer_static_example():
    """Composite two vector layers on one matplotlib axes (no basemap)."""
    import matplotlib.pyplot as plt

    from databricks.labs.gbx.vizx import plot_static, vector_layer

    plt.close("all")

    gdf_poly = _small_polygon_gdf()
    gdf_pts = _small_point_gdf()

    # Layer order: polygon footprint on the bottom, point sites on top.
    layers = [
        vector_layer(gdf_poly, color="#3388ff", opacity=0.5, label="zones"),
        vector_layer(gdf_pts, color="#e04e2a", label="sites"),
    ]
    ax = plot_static(layers, basemap=False)

    assert ax is not None, "plot_static should return an Axes"
    children = ax.get_children()
    assert len(children) >= 2, f"Expected >=2 artists on the axes, got {len(children)}"
    plt.close("all")
    return ax


def multilayer_interactive_example():
    """Two vector layers composited in one MapLibre map; dry_run returns the audit dict."""
    from databricks.labs.gbx.vizx import plot_interactive, vector_layer

    gdf_pts = _small_point_gdf()
    gdf_poly = _small_polygon_gdf()

    layers = [
        vector_layer(gdf_poly, color="#e04e2a", opacity=0.5, label="zones"),
        vector_layer(gdf_pts, color="#1f6fb5", label="sites"),
    ]
    # dry_run=True returns the audit dict without calling displayHTML.
    # This works both inside and outside a notebook environment.
    result = plot_interactive(layers, basemap="carto-positron", dry_run=True)

    assert isinstance(result, dict), "Expected audit dict from dry_run=True"
    assert result["fits"] is True, f"Small layers should fit: {result}"
    assert result["verdict"] == "embed", f"Expected 'embed', got {result['verdict']!r}"
    assert len(result["layers"]) == 2, f"Expected 2 layers, got {len(result['layers'])}"
    assert result["layers"][0]["label"] == "zones"
    assert result["layers"][1]["label"] == "sites"
    return result


def audit_layers_example():
    """audit_layers returns an actionable size report before rendering."""
    from databricks.labs.gbx.vizx import audit_layers, vector_layer

    gdf = _small_polygon_gdf()
    layers = [vector_layer(gdf, label="zones")]
    result = audit_layers(layers, max_embed_mb=64)

    assert "fits" in result, "audit dict must have 'fits'"
    assert "verdict" in result, "audit dict must have 'verdict'"
    assert "total_embed_bytes" in result, "audit dict must have 'total_embed_bytes'"
    assert result["fits"] is True, f"Small GeoDataFrame should fit in 64 MB: {result}"
    assert result["verdict"] == "embed", (
        f"Expected verdict='embed', got {result['verdict']!r}"
    )
    assert result["total_embed_bytes"] > 0
    return result


def simplify_ephemeral_example():
    """simplify_tiles_from_source returns in-memory PMTiles bytes (ephemeral)."""
    if shutil.which("tippecanoe") is None:
        # tippecanoe absent — skip gracefully in non-vizx environments.
        return None

    from databricks.labs.gbx.vizx import simplify_tiles_from_source

    gdf = _small_polygon_gdf()
    result = simplify_tiles_from_source(gdf, spec={"max_z": 6, "min_z": 0})

    assert isinstance(result, (bytes, bytearray)), "Expected bytes"
    assert len(result) > 0, "PMTiles bytes should be non-empty"
    assert result[:7] == b"PMTiles", "Should start with PMTiles magic bytes"
    return result


def simplify_durable_example(tmp_path):
    """simplify_tiles_from_source with out_path writes a reusable durable file."""
    if shutil.which("tippecanoe") is None:
        return None

    from databricks.labs.gbx.vizx import simplify_tiles_from_source

    gdf = _small_polygon_gdf()
    out = os.path.join(tmp_path, "overview.pmtiles")
    result = simplify_tiles_from_source(gdf, spec={"max_z": 5}, out_path=out)

    assert result == out, f"Return value should be out_path, got {result!r}"
    assert os.path.exists(out), f"File should exist at {out}"
    with open(out, "rb") as f:
        header = f.read(7)
    assert header == b"PMTiles", "File should be a valid PMTiles archive"
    return out
