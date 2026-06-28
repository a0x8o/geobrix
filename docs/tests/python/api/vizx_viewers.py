"""Executable doc examples for the VizX PMTiles and COG viewers (Docker)."""

import io
import os

import matplotlib

matplotlib.use("Agg")

from pmtiles.tile import Compression, TileType, zxy_to_tileid  # noqa: E402
from pmtiles.writer import Writer  # noqa: E402


def _build_archive(tiles, tile_type, *, name="demo"):
    """Build an in-memory .pmtiles archive from (z, x, y, payload) tuples."""
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


def _distinct_png(seed: int) -> bytes:
    """Return a PNG whose bytes differ per seed (avoids writer dedup)."""
    import numpy as np
    from matplotlib.image import imsave

    rng = np.random.default_rng(seed)
    buf = io.BytesIO()
    imsave(buf, rng.random((8, 8, 3)), format="png")
    return buf.getvalue()


def pmtiles_info_example():
    """Build a two-tile PNG archive, inspect it with pmtiles_info."""
    from databricks.labs.gbx.pmtiles import pmtiles_info

    archive = _build_archive(
        [
            (0, 0, 0, _distinct_png(0)),
            (1, 0, 0, _distinct_png(1)),
        ],
        TileType.PNG,
    )
    info = pmtiles_info(archive)

    assert info["tile_type"] == "png"
    assert info["tile_count"] == 2
    assert info["min_zoom"] == 0
    assert info["max_zoom"] == 1
    assert len(info["bounds"]) == 4  # (min_lon, min_lat, max_lon, max_lat)
    assert len(info["center"]) == 3  # (lon, lat, zoom)


def _build_raster_tile_archive():
    """Build a tiny PMTiles archive whose single tile is a valid GeoTIFF.

    Using a GeoTIFF instead of a plain PNG avoids the rasterio
    NotGeoreferencedWarning that occurs when rasterio opens a PNG with no
    spatial reference (the doc-test suite promotes UserWarning to errors).
    """
    import tempfile

    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        tif_path = f.name
    try:
        size = 8
        rng = np.random.default_rng(99)
        data = (rng.random((3, size, size)) * 255).astype("uint8")
        transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
        with rasterio.open(
            tif_path,
            "w",
            driver="GTiff",
            height=size,
            width=size,
            count=3,
            dtype="uint8",
            crs="EPSG:3857",
            transform=transform,
        ) as dst:
            dst.write(data)
        with open(tif_path, "rb") as f:
            return f.read()
    finally:
        os.unlink(tif_path)


def plot_pmtiles_static_example():
    """plot_pmtiles with max_embed_mb=0 forces the static path (no network)."""
    import matplotlib.pyplot as plt
    from databricks.labs.gbx.vizx import plot_pmtiles

    plt.close("all")

    tile_bytes = _build_raster_tile_archive()
    archive = _build_archive(
        [(0, 0, 0, tile_bytes)],
        # Declare PNG type in the PMTiles header so plot_pmtiles routes to the
        # raster static fallback; the actual payload bytes are a GeoTIFF, which
        # rasterio opens without warnings (it has a valid geotransform).
        TileType.PNG,
    )

    # max_embed_mb=0 → static raster fallback (offline-safe).
    # basemap=False is forwarded through plot_pmtiles; the static raster path
    # strips it before calling plot_raster (which does not accept basemap).
    plot_pmtiles(archive, max_embed_mb=0, basemap=False)

    # The static path delegates to plot_raster which opens a matplotlib figure.
    assert len(plt.get_fignums()) >= 1
    plt.close("all")


def plot_cog_example():
    """plot_cog reads a local GeoTIFF and renders a matplotlib figure."""
    import tempfile

    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from databricks.labs.gbx.vizx import plot_cog
    from rasterio.transform import from_bounds

    plt.close("all")

    # Build a minimal GeoTIFF in memory (3-band UInt16, Web Mercator).
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        path = f.name

    try:
        size = 16
        rng7 = np.random.default_rng(7)
        data = (rng7.random((3, size, size)) * 1000).astype("uint16")
        transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=size,
            width=size,
            count=3,
            dtype="uint16",
            crs="EPSG:3857",
            transform=transform,
        ) as dst:
            dst.write(data)

        # basemap=False skips contextily (offline-safe)
        plot_cog(path, basemap=False)

        assert len(plt.get_fignums()) >= 1
        plt.close("all")
    finally:
        os.unlink(path)
