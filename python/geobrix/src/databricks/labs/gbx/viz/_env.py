"""Lazy-dependency guard for gbx.viz (the [viz] extra).

Visualization deps are heavy and optional. Package code imports them only inside
functions, after calling assert_viz_available(), which raises a clear install
hint when they are absent — mirroring pyrx/_env.py::assert_rasterio_available().
"""


def assert_viz_available() -> None:
    """Raise ImportError with [viz] guidance if matplotlib or geopandas is missing.

    Only the deps gbx.viz code actually imports are checked (matplotlib for raster
    rendering, geopandas for the GeoDataFrame adapters). folium / mapclassify are
    user-side GeoDataFrame.explore() deps and are not imported by this package.
    """
    missing = []
    for mod in ("matplotlib", "geopandas"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        raise ImportError(
            "gbx.viz requires the [viz] extra (missing: "
            + ", ".join(missing)
            + "). Install with: pip install 'geobrix[viz]'"
        )
