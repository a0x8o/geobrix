"""GDAL/PROJ environment configuration for the bundled rasterio wheel.

rasterio's manylinux/macOS wheels ship their own GDAL + PROJ data. We point
GDAL_DATA / PROJ_DATA at those bundled paths *only if unset*, so pyrx does not
collide with any cluster-level GDAL installed by the heavyweight init script.
"""

import os
from typing import Optional, Tuple


def _bundled_gdal_data() -> Optional[str]:
    """Return rasterio's bundled GDAL data dir, or None."""
    try:
        from rasterio._env import get_gdal_data

        path = get_gdal_data()
        return path if path and os.path.isdir(path) else None
    except Exception:
        return None


def _bundled_proj_data() -> Optional[str]:
    """Return rasterio's bundled PROJ data dir, or None.

    rasterio._env.PROJDataFinder().search() returns a single directory string.
    """
    try:
        from rasterio._env import PROJDataFinder

        path = PROJDataFinder().search()
        return path if path and os.path.isdir(path) else None
    except Exception:
        return None


def configure_gdal_env() -> None:
    """Idempotently set GDAL_DATA / PROJ_DATA from rasterio's bundled data.

    Existing values are respected (never overwritten).
    """
    if not os.environ.get("GDAL_DATA"):
        p = _bundled_gdal_data()
        if p:
            os.environ["GDAL_DATA"] = p
    if not os.environ.get("PROJ_DATA") and not os.environ.get("PROJ_LIB"):
        p = _bundled_proj_data()
        if p:
            os.environ["PROJ_DATA"] = p


def assert_rasterio_available() -> Tuple[str, str]:
    """Return (gdal_version, rasterio_version); raise a clear error if missing."""
    try:
        import rasterio
    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "pyrx requires rasterio. Install with: pip install 'geobrix[pyrx]'"
        ) from e
    return rasterio.__gdal_version__, rasterio.__version__
