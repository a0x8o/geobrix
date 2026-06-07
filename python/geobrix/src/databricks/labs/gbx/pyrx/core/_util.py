"""Shared Spark-free helpers for pyrx core ops."""

from rasterio.enums import Resampling


def resampling_enum(name: str) -> Resampling:
    """Map a gdalwarp-style resampling name to rasterio's Resampling enum.

    Accepts 'near' as an alias for 'nearest'.
    """
    key = "nearest" if name == "near" else str(name)
    return Resampling[key]
