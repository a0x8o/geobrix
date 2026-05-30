"""Spark-free reproject/warp helpers (rasterio.warp). Tile-returning: each
function returns new GTiff bytes."""

import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform, reproject

from databricks.labs.gbx.pyrx.core._util import resampling_enum


def reproject_to_srid(ds, target_srid: int, resampling: str = "nearest") -> bytes:
    """Reproject an open dataset to EPSG:<target_srid>; return GTiff bytes."""
    dst_crs = f"EPSG:{int(target_srid)}"
    transform, width, height = calculate_default_transform(
        ds.crs, dst_crs, ds.width, ds.height, *ds.bounds
    )
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    resamp = resampling_enum(resampling)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for i in range(1, ds.count + 1):
                reproject(
                    source=rasterio.band(ds, i),
                    destination=rasterio.band(dst, i),
                    src_transform=ds.transform,
                    src_crs=ds.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=resamp,
                )
        return mf.read()
