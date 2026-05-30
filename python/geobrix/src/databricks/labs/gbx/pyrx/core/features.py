"""Spark-free vector<->raster bridge ops. rasterize: build a raster from a
geometry. fill_nodata: interpolate across NoData. Both return GTiff bytes."""

import numpy as np
import shapely.wkb
from rasterio.features import rasterize as _rasterize
from rasterio.fill import fillnodata as _fillnodata
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

_NODATA = -9999.0


def rasterize_geom(
    geom_wkb: bytes, value, xmin, ymin, xmax, ymax, width_px, height_px, srid
) -> bytes:
    """Burn a geometry (WKB) into a new raster at the given extent/size/SRID.

    Pixels inside the geometry get *value*; outside pixels get NoData (-9999.0).
    Returns GTiff bytes.
    """
    geom = shapely.wkb.loads(bytes(geom_wkb))
    width_px = int(width_px)
    height_px = int(height_px)
    transform = from_bounds(
        float(xmin), float(ymin), float(xmax), float(ymax), width_px, height_px
    )
    arr = _rasterize(
        [(geom, float(value))],
        out_shape=(height_px, width_px),
        transform=transform,
        fill=_NODATA,
        dtype="float64",
    )
    profile = dict(
        driver="GTiff",
        width=width_px,
        height=height_px,
        count=1,
        dtype="float64",
        crs=f"EPSG:{int(srid)}",
        transform=transform,
        nodata=_NODATA,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr, 1)
        return mf.read()


def fill_nodata(ds, max_search_dist=None, smoothing_iter=None) -> bytes:
    """Interpolate across NoData gaps in a raster dataset.

    *ds* is an open rasterio DatasetReader. Returns GTiff bytes with NoData
    pixels filled by interpolation from their neighbours.
    """
    msd = 100.0 if max_search_dist is None else float(max_search_dist)
    smi = 0 if smoothing_iter is None else int(smoothing_iter)
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    bands = []
    for i in range(1, ds.count + 1):
        band = ds.read(i)
        msk = ds.read_masks(i)  # 0 where NoData, 255 where valid
        bands.append(
            _fillnodata(
                band, mask=msk, max_search_distance=msd, smoothing_iterations=smi
            )
        )
    data = np.stack(bands)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()
