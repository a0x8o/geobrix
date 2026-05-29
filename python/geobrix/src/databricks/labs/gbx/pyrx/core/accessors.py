"""Spark-free accessor functions over an open rasterio DatasetReader.

These contain ALL the raster logic; the Spark layer (functions.py) only wraps
them in Arrow UDFs. Keeping them Spark-free makes them fast to unit test.
"""
from typing import Dict, Optional

from shapely import wkb as _wkb
from shapely.geometry import box


def width(ds) -> int:
    return int(ds.width)


def height(ds) -> int:
    return int(ds.height)


def numbands(ds) -> int:
    return int(ds.count)


def srid(ds) -> Optional[int]:
    return ds.crs.to_epsg() if ds.crs is not None else None


def pixelwidth(ds) -> float:
    return float(ds.transform.a)


def pixelheight(ds) -> float:
    return float(ds.transform.e)


def upperleftx(ds) -> float:
    return float(ds.transform.c)


def upperlefty(ds) -> float:
    return float(ds.transform.f)


def boundingbox(ds) -> bytes:
    b = ds.bounds  # (left, bottom, right, top)
    return _wkb.dumps(box(b.left, b.bottom, b.right, b.top))


def metadata(ds) -> Dict[str, str]:
    meta = {
        "driver": ds.driver,
        "width": str(ds.width),
        "height": str(ds.height),
        "count": str(ds.count),
        "dtype": str(ds.dtypes[0]) if ds.count else "",
        "crs": ds.crs.to_string() if ds.crs is not None else "",
        "nodata": "" if ds.nodata is None else str(ds.nodata),
    }
    meta.update({f"tag.{k}": str(v) for k, v in ds.tags().items()})
    return meta
