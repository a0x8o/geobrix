"""Spark-free accessor functions over an open rasterio DatasetReader.

These contain ALL the raster logic; the Spark layer (functions.py) only wraps
them in Arrow UDFs. Keeping them Spark-free makes them fast to unit test.
"""

import math
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
    # Ground pixel size in X = magnitude including skew: sqrt(scaleX^2 + skewY^2).
    # Mirrors heavyweight RST_PixelWidth (always non-negative); distinct from the
    # raw signed scalex(). rasterio Affine: a=scaleX(gt1), d=skewY(gt4).
    return float(math.hypot(ds.transform.a, ds.transform.d))


def pixelheight(ds) -> float:
    # Ground pixel size in Y = magnitude including skew: sqrt(scaleY^2 + skewX^2).
    # Mirrors heavyweight RST_PixelHeight (always non-negative); distinct from the
    # raw signed scaley(). rasterio Affine: e=scaleY(gt5), b=skewX(gt2).
    return float(math.hypot(ds.transform.e, ds.transform.b))


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


def scalex(ds) -> float:
    return float(ds.transform.a)


def scaley(ds) -> float:
    return float(ds.transform.e)


def isempty(ds) -> bool:
    return int(ds.width) == 0 or int(ds.height) == 0 or int(ds.count) == 0
