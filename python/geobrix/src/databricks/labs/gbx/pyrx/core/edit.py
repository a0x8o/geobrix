"""Spark-free tile-returning edit ops: clip to geometry, change data type,
initialise NoData, threshold. Each returns new GTiff bytes."""

import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.mask import mask as _rio_mask

# GDAL data-type name -> numpy dtype string.
_GDAL_TO_NP = {
    "Byte": "uint8",
    "Int8": "int8",
    "UInt16": "uint16",
    "Int16": "int16",
    "UInt32": "uint32",
    "Int32": "int32",
    "Float32": "float32",
    "Float64": "float64",
}

_DEFAULT_NODATA = -9999.0


def _write(profile, data) -> bytes:
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def clip_to_geom(ds, geom_wkb: bytes, all_touched: bool = False) -> bytes:
    """Clip a raster to a geometry (WKB bytes); return GTiff bytes."""
    geom = shapely.wkb.loads(bytes(geom_wkb))
    out_image, out_transform = _rio_mask(
        ds, [geom], crop=True, all_touched=bool(all_touched)
    )
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        height=out_image.shape[1],
        width=out_image.shape[2],
        transform=out_transform,
    )
    return _write(profile, out_image)


def update_type(ds, new_type: str) -> bytes:
    """Cast all bands to a new GDAL data type name (e.g. 'Int32'); return GTiff bytes."""
    np_dtype = _GDAL_TO_NP[new_type]
    data = ds.read().astype(np_dtype)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", dtype=np_dtype)
    # Drop a nodata value that can't be represented in the new dtype.
    nd = profile.get("nodata")
    if nd is not None:
        import numpy as np

        if np.issubdtype(np.dtype(np_dtype), np.integer):
            info = np.iinfo(np_dtype)
            if not (info.min <= nd <= info.max):
                profile["nodata"] = None
    return _write(profile, data)


def init_nodata(ds, default: float = _DEFAULT_NODATA) -> bytes:
    """Ensure NoData is set on the raster; use *default* if not already set.

    If NoData is already set, the existing value is preserved.
    """
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    if profile.get("nodata") is None:
        profile["nodata"] = default
    return _write(profile, ds.read())


_THRESHOLD_OPS = {
    ">": np.greater,
    "<": np.less,
    ">=": np.greater_equal,
    "<=": np.less_equal,
    "==": np.equal,
    "!=": np.not_equal,
}


def threshold(ds, op: str = ">", value: float = 0.0) -> bytes:
    """Keep pixels satisfying ``op value``; set others to NoData.

    Args:
        ds:    Open rasterio DatasetReader.
        op:    Comparison operator string: one of ">", "<", ">=",
               "<=", "==", "!=".  Defaults to ">".
        value: Threshold scalar.  Defaults to 0.0.

    Returns:
        GTiff bytes with the same dtype and band count; pixels that fail
        the comparison are replaced with the raster's NoData value
        (``-9999.0`` when not set).
    """
    op = ">" if op is None else str(op)
    value = 0.0 if value is None else float(value)
    fn = _THRESHOLD_OPS[op]
    data = ds.read()
    nd = ds.nodata if ds.nodata is not None else _DEFAULT_NODATA
    keep = fn(data, value)
    out = np.where(keep, data, nd).astype(data.dtype)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", nodata=nd)
    return _write(profile, out)
