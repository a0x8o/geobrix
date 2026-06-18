"""Spark-free tile-returning edit ops: clip to geometry, change data type,
initialise NoData, threshold, stamp SRID, extract band. Each returns new
GTiff bytes."""

import numpy as np
import rasterio
import shapely
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


def clip_to_geom(ds, geom, all_touched: bool = False) -> bytes:
    """Clip a raster to a geometry; return GTiff bytes.

    ``geom`` may be a shapely geometry (preferred — carries SRID via
    ``shapely.set_srid``) or raw WKB/EWKB ``bytes`` (back-compat).

    Mirrors heavyweight ``RST_Clip``: if the cutline carries a positive SRID and
    the raster has a CRS, reproject the cutline from EPSG:srid to the raster CRS
    before masking. Otherwise (srid 0/unknown, or no raster CRS) the cutline is
    assumed to already be in the raster CRS and used as-is. Any failure to build
    the source CRS or transform falls back to using the cutline as-is.
    """
    if isinstance(geom, (bytes, bytearray)):
        geom = shapely.wkb.loads(bytes(geom))  # handles WKB and EWKB

    mask_shape = geom
    srid = shapely.get_srid(geom)  # 0 when no SRID is set
    dst_crs = ds.crs  # rasterio CRS or None
    if srid > 0 and dst_crs is not None:
        try:
            from rasterio.warp import transform_geom
            from shapely.geometry import mapping

            src_crs = rasterio.crs.CRS.from_epsg(srid)
            if src_crs != dst_crs:
                # transform_geom returns a GeoJSON-like dict; rasterio.mask
                # accepts GeoJSON geometries directly.
                mask_shape = transform_geom(src_crs, dst_crs, mapping(geom))
        except Exception:
            # Unknown EPSG / transform failure -> fall back to as-is (heavy's
            # "fall back to the raster's CRS" intent: assume already aligned).
            mask_shape = geom

    out_image, out_transform = _rio_mask(
        ds, [mask_shape], crop=True, all_touched=bool(all_touched)
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


def set_srid(ds, srid: int) -> bytes:
    """Stamp the CRS as ``EPSG:<srid>`` WITHOUT reprojecting.

    Mirrors the heavyweight ``gbx_rst_setsrid`` (``gdal_edit.py -a_srs``):
    pixel values and the GeoTransform are unchanged; only the CRS metadata is
    rewritten. Use ``rst_transform`` for an actual reprojecting warp.

    Args:
        ds:   Open rasterio DatasetReader.
        srid: Positive EPSG code to stamp.

    Returns:
        GTiff bytes with the same pixels/transform but CRS = EPSG:srid.
    """
    srid = int(srid)
    if srid <= 0:
        raise ValueError(f"rst_setsrid requires a positive EPSG code; got {srid}")
    try:
        crs = rasterio.crs.CRS.from_epsg(srid)
    except Exception as exc:  # invalid / unknown EPSG
        raise ValueError(f"rst_setsrid: unknown EPSG code {srid}") from exc
    profile = ds.profile.copy()
    profile.update(driver="GTiff", crs=crs)
    return _write(profile, ds.read())


def band(ds, band_index: int) -> bytes:
    """Extract a single 1-based band as a new single-band GTiff tile.

    Mirrors the heavyweight ``gbx_rst_band`` (``gdal_translate -b``): the
    extracted tile preserves the source CRS, GeoTransform, nodata, and dtype;
    only the band count is reduced to 1.

    Args:
        ds:         Open rasterio DatasetReader.
        band_index: 1-based band index in ``[1 .. ds.count]``.

    Returns:
        Single-band GTiff bytes.
    """
    band_index = int(band_index)
    n_bands = ds.count
    if not (1 <= band_index <= n_bands):
        raise ValueError(
            f"rst_band: band_index {band_index} out of range [1..{n_bands}]"
        )
    data = ds.read(band_index)
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1)
    # nodata in the multi-band profile already applies per-band; keep as-is.
    return _write(profile, data[np.newaxis, :, :])
