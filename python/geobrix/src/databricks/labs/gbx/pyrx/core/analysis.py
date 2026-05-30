"""Spark-free RasterX *Analysis* ports: proximity (distance-to-source) and
COG conversion.

Faithful ports of the heavyweight ``gbx_rst_proximity`` (``gdal.ComputeProximity``)
and ``gbx_rst_cog_convert`` (``gdal.Translate -of COG``) expressions, implemented
without the JAR:

  * ``proximity`` uses ``scipy.ndimage.distance_transform_edt`` (scipy is already
    a pyrx dependency) instead of GDAL's ComputeProximity.
  * ``cog_convert`` uses ``rio-cogeo``'s ``cog_translate`` instead of
    ``gdal_translate -of COG``.
"""

import math
import os
import tempfile

import numpy as np
from rasterio.io import MemoryFile

# NoData sentinel for the proximity output — mirrors the heavyweight, which sets
# NODATA=-1.0 so beyond-max / unreachable pixels are distinguishable from
# zero-distance (source) pixels.
_PROXIMITY_NODATA = -1.0


def proximity(ds, target_values, distunits, max_distance):
    """Compute a Float32 proximity raster: each pixel holds the distance to the
    nearest source pixel.

    Mirrors the heavyweight ``gbx_rst_proximity`` semantics:

      * ``target_values``: optional comma-separated string of source pixel
        values. When given, source pixels are those whose value is in that set.
        When None/empty, the GDAL default applies: source = pixels with value
        ``!= 0``.
      * ``distunits``: ``"GEO"`` (default) measures distance in CRS ground units
        (scaled by the pixel size from the GeoTransform); ``"PIXEL"`` measures in
        pixel counts. Any other value raises ``ValueError``.
      * ``max_distance``: optional cap; must be ``> 0`` and finite when given.
        Pixels whose distance exceeds it become NoData.

    The output is a single-band Float32 GTiff at the same extent/CRS as ``ds``,
    with ``nodata = -1.0``. Source pixels get distance 0.

    Args:
        ds:            Open rasterio DatasetReader.
        target_values: Optional comma-separated source-value string, or None.
        distunits:     ``"GEO"`` or ``"PIXEL"``.
        max_distance:  Optional positive distance cap, or None.

    Returns:
        Single-band Float32 GTiff bytes (nodata = -1.0).
    """
    distunits = "GEO" if distunits is None else str(distunits)
    if distunits not in ("GEO", "PIXEL"):
        raise ValueError(
            f"rst_proximity: distunits must be 'GEO' or 'PIXEL'; got '{distunits}'"
        )
    if max_distance is not None:
        max_distance = float(max_distance)
        if (
            not (max_distance > 0.0)
            or math.isinf(max_distance)
            or math.isnan(max_distance)
        ):
            raise ValueError(
                f"rst_proximity: max_distance must be > 0 and finite; got {max_distance}"
            )

    # scipy is a pyrx dependency; import lazily so module import stays cheap.
    from scipy import ndimage

    band1 = ds.read(1)

    # Build the source mask.
    if target_values is not None and str(target_values).strip() != "":
        vals = [float(v) for v in str(target_values).split(",") if v.strip() != ""]
        source_mask = np.isin(band1, np.array(vals, dtype=band1.dtype))
    else:
        # GDAL default: any non-zero pixel is a source/target.
        source_mask = band1 != 0

    # distance_transform_edt computes, for every True (non-zero) cell of its
    # input, the distance to the nearest False (zero) cell. We want the distance
    # to the nearest SOURCE pixel, so invert the source mask: source pixels are
    # the "background" (0 distance) and everything else measures distance to it.
    if distunits == "GEO":
        # Pixel ground size from the GeoTransform. sampling = (row spacing,
        # col spacing) = (pixel height, pixel width).
        px_w = abs(ds.transform.a)
        px_h = abs(ds.transform.e)
        sampling = (px_h, px_w)
    else:
        sampling = 1.0

    if not source_mask.any():
        # No source pixels at all: every pixel is unreachable -> all NoData.
        dist = np.full(band1.shape, _PROXIMITY_NODATA, dtype="float32")
    else:
        dist = ndimage.distance_transform_edt(~source_mask, sampling=sampling)
        dist = dist.astype("float32")
        if max_distance is not None:
            dist = np.where(dist > max_distance, _PROXIMITY_NODATA, dist).astype(
                "float32"
            )

    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        count=1,
        dtype="float32",
        nodata=_PROXIMITY_NODATA,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dist, 1)
        return mf.read()


def cog_convert(ds, compression, blocksize, overview_resampling):
    """Convert a raster to a Cloud Optimized GeoTIFF (COG) layout.

    Mirrors the heavyweight ``gbx_rst_cog_convert`` (``gdal.Translate -of COG``)
    using rio-cogeo's ``cog_translate``.

      * ``compression`` (default ``"DEFLATE"``): mapped to a rio-cogeo output
        profile (``cog_profiles.get(compression.lower())``). Unknown profiles
        raise ``ValueError``.
      * ``blocksize`` (default 512): internal tile size; must be ``> 0``.
      * ``overview_resampling`` (default ``"AVERAGE"``): downsampling algorithm
        for the auto-generated overview pyramid.

    The result is COG-layout GTiff bytes that reopen with rasterio.

    Args:
        ds:                  Open rasterio DatasetReader.
        compression:         COG compression / rio-cogeo profile name.
        blocksize:           Internal tile size in pixels (square).
        overview_resampling: Overview resampling algorithm name.

    Returns:
        COG-layout GTiff bytes.
    """
    blocksize = int(blocksize)
    if blocksize <= 0:
        raise ValueError(f"rst_cog_convert: blocksize must be > 0; got {blocksize}")
    if compression is None or str(compression).strip() == "":
        raise ValueError("rst_cog_convert: compression must be non-empty")
    if overview_resampling is None or str(overview_resampling).strip() == "":
        raise ValueError("rst_cog_convert: overview_resampling must be non-empty")
    compression = str(compression)
    overview_resampling = str(overview_resampling)

    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    try:
        output_profile = cog_profiles.get(compression.lower())
    except KeyError as exc:  # rio-cogeo raises KeyError for an unknown profile
        raise ValueError(
            f"rst_cog_convert: unknown compression '{compression}'; "
            f"valid profiles: {', '.join(sorted(cog_profiles.keys()))}"
        ) from exc
    if output_profile is None:
        raise ValueError(
            f"rst_cog_convert: unknown compression '{compression}'; "
            f"valid profiles: {', '.join(sorted(cog_profiles.keys()))}"
        )
    output_profile = dict(output_profile)
    output_profile.update(blockxsize=blocksize, blockysize=blocksize)

    # cog_translate needs a destination path; a real temp file is the most
    # reliable target (in_memory=True builds the COG in RAM, then writes it out).
    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.close()
    try:
        cog_translate(
            ds,
            tmp.name,
            output_profile,
            overview_resampling=overview_resampling.lower(),
            in_memory=True,
            quiet=True,
        )
        with open(tmp.name, "rb") as fh:
            return fh.read()
    finally:
        os.unlink(tmp.name)
