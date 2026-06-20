from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile
from rasterio.plot import show

from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import *

# -- GeoBrix tier selection (keep in sync with config_nb.ipynb) --
# option-1: lightweight tier (pure Python / PySpark, runs on Serverless) -- DEFAULT
from databricks.labs.gbx.pyrx import functions as rx
# option-2: heavyweight tier (Scala JAR + GDAL init script on a classic x86 cluster)
# from databricks.labs.gbx.rasterx import functions as rx

import json
import numpy as np
import pandas as pd
import rasterio
import requests
import shapely.geometry

FILE_SIZE_THRESHOLD = 1024
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"


def _set_conf_safe(spark, key, value):
  """Set a Spark conf, no-op on Serverless (which forbids runtime conf mutation).
  Mirrors config_nb's set_conf_safe so library helpers are Serverless-safe too."""
  try:
    spark.conf.set(key, value)
    return True
  except Exception as e:
    print(f"... skipping spark.conf.set({key}) [Serverless?]: {type(e).__name__}")
    return False


def generate_cells(extent, resolution, spark):
  # NOTE: uses rx.rst_h3_tessellateexplode, which is heavyweight-only (no pyrx binding).
  # Not called by the EO-series notebooks; on the lightweight tier prefer the
  # rst_h3_tessellate UDTF (as used in gen_tessellate_tiled_band) instead.
  polygon = shapely.geometry.box(*extent, ccw=True)
  wkt_poly = str(polygon.wkt)
  cells = spark.createDataFrame([[wkt_poly]], ["geom"])
  cells = cells.withColumn("grid", rx.rst_h3_tessellateexplode("geom", F.lit(resolution)))
  return cells


def get_unique_hrefs(assets_df, item_name):
  return (
    assets_df
      .select(
        "area_id",
        "h3",
        "asset.name",
        "asset.href",
        "item_id",
        F.to_date("item_properties.datetime").alias("date")
      )
      .where(
        f"name == '{item_name}'"
      )
      .groupBy(
      "href", "item_id", "date"
      )
      .agg(F.first("h3").alias("h3"))
  )


def to_numpy_arr(raster):
  with MemoryFile(BytesIO(raster)) as memfile:
    with memfile.open() as src:
      return src.read()


def _decimated_read(src, max_pixels):
    """Read `src` (a rasterio Dataset) decimated so that max(width, height) <= max_pixels.
    Returns (data, transform, scale). `masked=True` so nodata pixels (e.g. 65535 in
    Sentinel-2 saturation, 0 in many fill regions) are honored by downstream plotting
    and excluded from the percentile-stretch statistics."""
    scale = max(src.width, src.height) / max_pixels
    if scale > 1:
        out_shape = (src.count, int(src.height // scale), int(src.width // scale))
        data = src.read(
          out_shape=out_shape,
          resampling=rasterio.enums.Resampling.bilinear,
          masked=True,
        )
        transform = src.transform * src.transform.scale(
          src.width / data.shape[-1],
          src.height / data.shape[-2],
        )
    else:
        data = src.read(masked=True)
        transform = src.transform
    return data, transform, scale


def _needs_percentile_stretch(data):
    """True when `data` is integer-typed with values exceeding matplotlib's RGB int
    range of [0, 255]. Sentinel-2 / Landsat / most other UInt16 EO products fall here;
    if shown raw, matplotlib clips everything above 255 to white and warns. The fix
    is per-band 2nd–98th percentile stretch to [0, 1] float (see _percentile_stretch)."""
    if not np.issubdtype(data.dtype, np.integer):
        return False
    mx = np.ma.max(data) if isinstance(data, np.ma.MaskedArray) else data.max()
    if mx is np.ma.masked:
        return False
    return int(mx) > 255


def _percentile_stretch(data, lo_pct=2, hi_pct=98):
    """Per-band 2nd–98th percentile stretch to [0, 1] float32. What QGIS / EO Browser /
    most EO viewers do by default. Ignores masked pixels (nodata, saturation) when
    computing per-band percentiles so outliers don't compress the visible range. Mask
    is preserved on the returned array so matplotlib still renders nodata transparently."""
    if data.ndim == 2:
        data = data[np.newaxis, ...]
    is_masked = isinstance(data, np.ma.MaskedArray)
    out = np.empty(data.shape, dtype=np.float32)
    for b in range(data.shape[0]):
        band = data[b]
        valid = band.compressed() if is_masked else np.asarray(band).ravel()
        if valid.size == 0:
            out[b] = 0.0
            continue
        lo, hi = np.percentile(valid, (lo_pct, hi_pct))
        rng = max(float(hi - lo), 1e-9)
        out[b] = np.clip((np.asarray(band, dtype=np.float32) - lo) / rng, 0.0, 1.0)
    return np.ma.MaskedArray(out, mask=data.mask) if is_masked else out


def _render(data, transform, *, title, fig_w, fig_h, scale):
    """Apply percentile stretch when needed, then plot via rasterio.plot.show.
    Single-band rasters render with the 'viridis' colormap; multi-band render as RGB
    (rasterio's `show` handles that automatically). Title is suffixed with the
    decimation factor when the source was downsampled."""
    if _needs_percentile_stretch(data):
        data = _percentile_stretch(data)
    fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
    if data.shape[0] == 1:
        show(data, ax=ax, transform=transform, cmap='viridis')
    else:
        show(data, ax=ax, transform=transform)
    full_title = f"{title} (scale 1/{round(scale, 1)}x)" if scale > 1 else title
    ax.set_title(full_title)
    pyplot.show()


def plot_raster(raster_bytes, fig_w=10, fig_h=10, max_pixels=2000):
    """Render a raster from its in-memory bytes (e.g. `tile.raster` from a GeoBrix
    DataFrame). Auto-decimates if the source exceeds `max_pixels` on either axis; for
    integer rasters whose values exceed 255 (typical for EO data — Sentinel-2 UInt16
    reflectance, Landsat, etc.) applies a per-band 2nd–98th percentile stretch so
    matplotlib doesn't clip the visible range to white. Single-band rasters use the
    'viridis' colormap; multi-band render as RGB."""
    with MemoryFile(BytesIO(raster_bytes)) as memfile:
        with memfile.open() as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(data, transform, title="tile.raster", fig_w=fig_w, fig_h=fig_h, scale=scale)


def plot_file(file_path, fig_w=10, fig_h=10, max_pixels=2000):
    """Render a raster from disk (TIF, VRT, etc.) with the same decimation +
    percentile-stretch pipeline as `plot_raster`. See its docstring for details."""
    with rasterio.open(file_path) as src:
        data, transform, scale = _decimated_read(src, max_pixels)
        _render(
          data, transform,
          title=f"File: {file_path.split('/')[-1]}",
          fig_w=fig_w, fig_h=fig_h, scale=scale,
        )


def rasterio_lambda(raster, lambda_f):
  @udf(returnType=DoubleType())
  def f_udf(f_raster):
    with MemoryFile(BytesIO(f_raster)) as memfile:
      with memfile.open() as dataset:
        x = lambda_f(dataset)
        return float(x)

  return f_udf(raster)
