from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile
from rasterio.plot import show

from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import *

from databricks.labs.gbx.rasterx import functions as rx

import json
import numpy as np
import pandas as pd
import planetary_computer
import pystac_client
import rasterio
import requests
import shapely.geometry

FILE_SIZE_THRESHOLD = 1024
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"

ps_client =  pystac_client.Client.open(
  "https://planetarycomputer.microsoft.com/api/stac/v1",
  modifier=planetary_computer.sign_inplace
)


def generate_cells(extent, resolution, spark):
  polygon = shapely.geometry.box(*extent, ccw=True)
  wkt_poly = str(polygon.wkt)
  cells = spark.createDataFrame([[wkt_poly]], ["geom"])
  cells = cells.withColumn("grid", rx.rst_h3_tessellateexplode("geom", F.lit(resolution)))
  return cells


@udf(returnType=ArrayType(StringType()))
def get_assets(item):
  item_dict = json.loads(item)
  assets = item_dict["assets"]
  return [json.dumps({**{"name": asset}, **assets[asset]}) for asset in assets]


@pandas_udf(ArrayType(StringType()))
def get_items(geojsons: pd.Series, date_times: pd.Series, collections: pd.Series) -> pd.Series:

  from tenacity import retry, wait_exponential

  @retry(wait=wait_exponential(multiplier=2, min=4, max=240))
  def search_with_retry(geojson, catalog, collection, dt):
    search = catalog.search(
        collections = collection,
        intersects = geojson,
        datetime = dt
      )
    items = search.item_collection()
    return [json.dumps(item.to_dict()) for item in items]

  def search_catalog(geojson, catalog, collection, dt):
    try:
      return search_with_retry(geojson, catalog, collection, dt)
    except Exception as inst:
      return [str(inst)]

  catalog =  pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
  )
  # - iterate over all the series at once
  items = []
  for geojson, collection, date_time in zip(geojsons, collections, date_times):
    items.append(
      search_catalog(geojson, catalog, collection, date_time)
    )
  return pd.Series(items)


def get_assets_for_cells(cells_df, period, source, spark, repart_num=512):
  try:
    orig_repart_num = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
    spark.conf.set("spark.sql.shuffle.partitions", repart_num)
    print(f"\t...shuffle partitions to {repart_num} for this operation.")
    return (
      cells_df
      .repartition(repart_num)
        .withColumn("items", get_items("geojson", F.lit(period), F.array(F.lit(source))))
      .repartition(repart_num)
        .withColumn("items", F.explode("items"))
        .withColumn("assets", get_assets("items"))
      .repartition(repart_num)
        .withColumn("assets", F.explode("assets"))
        .withColumn("asset", F.from_json(F.col("assets"), MapType(StringType(), StringType())))
        .withColumn("item", F.from_json(F.col("items"), MapType(StringType(), StringType())))
        .withColumn("item_properties", F.from_json("item.properties", MapType(StringType(), StringType())))
        .withColumn("item_collection", F.col("item.collection"))
        .withColumn("timestamp", F.col("item_properties").getItem("datetime").cast("timestamp"))
        .withColumn("date", F.col("timestamp").cast("date"))
        .withColumn("item_bbox", F.col("item.bbox"))
        .withColumn("item_id", F.col("item.id"))
        .withColumn("stac_version", F.col("item.stac_version"))
      .drop("assets", "items", "item")
      .repartition(repart_num, "item_id")
    )
  finally:
    # print(f"...setting shuffle partitions back to {orig_repart_num}")
    spark.conf.set("spark.sql.shuffle.partitions", orig_repart_num)


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


@pandas_udf(StringType())
def download_asset_v2(
  item_ids:pd.Series, asset_names:pd.Series, dir_fuse_paths:pd.Series, out_filenames:pd.Series
) -> pd.Series:
  """
  Do not accept an asset as downloaded below the size threshold.
  - this is because Planetary Computer will provide a message instead of
    the actual data when free tier limits are being hit or
    urls not signed (possibly expired)
  - write outpaths below size_threshold to dir_fuse_invalids
  - asset href is signed here to ensure it does not go stale
  """
  from pystac_client import Client
  from pystac_client.stac_api_io import StacApiIO
  from requests.adapters import HTTPAdapter
  from tenacity import retry, wait_exponential
  from urllib3 import Retry

  import os
  import pandas as pd
  import pystac_client
  import planetary_computer
  import requests

  @retry(wait=wait_exponential(multiplier=2, min=4, max=240))
  def download_href(href, outpath):
    # Make the actual request, set the timeout for no data to 10 seconds and enable streaming responses so we don't have to keep the large files in memory
    request = requests.get(href, timeout=100, stream=True)

    # Open the output file and make sure we write in binary mode
    with open(outpath, 'wb') as fh:
      # Walk through the request response in chunks of 1024 * 1024 bytes, so 1MiB
      for chunk in request.iter_content(1024 * 1024):
        # Write the chunk to the file
        fh.write(chunk)
        # Optionally we can check here if the download is taking too long
    return outpath

  def write_asset(collection, item_id, asset_name, out_dir, filename):
    """
    """
    size_threshold = 1024
    try:
      # - make sure out dir exists
      os.makedirs(out_dir, exist_ok=True)

      # - outpath assembled
      outpath = f'{out_dir}/{filename}'
      if not os.path.exists(outpath) or os.path.getsize(outpath) <= size_threshold:
        # - get the asset by asset_id and asset_name href
        item = collection.get_item(item_id)
        return download_href(item.assets[asset_name].href, outpath)
      else:
        #print(f"...skipping '{outpath}', already exits. Size? {os.path.getsize(outpath)}")
        return outpath
    except Exception as error:
      #print("EXCEPTION: ", error)
      return None
  
  # - construct catalog (with retry)
  # https://pystac-client.readthedocs.io/en/stable/usage.html#configuring-retry-behavior
  retry = Retry(
    total=5, backoff_factor=1, status_forcelist=[502, 503, 504], allowed_methods=None
  )
  stac_api_io = StacApiIO(max_retries=retry)
  client =  Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    stac_io=stac_api_io,
    modifier=planetary_computer.sign_inplace
  )
  collection = client.get_collection("sentinel-2-l2a")

  # - iterate over all the series at once
  out_file_paths = []
  for item_id, asset_name, dir_fuse_path, out_filename in zip(item_ids, asset_names, dir_fuse_paths, out_filenames):
    out_file_paths.append(
      write_asset(collection, item_id, asset_name, dir_fuse_path, out_filename)
    )
  return pd.Series(out_file_paths)


@pandas_udf(StringType())
def download_asset(
  item_ids:pd.Series, asset_names:pd.Series, dir_fuse_paths:pd.Series, out_filenames:pd.Series
) -> pd.Series:
  """
  Do not accept an asset as downloaded below the size threshold.
  - this is because Planetary Computer will provide a message instead of
    the actual data when free tier limits are being hit or
    urls not signed (possibly expired)
  - write outpaths below size_threshold to dir_fuse_invalids
  - asset href is signed here to ensure it does not go stale
  """
  from tenacity import retry, wait_exponential
  import os
  import pandas as pd
  import pystac_client
  import planetary_computer
  import requests

  @retry(wait=wait_exponential(multiplier=2, min=4, max=240))
  def download_href(href, outpath):
    # Make the actual request, set the timeout for no data to 10 seconds and enable streaming responses so we don't have to keep the large files in memory
    request = requests.get(href, timeout=100, stream=True)

    # Open the output file and make sure we write in binary mode
    with open(outpath, 'wb') as fh:
      # Walk through the request response in chunks of 1024 * 1024 bytes, so 1MiB
      for chunk in request.iter_content(1024 * 1024):
        # Write the chunk to the file
        fh.write(chunk)
        # Optionally we can check here if the download is taking too long
    return outpath

  def write_asset(catalog, item_id, asset_name, out_dir, filename):
    """
    """
    size_threshold = 1024
    try:
      # - make sure out dir exists
      os.makedirs(out_dir, exist_ok=True)

      # - outpath assembled
      outpath = f'{out_dir}/{filename}'
      if not os.path.exists(outpath) or os.path.getsize(outpath) <= size_threshold:
        # - get the asset by asset_id and asset_name href
        item = next(catalog.get_items(item_id), None)
        return download_href(item.assets[asset_name].href, outpath)
      else:
        #print(f"...skipping '{outpath}', already exits. Size? {os.path.getsize(outpath)}")
        return outpath
    except Exception as error:
      #print("EXCEPTION: ", error)
      return None

  # - construct catalog
  catalog =  pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
  )

  # - iterate over all the series at once
  out_file_paths = []
  for item_id, asset_name, dir_fuse_path, out_filename in zip(item_ids, asset_names, dir_fuse_paths, out_filenames):
    out_file_paths.append(
      write_asset(catalog, item_id, asset_name, dir_fuse_path, out_filename)
    )
  return pd.Series(out_file_paths)


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