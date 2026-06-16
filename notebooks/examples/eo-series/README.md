# EO Series — End-to-End STAC to Gridded Rasters with GeoBrix

An end-to-end Earth Observation (EO) example series built on [GeoBrix](https://databrickslabs.github.io/geobrix/)'s RasterX functions, Databricks built-in Spatial SQL functions, and Microsoft [Planetary Computer](https://planetarycomputer.microsoft.com/) as the [STAC](https://stacspec.org/en) source.

The four main notebooks move from vector area-of-interest → STAC discovery → band download → gridded (H3) raster tables → multi-band stacking and clipping. Sentinel-2 L2A over Alaska is used as the working dataset (scoped to a single county — Ketchikan, `GEOID=2130` — to fit Planetary Computer free-tier limits).

> __Note:__ Downloads are throttled on the Planetary Computer free tier. Notebooks are written to be safely interruptible and idempotent — re-runs skip files that already exist at size.

---

## Notebooks at a glance

### 01 — Discover EO imagery via STAC

![Notebook 01 — AOI polygon → H3 res-2 cells → Planetary Computer STAC search → cell_assets Delta](../../../resources/images/eo-series-01.png)

- **Spatially-indexed STAC search** — tessellate any AOI polygon to H3 res-2 cells and query Planetary Computer per cell, so results are pre-keyed to the grid you'll join on later.
- **Shapefile I/O without unzipping** — the `shapefile_ogr` reader pulls TIGER counties straight from a `.zip` blob in the Volume; no scratch-disk shuffling required.
- **Persisted, time-travel-friendly catalog** — every search lands in a timestamped `cell_assets_<ts>.delta` directory, giving an auditable handoff into notebook 02.

### 02 — Parallel band download with idempotent retry

![Notebook 02 — STAC items fanning out to per-band downloads with retry loop and per-band Delta tables](../../../resources/images/eo-series-02.png)

- **Spark-driven concurrent download** — a `pandas_udf` (`download_band`) fans out per-(item, band) HTTPS retrievals across the cluster, writing files into a Volume and one Delta table per band.
- **Idempotent and self-healing** — a 1 KB validity threshold detects throttled auth-error payloads, and a Delta MERGE retry path (`update_assets` / `download_missing_assets`) repairs corrupt files without re-downloading the whole catalog.
- **Cleanly bounded scope** — only the bands you ask for (B02 / B03 / B04 / B08 by default) are pulled; the same flow extends to any other Sentinel-2 band.

### 03 — Tessellate rasters to H3 cells

![Notebook 03 — Sentinel-2 scene → typed tile struct → H3 res-7 tessellation → per-cell timeseries](../../../resources/images/eo-series-03.png)

- **One-step raster ingestion** — the `gdal` reader (and the `binaryFile` → `rst_fromcontent` pattern) materializes a typed `tile` column with bytes, bbox, SRID, and standardized nodata in a single pass.
- **Spatial-indexed raster tables** — `rst_h3_tessellate` shreds each Sentinel-2 scene into H3 resolution-7 cells, producing `band_b0X_h3` Delta tables that join cleanly across bands and dates.
- **Raster analytics from SQL/PySpark** — `rst_summary` for per-tile stats, `h3_kring` + `rst_merge_agg` for spatial neighborhoods, and `rasterio_lambda` for raster-to-timeseries projection — no driver-side rasterio loops.

### 04 — Band Stacking + Clipping

![Notebook 04 — per-band cell tables → rst_frombands → multi-band raster → GeoTIFF write-back → CRS-safe clip](../../../resources/images/eo-series-04.png)

- **Multi-band assembly from grid joins** — joins `band_b02_h3` / `b03` / `b04` / `b08` on `(cellid, date)`, then `rst_frombands` produces a single 4-band (R, G, B, NIR) tile per cell-date.
- **Round-trip GeoTIFF writes** — the `gdal` writer (`mode("append")`, `option("ext", "tif")`) materializes the stacked rasters back to disk in a Volume, ready for downstream tools.
- **CRS-safe geometry clipping** — clip cutlines built from `st_envelope` / `st_buffer` are passed as **EWKB** with embedded SRID, and `rst_clip` reprojects automatically — no per-tile CRS bookkeeping.

---

## Files

| File | Purpose |
|---|---|
| `config_nb.ipynb` | Shared setup (`%run ./config_nb` from every main notebook). Installs pip deps, imports Spark/Delta/GeoBrix, sets Unity Catalog `catalog_name` / `schema_name`, creates the `/Volumes/<cat>/<schema>/data/alaska` ETL tree, and defines helper functions (`download_band`, `update_assets`, `download_missing_assets`, `finalize_tiled_band_tbl`, `gen_tessellate_tiled_band`, viz helpers `as_gdf` / `cells_as_gdf`). |
| `library.py` | Python module (not a notebook) with reusable functions imported from `config_nb`: `pystac_client` access, pandas UDFs for STAC search (`get_items`, `get_assets`) and asset download (`download_asset`, `download_asset_v2`), H3 cell generation (`generate_cells`), and raster/rasterio plotting helpers (`plot_raster`, `plot_file`, `rasterio_lambda`, `to_numpy_arr`). |
| `01. Search STACs.ipynb` | Loads the TIGER US Counties shapefile via the `shapefile_ogr` reader, filters to Ketchikan, tessellates into H3 resolution-2 cells, converts each cell to GeoJSON, and queries Planetary Computer for `sentinel-2-l2a` items intersecting each cell. Writes the resulting STAC asset metadata to a timestamped Delta directory (`cell_assets_<ts>.delta`). |
| `02. Download STACs.ipynb` | Reads `cell_assets_*.delta`, consolidates to unique `item_id`s, and downloads GeoTIFFs for bands of interest (B02, B03, B04, B08) into `/Volumes/.../alaska/<band>/` using the `download_band` helper. Creates one `band_<band>` Delta table per band with `out_file_path` / `out_file_sz` / `is_out_file_valid` columns. Includes `download_missing_assets` / `update_assets` flows to patch files corrupted by free-tier throttling. |
| `03. Gridded EO Data.ipynb` | For each band, joins the Delta band table with the `gdal` reader (GTiff), materializes `band_<band>_tile` (adds `size`, `bbox`, `srid`, and standardized nodata), then tessellates each tile to H3 resolution 7 into `band_<band>_h3`. Demonstrates `rst_summary`, bounding-box reprojection, `h3_kring` with `rst_merge_agg`, and raster → timeseries projection via `rasterio_lambda`. |
| `04. Band Stacking + Clipping.ipynb` | Joins the four `band_<band>_h3` tables on `(cellid, date)`, stacks bands in (R, G, B, NIR) order with `rst_frombands` into the `band_stack` table, writes multi-band TIFs back out via the `gdal` writer, and demonstrates per-tile clipping with `rst_clip` using a centroid-envelope buffer built from Databricks built-in ST functions. |

---

## Prerequisites

- **Databricks Runtime 17.3 LTS or 18 LTS** (Scala 2.13 / Spark 4 / Python 3.12).
- **GeoBrix** installed on the cluster (JAR + Python wheel). The notebooks `import` the Python bindings directly from `databricks.labs.gbx.rasterx`.
- **Unity Catalog**: edit `config_nb.ipynb` to set `catalog_name` and `schema_name` to your own locations. A Volume named `data` must already exist under `<catalog>/<schema>`. The notebooks create a schema if missing but will not create the Volume for you.
- **Compute sizing** (the values used for the captured runs):
  - Notebooks 01/02 (search + download): AWS `m5d.xlarge`, 2–16 workers auto-scaling (up to ~64 concurrent downloads).
  - Notebooks 03/04 (raster processing): AWS `r6id.2xlarge`, 20 workers. An `x86` instance is required for the GDAL JNI natives; memory/disk-optimized variants are recommended. For a single county a much smaller cluster is sufficient.

---

## Run order

1. Open `config_nb.ipynb`, set `catalog_name` / `schema_name`, and verify the Volume exists.
2. Run notebooks in numeric order: **01 → 02 → 03 → 04**. Each notebook starts with `%run ./config_nb` so the shared state is re-established every time.

Each notebook is safe to re-run — Delta tables use `do_overwrite=False` / `do_append=False` by default, and file downloads skip anything already present above `library.FILE_SIZE_THRESHOLD` (1 KB, used to detect Planetary Computer auth-error responses masquerading as tiny "downloads").

---

## Data flow

```
TIGER shapefile (shapefile_ogr reader)
        │
        ▼  Ketchikan polygon → H3 res-2 cells → GeoJSON
STAC search (Planetary Computer, sentinel-2-l2a)
        │
        ▼  cell_assets_<ts>.delta      (nb 01)
Per-band asset download
        │
        ▼  band_b02, band_b03, band_b04, band_b08  (nb 02)
GDAL read + tile metadata
        │
        ▼  band_b0X_tile                (nb 03)
H3 tessellation @ resolution 7
        │
        ▼  band_b0X_h3                  (nb 03)
Join on (cellid, date) + rst_frombands
        │
        ▼  band_stack                   (nb 04)
GDAL writer → stacked TIFs + rst_clip
        │
        ▼  /Volumes/.../alaska/out/stacked-tif
```

---

## Key GeoBrix / Databricks functions shown

- **GeoBrix RasterX** (`rx.rst_*`): `rst_h3_tessellate`, `rst_h3_tessellateexplode`, `rst_memsize`, `rst_initnodata`, `rst_boundingbox`, `rst_srid`, `rst_tryopen`, `rst_summary`, `rst_metadata`, `rst_numbands`, `rst_frombands`, `rst_fromcontent`, `rst_merge_agg` (aggregator), `rst_clip`, `rst_isempty`.
- **GeoBrix readers/writers**: `shapefile_ogr` (zipped shapefiles without unzipping), `gdal` (GTiff reader + writer with `mode("append")` and `option("ext", "tif")`), `binaryFile` → `rst_fromcontent` pattern.
- **Databricks built-in ST / H3** (`DBF.*`): `st_geomfromwkt`, `st_transform`, `st_buffer`, `st_simplify`, `st_astext`, `st_asgeojson`, `st_aswkb`, `st_asewkb`, `st_centroid`, `st_envelope`, `h3_tessellateaswkb`, `h3_boundaryasgeojson`, `h3_boundaryaswkt`, `h3_toparent`, `h3_kring`.

---

## Gotchas

- **Antimeridian**: Alaska straddles the 180° meridian, so folium renderings can show results on both sides of the map.
- **SRID awareness**: Sentinel-2 tiles arrive in UTM zones (e.g. `32608`, `32609`), not EPSG:4326 — reproject bboxes before plotting on a web map.
- **Free-tier auth-failure payloads**: Planetary Computer returns a ~550-byte XML error body when SAS tokens expire or rate limits hit. The `is_out_file_valid` column uses the 1 KB `FILE_SIZE_THRESHOLD` to detect these and enables a Delta MERGE-based retry via `update_assets` / `download_missing_assets`.
- **Shuffle partitioning**: Several helpers temporarily disable `spark.sql.adaptive.coalescePartitions.enabled` and raise `spark.sql.shuffle.partitions` during download / stacking to keep parallelism high, then restore the original value.
- **Prefer EWKB for `rst_clip` cutlines**: notebook 04 uses `DBF.st_asewkb(DBF.st_envelope("buffer"))` so the cutline's SRID travels with the bytes into `rst_clip`. Plain WKB (no SRID) is assumed to already be in the raster's CRS and is **not** reprojected; EWKB (or EWKT) with a valid SRID triggers reprojection when it differs from the raster CRS. Use `st_asewkb` / `st_asewkt` for robust, CRS-agnostic clipping.
- **Scalar booleans pass through directly**: in 0.3.0 `rx.rst_clip("tile", "clip_wkb", True)` accepts a bare Python `True` for the `cutToCutline` flag — no `F.lit(True)` wrapping needed.
