# Clipping - xView — Per-Object Raster Clipping with GeoBrix

An end-to-end example showing how to load high-resolution aerial GeoTIFFs from the [xView Detection Challenge](https://challenge.xviewdataset.org/) dataset into Lakehouse tables and clip rasters to labeled objects in the accompanying GeoJSON, using GeoBrix RasterX together with Databricks built-in Spatial SQL functions.

The single notebook moves from raw xView TGZ archives → `binaryFile`-loaded raster table → GeoJSON-derived object table (EWKT with SRID) → per-object clipped tiles written back to a Unity Catalog Volume as individual TIFs.

---

## Files

| File | Purpose |
|---|---|
| `Clipping - xView.ipynb` | The full pipeline. Sets up the catalog / schema / Volume, downloads + extracts the xView training TGZ and label GeoJSON to `/Volumes/<cat>/<schema>/data/`, loads rasters via `binaryFile` → `rst_fromfile`, builds an object table from `xView_train.geojson` via `st_geomfromgeojson` + `st_asewkt`, joins objects to their source tiles, applies `rst_clip(tile, wkt_clip, True)` with EWKT-encoded polygons, and writes the clipped rasters back to the Volume as `<index_right>_<type_id>_<feature_id>.tif`. |

---

## Prerequisites

- **Databricks Runtime 17.3 LTS** (Scala 2.13 / Spark 4 / Python 3.12).
- **GeoBrix** installed on the cluster (JAR + Python wheel). The notebook imports `databricks.labs.gbx.rasterx` directly; use version >= 0.3.0.
- **Unity Catalog**: set `catalog_name` / `schema_name` at the top of the notebook. A Volume named `data` must already exist under `<catalog>/<schema>`; the notebook will create the schema but not the Volume.
- **xView account**: you need a free account at [challenge.xviewdataset.org](https://challenge.xviewdataset.org/) to obtain session-based download links for `xView_train.tgz` (training imagery) and the labels archive. Paste those URLs into the `train_url` / `labels_url` cells before running the download step.
- **`rasterio==1.4.3`** (installed via `%pip` at the top of the notebook) — used for in-notebook previewing of both full tiles and clipped outputs.
- **Compute sizing**: an x86 cluster is required for the GDAL JNI natives. Memory/disk-optimized variants (e.g. `r6id.*`, `m5d.*`) are recommended for the raster-processing step; xView training tiles are ~3000×3000 RGB GeoTIFFs.

---

## Pipeline

```
xView train TGZ + xView_train.geojson  (session-signed downloads)
          │
          ▼  download_extract → /Volumes/<cat>/<schema>/data/{train_images, train_labels}
binaryFile reader over train_images/*.tif
          │
          ▼  rst_fromfile("path", "GTiff") + bbox + srid  →  xview_raster
GeoJSON objects (features)
          │
          ▼  st_geomfromgeojson → st_asewkt (SRID=4326)   →  xview_object
Join objects to rasters on path
          │
          ▼  rst_clip(tile, wkt_clip, True)                →  xview_object_clip
foreachPartition → open/write bytes
          │
          ▼  /Volumes/<cat>/<schema>/data/clip/<index_right>_<type_id>_<feature_id>.tif
```

---

## Key GeoBrix / Databricks functions shown

- **GeoBrix RasterX** (`rx.rst_*`): `rst_fromfile` (GTiff), `rst_boundingbox`, `rst_srid`, `rst_summary`, `rst_clip` (with EWKT input).
- **Databricks built-in ST** (`DBF.*`): `st_geomfromgeojson`, `st_asewkt` — used to emit each feature's polygon as `SRID=4326;POLYGON(...)` so that the SRID travels with the WKT into `rst_clip`.
- **Readers**: `binaryFile` (primary raster loader — `rst_fromfile` inlines the file bytes into the tile), `json` (multiline) for the xView labels GeoJSON.
- **Writers**: plain `open(..., "wb")` via `foreachPartition` to materialize clipped TIFs as individual files on the Volume.

---

## Gotchas

- **xView is session-signed**: download links from `challenge.xviewdataset.org` are time-limited. If `download_extract` hangs or returns tiny files, regenerate your links and re-run — the helper cleans `/tmp` on each run, so it is safe to retry.
- **Pass `[E]WKT` strings or `[E]WKB` bytes — not native geometry columns**: `rst_clip` expects a `String`/`Binary` column. Do **not** pass `st_geomfromtext(...)` / `st_geomfromwkb(...)` / DBR geometry or geography types directly. Serialize with `st_asewkt` (preferred — carries SRID) or `st_aswkb` / `st_asewkb` first.
- **EWKT vs WKT (SRID handling)**: in 0.3.0, if you pass plain **WKT**/**WKB** (no SRID), `rst_clip` assumes the geometry is already in the raster's CRS and **does not reproject**. If you pass **EWKT**/**EWKB** (SRID set) and the SRID differs from the raster CRS, the cutline is reprojected before clipping. xView imagery is EPSG:4326, and `st_asewkt(st_geomfromgeojson(...))` emits `SRID=4326;...`, so the two match for this dataset — but EWKT is the robust choice when sources differ.
- **Filter before clipping for demo runs**: `xview_object` contains hundreds of thousands of features across 60+ classes. The notebook filters to `type_name = 'Yacht'` (type_id=50) before the join + clip so the demo finishes quickly. Remove the filter for a full materialization.
- **Class label dictionary is inline**: `xv_type_dict` is copied from the [xView baseline repo](https://github.com/DIUx-xView/xView1_baseline/blob/master/xview_class_labels.txt); update it if xView publishes new classes.
