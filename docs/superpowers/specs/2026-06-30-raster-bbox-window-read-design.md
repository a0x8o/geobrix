# Raster `bbox` Window-on-Read Design

**Status:** design (awaiting review)
**Date:** 2026-06-30
**Tier scope:** light (`raster_gbx`/`gtiff_gbx` + `StacClient`). Heavy (`gdal`/`gtiff_gdal`) parity is queued, not in this feature.

## Goal

Let customers window a (Cloud-Optimized) GeoTIFF to an area of interest **on read**, correct-by-construction, with one reader option — and the same capability on `StacClient.download`. Eliminate hand-rolled rasterio windowed reads in customer/notebook code (and the georeferencing footgun they invite).

## Context / motivation

The Helios NB-02 `stage_naip` (and NB-03 `stage_dem`) helpers hand-rolled a rasterio windowed read to clip a COG to an AOI:

```python
win = window_from_bounds(*proj_bounds, transform=src.transform)
data = src.read(window=win)                  # boundless=False -> CLIPS to the dataset
transform = src.window_transform(win)        # uses the UNCLIPPED window's origin
```

When the requested window overhangs the dataset edge (an AOI larger than a single COG tile — common), `read` returns only the overlapping pixels while `window_transform` keeps the unclipped window's top-left, so the raster is georeferenced off by the overhang (NB-02 NAIP rendered ~one image-height too far north). NB-03's `stage_dem` omits even the clip, so it is more exposed. This class of bug is exactly what a customer should never have to get right by hand.

GeoBrix already covers the *clip-a-loaded-raster-to-a-geometry* case with `gbx_rst_clip` (EWKT/EWKB carry an SRID and the cutline is reprojected to the raster's CRS; plain WKT/WKB is assumed to be in the raster's CRS). The gap is **window-on-read**: the `raster_gbx`/`gtiff_gbx` readers expose only `path`/`sizeInMB`/`filterRegex`/`nameCol`/`ext` (`ds/raster.py:77-81`, `:204-212`) — no spatial window — and `StacClient.download` fetches the *whole* asset (`stac/client.py:209`). The **vector** reader already has the pattern: `bbox` = `"xmin,ymin,xmax,ymax"` pushed down to pyogrio (`ds/vector.py:537-547`).

## Scope decisions (resolved)

- **CRS convention — source CRS by default, `bboxCrs` override.** A plain `bbox` is interpreted in the raster's native CRS (mirrors `rst_clip`'s plain-WKT "assume raster CRS", the vector reader's layer-CRS `bbox`, and the GDAL `-projwin` norm). An optional `bboxCrs` option (e.g. `"EPSG:4326"`) is the SRID-equivalent escape hatch: declare the bbox's CRS and the reader reprojects the bbox to the source CRS before windowing.
- **Light tier now.** `raster_gbx`/`gtiff_gbx` readers + `StacClient.download`. Heavy `gdal`/`gtiff_gdal` (Scala) `bbox` parity is a tracked follow-up. Reader options are not under the function binding-parity gate, so a light-only reader option is consistent.
- **Decimation is out of scope.** Downsampling on read is a separate, *existing* concern: `gbx_rst_resample_to_res` / `gbx_rst_resample_to_size` (`pyrx/functions.py:614/600`). The windowed read stays pure spatial windowing; callers who want a smaller AOI raster chain a resample after.

## Global constraints

- **Serverless-safe (light tier).** No `spark.conf.set`, no `_jvm`/`sparkContext`/`.rdd` in the reader path. GDAL only via `rasterio` (never raw `osgeo.gdal`); configure the GDAL env via the existing light-tier helper (`databricks.labs.gbx.pyrx._env.configure_gdal_env` pattern) inside the executor read.
- **No new SQL function.** This is reader/client option surface, not a registered `gbx_*` function — no `function-info.json` / `registered_functions.txt` change.
- **Georeferencing correctness is the headline requirement.** The windowing primitive must be unit-tested against the overhang case that caused the original bug.

## Architecture

Three units, one shared correctness core:

```
ds/_window.py            window_to_bbox(src, bbox, bbox_crs)  <-- correctness core
   ^                          ^
   |                          |
ds/raster.py             stac/client.py
RasterGbxReader          StacClient.download(bbox=, bbox_crs=)
(+ gtiff_gbx variant)
```

### Component 1 — shared windowing primitive (`ds/_window.py`, new)

Single source of truth for "open dataset + AOI → correctly-georeferenced windowed read." Used by both the reader and `StacClient.download` so the footgun is fixed in exactly one place.

**Interface:**

```python
def window_to_bbox(
    src,                       # open rasterio DatasetReader
    bbox: tuple[float, float, float, float],   # (minx, miny, maxx, maxy)
    bbox_crs: str | None = None,               # None => bbox already in src.crs
) -> tuple[np.ndarray, "Affine", dict] | None:
    """Read the window of `src` covering `bbox`, georeferenced correctly.

    Returns (data, transform, profile) or None when the bbox does not overlap
    the dataset. `data` is (bands, h, w); `profile` is src.profile updated with
    the windowed width/height/transform and driver=GTiff.
    """
```

**Algorithm (the footgun fix):**

1. If `bbox_crs` is set and differs from `src.crs`: `bbox = rasterio.warp.transform_bounds(bbox_crs, src.crs, *bbox)`.
2. `win = rasterio.windows.from_bounds(*bbox, transform=src.transform)`.
3. **Clip to the dataset before anything else:** `win = win.intersection(Window(0, 0, src.width, src.height))`. If there is no intersection (rasterio raises `WindowError`, or the clipped window has width/height < 1), return `None`.
4. `data = src.read(window=win)` — clipped window → clipped pixels.
5. `transform = src.window_transform(win)` — the **same clipped window** → georef matches the pixels exactly.
6. `profile = {**src.profile, "driver": "GTiff", "width": data.shape[2], "height": data.shape[1], "transform": transform}`.
7. Return `(data, transform, profile)`.

This is the verified fix: read and `window_transform` always consume the *same clipped* window, so the top-left can never drift north of the data. (Validated locally: a 3000 m north overhang produced a 3000 m georef error in the old pattern and 0 m with the clip.)

### Component 2 — raster reader `bbox`/`bboxCrs` option

`RasterGbxReader.__init__` (`ds/raster.py:76`) parses two new options alongside the existing ones:

- `bbox` — `"minx,miny,maxx,maxy"` (same string format as the vector reader). Parsed to a 4-float tuple; a non-4 value raises `ValueError("raster bbox option must be 'minx,miny,maxx,maxy'; got ...")` (mirrors `ds/vector.py:540-544`).
- `bboxCrs` — optional CRS string (e.g. `"EPSG:4326"`). Default `None` ⇒ bbox is in the source CRS.

Behavior:

- When `bbox` is set, the **whole-image GTiff fast path is disabled** (`ds/raster.py:134`): a windowed read can no longer be a verbatim byte copy. Each matched source file is opened with rasterio and run through `window_to_bbox`; the returned `(data, profile)` is re-encoded to a GTiff tile via the reader's existing tile-encode path.
- A source file whose extent does not overlap the bbox (`window_to_bbox` → `None`) **yields no tile** (the file is skipped), consistent with a spatial filter. With multiple input files this naturally keeps only the intersecting ones.
- `gtiff_gbx` inherits the same option surface (it is the GTiff-specialized variant of the same reader).

The bbox is applied per source file independently, so it composes with `filterRegex` and the existing fan-out (one partition per source path).

### Component 3 — `StacClient.download(bbox=, bbox_crs=)`

`StacClient.download` (`stac/client.py:209`) gains optional `bbox` and `bbox_crs` parameters. In the fetch path (where the raw href is signed once via `resolve_signer`, `stac/client.py:261-270`), when `bbox` is provided the signed href is opened with rasterio (GDAL `/vsicurl` → only the AOI byte ranges are fetched from the remote COG) and read through `window_to_bbox`; the windowed GeoTIFF is written instead of the whole asset. When `bbox` is `None`, behavior is unchanged (whole-asset fetch).

This is the change that fully removes the `stage_naip`/`stage_dem` hand-rolled staging: a customer calls `StacClient.search(...).download(..., bbox=SF_CITY_BBOX, bbox_crs="EPSG:4326")` and gets a correctly-windowed, correctly-georeferenced COG on the Volume with no rasterio in their code.

## Error handling

- Malformed `bbox` string → `ValueError` with the expected format (fail fast at option-parse time).
- `bbox_crs` that rasterio/pyproj cannot resolve → the underlying CRS error propagates (actionable).
- No overlap → `None` from the primitive → reader skips the file; `StacClient.download` with a bbox that misses the item raises a clear "bbox does not overlap asset" error (a single explicit download should not silently produce nothing).
- Reading remote hrefs is subject to the usual GDAL `/vsicurl` network errors; no new handling beyond what `StacClient` already does.

## Testing (TDD)

**`ds/_window.py` primitive (unit, with small local GeoTIFF fixtures):**
- Fully-inside window → exact bounds + pixel count.
- **North / south / east / west overhang → exact georef** (regression for the NB-02 bug; assert the clipped window's top-left equals the dataset's where it overhangs).
- No overlap → returns `None`.
- `bbox_crs` differs from source (WGS84 bbox against a UTM fixture) → bbox transformed, correct window.
- `bbox_crs=None` → bbox taken as source CRS (no transform).

**Reader (integration, Spark, `raster_gbx` + `gtiff_gbx`):**
- `.option("bbox", ...)` windows a fixture COG to the AOI; output tile bounds == AOI∩source.
- `.option("bboxCrs", "EPSG:4326")` with a WGS84 bbox on a UTM source → correct.
- A non-overlapping file in a multi-file read → no rows for it.
- `raster_gbx` vs `gtiff_gbx` parity on the same input.

**`StacClient.download(bbox=)`:** with a small local fixture served as a file (or `/vsicurl` against a fixture), windowed download has correct georef; `bbox=None` path unchanged.

## Out of scope / queued (tracked, not in this feature)

- Heavy `gdal`/`gtiff_gdal` (Scala) `bbox` parity.
- Backporting `bboxCrs` to the **vector** reader (its `bbox` is currently layer-CRS only) for cross-reader symmetry.
- On-read decimation/resolution options (use existing `rst_resample_to_res`).
- The downstream **Helios notebook rework** that consumes this (NAIP/DEM via `StacClient.download(bbox=)`, all PMTiles writes → `pmtiles_gbx` writer, NB-04 → sharded writer + catalog, NB-03 COG → `gtiff_gbx`) — its own plan, after this feature lands and is parity-validated.
- A vizx helper to render a written shard `catalog.json` as one mosaic map (NB-04's hand-rolled `mosaic.json`).

## Downstream consumer (motivation, not built here)

Once shipped, `stage_naip`/`stage_dem` collapse to a `StacClient.search(...).download(..., bbox=SF_CITY_BBOX, bbox_crs="EPSG:4326")` call (per source quad), and the full-SF multi-tile NAIP rework reads correctly-windowed quads with no notebook rasterio.
