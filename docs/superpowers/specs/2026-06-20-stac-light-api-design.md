# STAC Lightweight API — Design

**Date:** 2026-06-20
**Status:** Approved (design); pending implementation plan
**Scope:** A lightweight, Serverless-safe STAC client for GeoBrix — distributed **search**, resilient **download**, and **repair** — consolidating logic currently scattered across the EO-series `library.py` + `config_nb` helpers.

## Motivation

The EO-series example re-implements STAC query + download from per-notebook helpers (`library.py`: `get_items`, `get_assets`, `get_assets_for_cells`, `download_asset_v2`; `config_nb`: `download_band`, `update_assets`, `download_missing_assets`). This logic is:

- **Scattered + copy-pasted** across `library.py` and `config_nb` — every notebook re-wires it.
- **Hardcoded to Planetary Computer** + `sign_inplace`.
- **Not Serverless-native** until recently (used `spark.conf.set`, now guarded).
- **Missing download resilience** until recently (size-only validity accepted throttled/truncated files).

This design packages the now-proven, hardened logic behind one importable, catalog-agnostic, Serverless-safe surface so notebooks become a few calls instead of ~150 lines of helpers.

This is **net-new lightweight capability** — the heavyweight tier has no STAC equivalent, so there is no cross-tier parity requirement.

## Goals

- A `StacClient` class (config held once) exposing `search`, `download`, `repair`.
- **Catalog-agnostic** (configurable catalog URL + pluggable signing), defaulting to Planetary Computer + `sign_inplace`.
- **Serverless-safe**: no `spark.conf.set`, no `.cache()`/`persist`; parallelism via `DataFrame.repartition(N)`.
- **Resilient download**: raise-on-HTTP-error, read-validation (open + decode a window), re-sign + retry/backoff, download-to-local → publish-to-Volume-only-when-valid.
- **Repair**: re-download only invalid rows (Delta MERGE).
- Distributed (Spark fan-out), idempotent (skip already-valid files).

## Non-goals (YAGNI)

Each is deliberately out of the initial build, with the reasoning and a revisit trigger.

- **No async / concurrent-within-task client.** Parallelism comes from Spark (one task per AOI/asset via `repartition`), not `asyncio`/threads inside a UDF, which only adds failure modes (loop lifecycle, partial-await cleanup) when the executor already fans out. *Revisit if* a single task must issue many small requests where per-request latency dominates (not the case for whole-asset GeoTIFF fetches).

- **No caching / memoization layer.** Search results and downloaded files are materialized by the *caller* (a Delta table, files on a Volume) — that already is the cache, inspectable and time-travel-friendly. *Revisit if* repeated identical searches in one session become a measured cost.

- **`download` is a faithful fetch — no transformation, no non-raster validation.** `search` surfaces every asset's `href`/metadata, but `download` fetches bytes as the catalog serves them and read-validates assuming a raster (open + decode a window). It does **not** reproject / re-tile / COG-convert / CRS-reconcile (the raster tier's job: `pyrx` `rst_*`, `gtiff_gbx`, `rst_merge_agg`), and it does **not** download-validate non-raster assets (JSON, thumbnails, vector sidecars — discoverable via `search`, just not first-class download targets). Keeps provenance intact. *Revisit if* validated non-raster downloads are needed (per-type validation strategy).

- **No catalog/item writing or publishing.** A read/consume client only (query a STAC API, fetch assets); creating items, writing a static catalog, or registering products is a separate feature.

- **No AOI grid generation (`generate_cells`).** The client takes a geometry/GeoJSON column as input; tessellation lives in the `rst_h3_tessellate` UDTF / Databricks built-in H3. (The existing `library.py` helper is dead code using the heavyweight-only `rst_h3_tessellateexplode`.)

- **No repair scheduling / orchestration / UI.** `repair` is a callable pass over invalid rows; deciding *when* to re-run it (a job schedule, loop-until-complete, alerting) is the caller's. *Revisit if* an "auto-complete until all valid" loop helper proves widely wanted.

- **No secret/credential management beyond `sign`.** Auth is expressed only through `sign` (`'planetary_computer'` | `None` | callable); token storage/refresh and provider auth flows are the caller's. *Revisit if* a catalog needs request-time auth headers (extend the signing/transport hook).

## Packaging

- New top-level package `databricks.labs.gbx.stac` (parallel to `pyrx`, `pyvx`, `pygx`, `ds`).
- New **optional** extra `geobrix[stac]` → `pystac-client`, `planetary-computer` (`tenacity` is already pulled by `[light]`). Keeps `[light]` lean; STAC is opt-in (`pip install geobrix[light,stac]`).
- Pure-Python; runs on Serverless (environment version 5+, Python 3.12) and classic.

## Module layout

```
gbx/stac/
  __init__.py        # exports StacClient
  client.py          # StacClient: config + orchestration (search / download / repair)
  _search.py         # pandas-UDF internals: per-AOI catalog.search() + item/asset parsing
  _download.py       # resilient download UDF (raise-on-HTTP-error, read-validate,
                     #   re-sign + retry/backoff, local-stage -> Volume publish)
  _sign.py           # signing strategies: 'planetary_computer' | None | callable(href)->href
```

## API surface

```python
from databricks.labs.gbx.stac import StacClient

client = StacClient(
    catalog="https://planetarycomputer.microsoft.com/api/stac/v1",  # default
    sign="planetary_computer",          # 'planetary_computer' | None | callable(href)->href
)

# SEARCH — AOI rows (a GeoJSON-geometry column) -> one row per (aoi, item, asset).
assets_df = client.search(
    df,
    geojson_col="geojson",
    collections=["sentinel-2-l2a"],
    datetime="2022-06-01",
    partitions=512,                     # repartition fan-out; no spark.conf
)
# columns: <carried input cols>, item_id, date, item_bbox, asset_name, href, item_properties

# DOWNLOAD — resilient, validated; one task per asset.
files_df = client.download(
    assets_df,
    out_dir,                            # a UC Volume path
    asset_names=["B02", "B03", "B04", "B08"],   # None = all assets present
    name="{asset_name}_{item_id}.tif",  # filename template
    validate=True,                      # read-validation (open + decode a window)
    max_tries=5,
    partitions=None,                    # default: one task per asset (count-based)
)
# columns: item_id, asset_name, out_file_path, out_file_sz, is_out_file_valid

# REPAIR — re-download only invalid rows (Delta MERGE); returns the repaired subset.
client.repair(table_or_df, where="is_out_file_valid = false")
```

## Behavior / data flow

### search
1. `repartition(partitions)` the AOI DataFrame (Serverless-safe fan-out).
2. A `pandas_udf` opens the configured catalog (with the configured signer) and runs `catalog.search(collections=, intersects=<geojson>, datetime=)` per row, with `tenacity` exponential retry; returns item JSONs.
3. Explode items → parse `item_id`, `date` (from `properties.datetime`), `item_bbox`, `item_properties`; explode assets → `asset_name`, `href`.
4. Returns one row per `(aoi, item, asset)`, carrying the input columns through.

### download
1. Optionally filter to `asset_names`.
2. **Dedup to unique `(item_id, asset_name)`** — `search` emits one row per *(aoi, item, asset)*, so the same item reached via multiple AOIs/cells would otherwise be fetched repeatedly. (The href is re-signed per attempt from `item_id`+`asset_name`, so a stale search-time href is not relied on.)
4. `repartition` to one task per asset (or `partitions`).
5. Resilient download UDF per asset:
   - **(re-)sign** the href each attempt (signed URLs expire).
   - `requests.get(...).raise_for_status()` (HTTP throttle/expiry → tenacity backoff).
   - download to **worker-local** disk.
   - **read-validation**: `rasterio.open` + decode a window (rejects throttled error bodies and truncated files that a size check would accept).
   - publish to the Volume with a **sequential copy** (FUSE-safe) only when valid.
   - on failure: exponential backoff, re-sign, retry up to `max_tries`; then return `None`.
6. Compute `is_out_file_valid` from the validated outcome; return the files DataFrame. Idempotent: an existing file above the size floor is treated as already-published (only validated files are ever written).

### repair
1. Read the band/files table (or DataFrame); filter to invalid (`is_out_file_valid` false/null) and optional `where`.
2. Re-run the resilient download on that subset.
3. `DeltaTable.merge` the repaired rows back (`out_file_path`, `out_file_sz`, `is_out_file_valid`, `last_update`).
4. Return the repaired subset.

## Error handling

- **Search:** per-AOI failures retry (tenacity); a permanently failing AOI yields an empty item list for that row rather than failing the job.
- **Download:** every failure mode (HTTP error, throttled body, truncation, expired URL, decode failure) funnels to `is_out_file_valid=False` and is retried in-UDF, then by `repair`. No partial/corrupt file is left on the Volume.
- **Serverless:** no `spark.conf.set` / `.cache()`; any incidental conf use goes through a guarded no-op helper.

## Testing

- **Unit (CI, no network):** `StacClient` accepts an injectable catalog opener (`_catalog_opener`) so tests use a fake catalog returning canned items. Cover: search parsing (items→assets→typed cols), download read-validation (good vs throttled/truncated bytes), retry/backoff (counts attempts), repair MERGE, and a Serverless-guard source check (no `spark.conf`/`.cache()` in the module).
- **Integration (marked, network):** `@pytest.mark.integration` — real Planetary Computer search + download of one small asset; excluded from CI (matches the existing integration-marker convention in `pyproject.toml`).

## EO-series refactor (follow-on, same effort)

Once built, refactor the EO-series to use the client:
- nb01 search → `client.search(cells_df, ...)` (replaces `get_assets_for_cells`).
- nb02 download → `client.download(...)` + `client.repair(...)` (replaces `download_band` / `download_missing_assets` orchestration).
- Remove the now-redundant `library.py`/`config_nb` STAC helpers (keep viz/plot helpers).

## Rollout

`stac` ships in the same wheel as `[light]` but behind the `[stac]` extra. The EO-series `config_nb` install line becomes `geobrix[light,stac]`.
