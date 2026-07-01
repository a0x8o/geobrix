# `DemDownloader` — 3DEP DEM Downloader (`gbx.sample`)

**Status:** design (awaiting review)
**Date:** 2026-06-30
**Tier:** light (`gbx.sample`, pure Python/PySpark, Serverless-safe). Wraps `StacClient`.

## Goal

Add a product **`DemDownloader`** to `gbx.sample` — an AOI-driven downloader for USGS
**3DEP** elevation (STAC on Planetary Computer) — mirroring the existing `NaipDownloader`.
It replaces the hand-rolled inline STAC glue in Helios **NB-03 §1** (cell-5) with a single
product call, and gives users a first-class way to stage a DEM for an AOI.

## Context / motivation

NB-03 currently stages its DEM with ~30 lines of inline `StacClient` glue: build an AOI
GeoJSON, `client.search("3dep-seamless")`, filter to the `"data"` asset, compute the
minimum `gsd` and filter to it (3DEP-seamless offers the same area at multiple
resolutions — 10 m and 30 m), then `client.download(..., bbox=, bbox_crs=)`. `NaipDownloader`
already encapsulates exactly this shape for NAIP imagery (discover / download / read +
a one-shot `download_naip_aoi`). A DEM equivalent removes the last hand-rolled STAC glue
from the Helios series and is a reusable sample-data primitive.

`NaipDownloader` is **NAIP-flavored**: collection default `"naip"`, asset `"image"`, and
vintage selection by **latest year**. The DEM equivalent is **3DEP-flavored**: collection
default `"3dep-seamless"`, asset `"data"`, and selection by **finest resolution (gsd)**.

## Scope decisions (resolved)

- **Name / placement.** `class DemDownloader` + `download_dem_aoi` one-shot in a new
  `python/geobrix/src/databricks/labs/gbx/sample/dem.py`; exported from
  `sample/__init__.py`. Docs page **"3DEP Downloader (DEM)"** under Sample Data (mirrors
  the Overture / NAIP downloader pages).
- **Independent, not shared.** Kept parallel to `NaipDownloader` (as `NaipDownloader` and
  `OvertureClient` already coexist independently). No shared base class is extracted now —
  YAGNI until a third STAC downloader proves the pattern.
- **Selection axis = resolution, not year.** `resolution="finest"` (default) selects the
  minimum `gsd`; an int (`10` / `30`) filters to that exact gsd. Replaces NaipDownloader's
  `year` axis.
- **`collection` and `asset` are `__init__` params** (defaults `"3dep-seamless"` / `"data"`),
  overridable so the same class can serve another DEM collection; gsd-selection **no-ops
  gracefully** when a source has no `gsd` property (keeps all items).
- **`max_mpp=None` by default** (3DEP ~10 m over a city AOI is already small — no
  decimation), but exposed for parity with `NaipDownloader`.

## Out of scope

- Offline / synthetic fallback (online-only, like NaipDownloader).
- Non-STAC DEM sources (e.g. the AWS SRTM/`skadi` path in `sample/_bundle.py` — a separate,
  older bundle generator).
- A shared `_StacAoiDownloader` base (future, if a 3rd STAC downloader lands).
- Mosaicking / seam-handling across multiple DEM tiles (the caller's concern; NB-03 already
  documents the terrain-seam caveat).

## Architecture

New module `sample/dem.py`, mirroring `sample/naip.py`:

```
PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"
DEM_COLLECTION     = "3dep-seamless"
_DEM_ASSET         = "data"
_DEM_DATETIME      = "2000-01-01/2030-01-01"   # wide bracket; 3dep-seamless is a mosaic

class DemDownloader:
    def __init__(self, catalog=PLANETARY_COMPUTER, sign="planetary_computer",
                 collection=DEM_COLLECTION, asset=_DEM_ASSET, _stac_client=None): ...

    def discover(self, bbox, resolution="finest", spark=None) -> DataFrame:
        # columns: item_id (str), gsd (int), item_bbox (array<double>), href (str)
        # resolution="finest" -> return all matching items (caller sees the gsd tiers);
        # resolution=<int>     -> keep only items whose gsd == that value.

    def download(self, bbox, out_dir, resolution="finest", bbox_crs="EPSG:4326",
                 max_mpp=None, partitions=None, spark=None) -> DataFrame:
        # search -> filter asset -> resolve gsd tier -> StacClient.download(bbox, bbox_crs, max_mpp, partitions)
        # returns StacClient.download result: item_id, asset_name, out_file_path,
        #   out_file_sz, is_out_file_valid, last_update

    def read(self, out_dir, spark=None) -> DataFrame:
        # raster_gbx reader -> repartition(64, source) -> select("tile")   (mirrors NaipDownloader.read)

def download_dem_aoi(spark, bbox, out_dir, resolution="finest", max_mpp=None, **kw) -> DataFrame:
    # one-shot convenience wrapper (constructs a default DemDownloader, calls download)
```

**Resolution (gsd) selection** — the one piece of real logic, mirroring NB-03's inline code:
- Build `gsd_col = item_properties["gsd"].cast(int)`.
- `resolution == "finest"`: `min_gsd = df.agg(F.min(gsd_col)).first()[0]`; if `min_gsd is not
  None`, filter to `gsd_col == min_gsd`; else no-op (source lacks gsd → keep all).
- `resolution == <int>`: filter `gsd_col == <int>`.
- Empty selection → pass an empty projection to `StacClient.download` so it returns the
  canonical empty-schema DataFrame (same pattern as `NaipDownloader`).

## Data flow

`bbox (EPSG:4326)` → driver-side `discover`/search (metadata only) → resolve the gsd tier →
`StacClient.download(select(item_id, asset_name, href), out_dir, bbox=, bbox_crs=, max_mpp=,
partitions=)` → **distributed** windowed download (each asset in its own Spark task via
StacClient's `spark.range` fan-out; `bbox`+`bbox_crs` hand the AOI clip + correct georef to
the product) → GeoTIFF(s) in `out_dir`.

## Global constraints

- **Serverless-safe:** no `spark.conf.set` / `_jvm` / `sparkContext` / `.rdd`; parallelism
  comes only from `StacClient.download`'s range-scan fan-out.
- **Online-only:** requires `pystac-client` + `planetary-computer` (like NaipDownloader);
  no offline/synthetic fallback.
- **Injection seam:** `_stac_client` param bypasses catalog/network for offline unit tests.
- **No new SQL function**; no `function-info.json` / `registered_functions.txt` change
  (this is a Python `gbx.sample` client, not a columnar expression).
- CRS: `bbox_crs` defaults `"EPSG:4326"`; StacClient reprojects the bbox to the source CRS
  (3DEP is often EPSG:4269 NAD83) for the windowed read.

## Error handling

- No items intersect the AOI (or none match the requested gsd) → empty DataFrame with the
  canonical `StacClient.download` schema (Boolean `is_out_file_valid`, no `href`) — not a crash.
- Malformed `resolution` (not `"finest"` and not int-coercible) → `ValueError` with a clear message.
- Signed-URL expiry → handled by `StacClient`'s per-attempt re-sign (unchanged).

## Testing

Mirror `python/geobrix/test/sample/test_naip.py` (injected mock `_stac_client`, no network):
- **discover**: `resolution="finest"` returns all gsd tiers; `resolution=10` keeps only 10 m items.
- **download vintage selection**: with a mock search returning 10 m + 30 m items over the AOI,
  `resolution="finest"` selects the 10 m items (passed to the mock `download`); `resolution=30`
  selects the 30 m items; a no-gsd source keeps all.
- **empty**: no matching items → empty result with the canonical schema.
- **Serverless-safe**: assert no `spark.conf.set`/`_jvm`/`.rdd` in the module (a source scan,
  as other pyrx tests do).
- **Local end-to-end + Serverless smoke** (as done for the NB-03 gtiff swap): a small real
  `download_dem_aoi` against the live 3dep-seamless collection over a tiny AOI, asserting a
  valid GeoTIFF is staged with the finest gsd — run on Serverless (env v5) before NB-03 is wired.

Tests run on the host venv: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate
&& python -m pytest python/geobrix/test/sample/test_dem.py -v` (the offline/mock ones); the
Serverless smoke via a one-time job.

## Downstream consumer (motivation, not built here)

Helios NB-03 §1 (cell-5) collapses its inline STAC glue to:
```python
from databricks.labs.gbx.sample import download_dem_aoi
staged = download_dem_aoi(spark, SF_CITY_BBOX, DEM_DIR, resolution="finest")
DEM_PATHS = sorted(
    r["out_file_path"] for r in staged.collect()
    if r["is_out_file_valid"] and r["out_file_path"]
)
```
and the §1 narrative updates to describe the product `DemDownloader` (finest-gsd 3DEP
staging, windowed to the AOI in-product) instead of the hand-rolled `pystac`/filter glue —
paralleling how §1 of NB-02 reads for `NaipDownloader`. (Wiring NB-03 is a follow-up, not
part of this spec.)
