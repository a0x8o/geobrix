# Design: PMTiles Multi-Tiling Series + Overture Data Source ("Project Helios")

**Date:** 2026-06-27
**Branch target:** `beta/0.4.0` (new feature branch off it; PR into `beta/0.4.0`)
**Status:** Approved design — ready for implementation planning.

## Summary

A "twofer" deliverable for GeoBrix 0.4.0, decomposed into **three sequenced sub-projects**:

1. **Overture data source** (`gbx.sample.overture`) — an API-level, distributed, AOI-driven
   downloader for Overture Maps GeoParquet (all themes/types), via Overture's STAC catalog,
   into a Unity Catalog Volume — with an optional metadata Delta table that catalogs each
   asset's Volumes path (`source`/`path` column) for queryable, re-runnable, reader-ready output.
2. **VizX viewers** — net-new public `gbx.vizx` functions `plot_pmtiles` (and `plot_cog`) that
   render a PMTiles archive / COG inline in a Databricks notebook, plus a small reusable
   Python PMTiles inspector in `gbx.pmtiles`.
3. **The notebook series** (`notebooks/examples/helios/`) — a `config_nb` spine + three focused
   notebooks demonstrating vector (MVT), raster (XYZ), and elevation (COG + STAC) tiling, all
   written as PMTiles, over a **San Francisco** AOI with a solar site-selection meta-narrative.

The notebooks consume sub-projects 1 and 2 plus already-shipped tiling primitives, so the build
order is **SP1 → SP2 → SP3**.

## Goals

- Show GeoBrix's distributed tiling story end-to-end: ingest → tile (MVT / XYZ / COG) →
  package as PMTiles → inspect/visualize, all on Databricks (Serverless-safe lightweight tier
  by default, heavyweight switchable).
- Give Overture Maps a first-class, data-source-specific API (all themes), since it is the
  most popular open vector source. Other sources (NAIP, USGS 3DEP) stay as notebook helpers.
- Provide an in-notebook PMTiles viewer (and COG viewer) in VizX so the series can *show* the
  output, not just write it.
- Keep the series DRY via a `config_nb`, promoting only genuinely reusable helpers into the
  light API.

## Non-goals

- No Spark-side PMTiles *read* (still unsupported; the Python inspector reads on the driver).
- No new heavyweight Scala expressions are required; the series uses existing functions
  (`gbx_st_asmvt`, `st_asmvt_pyramid`, `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`,
  `gbx_rst_cog_convert`, terrain, `gbx_pmtiles_agg`, `.write.format("pmtiles")`).
- NAIP / USGS 3DEP do **not** get module-level APIs — notebook/`config_nb` helpers only.
- No tile server / external hosting; the viewer renders entirely in-notebook.

---

## Sub-project 1 — Overture data source (`gbx.sample.overture`)

A data-source-specific extension of the existing `sample` package (sibling to `_bundle.py`),
shipped in the WHL. Mirrors `gbx.stac.StacClient`'s shape, distribution strategy, and
test-injection seams.

### Public surface

```python
class OvertureClient:
    def __init__(self, catalog="https://stac.overturemaps.org/catalog.json",
                 release=None, _catalog_opener=None, _get_fn=None): ...

    def discover(self, bbox, themes=None, release=None) -> "DataFrame":
        """One row per intersecting GeoParquet asset for the AOI.
        Columns: theme, type, href, asset_bbox, release.
        themes=None => ALL themes/types."""

    def download(self, assets_df, out_dir, *, table=None, validate=True,
                 max_tries=5, partitions=None) -> "DataFrame":
        """Distributed download of discovered assets to out_dir (a Volume).
        Serverless-safe repartition(N, col); idempotent skip; parquet-readable
        validation.

        Returns a metadata DataFrame: theme, type, source (the Volumes path of
        the downloaded asset; also aliased as `path`), out_file_sz,
        is_out_file_valid, last_update, plus carried discovery columns
        (asset_bbox, release, href).

        table=<delta_table_name> also persists/UPSERTs that metadata to a Delta
        table (idempotent MERGE keyed by (theme, type, source)), so the catalog
        of downloaded assets is queryable and re-runnable."""

    def read(self, source, theme=None, type=None, bbox=None) -> "DataFrame":
        """Load downloaded GeoParquet back into Spark, optional bbox-struct AOI
        filter (the overture.py loader pattern). `source` may be a Volume
        directory OR a metadata Delta table / DataFrame whose `source`/`path`
        column points at the per-asset Volumes paths."""

# convenience one-shot
def download_overture_aoi(bbox, out_dir, themes=None, release=None,
                          table=None) -> "DataFrame": ...
```

### Behavior / design notes

- **Discovery:** Overture's STAC is a **static catalog** (`catalog.json` traversal → collections
  → items), *not* a searchable STAC API. So discovery cannot reuse `StacClient` directly; it
  traverses the catalog and filters items client-side by axis-aligned bbox intersection.
  An `overturemaps` CLI fast-path is used when the CLI is installed; otherwise the
  pystac/static-traversal fallback runs. (Both paths are present in the reference
  `stac_download.py`; this generalizes them to all themes and to distributed download.)
- **Themes/types covered (all):** `addresses`, `base/*` (infrastructure, land, land_cover,
  land_use, water, bathymetry), `buildings/{building,building_part}`,
  `divisions/{division,division_area,division_boundary}`, `places/place`,
  `transportation/{connector,segment}`. `themes=None` selects everything; a list narrows it.
- **Release handling:** `release=None` resolves the latest Overture release from the catalog;
  an explicit string pins it.
- **Distribution (Serverless is the first-class target, not Classic):** the distribution
  strategy is designed for Serverless and must drive aggressive parallelism, modeled on the
  h3-rasterize example (fan the work into many fine-grained, balanced units rather than tuning
  cluster knobs). Concretely:
  - **Performant default — distributed read + AOI rewrite.** STAC resolves the release and the
    set of Overture GeoParquet paths for the bbox; the heavy I/O is then a *distributed Spark
    read* of those parquet files over the cloud path (`s3://overturemaps-us-west-2/...` /
    `abfs://...`) with **`bbox`-struct predicate pushdown** so only AOI rows are read, written
    distributed to the Volume (+ the metadata Delta table). The bytes move on workers, in
    parallel — the AOI subset, not whole continental files.
  - **Fallback — asset-level parallel download.** When only an `https` href is available (no
    direct cloud read), fan whole-file downloads out with `repartition(N, F.col(<asset key>))`.
  - **Avoid the file-count bottleneck.** A bbox may intersect only a few very large parquet
    files; to keep parallelism high (one balanced unit per core, h3-rasterize style), fan finer
    than file granularity — by parquet **row-group / byte-range** — rather than letting a
    handful of files cap task count.
  - **Serverless constraints (hard):** only `repartition(N, column)` for parallelism — never
    number-only `repartition(N)` (AQE-coalesced to serial); **no** `spark.conf` / cache /
    persist / checkpoint / `.rdd` / `sparkContext`. Verify partitions are not coalesced
    (`getNumPartitions`) when iterating the plan locally. `CREATE TEMP TABLE` materialization
    (used to pin a distributed result) is Serverless / DBR 18.1+ only.
  - **No driver bottleneck:** catalog traversal/discovery is driver-side but lightweight
    (metadata only); all asset I/O is distributed.
- **Validation:** `validate=True` means the downloaded parquet opens (pyarrow/geopandas),
  not rasterio-decodable. Idempotent skip when the target exists and is valid.
- **Output targets (both supported):** (1) asset **files on a UC Volume** under `out_dir`;
  (2) an optional **metadata Delta table** (`table=...`) with one row per asset and a `source`
  column (aliased `path`) holding that asset's Volumes path. The table is UPSERTed via Delta
  `MERGE` keyed by `(theme, type, source)` so re-runs are idempotent and a `repair()`-style
  re-download of invalid rows works (the StacClient/eo-series pattern). The `source`/`path`
  naming matches what downstream GeoBrix readers and `read()` consume, so the metadata table
  can directly drive distributed reads.
- **Testability:** `_catalog_opener` and `_get_fn` injection seams (exactly like `StacClient`)
  so unit tests run offline on the driver with a fake catalog and fake fetcher — no network.

### Files

- `python/geobrix/src/databricks/labs/gbx/sample/overture.py` (public `OvertureClient` +
  `download_overture_aoi`).
- `python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py` (catalog traversal /
  CLI fast-path / bbox intersect — kept separate so it is unit-testable in isolation).
- Re-export from `sample/__init__.py`.
- Tests: `python/geobrix/test/sample/test_overture.py` (offline, injected opener + fetcher).

### Dependencies / CI lock

- New runtime dep: `pystac` (catalog traversal). Parquet read via existing `geopandas`/`pyarrow`.
  `overturemaps` CLI is optional (fast-path only).
- Follow the light-CI-lock checklist: add deps to `requirements-pyrx-ci.in` **and**
  `requirements-dev-container.in`, then recompile the hashed `.txt` files; register the new
  `test/sample/` directory in **both** the light-test dir list and the `pyrx_build` dir list.

---

## Sub-project 2 — VizX viewers (`gbx.vizx`)

Net-new public functions exported from `vizx/__init__.py`, behind the existing `[vizx]` extra.

### Public surface

```python
def plot_pmtiles(path_or_bytes, *, max_embed_mb=64, fallback=True,
                 style=None, **map_kwargs):
    """Render a .pmtiles archive inline. Interactive MapLibre GL JS + pmtiles.js
    (archive base64-embedded as an in-browser FileSource) via displayHTML.
    Auto-detects vector (MVT -> vector layer) vs raster (PNG/JPG/WebP -> raster
    layer) from the archive header. Above max_embed_mb (or fallback path),
    renders a Python-side static image instead."""

def plot_cog(path, *, band=None, **kw):
    """Render a Cloud-Optimized GeoTIFF: rasterio overview read -> plot_raster;
    optionally also added as a raster source in the interactive map."""
```

### Design notes

- **Interactive path:** build a MapLibre GL JS HTML page (libraries from CDN: maplibre-gl +
  pmtiles), register the `pmtiles://` protocol, and feed the archive bytes as a base64
  `FileSource` (`new pmtiles.PMTiles(new pmtiles.FileSource(...))`) so it streams entirely
  in-browser — no HTTP server, no range requests against remote storage. Render through the
  existing `_notebook_display_html()` channel (IPython `user_ns['displayHTML']`) from
  `_interactive.py`; reuse its fallback chain.
- **Size guard + static fallback:** archives larger than `max_embed_mb` (base64 bloats ~33%)
  would hang the notebook; instead decode tiles with the Python `pmtiles` reader and composite
  — raster PMTiles → reuse `plot_raster`; vector PMTiles → decode MVT to geometries → reuse
  `plot_static`. Mirrors `plot_interactive`'s scale-safe philosophy.
- **Static rendering uses `contextily` basemaps (continued).** The vector static fallback and
  `plot_cog` lay their layers over a `contextily` basemap, consistent with the existing
  `plot_static` (`basemap=True`, `basemap_source=...`). `contextily` is already a `[vizx]`
  dependency — no new dep — so the static path stays visually consistent with the rest of VizX.
- **Vector vs raster detection:** read the PMTiles header `tile_type` (PNG/JPEG/WebP/MVT) via
  the inspector (below).

### Light-API promotion: PMTiles inspector

`gbx.pmtiles.pmtiles_info(path) -> dict` (header: tile_type, min/max zoom, bounds, tile count,
tilejson-ish metadata). Spark-side read is unsupported, so a driver-side inspector is broadly
useful and is needed by both the viewer and the static fallback. Implemented with the existing
`pmtiles` PyPI dependency.

### Files

- `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py` (`plot_pmtiles`).
- `python/geobrix/src/databricks/labs/gbx/vizx/_cog.py` (`plot_cog`).
- `python/geobrix/src/databricks/labs/gbx/pmtiles/_inspect.py` (`pmtiles_info`); re-export from
  `pmtiles/__init__.py`.
- Export `plot_pmtiles`, `plot_cog` from `vizx/__init__.py` (+ `__all__`).
- Tests: `test/vizx/test_pmtiles.py`, `test/vizx/test_cog.py` (assert HTML structure for the
  interactive path, image output for the fallback, header parsing for the inspector).

### Dependencies / CI lock

- No new Python deps for the interactive path (CDN JS); `pmtiles` + `rasterio` already present.
- `plot_cog` uses `rasterio` (present) over a `contextily` basemap (present); `rio-tiler`
  optional for nicer overview selection.
- CI-lock: `vizx` test dir already registered; only add new deps if `plot_cog` adopts
  `rio-tiler`.

---

## Sub-project 3 — The notebook series (`notebooks/examples/helios/`)

Mirrors the eo-series layout (`config_nb.ipynb` + numbered notebooks + `README.md`) plus a
docs page and sidebar entry.

### Layout

```
notebooks/examples/helios/
  config_nb.ipynb            # shared spine, %run by each notebook
  01. Vector Engine (MVT).ipynb
  02. Visual Basemap (XYZ).ipynb
  03. Analytical Core (COG + STAC).ipynb
  README.md
docs/docs/notebooks/helios.mdx   # docs page; add to docs/sidebars.js
```

### `config_nb.ipynb` (the spine)

`%pip` install (light tier by default), imports, `OvertureClient` + `StacClient` setup, the
**San Francisco AOI bbox constant**, `ETL_DIR` Volume config, tier switch (light/heavy),
rebuild flags, and **series-only helpers**: solar-slope/aspect scoring, demo plot wrappers,
Delta table finalizers. Anything that proves generally reusable is promoted to the light API
(SP1/SP2) instead of living here.

### Notebook arcs (one SF AOI, solar site-selection narrative)

- **NB01 — Vector Engine (MVT):** Overture **buildings** for SF (`OvertureClient`) →
  `gbx_st_asmvt` / `st_asmvt_pyramid` → `gbx_pmtiles_agg` → vector PMTiles in a Volume →
  `plot_pmtiles`. Narrative: roof footprints as candidate solar surfaces.
- **NB02 — Visual Basemap (XYZ raster):** NAIP imagery (notebook helper download) →
  `gbx_rst_to_webmercator` → `gbx_rst_xyzpyramid` → raster PMTiles → `plot_pmtiles`.
  Narrative: aerial site context.
- **NB03 — Analytical Core (COG + STAC + hillshade):** USGS 3DEP DEM (notebook helper) →
  `gbx_rst_cog_convert` → COGs in a Volume → STAC-catalog the COGs into a Delta table →
  hillshade/slope → hillshade PMTiles → `plot_cog` + `plot_pmtiles`. Narrative: roof
  slope/aspect for solar yield.

Each notebook includes meta-narrative markdown, a catchy data→tile→PMTiles flow diagram, and
ample plotting sections.

---

## Cross-cutting concerns

### Testing (TDD)

- SP1 and SP2 are built test-first with offline injection seams (no network in unit tests).
- Doc/example code executes real assertions on real sample data per repo convention; notebooks
  are validated in Docker (`gbx:test:notebooks`). Doc tests are the documentation source.
- The viewer's interactive path is asserted by HTML structure (script tags, embedded source,
  protocol registration); the fallback path by produced image; the inspector by parsed header.

### Sequencing & plans

- Build order: **SP1 → SP2 → SP3**. Each sub-project gets its own implementation plan from the
  `writing-plans` skill (three plans under `docs/superpowers/plans/`).

### Bench

- The Overture path is a downloader (light-only); add a light bench only if a reader/writer
  surfaces that warrants the "bench each reader/writer" convention. No heavy comparison expected.

### Docs voice

- All user-facing docs (README, `helios.mdx`) avoid internal planning vocabulary (no wave
  numbers / dispatch references); QC `internals-leak` check enforces this.

## Open items deferred to planning

- Exact SF AOI bbox extent (small enough for demo-friendly data volumes; reuses the
  h3-rasterize SF area where possible).
- Whether `plot_cog` also injects the COG as a raster layer in the interactive map, or stays
  static-only (decide during SP2 implementation).
- CDN pin vs vendored copy for maplibre-gl / pmtiles JS (pin a specific version for
  reproducibility).
- Confirm Serverless can read Overture's public cloud paths directly (`s3://overturemaps-us-west-2`
  / `abfs://...overturemapswestus2...`) — including any requester-pays / credential config — since
  the performant distributed-read default depends on it; otherwise the asset-level HTTP-href
  download fallback becomes the primary path. Validate during SP1.
