# EO-Series → StacClient Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Refactor the EO-series example notebooks to perform STAC search/download/repair via the new `databricks.labs.gbx.stac.StacClient`, removing the redundant per-notebook STAC + resilient-download helpers, while preserving the `band_<band>` table contract that nb03/nb04 consume.

**Architecture:** nb01 search → `client.search()`; nb02 download+repair → `client.download()` + `client.repair()`, with each per-band table rebuilt as a notebook-level join (download results ⋈ per-item metadata). nb03/nb04 are NOT modified — they read the same downstream columns. config_nb installs `geobrix[light,stac]`, instantiates the client, and keeps only the non-STAC utilities.

**Tech Stack:** Databricks Serverless (environment version 5, Python 3.12), PySpark, `databricks.labs.gbx.stac`, `databricks.labs.gbx.pyrx` (rx), Delta.

## Global Constraints

- **Serverless-safe:** NO `spark.conf.set` (use `set_conf_safe`), NO `.cache()`/`.persist()`, parallelism via `DataFrame.repartition(N)` only. (eo-series already follows this.)
- **`band_<band>` table contract (nb02 → nb03):** MUST contain at least `item_id`, `band_name`, `date`, `out_file_path`. MUST also carry `is_out_file_valid` + `out_file_sz` (nb02's own repair reads them). All other legacy columns (`timestamp`, `h3_set`, `item_collection`, `stac_version`, `item_bbox`, `item_properties`, `asset`, `out_dir_fuse`, `out_filename`, `last_update`) may be DROPPED — `finalize_tiled_band_tbl` discards them and nb03/nb04 never read them.
- **`band_*_h3` contract (nb03 → nb04):** unchanged (`cellid`, `date`, `band_name`, `tile`); nb03/nb04 untouched.
- **`StacClient` API (verbatim):**
  - `StacClient(catalog=PLANETARY_COMPUTER, sign="planetary_computer")`
  - `search(df, geojson_col, collections: List[str], datetime: str, partitions=512) -> DataFrame` → carried-input-cols + `item_id, date, item_bbox, asset_name, href, item_properties`. One row per (input-row, item, asset).
  - `download(df, out_dir, asset_names=None, name="{asset_name}_{item_id}.tif", validate=True, max_tries=5, partitions=None) -> DataFrame` → `item_id, asset_name, out_file_path, out_file_sz, is_out_file_valid, last_update`. Input df must have `item_id` + `asset_name`; href is RE-SIGNED per attempt from item_id+asset_name (a stale search href is not required). Dedups to unique `(item_id, asset_name)` internally.
  - `repair(table_or_df, where="is_out_file_valid = false") -> DataFrame` → re-downloads invalid rows, Delta MERGE back, returns repaired subset.
- **No restartPython in config_nb** (it is `%run`-ed; restart wipes the caller's context). The `%pip install` lives in config_nb's own cells which run before the `%run`-based imports — keep the existing placement.
- **Install line:** config_nb installs `geobrix[light,stac]` from the staged wheel (`file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl`).
- **Notebook edits:** edit `.ipynb` via the NotebookEdit tool (these are Jupyter JSON). Keep existing `displayHTML` screenshot cells intact (they are the committed visual output).
- **Collection/datetime:** the series uses collection `"sentinel-2-l2a"` and a datetime range string like `"2022-06-01/2022-06-01"`. The geojson column is named `"geojson"`.

---

### Task 1: config_nb — install `[light,stac]`, instantiate StacClient, strip redundant helpers

**Files:**
- Modify: `notebooks/examples/eo-series/config_nb.ipynb`
- Modify: `notebooks/examples/eo-series/library.py`

**Interfaces:**
- Produces: a module/global `stac_client = StacClient(...)` available to nb01/nb02 after `%run ./config_nb`; retains `set_conf_safe`, `file_size`, `timestamp_filename`, `get_now_formatted`, `finalize_tiled_band_tbl`, `gen_tessellate_tiled_band`, `FORCE_REBUILD`, and all viz helpers (`plot_raster`, `plot_file`, `to_numpy_arr`, `rasterio_lambda`, `_decimated_read`, `_percentile_stretch`).

- [ ] **Step 1: Update the `%pip install` cell** so the GeoBrix line installs the stac extra. Change the `geobrix[light]` line to:
  ```
  %pip install --quiet "geobrix[light,stac] @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"
  ```
  Remove `pystac pystac_client planetary_computer tenacity` from the SECOND `%pip` line (they now come via `[stac]`); KEEP `folium mapclassify geopandas rich` (viz deps not in `[stac]`).

- [ ] **Step 2: Add a StacClient instantiation cell** (after the imports cell that does `from databricks.labs.gbx.stac import StacClient`). Add the import to the existing import block and a cell:
  ```python
  from databricks.labs.gbx.stac import StacClient
  stac_client = StacClient()   # default catalog = Planetary Computer, sign = planetary_computer
  ```

- [ ] **Step 3: Remove the redundant STAC + download helpers from `library.py`:** delete `ps_client`, `get_items`, `get_assets`, `get_assets_for_cells`, `download_asset`, `download_asset_v2`. KEEP the viz helpers (`plot_raster`, `plot_file`, `to_numpy_arr`, `rasterio_lambda`, `_decimated_read`, `_percentile_stretch`, `_needs_percentile_stretch`, `_render`) and any non-STAC imports they need. Remove now-unused imports (`pystac_client`, `planetary_computer`, `tenacity`) from library.py if nothing else uses them.

- [ ] **Step 4: Remove the orchestration helpers from config_nb** that StacClient replaces: `download_band`, `update_assets`, `download_missing_assets`. KEEP `set_conf_safe`, `file_size`, `timestamp_filename`, `get_now_formatted`, `finalize_tiled_band_tbl`, `gen_tessellate_tiled_band`. (These last two are GeoBrix tiling, not STAC.)

- [ ] **Step 5: Verify config_nb has no remaining references** to the removed names. Grep the eo-series dir:
  Run: `grep -rn "get_assets_for_cells\|download_asset_v2\|download_asset(\|download_band\|download_missing_assets\|update_assets\|ps_client\|library.get_items\|library.get_assets" notebooks/examples/eo-series/`
  Expected: only references inside nb01/nb02 that Tasks 2–3 will fix (config_nb + library.py clean).

- [ ] **Step 6: Commit.**
  ```bash
  git add notebooks/examples/eo-series/config_nb.ipynb notebooks/examples/eo-series/library.py
  git commit -m "refactor(eo-series): config_nb installs [light,stac] + StacClient; drop redundant STAC/download helpers"
  ```

---

### Task 2: nb01 — search via `client.search()`

**Files:**
- Modify: `notebooks/examples/eo-series/01. Search STACs.ipynb`

**Interfaces:**
- Consumes: `stac_client` (Task 1), `df_cell_json` (H3 cell rows with a `"geojson"` column), `time_range` (e.g. `"2022-06-01/2022-06-01"`).
- Produces: the `cell_assets_<...>` Delta table with `client.search()` columns: carried `cellid` (+ any other carried input cols), `item_id`, `date`, `item_bbox`, `asset_name`, `href`, `item_properties`. One row per (cell, item, asset).

- [ ] **Step 1: Replace the main search call.** Find the cell calling `library.get_assets_for_cells(df_cell_json.repartition(512), time_range, "sentinel-2-l2a", spark)` and replace with:
  ```python
  cell_assets_df = stac_client.search(
      df_cell_json,
      geojson_col="geojson",
      collections=["sentinel-2-l2a"],
      datetime=time_range,
      partitions=512,
  )
  ```
  Keep the surrounding `if LAST_UPDATED is None`/`FORCE_REBUILD` guard and the table write. If a `last_update` provenance column is desired, add `.withColumn("last_update", F.current_timestamp())` (lazy, Serverless-safe) before the write.

- [ ] **Step 2: Fix the demo-only search cell** (the manual `library.ps_client.search(...)` viz cell). Replace `library.ps_client` with a local client opened for the demo, OR drop the demo cell if redundant. Minimal replacement:
  ```python
  import pystac_client, planetary_computer
  _demo_cat = pystac_client.Client.open(
      "https://planetarycomputer.microsoft.com/api/stac/v1",
      modifier=planetary_computer.sign_inplace,
  )
  _demo_items = _demo_cat.search(collections=["sentinel-2-l2a"], intersects=region, datetime=time_range).item_collection()
  ```

- [ ] **Step 3: Adapt any downstream column references in nb01** that used the old schema. The old output had `asset` (map with `.name`/`.href`); the new output has flat `asset_name`/`href`. Update any `asset.name`→`asset_name`, `asset.href`→`href`. The old `timestamp`/`item_collection`/`stac_version` columns no longer exist — remove references (nb02 no longer needs them per the contract).

- [ ] **Step 4: Update the nb01 markdown** that describes the search step to reference `stac_client.search(...)` and the one-row-per-(cell,item,asset) output (keep the Serverless-strategy notes). Do not leak internal vocabulary.

- [ ] **Step 5: Commit.**
  ```bash
  git add "notebooks/examples/eo-series/01. Search STACs.ipynb"
  git commit -m "refactor(eo-series): nb01 search via StacClient.search"
  ```

---

### Task 3: nb02 — download + repair via `client.download()` / `client.repair()`, rebuild band tables

**Files:**
- Modify: `notebooks/examples/eo-series/02. Download STACs.ipynb`

**Interfaces:**
- Consumes: `stac_client`, the `cell_assets_<...>` table from nb01 (`eod_item_df`), `FORCE_REBUILD`, `EO_DIR`.
- Produces: per-band `band_<band>` Delta tables with columns `item_id, band_name, date, out_file_path, out_file_sz, is_out_file_valid` (+ harmless `last_update`). Satisfies the nb03 contract (`item_id, band_name, date, out_file_path`).

- [ ] **Step 1: Replace the per-band download.** For each band, replace the `download_band(...)` call with a notebook block that (a) filters search rows to the band, (b) downloads, (c) joins back per-item `date`, (d) writes the band table. Reference implementation (factor into a small local helper `build_band_table(band)` defined in a nb02 cell — it is example-local orchestration, intentionally NOT in config_nb):
  ```python
  from pyspark.sql import functions as F

  def build_band_table(band: str, eod_item_df, force_rebuild: bool):
      band_tbl = f"band_{band.lower()}"
      if not force_rebuild and spark.catalog.tableExists(band_tbl):
          return spark.read.table(band_tbl)
      # one (item, asset) per band; download dedups to unique (item_id, asset_name)
      band_rows = eod_item_df.filter(F.col("asset_name") == band)
      # per-item metadata to rejoin (date) — distinct so it is one row per item
      item_meta = band_rows.select("item_id", "date").distinct()
      out_dir = f"{EO_DIR}/{band}"
      files = stac_client.download(
          band_rows.select("item_id", "asset_name"),
          out_dir,
          asset_names=[band],
          name="{asset_name}_{item_id}.tif",
          validate=True,
          max_tries=5,
      )
      band_df = (
          files.join(item_meta, on="item_id", how="left")
               .withColumn("band_name", F.lit(band))
               .select("item_id", "band_name", "date",
                       "out_file_path", "out_file_sz", "is_out_file_valid", "last_update")
      )
      band_df.write.mode("overwrite").saveAsTable(band_tbl)
      return spark.read.table(band_tbl)
  ```

- [ ] **Step 2: Replace the band loop / single-band example.** Where nb02 currently loops `download_band(...)` over the band list, call `build_band_table(band, eod_item_df, FORCE_REBUILD)`. Preserve the example's single-band (`B02`) demonstration cell, now calling `build_band_table("B02", ...)`.

- [ ] **Step 3: Replace `download_missing_assets(...)` with `client.repair(...)`.** Where nb02 retries invalid rows:
  ```python
  repaired = stac_client.repair(f"band_{band.lower()}", where="is_out_file_valid = false")
  ```
  Keep the dry-run/demo framing as markdown if useful (note `repair` itself has no dry-run; describe it, then run the live call). Remove the `do_dry_run` plumbing.

- [ ] **Step 4: Update nb02 markdown** to describe `client.download()` (resilient: re-sign per attempt, read-validate, retry/backoff, local-stage→publish; dedups (item_id, asset_name)) and `client.repair()` (Delta MERGE of invalid rows). Keep the Serverless-strategy notes (repartition/Arrow-cap). No internal vocabulary.

- [ ] **Step 5: Verify no removed-helper references remain.** Run:
  `grep -rn "download_band\|download_missing_assets\|update_assets\|download_asset" "notebooks/examples/eo-series/02. Download STACs.ipynb"`
  Expected: nothing.

- [ ] **Step 6: Commit.**
  ```bash
  git add "notebooks/examples/eo-series/02. Download STACs.ipynb"
  git commit -m "refactor(eo-series): nb02 download+repair via StacClient; rebuild band tables as join"
  ```

---

### Task 4: Docs — new STAC API page + eo-series page + README parity

**Files:**
- Create: `docs/docs/api/stac.mdx`
- Modify: `docs/sidebars.js`
- Modify: `docs/docs/notebooks/eo-series.mdx`
- Modify: `notebooks/examples/eo-series/README.md`

**Interfaces:**
- Produces: a user-facing STAC API page registered in the API sidebar category.

- [ ] **Step 1: Create `docs/docs/api/stac.mdx`** — a dedicated STAC API page. Frontmatter: `--- sidebar_position: <next> title: STAC ---` (match the existing api pages' frontmatter style; set a real `title` so the browser tab isn't the logo JSX, per the existing api-page convention). Cover, in user-facing voice (NO internal/wave vocabulary):
  - What it is: a lightweight, Serverless-safe STAC client (`databricks.labs.gbx.stac.StacClient`) for **distributed** search + **resilient** download + **repair** against any STAC catalog (default Planetary Computer). Frame the distributed-parallelism advantage over single-node STAC scripts (factual, not marketing) — consistent with the other light-tier overview pages.
  - Install: `pip install geobrix[light,stac]` (opt-in extra: pystac-client, planetary-computer, tenacity, requests). Note Serverless environment version 5 (Python 3.12).
  - API reference for the three methods with their exact signatures + the output columns (copy from the Global Constraints `StacClient API` block): `search(df, geojson_col, collections, datetime, partitions=512)`, `download(df, out_dir, asset_names=None, name=..., validate=True, max_tries=5, partitions=None)`, `repair(table_or_df, where=...)`.
  - A short end-to-end example (search AOI rows → download assets → repair invalid), illustrative code blocks.
  - Resilience behavior: re-sign per attempt, read-validation (rejects throttled/truncated), retry/backoff, local-stage→publish-only-when-valid, idempotent skip of already-valid files; dedup to unique (item_id, asset_name).
  - Serverless notes: parallelism via `partitions=`/repartition (no spark.conf), no caching (materialize to a Delta table / Volume), one task per asset.
  - A link to the EO-series notebooks as the full worked example.
  - (Doc-test backing is NOT required for this page — StacClient is an integration/network client; the doc-tests-as-source convention targets the Docker+sample-data SQL/raster examples. Use clear illustrative code blocks and point to the eo-series notebooks for executed end-to-end usage.)

- [ ] **Step 2: Register the page in `docs/sidebars.js`** in the API category (the `items:` array around lines 76–99), e.g. add `{ type: 'doc', id: 'api/stac', label: 'STAC' }` after the PMTiles entry (`api/pmtiles-functions`) so it sits with the other light-tier API pages.

- [ ] **Step 3:** Update the eo-series docs page (`docs/docs/notebooks/eo-series.mdx`) + the eo-series `README.md` to describe the STAC step as `StacClient` (search/download/repair) rather than the old per-notebook helpers; note the `geobrix[light,stac]` install; link to the new `api/stac` page. Keep voice user-facing (no wave numbers / internal vocabulary). Preserve the existing lightweight-tier / Serverless framing.

- [ ] **Step 4: Quick internal-vocab check.** Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/stac.mdx docs/docs/notebooks/eo-series.mdx` → must print nothing.

- [ ] **Step 5: Commit.**
  ```bash
  git add docs/docs/api/stac.mdx docs/sidebars.js docs/docs/notebooks/eo-series.mdx notebooks/examples/eo-series/README.md
  git commit -m "docs(stac): add STAC API page + eo-series page/README describe StacClient + [light,stac]"
  ```

---

### Task 5: Serverless re-validation (nb01 → nb04 chain)

**Files:** none (validation only).

This is the acceptance gate. The refactor touches nb01/nb02 schemas; nb03/nb04 must still run green against the rebuilt band tables. Re-stage the wheel (built from current source incl. the stac module) and run the chain on Serverless env v5 with `FORCE_REBUILD=True`.

- [ ] **Step 1:** Build + stage the `[light,stac]` wheel to `dbfs:/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl` (see `/tmp/stac-smoke-task.md` for the exact build+stage commands: `GBX_BUNDLE_SKIP_JAR_UPLOAD=1` build via `.venv-pyrx`, then `databricks fs cp --overwrite` to the sample-data volume; profile `oauth-fe`, `DATABRICKS_AUTH_STORAGE=plaintext`). NOTE: nb03/nb04 use heavy `rx.rst_*`? No — they use the light `rx` (pyrx) tier; the wheel's `[light]` covers them, but if any cell needs the JAR, build WITH the JAR instead.
- [ ] **Step 2:** Run nb01 on Serverless env v5 with `FORCE_REBUILD=True`; confirm SUCCESS and the `cell_assets_<...>` table is written with the new flat schema (`item_id, asset_name, href, date, item_bbox, item_properties, cellid`).
- [ ] **Step 3:** Run nb02 with `FORCE_REBUILD=True`; confirm each `band_<band>` table has `item_id, band_name, date, out_file_path, out_file_sz, is_out_file_valid` and `is_out_file_valid` is mostly true (PC throttling may flag a few — run the `client.repair` cell and confirm it recovers them).
- [ ] **Step 4:** Run nb03 then nb04 with `FORCE_REBUILD=True`; confirm SUCCESS and the final outputs match the previously-validated run (band_*_h3 row counts in the same ballpark, band_stack produced, stacked tifs written). Investigate any schema/contract break.
- [ ] **Step 5:** Report the run URLs + row counts. No commit (validation only); the executed-notebook EXPORT is a separate follow-on phase (not in this plan).

---

## Self-Review notes

- Spec coverage: config_nb install/client + helper removal (T1), nb01 search (T2), nb02 download+repair+band rebuild (T3), docs — new STAC API page + eo-series page + README (T4), validation (T5). ✔
- Contract: band_<band> keeps `item_id/band_name/date/out_file_path` (nb03) + `is_out_file_valid/out_file_sz` (repair); nb03/nb04 untouched. ✔
- Type consistency: `client.search` output `asset_name`/`href` flat (not `asset` map) — T2/T3 adapt refs. `client.download` needs `item_id`+`asset_name` input — T3 passes exactly those. ✔
- Serverless: no conf/cache; repartition handled inside StacClient; band write via saveAsTable (no persist). ✔
