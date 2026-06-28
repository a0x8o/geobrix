# Helios Notebook Series (SP3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task is one independently-reviewable deliverable (a notebook, README, docs page, or sidebar entry). Notebooks are NOT TDD unit code, so each task ends in a concrete **VALIDATION** step (Docker cell-by-cell execution + asserted artifact) instead of a pytest assert — same rigor, different evidence.

**Goal:** Build the `notebooks/examples/helios/` series — a `config_nb.ipynb` spine + three numbered notebooks (NB01 Vector Engine / MVT, NB02 Visual Basemap / XYZ, NB03 Analytical Core / COG+STAC) + `README.md`, plus a `docs/docs/notebooks/helios.mdx` page and `docs/sidebars.js` entry. The series tells one **solar site-selection** meta-narrative over a single **San Francisco** AOI, end-to-end: ingest → tile (MVT / XYZ / COG) → package as PMTiles → inspect/visualize, Serverless lightweight tier by default, heavyweight switchable.

**Architecture:** Mirror the eo-series spine exactly. `config_nb.ipynb` holds all shared state (%pip light tier, imports, `OvertureClient` + `StacClient` setup, the SF AOI bbox constant, `ETL_DIR` Volume config, the `set_conf_safe()` Serverless guard, the tier switch, `FORCE_REBUILD`, and **series-only** helpers: solar slope/aspect scoring, demo plot wrappers, Delta table finalizers). Each numbered notebook begins with `%run ./config_nb` and produces named Volume paths / Delta tables / PMTiles archives that the next notebook (and the docs page) reference by exact name. Anything generally reusable was already promoted to SP1 (`gbx.sample.overture`) / SP2 (`gbx.vizx.plot_pmtiles`/`plot_cog`, `gbx.pmtiles.pmtiles_info`) — this plan **consumes** those signatures and re-implements nothing.

**Tech Stack:** Databricks notebooks (`.ipynb`), Python 3.12 / Spark 4 / Serverless (env v5+); GeoBrix `[light,stac,vizx]` wheel; `databricks.labs.gbx.pyrx`/`pyvx` (lightweight) with a commented `rasterx`/`vectorx` heavyweight option; existing registered SQL `gbx_st_asmvt`, `gbx_st_asmvt_pyramid`, `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`, `gbx_rst_cog_convert`, `gbx_pmtiles_agg`; Docusaurus MDX docs; validated in Docker via `gbx:test:notebooks`.

**Spec:** `docs/superpowers/specs/2026-06-27-helios-tiling-series-overture-design.md` (Sub-project 3 + cross-cutting).
**Branch:** the SP3 feature branch off `beta/0.4.0` (created for the Helios work; SP1/SP2 land first). PR into `beta/0.4.0`.

---

## Global Constraints

These apply to every task; do not restate them per step, but honor them everywhere.

- **Serverless lightweight tier is the DEFAULT, heavyweight switchable.** `config_nb` selects `pyrx`/`pyvx` (option-1, default); a commented option-2 imports `rasterx`/`vectorx` for a classic x86 cluster + JAR + GDAL init script. Heavy/light call the **same SQL names** (`gbx_*`), so the notebook body is tier-agnostic after the import switch.
- **Serverless hard rules** (from the spec + repo memory): parallelism only via `DataFrame.repartition(N, "col")` — never number-only `repartition(N)` (AQE-coalesced to serial); **no** `spark.conf.set` outside the `set_conf_safe()` guard, **no** `.cache()`/`.persist()`/`.checkpoint()`/`.rdd`/`sparkContext`. Where you'd cache, write a managed Delta table and read it back. `CREATE TEMP TABLE` is Serverless / DBR 18.1+ only.
- **One San Francisco AOI** for all three notebooks: the N37W123 quad reused from h3-rasterize. `SF_AOI_BBOX = (-123.0, 37.0, -122.0, 38.0)` (minx, miny, maxx, maxy, EPSG:4326). A tighter demo sub-bbox `SF_CITY_BBOX = (-122.52, 37.70, -122.35, 37.83)` (the SF peninsula proper) keeps Overture buildings + DEM volumes demo-friendly; both live in `config_nb`.
- **User-facing docs voice** — README.md and helios.mdx are read by end users: NO internal planning vocabulary (no wave numbers, no dispatch/subagent references, no "SP1/SP2/SP3"). The QC `internals-leak` check enforces this. Frame GeoBrix as an on-ramp to Databricks-native spatial where it fits; factual, not marketing.
- **Notebooks validated in Docker** via `gbx:test:notebooks --path examples/helios/<file>.ipynb` (cell-by-cell, no kernel; the container must be started with `start_docker_with_volumes.sh` so `/Volumes` is mounted). Doc tests are the documentation source convention applies to `docs/tests/` code, not these example notebooks — but every notebook must execute green cell-by-cell in Docker against real sample data, with real asserted artifacts (a PMTiles archive exists on the Volume and `pmtiles_info` parses it; a plot returns a figure).
- **Commit hygiene** — subject ≤72 chars, a WHY body for non-trivial commits, end with the `Co-authored-by: Isaac` trailer. One commit per task. Hold pushes (commit locally; push on the user's go).
- **No placeholders / TODOs** in committed notebooks. Every cell has real narrative or real runnable code.
- **Shipped default `INTERACTIVE_PLOTS = False`** (GitHub-renderable static images); `True` for live folium/MapLibre. All notebook plot calls route through the toggle-aware `config_nb` helpers (`show_pmtiles`, `show_cog`, and any demo plot wrappers), so the committed `.ipynb` renders fast static images on GitHub by default and flips to the interactive experience with one variable.
- **Compose with Databricks-native spatial where natural.** GeoBrix tiling is an on-ramp to Databricks-native `ST_*` / H3; NB01 and NB03 weave native functions into the solar narrative where it reads naturally (roof geometry / H3 roof-density and slope-per-cell aggregation), NB02 deliberately does not (a pure raster basemap step). Factual framing, no marketing, no internal vocabulary.

---

## Task 0: Series scaffold + diagram generator

**Files:**
- Create dir: `notebooks/examples/helios/`
- Create: `resources/images/helios.py` (the per-notebook diagram generator, mirroring `resources/images/eo-series.py`)
- Produces (committed): `resources/images/helios-01.svg`/`.png`, `helios-02.svg`/`.png`, `helios-03.svg`/`.png`

**Interfaces:** Produces the three `helios-0N.png` images that NB01–NB03 and `helios.mdx` reference by exact path. No code dependency on SP1/SP2.

- [ ] **Step 1: Copy and adapt the diagram generator.** Copy `resources/images/eo-series.py` to `resources/images/helios.py`. Keep its palette, glyph, chip, and four-stage layout machinery verbatim. Replace the four notebook diagram definitions with **three** Helios diagrams. Each is a catchy **data → tile → PMTiles → view** flow with a hero glyph per stage and a footer of the GeoBrix/Databricks function chips that notebook actually uses:
  - `helios-01` (Vector Engine / MVT): stages `Overture buildings (SF)` → `gbx_st_asmvt + st_asmvt_pyramid` → `gbx_pmtiles_agg → vector .pmtiles` → `plot_pmtiles`. Glyphs: building footprints → vector-tile grid → stacked-archive → map pin. Chips: `OvertureClient.discover/download/read`, `gbx_st_asmvt`, `gbx_st_asmvt_pyramid`, `gbx_pmtiles_agg`, `plot_pmtiles`.
  - `helios-02` (Visual Basemap / XYZ): stages `NAIP aerial (SF)` → `gbx_rst_to_webmercator` → `gbx_rst_xyzpyramid → gbx_pmtiles_agg → raster .pmtiles` → `plot_pmtiles`. Glyphs: aerial swatch → web-mercator globe → XYZ pyramid → map pin. Chips: `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`, `gbx_pmtiles_agg`, `pmtiles_info`, `plot_pmtiles`.
  - `helios-03` (Analytical Core / COG+STAC): stages `USGS 3DEP DEM (SF)` → `gbx_rst_cog_convert → COGs + STAC Delta` → `slope/hillshade → gbx_rst_xyzpyramid → .pmtiles` → `plot_cog + plot_pmtiles`. Glyphs: contour DEM → catalog/COG → hillshade relief → map pin. Chips: `gbx_rst_cog_convert`, `StacClient`/STAC Delta, `rst_terrainslope`/`rst_hillshade`, `gbx_rst_xyzpyramid`, `plot_cog`, `plot_pmtiles`.
- [ ] **Step 2: Update the module docstring** re-render block to loop `01 02 03` (not `01 02 03 04`) and to point at `helios-$n.svg`/`.png`. Keep the Chrome-headless screenshot + PIL bbox-trim recipe identical.
- [ ] **Step 3 (VALIDATION):** Run `python3 resources/images/helios.py` then the Chrome-headless + PIL crop recipe from the docstring on the host (Chrome is host-only, not in Docker). Assert the three `.svg` and three trimmed `.png` files exist and open (`python3 -c "from PIL import Image; [Image.open(f'resources/images/helios-{n}.png').load() for n in ('01','02','03')]"` exits 0). Expected: three non-empty PNGs, each a four-stage horizontal flow with a chip footer.
- [ ] **Step 4 (commit):** `git add notebooks/examples/helios resources/images/helios.py resources/images/helios-0*.svg resources/images/helios-0*.png && git commit` — subject `feat(helios): add notebook-series diagram generator + images`; body: WHY (per-notebook data→tile→PMTiles flow diagrams for the SF solar series, mirrors eo-series.py). Trailer `Co-authored-by: Isaac`.

---

## Task 1: `config_nb.ipynb` — the shared spine

**Files:**
- Create: `notebooks/examples/helios/config_nb.ipynb`

**Interfaces:**
- *Consumes:* `from databricks.labs.gbx.sample.overture import OvertureClient` (`OvertureClient()`); `from databricks.labs.gbx.stac import StacClient`; `from databricks.labs.gbx.vizx import plot_pmtiles, plot_cog` + existing `plot_raster, plot_file, plot_static, cells_as_gdf`; `from databricks.labs.gbx.pmtiles import pmtiles_info`; `from databricks.labs.gbx.pyrx import functions as rx`, `from databricks.labs.gbx.pyvx import functions as vx`, `from databricks.labs.gbx.ds.register import register`.
- *Produces (notebook globals every later NB relies on, by exact name):* `overture = OvertureClient()`, `stac_client = StacClient()`, `rx`, `vx`, `register`, `plot_pmtiles`, `plot_cog`, `pmtiles_info`, `plot_static`, `plot_interactive`, `set_conf_safe`, `FORCE_REBUILD`, `INTERACTIVE_PLOTS`, `catalog_name="geospatial_docs"`, `schema_name="helios"`, `ETL_DIR=/Volumes/<cat>/<schema>/data`, `HELIOS_DIR=${ETL_DIR}/sf`, `SF_AOI_BBOX`, `SF_CITY_BBOX`, and series helpers `solar_score(...)`, `finalize_delta(...)`, `show_pmtiles(...)`, `show_cog(...)`, `show_raster(...)`.

Build the notebook cell-by-cell. Use real cell content (markdown text + python code) exactly as written below.

- [ ] **Step 1 — markdown cell (title):**
  ```markdown
  # Helios — Shared Configuration Notebook

  This notebook sets up the **Helios** tiling series: a San Francisco **solar
  site-selection** walkthrough that takes three data layers — building footprints,
  aerial imagery, and terrain — all the way to **PMTiles** map archives you can view
  inline. Every numbered notebook (`01.`, `02.`, `03.`) runs `%run ./config_nb` to
  establish catalog / schema / Volume paths, install GeoBrix, instantiate the
  `OvertureClient` and `StacClient`, and register shared helpers.
  ```

- [ ] **Step 2 — markdown cell (libraries / UC):**
  ```markdown
  __Libraries__

  * GeoBrix is installed below (the lightweight `[light,stac,vizx]` wheel) — nothing is assumed pre-staged.
  * Default tier is **lightweight** (`databricks.labs.gbx.pyrx` / `pyvx`), which runs on **Serverless**. Flip to option-2 (`rasterx` / `vectorx`) for the heavyweight tier on a classic x86 cluster (JAR + GDAL init script).

  __Unity Catalog__

  * Replace `catalog_name` and `schema_name` with your preferred locations.
  * A Volume named `data` must exist under `catalog_name`/`schema_name`.
  ```

- [ ] **Step 3 — code cell (%pip install, light tier):**
  ```python
  # -- GeoBrix: lightweight tier (option-1, default). Installed here so nothing is
  #    assumed pre-staged. For the heavyweight tier (option-2 below) attach the
  #    GeoBrix JAR + GDAL init script to a classic x86 cluster.
  %pip install --quiet --disable-pip-version-check --force-reinstall --no-deps "geobrix @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"
  %pip install --quiet "geobrix[light,stac,vizx] @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"
  %pip install --quiet rich
  ```

- [ ] **Step 4 — code cell (`%restart_python`):**
  ```python
  %restart_python
  ```

- [ ] **Step 5 — code cell (Spark/Delta imports):**
  ```python
  # -- databricks + delta + spark functions
  from delta.tables import *
  from pyspark.databricks.sql import functions as DBF
  from pyspark.sql import functions as F
  from pyspark.sql.functions import col, udf, pandas_udf
  from pyspark.sql.types import *
  from pyspark.sql.window import Window
  ```

- [ ] **Step 6 — code cell (other imports):**
  ```python
  # -- other imports
  from datetime import datetime
  from databricks.labs.gbx.stac import StacClient
  from databricks.labs.gbx.sample.overture import OvertureClient

  import os
  import pandas as pd
  import pathlib
  import warnings

  warnings.simplefilter("ignore")
  ```

- [ ] **Step 7 — markdown cell (client setup):**
  ```markdown
  ## Data-source clients

  - `OvertureClient` discovers and downloads Overture Maps GeoParquet (buildings, transportation, places, …) for an AOI into a Volume, with an optional metadata Delta catalog. Used in notebook 01.
  - `StacClient` catalogs the COGs we produce in notebook 03 into a queryable Delta table (the same client used by the EO series for Planetary Computer). See [STAC API](https://databrickslabs.github.io/geobrix/docs/api/stac).
  ```

- [ ] **Step 8 — code cell (clients):**
  ```python
  overture = OvertureClient()      # Overture Maps static STAC catalog; latest release resolved by default
  stac_client = StacClient()       # used in NB03 to STAC-catalog the COGs we generate
  ```

- [ ] **Step 9 — code cell (`set_conf_safe`, Serverless guard):**
  ```python
  # -- Spark conf tuning, guarded for Serverless --
  # Serverless forbids runtime spark.conf.set; set_conf_safe() no-ops there (AQE
  # handles partitioning). On classic clusters it applies high-parallelism tuning.
  def set_conf_safe(key, value):
      try:
          spark.conf.set(key, value)
          return True
      except Exception as e:
          print(f"... skipping spark.conf.set({key}) [Serverless?]: {type(e).__name__}")
          return False

  set_conf_safe("spark.sql.adaptive.coalescePartitions.enabled", "false")
  set_conf_safe("spark.sql.shuffle.partitions", 512)
  ```

- [ ] **Step 10 — code cell (tier switch + registration):**
  ```python
  # -- GeoBrix tier selection (default: lightweight) --
  # option-1: lightweight tier (pure Python / PySpark, runs on Serverless) -- DEFAULT
  from databricks.labs.gbx.pyrx import functions as rx     # rx.rst_*  (raster)
  from databricks.labs.gbx.pyvx import functions as vx     # vx.st_*   (vector)

  # option-2: heavyweight tier (Scala JAR + GDAL init script on a classic x86 cluster)
  # from databricks.labs.gbx.rasterx import functions as rx
  # from databricks.labs.gbx.vectorx import functions as vx

  rx.register(spark)    # registers gbx_rst_*, gbx_rst_xyzpyramid, gbx_rst_cog_convert, gbx_pmtiles_agg, ...
  vx.register(spark)    # registers gbx_st_asmvt, gbx_st_asmvt_pyramid, ...

  # -- light readers/writers (gtiff_gbx, binaryFile patterns, pmtiles writer) --
  from databricks.labs.gbx.ds.register import register
  register(spark)
  ```

- [ ] **Step 11 — code cell (viz + inspector imports):**
  ```python
  # -- visualization: PMTiles + COG viewers (net-new), plus the raster + vector helpers.
  #    plot_static / plot_interactive back the INTERACTIVE_PLOTS toggle (static images for
  #    GitHub by default; folium pan/zoom maps when True). See the helper cells below.
  from databricks.labs.gbx.vizx import (
      plot_pmtiles, plot_cog, plot_raster, plot_file, plot_static, plot_interactive,
      cells_as_gdf,
  )
  from databricks.labs.gbx.pmtiles import pmtiles_info       # driver-side PMTiles header inspector
  ```

- [ ] **Step 12 — code cell (catalog/schema/rebuild):**
  ```python
  # -- rebuild control: when True, force re-create of tables / re-download by feeding
  #    the do_overwrite / skip-guards below. Set per-notebook after %run ./config_nb.
  FORCE_REBUILD = False

  # Interactive folium maps are slow to render and heavy in result size.
  # False (default; jobs + docs) -> fast static images. True -> interactive folium maps.
  INTERACTIVE_PLOTS = False

  catalog_name = "geospatial_docs"
  schema_name = "helios"

  sql(f"USE CATALOG {catalog_name}")
  sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
  sql(f"USE DATABASE {schema_name}")
  print(f"... catalog: '{catalog_name}' (USE)")
  print(f"... schema: '{schema_name}' (CREATE / USE)")
  ```

- [ ] **Step 13 — code cell (ETL dirs + SF AOI):**
  ```python
  ETL_DIR = f"/Volumes/{catalog_name}/{schema_name}/data"  # <- Volume ('data') must exist
  HELIOS_DIR = f"{ETL_DIR}/sf"
  dbutils.fs.mkdirs(HELIOS_DIR)
  os.environ["ETL_DIR"] = ETL_DIR
  os.environ["HELIOS_DIR"] = HELIOS_DIR

  # -- San Francisco AOI (reuses the h3-rasterize N37W123 quad) -------------------
  # Full 1x1-deg quad (minx, miny, maxx, maxy, EPSG:4326): SF peninsula, Marin, East Bay.
  SF_AOI_BBOX = (-123.0, 37.0, -122.0, 38.0)
  # Tighter city sub-bbox keeps Overture/DEM volumes demo-friendly (the SF peninsula proper).
  SF_CITY_BBOX = (-122.52, 37.70, -122.35, 37.83)

  print(f"... ETL_DIR: '{ETL_DIR}'")
  print(f"... HELIOS_DIR: '{HELIOS_DIR}' (MKDIRS)")
  print(f"... SF_CITY_BBOX: {SF_CITY_BBOX}")
  ```

- [ ] **Step 14 — markdown cell (Helper Functions header):**
  ```markdown
  ## Helper Functions

  Series-only helpers (not promoted to the GeoBrix API because they are specific to
  this solar-site walkthrough):

  - `solar_score(slope_col, aspect_col)` — a simple south-facing-gentle-slope solar
    suitability score for the terrain step (notebook 03).
  - `finalize_delta(df, tbl_name, ...)` — idempotent managed-Delta materializer
    (write-once unless `FORCE_REBUILD`), the Serverless-safe alternative to `.cache()`.
  - `show_pmtiles(path)` / `show_cog(path)` / `show_raster(...)` — thin demo wrappers
    that print `pmtiles_info(...)` header metadata then render through the
    `INTERACTIVE_PLOTS` toggle: **False** (default; GitHub-renderable static images) calls
    `plot_pmtiles(path, max_embed_mb=0, ...)` / `plot_cog(...)` / `plot_static(...)`;
    **True** calls the interactive `plot_pmtiles(path, ...)` (MapLibre) and
    `plot_interactive(...)` (folium) paths. Set `INTERACTIVE_PLOTS = True` in a notebook
    (after `%run ./config_nb`) for live pan/zoom maps.
  ```

- [ ] **Step 15 — code cell (`solar_score`):**
  ```python
  def solar_score(slope_col="slope_deg", aspect_col="aspect_deg"):
      """Column expression: solar-suitability score in [0, 1] for a roof/terrain facet.

      Favors gentle slopes (best near a target tilt for SF latitude ~30 deg) and
      south-facing aspect (180 deg). Aspect is GDAL convention (0=N, 90=E, 180=S,
      270=W). This is a didactic score, not a production PV yield model.
      """
      target_tilt = 30.0
      slope_term = 1.0 - (F.abs(F.col(slope_col) - target_tilt) / 90.0)
      # cos of (aspect - south) folds 0..360 into a south-preference in [0, 1]
      aspect_term = (F.cos(F.radians(F.col(aspect_col) - 180.0)) + 1.0) / 2.0
      return F.greatest(F.lit(0.0), slope_term) * aspect_term
  ```

- [ ] **Step 16 — code cell (`finalize_delta`):**
  ```python
  def finalize_delta(df, tbl_name, do_display=True):
      """Idempotent managed-Delta materializer (Serverless-safe stand-in for cache()).

      Writes `df` to managed table `tbl_name` once; re-reads on subsequent runs unless
      FORCE_REBUILD is True. Returns the table DataFrame.
      """
      if FORCE_REBUILD:
          sql(f"DROP TABLE IF EXISTS {tbl_name}")
      if not spark.catalog.tableExists(tbl_name):
          df.write.mode("overwrite").saveAsTable(tbl_name)
          print(f"... wrote table {tbl_name} ({spark.table(tbl_name).count():,} rows)")
      else:
          print(f"... table {tbl_name} exists (skip; FORCE_REBUILD=False)")
      out = spark.table(tbl_name)
      if do_display:
          out.printSchema()
      return out
  ```

- [ ] **Step 17 — code cell (toggle-aware viewers: `show_pmtiles` / `show_cog` / `show_raster`):**
  ```python
  def show_pmtiles(path, **kw):
      """Print the PMTiles header, then render through the INTERACTIVE_PLOTS toggle.

      INTERACTIVE_PLOTS=False (default): plot_pmtiles(path, max_embed_mb=0, ...) forces
      the GitHub-renderable static image. True: the interactive MapLibre map (default
      plot_pmtiles path, base64-embedded in-browser FileSource).
      """
      info = pmtiles_info(path)
      print(f"... pmtiles: type={info.get('tile_type')} "
            f"zoom={info.get('min_zoom')}-{info.get('max_zoom')} "
            f"bounds={info.get('bounds')}")
      if INTERACTIVE_PLOTS:
          return plot_pmtiles(path, **kw)               # interactive MapLibre (default)
      return plot_pmtiles(path, max_embed_mb=0, **kw)   # static render (max_embed_mb=0)

  def show_cog(path, **kw):
      """Render a COG. plot_cog is static (rasterio overview read over a contextily
      basemap) in both modes; the toggle is honored for API symmetry with show_pmtiles."""
      return plot_cog(path, **kw)

  def show_raster(df_or_path, **kw):
      """Render a raster/vector overlay through the toggle: plot_static (False, default)
      or plot_interactive (True, folium). Accepts a Spark DataFrame of cells/geoms or a
      file path, matching the underlying vizx helpers."""
      if INTERACTIVE_PLOTS:
          return plot_interactive(df_or_path, **kw)
      return plot_static(df_or_path, **kw)
  ```
  > NOTE: `show_cog` honors `INTERACTIVE_PLOTS` only for call-site symmetry — per SP2, `plot_cog` is static-only by design (a COG is not a PMTiles archive; an interactive remote-raster source would need range requests, the very thing PMTiles base64 embedding avoids). Keep `show_cog` static in both modes; do not invent an interactive COG path.

- [ ] **Step 18 (VALIDATION):** `gbx:test:notebooks --path examples/helios/config_nb.ipynb --log helios-config.log` in the Docker container (started with `start_docker_with_volumes.sh`). Note the `%pip`/`%restart_python` cells run in the runner's venv; the cell-by-cell runner pre-installs the light deps. Expected: every cell reports OK; the final cell defines `solar_score`, `finalize_delta`, `show_pmtiles`, `show_cog`, `show_raster` without error; `INTERACTIVE_PLOTS` defaults to `False` and `SF_CITY_BBOX` is printed. If `%pip` from the Volume wheel can't resolve in the runner venv (the runner pre-installs light deps), confirm the runner picks up `OvertureClient`/`plot_pmtiles` from the installed `geobrix` — if not, the env-lock work in SP1/SP2 must register the new modules; record the gap and proceed (config_nb is %run-only, exercised end-to-end in Task 4's full-series validation).
- [ ] **Step 19 (commit):** `git add notebooks/examples/helios/config_nb.ipynb && git commit` — subject `feat(helios): add config_nb spine for SF solar tiling series`; body: WHY (shared %run setup: light tier, Overture+STAC clients, SF AOI, ETL dirs, series-only solar/Delta/viewer helpers). Trailer.

---

## Task 2: NB01 — Vector Engine (MVT)

**Files:**
- Create: `notebooks/examples/helios/01. Vector Engine (MVT).ipynb`

**Interfaces:**
- *Consumes (from `%run ./config_nb`):* `overture` (`OvertureClient`), `vx`, `register`, `plot_pmtiles`/`show_pmtiles`, `pmtiles_info`, `HELIOS_DIR`, `SF_CITY_BBOX`, `finalize_delta`, `FORCE_REBUILD`. Overture API: `overture.discover(SF_CITY_BBOX, themes=["buildings"])` → DataFrame `theme,type,href,asset_bbox,release`; `overture.download(assets_df, out_dir, *, table="overture_buildings_meta", validate=True, partitions=...)` → `theme,type,source,path,out_file_sz,is_out_file_valid,last_update,asset_bbox,release,href`; `overture.read(source, theme="buildings", type="building", bbox=SF_CITY_BBOX)` → GeoParquet rows with a geometry column. GeoBrix SQL: `gbx_st_asmvt`, `gbx_st_asmvt_pyramid`, `gbx_pmtiles_agg`. Databricks-native SQL (the on-ramp composition): `st_geomfromwkb`, `st_area`, `st_centroid`, `st_x`, `st_y`, `h3_longlatash3`.
- *Produces:* Volume dir `${HELIOS_DIR}/overture/` (downloaded GeoParquet), Delta table `overture_buildings_meta` (asset catalog), Delta table `sf_buildings_mvt_tiles` (per `(z,x,y)` MVT bytes), and the vector PMTiles archive `${HELIOS_DIR}/tiles/sf_buildings.pmtiles`.

- [ ] **Step 1 — markdown cell (title + meta-narrative):**
  ```markdown
  # Helios 01 — Vector Engine: building footprints to vector PMTiles

  ![Overture buildings to vector PMTiles](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/helios-01.png)

  **Solar site-selection, step 1: the candidate surfaces.** Every rooftop in San
  Francisco is a potential solar surface. This notebook pulls **Overture Maps building
  footprints** for the city, encodes them into **Mapbox Vector Tiles** with
  `gbx_st_asmvt`, pyramids them across zoom levels with `gbx_st_asmvt_pyramid`, folds
  the whole pyramid into one **PMTiles** archive with `gbx_pmtiles_agg`, and views it
  inline with `plot_pmtiles`. The result is a single self-contained vector basemap of
  every candidate roof — the geometry layer the later notebooks score for solar yield.

  Along the way we compose with **Databricks-native** `ST_*` and `H3` functions — GeoBrix
  tiling is an on-ramp into the native spatial engine, not a replacement for it — to
  quantify roof area and bin roofs into an H3 roof-density surface.

  > Runs on the **lightweight tier (Serverless)** by default. See `config_nb` for the
  > heavyweight switch.
  ```

- [ ] **Step 2 — code cell (`%run`):**
  ```python
  %run ./config_nb
  ```

- [ ] **Step 3 — code cell (optional rebuild flag):**
  ```python
  # Flip to True to fully rebuild this notebook's tables / re-download / re-tile.
  FORCE_REBUILD = False
  ```

- [ ] **Step 4 — markdown cell (discover):**
  ```markdown
  ## 1. Discover Overture building assets for the SF AOI

  `OvertureClient.discover` traverses Overture's static STAC catalog and returns one
  row per GeoParquet asset that intersects our bbox — metadata only, on the driver.
  We narrow to the `buildings` theme; `themes=None` would select every theme.
  ```

- [ ] **Step 5 — code cell (discover):**
  ```python
  assets = overture.discover(SF_CITY_BBOX, themes=["buildings"])
  display(assets)        # theme, type, href, asset_bbox, release
  print(f"... {assets.count()} intersecting building assets")
  ```

- [ ] **Step 6 — markdown cell (download):**
  ```markdown
  ## 2. Download the AOI subset to a Volume (+ catalog it in Delta)

  `OvertureClient.download` reads only the AOI rows (bbox predicate pushdown) and
  writes them to the Volume **distributed across workers**, returning a metadata
  DataFrame and UPSERTing it into the `overture_buildings_meta` Delta table (idempotent
  MERGE keyed by `theme, type, source`). On Serverless the download fans out via
  `repartition(N, col)` — no cluster knobs to tune.
  ```

- [ ] **Step 7 — code cell (download):**
  ```python
  OVERTURE_DIR = f"{HELIOS_DIR}/overture"
  meta = overture.download(
      assets, OVERTURE_DIR,
      table="overture_buildings_meta",
      validate=True,
      partitions=64,                 # hash fan-out; Serverless-safe parallelism
  )
  display(meta.select("theme", "type", "source", "out_file_sz", "is_out_file_valid"))
  ```

- [ ] **Step 8 — markdown cell (read):**
  ```markdown
  ## 3. Load the building geometries

  `OvertureClient.read` loads the downloaded GeoParquet back into Spark, re-applying
  the bbox AOI filter. We keep the building polygon geometry and a stable id.
  ```

- [ ] **Step 9 — code cell (read + project to tile inputs):**
  ```python
  buildings = (
      overture.read("overture_buildings_meta", theme="buildings", type="building", bbox=SF_CITY_BBOX)
              .select(F.col("id").alias("feature_id"), F.col("geometry"))
  )
  print(f"... {buildings.count():,} building footprints in the AOI")
  display(buildings.limit(5))
  ```

- [ ] **Step 9b — markdown cell (native ST/H3: roof area + H3 roof density):**
  ```markdown
  ## 3b. Quantify candidate roof space with Databricks-native ST + H3

  Before tiling, we use **Databricks-native** spatial functions on the same footprints —
  GeoBrix tiling composes directly with the native engine. Native `st_area` /
  `st_centroid` (over `st_geomfromwkb`) give each roof's **available area** and a point
  to index; native `h3_longlatash3` bins those centroids into H3 cells so we can
  aggregate **roof density and total roof area per cell** — a coarse "where are the
  candidate solar surfaces concentrated?" view that complements the per-building tiles.
  ```

- [ ] **Step 9c — code cell (native ST roof metrics):**
  ```python
  # Native ST: parse WKB -> GEOMETRY, then area (m^2, planar in the layer CRS) + centroid.
  # GeoBrix readers hand us a WKB geometry column; st_geomfromwkb bridges to native ST.
  roofs = buildings.selectExpr(
      "feature_id",
      "geometry",
      "st_area(st_geomfromwkb(geometry)) AS roof_area_m2",
      "st_x(st_centroid(st_geomfromwkb(geometry))) AS lon",
      "st_y(st_centroid(st_geomfromwkb(geometry))) AS lat",
  )
  display(roofs.orderBy(F.col("roof_area_m2").desc()).limit(5))
  ```
  > NOTE: `st_geomfromwkb`, `st_area`, `st_centroid`, `st_x`, `st_y` are Databricks-native ST built-ins (already used across GeoBrix docs, e.g. `docs/tests/python/api/sql_api.py`). Confirm the GeoBrix reader's geometry column is WKB (not EWKB/WKT); if it is already a native `GEOMETRY`, drop the `st_geomfromwkb(...)` wrapper. ST area is planar in the column CRS — for true m² either work in a projected CRS or use `GEOGRAPHY`/`st_area` semantics per the Databricks ST reference; this didactic step reports relative roof size. No placeholder — wire the real geometry encoding during implementation.

- [ ] **Step 9d — code cell (native H3 roof-density aggregation):**
  ```python
  # Native H3: index each roof centroid to an H3 cell (res 11 ~ city-block scale) and
  # aggregate roof count + total area per cell -> a roof-density surface.
  H3_RES = 11
  roof_density = (
      roofs.selectExpr("*", f"h3_longlatash3(lon, lat, {H3_RES}) AS h3_cell")
           .groupBy("h3_cell")
           .agg(F.count("*").alias("n_roofs"),
                F.sum("roof_area_m2").alias("total_roof_area_m2"))
  )
  display(roof_density.orderBy(F.col("total_roof_area_m2").desc()).limit(10))
  ```
  > NOTE: `h3_longlatash3(lng, lat, res)` is the Databricks-native point->H3 built-in (arg order is longitude, latitude; see the Databricks H3 reference / `docs/docs/databricks-spatial.mdx` H3_POINT_INDEX). Confirm the exact name + arg order against the H3 functions reference during implementation (`h3_longlatash3` is the standard; some surfaces expose `h3_pointash3` over a geometry). Native H3 requires Photon or Databricks SQL (Pro/Serverless). No placeholder.

- [ ] **Step 10 — markdown cell (MVT pyramid):**
  ```markdown
  ## 4. Encode + pyramid to vector tiles

  `gbx_st_asmvt_pyramid` is a table-valued function (UDTF): for each feature it emits
  one `(z, x, y, mvt_bytes)` row per zoom level in the requested range, binning the
  geometry into the web-mercator tile grid and encoding tile-local MVT. We pick a
  city-scale zoom range (z12–z16). Attributes ride along natively (here just
  `feature_id`).
  ```

- [ ] **Step 11 — code cell (asmvt_pyramid via SQL):**
  ```python
  buildings.createOrReplaceTempView("sf_buildings")
  mvt = spark.sql("""
      SELECT t.zoom AS z, t.tile_x AS x, t.tile_y AS y, t.mvt AS mvt
      FROM sf_buildings,
           LATERAL gbx_st_asmvt_pyramid(geometry, 12, 16, named_struct('feature_id', feature_id)) AS t
  """)
  sf_mvt = finalize_delta(mvt, "sf_buildings_mvt_tiles")
  print(f"... {sf_mvt.count():,} (z,x,y) MVT tiles across z12-z16")
  ```
  > NOTE: the exact `gbx_st_asmvt_pyramid` LATERAL output column names (`zoom`/`tile_x`/`tile_y`/`mvt`) must be confirmed against `pyvx` registration during implementation; if `gbx_st_asmvt` (single-tile, grouped) is the registered pyramid entry point instead of a UDTF, group buildings by an `gbx_st_tile_id`-style binning and aggregate per `(z,x,y)`. Confirm against `docs/docs/api/vectorx-functions.mdx` and adjust this cell to the real signature before finalizing — no placeholder.

- [ ] **Step 12 — markdown cell (PMTiles agg):**
  ```markdown
  ## 5. Fold the tile pyramid into one PMTiles archive

  `gbx_pmtiles_agg` is a grouped aggregate that folds a set of `(mvt, z, x, y)` tiles
  into a single PMTiles v3 archive (BINARY). We aggregate the whole pyramid into one
  archive and write it to the Volume.
  ```

- [ ] **Step 13 — code cell (pmtiles_agg + write):**
  ```python
  archive_row = (
      sf_mvt.groupBy(F.lit(1).alias("_g"))
            .agg(F.expr("gbx_pmtiles_agg(mvt, z, x, y)").alias("archive"))
            .select("archive")
            .collect()[0]
  )
  TILES_DIR = f"{HELIOS_DIR}/tiles"
  dbutils.fs.mkdirs(TILES_DIR)
  PMTILES_PATH = f"{TILES_DIR}/sf_buildings.pmtiles"
  # FUSE-safe sequential write from the driver (single archive, bytes already in memory)
  with open(PMTILES_PATH, "wb") as f:
      f.write(archive_row["archive"])
  print(f"... wrote {PMTILES_PATH} ({os.path.getsize(PMTILES_PATH):,} bytes)")
  ```

- [ ] **Step 14 — markdown cell (view):**
  ```markdown
  ## 6. View the vector PMTiles inline

  `show_pmtiles` prints the `pmtiles_info` header, then renders through the
  `INTERACTIVE_PLOTS` toggle (set in `config_nb`): the default **False** produces a
  fast static image that renders on GitHub and the docs site; set `INTERACTIVE_PLOTS =
  True` for an interactive MapLibre layer (streamed in-browser, no tile server).
  `plot_pmtiles` auto-detects the vector archive in either mode.
  ```

- [ ] **Step 15 — code cell (view):**
  ```python
  show_pmtiles(PMTILES_PATH)
  ```

- [ ] **Step 16 — markdown cell (recap):**
  ```markdown
  ## What we built

  - `overture_buildings_meta` (Delta) — the queryable asset catalog of downloaded GeoParquet.
  - `sf_buildings_mvt_tiles` (Delta) — one row per `(z, x, y)` vector tile.
  - `sf_buildings.pmtiles` (Volume) — a self-contained vector basemap of every candidate roof.
  - A native **ST roof-area** table and an **H3 roof-density** aggregation — showing the
    tiles compose directly with Databricks-native spatial.

  Next: **notebook 02** drapes NAIP aerial imagery behind these footprints as a visual basemap.
  ```

- [ ] **Step 17 (VALIDATION):** `gbx:test:notebooks --path "examples/helios/01. Vector Engine (MVT).ipynb" --log helios-01.log` in Docker (with `/Volumes`). Because the runner remaps absolute Volume paths under a temp workdir by default, run with `--allow-absolute-reads --allow-absolute-writes` only if the Overture/sample data must come from the real Volume; otherwise rely on the remap + sample data. Expected: cells execute green; `sf_buildings.pmtiles` exists and `pmtiles_info(PMTILES_PATH)["tile_type"]` is the MVT type; `show_pmtiles` returns a rendered object (static image by default since `INTERACTIVE_PLOTS=False`); the native-ST roof-metrics + H3 roof-density cells produce non-empty results. If the network Overture catalog is unreachable in Docker, gate the discover/download cells behind an env check and fall back to a small committed sample GeoParquet under sample-data; record the assumption and keep the tiling+PMTiles+view cells live. NOTE: native `st_*`/`h3_longlatash3` need Databricks ST/H3 (Photon / Databricks SQL); the cell-by-cell Docker runner may lack them — if so, gate the native-ST/H3 cells behind a capability check (try the expr; skip with a printed note on failure) so tiling stays green, and record the gap. Resolve the exact native names in Task 7 Step 3.
- [ ] **Step 18 (commit):** `git add "notebooks/examples/helios/01. Vector Engine (MVT).ipynb" && git commit` — subject `feat(helios): add NB01 Overture buildings to vector PMTiles`; body WHY. Trailer.

---

## Task 3: NB02 — Visual Basemap (XYZ raster)

**Files:**
- Create: `notebooks/examples/helios/02. Visual Basemap (XYZ).ipynb`

**Interfaces:**
- *Consumes (from `%run ./config_nb`):* `rx`, `register`, `plot_pmtiles`/`show_pmtiles`, `pmtiles_info`, `plot_file`, `HELIOS_DIR`, `SF_CITY_BBOX`, `finalize_delta`, `FORCE_REBUILD`. SQL: `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`, `gbx_pmtiles_agg`. Reader: `binaryFile` → `rst_fromcontent` and/or `gtiff_gbx`.
- *Produces:* Volume dir `${HELIOS_DIR}/naip/` (staged NAIP GeoTIFF), Delta table `sf_naip_xyz_tiles` (per `(z,x,y)` PNG tile bytes), raster PMTiles `${HELIOS_DIR}/tiles/sf_naip.pmtiles`.

- [ ] **Step 1 — markdown cell (title + meta-narrative):**
  ```markdown
  # Helios 02 — Visual Basemap: NAIP aerial imagery to raster PMTiles

  ![NAIP aerial to raster PMTiles](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/helios-02.png)

  **Solar site-selection, step 2: the visual context.** Before scoring roofs, we want
  to *see* them. This notebook stages **NAIP** (National Agriculture Imagery Program)
  aerial imagery for San Francisco, reprojects it to web mercator with
  `gbx_rst_to_webmercator`, slices it into an **XYZ tile pyramid** with
  `gbx_rst_xyzpyramid`, packages the pyramid as raster **PMTiles** via
  `gbx_pmtiles_agg`, and views it with `plot_pmtiles`. This aerial basemap sits behind
  the building footprints from notebook 01.

  > Runs on the **lightweight tier (Serverless)** by default.
  ```

- [ ] **Step 2 — code cell (`%run`):**
  ```python
  %run ./config_nb
  ```

- [ ] **Step 3 — code cell (rebuild flag):**
  ```python
  FORCE_REBUILD = False
  ```

- [ ] **Step 4 — markdown cell (NAIP staging — notebook helper, NOT a module):**
  ```markdown
  ## 1. Stage NAIP aerial imagery (notebook helper)

  NAIP is hosted as Cloud-Optimized GeoTIFFs in the public AWS Open Data registry
  (`s3://naip-analytic`, public/requester-pays) and is also discoverable via STAC on
  Planetary Computer (collection `naip`). NAIP does **not** get a module-level API in
  GeoBrix — this is a **notebook-local helper** that fetches one SF tile and stages it
  to the Volume (idempotent, FUSE-safe sequential copy). On Serverless it uses
  rasterio's bundled GDAL (no `gdal_translate` CLI).
  ```

- [ ] **Step 5 — code cell (NAIP staging helper):**
  ```python
  import shutil
  from databricks.labs.gbx.sample import get_temp_dir          # node-local scratch helper

  NAIP_DIR = f"{HELIOS_DIR}/naip"
  dbutils.fs.mkdirs(NAIP_DIR)
  NAIP_PATH = f"{NAIP_DIR}/sf_naip.tif"

  def stage_naip(dest=NAIP_PATH, bbox=SF_CITY_BBOX):
      """Stage one SF NAIP COG to the Volume via Planetary Computer STAC (idempotent)."""
      if os.path.exists(dest) and not FORCE_REBUILD:
          print(f"... NAIP already staged at {dest}")
          return dest
      import planetary_computer as pc
      import pystac_client, rasterio
      cat = pystac_client.Client.open(
          "https://planetarycomputer.microsoft.com/api/stac/v1",
          modifier=pc.sign_inplace,
      )
      minx, miny, maxx, maxy = bbox
      item = next(cat.search(collections=["naip"], bbox=[minx, miny, maxx, maxy],
                             limit=1).items())
      href = item.assets["image"].href
      tmp = get_temp_dir()
      local = tmp / "sf_naip.tif"
      with rasterio.open(href) as src:
          profile = {**src.profile, "driver": "GTiff"}
          win = src.window(minx, miny, maxx, maxy)   # crop to AOI to keep volume demo-friendly
          data = src.read(window=win)
          profile.update(width=data.shape[2], height=data.shape[1],
                         transform=src.window_transform(win))
          with rasterio.open(local, "w", **profile) as dst:
              dst.write(data)
      shutil.copy(str(local), dest)                  # FUSE-safe sequential copy
      print(f"... staged NAIP -> {dest} ({os.path.getsize(dest):,} bytes)")
      return dest

  stage_naip()
  ```

- [ ] **Step 6 — markdown cell (preview the source):**
  ```markdown
  ## 2. Preview the source imagery

  `plot_file` renders the staged GeoTIFF straight from the Volume (auto-decimation,
  per-band percentile stretch). This is a static source preview (a raw source raster has
  no tiled interactive form); the tiled PMTiles **product** is what the `INTERACTIVE_PLOTS`
  toggle governs at the view step below.
  ```

- [ ] **Step 7 — code cell (preview):**
  ```python
  plot_file(NAIP_PATH, fig_w=8, fig_h=6)
  ```

- [ ] **Step 8 — markdown cell (load as tile):**
  ```markdown
  ## 3. Load the imagery into a typed tile

  We read the GeoTIFF bytes with the `binaryFile` reader and build a typed `tile`
  struct via `rst_fromcontent` — the temp-file-free path that avoids executor races.
  ```

- [ ] **Step 9 — code cell (binaryFile → rst_fromcontent):**
  ```python
  naip = (
      spark.read.format("binaryFile").load(NAIP_PATH)
           .select(rx.rst_fromcontent(F.col("content")).alias("tile"))
  )
  print(f"... loaded {naip.count()} source tile(s)")
  ```

- [ ] **Step 10 — markdown cell (reproject):**
  ```markdown
  ## 4. Reproject to web mercator

  XYZ / PMTiles tiles live in web mercator (EPSG:3857). `gbx_rst_to_webmercator`
  reprojects the tile so the pyramid aligns to the slippy-map grid.
  ```

- [ ] **Step 11 — code cell (to_webmercator):**
  ```python
  naip_3857 = naip.select(rx.rst_to_webmercator("tile").alias("tile"))
  ```

- [ ] **Step 12 — markdown cell (xyzpyramid):**
  ```markdown
  ## 5. Build the XYZ tile pyramid

  `gbx_rst_xyzpyramid` slices the reprojected raster into a pyramid of slippy-map PNG
  tiles across a zoom range, emitting `(z, x, y, tile_bytes)` rows. We pick a
  city-scale zoom range (z12–z16) to match notebook 01.
  ```

- [ ] **Step 13 — code cell (xyzpyramid):**
  ```python
  naip_3857.createOrReplaceTempView("sf_naip_tile")
  xyz = spark.sql("""
      SELECT p.zoom AS z, p.tile_x AS x, p.tile_y AS y, p.tile AS png
      FROM sf_naip_tile,
           LATERAL gbx_rst_xyzpyramid(tile, 12, 16) AS p
  """)
  sf_xyz = finalize_delta(xyz, "sf_naip_xyz_tiles")
  print(f"... {sf_xyz.count():,} (z,x,y) raster tiles across z12-z16")
  ```
  > NOTE: confirm the `gbx_rst_xyzpyramid` output column names + arg order (tile, min_zoom, max_zoom) against `docs/docs/api/raster-functions.mdx` / pyrx registration during implementation; adjust to the real signature — no placeholder.

- [ ] **Step 14 — markdown cell (pmtiles agg):**
  ```markdown
  ## 6. Package as raster PMTiles

  Same `gbx_pmtiles_agg` aggregate as notebook 01 — it auto-detects PNG tiles and
  writes a **raster** PMTiles archive.
  ```

- [ ] **Step 15 — code cell (pmtiles_agg + write):**
  ```python
  archive_row = (
      sf_xyz.groupBy(F.lit(1).alias("_g"))
            .agg(F.expr("gbx_pmtiles_agg(png, z, x, y)").alias("archive"))
            .select("archive").collect()[0]
  )
  TILES_DIR = f"{HELIOS_DIR}/tiles"
  dbutils.fs.mkdirs(TILES_DIR)
  NAIP_PMTILES = f"{TILES_DIR}/sf_naip.pmtiles"
  with open(NAIP_PMTILES, "wb") as f:
      f.write(archive_row["archive"])
  print(f"... wrote {NAIP_PMTILES} ({os.path.getsize(NAIP_PMTILES):,} bytes)")
  ```

- [ ] **Step 16 — markdown cell (view):**
  ```markdown
  ## 7. View the raster PMTiles inline

  `show_pmtiles` renders through the `INTERACTIVE_PLOTS` toggle — a static image by
  default (GitHub/docs-renderable), or an interactive MapLibre raster layer when
  `INTERACTIVE_PLOTS = True`.
  ```

- [ ] **Step 17 — code cell (view):**
  ```python
  show_pmtiles(NAIP_PMTILES)
  ```

- [ ] **Step 18 — markdown cell (recap):**
  ```markdown
  ## What we built

  - `sf_naip_xyz_tiles` (Delta) — one row per `(z, x, y)` PNG tile.
  - `sf_naip.pmtiles` (Volume) — a self-contained aerial basemap.

  Next: **notebook 03** adds the analytical layer — terrain slope and aspect from a
  USGS 3DEP DEM, the inputs to a solar suitability score.
  ```

- [ ] **Step 19 (VALIDATION):** `gbx:test:notebooks --path "examples/helios/02. Visual Basemap (XYZ).ipynb" --log helios-02.log` in Docker. Expected: green cells; `sf_naip.pmtiles` exists; `pmtiles_info` reports a raster `tile_type` (PNG/JPEG/WebP); `show_pmtiles` renders. If NAIP STAC is unreachable in Docker, gate `stage_naip` behind a reachability check and fall back to the committed `srtm_n37w123.tif` (or a small RGB sample) so the reproject→pyramid→PMTiles→view chain still runs; record the assumption.
- [ ] **Step 20 (commit):** `git add "notebooks/examples/helios/02. Visual Basemap (XYZ).ipynb" && git commit` — subject `feat(helios): add NB02 NAIP imagery to raster PMTiles`; body WHY. Trailer.

---

## Task 4: NB03 — Analytical Core (COG + STAC)

**Files:**
- Create: `notebooks/examples/helios/03. Analytical Core (COG + STAC).ipynb`

**Interfaces:**
- *Consumes (from `%run ./config_nb`):* `rx`, `register`, `stac_client`, `plot_cog`/`show_cog`, `plot_pmtiles`/`show_pmtiles`, `pmtiles_info`, `solar_score`, `finalize_delta`, `HELIOS_DIR`, `SF_CITY_BBOX`, `FORCE_REBUILD`. GeoBrix SQL: `gbx_rst_cog_convert`, terrain `rst_terrainslope`/`rst_terrainaspect`/`rst_hillshade` (confirm registered names), `gbx_rst_h3_rastertogridavg`, `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`, `gbx_pmtiles_agg`. Databricks-native SQL (the on-ramp composition): `h3_centeraswkb` (companions `h3_boundaryaswkb`/`h3_hexring`).
- *Produces:* Volume dir `${HELIOS_DIR}/dem/` (staged 3DEP DEM), Volume dir `${HELIOS_DIR}/cog/` (COGs), Delta table `sf_cog_catalog` (STAC catalog of the COGs), Delta table `sf_terrain` (slope/aspect/hillshade tiles), Delta table `sf_solar_cells` (per-H3-cell avg slope/aspect + `solar_score` + native H3 cell geometry), raster PMTiles `${HELIOS_DIR}/tiles/sf_hillshade.pmtiles`.

- [ ] **Step 1 — markdown cell (title + meta-narrative):**
  ```markdown
  # Helios 03 — Analytical Core: terrain, COGs, STAC, and solar scoring

  ![3DEP DEM to COG + STAC + hillshade PMTiles](https://raw.githubusercontent.com/databrickslabs/geobrix/main/resources/images/helios-03.png)

  **Solar site-selection, step 3: the analytical layer.** Roof solar yield depends on
  **slope** and **aspect** (south-facing, gently sloped wins). This notebook stages a
  **USGS 3DEP** DEM for San Francisco, converts it to **Cloud-Optimized GeoTIFFs** with
  `gbx_rst_cog_convert`, **catalogs the COGs into a queryable STAC Delta table** with
  `StacClient`, derives slope/aspect/hillshade, aggregates them into a per-**H3-cell**
  `solar_score` index (composing with Databricks-native H3), and renders the hillshade as
  PMTiles. The COG + STAC catalog is the analysis-ready, time-travel-friendly artifact;
  the H3 solar-suitability cells are the grid-indexed analytical layer; the hillshade
  PMTiles is the human-readable relief view.

  > GeoBrix tiling and raster→grid aggregation are an on-ramp into Databricks-native
  > spatial: the H3 `cellid` we produce is a standard native id you can join and render
  > with native H3 functions.

  > Runs on the **lightweight tier (Serverless)** by default.
  ```

- [ ] **Step 2 — code cell (`%run`):**
  ```python
  %run ./config_nb
  ```

- [ ] **Step 3 — code cell (rebuild flag):**
  ```python
  FORCE_REBUILD = False
  ```

- [ ] **Step 4 — markdown cell (DEM staging helper):**
  ```markdown
  ## 1. Stage a USGS 3DEP DEM (notebook helper)

  USGS 3DEP elevation is on the public AWS Open Data registry and discoverable via
  Planetary Computer STAC (collection `3dep-seamless`). Like NAIP, 3DEP stays a
  **notebook-local helper** — no module API. We stage one SF DEM tile to the Volume
  (idempotent). For offline runs we fall back to the h3-rasterize SRTM tile already
  staged at `geobrix-examples/sf/elevation/srtm_n37w123.tif`.
  ```

- [ ] **Step 5 — code cell (DEM staging helper):**
  ```python
  import shutil
  from databricks.labs.gbx.sample import get_temp_dir

  DEM_DIR = f"{HELIOS_DIR}/dem"
  dbutils.fs.mkdirs(DEM_DIR)
  DEM_PATH = f"{DEM_DIR}/sf_3dep.tif"
  SRTM_FALLBACK = "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-examples/sf/elevation/srtm_n37w123.tif"

  def stage_dem(dest=DEM_PATH, bbox=SF_CITY_BBOX):
      """Stage one SF 3DEP DEM tile via Planetary Computer STAC; fall back to the
      already-staged SRTM tile when offline (idempotent)."""
      if os.path.exists(dest) and not FORCE_REBUILD:
          print(f"... DEM already staged at {dest}")
          return dest
      try:
          import planetary_computer as pc
          import pystac_client, rasterio
          cat = pystac_client.Client.open(
              "https://planetarycomputer.microsoft.com/api/stac/v1",
              modifier=pc.sign_inplace,
          )
          minx, miny, maxx, maxy = bbox
          item = next(cat.search(collections=["3dep-seamless"],
                                 bbox=[minx, miny, maxx, maxy], limit=1).items())
          href = item.assets["data"].href
          tmp = get_temp_dir(); local = tmp / "sf_3dep.tif"
          with rasterio.open(href) as src:
              win = src.window(minx, miny, maxx, maxy)
              data = src.read(window=win)
              profile = {**src.profile, "driver": "GTiff",
                         "width": data.shape[2], "height": data.shape[1],
                         "transform": src.window_transform(win)}
              with rasterio.open(local, "w", **profile) as dst:
                  dst.write(data)
          shutil.copy(str(local), dest)
          print(f"... staged 3DEP -> {dest}")
      except Exception as e:
          print(f"... 3DEP STAC unavailable ({type(e).__name__}); falling back to SRTM")
          shutil.copy(SRTM_FALLBACK, dest)
      return dest

  stage_dem()
  plot_file(DEM_PATH, fig_w=8, fig_h=6)
  ```

- [ ] **Step 6 — markdown cell (COG convert):**
  ```markdown
  ## 2. Convert the DEM to Cloud-Optimized GeoTIFF

  `gbx_rst_cog_convert` rewrites the raster as a COG (internal tiling + overviews) so
  downstream tools can do fast windowed/overview reads. We load the DEM as a typed
  tile, convert, and write the COG bytes to the Volume.
  ```

- [ ] **Step 7 — code cell (cog_convert + write):**
  ```python
  dem = (
      spark.read.format("binaryFile").load(DEM_PATH)
           .select(rx.rst_fromcontent(F.col("content")).alias("tile"))
  )
  cog = dem.select(rx.rst_cog_convert("tile").alias("tile"))  # SQL: gbx_rst_cog_convert
  COG_DIR = f"{HELIOS_DIR}/cog"; dbutils.fs.mkdirs(COG_DIR)
  COG_PATH = f"{COG_DIR}/sf_dem_cog.tif"
  cog_bytes = cog.select(rx.rst_asbinary("tile").alias("b")).collect()[0]["b"]
  with open(COG_PATH, "wb") as f:
      f.write(cog_bytes)
  print(f"... wrote COG {COG_PATH} ({os.path.getsize(COG_PATH):,} bytes)")
  ```
  > NOTE: confirm the bytes-extraction accessor (`rst_asbinary` vs `rst_tobytes` vs a `.tile.raster` struct field) and `rst_cog_convert` arg signature against pyrx registration during implementation; use the `gtiff_gbx`/`pmtiles`-style writer if that is the canonical COG write path. No placeholder — wire the real accessor.

- [ ] **Step 8 — markdown cell (view the COG):**
  ```markdown
  ## 3. View the COG

  `plot_cog` does a rasterio overview read and renders the elevation surface.
  ```

- [ ] **Step 9 — code cell (plot_cog):**
  ```python
  show_cog(COG_PATH)
  ```

- [ ] **Step 10 — markdown cell (STAC catalog the COGs):**
  ```markdown
  ## 4. Catalog the COGs into a STAC Delta table

  We register the COG(s) as STAC items in a queryable Delta table — a re-runnable,
  time-travel-friendly catalog of the analysis-ready elevation assets, keyed by their
  Volume path. This is the same cataloging shape the EO series uses for downloaded assets.
  ```

- [ ] **Step 11 — code cell (build STAC catalog rows):**
  ```python
  import rasterio
  with rasterio.open(COG_PATH) as src:
      b = src.bounds
      cog_meta = [(
          "sf_dem_cog", COG_PATH, str(src.crs),
          float(b.left), float(b.bottom), float(b.right), float(b.top),
          "3dep-seamless",
      )]
  cog_df = spark.createDataFrame(
      cog_meta,
      "item_id string, source string, crs string, minx double, miny double, "
      "maxx double, maxy double, collection string",
  )
  sf_cog_catalog = finalize_delta(cog_df, "sf_cog_catalog")
  display(sf_cog_catalog)
  ```
  > NOTE: if `StacClient` exposes a `catalog(...)`/`register_items(...)` helper for local COGs, prefer it over hand-building rows; confirm the StacClient surface during implementation and use the real method if present — otherwise the hand-built Delta catalog above is the documented fallback.

- [ ] **Step 12 — markdown cell (slope/aspect/hillshade + solar score):**
  ```markdown
  ## 5. Derive slope, aspect, hillshade, and a solar score

  Slope and aspect come straight from the DEM tile (`rst_terrainslope` / `rst_terrainaspect`,
  auto-scaled from the CRS). `solar_score` (defined in `config_nb`) favors gently sloped,
  south-facing terrain. `rst_hillshade` produces the relief shading we tile for the view.
  ```

- [ ] **Step 13 — code cell (terrain + score):**
  ```python
  terrain = cog.select(
      rx.rst_terrainslope("tile").alias("slope_tile"),
      rx.rst_terrainaspect("tile").alias("aspect_tile"),
      rx.rst_hillshade("tile").alias("hillshade_tile"),
  )
  sf_terrain = finalize_delta(terrain, "sf_terrain", do_display=True)
  print("... slope / aspect / hillshade tiles materialized")
  # solar_score(slope_col, aspect_col) is applied per H3 cell in the next step (5b),
  # where slope+aspect are aggregated onto an H3 grid via gbx_rst_h3_rastertogridavg.
  ```
  > NOTE: confirm the registered terrain function names (`rst_terrainslope`/`rst_slope`, `rst_terrainaspect`/`rst_aspect`, `rst_hillshade`) and whether they take a CRS-scale arg, against `docs/docs/api/raster-functions.mdx`. The repo memory "Terrain CRS-scale GDAL-normal" notes slope/hillshade auto-scale from CRS — no manual `-s`. Wire the real names; no placeholder.

- [ ] **Step 13b — markdown cell (native H3 solar-suitability index):**
  ```markdown
  ## 5b. Aggregate slope + aspect into a per-H3-cell solar-suitability index

  GeoBrix raster→grid aggregation bins the slope and aspect rasters onto **H3 cells**
  (`rst_h3_rastertogridavg`), emitting a standard H3 integer `cellid` per cell. Because
  that `cellid` is a native H3 id, the result joins and renders directly with
  **Databricks-native H3** functions — `h3_centeraswkb` / `h3_boundaryaswkb` for cell
  geometry, `h3_hexring` for neighborhoods. We apply `solar_score(slope, aspect)` (from
  `config_nb`) per cell to get a coarse south-facing-gentle-slope suitability surface.
  This is the analytical payoff: a queryable, grid-indexed solar-suitability layer that
  composes with native H3 and the NB01 roof-density cells on the same index.
  ```

- [ ] **Step 13c — code cell (raster→H3 grid + solar score + native H3 geometry):**
  ```python
  H3_RES = 11
  # GeoBrix raster->H3 grid aggregation: mean slope + mean aspect per H3 cell.
  cog.createOrReplaceTempView("sf_cog_tile")
  cells = spark.sql(f"""
      SELECT s.cellID AS cellid, s.measure AS avg_slope, a.measure AS avg_aspect
      FROM (SELECT t.cellID, t.measure
              FROM sf_cog_tile,
                   LATERAL gbx_rst_h3_rastertogridavg(rst_terrainslope(tile), {H3_RES}) t) s
      JOIN (SELECT t.cellID, t.measure
              FROM sf_cog_tile,
                   LATERAL gbx_rst_h3_rastertogridavg(rst_terrainaspect(tile), {H3_RES}) t) a
        ON s.cellID = a.cellID
  """)
  # solar_score expects degree columns; alias to its defaults.
  scored = cells.select(
      "cellid", "avg_slope", "avg_aspect",
      solar_score(slope_col="avg_slope", aspect_col="avg_aspect").alias("solar_score"),
  )
  # Native H3: cellid is a standard H3 id -> native cell geometry for joins/rendering.
  scored = scored.selectExpr(
      "*", "h3_centeraswkb(cellid) AS cell_center_wkb"
  )
  sf_solar_cells = finalize_delta(scored, "sf_solar_cells")
  display(sf_solar_cells.orderBy(F.col("solar_score").desc()).limit(10))
  ```
  > NOTE: confirm the GeoBrix raster→H3 grid function name + LATERAL output columns (`gbx_rst_h3_rastertogridavg` and its `cellID`/`measure` fields) against `docs/docs/api/raster-functions.mdx` (the `rst_h3_rastertogrid*` family) and the registered terrain names (`rst_terrainslope`/`rst_terrainaspect`). `h3_centeraswkb(cellid)` is the Databricks-native H3 cell-center built-in (companion: `h3_boundaryaswkb`, `h3_hexring`); confirm the exact native name + that it accepts an integer cell id against the Databricks H3 functions reference. Native H3 requires Photon or Databricks SQL. No placeholder — wire the real signatures.

- [ ] **Step 14 — markdown cell (hillshade → PMTiles):**
  ```markdown
  ## 6. Tile the hillshade to raster PMTiles

  Reproject the hillshade to web mercator, pyramid it, and fold into PMTiles — the same
  `to_webmercator → xyzpyramid → pmtiles_agg` chain as notebook 02, now over the relief.
  ```

- [ ] **Step 15 — code cell (hillshade pmtiles):**
  ```python
  hs_3857 = sf_terrain.select(rx.rst_to_webmercator("hillshade_tile").alias("tile"))
  hs_3857.createOrReplaceTempView("sf_hillshade_tile")
  hs_xyz = spark.sql("""
      SELECT p.zoom AS z, p.tile_x AS x, p.tile_y AS y, p.tile AS png
      FROM sf_hillshade_tile,
           LATERAL gbx_rst_xyzpyramid(tile, 11, 14) AS p
  """)
  archive_row = (
      hs_xyz.groupBy(F.lit(1).alias("_g"))
            .agg(F.expr("gbx_pmtiles_agg(png, z, x, y)").alias("archive"))
            .select("archive").collect()[0]
  )
  TILES_DIR = f"{HELIOS_DIR}/tiles"; dbutils.fs.mkdirs(TILES_DIR)
  HS_PMTILES = f"{TILES_DIR}/sf_hillshade.pmtiles"
  with open(HS_PMTILES, "wb") as f:
      f.write(archive_row["archive"])
  print(f"... wrote {HS_PMTILES} ({os.path.getsize(HS_PMTILES):,} bytes)")
  ```

- [ ] **Step 16 — code cell (view):**
  ```python
  show_pmtiles(HS_PMTILES)
  ```

- [ ] **Step 17 — markdown cell (recap + series close):**
  ```markdown
  ## What we built — and the full picture

  - `sf_cog_catalog` (Delta) — the queryable STAC catalog of analysis-ready COGs.
  - `sf_terrain` (Delta) — slope / aspect / hillshade tiles.
  - `sf_solar_cells` (Delta) — per-H3-cell avg slope/aspect + `solar_score`, with native
    H3 cell geometry — joins directly with the NB01 roof-density cells on the same index.
  - `sf_hillshade.pmtiles` (Volume) — the relief view.

  Across the series we built three PMTiles layers over one SF AOI — **buildings**
  (vector, NB01), **NAIP aerial** (raster, NB02), and **hillshade** (raster, NB03) —
  plus a COG + STAC catalog of the elevation. Stack the building footprints over the
  aerial basemap, score each roof by the terrain `solar_score`, and you have an
  end-to-end distributed solar site-selection pipeline — ingest → tile → PMTiles → view,
  all on Databricks.
  ```

- [ ] **Step 18 (VALIDATION):** `gbx:test:notebooks --path "examples/helios/03. Analytical Core (COG + STAC).ipynb" --log helios-03.log` in Docker. Expected: green cells; `sf_dem_cog.tif` exists and `plot_cog` renders; `sf_cog_catalog`/`sf_terrain`/`sf_solar_cells` tables exist (`sf_solar_cells` has a `solar_score` column and a native-H3 `cell_center_wkb`); `sf_hillshade.pmtiles` exists and `pmtiles_info` reports a raster type. The DEM staging helper already falls back to the staged SRTM tile offline, so this notebook should validate fully in Docker against the committed sample DEM. NOTE: native H3 (`h3_centeraswkb`) needs Photon / Databricks SQL; the cell-by-cell Docker runner may not have native H3 registered — if so, gate the native-H3 columns behind a capability check (try the expr, fall back to skipping the `cell_center_wkb` column) so the rest of the notebook stays green, and record the gap. The GeoBrix raster→grid aggregation + `solar_score` run in either environment.
- [ ] **Step 19 (commit):** `git add "notebooks/examples/helios/03. Analytical Core (COG + STAC).ipynb" && git commit` — subject `feat(helios): add NB03 DEM to COG+STAC + hillshade PMTiles`; body WHY. Trailer.

---

## Task 5: README.md

**Files:**
- Create: `notebooks/examples/helios/README.md`

**Interfaces:** Documents the artifacts NB01–NB03 produce by their exact table/Volume names; references the three `helios-0N.png` images by `../../../resources/images/...` relative path (matching eo-series/h3-rasterize). No code.

- [ ] **Step 1:** Write README.md mirroring the eo-series/h3-rasterize structure, sections in order:
  - **Title + one-paragraph intro** — "Helios — Distributed Tiling to PMTiles" framing: one SF AOI, three layers (buildings/MVT, NAIP/XYZ, terrain/COG+STAC), all to PMTiles, solar site-selection narrative. Link to GeoBrix docs.
  - **Lightweight-tier blockquote** — light `[light,stac,vizx]` default on Serverless; heavyweight switch via option-2; link Execution Tiers. (Copy the phrasing pattern from h3-rasterize/eo-series.)
  - **Data-source blockquote** — Overture Maps (buildings) via `OvertureClient`; NAIP + USGS 3DEP via notebook helpers from Planetary Computer / AWS Open Data, with the SRTM offline fallback; all staged to the Volume idempotently.
  - **Notebooks at a glance** — three `###` subsections (01/02/03), each with `![...](../../../resources/images/helios-0N.png)` and 3 bullet highlights (distributed fan-out, the key functions, the produced PMTiles artifact). Stress the distributed parallelism advantage over single-node tile generation (factual). For NB01 and NB03, one highlight notes the **Databricks-native** composition (NB01: ST roof area + H3 roof density; NB03: H3 per-cell `solar_score`) — the on-ramp framing, factual, not marketing.
  - **Files** table — `config_nb.ipynb` + the three numbered notebooks + their one-line purpose (mirror the eo-series Files table wording).
  - **Prerequisites** — DBR 17.3/18 LTS or Serverless (env v5+); GeoBrix 0.4.0 `[light,stac,vizx]` wheel; UC `catalog_name`/`schema_name` + a `data` Volume; heavyweight x86 + JAR + GDAL note.
  - **Run order** — open config_nb, set catalog/schema, run 01→02→03; each starts `%run ./config_nb`; `FORCE_REBUILD=True` to rebuild. One line: the notebooks ship with `INTERACTIVE_PLOTS = False` so the committed `.ipynb` renders fast static maps on GitHub — set `INTERACTIVE_PLOTS = True` (in `config_nb` or after `%run`) for interactive folium/MapLibre maps.
  - **Data flow** — an ASCII flow (the catchy data→tile→PMTiles spine), see Step 2.
  - **Serverless execution strategy** — copy the eo-series section's substance (no `spark.conf` tuning outside `set_conf_safe`, `repartition(N, col)` not number-only, no `.cache()` → write Delta, sequential Volume I/O), trimmed to this series.
  - **Key GeoBrix / Databricks functions shown** — `OvertureClient.discover/download/read`; `gbx_st_asmvt`, `gbx_st_asmvt_pyramid`; `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`, `gbx_rst_cog_convert`, `gbx_rst_h3_rastertogridavg`, terrain; `gbx_pmtiles_agg`; `plot_pmtiles`, `plot_cog`, `pmtiles_info`; `StacClient`. Composed with **Databricks-native** spatial (the on-ramp): `st_geomfromwkb`/`st_area`/`st_centroid` for roof metrics (NB01), `h3_longlatash3` for roof density (NB01), `h3_centeraswkb` for H3 solar-suitability cell geometry (NB03).
  - **Gotchas** — PMTiles read is driver-side only (no Spark read); Overture cloud-path read vs HTTP-href fallback; NAIP/3DEP network reachability + SRTM fallback; base64 embed size guard on `plot_pmtiles` (>64 MB → static); Serverless repartition-by-column.
  - **Related resources** — links to the [Helios docs page], [EO-Series], [H3 Rasterize], RasterX/VectorX/VizX/PMTiles API pages.
- [ ] **Step 2:** Embed this ASCII data flow:
  ```text
  San Francisco AOI (one bbox, reused across all three notebooks)
          │
    ┌─────┴───────────────┬─────────────────────────────┐
    ▼                     ▼                             ▼
  Overture buildings    NAIP aerial (helper)         USGS 3DEP DEM (helper)
  (OvertureClient)       │                             │
    │                    ▼ gbx_rst_to_webmercator      ▼ gbx_rst_cog_convert
    ▼ gbx_st_asmvt        │                             │  → COGs + STAC Delta
      + st_asmvt_pyramid  ▼ gbx_rst_xyzpyramid          ▼ slope/aspect/hillshade
    │                    │                             ▼ gbx_rst_xyzpyramid
    ▼ gbx_pmtiles_agg     ▼ gbx_pmtiles_agg             ▼ gbx_pmtiles_agg
  sf_buildings.pmtiles   sf_naip.pmtiles              sf_hillshade.pmtiles
    │                     │                             │
    └─────────────────────┴──────────────┬──────────────┘
                                          ▼
                            plot_pmtiles / plot_cog (inline)
                              → solar site-selection view
  ```
- [ ] **Step 3 (VALIDATION):** `grep -rniE "wave [0-9]+|wave-[0-9]+|subagent|dispatch|SP[0-9]|sub-project" notebooks/examples/helios/README.md` must print nothing (doc-voice). Confirm the three image paths resolve (`ls resources/images/helios-0{1,2,3}.png`). Render-preview the markdown to eyeball tables/flow.
- [ ] **Step 4 (commit):** `git add notebooks/examples/helios/README.md && git commit` — subject `docs(helios): add notebook-series README`; body WHY. Trailer.

---

## Task 6: docs page + sidebar entry

**Files:**
- Create: `docs/docs/notebooks/helios.mdx`
- Modify: `docs/sidebars.js` (add `'notebooks/helios'` to the Notebooks category)

**Interfaces:** The MDX mirrors `docs/docs/notebooks/eo-series.mdx` structure; image refs use `../../../resources/images/helios-0N.png` (same relative depth as eo-series.mdx). Sidebar adds one entry.

- [ ] **Step 1:** Write `docs/docs/notebooks/helios.mdx` mirroring `eo-series.mdx`:
  - Frontmatter: `--- \n sidebar_position: 4 \n title: Helios — Tiling to PMTiles \n ---` (eo-series is position 1; h3-rasterize/xview follow; place Helios after them).
  - `# Helios — Distributed Tiling to PMTiles` + the one-paragraph intro (SF AOI, three layers, solar narrative, on-ramp-to-Databricks-native framing where natural).
  - `:::tip View on GitHub` admonition → `https://github.com/databrickslabs/geobrix/tree/main/notebooks/examples/helios`.
  - `:::info Runs on the lightweight tier (Serverless) by default` admonition (copy the eo-series wording; light `[light,stac,vizx]`, heavyweight option-2 switch, `set_conf_safe`, Execution Tiers link).
  - `:::note` on the PMTiles viewer (driver-side render, base64 in-browser FileSource, >64 MB static fallback) — link the [VizX](../api/vizx) and [PMTiles](../api/pmtiles-functions) pages. Add a one-line note that the notebooks ship with `INTERACTIVE_PLOTS = False` for GitHub-renderable static maps and that readers can set `INTERACTIVE_PLOTS = True` for interactive folium/MapLibre maps.
  - `:::tip` (or a sentence in `## Run order`) — GeoBrix tiling composes with Databricks-native `ST_*`/H3 (the on-ramp): NB01 uses native ST roof area + H3 roof density, NB03 builds a per-H3-cell `solar_score`. Factual, no internal vocabulary.
  - `## Notebooks at a glance` — three `###` subsections with `![...](../../../resources/images/helios-0N.png)` and 3 bullets each (same content as the README highlights).
  - `## Files` table (config_nb + 3 notebooks).
  - `## Prerequisites`, `## Run order`, `## Data flow` (the same ASCII flow as the README), `## Key GeoBrix / Databricks functions shown`, `## Gotchas` — all mirroring eo-series.mdx, trimmed to Helios.
- [ ] **Step 2:** Edit `docs/sidebars.js` — in the `Notebooks` category `items` array (currently `'notebooks/eo-series'`, `'notebooks/xview'`, `'notebooks/h3-rasterize'`), append `'notebooks/helios'`.
- [ ] **Step 3 (VALIDATION):** Doc-voice grep **must be empty**: `grep -rniE "wave [0-9]+|wave-[0-9]+" docs/docs/notebooks/helios.mdx` prints nothing; also `grep -rniE "subagent|dispatch|\bSP[0-9]\b|sub-project" docs/docs/notebooks/helios.mdx` prints nothing. Build-check the docs locally if practical (`gbx:docs:dev` or the docs build) to confirm the MDX parses and the sidebar entry resolves; at minimum confirm `notebooks/helios` matches the new file id and the three image paths exist.
- [ ] **Step 4 (commit):** `git add docs/docs/notebooks/helios.mdx docs/sidebars.js && git commit` — subject `docs(helios): add Helios docs page + sidebar entry`; body WHY. Trailer.

---

## Task 7: Full-series Docker validation + doc-voice sweep + gains capture

**Files:** none new (validation + possible fixups to Tasks 1–6 artifacts; performance corpus files only if a gain is found).

**Interfaces:** End-to-end exercise of the whole series through `%run ./config_nb`.

- [ ] **Step 1 (full-series run):** In the Docker container (started via `start_docker_with_volumes.sh`), run all four notebooks in order so `%run ./config_nb` state threads through:
  ```
  gbx:test:notebooks --path "examples/helios/config_nb.ipynb"       --log helios-all-00.log
  gbx:test:notebooks --path "examples/helios/01. Vector Engine (MVT).ipynb"        --log helios-all-01.log
  gbx:test:notebooks --path "examples/helios/02. Visual Basemap (XYZ).ipynb"       --log helios-all-02.log
  gbx:test:notebooks --path "examples/helios/03. Analytical Core (COG + STAC).ipynb" --log helios-all-03.log
  ```
  Tail each log; assert every cell reports OK and the three `*.pmtiles` archives + the COG exist. NOTE: the cell-by-cell runner does not chain `%run` across separate invocations — if a numbered notebook depends on config_nb globals, the runner must `exec` config_nb first (it handles `%run` by inlining). Confirm the runner inlines `%run ./config_nb`; if it does not, add a `notebooks/tests/examples/` thin pytest harness that execs config_nb then the notebook in one interpreter (mirror an existing harness) — fix the harness, do not work around in the notebook.
- [ ] **Step 2 (doc-voice final sweep):** `grep -rniE "wave [0-9]+|wave-[0-9]+" docs/docs/notebooks/helios.mdx` **must be empty**. Also sweep the notebooks + README: `grep -rniE "wave [0-9]+|subagent|dispatch|\bSP[0-9]\b|sub-project|orchestrator" notebooks/examples/helios/ docs/docs/notebooks/helios.mdx` — empty. Fix any leak inline and amend the owning task's commit (or a small `docs(helios): scrub internal vocabulary` commit).
- [ ] **Step 3 (binding/registered-name confirmation):** Confirm every **GeoBrix** SQL name used in the notebooks exists in `docs/tests-function-info/registered_functions.txt` (`gbx_st_asmvt`, `gbx_st_asmvt_pyramid`, `gbx_rst_to_webmercator`, `gbx_rst_xyzpyramid`, `gbx_rst_cog_convert`, `gbx_rst_h3_rastertogridavg`, `gbx_pmtiles_agg`, and the terrain names) and that the pyrx/pyvx Python wrappers used (`rx.rst_fromcontent`, `rx.rst_to_webmercator`, `rx.rst_cog_convert`, `rx.rst_terrainslope`/etc., `vx.*`) match real bindings. Separately confirm the **Databricks-native** names the on-ramp cells call (`st_geomfromwkb`, `st_area`, `st_centroid`, `st_x`, `st_y`, `h3_longlatash3`, `h3_centeraswkb`) against the Databricks ST / H3 SQL functions reference and `docs/docs/databricks-spatial.mdx` — these are Databricks built-ins, NOT in `registered_functions.txt`, so the binding-parity check does not cover them; verify name + arg order (esp. `h3_longlatash3(lng, lat, res)`) and the geometry encoding the GeoBrix reader emits (WKB vs already-native GEOMETRY). Where a NOTE in Tasks 2–4 flagged an unconfirmed signature (GeoBrix or native), resolve it now and patch the notebook cell to the real signature. No placeholder ships.
- [ ] **Step 4 (capture validated gains — standing practice):** If building/validating any notebook surfaced a tiling-path improvement (e.g. an XYZ/MVT pyramid or `pmtiles_agg` speedup, a Serverless repartition fix, a COG-convert windowing win), capture it per the spec's performance methodology:
  - Create (if absent) `docs/superpowers/performance/README.md` (index) and one pattern file `docs/superpowers/performance/<slug>.md` with: problem → symptom/signature → the fix → applicability matrix (light-similar fns / heavy same+similar fns, verdict recorded even when "not applicable") → evidence/bench numbers → canonical code refs.
  - Add a paired thin pointer memory (slug + one line) `[[linking]]` to that corpus file (keep `MEMORY.md` index entries one line; the file is over the size limit, so do not bloat it).
  - If the gain touches a function classified by execution shape, reflect it in user-facing `docs/docs/api/performance.mdx` and `benchmarking.mdx` per the "bench changes → update docs" rule — kept distinct from the internal corpus.
  - If **no** gain was found, record that verdict in the task commit body (one line) and skip the corpus files.
- [ ] **Step 5 (commit):** If Steps 1–4 produced fixups or corpus files: `git add -A && git commit` — subject `test(helios): validate full series + capture tiling gains` (or `docs(helios): scrub vocabulary + confirm signatures` if no perf file); body: what was validated, any signature corrections, the gains verdict (captured-as `<slug>` / none-found). Trailer.

---

## Self-review against the spec (SP3 + cross-cutting)

- **Coverage:** config_nb spine ✓ (Task 1), NB01 MVT ✓ (Task 2), NB02 XYZ ✓ (Task 3), NB03 COG+STAC ✓ (Task 4), README ✓ (Task 5), helios.mdx + sidebar ✓ (Task 6), full-series Docker validation + doc-voice grep + gains capture ✓ (Task 7), diagrams ✓ (Task 0). One SF AOI ✓; solar narrative ✓; per-notebook data→tile→PMTiles diagram ✓ (Task 0 generates the committed PNGs); ample plotting ✓ (`plot_file`/`plot_cog`/`show_pmtiles` in each NB); series-only helpers in config_nb ✓ (`solar_score`, `finalize_delta`, `show_pmtiles`/`show_cog`/`show_raster`); no SP1/SP2 re-implementation ✓ (consumed only).
- **Type consistency with pinned SP1/SP2 signatures:** `OvertureClient().discover(bbox, themes=...)`, `.download(assets_df, out_dir, *, table=..., validate=..., partitions=...)` returning `theme,type,source,path,...`, `.read(source, theme=, type=, bbox=)` — used verbatim in NB01. `plot_pmtiles(path, ...)`, `plot_cog(path, ...)`, `pmtiles_info(path)` — used verbatim; the toggle helpers use the pinned `plot_pmtiles(path, max_embed_mb=0, ...)` static form from SP2 (where `max_embed_mb=0` forces the static render) and `plot_cog` stays static-only per the SP2 decision. `plot_static`/`plot_interactive` imported from vizx for the `show_raster` toggle.
- **`INTERACTIVE_PLOTS` toggle (Refinement 1):** added to `config_nb` Step 12 (rebuild-control cell, next to `FORCE_REBUILD`), default `False` with the exact required comment. Viz imports (Step 11) add `plot_interactive`. Toggle-aware helpers (`show_pmtiles`/`show_cog`/`show_raster`, Step 17) branch on it — static (`plot_pmtiles(..., max_embed_mb=0)` / `plot_static` / `plot_cog`) by default, interactive (`plot_pmtiles(...)` MapLibre / `plot_interactive` folium) when `True`. NB01 view (Step 14 md + Step 15 `show_pmtiles`) and NB02 view (Step 16 md + Step 17 `show_pmtiles`) and NB03 views (`show_cog`/`show_pmtiles`) all route through the helpers; `plot_file` source-previews are intentionally left static (a raw source raster has no tiled interactive form, noted in NB02 Step 6). Global Constraint + README Run-order line + helios.mdx `:::note` carry the one-line reader instruction.
- **Native ST/H3 injection (Refinement 2) — where, and where NOT:**
  - **NB01 (injected):** native `st_geomfromwkb` + `st_area`/`st_centroid`/`st_x`/`st_y` for roof area + centroid (Step 9c, "available roof space"), and native `h3_longlatash3` to bin roof centroids into an H3 **roof-density** aggregation (Step 9d). On-ramp framing in the intro + recap.
  - **NB03 (injected):** GeoBrix `gbx_rst_h3_rastertogridavg` aggregates slope+aspect onto H3 cells, `solar_score` applied per cell, and native `h3_centeraswkb` gives the native H3 cell geometry (Step 13c → `sf_solar_cells`); the `cellid` is a standard native H3 id that joins NB01's roof-density cells on the same index. On-ramp framing in intro + recap.
  - **NB02 (deliberately NOT):** a pure NAIP raster basemap step — no natural ST/H3 fit, so none was forced (per the "don't force it" instruction).
  - **config_nb AOI (deliberately NOT):** the SF AOI stays a plain bbox tuple — Overture/NAIP/3DEP staging helpers consume `(minx,miny,maxx,maxy)` directly; expressing it as a native ST geometry / H3 cell set would not simplify any downstream cell, so it was left as-is.
- **Native function names flagged for confirmation (Task 7 Step 3):** `st_geomfromwkb`/`st_area`/`st_centroid`/`st_x`/`st_y` are confirmed in-repo (used in `docs/tests/python/api/sql_api.py` etc.); `h3_longlatash3` (arg order lng,lat,res) and `h3_centeraswkb` (+ companions `h3_boundaryaswkb`/`h3_hexring`) are flagged with NOTE blocks to confirm exact name + arg order against the Databricks H3 SQL reference / `databricks-spatial.mdx`. The reader-geometry encoding (WKB vs native GEOMETRY) is flagged too. All native cells carry a "gate behind a capability check, skip with a printed note if native ST/H3 unavailable in the Docker runner" instruction so tiling stays green; binding-parity does NOT cover native built-ins (not in `registered_functions.txt`).
- **Placeholder scan:** every cell has real narrative + real code; the few unconfirmed SQL signatures (GeoBrix and native) are flagged with an explicit "confirm + wire the real signature, no placeholder" NOTE and are resolved in Task 7 Step 3 before ship.
- **Doc voice:** README + mdx avoid internal vocabulary; the native-spatial framing is factual on-ramp language (no marketing); Task 5/6/7 grep gates enforce it (QC `internals-leak`).
- **Validation rigor:** each notebook task ends in a concrete Docker `gbx:test:notebooks` run with asserted artifacts (PMTiles archive exists + `pmtiles_info` parses + plot renders + native-derived tables non-empty / capability-gated), not just "should work."
