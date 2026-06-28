# Design: VizX multi-layer viewers (unified vector / raster / grid)

**Date:** 2026-06-28
**Branch:** `beta/0.4.0`
**Status:** Proposed — awaiting user review before planning.

## Problem

VizX today renders one thing at a time. An audit of the viewers found:

- `plot_static` (matplotlib) already composes layers via `ax=` chaining and reprojects every
  layer to EPSG:3857 — but only for vector/grid; `plot_cog` makes its own figure (no `ax=`).
- `plot_interactive` (folium) renders one geometry set; no overlay parameter.
- `plot_pmtiles` (MapLibre GL) embeds exactly **one** base64 archive → one source → one layer.
- `plot_cog` renders one COG over a contextily basemap.

So the capability the Helios series is built toward — **buildings + NAIP + hillshade overlaid in
one interactive map** — does not exist. Worse, the Helios prose *overstates* it: cells call
`show_pmtiles(one_archive)` and render layers separately, while the prose implies a combined
overlay. That factual gap must be corrected regardless of what we build.

We want in-notebook rendering that is (a) **unified** across vector, raster, and grid, and
(b) **simple for users** even if complex underneath — a "halo" surface the library is remembered
for.

## Goals

- One coherent **`Layer`** abstraction (vector / raster / grid / pmtiles) consumed by two
  renderers: `plot_static` (matplotlib) and `plot_interactive` (MapLibre GL).
- Any reasonable **combination and number** of layers in one view.
- **Consolidate interactive rendering on MapLibre GL**; retire folium (one interactive engine
  that does GeoJSON + raster image + raster/vector tiles + PMTiles).
- A server-less notebook experience that still works on **Serverless**, classic clusters, and as
  a **static thumbnail on GitHub** (committed `.ipynb`).
- Honest, actionable behavior at scale: a defined `>64 MB` strategy, budget-bounded
  simplification, and graceful fallback — **no silent degradation**.

## Non-goals (Phase-2 roadmap, not built here)

- A Databricks **App** with a FastAPI tile server (the unconditional dynamic / indefinite-size
  single-archive path).
- DuckDB as a second spatial engine. GeoBrix's own pyrx/pyvx + tippecanoe + rasterio cover the
  preparation needs; DuckDB is not introduced.
- Any heavy-tier (Scala) or new-Spark-function changes. This is Python `gbx.vizx` only.

---

## Core concept: the `Layer` model

A lightweight dataclass declared with typed constructor helpers (discoverable params per type),
so users write intent, not plumbing:

```python
vector_layer(data, *, geom_col=None, column=None, cmap="viridis",
             fill=True, color=None, width=None, opacity=0.8, label=None)
raster_layer(data, *, band=None, cmap="viridis", opacity=1.0, label=None)         # COG path | array | tile struct
grid_layer(data, *, grid_system, cellid_col=None, column=None, cmap="viridis",
           opacity=0.7, label=None)                                                # H3 / BNG / quadbin
pmtiles_layer(path_or_bytes_or_url, *, style=None, simplify=None, label=None)      # already-tiled archive
```

### Column-naming convention (settled)

One canonical name per concept across the whole VizX surface (beta = no aliases):

- **`geom_col`** — vector geometry column.
- **`cellid_col`** — DGGS cell-id column. `None` → auto-detect via the existing
  `_CELL_COL_CANDIDATES = ("cellid","cell","cell_id","h3","quadbin","bng","index")`
  (matches `cells_as_gdf(cell_col="cellid")`).
- **`column`** — the value to color / symbolize by (choropleth). `plot_static` already uses
  `column`; we keep it (no rename to `value_col`).

`data` may be a Spark DataFrame, a pandas/GeoDataFrame, a Volume path, an array, or bytes,
depending on layer type — each constructor documents what it accepts.

---

## The two renderers

Both accept a single `Layer`, a list of `Layer`s, or (back-compat) a bare DataFrame/path that is
wrapped as one layer. Layers draw in list order (first = bottom).

```python
plot_static(layers, *, basemap=True, basemap_source=None, title=None,
            fig_w=10, fig_h=10, ax=None, ...)
plot_interactive(layers, *, basemap="carto-positron", simplify_tiles_spec=None,
                 max_embed_mb=64, fallback=True, ...)
```

- **`plot_static`** — matplotlib. Each layer reprojected to 3857 and drawn on one `Axes`.
  Requires giving **`plot_cog` an `ax=` parameter** so COGs compose with vector/grid; raster
  layers draw via `imshow`/`rasterio.show` onto the shared axes. Always works (incl. GitHub).
- **`plot_interactive`** — one self-contained **MapLibre GL** HTML page (see below). Folium is
  retired and dropped from the `[vizx]` extra.

### Back-compat wrappers (kept)

`plot_pmtiles`, `plot_cog`, `plot_raster`, `plot_file`, `plot_mask_layers` remain as single-layer
convenience wrappers that build the appropriate `Layer` and delegate. The `show_*` config_nb
wrappers are updated to the new entry points. The single-layer call stays the common case.

---

## MapLibre compositor internals

One HTML page, N sources + N layers, server-less. Per-layer adapter → MapLibre source(s)+layer(s):

| Layer type | MapLibre representation |
|---|---|
| vector / grid (tiny) | inline **GeoJSON** source + fill/line/circle layers |
| vector / grid (larger) | **tippecanoe → PMTiles** (zoom-aware LOD), embedded or simplified |
| raster (COG / array) | georeferenced **image source** (4-corner coords), decimated to `max_px` |
| pmtiles | `pmtiles://` protocol source (base64 `FileSource`, or URL `FetchSource`) |

Default basemap is **CARTO Positron** (hosted raster style), configurable, with a `none` option —
it must work under **normal Serverless conditions** (browser has outbound internet). The current
single-archive `_pmtiles.py` HTML builder generalizes into this multi-source builder;
`_interactive.py` (folium) is replaced by it.

**contextily keeps its place — on the static path.** The interactive engine (MapLibre) uses hosted
basemap tiles; the **static** surface (`plot_static`, `plot_cog`, and the static fallback) stays
matplotlib, and that is where contextily provides the basemap. Complementary, not redundant.

---

## Preparation, the `>64 MB` strategy, and Volume access

`displayHTML` output runs in a sandboxed iframe served from `databricksusercontent.com` — a
**different origin** from the workspace API. Findings that constrain the design:

- **Empirically confirmed (2026-06-28, CLI probe of the AWS workspace Files API):** the gateway
  returns `Access-Control-Allow-Origin: *` and allows the `Authorization` header cross-origin, **but
  its `Access-Control-Allow-Headers` does NOT include `Range`.** pmtiles.js range reads send a
  `Range` header, so the browser **preflight fails on that header** and the request is blocked. So
  the block is not the origin — it is specifically the `Range` request header not being permitted
  cross-origin. The same wall blocks `driver-proxy`. **pmtiles.js cannot range-read a Volume via the
  Files API from the notebook iframe.**
- Copying a Volume archive to a **driver temp path does not help** — the browser cannot read a
  driver-local file; bytes still have to be either embedded in the page or served over a
  CORS-reachable URL the driver can't provide to the iframe.
- Given the Files-API result, the **only** remaining notebook route to indefinite single-archive
  size is a **presigned object-store URL** (range-read straight from S3/ADLS/GCS, bypassing the
  gateway so the *bucket's* CORS governs, not the API gateway's), gated on that bucket's CORS
  allowing the iframe origin **and the `Range` header** — **conditional and spike-gated** (often not
  controllable for UC-managed storage; minting a presigned URL for a managed Volume file is itself
  uncertain). This is what **Spike A** tests conclusively.
- The durable answer for indefinite single-archive streaming is the **Phase-2 App** (same-origin).

### Budget is measured on the *prepared artifact* bytes, not raw input

A 500 MB DataFrame can prepare to a 2 MB GeoJSON / a small tippecanoe PMTiles; a 4 GB DEM
decimates to a small PNG. The ladder for a layer (per layer; the page total is the sum):

```text
1. layer has an explicit CORS-reachable http(s) URL  -> FetchSource (range)        [indefinite size]
2. prepared bytes <= max_embed_mb                    -> embed (FileSource / inline) [<= ~64 MB]
3. simplify_tiles_spec present                       -> simplify to <= budget, embed [NEW]
4. else                                              -> static composite fallback    [always works]
```

- A **finished PMTiles archive is never silently shrunk** — only a streamable URL or the static
  fallback get it past the budget.
- **Every reduction warns, loudly and actionably** (no silent caps).
- Raster is bounded by decimation at prep, so it rarely drives the budget.

### Two complementary scaling axes for "indefinite size", server-less

- **Sharding (spatial axis, already in NB04):** many bounded per-shard archives + `mosaic.json`;
  embed/preview a shard, assemble the mosaic client-side.
- **Simplification (zoom/precision axis, this spec):** one archive simplified across zoom levels
  to a byte budget (`simplify_tiles()`).

They compose (shard *and* simplify).

---

## `simplify_tiles()` and `simplify_tiles_spec`

One engine, one spec, two materialization modes. The spec is plain JSON/dict (consistent with
`grid_conf`), so it is the single source of truth and serializes cleanly.

**Two flavors of the engine (decided): split by input rather than overload one function with two
modes.** Combining "re-tile from source" and "trim an existing archive" in one signature is
confusing, so:

```python
simplify_tiles_from_source(source, *, spec=<dict>, out_path=None) -> bytes | path    # re-tile (tippecanoe / distributed)
simplify_tiles_from_archive(pmtiles_path, *, spec=<dict>, out_path=None) -> bytes | path  # tile-join down-zoom/trim
plot_interactive(layers, *, simplify_tiles_spec=<dict|None>, ...)                    # inline, picks the right flavor by layer input
```

`plot_interactive` routes to the right flavor automatically based on whether the layer carries
source data or an existing archive path. Both flavors consume the **same** `spec`.

### Spec schema

```json
{
  "budget_mb": 64,          // total embed ceiling for the simplified archive
  "min_z": 0,
  "max_z": 10,              // overview ceiling — and the zoom cut-over seam (one knob, not two)
  "tolerance": "auto",      // geometry simplification; "auto" derives per-zoom, or a number
  "drop_densest": true,     // shed least-important features when a tile exceeds budget
  "cluster_distance": null, // optional point clustering (tippecanoe --cluster-distance)
  "keep_attrs": null,       // null = all; a list prunes attributes (a big size lever)
  "raster_max_px": 1024,    // overview downsample ceiling for raster layers
  "effort": "fast"          // "fast" (inline default) | "full" (durable default)
}
```

`max_z` defines both the simplified-overview ceiling and the zoom cut-over seam; there is no
separate `overview_max_z`.

### Default vs per-layer override

`simplify_tiles_spec` on `plot_interactive` is the **default policy**; a `Layer` may carry its own
`simplify=` to override (e.g. `vector_layer(df, simplify={...})`). A layer with no spec that is
under budget embeds as-is.

### Ephemeral vs durable (the tension, resolved by the two entry points)

| | (a) Ephemeral "just let me see it" | (b) Durable "prepare once, reuse" |
|---|---|---|
| Entry | `plot_interactive(layers, simplify_tiles_spec=…)` | `simplify_tiles(source, spec, out_path="/Volumes/…/overview.pmtiles")` |
| Output | driver temp, session-cached by `hash(source, spec)`, GC'd | persistent PMTiles on a Volume |
| Defaults | `effort: "fast"` (favor latency) | `effort: "full"` (favor fidelity) |
| Reuse | this cell / session | many cells / sessions / notebooks / Phase-2 App / external |
| Identity | transient | an ETL artifact — catalog-able, versionable |

The same spec drives both, so a preview you liked is **promoted to durable** by handing that exact
spec to `simplify_tiles(out_path=…)`; the result is then a `pmtiles_layer(path)` both compositors
consume with zero re-prep.

**Guardrails so (a) never masquerades as (b):**
- `plot_interactive`'s inline simplify is best-effort, **cached by `hash(source, spec)`**, and
  **transient**, and it **warns** when it finds itself doing heavy simplification repeatedly,
  nudging the user to `simplify_tiles(out_path=…)`.

### Engine policy (where tippecanoe lands)

tippecanoe ships **pip-installable manylinux wheels** that bundle the binary — gold-standard
budget-aware vector simplification (`--maximum-tile-bytes`, `--drop-densest-as-needed`,
`--cluster-distance`, `--accumulate-attribute`) with PMTiles output. Its expanded role here:

- **Default vector tiler in `prepare()`** (not just simplification): tiny vector → inline GeoJSON;
  larger → tippecanoe → PMTiles with zoom-aware LOD. Simplification is just tippecanoe with a
  budget — not a separate code path.
- **`tile-join`** powers `simplify_tiles()` when the input is an **existing PMTiles** (Volume path):
  down-zoom + budget-trim an already-built archive into a 0–`max_z` overview without re-tiling
  from source.
- **Clustering** (`--cluster-distance`) for dense point/grid layers.

Policy by scale and type:

- **Vector, moderate (driver-local):** tippecanoe.
- **Vector, very large:** GeoBrix **distributed** tiling (pyvx) — fan out, then aggregate a bounded
  overview to the driver. (tippecanoe is single-node and would bottleneck/OOM.)
- **Raster:** rasterio overview downsampling (no tippecanoe).

**Positioning guardrail:** tippecanoe is *VizX plumbing for viz simplification/overviews*. GeoBrix's
own distributed tiling (`gbx_st_asmvt_pyramid`, `gbx_pmtiles_agg`, the Helios pipelines) **remains
the product tiling story** for at-scale, full-fidelity, published deliverables. The two are
complementary, not competing, and the doc/notebooks frame them that way.

Caveats: single-node ceiling (driver), vector-only, **exact-pin + hash** in the `[vizx]` extra,
and **verify the cp312/manylinux wheel on the real Serverless env** (a one-line spike).

---

## Zoom cut-over (contingent on Phase 1.5)

Low zooms (0–`max_z`) are cheap to embed (few tiles, aggressive simplification); the budget
pressure is at high zoom (full detail). So:

- Embed the simplified `min_z..max_z` overview as a `maxzoom=max_z` PMTiles source.
- Stream `max_z+1..N` detail dynamically (a dynamic source with `minzoom=max_z+1`), the
  `moveend`/`zoomend` hook firing only at `z > max_z`.

This **only lights up if the Phase-1.5 spike succeeds**. If it fails, the map is interactive up to
`max_z` (overview embedded) and sends users to static / sharding for detail — still coherent.

---

## Phase 1.5 spike (gating): in-notebook dynamic loading

The only notebook-native bidirectional JS↔Python channel that sidesteps CORS is **ipywidgets /
[AnyWidget](https://anywidget.dev/)** — its comm rides the kernel socket, not a cross-origin HTTP
request. The dynamic loop would be: MapLibre `moveend` → `model.send({bbox, zoom})` → a driver-side
callback prepares viewport data → trait update → JS `model.on("change")` refreshes the map source.

**Spike question:** does the AnyWidget/ipywidgets comm work on **Serverless** (the strategic
target)? Solid on recent classic DBR; Serverless support must be proven before we bank on it.

- **Pass →** build the dynamic high-zoom cut-over onto the embedded overview.
- **Fail →** document honestly; dynamic deferred to the Phase-2 App; Phase-1 stays embed + static
  + cell-driven re-render (a Python-side helper that re-prepares for a new bbox and re-displays).

Caveats even on pass: AnyWidget does not render in a committed `.ipynb` on GitHub (needs a live
kernel) — the static category-2 fallback covers that surface; and it is a meaningfully larger build.

---

## Phasing

- **Phase 1 (build):** `Layer` model + constructors; `plot_static(layers)` (incl. `plot_cog ax=`);
  `plot_interactive(layers)` on MapLibre (folium retired); the `>64 MB` ladder; `simplify_tiles()`
  + `simplify_tiles_spec` (ephemeral/durable modes, tippecanoe/distributed/rasterio engine policy);
  the repo-wide notebook/doc audit + Helios NB02/NB03 rewiring + prose fix.
- **Phase 1.5 (gating spike, run BEFORE planning):** AnyWidget comm on Serverless (Spike B) and the
  presigned-URL CORS/range path (Spike A). **Spike B pass → the dynamic zoom cut-over is built as an
  immediate follow-on within this effort** (not deferred to Phase 2). Spike B fail → documented;
  cut-over degrades to overview-only-interactive + cell-driven re-render. Spike A pass → wire the
  presigned-URL helper into ladder step 1 for indefinite-size Volume archives; fail → indefinite
  single-archive is Phase-2 only.
- **Phase 2 (roadmap only):** Databricks App tile server for unconditional dynamic / indefinite
  single-archive streaming.

> **Spikes run before the plan** (per direction): the CORS Files-API probe is **done** (Range header
> blocked — see above). Spike A (presigned-URL) and Spike B (AnyWidget) are browser/notebook-frontend
> behaviors that require a human run on a live Serverless notebook; harnesses are provided. Their
> results finalize whether the cut-over and the indefinite-size ladder rung are in or out before
> task-by-task planning begins.

---

## Scope

**In:** the `Layer` model + constructors; `plot_static(layers)` (+ `plot_cog ax=`); MapLibre
`plot_interactive(layers)` replacing folium; the `>64 MB` ladder; `simplify_tiles()` +
`simplify_tiles_spec`; docs (`vizx.mdx`) + tests; a **repo-wide audit of every notebook + doc that
calls the VizX functions** (`plot_interactive`/`plot_static`/`plot_pmtiles`/`plot_cog`/`plot_raster`
+ `show_*`) so the folium retirement and the `plot_interactive` signature change don't strand
`eo-series`, `h3-rasterize`, `xview`, or Helios; and the **Helios NB02/NB03 rewiring + prose fix**.

**Out:** the Phase-2 App, FastAPI tile server, DuckDB, on-the-fly server tiling; heavy-tier or
new-Spark-function changes.

---

## Documentation (red-carpet — this is a halo surface)

Rendering data "that doesn't quite fit in a notebook cell" is something many users struggle to
noodle out on their own. That makes this a **halo capability**, and the docs get first-class
treatment, not a reference-table afterthought:

- **A dedicated narrative page** (`docs/docs/api/vizx-layers.mdx` or a prominent expansion of
  `vizx.mdx`) that *teaches the problem and the ladder*: "I have more tile data than a notebook cell
  can hold — here's how to see it anyway." Walk the decision tree (embed → simplify → URL → shard →
  static) in plain language with runnable examples for each rung.
- **Multi-layer worked examples** for both renderers (static composite and interactive MapLibre),
  using real sample data — vector + raster + grid in one map.
- **The ephemeral-vs-durable story made explicit**: when to `plot_interactive(simplify_tiles_spec=…)`
  vs. when to materialize with `simplify_tiles_from_source(out_path=…)` and reuse — with a
  copy-paste "promote your preview to a durable artifact" snippet.
- **Honest scale guidance**: the `>64 MB` behavior, the sharding alternative (link to Helios NB04),
  and the Phase-2 App as the path for truly indefinite single-archive interactivity — so users are
  never surprised by a fallback.
- **Visual-first**: screenshots/GIFs of multi-layer output; a decision-tree diagram (a new
  `resources/images/diagrams/vizx/` asset following the established diagram-generator pattern).
- Doc code is **executable doc-tests** (the repo's single-source rule), so every example is proven.

The audit-and-migrate of existing notebooks/docs (below) is part of this: every VizX usage across
the docs site reads consistently against the new surface.

## Dependencies & supply chain

- **tippecanoe** (PyPI manylinux wheel), **anywidget** (Phase-1.5 spike only), MapLibre GL JS +
  pmtiles.js (vendored/CDN as today). folium **removed** from `[vizx]`.
- All execution-env packages **exact-version + hash-pinned** (`--require-hashes`), per the repo's
  supply-chain rule. Add to the `[vizx]` extra and the CI lock.
- The MapLibre GL JS + pmtiles.js the viewer injects must be **vendored or loaded with Subresource
  Integrity** (`integrity="sha384-…" crossorigin="anonymous"`), not bare CDN `<script>` tags — same
  supply-chain posture as the Python deps. (The spike harnesses use bare CDN for expediency; the
  product must not.)
- **Verify** the tippecanoe cp312/manylinux wheel and ipywidgets comm on the real **Serverless**
  environment before depending on them (two small spikes).

---

## Testing strategy

- **Unit:** each Layer adapter (vector/raster/grid/pmtiles → MapLibre source+layers; → matplotlib
  artist); the `>64 MB` ladder branching (URL / embed / simplify / static) with size-forced cases;
  `simplify_tiles_spec` parsing + default/override resolution; `simplify_tiles()` budget loop
  (asserts output ≤ budget and zoom range honored); the inline cache by `hash(source, spec)`.
- **Multi-layer:** static composite (vector+raster+grid on one axes) and MapLibre composite
  (N sources) render without error and preserve draw order; a POLYGON case (not points-only).
- **Doc-tests:** real code + assertions in `docs/tests/python/`, executed in Docker, feeding
  `vizx.mdx` — multi-layer static and interactive examples; the ephemeral-vs-durable `simplify`
  examples.
- **Spikes (gating, reported, not silently assumed):** tippecanoe Serverless wheel; AnyWidget
  Serverless comm; presigned-Volume-URL CORS.
- **Regression:** existing single-layer `plot_*` calls and the current PMTiles/COG tests stay green;
  the notebook audit confirms every migrated notebook still runs to its config_nb ceiling.

---

## Decisions log (settled in design)

1. Interactive consolidates on **MapLibre GL**; **folium retired**.
2. **Phase-1 only** in this spec; the App is a roadmap section, not built.
3. Column naming: `geom_col` / `cellid_col` / `column`; `plot_static` unchanged; no `value_col`.
4. `plot_pmtiles` / `plot_cog` / `plot_raster` **kept as single-layer wrappers**.
5. `simplify_tiles()` is a standalone function; `simplify_tiles_spec` is a `plot_interactive` param;
   both consume the same spec. `max_z` is the single knob for overview ceiling **and** cut-over seam.
6. Indefinite **single-archive** size in a notebook is **Phase-2** (App); Phase-1 covers scale via
   user-supplied CORS URLs, the spike-gated presigned-URL helper, `simplify_tiles()`, and sharding.
7. tippecanoe is the default **viz** vector tiler/simplifier (driver), with GeoBrix distributed
   tiling as the scale path and the product's tiling story; rasterio overviews for raster.
8. Helios NB02/NB03 rewiring + prose fix and a repo-wide notebook/doc VizX-usage audit are in scope.
9. **Basemap default = CARTO Positron** (hosted), must work under normal Serverless conditions;
   **contextily retained** for the static path.
10. **Dynamic zoom cut-over is an immediate follow-on within this effort** if the AnyWidget spike
    (Spike B) passes — not deferred to Phase 2.
11. **Two flavors of the simplify engine** (`simplify_tiles_from_source` / `simplify_tiles_from_archive`)
    rather than one overloaded function; `plot_interactive` routes by layer input.
12. **Docs get red-carpet treatment** — this is a halo surface (see the Documentation section).
13. **CORS finding is empirical:** the Files API blocks the cross-origin `Range` header → no
    in-notebook Volume range reads via the gateway; presigned-S3 (Spike A) is the only remaining
    in-notebook candidate; the App is the durable answer.

---

## Spikes to run before planning (per direction)

- **CORS Files-API probe — DONE.** Files API: `ACAO: *`, `Authorization` allowed, **`Range` not in
  `Access-Control-Allow-Headers`** → ranged pmtiles.js fetch fails preflight. In-notebook Volume
  range via the gateway is ruled out.
- **Spike A — presigned-S3 URL (CORS + range + render).** Can we mint a browser-reachable presigned
  URL to a Volume archive's backing object, and does a `databricksusercontent.com` iframe
  successfully range-read + render it? Pass → indefinite-size ladder rung 1; fail → Phase-2 only.
- **Spike B — AnyWidget on Serverless (JS↔kernel comm round-trip) + tippecanoe `%pip` install.**
  Pass → dynamic cut-over built as an immediate follow-on; also confirms the tippecanoe
  manylinux/cp312 wheel installs on the Serverless env.

Spikes A and B are browser/notebook-frontend behaviors; harnesses are provided to run on a live
Serverless notebook. Results finalize the plan's scope.
