# Execution Tiers Docs — Design Spec

**Date:** 2026-05-30 · **Branch:** `pyrx-0.4.0` · **Status:** approved design, pre-implementation.

## Goal

Restructure the GeoBrix documentation so the **lightweight (pyrx)** and **heavyweight (rasterx)** raster paths read as two **execution tiers of one API** — unified where they don't differ, clearly delineated where they do, with the lightweight tier elevated and discoverable. A reader should come away understanding the tradeoffs yet thinking of the two as *mostly compatible / swappable*.

## Terminology (load-bearing)

- Use **"Execution Tiers"** — the two tiers are **Lightweight** and **Heavyweight**.
- **Do NOT** use "runtime" / "runtimes" anywhere in this feature — it collides with **Databricks Runtime (DBR)** and confuses readers. The toggle, comparison page, badges, and identifiers all use *tier* language (`groupId="gbx-tier"`, URL `?tier=lightweight`).

## Framing: everything here is WIP for 0.4.0

Nothing in the 0.4.0 docs is treated as "shipped." Consequences:
- **Do not document interim within-0.4.0-cycle changes.** Release notes, change logs, and similar describe only the **net 0.4.0 end-state vs 0.3.0** — e.g. "0.4.0 adds a lightweight execution tier (pyrx)" — never the step-by-step docs restructuring done this cycle.
- The existing `rasterx-functions.mdx` is **not** sacred shipped content; restructure freely. Client-side redirects for moved pages are a **nice-to-have** (avoid 404s for anyone holding 0.3.0 links), not a hard requirement.

## Chosen approach

Hybrid (Option C) with **full migration now**: build the cross-cutting framing layer *and* migrate the existing raster reference into the new structure in this project.

## 1. Information architecture (sidebar)

```
Getting started
  └─ Choosing an Execution Tier      ← NEW: comparison / elevation page
Function Reference
  ├─ Raster Functions                ← NEW unified page: functions BOTH tiers provide (~52 today)
  ├─ Raster Functions — Heavyweight only   ← NEW: rasterx-only functions (~55 today)
  ├─ GridX Functions                 (heavyweight; gains a Heavyweight tier badge)
  ├─ VectorX Functions               (heavyweight; gains a Heavyweight tier badge)
  └─ PMTiles Functions               (heavyweight; gains a Heavyweight tier badge)
```

`rasterx-functions.mdx` and `pyrx-functions.mdx` both retire: shared functions → **Raster Functions**; rasterx-only functions → **Heavyweight only**; pyrx overview/tradeoffs prose → **Choosing an Execution Tier**.

## 2. The Execution Tier toggle (the "feels swappable" mechanism)

Native Docusaurus synced tabs — **no custom React**:
```mdx
<Tabs groupId="gbx-tier" queryString="tier">
  <TabItem value="lightweight" label="Lightweight (pyrx)"> … </TabItem>
  <TabItem value="heavyweight" label="Heavyweight (rasterx)"> … </TabItem>
</Tabs>
```
Choosing a tier once flips every synced tab site-wide, persists in `localStorage`, and rides in the URL (`?tier=lightweight`) for shareable links. **Used only where the paths truly diverge:** installation, `register`/setup, the import line, and the few functions with per-tier behavior. Everything else (function purpose, parameters, tile struct) is authored once, outside tabs.

## 3. "Choosing an Execution Tier" page (elevation)

- What each tier is; the **one-line swap** — change only the import, keep the **same alias `rx`**, and all downstream code is byte-identical:
  ```python
  from databricks.labs.gbx.rasterx import functions as rx   # Heavyweight tier
  from databricks.labs.gbx.pyrx    import functions as rx   # Lightweight tier — SAME alias `rx`
  # everything below is unchanged across tiers:
  df.select(rx.rst_slope("tile", unit="degrees"))
  ```
  Docs consistently alias **both** tiers as `rx` (never `prx`) — the identical alias is what makes the swap a single-line change. Same `rst_*` names; same `gbx_rst_*` SQL after explicit `register(spark)`.
- A **tradeoffs table**: install (init script + JAR vs `pip install geobrix[pyrx]`), native GDAL (system/PPA vs rasterio-bundled), ARM / serverless / shared-cluster / Lakeflow-SDP support, execution model & performance (JVM-native vs Python-worker UDFs), driver coverage, SQL default-argument behavior, function coverage, **and readers/writers (tier-specific format names — see §8, not a transparent swap).**
- "How to choose" guidance.
- **Elevated via BOTH** the navbar (a top-level link) **and** the docs homepage/landing (a card or callout), so the lightweight tier is discoverable, not buried.

## 4. Unified "Raster Functions" page (both tiers)

- Covers the functions available in **both** tiers (the pyrx-implemented set; grows as pyrx grows).
- Each entry: one shared signature + description; a **tier badge**; per-tier caveats as a short note (e.g. "Lightweight: NumPy reimplementation, not bit-identical to gdaldem"; "SQL: pass all arguments explicitly — no Python defaults").
- Where a runnable example diverges, it's an Execution Tier **tab** pulling the heavyweight snippet from `docs/tests/python/api/rasterx_functions.py` and the lightweight snippet from `docs/tests/python/api/pyrx_functions.py` — preserving the "tests ARE the docs" single-sourcing.
- Top of page: a compact **availability matrix** (functions × Lightweight/Heavyweight, grouped by category, with footnoted caveats).

## 5. "Raster Functions — Heavyweight only" page

- The rasterx functions not (yet) in pyrx (~55: aggregators, H3/quadbin grid, tiling/generators, contour/proximity/viewshed, asformat/cog_convert/buildoverviews, etc.).
- Clearly labeled as **Heavyweight only**. As pyrx implements one, it graduates from this page into the unified Raster Functions page (a documented maintenance step).
- Heavyweight-specific concept prose (e.g. VRT Python pixel functions, the tile-payload invariant) lives here (or in a shared concepts area where genuinely shared).

## 6. Tier badges

A small reusable MDX/React component (e.g. `<Tier both/>`, `<Tier heavy/>`, `<Tier light/>`) rendering compact colored pills, with minimal CSS. Applied to:
- function entries on the Raster Functions and Heavyweight-only pages,
- the availability matrix,
- the GridX / VectorX / PMTiles pages (a **Heavyweight** badge near the top so their tier is unambiguous).

## 7. Release notes (0.4.0 end-state only)

Add/adjust a single `docs/docs/beta-release-notes.mdx` entry framed as 0.4.0 vs 0.3.0: **"0.4.0 introduces a lightweight execution tier (pyrx) — the raster API on pure Python + rasterio, no JAR/native GDAL, for serverless/shared/ARM/SDP."** No entries about the interim docs restructuring.

## 8. Readers & Writers (tier-specific, NOT a transparent swap)

Unlike the function API (same `rst_*` names across tiers), readers and writers are intentionally **kept separate by name** — the GeoBrix wheel is installed and used *both* with and without the JAR, so the `format(...)` string must make the tier unambiguous:

- **Heavyweight:** Scala DataSourceV2 readers/writers requiring the JAR — `spark.read.format("gtiff_gdal")`, `format("gdal")`, the OGR vector readers, the `gdal` writer, etc.
- **Lightweight:** `*_pyrx`-suffixed readers/writers built on the **PySpark Python Data Source API** (https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) — e.g. `spark.read.format("gtiff_pyrx")` — which work with **no JAR and no init script**.

Docs treatment: the Readers/Writers pages use the same Execution Tier **tabs**, but here the tab *content genuinely differs* (the format name itself changes per tier) — this is a clarity choice, not a swap. The "Choosing an Execution Tier" tradeoffs table notes that readers/writers are selected per tier by format name (`*_pyrx` vs `*_gdal`/OGR).

**Current state vs direction (be honest in the docs):** the lightweight tier today ingests rasters via Spark's built-in `binaryFile` reader + `rx.rst_fromcontent(content, driver)`; the dedicated `*_pyrx` Python-Data-Source readers/writers are a **separate, forthcoming implementation** (not part of this docs project). The docs present the `binaryFile` + `rst_fromcontent` path as today's lightweight ingest and describe `*_pyrx` as the direction — without claiming `gtiff_pyrx` exists before it does.

## Mechanics, constraints & risks

- **Native tabs + a small badge component + (optional) `@docusaurus/plugin-client-redirects`** — no heavy custom code; no global navbar mode-switch (synced tabs deliver the persistent global feel at far lower risk).
- **Hard constraint — doc-coverage QC:** the `doc-coverage` check asserts every one of the 154 registered functions stays documented on a page. The migration must land each raster function on either *Raster Functions* or *Heavyweight only* (none dropped); this is the migration's acceptance test. Run `gbx:test:*-docs` / the doc-coverage check after migration.
- **internals-leak QC + docs voice:** no "wave N", no internal process vocabulary anywhere under `docs/docs/`.
- **Re-alias lightweight examples:** existing pyrx docs/examples (and `docs/tests/python/api/pyrx_functions.py`) currently import `as prx`; migrate them to `as rx` so the swap message holds (downstream code identical across tiers).
- **Biggest effort/risk:** migrating the comprehensive raster reference (107 raster fns) without losing content or coverage, and reconciling the two pages' doc-test imports. The implementation plan stages it: scaffolding (tier tabs, badge component, comparison page) → unified shared page → heavyweight-only page → GridX/VectorX/PMTiles badges → Readers/Writers tier tabs (distinct `*_pyrx`/`*_gdal` format names per §8) → re-alias lightweight examples to `rx` → release-note entry → retire old pages (+ optional redirects) → verify doc-coverage + lint + internals-leak.

## Out of scope

- Implementing additional pyrx functions (separate track).
- A custom global navbar mode-switch (synced tabs suffice).
- Restructuring GridX/VectorX/PMTiles content beyond adding a Heavyweight tier badge.
- Versioned-docs or multi-instance-docs machinery.

## Acceptance criteria

1. A reader can pick an Execution Tier once and have it persist across pages (synced tabs).
2. The lightweight tier is reachable from both the navbar and the homepage.
3. Every shared raster function appears once on **Raster Functions** with a tier badge; every heavyweight-only function appears on **Heavyweight only**; GridX/VectorX/PMTiles show a Heavyweight badge.
4. `doc-coverage`, lint, and internals-leak checks pass; no "runtime" terminology and no "wave N" appear in the new/changed docs.
5. Release notes describe only the 0.4.0 end-state (lightweight tier added), not interim churn.
6. Both tiers are aliased `rx` in all examples (never `prx`); the swap is demonstrably a single import-line change.
7. Readers/Writers are shown per tier with distinct format names (`*_pyrx` via the PySpark Python Data Source API vs `*_gdal`/OGR), not as a transparent swap; lightweight ingest today is documented as `binaryFile` + `rst_fromcontent`.

---
*Approved via brainstorming on 2026-05-30. Next: writing-plans for the staged implementation.*
