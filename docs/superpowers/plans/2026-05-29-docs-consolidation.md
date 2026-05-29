# Docs Consolidation + Function Backfill + QC Guards Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Eliminate the Packages-vs-Functions duplication (it already drifted), make the "Functions" pages the single source of truth for function docs, backfill the 15 recently-shipped functions with representative outputs, refresh the rasterx diagram + release notes, and add QC guards so this can't silently rot again.

**Why:** The `docs/docs/packages/*.mdx` pages list functions by category AND the `docs/docs/api/*-functions.mdx` pages list them per-function — two hand-maintained sources → guaranteed drift (neither has the 15 new functions; example outputs are placeholder `...`). Consolidating to one source per package + deterministic QC checks fixes the root cause.

**Architecture:** Merge each `packages/<pkg>.mdx` (concepts) into the top of `api/<pkg>-functions.mdx` (reference), streamlining verbose prose; merge `packages/overview.mdx` into `api/overview.mdx`; delete `docs/docs/packages/`; rename the sidebar "API Reference" category to **"Functions"** and drop the "Packages" category. Then backfill the 15 new functions into the consolidated pages with real outputs, refresh the diagram + release notes, and add QC checks (validated against the single source) + fix the git pre-push hook so QC actually runs.

**Tech Stack:** Docusaurus MDX (`CodeFromTest` component reads `*_sql_example`/`_output` from `docs/tests/python/api/*.py` via raw-loader), `docs/sidebars.js`, the QC judge (`<repo>/.claude/qc-judge/config.json` + checks). Docs build via `gbx:docs:static-build` (Docusaurus `onBrokenLinks` fails the build on dangling links — the link-fix gate). Function/SQL example pipeline in Docker via `gbx:*`.

**Conventions:** Build/verify docs via `gbx:docs:static-build` (FOREGROUND, wait). `gh auth switch --user mjohns-databricks` before push. Run `gbx:lint:python --check` before pushing python (test/example) changes. ASCII-only. Frame all docs by utility (no "Mosaic-faithful" framing). Commit per task.

**Decisions (locked):** section name = **Functions** (drop Packages); **migrate** package concepts but **streamline** verbose prose; **consolidation-first**. Plan-author decisions: diagram = update its hardcoded list + add a QC staleness check (not a full data-driven rewrite — efficient); release-notes QC = **deterministic** (new `gbx_*` names added to `registered_functions.txt` in the push range must appear in `beta-release-notes.mdx`); binary-returning functions show a descriptor output (`[GTiff tile]` / `<WKB geometry>`), scalar/array/WKT/struct show real values.

---

## Phase A — Consolidate Packages → Functions

Each `packages/<pkg>.mdx` has conceptual content (Overview, Key Features, package-specific concepts) + a "Function Categories" listing + Usage Examples. The matching `api/<pkg>-functions.mdx` has the per-function reference. Merge: concepts → top of the functions page (streamlined), keep the per-function reference, drop the duplicated category-listing where it just re-lists functions (keep category *headings* as section organization if useful).

### Task A1: Merge `packages/rasterx.mdx` → `api/rasterx-functions.mdx`
**Files:** read both; edit `docs/docs/api/rasterx-functions.mdx`; (later-deleted) `docs/docs/packages/rasterx.mdx`.
- [ ] **Step 1: Read** both pages fully. Identify CONCEPTUAL content in `packages/rasterx.mdx` not already on the functions page: Overview, Key Features, Tile payload, **VRT Python pixel functions** (setup/trusted-modules — important, keep), and the category structure. Identify pure function-category *listings* that duplicate the functions page.
- [ ] **Step 2:** At the TOP of `api/rasterx-functions.mdx` (after the existing intro/setup), add a streamlined **Overview + Key Features + concepts** section migrated from the package page (Tile payload, VRT pixel functions). Streamline verbose prose; preserve all unique technical content + any `CodeFromTest` examples (carry their imports). Do NOT duplicate per-function reference that already exists below.
- [ ] **Step 3:** Verify the page still imports everything it references (raw-loader imports for any migrated `CodeFromTest`). Do not delete `packages/rasterx.mdx` yet (Task A6 deletes the dir).
- [ ] **Step 4: Commit** `git commit -m "docs(functions): merge RasterX package concepts into rasterx-functions"`

### Task A2: Merge `packages/gridx.mdx` → `api/gridx-functions.mdx`
- [ ] Same pattern. Migrate GridX Overview, Key Features, **BNG Structure / BNG Grid Reference Format / Precision Levels**, Quadbin concepts → top of `api/gridx-functions.mdx`, streamlined. Preserve unique concepts; drop duplicated category listings. Commit `docs(functions): merge GridX package concepts into gridx-functions`.

### Task A3: Merge `packages/vectorx.mdx` → `api/vectorx-functions.mdx`
- [ ] Migrate the VectorX overview + the `gbx_st_asmvt` / `gbx_st_asmvt_pyramid` narrative sections (the package page has fuller MVT examples than the functions page — reconcile: keep the best single version on the functions page). Commit `docs(functions): merge VectorX package concepts into vectorx-functions`.

### Task A4: Merge `packages/pmtiles.mdx` → `api/pmtiles-functions.mdx`
- [ ] Migrate the PMTiles UDAF-vs-DataSource narrative, schema contract, tile-type detection, compression, serving, limits. Commit `docs(functions): merge PMTiles package concepts into pmtiles-functions`.

### Task A5: Merge `packages/overview.mdx` → `api/overview.mdx`
- [ ] Migrate Available Packages, **Package Comparison**, "Choosing the Right Package", **Function Naming Convention** into `api/overview.mdx` (streamlined). Commit `docs(functions): merge packages overview into Functions overview`.

### Task A6: Sidebar + delete Packages + fix internal links + build-verify
**Files:** `docs/sidebars.js`, delete `docs/docs/packages/`, link fixes across `docs/docs/**`.
- [ ] **Step 1:** In `docs/sidebars.js`: remove the entire `Packages` category block; rename the `label: 'API Reference'` category to `label: 'Functions'`. (Keep its items: overview, tile-structure, Function Reference subcategory, scala/python/sql.)
- [ ] **Step 2:** `git rm docs/docs/packages/*.mdx` (the whole dir).
- [ ] **Step 3:** Fix internal links in `docs/docs/**` that point to `packages/<x>` (markdown links like `(../packages/rasterx)`, `(/geobrix/docs/packages/...)`, `(./packages/...)`) → repoint to the corresponding `api/<x>-functions` (or `api/overview` for the packages overview). Grep `docs/docs/` for `packages/` link targets; update each. (Scope to source `.mdx`; ignore any `docs/build/`.)
- [ ] **Step 4: Build-verify** (FOREGROUND, wait): `gbx:docs:static-build` (i.e. `bash scripts/commands/gbx-docs-static-build.sh`). Docusaurus `onBrokenLinks` FAILS the build on any dangling `/packages/...` link — fix every reported broken link until the build is GREEN. This is the definitive link gate.
- [ ] **Step 5: Commit** `git commit -m "docs: retire Packages section, fold into Functions; fix links; sidebar rename"`

---

## Phase B — Backfill the 15 new functions + representative outputs

For each new function: add an MDX reference section to its consolidated Functions page (mirror the existing per-function section format on that page: heading `## <fn>` or `### gbx_<fn>(...)`, description, params, returns, `<CodeFromTest ... functionName="<name>_sql_example" outputConstant="<name>_sql_example_output" />`), AND set a representative `*_sql_example_output` in the example file. Outputs: real values for scalar/array/WKT/struct; `[GTiff tile, 1 band]`-style descriptor for raster tiles; `<WKB POINT/POLYGON ...>` descriptor for binary geometry. Fix `st_triangulate`'s bare-string `_output`.

### Task B1: RasterX new functions → `api/rasterx-functions.mdx`
- [ ] Add sections for `gbx_rst_dtmfromgeoms`, `gbx_rst_dtmfromgeoms_agg`, `gbx_rst_rasterize_agg`, `gbx_rst_frombands_agg` (params/returns/example via CodeFromTest). Set representative `_output` for each in `docs/tests/python/api/rasterx_functions_sql.py` (tiles → `+----+\n|dtm |\n+----+\n|[GTiff tile, 1 band]|\n...` descriptor, not bare `...`). Commit `docs(functions): document rst_dtmfromgeoms(+agg), rst_rasterize_agg, rst_frombands_agg`.

### Task B2: GridX new functions → `api/gridx-functions.mdx`
- [ ] Add sections for `gbx_custom_grid`, `gbx_custom_pointascell`, `gbx_custom_cellaswkb`, `gbx_custom_cellaswkt`, `gbx_custom_centroid`, `gbx_custom_polyfill`, `gbx_custom_kring`, `gbx_quadbin_cellunion_agg`. Representative `_output` in `gridx_functions_sql.py`: `custom_pointascell`→a real cell-id integer; `custom_cellaswkt`→a real `POLYGON ((...))`; `custom_polyfill`/`custom_kring`→a real `[id, id, ...]` array; `custom_grid`→the struct values; `custom_cellaswkb`/`custom_centroid`/`quadbin_cellunion_agg`→`<WKB ...>` descriptor. Commit `docs(functions): document gbx_custom_* + quadbin_cellunion_agg`.

### Task B3: VectorX new functions → `api/vectorx-functions.mdx`
- [ ] Add sections for `gbx_st_triangulate`, `gbx_st_interpolateelevationbbox`, `gbx_st_interpolateelevationgeom`. Representative `_output` in `vectorx_functions_sql.py`: these emit rows of WKB geometries (generators) → `<WKB POLYGON ...>` / `<WKB POINT Z ...>` descriptor (fix `st_triangulate`'s bare `triangle`). Commit `docs(functions): document st_triangulate + st_interpolateelevation{bbox,geom}`.

### Task B4: Regenerate function-info + verify outputs render
- [ ] Run `gbx:docs:function-info` (FOREGROUND, wait) to resync function-info.json with any example edits; `gbx:test:function-info` passes; `gbx:docs:static-build` GREEN (the new sections render, no MDX errors). Commit any regenerated `function-info.json`. `docs(functions): regenerate function-info after backfill`.

---

## Phase C — Diagram + release notes

### Task C1: Refresh RasterX function-categories diagram
**Files:** `resources/images/rasterx-function-categories.py`, regenerated PNG.
- [ ] **Step 1:** Update the script's hardcoded `CARDS_LEFT`/`CARDS_RIGHT` function lists to include the 42 missing rst_ functions (categorize sensibly into existing/added cards) and fix the hardcoded count string (`"65 SQL functions"` → the current count). Keep ASCII.
- [ ] **Step 2:** Regenerate per the script docstring: `python3 resources/images/rasterx-function-categories.py` then the Chrome-headless screenshot to `resources/images/rasterx-function-categories.png`. (Verify the PNG referenced by `docs/docs/api/rasterx-functions.mdx` updates.)
- [ ] **Step 3:** Build-verify the image renders. Commit `docs(images): refresh rasterx function-categories diagram for current function set`.

### Task C2: Update beta release notes
- [ ] Add the new functions to `docs/docs/beta-release-notes.mdx` (v0.4.0 section): a concise entry per capability group — DTM-from-geoms (raster + agg), streaming aggregators (quadbin_cellunion_agg, rst_rasterize_agg, rst_frombands_agg), VectorX TIN (st_triangulate, st_interpolateelevation{bbox,geom}), custom grid (gbx_custom_*). Utility-framed, no Mosaic references. Commit `docs(release-notes): note dtmfromgeoms, streaming aggregators, TIN functions, custom grid`.

---

## Phase D — QC guards + hook fix

Each QC check: add to `<repo>/.claude/qc-judge/config.json` (project config), `command` type, with a backing deterministic script in the repo where logic is non-trivial (like `binding-parity` → `docs/scripts/check-binding-parity.py`). SELF-TEST each (inject a deliberate failure, confirm exit 1, restore).

### Task D1: Q0 — make QC run on terminal pushes (hook fix)
- [ ] The geobrix repo's local `core.hooksPath=.git/hooks` (git-lfs pre-push) overrides the global QC chained hook, so terminal `git push` skips QC. Fix by chaining QC into the existing `.git/hooks/pre-push` (append `~/.claude/qc-judge/qc.py --git-pre-push` AFTER the git-lfs invocation, preserving git-lfs), so both run. This is a LOCAL `.git/hooks` change (not committed). Verify with a dry `git push --dry-run`-style or a no-op push that QC fires. Report the change (no commit — `.git/hooks` is not version-controlled). If chaining is fragile, document the exact manual step for the user instead.

### Task D2: Q1 — every registered function has a Functions-page section
**Files:** `docs/scripts/check-doc-coverage.py` (new), `<repo>/.claude/qc-judge/config.json`.
- [ ] **Step 1:** Write `docs/scripts/check-doc-coverage.py` (stdlib): for each `gbx_*` name in `registered_functions.txt`, verify it (or its bare `<name>` / `*_sql_example` constant) appears as a documented section in the matching `docs/docs/api/<pkg>-functions.mdx` (map prefix → page: `gbx_rst_*`/`gbx_custom_*`→ which page; `gbx_bng_*`/`gbx_quadbin_*`/`gbx_custom_*`→gridx; `gbx_st_*`→vectorx; `gbx_pmtiles_*`→pmtiles). Detection: the function name appears in the page text OR its `outputConstant`/`functionName` is referenced. Exit 1 listing undocumented functions. Negative-test it.
- [ ] **Step 2:** Add a `doc-coverage` command check to the project qc config (`cmd: "[ -f docs/scripts/check-doc-coverage.py ] || exit 0; python3 docs/scripts/check-doc-coverage.py"`, expect_exit 0, severity warn). Confirm it PASSES now (after Phase B). Add a `gbx:test:doc-coverage` command wrapper (optional, mirror `gbx:test:bindings`).
- [ ] **Step 3: Commit** `feat(qc): doc-coverage check — every registered function documented on its Functions page`.

### Task D3: Q2 — flag placeholder-only example outputs
- [ ] Add to `check-doc-coverage.py` (or a sibling) a check that each registered function's `*_sql_example_output` in `docs/tests/python/api/*.py` is NOT placeholder-only (a table whose only data row is `...`/empty, or a bare non-table string). Allow the binary descriptor convention (`[GTiff tile...]`, `<WKB ...>`). Wire into the same/related qc check. Negative-test. Commit `feat(qc): flag placeholder-only SQL example outputs`.

### Task D4: Q3 — rasterx diagram staleness
- [ ] Add `docs/scripts/check-diagram-coverage.py` (or extend): parse the function names listed in `resources/images/rasterx-function-categories.py` and verify they cover all `gbx_rst_*` in `registered_functions.txt` (and the count string matches). Exit 1 on drift. Add a `diagram-coverage` qc command check. Negative-test. Commit `feat(qc): rasterx diagram coverage check`.

### Task D5: Q4 — reliable release-notes check (deterministic)
- [ ] Replace/augment the project's `release-notes-current`: add a `release-notes-functions` command check — for each `gbx_*` name ADDED to `registered_functions.txt` within `$QC_RANGE` (`git diff $QC_RANGE -- docs/tests-function-info/registered_functions.txt | grep '^+gbx_'`), verify it appears in `docs/docs/beta-release-notes.mdx`; exit 1 listing unmentioned new functions. Deterministic (no LLM timeout/leniency). Add to qc config; in the project config, disable the flaky LLM `release-notes-current` (`{"enabled": false}`) in favor of this. Negative-test. Commit `feat(qc): deterministic release-notes-functions check; disable flaky LLM release-notes check`.

---

## Phase E — Full verification + push

- [ ] **Step 1: docs build** — `gbx:docs:static-build` GREEN (no broken links, all sections render).
- [ ] **Step 2: QC self-run** — run each new check's cmd from repo root; all exit 0 on the current tree (doc-coverage, placeholder-output, diagram-coverage, release-notes-functions). Confirm via the qc merge (like binding-parity verification) that they're registered + PASS.
- [ ] **Step 3:** `gbx:test:function-info` pass; `bash scripts/commands/gbx-test-bindings.sh` pass (parity unaffected); `gbx:lint:python --check` clean (example-file edits).
- [ ] **Step 4: Push** (`gh auth switch --user mjohns-databricks`): `git push origin beta/0.4.0`. With the hook fix (D1), QC runs; the new checks gate. Address findings.

---

## Self-review notes (author)
- **Decisions honored:** section renamed Functions, Packages dropped; concepts migrated + streamlined; consolidation-first; diagram hardcoded-list-update + QC check (not full rewrite); deterministic release-notes check; binary-output descriptor convention.
- **Coverage:** consolidation (A) → backfill 15 funcs + real outputs (B) → diagram + release notes (C) → 4 QC checks + hook fix (D) → verify+push (E). The doc-coverage check (Q1) is the durable guard that would have caught the original gap; it's validated to PASS only AFTER Phase B backfills the 15.
- **Risk:** A6 link-fixing is broad — Docusaurus `onBrokenLinks` build failure is the gate (fix until green). Content migration is judgment-heavy (streamline without losing unique concepts) — per-package tasks let a subagent hold one page-pair in context. Diagram regen needs Chrome-headless (local, macOS) — if unavailable in the agent env, regenerate the SVG + report the manual screenshot step.
