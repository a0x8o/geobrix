# Readers & Writers Tabbed Tiers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the Readers & Writers docs by **format** (not tier): one page per format with synced **Lightweight (default/first) / Heavyweight** tabs; single-tier formats stay single pages with a note.

**Architecture:** Merge each light+heavy page-pair into one format-named `.mdx` using Docusaurus `<Tabs groupId="gbx-tier" queryString="tier">` (the convention already used by quick-start / the overviews, so the tier choice syncs site-wide). The heavy/light bodies move verbatim into their tabs — the imported example code (and the executable doc-tests) are unchanged. Single-tier (vector) pages get a "no lightweight equivalent yet" note. Sidebars regroup under Readers → General/Named and Writers → General/Named.

**Tech Stack:** Docusaurus MDX, `@theme/Tabs`/`@theme/TabItem`, the repo's `CodeFromTest` component, the `gbx:test:python-docs` doc-test harness, `gbx:docs:*` build commands.

**Reference spec:** `docs/superpowers/specs/2026-06-12-readers-writers-tabbed-tiers-design.md`

---

## Conventions for this plan

- **Tabs convention (match the existing site):** every tier tab group uses
  `<Tabs groupId="gbx-tier" queryString="tier">` with **`<TabItem value="lightweight" …>` FIRST** then `<TabItem value="heavyweight" …>`. The shared `groupId` + `queryString` sync the choice across all pages and the URL; **lightweight first = lightweight default** on a fresh visit. (The spec wrote `groupId="tier"`/`light`/`heavy`; we use the established `gbx-tier`/`lightweight`/`heavyweight` so the new pages sync with quick-start and the overviews.)
- **Tab labels** carry the format/engine name: `label="Lightweight · raster_gbx"`, `label="Heavyweight · gdal"`. (Labels may differ per page; the `value` is what syncs.)
- **"Move the body verbatim"** means: take everything in the source page **after** its frontmatter and `import` lines, and paste it unchanged inside the target `<TabItem>` — keep every `<CodeFromTest … />` call and its props exactly. Do NOT edit the imported example `.py`/`.scala` files.
- **No redirects** (beta/WIP — old URLs may break).
- **Commits:** before each `git commit` run `chmod -R u+rwX .git/objects`; trailer is exactly `Co-authored-by: Isaac` (repo convention; a security linter may warn — ignore it; never a human name); subjects ≤72 chars. No push (the operator pushes at the end).
- **Docs build/test:** `bash scripts/commands/gbx-docs-start.sh` (or `gbx:docs:dev`) for a local build; `gbx:test:python-docs --path readers/` / `--path writers/` for doc-tests (Docker). Run doc-test/build steps via a Task subagent (they touch Docker / take minutes).
- **MDX gotcha:** a `<TabItem>` body that starts with a Markdown heading or import must have a blank line after the `<TabItem …>` tag and before `</TabItem>`. Keep the existing pages' blank-line spacing when moving bodies.

## File map

**Create (merged, tabbed):**
- `docs/docs/readers/raster.mdx` ← `raster_gbx.mdx` (light) + `gdal.mdx` (heavy)
- `docs/docs/readers/geotiff.mdx` ← `gtiff_gbx.mdx` (light) + `gtiff.mdx` (heavy)
- `docs/docs/writers/raster.mdx` ← `raster_gbx.mdx` (light) + `gdal.mdx` (heavy)
- `docs/docs/writers/geotiff.mdx` ← `gtiff_gbx.mdx` (light) + new heavy tab (`WRITE_GTIFF_GDAL` from `gdal_examples.py`)

**Modify in place (merge into existing id):**
- `docs/docs/writers/pmtiles.mdx` ← add light `pmtiles_gbx` tab (from `pmtiles_gbx.mdx`) above the existing heavy body

**Delete (folded into the above):**
- `docs/docs/readers/raster_gbx.mdx`, `docs/docs/readers/gdal.mdx`, `docs/docs/readers/gtiff_gbx.mdx`, `docs/docs/readers/gtiff.mdx`
- `docs/docs/writers/raster_gbx.mdx`, `docs/docs/writers/gdal.mdx`, `docs/docs/writers/gtiff_gbx.mdx`, `docs/docs/writers/pmtiles_gbx.mdx`

**Modify (labels + note):** `docs/docs/readers/{ogr,shapefile,geojson,geopackage,filegdb}.mdx`

**Modify (light-first + relink):** `docs/docs/readers/overview.mdx`, `docs/docs/writers/overview.mdx`

**Modify (light-first sweep):** `docs/docs/installation.mdx`, `docs/docs/api/raster-functions.mdx` (+ confirm `quick-start.mdx` is already light-first)

**Modify:** `docs/sidebars.js` (Readers & Writers block), plus cross-links across `docs/docs/**`.

---

### Task 1: Merged `readers/raster.mdx` (Raster reader — light + heavy tabs)

**Files:**
- Create: `docs/docs/readers/raster.mdx`
- Read (sources to fold): `docs/docs/readers/raster_gbx.mdx`, `docs/docs/readers/gdal.mdx`

- [ ] **Step 1: Create the merged page scaffold**

Create `docs/docs/readers/raster.mdx` with this exact head, then fill the two tab bodies per Steps 2–3:

```mdx
---
sidebar_position: 1
sidebar_label: Raster
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import rasterGbxExamples from '!!raw-loader!../../tests/python/readers/raster_gbx_read_examples.py';
import gdalExamples from '!!raw-loader!../../tests/python/readers/gdal_examples.py';
import gdalScala from '!!raw-loader!../../tests/scala/readers/GDALExamples.scala';

# Raster Reader

Read rasters into the shared `(source, tile)` schema. GeoBrix offers two
interchangeable tiers: a **lightweight** pure-Python/PySpark reader (`raster_gbx`,
`rasterio`-backed, JAR-free, Serverless-safe) and a **heavyweight** GDAL-backed
reader (`gdal`). They emit the same schema, so swapping is a one-line
`format(...)` change — see [Choosing an Execution Tier](../api/execution-tiers#the-one-line-swap).

> The heavyweight `gdal` reader supports the full set of GDAL drivers (NetCDF,
> HDF5, COG, …); the lightweight reader covers the common raster path. The pairing
> is a corresponding *general raster reader* per tier, not a feature-identical one.

<Tabs groupId="gbx-tier" queryString="tier">
<TabItem value="lightweight" label="Lightweight · raster_gbx">

<!-- LIGHT BODY (Step 2) -->

</TabItem>
<TabItem value="heavyweight" label="Heavyweight · gdal">

<!-- HEAVY BODY (Step 3) -->

</TabItem>
</Tabs>
```

- [ ] **Step 2: Fill the Lightweight tab**

Replace `<!-- LIGHT BODY (Step 2) -->` with the **body of `docs/docs/readers/raster_gbx.mdx` moved verbatim** — everything after its `import` lines (i.e. from `## Register` onward, including the `:::note Lightweight readers and writers are not auto-registered` admonition, `## Read (catch-all)`, `## Options`, and `## Performance vs the heavyweight reader`). Keep all `<CodeFromTest … />` calls exactly. Drop its old `# Lightweight Raster Reader (\`raster_gbx\`)` H1 (the page H1 + tab already establish context); start the tab body at `## Register`. Re-point any in-body relative links per Task 10 (do not worry about them now).

- [ ] **Step 3: Fill the Heavyweight tab**

Replace `<!-- HEAVY BODY (Step 3) -->` with the **body of `docs/docs/readers/gdal.mdx` moved verbatim** — everything after its `import` lines (drop its `# GDAL Reader` H1; start at its first section). Keep all `<CodeFromTest … />` and `gdalScala` usages exactly.

- [ ] **Step 4: Delete the two source pages**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
git rm docs/docs/readers/raster_gbx.mdx docs/docs/readers/gdal.mdx
```

- [ ] **Step 5: Sanity-check the merged page parses (imports + tabs balanced)**

Run:
```bash
grep -c "<TabItem" docs/docs/readers/raster.mdx   # expect 2
grep -c "</TabItem>" docs/docs/readers/raster.mdx  # expect 2
grep -c "raw-loader" docs/docs/readers/raster.mdx  # expect 3 (rasterGbx, gdal py, gdal scala)
```
Expected: `2`, `2`, `3`.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/readers/raster.mdx docs/docs/readers/raster_gbx.mdx docs/docs/readers/gdal.mdx
git commit -m "docs(readers): merge raster_gbx + gdal into tabbed Raster reader

Co-authored-by: Isaac"
```

---

### Task 2: Merged `readers/geotiff.mdx` (GeoTIFF reader — light + heavy tabs)

**Files:**
- Create: `docs/docs/readers/geotiff.mdx`
- Read (sources): `docs/docs/readers/gtiff_gbx.mdx`, `docs/docs/readers/gtiff.mdx`

- [ ] **Step 1: Create the merged page scaffold**

```mdx
---
sidebar_position: 1
sidebar_label: GeoTIFF
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import rasterGbxExamples from '!!raw-loader!../../tests/python/readers/raster_gbx_read_examples.py';
import gtiffExamples from '!!raw-loader!../../tests/python/readers/gtiff_examples.py';
import gtiffScala from '!!raw-loader!../../tests/scala/readers/GTiffExamples.scala';

# GeoTIFF Reader

Read GeoTIFFs into the shared `(source, tile)` schema. The **lightweight**
`gtiff_gbx` reader (the `raster_gbx` catch-all with the GeoTIFF driver preset,
`rasterio`-backed, JAR-free) and the **heavyweight** `gtiff_gdal` reader are
interchangeable — see [Choosing an Execution Tier](../api/execution-tiers#the-one-line-swap).

<Tabs groupId="gbx-tier" queryString="tier">
<TabItem value="lightweight" label="Lightweight · gtiff_gbx">

<!-- LIGHT BODY -->

</TabItem>
<TabItem value="heavyweight" label="Heavyweight · gtiff_gdal">

<!-- HEAVY BODY -->

</TabItem>
</Tabs>
```

- [ ] **Step 2: Fill the Lightweight tab** with the body of `docs/docs/readers/gtiff_gbx.mdx` moved verbatim (everything after its imports; drop its H1). Keep the `<CodeFromTest … functionName="READ_GTIFF_GBX" …/>` call. Replace its "Register the lightweight DataSources first (see …)" link target per Task 10.

- [ ] **Step 3: Fill the Heavyweight tab** with the body of `docs/docs/readers/gtiff.mdx` moved verbatim (after imports; drop its `# GeoTIFF Reader` H1). Keep all `gtiffExamples`/`gtiffScala` `<CodeFromTest>` calls.

- [ ] **Step 4: Delete sources**

```bash
git rm docs/docs/readers/gtiff_gbx.mdx docs/docs/readers/gtiff.mdx
```

- [ ] **Step 5: Sanity-check**

```bash
grep -c "<TabItem" docs/docs/readers/geotiff.mdx   # 2
grep -c "</TabItem>" docs/docs/readers/geotiff.mdx # 2
```
Expected: `2`, `2`.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/readers/geotiff.mdx docs/docs/readers/gtiff_gbx.mdx docs/docs/readers/gtiff.mdx
git commit -m "docs(readers): merge gtiff_gbx + gtiff_gdal into tabbed GeoTIFF reader

Co-authored-by: Isaac"
```

---

### Task 3: Merged `writers/raster.mdx` (Raster writer — light + heavy tabs)

**Files:**
- Create: `docs/docs/writers/raster.mdx`
- Read (sources): `docs/docs/writers/raster_gbx.mdx`, `docs/docs/writers/gdal.mdx`

- [ ] **Step 1: Create the merged page scaffold**

```mdx
---
sidebar_position: 1
sidebar_label: Raster
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import rasterGbxWrite from '!!raw-loader!../../tests/python/writers/raster_gbx_write_examples.py';
import gdalWriteExamples from '!!raw-loader!../../tests/python/writers/gdal_examples.py';
import gdalWriteScala from '!!raw-loader!../../tests/scala/writers/GDALWriteExamples.scala';

# Raster Writer

Write raster tiles from the shared `(source, tile)` schema. The **lightweight**
`raster_gbx` writer (`rasterio`-backed, JAR-free, supports `overwrite`) and the
**heavyweight** GDAL-backed `gdal` writer (append-only) take the same schema — see
[Choosing an Execution Tier](../api/execution-tiers#the-one-line-swap).

<Tabs groupId="gbx-tier" queryString="tier">
<TabItem value="lightweight" label="Lightweight · raster_gbx">

<!-- LIGHT BODY -->

</TabItem>
<TabItem value="heavyweight" label="Heavyweight · gdal">

<!-- HEAVY BODY -->

</TabItem>
</Tabs>
```

- [ ] **Step 2: Fill the Lightweight tab** with the body of `docs/docs/writers/raster_gbx.mdx` moved verbatim (after imports; drop its H1; start at `## Write raster tiles`). Keep its `:::note Register before writing` admonition and all `<CodeFromTest>` calls.

- [ ] **Step 3: Fill the Heavyweight tab** with the body of `docs/docs/writers/gdal.mdx` moved verbatim (after imports; drop its `# GDAL Writer` H1). Keep all `gdalWriteExamples`/`gdalWriteScala` `<CodeFromTest>` calls.

- [ ] **Step 4: Delete sources**

```bash
git rm docs/docs/writers/raster_gbx.mdx docs/docs/writers/gdal.mdx
```

- [ ] **Step 5: Sanity-check**

```bash
grep -c "<TabItem" docs/docs/writers/raster.mdx    # 2
grep -c "raw-loader" docs/docs/writers/raster.mdx  # 3
```
Expected: `2`, `3`.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/writers/raster.mdx docs/docs/writers/raster_gbx.mdx docs/docs/writers/gdal.mdx
git commit -m "docs(writers): merge raster_gbx + gdal into tabbed Raster writer

Co-authored-by: Isaac"
```

---

### Task 4: Merged `writers/geotiff.mdx` (GeoTIFF writer — light + heavy tabs; fills the gtiff_gdal-writer doc gap)

**Files:**
- Create: `docs/docs/writers/geotiff.mdx`
- Read (sources): `docs/docs/writers/gtiff_gbx.mdx`; heavy example `WRITE_GTIFF_GDAL` lives in `docs/tests/python/writers/gdal_examples.py` (already exercised by `test_gdal_examples.py::test_write_gdal`).

- [ ] **Step 1: Create the merged page**

```mdx
---
sidebar_position: 2
sidebar_label: GeoTIFF
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import rasterGbxWrite from '!!raw-loader!../../tests/python/writers/raster_gbx_write_examples.py';
import gdalWriteExamples from '!!raw-loader!../../tests/python/writers/gdal_examples.py';

# GeoTIFF Writer

Write GeoTIFF tiles. The **lightweight** `gtiff_gbx` writer (the `raster_gbx`
writer with the GeoTIFF driver forced) and the **heavyweight** `gtiff_gdal` writer
(the GDAL writer restricted to the GTiff driver) are interchangeable — see
[Choosing an Execution Tier](../api/execution-tiers#the-one-line-swap).

<Tabs groupId="gbx-tier" queryString="tier">
<TabItem value="lightweight" label="Lightweight · gtiff_gbx">

<!-- LIGHT BODY -->

</TabItem>
<TabItem value="heavyweight" label="Heavyweight · gtiff_gdal">

`gtiff_gdal` is the GDAL writer restricted to the GeoTIFF driver — it reads and
writes `.tif` rasters and is **append-only** (use the lightweight writer for
`overwrite`). It takes the same `(source, tile)` schema as the other writers.

<CodeFromTest code={gdalWriteExamples} language="python" functionName="WRITE_GTIFF_GDAL"
  source="docs/tests/python/writers/gdal_examples.py"
  testFile="docs/tests/python/writers/test_gdal_examples.py" />

See the [Raster Writer → Heavyweight](./raster?tier=heavyweight) tab for the full
GDAL-writer options (`path` / `nameCol` / `ext`, format & compression from
`tile.metadata`).

</TabItem>
</Tabs>
```

- [ ] **Step 2: Fill the Lightweight tab** with the body of `docs/docs/writers/gtiff_gbx.mdx` moved verbatim (after imports; drop its H1). Keep its `<CodeFromTest … functionName="WRITE_GTIFF_GBX" …/>` call. Re-point its `./raster_gbx` links per Task 10.

- [ ] **Step 3: Delete the light source page**

```bash
git rm docs/docs/writers/gtiff_gbx.mdx
```

- [ ] **Step 4: Sanity-check + confirm the heavy example exists**

```bash
grep -c "<TabItem" docs/docs/writers/geotiff.mdx           # 2
grep -n "WRITE_GTIFF_GDAL" docs/tests/python/writers/gdal_examples.py  # present
```
Expected: `2`, and a match for `WRITE_GTIFF_GDAL`.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/writers/geotiff.mdx docs/docs/writers/gtiff_gbx.mdx
git commit -m "docs(writers): tabbed GeoTIFF writer (documents gtiff_gdal heavy)

Co-authored-by: Isaac"
```

---

### Task 5: Merge light PMTiles into `writers/pmtiles.mdx`

The heavy PMTiles writer page (`writers/pmtiles.mdx`) keeps its id; add a Lightweight tab from `writers/pmtiles_gbx.mdx` above the existing heavy body.

**Files:**
- Modify: `docs/docs/writers/pmtiles.mdx`
- Read (source): `docs/docs/writers/pmtiles_gbx.mdx`

- [ ] **Step 1: Read both files** and note: `pmtiles_gbx.mdx` imports `pmtilesEx` from `'!!raw-loader!../../tests/python/writers/pmtiles_gbx_examples.py'`; check what (if any) raw-loader imports `pmtiles.mdx` (heavy) currently has.

- [ ] **Step 2: Rewrite `docs/docs/writers/pmtiles.mdx`** to the tabbed form:

```mdx
---
sidebar_position: 3
sidebar_label: PMTiles
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import pmtilesEx from '!!raw-loader!../../tests/python/writers/pmtiles_gbx_examples.py';
<!-- plus any raw-loader import(s) the existing heavy pmtiles.mdx body uses -->

# PMTiles Writer

Package a tile pyramid (`(z, x, y, bytes)`) into [PMTiles](https://docs.protomaps.com/pmtiles/)
archives. The **lightweight** `pmtiles_gbx` writer (pure-Python, Serverless-safe,
distributed spatial sharding) and the **heavyweight** `pmtiles` writer take the
same input and produce decoded-tile-identical archives (verified in the
[benchmark](../api/benchmarking#results--tiled-output-pmtiles-writer)).

<Tabs groupId="gbx-tier" queryString="tier">
<TabItem value="lightweight" label="Lightweight · pmtiles_gbx">

<!-- LIGHT BODY: body of pmtiles_gbx.mdx after its imports (drop its H1) -->

</TabItem>
<TabItem value="heavyweight" label="Heavyweight · pmtiles">

<!-- HEAVY BODY: current body of pmtiles.mdx after its frontmatter/H1 -->

</TabItem>
</Tabs>
```
Move the `pmtiles_gbx.mdx` body (after its imports, drop its H1) into the Lightweight tab, and the **current** `pmtiles.mdx` body (after its frontmatter + `# PMTiles Writer` H1) into the Heavyweight tab. Hoist any raw-loader import the heavy body needs into the import block.

- [ ] **Step 3: Delete the light source page**

```bash
git rm docs/docs/writers/pmtiles_gbx.mdx
```

- [ ] **Step 4: Sanity-check**

```bash
grep -c "<TabItem" docs/docs/writers/pmtiles.mdx   # 2
grep -n "pmtiles_gbx_examples" docs/docs/writers/pmtiles.mdx  # present
```
Expected: `2`, present.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/writers/pmtiles.mdx docs/docs/writers/pmtiles_gbx.mdx
git commit -m "docs(writers): tabbed PMTiles writer (pmtiles_gbx + heavy pmtiles)

Co-authored-by: Isaac"
```

---

### Task 6: Single-tier reader pages — format labels + "no lightweight equivalent" note

**Files:**
- Modify: `docs/docs/readers/ogr.mdx`, `shapefile.mdx`, `geojson.mdx`, `geopackage.mdx`, `filegdb.mdx`

- [ ] **Step 1: Update `sidebar_label` on each** to the format name (drop the `OGR` engine suffix added on 2026-06-11):

| File | new `sidebar_label` |
|---|---|
| `readers/ogr.mdx` | `Vector` |
| `readers/shapefile.mdx` | `Shapefile` |
| `readers/geojson.mdx` | `GeoJSON` |
| `readers/geopackage.mdx` | `GeoPackage` |
| `readers/filegdb.mdx` | `GeoDatabase` |

Edit each frontmatter, e.g. `readers/ogr.mdx`:
```
sidebar_label: OGR
```
→
```
sidebar_label: Vector
```
(and the analogous change for the other four: `Shapefile OGR`→`Shapefile`, `GeoJSON OGR`→`GeoJSON`, `GeoPackage OGR`→`GeoPackage`, `FileGDB OGR`→`GeoDatabase`).

- [ ] **Step 2: Add the lightweight-equivalent note** to each page, immediately after its H1 line. Use this admonition (adjust the format word per page):

```
:::note No lightweight equivalent yet
This vector format does not have a lightweight (`pyvx`) reader yet — it is planned
with the light vector tier. Use the heavyweight reader documented below.
:::
```

- [ ] **Step 3: Verify**

```bash
grep -n "sidebar_label" docs/docs/readers/ogr.mdx docs/docs/readers/shapefile.mdx docs/docs/readers/geojson.mdx docs/docs/readers/geopackage.mdx docs/docs/readers/filegdb.mdx
grep -rc "No lightweight equivalent yet" docs/docs/readers/ogr.mdx docs/docs/readers/shapefile.mdx docs/docs/readers/geojson.mdx docs/docs/readers/geopackage.mdx docs/docs/readers/filegdb.mdx
```
Expected: the five new labels; `1` note in each file.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/readers/ogr.mdx docs/docs/readers/shapefile.mdx docs/docs/readers/geojson.mdx docs/docs/readers/geopackage.mdx docs/docs/readers/filegdb.mdx
git commit -m "docs(readers): format labels + no-lightweight-yet note on vector pages

Co-authored-by: Isaac"
```

---

### Task 7: Overviews — light-first + relink to merged pages

**Files:**
- Modify: `docs/docs/readers/overview.mdx`, `docs/docs/writers/overview.mdx`

- [ ] **Step 1: Flip both overviews to light-first.** Each currently has `<TabItem value="heavyweight" …>` before `<TabItem value="lightweight" …>`. **Reorder so the `lightweight` TabItem comes first** (move the whole `<TabItem value="lightweight" …> … </TabItem>` block above the `heavyweight` one). Do not change the tab contents yet beyond reordering.

- [ ] **Step 2: Repoint format links + tables** in both overviews to the new page ids:
  - `readers/raster_gbx` and `readers/gdal` → `readers/raster`
  - `readers/gtiff_gbx` and `readers/gtiff` → `readers/geotiff`
  - `writers/raster_gbx` and `writers/gdal` → `writers/raster`
  - `writers/gtiff_gbx` → `writers/geotiff`
  - `writers/pmtiles_gbx` → `writers/pmtiles`
  - vector readers (`ogr`/`shapefile`/`geojson`/`geopackage`/`filegdb`) unchanged.
  In each overview's reader/writer **table**, collapse the separate light/heavy rows for raster & GeoTIFF into one row per format that links to the merged page (the tier is now a tab, not a page).

- [ ] **Step 3: Verify no dangling links to deleted pages** remain in the overviews:
```bash
grep -nE "raster_gbx|writers/gdal|readers/gdal|gtiff_gbx|readers/gtiff[^a-z]|pmtiles_gbx" docs/docs/readers/overview.mdx docs/docs/writers/overview.mdx ; echo "exit:$?"
```
Expected: no matches (`exit:1`).

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/readers/overview.mdx docs/docs/writers/overview.mdx
git commit -m "docs(readers/writers): light-first overviews relinked to merged pages

Co-authored-by: Isaac"
```

---

### Task 8: Light-first sweep for the remaining `gbx-tier` tab groups

**Files:**
- Modify: `docs/docs/installation.mdx`, `docs/docs/api/raster-functions.mdx`
- Verify only: `docs/docs/quick-start.mdx` (already light-first)

- [ ] **Step 1: For each `<Tabs groupId="gbx-tier" …>` in `installation.mdx` and `raster-functions.mdx`, ensure the `<TabItem value="lightweight" …>` block is FIRST.** If a group has `heavyweight` first, move the `lightweight` `<TabItem>…</TabItem>` block above it (content unchanged). This makes lightweight the default on a fresh visit, consistently with every other tier tab group.

- [ ] **Step 2: Confirm quick-start is already light-first** (no change expected):
```bash
awk '/<Tabs groupId="gbx-tier"/{f=1} f&&/TabItem value=/{print FILENAME": "$0; f=0}' docs/docs/quick-start.mdx docs/docs/installation.mdx docs/docs/api/raster-functions.mdx
```
Expected: the first `TabItem value=` after each `<Tabs groupId="gbx-tier"` is `value="lightweight"`.

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/installation.mdx docs/docs/api/raster-functions.mdx
git commit -m "docs: lightweight-first on all gbx-tier tab groups

Co-authored-by: Isaac"
```

---

### Task 9: Rewrite the `sidebars.js` Readers & Writers block

**Files:**
- Modify: `docs/sidebars.js` (the `Readers & Writers` category, currently lines ~44–101)

- [ ] **Step 1: Replace the entire `Readers & Writers` category object** with the format-grouped structure:

```javascript
    {
      type: 'category',
      label: 'Readers & Writers',
      collapsed: false,
      items: [
        'readers/overview',
        'writers/overview',
        {
          type: 'category',
          label: 'Readers',
          collapsed: false,
          items: [
            { type: 'category', label: 'General', collapsed: false, items: ['readers/raster', 'readers/ogr'] },
            { type: 'category', label: 'Named', collapsed: false, items: ['readers/geotiff', 'readers/shapefile', 'readers/geojson', 'readers/geopackage', 'readers/filegdb'] },
          ],
        },
        {
          type: 'category',
          label: 'Writers',
          collapsed: false,
          items: [
            { type: 'category', label: 'General', collapsed: false, items: ['writers/raster'] },
            { type: 'category', label: 'Named', collapsed: false, items: ['writers/geotiff', 'writers/pmtiles'] },
          ],
        },
      ],
    },
```

- [ ] **Step 2: Verify no deleted ids remain referenced in the sidebar:**
```bash
grep -nE "raster_gbx|gtiff_gbx|pmtiles_gbx|readers/gdal|writers/gdal|readers/gtiff'" docs/sidebars.js ; echo "exit:$?"
```
Expected: no matches (`exit:1`).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/sidebars.js
git commit -m "docs(nav): regroup readers/writers by format (tier is now a tab)

Co-authored-by: Isaac"
```

---

### Task 10: Repoint all cross-links to the merged/renamed pages

Other docs link to the now-deleted page ids. Find and fix every internal reference.

**Files:**
- Modify: any `docs/docs/**/*.mdx` that links to a deleted id.

- [ ] **Step 1: Find all references to deleted ids** (excluding the spec/plan files under `docs/superpowers/`):
```bash
cd /Users/mjohns/IdeaProjects/geobrix
grep -rnE "readers/raster_gbx|readers/gdal\b|readers/gtiff_gbx|readers/gtiff\b|writers/raster_gbx|writers/gdal\b|writers/gtiff_gbx|writers/pmtiles_gbx|\./raster_gbx|\./gdal\b|\./gtiff_gbx|\./gtiff\b|\.\./writers/raster_gbx|\.\./readers/raster_gbx" docs/docs/ | grep -v "docs/superpowers/"
```

- [ ] **Step 2: Repoint each match** using this mapping (links may be `./x`, `../readers/x`, or doc-id form):
  - `raster_gbx` (reader) and `readers/gdal` → `readers/raster` (or `./raster`)
  - `gtiff_gbx` (reader) and `readers/gtiff` → `readers/geotiff` (or `./geotiff`)
  - `writers/raster_gbx` and `writers/gdal` → `writers/raster`
  - `writers/gtiff_gbx` → `writers/geotiff`
  - `writers/pmtiles_gbx` → `writers/pmtiles`
  - To deep-link a specific tier, append `?tier=lightweight` or `?tier=heavyweight` (e.g. `../writers/raster?tier=heavyweight`).
  Anchors like `raster_gbx#register` become `raster#register` (the `## Register` heading now lives inside the lightweight tab of `readers/raster`).

- [ ] **Step 3: Re-run the grep to confirm zero remaining references** (outside `docs/superpowers/`):
```bash
grep -rnE "readers/raster_gbx|readers/gdal\b|readers/gtiff_gbx|writers/raster_gbx|writers/gdal\b|writers/gtiff_gbx|writers/pmtiles_gbx" docs/docs/ | grep -v "docs/superpowers/" ; echo "exit:$?"
```
Expected: no matches (`exit:1`).

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs
git commit -m "docs: repoint cross-links to merged reader/writer pages

Co-authored-by: Isaac"
```

---

### Task 11: Build + doc-tests + manual verification (dispatch Docker steps as a Task subagent)

**Files:** none (verification only)

- [ ] **Step 1: Docusaurus build must succeed** (catches broken MDX, dangling sidebar ids, broken links). Run the docs build:
```bash
bash scripts/commands/gbx-docs-start.sh --log tabs-build.log   # or the project's build/CI-docs command
```
Expected: build completes with **no broken-link / unknown-sidebar-id errors**. If `gbx:docs:start` only serves, use the CI docs build command (`gbx:ci:docs`) or `cd docs && npm run build`.

- [ ] **Step 2: Doc-tests stay green** (the imported example code is unchanged; this confirms the merged pages still resolve their `CodeFromTest` sources). Dispatch as a Task subagent (Docker, minutes):
```bash
gbx:test:python-docs --path readers/ --log tabs-readers-docs.log
gbx:test:python-docs --path writers/ --log tabs-writers-docs.log
```
Expected: the same pass/skip set as before this work (no NEW failures; the pre-existing heavy `gdal`/`gtiff_gdal` writer doc-test `DATA_SOURCE_NOT_FOUND` failures, if still present, are pre-existing — not introduced here).

- [ ] **Step 3: Internals-leak check** (clean):
```bash
grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ ; echo "exit:$?"
```
Expected: no matches (`exit:1`).

- [ ] **Step 4: Manual tab check** in `gbx:docs:dev` (port 3000): open `readers/raster` — Lightweight tab is shown **first and by default**; switching to Heavyweight and navigating to `writers/raster` keeps Heavyweight (synced via `gbx-tier`/`queryString`); a fresh load (clear the `?tier=` and localStorage) defaults to Lightweight. Confirm the vector pages (e.g. `readers/shapefile`) show the note and **no empty tab**.

- [ ] **Step 5: No commit** (verification only). If any step fails, fix the offending page and re-run.

---

## Self-Review notes (plan vs spec)

- **Spec coverage:** format-grouped nav → Task 9; merged tabbed pages (raster/geotiff readers, raster/geotiff/pmtiles writers) → Tasks 1–5; single-tier note + labels → Task 6; light-first/default + synced toggle → Tasks 1–5 (new pages) + 7 (overviews) + 8 (rest of site); overview consolidation → Task 7; sidebar label supersession → Tasks 1–6 (`sidebar_label` set to format names); `gtiff_gdal`-writer gap filled → Task 4; cross-link repointing → Task 10; no doc-test churn + build/leak verification → Task 11; no redirects → honored (no redirect task).
- **Deviation from spec (intentional, better):** the spec wrote `groupId="tier"` with values `light`/`heavy`; the plan uses the **established** `groupId="gbx-tier" queryString="tier"` with `lightweight`/`heavyweight` so the new pages sync with the existing quick-start/overview tier tabs site-wide. Tab **labels** carry the `· raster_gbx` / `· gdal` format names as the spec intended.
- **Type/name consistency:** new doc ids (`readers/raster`, `readers/geotiff`, `writers/raster`, `writers/geotiff`, `writers/pmtiles`) are used identically in the page frontmatter, `sidebars.js` (Task 9), and the cross-link mapping (Task 10). Deleted ids (`*_gbx`, `readers/gdal`, `writers/gdal`, `readers/gtiff`) are removed in their create/merge task AND scrubbed from sidebars (Task 9) and links (Task 10).
- **Placeholder scan:** the `<!-- … BODY … -->` markers are explicit move instructions (move the named source page's body verbatim), not placeholders; each has an exact source file + what to drop (frontmatter/imports/H1). No TBD/TODO.
- **No-doc-test-churn:** confirmed — every `CodeFromTest` source/example file referenced by the merged pages already exists and is unchanged; the only "new" content (Task 4 heavy GeoTIFF tab) reuses the existing tested `WRITE_GTIFF_GDAL` example.
