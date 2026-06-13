# Readers & Writers: tabbed light/heavy tiers (consolidated by format) Design

**Date:** 2026-06-12
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan

## Summary

Consolidate the Readers & Writers docs **by format instead of by tier**. Today the
nav splits into `Lightweight` and `Heavyweight` subtrees, so a format with both
tiers (e.g. raster) appears as two separate pages (`raster_gbx` and `gdal`). As the
light and heavy tiers converge on a **1:1 reader/writer correspondence**, that split
duplicates structure and hides the equivalence.

New model: **one page per format**, with common format-level intro text followed by
**`<Tabs>`** to toggle between the **Lightweight** (default, first) and
**Heavyweight** tiers. Formats that exist in only one tier stay single pages with a
short note that the other tier has no equivalent yet — **no placeholder tabs**.

This is a **docs-only** change (structure + prose). The executable doc-tests under
`docs/tests/` are unchanged; pages keep importing the same example files via
raw-loader. One genuine gap is filled: the heavy **`gtiff_gdal` writer** (a real,
registered DataSource — `src/main/scala/.../rasterx/ds/gtiff/GTiff_DataSource.scala`,
shortName `gtiff_gdal`, "read/write .tif") is currently undocumented; its write side
gets a Heavyweight tab on the GeoTIFF-writer page.

## Goals / non-goals

- **Goal:** group by format; light/heavy as synced tabs; light default + first
  everywhere; document the equivalence (and where it isn't exact).
- **Goal:** fill the `gtiff_gdal`-writer doc gap.
- **Non-goal:** any change to the executable doc-tests' assertions or the example
  `.py`/`.scala` code (only which snippet renders under which tab).
- **Non-goal:** a light vector tier — vector reader pages stay heavyweight-only +
  note until `pyvx` lands.

## Final navigation

```
Readers & Writers
├── Overview                 (readers/overview + writers/overview — consolidated)
├── Readers
│   ├── General
│   │   ├── Raster           TABS: Lightweight (raster_gbx) | Heavyweight (gdal)
│   │   └── Vector     heavyweight-only (ogr) + note
│   └── Named
│       ├── GeoTIFF          TABS: Lightweight (gtiff_gbx) | Heavyweight (gtiff_gdal)
│       ├── Shapefile        heavyweight-only + note
│       ├── GeoJSON          heavyweight-only + note
│       ├── GeoPackage       heavyweight-only + note
│       └── GeoDatabase      heavyweight-only + note
└── Writers
    ├── General
    │   └── Raster           TABS: Lightweight (raster_gbx) | Heavyweight (gdal)
    └── Named
        ├── GeoTIFF          TABS: Lightweight (gtiff_gbx) | Heavyweight (gtiff_gdal)*
        └── PMTiles          TABS: Lightweight (pmtiles_gbx) | Heavyweight (pmtiles)

* Heavyweight GeoTIFF-writer tab is NEW documentation (gtiff_gdal was undocumented).
```

## Page consolidation map

Each tabbed page is **one new format-named `.mdx`** that absorbs the two tier pages.
Single-tier pages keep their existing id.

| New page (doc id) | Lightweight tab source | Heavyweight tab source | Old pages replaced |
|---|---|---|---|
| `readers/raster` | `readers/raster_gbx.mdx` | `readers/gdal.mdx` | both |
| `readers/geotiff` | `readers/gtiff_gbx.mdx` | `readers/gtiff.mdx` | both |
| `readers/ogr` (vector) | — (note) | `readers/ogr.mdx` | unchanged id |
| `readers/shapefile` | — (note) | `readers/shapefile.mdx` | unchanged |
| `readers/geojson` | — (note) | `readers/geojson.mdx` | unchanged |
| `readers/geopackage` | — (note) | `readers/geopackage.mdx` | unchanged |
| `readers/filegdb` | — (note) | `readers/filegdb.mdx` | unchanged |
| `writers/raster` | `writers/raster_gbx.mdx` | `writers/gdal.mdx` | both |
| `writers/geotiff` | `writers/gtiff_gbx.mdx` | **new** (`gtiff_gdal`) | gtiff_gbx |
| `writers/pmtiles` | `writers/pmtiles_gbx.mdx` | `writers/pmtiles.mdx` | both |

URL note: the merged pages get new format-based ids, so prior tier-specific URLs
(`readers/raster_gbx`, `readers/gdal`, …) change. **No redirects** — the docs are
beta and the organization is still WIP, so old URLs are allowed to break.

## Page template

**Tabbed page:**
1. `# <Format> <Reader|Writer>` H1 + `sidebar_label: <Format>` (e.g. `Raster`,
   `GeoTIFF`, `PMTiles`).
2. **Common intro** — what this reader/writer does at the format level, the shared
   `(source, tile)` / input contract, links to the [one-line tier swap](execution-tiers).
3. **`<Tabs groupId="tier">`**:
   - `<TabItem value="light" label="Lightweight · <fmt_gbx>" default>` — light usage,
     options, `<CodeFromTest>` example(s), perf pointer.
   - `<TabItem value="heavy" label="Heavyweight · <fmt_gdal/ogr>">` — heavy usage,
     options, example(s).
4. Where the tiers aren't feature-identical, a one-line caveat (e.g. General Raster:
   "the heavy `gdal` reader supports many more GDAL drivers than the light path").

**Single-tier page:** the existing heavyweight content, plus an admonition:
```
:::note Lightweight equivalent
This format does not have a lightweight reader yet; it is planned with the light
vector tier. Use the heavyweight reader below.
:::
```

## Tabs mechanism

- Docusaurus theme `Tabs`/`TabItem` (import `@theme/Tabs`, `@theme/TabItem` in MDX).
- **`groupId="tier"`** on every `<Tabs>` so the tier choice **syncs across all pages
  and persists** (localStorage). Tab **`value`** is the stable key `light`/`heavy`;
  the **`label`** carries the format-specific name (`Lightweight · raster_gbx`).
- **Light is `default` and the first `<TabItem>`** on every page → light-first,
  light-default everywhere; a returning reader who picked Heavyweight stays there.

## Sidebar labels

Pages become **format-named** (`sidebar_label`): `Raster`, `GeoTIFF`, `Vector`,
`Shapefile`, `GeoJSON`, `GeoPackage`, `GeoDatabase`, `PMTiles`. This **supersedes**
the per-tier sidebar labels added on 2026-06-11 (`Raster GBX`, `GDAL`, `GeoTIFF GDAL`,
…) — those tier+engine strings move into the **tab labels** (`Lightweight · raster_gbx`
/ `Heavyweight · gdal`).

## Overview pages

Consolidate `readers/overview` and `writers/overview` to drop the per-tier framing:
list the formats (General vs Named), explain the **light/heavy tab model** + the
one-line tier swap, and link each format page. Keep them as the two entry pages under
Readers & Writers.

## New content: `gtiff_gdal` writer (fills a gap)

The heavy GeoTIFF writer (`gtiff_gdal`) is registered but undocumented. The existing
`docs/tests/python/writers/gdal_examples.py` already exercises `format("gtiff_gdal")`
writes, so the GeoTIFF-writer Heavyweight tab renders that snippet (extract a
`gtiff_gdal`-focused example function if the current one is GDAL-generic). No new
runtime behavior — just documenting an existing writer.

## Testing / validation

- `gbx:test:python-docs --path readers/` and `--path writers/` stay green (the
  imported example code is unchanged; only MDX structure changes). The new
  `gtiff_gdal`-writer example must execute under the doc-test harness (Docker, heavy).
- Docusaurus build succeeds (`gbx:docs:start` / CI docs build): valid `Tabs`/`TabItem`
  MDX, no broken sidebar ids, no dangling links to the removed tier pages.
- Internals-leak check stays clean (`grep -rn -iE "wave [0-9]+" docs/docs/`).

## Verify-during-impl checklist

1. `groupId="tier"` syncs across pages and persists; light is default+first on every
   tabbed page (manually confirm in `gbx:docs:dev`).
2. Every internal link/sidebar id that pointed at a removed tier page is repointed
   to the new format page (grep for `readers/raster_gbx`, `readers/gdal`,
   `writers/gtiff_gbx`, etc. across `docs/`).
3. Single-tier pages render the note and NO empty tab.
4. The `gtiff_gdal` writer example actually runs in the doc-test harness.
5. `function-info`/binding parity unaffected (no function changes; pmtiles_gbx etc.
   are formats, not registered functions).
6. No redirects from old ids (beta/WIP — old URLs may break).

## Out of scope (later)

- Light vector tier (`pyvx`) — when it lands, the vector reader pages gain a
  Lightweight tab and drop the note.
