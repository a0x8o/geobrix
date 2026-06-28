# Design: `gbx_pmtiles_agg` merges multi-feature vector tiles

**Date:** 2026-06-28
**Branch:** `beta/0.4.0`
**Status:** Proposed — awaiting approval before planning.

## Problem

`gbx_pmtiles_agg` (both tiers) deduplicates tiles by `(z, x, y)` **first-write-wins**:
- light `pmtiles/_agg_light.py`: `if tileid in seen: continue` (docstring: "duplicate (z,x,y) keep the first").
- heavy `PMTilesAcc`/`PMTiles_Agg.scala`: first-non-null per tile id.

`gbx_st_asmvt_pyramid` is a generator that emits **one single-feature MVT blob per `(feature, z, x, y)`**. So the documented composition — release notes: *"`st_asmvt_pyramid` … composes with `gbx_pmtiles_agg` for end-to-end vector publishing pipelines"* — **drops all but the first feature in each tile** for dense data (e.g. a buildings basemap: thousands of features per tile collapse to one). First-wins is correct for **raster** (one tile = one image) but wrong for **vector** (one tile = many features). The Helios NB01 buildings basemap is the first end-to-end exercise of this path and exposed it.

## Goal

Make `gbx_pmtiles_agg` produce correct multi-feature **vector** tiles, so the
`st_asmvt_pyramid → gbx_pmtiles_agg` pipeline works for real data, at parity across both tiers.

## Design

When packing a group of tiles, partition by tile id `(z, x, y)`. For each tile id with
multiple blobs:

- **Vector (MVT) tile type:** **merge** the blobs into one MVT tile — decode each blob, union
  its features into the combined tile **keyed by layer name** (a feature from layer `buildings`
  joins the merged `buildings` layer), then re-encode one MVT at the **same extent**. Geometry
  stays in tile-local `[0, extent]` integer space — **no reprojection** (the blobs are already
  tile-local for that exact `(z,x,y)`; decode→encode round-trips the local coords). Attributes
  are preserved per feature.
- **Raster (PNG/JPEG/WebP/etc.) tile type:** **keep first-write-wins** (unchanged — images can't
  be meaningfully merged; one tile = one image).

Tile type is auto-detected from the first non-null payload's magic bytes, as today. The
vector-vs-raster branch keys off that detected type.

### Light tier (`pmtiles/_agg_light.py`)
Group payloads by tileid. For vector: `mapbox_vector_tile.decode` each blob → accumulate
features per layer → `mapbox_vector_tile.encode` (or the existing `_mvt.encode_layer`) once per
tileid at the standard extent. For raster: first non-null. Then write one tile per tileid in
Hilbert order (unchanged). `mapbox_vector_tile` is already a dependency — **no new dep**.

### Heavy tier (`PMTilesAcc` / `PMTiles_Agg.scala`)
Same grouping + branch, using the JVM MVT codec already in the heavyweight tier (the
`vectorx`/`mvt` encoder behind `gbx_st_asmvt`). For vector tile ids with >1 blob, decode + union
features per layer + re-encode; raster keeps first. Preserve the existing serialize/deserialize
merge-phase and the partition size cap.

### Parity
Both tiers must produce equivalent merged tiles. Parity test MUST use a **POLYGON** multi-feature
case (points-only gives a false pass — see the MVT tile-local contract): build two single-feature
MVT blobs for the same `(z,x,y)`, pack via each tier, decode the packed tile, assert **both**
features are present with their attributes.

## Decisions (sensible defaults; flag if you disagree)

1. **Per-tile size cap on merge:** merge all features; rely on the **existing partition/buffer
   cap** (the 100 MiB guard). Per-tile feature **simplification/dropping** (RDP, drop-by-zoom —
   the PMTiles mental-model's "pressure point") is **out of scope** here and tracked as a future
   enhancement. A merged tile that's individually huge is allowed (the partition cap still guards
   OOM); we may add a per-tile soft-warn but not dropping.
2. **Duplicate features:** union without dedup (the pyramid won't emit the same feature twice for
   one tile; we don't pay to detect duplicates).
3. **Layer handling:** union by layer **name**; multiple layers preserved.
4. **Raster unchanged:** first-wins; existing raster PMTiles tests stay green.

## Testing

- Light unit: pack 2+ single-feature MVT blobs for one `(z,x,y)` → decode packed tile → assert N
  features. A raster test confirms first-wins is unchanged.
- Heavy unit: same, JVM side.
- **Light-vs-heavy parity** (POLYGON multi-feature) per the convention.
- Existing `gbx_pmtiles_agg` raster + the PMTiles writer tests stay green (regression).
- A focused re-run proving the `st_asmvt_pyramid → groupBy? no → pmtiles_agg` path now preserves
  features (NB01 will consume this).

## Docs / bench

- `docs/docs/api/pmtiles-functions.mdx`: document that `gbx_pmtiles_agg` **merges** multi-feature
  vector tiles per `(z,x,y)` and **first-wins** for raster. (User-facing voice; no internal vocab.)
- If merge changes timings materially, note it in `benchmarking.mdx` per the bench-doc rule.
- Capture any validated perf characteristic in the `docs/superpowers/performance/` corpus if warranted.

## Out of scope

- Per-tile feature simplification / drop-by-zoom (RDP) — future.
- Changing `st_asmvt_pyramid` itself (it stays a per-feature generator; the merge happens in the agg).

## Sequencing

PV lands before NB01 is finished. After PV merges + reviews green, NB01's pipeline
(`st_asmvt_pyramid → gbx_pmtiles_agg`) is correct; then fix NB01's residual items
(`dbutils.fs.mkdirs` → `os.makedirs`, remove the misleading "packs them correctly" comment, the
deg² unit note, the tier-split LATERAL syntax note) and accept it.
