# Light Tiled-Output Framework + PMTiles Writer (`pmtiles_gbx`) Design

**Date:** 2026-06-11
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan
**Supersedes:** the earlier narrow single-archive `pmtiles_gbx` draft (this redesign
reframes it as the first backend on a shared, consolidating tiled-output framework,
per the spatial-sharding mental model `input/pmtiles/pmtiles_mental_model.md`).

## Summary

Build a **shared lightweight tiled-output framework** at
`databricks.labs.gbx.ds.tiles` that consolidates the cross-format pipeline
logic for distributed, spatially-sharded tile/pyramid outputs, and ship its
**first backend — PMTiles (`pmtiles_gbx`)** — as a pure-Python DataSource V2
writer. The framework is deliberately factored so later backends (COG-by-quadbin
+ VRT, MVT-dir, MBTiles) are incremental additions, not rewrites.

Mental model (full doc: `input/pmtiles/pmtiles_mental_model.md`; see
[[pmtiles-spatial-sharding-model]]): treat tiled outputs as **immutable,
spatially-indexed shards** — partition the world by a grid (quadtree) → each
worker owns a parent tile → emits one **bounded, non-overlapping** shard → deliver
a **catalog** over the shards (not necessarily one merged file). Buffering keeps
edges correct; raster adds bottom-up pyramids, global scaling, sparse-skip.

**Writers consolidate the pipeline; the format is a pluggable backend.** Named,
documented writers (`pmtiles_gbx`, …) expose the behavior through `.option(...)`
knobs with sensible defaults (single-archive vs sharded, catalog type, tile
type/compression, metadata, mode) — not a parallel set of higher-level functions.

## Why a framework (not a one-off writer)

Per the function inventory (see the brainstorm analysis), PMTiles/COG/MVT/MBTiles
all share the same outer pipeline — **grid → shard → (pyramid) → assemble →
catalog** — and differ only in the per-shard container and catalog type. The
heavy tier reimplements this per format; the light tier should consolidate it once
and vary the leaf. The "consolidate logic in Writers/Readers" principle: a writer
does the spatial-sharding orchestration, not just "write exactly these bytes."

Two backend *shapes* share the same grid/shard/catalog layer:
- **Tile-archive backends** (PMTiles, MBTiles, MVT-dir): input `(z,x,y,bytes)` →
  assemble a container.
- **Raster-file backend** (COG, later): input raster tiles → one COG per cell.
The shared layer = grid + shard + catalog (+ pyramid helpers); the leaf = how one
shard becomes a file.

## Function consolidation (retraction principle)

As the framework absorbs pipeline logic, any `rst_*`/`st_*` function that is
*really* an I/O operation (writing a tiled/pyramid output, or reading a tiled
source) should **consolidate into the writer/reader, and the redundant function be
retracted** — one canonical path, no function-and-writer duplication. Beta +
no-aliases makes this clean. Decisions are made **per backend, not en masse**, and
each retraction is a coordinated **both-tier** change (remove from Scala
`override def name` + light `pyrx`, `registered_functions.txt`,
`function-info.json`, docs, and the function-count diagram) that must keep the QC
`binding-parity` / `doc-coverage` / `diagram-coverage` gates green.

- **`rst_cog_convert` → the `cog_gbx` writer** — the clearest candidate; "convert
  to COG" is a write. Retract when the COG-by-quadbin backend lands.
- **Tiling family** (`rst_xyzpyramid`, `rst_tilexyz`, `rst_maketiles`,
  `rst_retile`, `rst_tooverlappingtiles`) — keep the genuine "produce tiles"
  transforms, but fold pyramid/shard/overlap logic into the framework and retract
  whatever becomes pure writer-internals; evaluate as each backend absorbs it.
- **This (PMTiles-first) phase retracts nothing** — it is net-new (no existing
  light PMTiles function; heavy `pmtiles` stays). The principle is recorded here to
  guide the COG and tiling phases.

## Architecture

New tier-neutral Python DataSource package `databricks.labs.gbx.ds`
(`python/geobrix/src/databricks/labs/gbx/ds/`) — a sibling of `pyrx`/`bench`/etc.
(every Python DataSource is inherently light; the `_gbx` format names + docs tier
labels carry the tier signal, so the path need not). With:

### `ds/tiles/` — the shared framework

- **`grid.py`** — grid-pluggable tile math. A `Grid` protocol:
  `tile_bbox(z,x,y) -> (minlon,minlat,maxlon,maxlat)`, `parent(z,x,y, shard_zoom)
  -> (sz,sx,sy)`, `tiles_for_bbox(bbox, zoom) -> Iterable[(z,x,y)]`,
  `buffered_bbox(z,x,y, buffer) -> bbox`. Implementations: **`SlippyGrid`**
  (web-mercator XYZ — the PMTiles-native grid) and **`QuadbinGrid`** (via the
  `quadbin` pip package, already in `[light]` — for the COG path later). Pure
  Python; fixes the heavy `rst_xyzpyramid` CRS-unit issue by being explicit about
  units. The keystone reused by every tiled writer.
- **`shard.py`** — partition tiles by parent (at `shard_zoom`) and the generic
  **two-phase scratch→driver-merge** orchestration: per-partition write to a
  shared scratch shard keyed by parent-tile; driver groups by parent and hands
  each group to the backend to assemble one output shard. Backend-agnostic.
- **`catalog.py`** — `CatalogWriter` protocol `write(shard_entries, out_dir) ->
  catalog_path`. First impl **`TileJSONCatalog`** (a `tilejson`/`mosaic.json`
  over the per-shard `.pmtiles` with their bounds/min-max-zoom). Designed so
  **`VRTCatalog`** (GDAL virtual raster over COG shards — the DE "single Volume
  path", generated as pure-Python VRT XML from each shard's bounds/transform)
  slots in for the COG backend later.
- **`backend.py`** — `TileArchiveBackend` protocol `assemble(sorted_tiles_iter,
  header_info, out_path)`. First impl **`PMTilesBackend`** (uses the Protomaps
  `pmtiles` library: `Writer(open(out,'wb')).write_tile(tileid, bytes)` +
  `finalize(header, metadata)`; `pmtiles.tile.zxy_to_tileid`, `TileType`). Later
  `MBTilesBackend` (sqlite), `MVTDirBackend`. (COG is the other backend shape,
  added with `QuadbinGrid` + `VRTCatalog`.)
- **`_header.py`** — PMTiles `HeaderDict` assembly: `sniff_tile_type(bytes)`
  (PNG/JPEG/WebP/MVT magic → `pmtiles.tile.TileType`), and bounds/center/min-max
  zoom from a shard's z/x/y extent. Pure Python, unit-testable.

### `ds/pmtiles.py` — the DataSource

- **`PMTilesGbxDataSource`** (`DataSource`, `name()=="pmtiles_gbx"`) — `schema()`
  enforces `(z:int,x:int,y:int,bytes:binary)`; `writer(schema, overwrite)` returns
  the writer; `reader()` raises a clear "PMTiles is write-only here; read with the
  `pmtiles` reader or `format('gdal')`" message.
- **`PMTilesGbxWriter`** (`DataSourceWriter`) — thin: reads options, drives
  `tiles.shard` + `PMTilesBackend` (+ `TileJSONCatalog` when sharded).
- **Registration:** `gbx.ds.register.register(spark)` registers the light
  DataSources (initially `pmtiles_gbx`); `pyrx.ds.register` continues to register
  the raster ones. (Unifying registration + migrating `pyrx/ds/` raster
  readers/writers under `ds/` is a flagged follow-up, not in this spec.)
- **Dependency:** add `pmtiles` to the **`[light]`** extra.

## Write contract (`pmtiles_gbx`)

- **Input schema (enforced):** `(z:int, x:int, y:int, bytes:binary)` (from
  `st_asmvt`/`rst_xyzpyramid` upstream; tile-type sniffed). The writer packages +
  shards + catalogs — it does **not** tile/pyramid (that stays upstream).
- **Options (named writer + knobs, sensible defaults):**
  - `path` (required) — the `.save()` target.
  - `mode` — `overwrite` (default, clears prior output + scratch); `append`
    rejected (a finalized archive can't be appended to).
  - `shardZoom` — **unset (default) ⇒ single archive** at `path` (one `.pmtiles`);
    **set ⇒ sharded**: partition tiles by their parent at `shardZoom`, emit one
    `<z>_<x>_<y>.pmtiles` per parent under `path/` (a directory) + a catalog. This
    is the scale/real-world pattern; single-archive is the simple default.
  - `catalog` — when sharded: `tilejson` (default) | `none`. (Single-archive
    emits no catalog.)
  - `tileType` — default auto-sniff (PNG/JPEG/WebP/MVT; must agree within a shard);
    override available.
  - `tileCompression` — default none/passthrough.
  - `metadata` — JSON string → archive metadata.
- **Single-archive flow:** the two-phase shard machinery with a single implicit
  shard — per-partition scratch → driver sorts by tileid → `PMTilesBackend.assemble`
  → one `.pmtiles`.
- **Sharded flow:** `write(iterator)` (executor) writes each tile to a scratch
  shard keyed by `grid.parent(z,x,y,shardZoom)`; `commit(messages)` (driver) groups
  scratch by parent, assembles one `.pmtiles` per parent (sorted by tileid), then
  `TileJSONCatalog.write(...)` emits the catalog. `abort` cleans scratch + partials.
  Shards are **bounded + non-overlapping** (each parent owns a disjoint tile set).
- **Scratch** lives under the output parent (shared Volume), executor-write /
  driver-read; pure-Python `open`/`os` (Serverless-safe; no `_jvm`/`.rdd`).
- **Parity:** valid archive(s) the `pmtiles` reader (and heavy reader) decode to
  the same `z/x/y→bytes` set — decoded-tile parity, not byte-identical.

## Testing (TDD)

- **Framework units (no Spark):** `SlippyGrid` (tile_bbox, parent, tiles_for_bbox,
  buffered_bbox) against known web-mercator values; `_header.sniff_tile_type` +
  bounds/zoom math; `TileJSONCatalog.write` output shape (valid tilejson over fake
  shard entries); `PMTilesBackend.assemble` round-trips via the `pmtiles` reader.
- **Writer round-trip (local Spark):** single-archive — write `(z,x,y,bytes)` →
  one `.pmtiles` → read back, same tiles/type/zoom. Sharded (`shardZoom`) — write →
  directory of per-parent `.pmtiles` + a tilejson; assert shards are
  non-overlapping, cover all input tiles, each reads back correctly, and the
  catalog references them with correct bounds.
- **Multi-partition, strict schema, mode (`append` rejected / `overwrite`
  replaces), empty input, Serverless guard** (the no-`_jvm`/`.conf.set`/`.rdd`
  source scan, currently over `pyrx/`, extended to cover the new `gbx/ds/`).
- **Light-vs-heavy parity (Docker/integration, skip-if-heavy-unavailable):** same
  input → `pmtiles_gbx` (single mode) vs heavy `pmtiles` both read back to the same
  tile set.
- **Perf validation:** extend the bench `run_format_write` with a PMTiles write
  timing (light `pmtiles_gbx` vs heavy `pmtiles`) on the cluster, same method as
  the raster-writer bench; record the light-vs-heavy ratio.

## Registration & docs

- `gbx.ds.register.register(spark)` → `pmtiles_gbx`.
- New `docs/docs/writers/pmtiles_gbx.mdx` (Lightweight → Writers → Named, beside
  `gtiff_gbx`) + doc-test `docs/tests/python/writers/pmtiles_gbx_examples.py`:
  build a small pyramid (`rst_xyzpyramid`/`st_asmvt`), write single-archive **and**
  sharded+tilejson, round-trip read. Sidebar + `writers/overview.mdx`; cross-link
  the heavy `pmtiles`, the upstream tile producers, and the spatial-sharding model.

## Out of scope (designed-for, built later)

- **COG-by-quadbin backend + VRT catalog** (the primary *raster* DE target — the
  framework's `QuadbinGrid`, `VRTCatalog`, and a raster-file backend; its own spec).
- **MVT-dir / MBTiles backends.**
- **Bottom-up raster pyramid, global-scaling enforcement, sparse-skip, pixel-buffer
  ↔ xyz integration** (raster-depth phase).
- **Light vector tier (`pyvx`)** — `st_asmvt` etc. are heavy-only; the light
  vector→PMTiles path is a separate large effort (ties to the parked DuckDB-vs-
  pyogrio engine question).
- **Migrating the shipped `pyrx/ds/` raster readers/writers under `ds/`** and
  unifying registration — recommended follow-up consolidation, not here.
- A light PMTiles **reader**, light **MBTiles** writer, Tippecanoe/GeoJSON→PMTiles.

## Verify-during-impl checklist

1. `pmtiles` lib `HeaderDict` fields `finalize()` requires; confirm round-trip via
   the `pmtiles` reader. `Writer.write_tile` requires ascending tileid (commit
   sorts); whether it dedups identical content itself.
2. `tilejson` vs `mosaic.json` shape — pick the catalog format MapLibre/Leaflet
   consume directly; per-shard bounds from `grid.tile_bbox` of the shard's parent.
3. Sharded output layout: directory-at-`path` with `<z>_<x>_<y>.pmtiles` + catalog;
   scratch naming that won't collide and is cleaned on commit/abort; executor-write/
   driver-read on a Volume (FUSE).
4. `SlippyGrid` math validated against known tile→lon/lat values (and consistent
   with `rst_xyzpyramid`'s tiling so upstream tiles land in the right shards).
5. Backend/catalog/grid protocols kept minimal + documented so COG/VRT/quadbin slot
   in without reworking `shard.py`.
6. `tileCompression`: who gzips (the `pmtiles` lib vs us); match the header byte to
   the actual bytes (default none/passthrough).
7. Empty-input + empty-shard behavior (valid empty archive or skip with a note).
