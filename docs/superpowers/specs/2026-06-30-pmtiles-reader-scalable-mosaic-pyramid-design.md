# `pmtiles_gbx` Reader — Scalable Per-Tile Mosaic Pyramid + Archive Read

**Status:** design (awaiting review)
**Date:** 2026-06-30
**Tier:** light (`pyrx`/`ds`). Heavy-tier parity is out of scope (queued).

## Goal

Add a **read** side to the existing `pmtiles_gbx` DataSource so it becomes the symmetric PMTiles *tile family*: the writer turns `(z,x,y,bytes)` tile rows into `.pmtiles` archive(s); the reader produces a `(z,x,y,bytes)` tile stream from a selectable input. v1 implements two input modes:

- **`source="raster"`** — a set of source rasters (COGs) → an XYZ tile pyramid built by **per-tile mosaic reads** (scalable, seamless, memory-bounded). This replaces the memory-bound merge-then-pyramid approach.
- **`source="archive"`** — an existing `.pmtiles` file → its tiles back as a `(z,x,y,bytes)` stream (the literal "pmtiles reader"; useful for re-tiling, inspection, mosaicking, and round-trip tests).

Both feed the existing `pmtiles_gbx` writer with no glue.

## Context / motivation

A city-scale raster → XYZ pyramid cannot go through a single mosaic: `merge → reproject → pyramid` loads the whole AOI into one task and trips the Serverless 1 GB/UDF cap (coarsening to fit is a rejected hack). The proven-scalable pattern (validated this session) is **per-tile mosaic reads**: enumerate the output `(z,x,y)` tiles and, for each, read only its 256² window from the source rasters and composite the overlapping sources (rio-tiler `mosaic_reader`). Memory is bounded per tile, each `(z,x,y)` is produced once (no first-wins seams), and it scales with the AOI. A raw notebook prototype proved it (≈4 min, no OOM, seamless locally) but a cluster run rendered only the eastern quads, because the hand-rolled UDF (a) never called `pyrx._env.configure_gdal_env()`, (b) mutated a shared byte cache from `mosaic_reader`'s worker threads, and (c) did unguarded UC-Volume FUSE reads (random window seeks fail on FUSE). The product reader must handle all three correctly and — critically — be **unit-tested on local fixtures** so a coverage regression like that is caught before any cluster run.

## Scope decisions (resolved)

- **One DataSource, both directions.** Extend `PMTilesGbxDataSource` (`ds/pmtiles.py`) with a `reader()`; keep the writer unchanged. Output (read) schema == writer input schema == `(z int, x int, y int, bytes binary)`.
- **v1 input modes:** `raster` (the scalable pyramid) **and** `archive` (read existing `.pmtiles`). Selected by `.option("source", ...)`.
- **Raster mode targets uint8 imagery** (NAIP/RGB(A)). Rescale/nodata handling for elevation/EO is out of scope (queued).
- **Out of scope (queued):** `source="vector"` (MVT pyramid from vectors); inferring `source` from the path; on-read rescale/`in_range`; tippecanoe-style overview simplification; heavy-tier (`gdal`) parity.

## Global constraints

- **Serverless-safe:** in every executor read path call `pyrx._env.configure_gdal_env()` before opening rasters; **no** `spark.conf.set`/`_jvm`/`sparkContext`/`.rdd`. Fan-out comes from DataSource `InputPartition`s (scan partitions, not AQE-coalesced) — no `repartition`/`spark.range` tricks needed.
- **FUSE-safe:** never random-seek a UC Volume path. Read source bytes **sequentially** (`open(path,"rb").read()`) into an in-memory dataset (`rasterio.io.MemoryFile`); same for an archive (`pmtiles` `MemorySource` over the read bytes).
- **Thread-safe:** `mosaic_reader` reads assets in worker threads — the per-task source cache must be **fully pre-loaded single-threaded** before `mosaic_reader` runs (threads only read it), or `mosaic_reader(threads=0)`.
- **No new SQL function**; no `function-info.json` / `registered_functions.txt` change (DataSource option surface only).
- Libraries: `rio_tiler` (`Reader`, `mosaic.mosaic_reader`), `morecantile` (WebMercatorQuad), `pmtiles` (reader) — all already in the `[light]` deps.

## Architecture

`PMTilesGbxDataSource.reader(schema)` returns one of two `DataSourceReader`s based on `options["source"]` (default `"raster"`):

```
ds/pmtiles.py
  PMTilesGbxDataSource(DataSource)
    name() -> "pmtiles_gbx"
    schema() -> (z,x,y,bytes)            # shared read/write schema (exists)
    writer(...) -> PMTilesGbxWriter       # exists, unchanged
    reader(schema) -> PMtilesRasterReader | PMtilesArchiveReader   # NEW

ds/_xyz_mosaic.py   (NEW)  — the per-tile mosaic core (raster mode), unit-testable WITHOUT Spark:
    enumerate_tiles(bbox4326, min_z, max_z) -> list[(z,x,y)]      # morecantile WebMercatorQuad
    source_bounds_union(paths) -> bbox4326                         # default AOI
    render_tile(z, x, y, source_datasets, pixel_selection="first") -> bytes | None
        # mosaic_reader composites; returns PNG, or None if no source covers the tile
```

### Raster reader (`source="raster"`)
- **Options:** `path` (dir of source COGs; recursive list + `filterRegex`, default `.*\.tif$`), `bbox` (`"minx,miny,maxx,maxy"` EPSG:4326; optional → `source_bounds_union`), `minZoom`, `maxZoom`, `tilesPerPartition` (default 64), `tileFormat` (default `png`), `pixelSelection` (default `first`).
- **`partitions()`** (driver): list source COGs; resolve AOI; `enumerate_tiles`; group tiles **spatially** (sort by `z` then a row-major/parent-tile key so a partition's tiles are contiguous) into chunks of ≤`tilesPerPartition`; one `InputPartition` per chunk, carrying its tile list + the source paths whose bounds intersect the chunk's combined tile bbox. → N partitions fan out by construction.
- **`read(partition)`** (executor): `configure_gdal_env()`; **pre-load** the partition's intersecting source COGs FUSE-safely (sequential bytes → `MemoryFile` datasets) into a per-task dict (single-threaded); for each tile, `render_tile(...)` via `mosaic_reader` over those in-memory datasets (threads only read the cache); **skip `None`** (no coverage — not emitted); yield `(z,x,y,bytes)`.

### Archive reader (`source="archive"`)
- **Options:** `path` (a `.pmtiles` file), `tilesPerPartition` (default 2048).
- **`partitions()`** (driver): read the archive header + tile directory (sequential) → list `(z,x,y)` entries → chunk into `InputPartition`s.
- **`read(partition)`** (executor): read the `.pmtiles` bytes sequentially (FUSE-safe) → `MemorySource`; for the partition's entries, yield `(z,x,y, tile_bytes)`. (v1 reads the archive bytes per task — fine for the inspection/re-tile use; not the hot path.)

### Data flow (raster mode, the headline)
`source COGs (Volume)` → `partitions()` enumerates `(z,x,y)` + groups → `read()` per partition: in-memory mosaic per tile → `(z,x,y,bytes)` rows → `.write.format("pmtiles_gbx")` → `.pmtiles`.

## Error handling
- No source COGs found at `path` → `ValueError` (clear message).
- Malformed `bbox` (≠4 floats) → `ValueError`.
- AOI / source bounds don't intersect → zero tiles emitted (empty DataFrame with the correct schema), with a `log` note (not a silent success that looks like coverage).
- A tile with no covering source → skipped (no row), never an opaque/transparent filler tile.
- Archive mode: missing/unreadable `.pmtiles` → the `pmtiles` reader's error propagates.

## Testing (TDD — the headline requirement; local fixtures, no cluster)

Core (`ds/_xyz_mosaic.py`, pure, no Spark):
- `enumerate_tiles` returns the expected `(z,x,y)` set for a known bbox+zoom.
- `render_tile` over 2 overlapping fixture COGs: a tile spanning the boundary composites **both** (full coverage, no transparent gap) — the exact regression for the cluster western-quad bug.
- `render_tile` returns `None` for a tile outside all sources (→ skipped).
- correct georef/placement: the tile content for a known `(z,x,y)` matches the source at that location.

Reader (Spark, local `local[2]`, fixture COGs + a fixture `.pmtiles`):
- `source=raster` end-to-end → `(z,x,y,bytes)`; **interior tiles all present** (seamless), empty-only-where-no-source; multi-source compositing holds; schema is exactly `(z,x,y,bytes)`.
- partitioning: ≥2 partitions for a multi-tile AOI (fan-out), and the union of partition outputs == the full tile set (no tiles lost at chunk boundaries).
- `source=archive` round-trip: write tiles via `pmtiles_gbx` writer → read back via `pmtiles_gbx` reader (`source=archive`) → identical `(z,x,y)` set + bytes.
- FUSE-safe: the read path uses sequential byte reads + `MemoryFile`/`MemorySource` (no direct windowed path opens) — asserted by construction / a stubbed `open`.

## Downstream consumer (motivation, not built here)

Helios NB-02 §3 collapses to one reader→writer call:
```python
(spark.read.format("pmtiles_gbx")
    .option("source","raster").option("path", NAIP_DIR)
    .option("bbox", ",".join(str(v) for v in SF_CITY_BBOX)).option("minZoom","12").option("maxZoom","16")
    .load()
    .write.format("pmtiles_gbx").option("shardZoom","0").mode("overwrite").save(NAIP_PMTILES))
```
replacing the merge/reproject/pyramid glue (and the hand-rolled per-tile prototype).
