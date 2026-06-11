# Light PMTiles Writer (`pmtiles_gbx`) Design

**Date:** 2026-06-11
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan

## Summary

A pure-Python/PySpark PMTiles writer — `pmtiles_gbx` — as the lightweight-tier
counterpart of the heavy Scala `pmtiles` DataSource writer. It packages
already-tiled `(z, x, y, bytes)` input (produced distributed in Spark by
`st_asmvt` for vector or `rst_xyzpyramid` for raster) into a **single `.pmtiles`
archive**, using the official Protomaps **`pmtiles` Python library** as the
encoder. Built on Spark DataSource V2 (like `raster_gbx`), JAR-free, Serverless-
safe. Slots into Lightweight → Writers → Named.

Companion to the raster reader/writer specs (same `pyrx/ds/` subpackage).

## Why the `pmtiles` library (not a hand-rolled encoder)

The official Protomaps `pmtiles` package (pip, pure Python) **can write** PMTiles
v3 archives — `pmtiles.writer.Writer(f).write_tile(tileid, data)` + `finalize(
header, metadata)`, with `pmtiles.tile.zxy_to_tileid`, `TileType`, and **leaf-
directory support** (so no 16 KiB root-directory limit the heavy Scala
`PMTilesV3Encoder` has). Per the "use best-in-class packages, never a partial
stub" principle, we use it for encoding rather than hand-rolling the format. Our
build is the Spark DataSource V2 two-phase wrapper + the header assembly, not a
format implementation.

(Note: Tippecanoe — a native CLI — does source→tiles→PMTiles in one shot on a
single node; it is NOT a fit for the Spark-distributed, pip-only, Serverless
lightweight tier. geobrix tiles distributed (`st_asmvt`/`rst_xyzpyramid`) and this
writer packages the result. sqlite/MBTiles is a different format, out of scope.)

## Heavy PMTiles writer contract (parity target)

Verified against `src/main/scala/com/databricks/labs/gbx/pmtiles/`:

- **Input schema:** `(z:int, x:int, y:int, bytes:binary)`.
- **Format:** `pmtiles` (DataSource V2, write). **Output:** a single `.pmtiles`
  file (not a directory).
- **Distributed single-file assembly:** per-partition `PMTiles_RowWriter`
  streams tile bytes to scratch `_part_*` shards (+ entries metadata); driver-side
  `PMTiles_BatchWrite.commit()` merges, sorts by Hilbert TileID, builds the
  directory + 127-byte header, streams the final archive; `abort()` deletes
  scratch.
- **Tile-type** sniffed by magic bytes (PNG/JPEG/WebP, else MVT). **Options**
  include `metadataJson`, `tileCompression`, etc.
- Also a `gbx_pmtiles_agg` UDAF (in-memory, bounded) — the small-pyramid path; we
  mirror the **DataSource** (unbounded, two-phase) path here.

## Architecture

New module `python/geobrix/src/databricks/labs/gbx/pyrx/ds/pmtiles.py`:

- **`PMTilesGbxDataSource`** (`DataSource`, `name()=="pmtiles_gbx"`) — `schema()`
  enforces `(z,x,y,bytes)`; `writer(schema, overwrite)` returns the writer;
  `reader()` raises a clear "PMTiles is write-only here; read with the `pmtiles`
  reader or `format('gdal')`" message.
- **`PMTilesGbxWriter`** (`DataSourceWriter`) — two-phase shard→driver-merge
  (below).
- **`_pmtiles_header.py`** (helper, no Spark) — `sniff_tile_type(bytes)` (magic →
  `pmtiles.tile.TileType`) and `build_header(tile_type, tile_compression,
  zxy_extent)` → the `pmtiles` `HeaderDict` (min/max zoom, lon/lat bbox + center
  via standard tile→lon-lat math). Unit-testable.
- **Register:** add `PMTilesGbxDataSource` to `pyrx.ds.register._SOURCES`.
- **Dependency:** add `pmtiles` to the **`[light]`** extra in `pyproject.toml`.

## Write contract

- **Writer options:** `metadata` (JSON string → archive metadata), `tileType`
  (default auto-sniff; override), `tileCompression` (default none/passthrough),
  `path` (the `.save()` target — a single `.pmtiles` file).
- **Mode:** `overwrite` replaces the archive (clears any prior output + scratch on
  the driver before tasks); `append` is rejected with a clear message (a finalized
  single archive can't be appended to).
- **`write(iterator)` (executor / per partition):** for each row compute
  `tileid = pmtiles.tile.zxy_to_tileid(z, x, y)`; append `(tileid, bytes)` to a
  scratch shard under the output parent (`<out>._gbx_scratch/part-<uuid>`, a
  length-prefixed binary). Return a `WriterCommitMessage` carrying the **shard
  path** + a small summary (count, sniffed tile_type, min/max zoom, lon/lat
  extent) — never the tile bytes (keeps the driver transfer small; scales).
- **`commit(messages)` (driver, single):** read all shards, collect `(tileid,
  bytes)`, **sort by tileid**, open `pmtiles.writer.Writer(open(out, "wb"))`,
  `write_tile(tileid, bytes)` in order, then `finalize(header, metadata)` where
  `header` is built by `_pmtiles_header.build_header` from the merged summaries
  (tile_type must agree across shards; min/max zoom + bbox aggregated). Delete the
  scratch dir.
- **`abort(messages)`:** delete scratch shards + any partial output.
- **Dedup/RLE:** delegated to the `pmtiles` `Writer` (sorted tileid order + its
  directory run-length encoding). May differ from heavy's SHA-256 dedup — fine,
  since parity is "valid archive decoding to the same tiles", not byte-identical.
- **Scratch on shared storage:** shards live under the output path's parent (a
  Volume), readable by the driver in `commit()` — mirrors the heavy writer's
  `_part_*`. Pure Python `open()`/`os` (Serverless-safe; no `_jvm`/`.rdd`).

## Testing (TDD — tests are the contract)

- **Header helper (unit, no Spark):** tile-type sniff for PNG/JPEG/WebP/MVT;
  bbox/center/min-max-zoom from z/x/y extents; `HeaderDict` shape.
- **Writer round-trip (local Spark):** write a small `(z,x,y,bytes)` DataFrame via
  `pmtiles_gbx` → one `.pmtiles` file → read back with the `pmtiles` reader →
  assert the same `z/x/y→bytes` set, `tile_type`, zoom range. (Round-trip is the
  validity check.)
- **Multi-partition:** tiles across partitions all land in the merged archive,
  sorted.
- **Strict schema:** missing/extra columns fail. **Mode:** `append` rejected;
  `overwrite` replaces. **Empty input:** clear behavior (valid empty archive or
  explicit skip).
- **Serverless guard:** `pmtiles.py` (+ `_pmtiles_header.py`) covered by the
  existing `ds/` scan; no `_jvm`/`sparkContext`/`.rdd`/`.conf.set`.
- **Light-vs-heavy parity (Docker/integration, skip-if-heavy-unavailable):** same
  input → `pmtiles_gbx` and heavy `pmtiles` archives both read back (via the
  `pmtiles` reader) to the same tile set — decoded-tile parity, not byte.
- **Perf validation:** extend the bench `run_format_write` with a PMTiles write
  timing (light `pmtiles_gbx` vs heavy `pmtiles`) on the cluster, same method as
  the raster-writer bench; record the light-vs-heavy ratio. (Hypothesis: light is
  competitive — both just package pre-made tiles; the heavy path's per-tile
  encode/dedup vs the `pmtiles` lib's will decide.)

## Registration & docs

- `pyrx.ds.register.register(spark)` registers `pmtiles_gbx` (added to `_SOURCES`).
- New `docs/docs/writers/pmtiles_gbx.mdx` (Lightweight → Writers → Named, beside
  `gtiff_gbx`) importing a new doc-test `docs/tests/python/writers/
  pmtiles_gbx_examples.py` (build a small pyramid via `rst_xyzpyramid` or
  `st_asmvt`, write, round-trip read). Add to `docs/sidebars.js` (Lightweight
  Writers → Named) + `writers/overview.mdx`; cross-link the heavy `pmtiles` writer
  and the upstream tile producers.

## Out of scope

- A light PMTiles **reader** (use the `pmtiles` reader or heavy `gdal`).
- A light **MBTiles** (sqlite) writer.
- Byte-identical parity with heavy archives (different encoder; decoded-tile
  parity only).
- Tippecanoe / GeoJSON→PMTiles conversion (external, single-node).
- The in-memory `pmtiles_agg` UDAF light equivalent (the DataSource is the focus).

## Verify-during-impl checklist

1. `pmtiles` lib version + the exact `HeaderDict` fields `finalize()` requires
   (min_zoom/max_zoom, min/max lon/lat, center, tile_type, tile_compression,
   internal_compression) — supply all it needs; confirm a written archive
   round-trips through the `pmtiles` reader.
2. Whether `Writer.write_tile` requires strictly ascending tileid (it does —
   commit sorts) and whether it dedups identical content itself.
3. Tile-type uniformity: PMTiles header carries one `tile_type`; if shards
   disagree, error clearly (a pyramid should be uniform).
4. Scratch path under the output parent works for executor-write/driver-read on a
   Volume (FUSE); pick a name that won't collide and is cleaned on commit/abort.
5. `tileCompression`: if "gzip", who gzips — the `pmtiles` lib or us? Match the
   header's `tile_compression` byte to the actual tile bytes (default none /
   passthrough, like heavy).
6. Empty-input commit: produce a valid empty archive or skip with a clear note.
