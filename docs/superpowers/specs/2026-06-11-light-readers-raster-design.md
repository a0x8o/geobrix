# Light Readers — Raster (Python DataSource V2) Design

**Date:** 2026-06-11
**Branch:** `light-readers`
**Status:** Approved (design), revised post-recon; ready for implementation plan

## Revision 2026-06-11 (post-recon) — parity contract corrected

Reading the Scala `gdal` reader directly (`GDAL_Reader.scala:12-45`,
`RasterSerializationUtil.tileToRow`, `WindowedExtract.scala:108-119`,
`RasterDriver.writeToBytes`) overturned three assumptions baked into the
original parity contract below. The contract has been corrected throughout this
doc; this note records the change for review:

1. **`tile.raster` is NOT raw file bytes — it is a re-encoded GTiff (DEFLATE)
   tile.** The heavy reader splits each source raster into tiles and writes each
   tile out via `RasterDriver.writeToBytes`, which coerces to GTiff/DEFLATE
   regardless of source format. Two independent GDAL stacks (JVM bindings vs
   rasterio's bundled libgdal) cannot produce byte-identical GTiffs, so
   **byte-for-byte `tile.raster` equality is infeasible** and is replaced by
   **decoded pixel-array parity within tolerance** (the model `bench/compare.py`
   already uses: `REL_TOL`/`ABS_TOL = 1e-3`).
2. **One input file yields one row PER TILE, not one row per file.** The reader
   splits via `BalancedSubdivision` (power-of-4 split sized by the `sizeInMB`
   option, default 16). A sub-16MB raster produces exactly one tile (one row);
   larger rasters produce N tiles. `cellid` is the literal **`-1L`**, not 0.
3. **`metadata` is an 11-key map**, not driver/width/height/count:
   `path, sourcePath, driver, format, last_command, last_error, all_parents,
   size, compression, isZipped, isSubset`.

Also: reader options are `path` / `sizeInMB` (default `16`) / `filterRegex`
(default `.*`, recursive **regex** listing — not glob); the heavy reader is
**fail-fast with no `ignoreCorruptFiles`**, so the previously-proposed
`ignoreCorruptFiles` option is dropped to match heavy exactly.

## Summary

Provide pure-Python / PySpark raster readers and a writer that are **1:1
swap-outs** for the current GDAL-backed Scala readers, built on the **Spark
DataSource V2** API exposed in PySpark 4.0 (`pyspark.sql.datasource.DataSource`).
This extends the lightweight `pyrx` family (currently function-only) with I/O,
so a user can change a single format string and get the same result through a
pure-Python path.

Raster is the focus of this branch. Vector (`vector_gbx` + named readers, via
pyogrio) and `pygx` are explicitly out of scope and get their own spec; they are
named here only to validate the naming convention.

### Naming convention (validated here, applied to vector later)

| Tier | Catch-all raster | Named GeoTIFF | Catch-all vector | Named vector |
|---|---|---|---|---|
| Heavy (Scala, existing) | `gdal` | `gtiff_gdal` | `ogr` | `shapefile_ogr`, `geojson_ogr`, `gpkg_ogr`, `file_gdb_ogr` |
| Light (this work / future) | `raster_gbx` | `gtiff_gbx` | `vector_gbx` *(future)* | `shapefile_gbx`, `geojson_gbx`, `gpkg_gbx`, `file_gdb_gbx` *(future)* |

The light tier uses a `*_gbx` form (catch-all `<domain>_gbx`, named
`<format>_gbx`) rather than the heavy tier's `_gdal` / `_ogr` engine suffix. The
new names do not collide with the Scala-registered formats, so both tiers
coexist and a swap is a one-line `format(...)` change.

## Why Python DataSource V2 (not a UDF transform)

The reader is mandated to use DataSource V2. It is also the right choice for
Serverless: a Python DataSource is pure Python, with no `_jvm` / `sparkContext`
/ `.rdd` access, so it respects the Serverless constraint that the `pyrx`
product must never set Spark config or reach into the JVM. A `binaryFile +
pandas_udf` transform was rejected because it provides no
`spark.read.format("raster_gbx")` surface and therefore is not a reader; its
file-listing/glob semantics are still a useful reference for the driver-side
partition planning below.

## Architecture

New subpackage `python/geobrix/src/databricks/labs/gbx/pyrx/ds/`, mirroring the
Scala `rasterx/ds/` tree:

- **`_base.py`** — shared scaffolding: `DataSourceReader` / `InputPartition` /
  `PartitionReader` plumbing, path expansion (glob + recursive dir listing),
  the `(source, tile)` schema sourced from `pyrx._serde.TILE_SCHEMA`, and the
  rasterio open → metadata extraction → raw-bytes read.
- **`raster.py`** — `RasterGbxDataSource`, format `raster_gbx` (catch-all,
  generic over any rasterio-readable driver).
- **`gtiff.py`** — `GTiffGbxDataSource`, format `gtiff_gbx`; extends the
  catch-all and presets `driver="GTiff"` via an options-injection hook that
  mirrors the Scala `dsExtraMap` pattern (`DataSourceExtras`).
- **`writer.py`** — `RasterGbxWriter`, the DSv2 write path; enforces the exact
  `(source, tile)` schema like the GDAL writer. Write format: `gtiff_gbx`.
- **`register.py`** — `register(spark)` that calls `spark.dataSource.register(...)`
  for each source, mirroring `functions.register` (the explicit, documented entry
  point — call surface `pyrx.ds.register.register(spark)`). Also attempted
  opportunistically on `pyrx.ds` import, guarded for no-active-session. (Not on
  bare `pyrx` import — `pyrx/__init__` does not import `ds`, to avoid a circular
  import during package init.)

### Named-reader pattern (`dsExtraMap` mirror)

The named GeoTIFF reader extends the catch-all and injects driver presets
through an options hook, exactly as the Scala `GTiff_DataSource` extends
`GDAL_DataSource` and overrides `dsExtraMap` to inject `driver -> "GTiff"`. The
catch-all stays clean and generic; named readers add presets only.

## Parity contract (the swap-out guarantee)

Resolved against the Scala `gdal` reader source (see Revision note); asserted by
a dedicated parity test.

- **Schema:** identical `StructType` to the Scala reader — `source: string` +
  `tile: struct{cellid: long, raster: binary, metadata: map<string,string>}` —
  sourced from `pyrx._serde.TILE_SCHEMA` (one definition, not a copy).
- **`tile.raster`:** a **re-encoded GTiff (DEFLATE) tile**, matching the heavy
  reader's `RasterDriver.writeToBytes` behavior (always GTiff on the wire,
  regardless of source format). Written via `rasterio` to an in-memory GTiff
  with `compress="deflate"`. **Not byte-identical to heavy** (independent GDAL
  stacks) — parity is asserted on the **decoded pixel array**, not the bytes.
- **Row cardinality + `cellid`:** one row **per tile**. The reader splits each
  source raster into tiles using a port of `BalancedSubdivision`'s power-of-4
  split sized by `sizeInMB` (default 16): a sub-`sizeInMB` raster → 1 tile/row;
  larger → N tiles/rows. Every emitted tile carries `cellid = -1` (the heavy
  literal `-1L`).
- **`metadata`:** **key-set parity is enforced** over the 11 heavy keys —
  `path, sourcePath, driver, format, last_command, last_error, all_parents,
  size, compression, isZipped, isSubset`. Values are allowed to differ where
  GDAL-Java and rasterio legitimately disagree (e.g. the `path` in-memory URI is
  implementation-specific; `driver`/`format` = `"GTiff"`, `compression` =
  `"DEFLATE"`, `isZipped`/`isSubset` = `"false"` are fixed). Light-vs-heavy
  comparison testing surfaces any value gotchas.
- **`source`:** the resolved file path string from the recursive listing,
  matching the heavy reader (`partition.filePath`).

The parity test reads the same sample file through both `gdal` and `raster_gbx`
and asserts: schema equality, equal tile/row count, `cellid == -1` on every row,
metadata **key-set** equality, and **decoded pixel-array equality within
tolerance** (`REL_TOL`/`ABS_TOL = 1e-3`, decoding both tiers' `tile.raster` with
rasterio). That test is the operational definition of "1:1 swapout."

## Distribution model

The swap-viability crux: distribution must hold as well as the heavy reader's.

- **`reader.partitions()` (driver):** recursively lists files under the input
  `path` matching `filterRegex` (default `.*`), then emits **one
  `InputPartition` per file** (carrying the file path + `sizeInMB`), so partition
  count scales with the corpus and Spark spreads `read()` tasks across executors.
  This mirrors the Scala reader's one-partition-per-file parallelism
  (`GDAL_Batch.planInputPartitions`).
- **`read(partition)` (executor):** a Python task that opens the file with
  rasterio, splits it into tiles (BalancedSubdivision port), and for each tile
  windowed-reads + re-encodes a GTiff and yields a row. Reads are local to the
  task; cross-file parallelism comes from partitioning, tile fan-out happens
  within the task (same as heavy, where tiling is inside `GDAL_Reader`).
- Pure Python throughout — no `_jvm` / `sparkContext` / `.rdd` — so it holds on
  Serverless.
- A **distribution smoke test** asserts partition count tracks file count and
  that work spreads across more than one task (no accidental coalesce-to-1).

## Writer

DSv2 write lifecycle:

- Enforce the exact `(source, tile)` schema up front — extras *or* missing
  columns both fail (matches the GDAL writer's strict schema contract).
- `write(iterator)` runs on executors: for each row, write `tile.raster` bytes
  to the resolved path (GeoTIFF for `gtiff_gbx`).
- `abort` cleans up partial outputs on task failure; `commit` finalizes.

## Error handling

- Path expansion failures (no match, unreadable dir) raise at `partitions()` on
  the driver with a clear message — fail fast before launching tasks.
- Per-file open failures in `read(partition)`: **fail-fast** (surface the
  offending path), matching the heavy reader, which propagates the
  `RasterDriver.read` exception with no `ignoreCorruptFiles` escape hatch. No
  such option is added (would diverge from heavy).
- rasterio resource hygiene: every `DatasetReader` / `MemoryFile` opened in a
  `with` block so handles close per-file (the Python analogue of the
  `releaseDataset` try/finally discipline).

## Performance validation

Extend the bench harness with a new reader mode, reusing `store` / `results` /
`compare` plumbing for one reporting surface. Two timing surfaces mirror the
function bench:

- **pure-local:** single-file open + metadata + bytes; light (rasterio) vs heavy
  (GDAL via the existing heavy path). Per-op cost, no Spark.
- **cluster spark-path:** N files distributed via `raster_gbx` vs `gdal`,
  measuring wall-clock **and** parallel efficiency (per-partition timing
  spread), per the parallel-efficiency method already used for functions.

Scale policy: spark-path at the standard row/tile scale (1000), pure-local at 1
file. Light meaningfully slower than heavy is a **deprecation blocker** — the
reader bench produces the same light-vs-heavy ratio the function bench does, and
emits a per-run summary link.

## Testing (TDD — tests are the contract)

- **Parity test:** same sample file through `gdal` and `raster_gbx` → schema eq,
  byte-eq `tile.raster`, metadata key-set eq.
- **Round-trip test:** `raster_gbx` read → `gtiff_gbx` write → re-read; assert
  tile integrity.
- **Distribution smoke test:** partition count tracks file count, work spreads
  >1 task.
- **Named-reader test:** `gtiff_gbx` injects `driver="GTiff"` and reads a
  GeoTIFF identically to the catch-all with explicit driver.
- **Serverless guard:** extend the existing no-Spark-config test to the new DS
  modules.
- **Registration test:** `register(spark)` makes all formats resolvable via
  `spark.read.format(...)`.
- Real sample data from `/Volumes/main/geobrix_samples/...`; doc tests run in
  Docker per the doc-test convention. No mocking of Spark / rasterio / file I/O.

## Packaging

No new runtime dependency: rasterio is already in the `[pyrx]` extra, and Python
DataSource V2 ships in PySpark 4.0. pyogrio stays out (deferred to the vector
spec).

## Out of scope (this branch)

- Vector readers (`vector_gbx`, `shapefile_gbx`, `geojson_gbx`, `gpkg_gbx`,
  `file_gdb_gbx`) via pyogrio — own spec.
- `pygx` grid I/O.
- These are named only to validate the `*_gbx` naming convention.

### Known limitations (follow-up)

- **Colormaps / per-band masks not propagated.** The tile re-encode carries band
  data + nodata/dtype/crs/transform, but not source colormaps or per-band
  masks/alpha. Sources relying on those will differ structurally from the heavy
  reader. Tracked for a follow-up; the catch-all otherwise handles any
  rasterio-readable driver.
- **On-disk size keys the split.** Tile count is derived from `os.path.getsize`
  (matching heavy's `Files.size` for on-disk sources). Multi-file/VRT or remote
  sources, where the heavy `memSize` differs from a single file size, may tile
  differently — out of scope for the on-disk corpus this targets.

## Verify-during-design checklist — RESOLVED (2026-06-11)

1. **`cellid` literal** → `-1L` (`GDAL_Reader.scala:30`). Emit `-1`.
2. **`metadata` key set** → the 11 keys listed above
   (`WindowedExtract.scala:108-119`).
3. **`source` path convention** → the listed file path, `partition.filePath`
   (`GDAL_Reader.scala:34`).
4. **Corrupt-file behavior** → fail-fast, no option (`GDAL_Reader.scala:17`).
5. **`tile.raster` encoding** → re-encoded GTiff/DEFLATE
   (`RasterDriver.writeToBytes`), not raw bytes — drove the byte→pixel parity
   change.
6. **Tiling** → `BalancedSubdivision.splitRasterIter` power-of-4 split by
   `sizeInMB` (default 16); port `getTileSize`/split-count for row-count parity.
