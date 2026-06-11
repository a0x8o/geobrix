# Light Readers — Raster (Python DataSource V2) Design

**Date:** 2026-06-11
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan

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
  for each source, mirroring `functions.register`. Attempted opportunistically
  on `pyrx` import, guarded for Serverless / no-active-session.

### Named-reader pattern (`dsExtraMap` mirror)

The named GeoTIFF reader extends the catch-all and injects driver presets
through an options hook, exactly as the Scala `GTiff_DataSource` extends
`GDAL_DataSource` and overrides `dsExtraMap` to inject `driver -> "GTiff"`. The
catch-all stays clean and generic; named readers add presets only.

## Parity contract (the swap-out guarantee)

Locked with evidence during implementation, asserted by a dedicated parity test.

- **Schema:** identical `StructType` to the Scala reader — `source: string` +
  `tile: struct{cellid: long, raster: binary, metadata: map<string,string>}` —
  sourced from `pyrx._serde.TILE_SCHEMA` (one definition, not a copy).
- **`tile.raster`:** the **raw original file bytes** (not a re-encode), so every
  downstream consumer (pyrx core functions, heavy Scala expressions, the writer)
  opens them identically via `rasterio.MemoryFile` / GDAL. No transcoding, no
  precision drift.
- **`cellid`:** matches the Scala reader's default for a freshly-read,
  un-tessellated tile. **Verify-during-design:** confirm the Scala `GDAL_Reader`
  literal (expected `0` or a sentinel) and emit the same.
- **`metadata`:** **key-set parity is enforced.** **Verify-during-design:**
  extract the exact key set the Scala `GDAL_Reader` populates (driver,
  width/height, band count, etc.), then map rasterio's `DatasetReader`
  attributes onto those **same keys**. Values are allowed to differ where
  GDAL-Java and rasterio legitimately disagree; the light-vs-heavy comparison
  testing surfaces any value gotchas.
- **`source`:** the resolved file-path string, matching the Scala reader's
  convention (absolute vs. as-supplied — verify).

The parity test reads the same sample file through both `gdal` and `raster_gbx`
and asserts: schema equality, byte-for-byte `tile.raster` equality, and metadata
**key-set** equality. That test is the operational definition of "1:1 swapout."

## Distribution model

The swap-viability crux: distribution must hold as well as the heavy reader's.

- **`reader.partitions()` (driver):** expands the input path (glob, recursive
  dir walk) into a file list, then emits **one `InputPartition` per file** — or,
  behind a `maxFilesPerPartition` option, small batches — so partition count
  scales with the corpus and Spark spreads `read()` tasks across executors. This
  mirrors the Scala reader's one-unit-of-work-per-file parallelism.
- **`read(partition)` (executor):** a Python task that opens each file with
  rasterio, extracts metadata, reads raw bytes, and yields rows. Reads are local
  to the task; cross-file parallelism comes from partitioning.
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
- Per-file open failures in `read(partition)`: default **fail-fast** (surface
  the offending path), with an `ignoreCorruptFiles` option mirroring Spark's
  file-source semantics for opt-in skipping. **Verify-during-design:** match the
  Scala reader's default.
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

## Verify-during-design checklist (resolve before/while implementing)

1. Scala `GDAL_Reader` `cellid` literal for a freshly-read tile.
2. Exact `metadata` key set the Scala `GDAL_Reader` populates.
3. Scala reader's `source` path convention (absolute vs as-supplied).
4. Scala reader's default behavior on corrupt/unreadable files
   (fail-fast vs skip) — match it.
