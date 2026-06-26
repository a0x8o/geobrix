# Coherent UC Volume (`/Volumes`) access across the heavyweight tier — Design

**Date:** 2026-06-26
**Branch:** `beta/0.4.0`
**Status:** approved (design agreed in session; user approved proceed + revive `rst_fromfile`).

## Problem

UC Volumes (`/Volumes/...`) + Unity Catalog are a primary customer selling point, but the
0.4.0 heavyweight tier has **inconsistent** `/Volumes` support, and the documented rationale
for one decision (`rst_fromfile` removed from heavy) is **factually wrong**. We want one
coherent story: every heavy reader and writer reads/writes `/Volumes`, and our docs state the
*accurate* constraint.

## Root-cause model (empirically established on cluster 0519)

The credential issue is **driver-side raw Hadoop FS metadata** on `/Volumes`. The UC FUSE mount
is credential-gated; the Spark **analyzer thread** (DSV2 `inferSchema`) AND the **read-planning**
driver thread (`Batch.planInputPartitions`) lack the credential, so raw Hadoop FS
`getFileStatus`/`listStatus`/`listFiles` on `/Volumes` throws `FileNotFoundException` (it
delegates to `RawLocalFileSystem`'s POSIX stat wrapped in `WSFSCredentialForwardingHelper`,
which has no token on those threads).

**What reliably works on `/Volumes` (proven by probes):**
- **Spark FileIndex listing** — `spark.read.format("binaryFile").load(dir).inputFiles` — on the driver (forwards the credential). `binaryFile` *content* reads do NOT (use POSIX instead).
- **POSIX `java.io.File` / `Files.readAllBytes`** — on the driver-REPL thread AND executors.
- **Executor reads** — `NodeFileManager.readRemote` (Hadoop `file:` FS) and POSIX both work in Spark *tasks* (the executor task carries the credential): proven by `geojson_ogr` reading a 782,054-row `/Volumes` dataset and an executor reading a 6.9 MB raster.
- **Scheme:** `file:/Volumes/...` is correct (bare `/Volumes` and `dbfs:/Volumes` both fail `INVALID_DBFS_MOUNT`).

The credential-aware **toolkit** (already added in commit `ce61e84`):
- `HadoopUtils.listDataFilesSpark(spark, path)` — FileIndex listing (`.gdb`/`.zip` returned as-is).
- `HadoopUtils.stageHeadForSchemaSpark(spark, head, candidates)` — POSIX read (+ executor fallback) of the schema file/sidecars to a local temp.

## Per-component status + remediation

| Component | Status | Action |
|---|---|---|
| OGR readers `inferSchema`/planning (geojson/shapefile/gpkg/ogr) | fixed (ce61e84) | none |
| OGR/GDAL executor reads (`readRemote`→local) | works | none |
| `geojsonl` writer on `/Volumes` | works (proven) | none |
| **`GDAL_Batch.planInputPartitions`** (`gdal`/`gtiff_gdal` readers) | broken — `listAllHadoopFiles` (raw Hadoop FS) | **Task 1** |
| **OGR `.gdb`/`.gdb.zip`/`.zip` `inferSchema`** (`file_gdb_ogr`) | broken — `NodeFileManager.readRemote` on driver | **Task 2** |
| **`rst_fromfile`** removed from heavy | rationale false | **Task 3** (revive) |
| `pmtiles` / `gdal` writer driver-side commit on `/Volumes` | untested | **Task 4** (probe/harden) |
| docs/comments stating the wrong `rst_fromfile` reason | wrong | **Task 5** |

### Task 1 — `GDAL_Batch` listing
Replace `HadoopUtils.listAllHadoopFiles(inPath, hConf, regex)` (raw Hadoop FS recursive list) in
`GDAL_Batch.planInputPartitions` with a credential-aware FileIndex listing. Add
`HadoopUtils.listDataFilesSparkRecursive(spark, path, regexFilter)` (FileIndex via `binaryFile`
with `recursiveFileLookup=true`, applying the same regex/empty-file filter `listAllHadoopFiles`
applied), and use it. Keep `listAllHadoopFiles` for non-`/Volumes` callers if simpler, but route
the GDAL reader through the credential-aware path. Validate `gtiff_gdal`/`gdal` read a `/Volumes`
raster dir + single file.

### Task 2 — OGR `.gdb`/`.zip` schema inference
In `OGR_DataSource.inferSchema`, the `isGdbLike` branch currently calls
`NodeFileManager.readRemote(headPath)` (driver raw Hadoop FS). Replace with credential-aware
staging: for a `.gdb` **directory**, stage its whole tree to a local temp via POSIX/executor
(extend `stageHeadForSchemaSpark` to handle a directory dataset, or add
`stageDatasetDirForSchemaSpark`), then OGR-open the local copy. Validate `file_gdb_ogr` reads a
`/Volumes` `.gdb` (and `.gdb.zip`).

### Task 3 — Revive `rst_fromfile` in the heavyweight tier
Add the heavyweight Scala expression `RST_FromFile` (in `rasterx/expressions/constructor/`),
mirroring the light pyrx semantics + the existing `RST_FromContent` tile-construction. It reads
per-row **on executors** via the proven path: `NodeFileManager.readRemote(path)` (or
`RasterDriver.read`, which stages `/Volumes`→`/tmp` then `gdal.Open`s the local copy) → build the
tile struct. Register it in `rasterx/functions.scala` (remove the "NOT registered" block).
**Binding parity (enforced):** add the Scala `override def name` literal, the
`registered_functions.txt` entry, the `function-info.json` example, and confirm the Python
binding (it already exists as the pyrx UDF name — ensure the heavy registration coexists so SQL
`gbx_rst_fromfile` resolves to the JVM expression on heavy clusters). Add a Scala expression test
(read a LOCAL test raster by path → identical decoded pixels as `rst_fromcontent`). Validate on
0519: `gbx_rst_fromfile` on a `/Volumes` raster path returns a tile.

### Task 4 — Writer driver-side commit on `/Volumes`
Probe on 0519: do `pmtiles` and `gdal`/`gtiff_gdal` writers write to a `/Volumes` target
end-to-end (esp. `PMTiles_BatchWrite.commit`, which reads scratch files + writes the final
`.pmtiles` via driver `file:` FS)? If a driver-side raw Hadoop FS metadata/read on `/Volumes`
fails, harden it the same way (FileIndex listing / POSIX). `geojsonl` already works, so the
write-commit driver context may be fine — confirm, don't assume.

### Task 5 — Docs + comments correction
Correct every place stating "the executor JVM cannot read `/Volumes`" to the accurate model:
*driver-side raw Hadoop FS metadata on `/Volumes` lacks the FUSE credential; use Spark FileIndex
listing + POSIX/executor reads.* Files: `rasterx/functions.scala`, `util/HadoopUtils.scala` (the
`withFileSystem` NOTE), `docs/docs/api/raster-functions.mdx`, `docs/docs/beta-release-notes.mdx`,
the issue-34 references, and the test comments (`udfs.scala`, `BenchDispatch.scala`,
`ConstructorExpressionsTest.scala`). State that `rst_fromfile` IS available in both tiers.

## Validation
Each task validated on cluster 0519 (warm) via a notebook job reading/writing the relevant
`/Volumes` corpus (rasters under `…/data/out/netcdf-gtiff`, `.gdb.zip` under `…/data/gdb`,
geojsonl under `…/data/out/geojsonl`). JAR built+staged via `gbx:data:push-jar` to the
init-script volume; cold-restart 0519 to load.

## Out of scope
- Light tier (pyrx) — already FUSE-native; unchanged.
- Non-`/Volumes` paths (DBFS/Workspace/local) — unchanged behavior.

## Delivery
Commits to `beta/0.4.0` (flows into PR #46). Validated on 0519 before the batch is pushed.
