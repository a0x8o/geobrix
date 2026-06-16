# Heavy `geojsonl` writer — implementation plan

> Subagent-driven, TDD. Builds the first heavyweight vector writer: a multi-file GeoJSONL
> directory writer (`geojsonl`) matching the lightweight `geojsonl_gbx` (one shard per
> partition, no driver merge, `maxRecordsPerFile`), so the "any-scale, sharded" writer is
> available in both tiers.

**Goal:** a Scala DataSource V2 writer `geojsonl` that writes a directory of newline-delimited
GeoJSONL shards (one per partition; `maxRecordsPerFile` splits a partition), NO driver merge,
round-tripping with the `geojson_ogr`/`geojson_gbx` (`multi=true`/GeoJSONSeq) directory reader.

**Architecture:** DataSource V2 write set modeled on the **PMTiles writer** (`pmtiles/PMTiles_*`)
but *simpler* — each partition's `DataWriter` writes its own shard(s) and there is **no
consolidation in `commit()`**. The per-shard encoding uses **OGR `GeoJSONSeq`** (model
`vectorx/mvt/MvtWriter.scala`, which already writes via an OGR driver): registration guarded by
`GDALManager.initOgr()`, write to node-local temp, then `HadoopUtils.copyToPath` to the Volume.

**Tech:** Scala 2.13 / Spark 4.0 / GDAL-OGR JNI, in the `geobrix-dev` Docker container (mvn).

---

## Components (new files under `src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/`)
Model the `pmtiles/PMTiles_*` set:
- `GeoJSONL_DataSource.scala` — `TableProvider`/`DataSourceRegister`, `shortName = "geojsonl"`; `META-INF/services` entry.
- `GeoJSONL_Table.scala` — `SupportsWrite`; exposes `newWriteBuilder`.
- `GeoJSONL_WriteBuilder.scala` — validates the write schema (geom + `*_srid` + attrs, mirror the light writer / PMTiles exact-schema policy); requires `overwrite` mode (reject append for v1, matching the light writer); reads the `maxRecordsPerFile` option.
- `GeoJSONL_BatchWrite.scala` — `BatchWrite`; `createBatchWriterFactory`; `commit(msgs)` does **no merge** (optionally write `_SUCCESS`); `abort` best-effort deletes shards listed in the messages. On overwrite, clear the target dir once before tasks (in the builder/driver, like the light writer's `__init__`).
- `GeoJSONL_DataWriterFactory.scala` + `GeoJSONL_RowWriter.scala` — per-partition `DataWriter[InternalRow]`: buffer rows; when the buffer hits `maxRecordsPerFile` (or at `commit()`), flush a shard: `GDALManager.initOgr()` → `ogr.GetDriverByName("GeoJSONSeq").CreateDataSource(localTmpShard)` → `CreateLayer(name, srs, geomType)` → per row build a `Feature` (geometry via `ogr.CreateGeometryFromWkb(wkb)`, attrs via `SetField`), `layer.CreateFeature(f)` → close DS → `HadoopUtils.copyToPath(localShard, OUTDIR/part-<uuid>.geojsonl, hConf)` → delete local. Unique shard name per flush (`uuid`). Return a `GeoJSONL_WriterMsg(shardPaths)`.
- `GeoJSONL_WriterMsg.scala` — `WriterCommitMessage` carrying the written shard paths.
- Register in `ds/register/RegisterBatch.scala` + `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

## Schema contract
Input = the vector writer schema the readers emit: a geometry column (WKB binary) named `<g>`, its `<g>_srid` (string/int) and `<g>_srid_proj`, plus attribute columns. Reuse/mirror the light `_writer_col_roles` logic (the column paired with `*_srid` is the geometry; `*_srid_proj` is proj4; the rest are attributes). SRS from the srid/proj.

## Tests (TDD — Scala, run in Docker via `gbx:test:scala --suite '...geojsonl...'`)
- One shard per partition: write a small DF repartitioned to N → assert OUTDIR has exactly N `.geojsonl` shards (count matches expected — explicit).
- `maxRecordsPerFile`: M rows in one partition with k → exactly `ceil(M/k)` shards.
- Round-trip: read OUTDIR back with `geojson_ogr` (`multi=true`) → feature count == input; geometry + an attribute value match.
- Overwrite clears stale shards; append rejected.
- (Concurrency sanity: 2+ partitions write concurrently without a driver-registry race — covered by a multi-partition write test.)

## Verification
- `gbx:lint:scalastyle` (CI gate) + the Scala suite green in Docker.
- Build + stage the JAR (`gbx:data:push-jar`), restart the bench cluster, and **round-trip on the cluster**: write a few-M-row table → `geojsonl` directory on a Volume → read back via `geojson_gbx(multi=true)` → **confirm shard count == expected** and row count matches.
- Optionally bench it (light vs heavy geojsonl writer) like the other writers.

## Docs
- Add a **Heavyweight** tab to `docs/docs/writers/geojsonl.mdx` (`geojsonl` format name; same multi-file/`maxRecordsPerFile` semantics; classic-x86 + JAR requirement note). Keep the page template (Options near top, How-it-scales, Next Steps last).

## Risks / notes
- OGR thread-safety: register only via `GDALManager.initOgr()`; each task writes its own node-local file via the `GeoJSONSeq` driver (not the shared MEM driver), so no `GetDriverByName` race. [[gdal-ogr-register-via-guard]] [[vectorraster-bridge-not-threadsafe]]
- Node-local → Volume write-back via `HadoopUtils.copyToPath` (sequential; FUSE-safe). Each tests.jar change needs a cluster restart [[jar-stage-before-cluster-start]].
- First heavy vector writer → sets the pattern for future heavy vector writers (shapefile/gpkg).
