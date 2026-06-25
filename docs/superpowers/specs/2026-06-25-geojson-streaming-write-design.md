# Streaming `geojson_gbx` write (bounded driver memory) — Design

**Date:** 2026-06-25
**Branch:** `beta/0.4.0`
**File:** `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`VectorGbxWriter.commit` / `_write_local`)

## Purpose

`geojson_gbx` (the single merged-file GeoJSON writer) assembles the whole output
on the driver: today `commit` reads every partition fragment into memory and
`pa.concat_tables(...)` them into one table, then `pyogrio.write_arrow` serializes
it. For large inputs (e.g. 782k features) that peak OOMs / times out the
single-node driver, surfacing on Serverless as a masked
`CancelledKeyException`.

Make the GeoJSON write **stream**: feed `pyogrio.write_arrow` a single Arrow
**`RecordBatchReader`** that yields batches from the fragment `.arrow` files one
at a time, so neither a concat nor the final write materializes the whole
dataset. This bounds driver memory to ~one record batch and is a single O(n)
pass — which also avoids the GeoJSON quadratic-append problem (the reason we
concat today).

## Scope (GeoJSON only)

Only the **`GeoJSON`** driver changes. The other writers are deliberately left
as-is because they're limited by something streaming can't help, or already
stream:

- **`shapefile_gbx`** — stays **concat**. A streamed/batched shapefile write
  reintroduces silent `.dbf` field-width truncation (GDAL fixes string widths
  from the first batch); concat sizes fields to the global max. Making shapefile
  streaming safe would require a separate **field-width pre-scan** pass (scan all
  fragments for max widths, create the layer, then stream-append) — a future
  enhancement, not this spec. (Shapefile also has a 2 GB-per-file cap, but that is
  a separate hard limit, not the memory concern this spec addresses.)
- **`gpkg_gbx` / `file_gdb_gbx`** — already append per fragment (bounded memory,
  no 2 GB cap).
- **`geojsonl_gbx`** — already shards per partition (no driver merge); remains
  the most scalable large-data path. This change does NOT make single-file
  GeoJSON match geojsonl's throughput — it is still single-node, just
  memory-bounded.

## Mechanism

In `VectorGbxWriter.commit`, dispatch by driver into three paths:
`GeoJSON` → stream (new), `ESRI Shapefile` → concat (unchanged),
`GPKG`/`OpenFileGDB` → per-fragment append (unchanged). (`_should_concat`
becomes `ESRI Shapefile` only; GeoJSON moves to the new streaming branch.)

The GeoJSON streaming branch:

1. **Infer geometry type + CRS cheaply.** Read only the **first non-empty
   fragment** (one partition — bounded) and derive `geom_type` + `crs` from its
   first non-null feature via the existing `_infer_geom_crs` logic. Do not
   materialize all fragments for inference.
2. **Build a chaining `RecordBatchReader`.** Open each fragment `.arrow` (Arrow
   IPC file) and iterate its record batches; for each batch drop the meta columns
   (`srid`/`proj`, via the same column set as `_drop_meta_cols`) and yield it.
   Wrap the generator with
   `pyarrow.RecordBatchReader.from_batches(stream_schema, gen)`, where
   `stream_schema` is the fragment schema minus the meta columns. All fragments
   share one explicit schema (set at executor write time by
   `_writer_arrow_table`), so batches are schema-consistent.
3. **Single streaming write to driver-local temp.**
   `pyogrio.write_arrow(reader, local_out, driver="GeoJSON",
   geometry_name=self.geom_col, geometry_type=geom_type, crs=crs)` — one call;
   GDAL pulls batches and appends features incrementally to the local file.
   (GeoJSON geometry is structural, so the per-format output-geom rename does not
   apply.)
4. **Copy to the Volume.** Byte-copy the finished local `.geojson` to the target
   with the existing FUSE-safe `_copy_file_to_fuse`.

## Volume / object-storage constraint

GDAL needs random-access (seeking) to assemble the file, which a UC Volume
(cloud object storage, no random access) does not provide — so the assembly
stays on **driver-local temp** (random-access OK), exactly as today, and only the
**finished** file is copied to the Volume with a single sequential byte copy.
The streaming change is purely how the local temp file is assembled; the
Volume-side I/O (one byte copy) is unchanged.

## Memory profile

Peak ≈ one record batch being written + one fragment's batch being read + GDAL's
internal buffers — instead of "all fragments + the concatenated table." The
inference step reads one fragment (one partition), not the whole dataset.

## Fallback

If the bundled pyogrio/GDAL `write_arrow` does not accept a streaming
`RecordBatchReader` (only a materialized table), fall back to the current
concat-then-write path for GeoJSON. A TDD step confirms which the bundled
GDAL supports; the streaming path is gated on that capability.

## Testing

- **Round-trip correctness (primary):** a multi-partition `geojson_gbx` write
  streams to one file and round-trips via `geojson_gbx` to the same row count,
  geometry types, and attribute values (incl. an all-null attribute column, to
  confirm the streamed schema typing still holds per batch).
- **Reader chaining (unit):** the batch generator chains multiple fragment
  `.arrow` files and drops the meta (`srid`/`proj`) columns — assert the yielded
  schema excludes them and the row count equals the sum across fragments.
- **Dispatch policy (unit):** GeoJSON takes the streaming path; Shapefile still
  concats; GPKG/FileGDB still append per fragment.
- **CRS/geom-type inference:** derived from the first fragment matches the value
  the old full-scan produced for the same data.
- Deterministic memory assertions are impractical (the DataSource `commit` runs
  out-of-process and AQE coalesces tiny frames), so correctness + the
  dispatch/structure unit tests are the guards, not a memory-spy.

## Out of scope

- Shapefile / GPKG / FileGDB / GeoJSONL behavior (unchanged).
- Lifting the shapefile 2 GB cap (impossible) — `geojsonl`/`gpkg` remain the
  large-data recommendation, now documented in the writers overview.

## Delivery

Light tier only (no heavy `geojson` writer). Commits to `beta/0.4.0` (PR #46);
pushed on the user's go.
