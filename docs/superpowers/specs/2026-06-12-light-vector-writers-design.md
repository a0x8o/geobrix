# Light Vector Writers (`*_gbx`, pyogrio) — Phase 2 Design

**Date:** 2026-06-12
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan

## Summary

Phase 2 of the light vector track: **net-new** vector writers (the heavy tier has
none) as pyogrio-backed PySpark DataSource V2 writers, format names `*_gbx`. Input
is the **light vector reader's schema** (`attributes…, geom_0` WKB, `geom_0_srid`,
`geom_0_srid_proj`), so `read(ogr_gbx) → write(*_gbx)` round-trips. They also serve
as the **benchmark-corpus generator** for Phase 3 (write vector at scale / varied
formats, read back).

Five writers, mirroring the readers: `ogr_gbx` (generic) + `shapefile_gbx`,
`geojson_gbx`, `gpkg_gbx`, `file_gdb_gbx`.

## Output model — two-phase merge to one file

Vector formats are single-file datasets, but DataSource V2 `write()` runs per
partition. So (mirroring the PMTiles writer):
- **`write()`** (executor): build a pyarrow table from the partition's rows
  (attribute columns + the WKB geometry column) and `pyogrio.write_arrow(table,
  fragment_path, driver=…, geometry_name=…, geometry_type=…, crs=…)` — one
  **scratch fragment** per partition.
- **`commit()`** (driver): merge the fragments into ONE output file —
  `pyogrio.write_arrow(..., append=True)` appending each fragment's table in turn
  (or write the first, append the rest). Then clean scratch.
- **Shared filesystem is required** for the driver to read executor fragments —
  the lesson from the PMTiles writer. Scratch lives under the output parent; on a
  cluster that is a Volume/DBFS FUSE path, written **sequentially** (FUSE-safe; no
  random writes / rename). `abort()` removes scratch + any partial output.

## Geometry + CRS handling

`pyogrio.write_arrow` needs `driver`, `geometry_name`, `geometry_type`, `crs`:
- **Geometry column:** the input's geom column is `geom_0` (WKB binary) — pass it as
  the arrow geometry column. If the input geometry is WKT (`StringType`, from a
  reader run with `asWKB=false`), convert WKT→WKB via `shapely` first.
- **`geometry_type`:** **inferred** from the first non-null geometry
  (`shapely.from_wkb(g).geom_type` → `Point`/`LineString`/`Polygon`/…), with an
  optional `geometryType` writer option to override (mixed/empty inputs).
- **`crs`:** from the `geom_0_srid` column (`"4326"` → `"EPSG:4326"`); fall back to
  `geom_0_srid_proj` (PROJ4) when the authority code is `"0"`/empty.
- The `geom_0_srid` / `geom_0_srid_proj` columns are consumed for CRS, not written
  as attributes (they're reader metadata, not OGR fields).

## Options

| Option | Default | Behavior |
|---|---|---|
| `driverName` | required for `ogr_gbx`; preset by named writers | OGR driver. |
| `mode` | `overwrite` | `overwrite` only; **`append` rejected** (output is one merged file — keep it simple). |
| `geometryType` | inferred | Override the inferred geometry type. |
| `layerName` | format default | Output layer name where the driver supports it. |

## Architecture / files

Extend `python/geobrix/src/databricks/labs/gbx/ds/vector.py`:
- `OgrGbxWriter(DataSourceWriter)` — `write(iterator)` → scratch fragment via
  `write_arrow`; `commit(messages)` → merge fragments into the output file;
  `abort(messages)` → cleanup. A `_VectorCommitMessage` carries the fragment path.
- `OgrGbxDataSource.writer(self, schema, overwrite)` returns `OgrGbxWriter`
  (validates the input schema has a `geom_*` + `geom_*_srid` pair). The four named
  `*GbxDataSource` subclasses already preset `_DRIVER`; their `.writer()` inherits.
- Helpers: `_geometry_type_of(wkb)` (shapely), `_srid_to_crs(srid, proj4)` (inverse
  of the reader's `_crs_to_srid_proj`), reuse `_zip_vsi` for zipped targets.
- Pure-Python / Serverless-safe (pyogrio/pyproj/shapely lazy inside methods); the
  Serverless guard already scans `vector.py`.

## Docs

New **lightweight-only** vector writer pages (heavy has no vector writer — a
`:::note` says so), mirroring the reader format set: a generic Vector writer page +
named Shapefile/GeoJSON/GeoPackage/GeoDatabase writer pages (or one Vector writer
page with the formats — match the readers' granularity). Add them under Writers →
Named/General in the sidebar. New doc-test example files exercising a real
write→read round-trip against the corpus.

## Benchmark

Fold into the existing `--benchmark-vector` bench cell (one vector cell does readers
+ writers):
- Vector **writer** timing is **light-only** (no heavy vector writer to compare) —
  record the light write time per format.
- Add a **round-trip parity** check: write with `*_gbx` → read back with the `*_gbx`
  reader → assert the read-back feature count + geometries match the input. (This is
  the writer's correctness gate, replacing the light-vs-heavy parity used for
  readers.)
- Benchmarking.mdx: extend the "Results — vector readers" section (or add a writers
  subsection) with the writer timings + the round-trip note.

## Corpus generator (Phase 3 enabler)

A thin helper (a `gbx:data:*` command or a bench utility) that uses the writers to
generate **scaled / synthetic** vector data (N features, chosen geometry type,
chosen format) and stage it to the bench corpus — the input for Phase 3's
scaled/final benchmarking. Spec'd here as a deliverable; detailed scale knobs are
the Phase-3 plan's concern.

## Testing (TDD)

- **Round-trip parity** (local Spark) per format: build a small `(attrs…, geom_0
  WKB, geom_0_srid, geom_0_srid_proj)` DataFrame → `write.format("<fmt>_gbx").save` →
  read back with the `<fmt>_gbx` reader → same feature count + geometries +
  attributes. Covers `ogr_gbx` + the four named.
- **Two-phase merge** (local Spark): a multi-partition input writes one output file
  whose feature count equals the union of all partitions (no lost/duplicated rows).
- **CRS + geometry_type**: srid round-trips (`4326` in → `4326` out); inferred
  geometry_type matches; `geometryType` override honored.
- **`mode`**: `overwrite` replaces; `append` raises a clear error.
- **Serverless guard**: `vector.py` stays clean (no `_jvm`/`.conf.set`/`.rdd`).
- **Docker integration**: write→read round-trip against a real corpus file.

## Out of scope (later / other phases)

- **Multi-geometry-field** output (`geom_1`, …) — single `geom_0` in v1 (matches the reader).
- **Heavy vector writer** — none exists; nothing to match.
- **Phase 3 scaled benchmarking** — separate; this phase delivers the writer + the
  corpus-generator primitive it needs.

## Verify-during-impl checklist

1. `write_arrow` fragment write: the input geom column (`geom_0` WKB) maps to the
   arrow geometry column with the right `geometry_name`; attribute columns preserved;
   `geom_0_srid`/`_srid_proj` consumed for CRS, NOT written as fields.
2. `commit()` merge: `append=True` across fragments yields one file with all features;
   fragment order doesn't matter for vector (no tileid ordering); scratch cleaned.
3. Shared-FS: scratch under the output parent; sequential writes (FUSE-safe); on a
   cluster the output is a Volume/DBFS path. No `os.rename` on FUSE.
4. `geometry_type` inference handles a layer of one type; `geometryType` override for
   mixed; empty input → valid empty file or clear behavior.
5. CRS: `geom_0_srid="4326"` → `EPSG:4326` out; `"0"`/empty → fall back to proj4 or
   write CRS-less (match what the reader produced).
6. Round-trip: `read(ogr_gbx) → write(<fmt>_gbx) → read(<fmt>_gbx)` is feature- and
   geometry-stable for each corpus format.
7. Named writers preset the same `_DRIVER` as their reader counterpart (ESRI
   Shapefile / GeoJSON / GPKG / OpenFileGDB).
8. Docs: lightweight-only writer pages render + the round-trip doc-test passes; the
   "no heavyweight vector writer" note is present; internals-leak clean.
9. Bench: `--benchmark-vector` records writer timings + the round-trip parity gate.
