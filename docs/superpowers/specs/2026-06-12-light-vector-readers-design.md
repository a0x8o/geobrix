# Light Vector Readers (`*_gbx`, pyogrio) — heavy-parity Design

**Date:** 2026-06-12
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan

## Summary

Bring the **lightweight** (pure-Python/PySpark, JAR-free, Serverless-safe) reader
tier to **parity** with the heavyweight tier for **vector** data. Heavy has five
OGR-backed vector readers; light has none. Build the five light equivalents as
**pyogrio**-backed PySpark DataSource V2 readers that emit the **exact same schema**
as the heavy readers, so swapping tiers is a one-line `format(...)` change (the same
guarantee the raster tier already provides).

**Readers only.** The heavy tier has **no** vector *writers* in 0.4.0, so vector
write-parity is a no-op — out of scope.

## Scope — the five readers

| Light format (`_gbx`) | Heavy equivalent | OGR driver |
|---|---|---|
| `ogr_gbx` | `ogr` | auto-detect (or `driverName`) |
| `shapefile_gbx` | `shapefile_ogr` | `ESRI Shapefile` |
| `geojson_gbx` | `geojson_ogr` | `GeoJSON` / `GeoJSONSeq` |
| `gpkg_gbx` | `gpkg_ogr` | `GPKG` |
| `file_gdb_gbx` | `file_gdb_ogr` | `OpenFileGDB` |

`ogr_gbx` is the generic core; the four named readers are **thin presets** that set
`driverName` (mirroring how `gtiff_gbx` presets the raster driver over `raster_gbx`).

## Output schema — exact heavy parity

Match the heavy OGR reader's schema column-for-column so downstream code is
identical (verified by a light-vs-heavy parity test):

- Per geometry field `j`:
  - `geom_j` — `binary` (WKB) when `asWKB=true` (default); `string` (WKT) when `asWKB=false`.
  - `geom_j_srid` — `string` (authority code, e.g. `"4326"`; empty if unknown).
  - `geom_j_srid_proj` — `string` (PROJ4 definition; empty if unavailable).
- One column per OGR attribute field, named as in the source, with Spark types
  matching heavy's inference (integer/long, double, string, boolean, date/timestamp).
- **v1 supports a single geometry field (`geom_0`)** — the common case. Multi-geometry
  sources (`geom_1`, `geom_2`, …) are a documented follow-up (see Out of scope).

Implementation: `pyogrio.read_info(path, layer=…)` provides fields + geometry type
+ CRS for `schema()` without a full read; `pyogrio.read_dataframe` / `read_arrow`
provides rows; `shapely.to_wkb` encodes geometry; the CRS yields the SRID
(authority code) and PROJ4 string. Spark types are mapped from the OGR/arrow field
types to match heavy's column types — the **parity test gates any divergence**.

## Options — heavy parity

| Option | Default | Behavior |
|---|---|---|
| `driverName` | auto (from extension) | Explicit OGR driver (named readers preset it). |
| `asWKB` | `"true"` | Geometry as WKB binary (`true`) or WKT string (`false`). |
| `layerN` | `"0"` | Layer index for multi-layer sources. |
| `layerName` | `""` | Layer name (overrides `layerN`). |
| `chunkSize` | `"10000"` | Features per partition (parallel read). |

## Parallelism / partitioning

Partition a source into `chunkSize`-feature slices using pyogrio's `skip_features`
+ `max_features`, so large files read across Spark tasks (mirrors the heavy reader's
chunked read). Each `InputPartition` reads its `(skip, count)` slice via pyogrio and
yields rows. Layer count comes from `read_info`. This follows the raster reader's
"one slice per partition, picklable partition object, no Spark/JVM refs in `read()`"
pattern (Serverless-safe).

Zipped sources in the corpus (`.shp.zip`, `.gdb.zip`) are read via OGR's
`/vsizip/…` virtual path (pyogrio supports it), matching heavy's behavior.

## Architecture / files

One new module `python/geobrix/src/databricks/labs/gbx/ds/vector.py` holds all five
readers + helpers (the four named readers are ~3-line presets, so a separate module
isn't warranted):
- `OgrGbxDataSource` (`name()`→`ogr_gbx`, write-less; `reader()`),
  `OgrGbxReader(DataSourceReader)` (schema from `read_info`, `partitions()` =
  chunk slices, `read(partition)` = pyogrio slice → WKB rows).
- `_vector_schema(...)` — the heavy-parity schema builder — plus inline helpers for
  CRS→(srid, proj4) and arrow/OGR→Spark type mapping.
- `ShapefileGbxDataSource`, `GeoJSONGbxDataSource`, `GpkgGbxDataSource`,
  `FileGdbGbxDataSource` — subclasses of `OgrGbxDataSource` overriding `name()` +
  presetting `driverName` (and `multi=true` for GeoJSONSeq where heavy does).
- `register.py` — add the five sources to `_SOURCES`.

Pure-Python; Serverless-safe (no `_jvm`/`.conf.set`/`.rdd`/`sparkContext`). The
existing Serverless guard (`test/pyrx/test_serverless_no_spark_config.py`, scans
`gbx.ds`) covers the new modules — extend its explicit file-list assertion.

## Dependency

Add `pyogrio` to:
- `pyproject.toml` `[light]` extra — as a **range** (e.g. `pyogrio>=0.8,<1`).
- the hash-pinned locks `requirements-pyrx-ci.txt` (light CI) and
  `requirements-dev-container.txt` (doc-tests) — **pinned + hashed**, regenerated via
  `uv pip compile --generate-hashes --index-url <db pypi proxy>` (same pattern as
  `pmtiles`). pyogrio vendors its own `libgdal`; pin a version consistent with the
  rasterio already in the stack.

## Docs (slots into the tabbed structure)

The five vector reader pages (`readers/ogr`, `readers/shapefile`, `readers/geojson`,
`readers/geopackage`, `readers/filegdb`) currently are heavyweight-only with a
`:::note No lightweight equivalent yet` admonition. For each:
- Add a **Lightweight tab** (first/default) documenting the `*_gbx` reader, using the
  same `<Tabs groupId="gbx-tier" queryString="tier">` convention; move the existing
  heavy body into a **Heavyweight tab**.
- **Remove** the "no lightweight equivalent yet" note.
- New doc-test example file per reader under `docs/tests/python/readers/` (e.g.
  `shapefile_gbx_examples.py`) exercising a real read against the corpus, imported
  via raw-loader. Tests execute real reads with assertions (doc-tests-are-the-source).

## Benchmark (per the bench-each-reader/writer requirement)

For each of the five readers, a **light-vs-heavy** comparison (timing + parity),
mirroring the PMTiles writer bench:
- Reuse `bench/readers.py::run_format_read` (light `*_gbx` vs heavy `*_ogr`) for timing.
- Add a **parity** assertion: both tiers read the same source to the same row count
  and equivalent geometry/attribute values (compare WKB-decoded geometries +
  attribute columns).
- Wire a `--benchmark-vector` knob in `bench/cluster.py` + the launcher (a
  `_CELL_VECTOR` cell), mirroring `--benchmark-readers` / `--benchmark-pmtiles`.
- Add a "Results — vector readers" section to `docs/docs/api/benchmarking.mdx`.

## Testing (TDD)

- **Schema-parity units** (local, no Spark needed for the schema builder): assert
  `_vector_schema` produces the exact heavy column set/types for each corpus format.
- **Read round-trip** (local Spark): `ogr_gbx` + each named reader over the corpus —
  correct row counts, valid WKB (round-trips through `shapely.from_wkb`), SRID/PROJ4
  populated, attribute columns present with expected types; `asWKB=false` yields WKT;
  `layerN`/`layerName` select layers; `chunkSize` splits into multiple partitions and
  the union equals the whole file.
- **Light-vs-heavy parity** (Docker/integration, skip-if-heavy-unavailable): same
  source → light `*_gbx` vs heavy `*_ogr` produce identical schema + row set +
  decoded geometries. This is the parity gate.
- **Serverless guard**: the new `vector.py` (+ named module) are in the scan and
  contain no forbidden Spark-config/JVM calls.

## Out of scope (later)

- **Vector writers** — heavy has none in 0.4.0; nothing to match.
- **Multi-geometry-field** sources (`geom_1`, …) — v1 is single-geom parity.
- **GridX** readers/writers — separate tier, not part of the readers/writers section.
- A `pyvx` *functions* API (vector transforms) — this spec is DataSource readers only.

## Verify-during-impl checklist

1. `_vector_schema` matches the heavy OGR schema EXACTLY for each corpus format
   (names + Spark types) — diff against a heavy read in the parity test; fix the
   OGR/arrow→Spark type map until it matches (int width, date/timestamp, bool).
2. CRS→`srid` is the authority code STRING (e.g. `"4326"`), not an int; `proj4`
   empty-string when unavailable (match heavy's nullability/empties).
3. `chunkSize` partitioning: `skip_features`/`max_features` slices are exhaustive +
   non-overlapping; union row count == `read_info` feature count.
4. Zipped shapefile/filegdb (`.shp.zip`/`.gdb.zip`) read via `/vsizip/`; named
   readers handle the corpus's zipped paths like heavy.
5. `asWKB=false` (WKT) path produces `geom_j` as `string`.
6. Serverless-safe: pyogrio/shapely calls only; no `_jvm`/`.conf.set`/`.rdd`.
7. Docs: each vector page's lightweight tab renders + its doc-test reads the corpus;
   the "no lightweight equivalent yet" note is removed; tabs are lightweight-first.
8. Bench parity cell asserts light==heavy decoded rows; benchmarking.mdx section added.
9. pyogrio in `[light]` (range) + both locks (pinned+hashed); light CI + dev image
   install it.
