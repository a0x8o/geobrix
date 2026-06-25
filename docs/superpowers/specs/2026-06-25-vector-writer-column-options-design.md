# Vector writer column options (`geomCol` / `sridCol` / `projCol`) — Design

**Date:** 2026-06-25
**Branch:** `beta/0.4.0`
**Files:**
- Light: `python/geobrix/src/databricks/labs/gbx/ds/vector.py`
- Heavy: `src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/` (`GeoJSONL_DataSource.resolveRoles`, `GeoJSONL_RowWriter`)

## Purpose

Let the vector writers be pointed at a DataFrame's existing geometry / SRID /
proj columns **by name**, so a user does not have to rename their columns to the
`X` / `X_srid` / `X_srid_proj` convention the writers auto-derive today. Pure
input-side convenience — the on-disk output is unchanged.

## Tier scope (which writers exist)

The options are added to **every vector writer that actually writes** — in both
tiers — with identical names and semantics:

- **Light tier — all five writers:** `geojson_gbx`, `geojsonl_gbx`, `gpkg_gbx`,
  `shapefile_gbx`, `file_gdb_gbx` (shared `_writer_col_roles`).
- **Heavy tier — `geojsonl` only.** The heavy `geojsonl` writer is the only
  heavy vector *writer*; the other heavy OGR formats (`shapefile_ogr`,
  `gpkg_ogr`, `geojson_ogr`, `file_gdb_ogr`) are **read-only** (no write path),
  so there is nothing to add the options to there. (Heavy `geojsonl` already
  derives roles by the same convention via `GeoJSONL_DataSource.resolveRoles`
  and parses case-insensitive options in `GeoJSONL_RowWriter`.)

So `geojsonl` gains the options in **both** tiers (parity); the other four
formats gain them in the light tier, which is where they write.

## Options

Three new write options (camelCase, parsed case-insensitively like the existing
`driverName` / `layerName` / `geometryType` / `maxRecordsPerFile`):

| Option | Role | Required |
|---|---|---|
| `geomCol` | geometry column (Binary WKB **or** String WKT) | geometry must resolve |
| `sridCol` | CRS authority-code column (String; `"0"` = unknown) | srid must resolve |
| `projCol` | PROJ4 fallback column (String) | optional |

## Resolution

For each role: use the option if given; otherwise fall back to its
default/convention name **if that column is present**.

- **geom** = `geomCol`, else the auto-derived geom (the `X` paired with the lone
  `X_srid` column, as today). If `geomCol` is given, the srid/proj defaults are
  derived from it (`<geomCol>_srid`, `<geomCol>_srid_proj`).
- **srid** = `sridCol`, else `<geom>_srid` if present.
- **proj** = `projCol`, else `<geom>_srid_proj` if present.

**Required-ness** (the `if present at all` rule, made precise):
- **geom — required.** Cannot write without geometry; clear error if it does not
  resolve to an existing column.
- **srid — required.** Must resolve via `sridCol` or the `<geom>_srid` default;
  clear error otherwise, naming `sridCol`. (Matches today's behavior — the
  current `_writer_col_roles` already requires a `*_srid` column — and the
  `REQUIRED` annotation in the request. CRS-less output is therefore not a
  supported mode; use `"0"` for an unknown CRS.)
- **proj — optional.** Absent is fine; it is only consulted when srid is `"0"`.

So a frame with arbitrary names works with explicit options
(`geomCol="the_geom", sridCol="epsg", projCol="proj4"`), and a conventional
frame still needs zero options.

## Column semantics (unchanged from today)

- **geom encoding** is inferred from the resolved column's Spark type:
  `BinaryType` → WKB; `StringType` → WKT (WKT is converted to WKB internally
  before encoding).
- **CRS** comes from srid + proj via `_srid_to_crs`: `"EPSG:<srid>"` when srid is
  not `"0"`, else the proj4 string, else CRS-less.
- **srid / proj are CRS metadata** — they are dropped before the OGR write and
  are never emitted as attribute fields. Every other (non-geom) column is an
  attribute.

## Output geometry name

The geometry is written under the **format's conventional name**, not the input
geom column name, so output files are clean regardless of the input column
name:

| Driver | Output geometry name |
|---|---|
| `GeoJSON` / `GeoJSONSeq` | structural GeoJSON `geometry` member — no named field (N/A) |
| `ESRI Shapefile` | the shape record — no named field (N/A) |
| `GPKG` | `geom` |
| `OpenFileGDB` (FileGDB) | `SHAPE` |

On read-back, `geojson_gbx` / the other `*_gbx` readers reconstruct the
`geom_0` (+ `_srid` / `_srid_proj`) schema as today.

## Per-writer distinctives

The **column-role options behave identically** across all five writers. The only
per-format differences are independent of these options and already exist:

- **`shapefile_gbx`** — the `.dbf` format truncates attribute field names to 10
  characters (GDAL behavior); not affected by these options.
- **`file_gdb_gbx`** — writing requires the native GDAL/`osgeo` bindings
  (pyogrio's bundled GDAL ships a read-only `OpenFileGDB` driver). The column
  options resolve the same way; the geometry name default is `SHAPE`.
- **`geojson_gbx`** merges all partitions into one FeatureCollection on the
  driver; **`geojsonl_gbx`** writes one shard per partition. Both consume the
  same resolved roles.

## Implementation sketch — light tier

- Generalize `_writer_col_roles(schema)` →
  `_writer_col_roles(schema, geom_col=None, srid_col=None, proj_col=None)`:
  resolve each role per the rules above; raise clear errors when geom or srid
  cannot resolve.
- Both `VectorGbxWriter.__init__` and `GeoJSONLGbxWriter.__init__` read
  `geomCol` / `sridCol` / `projCol` from their (already-lowercased) options dict
  and pass them into `_writer_col_roles`.
- Output geometry name: a small per-driver default map; pass it as pyogrio's
  `geometry_name` (Arrow + classic paths) and as the osgeo FileGDB geometry
  field name. For structural-geometry drivers the value is inert.
- The all-null typing helper `_writer_arrow_table` is unaffected (it already
  keys off the resolved `geom_col`).

## Implementation sketch — heavy tier (`geojsonl` only)

- Generalize `GeoJSONL_DataSource.resolveRoles(schema)` →
  `resolveRoles(schema, geomCol=None, sridCol=None, projCol=None)` with the same
  resolution rules (option → else convention default if present; geom & srid
  required; proj optional). `resolveRoles` is the shared role-derivation and is
  called from two places — `GeoJSONL_Table.newWriteBuilder` and the
  `GeoJSONL_RowWriter` constructor — so both must thread the options through (the
  `WriteBuilder`/`BatchWrite`/`DataWriterFactory` chain already carries the
  options map to the `RowWriter`).
- `GeoJSONL_RowWriter` already lowercases options (`ciOptions`); read
  `geomcol` / `sridcol` / `projcol` alongside the existing `maxrecordsperfile` /
  `geometrytype` / `layername`.
- Geometry encoding (WKB/WKT auto-detect from the resolved column's Spark type)
  and the SRID/PROJ4 → `SpatialReference` mapping are unchanged. Output geometry
  is structural for GeoJSONSeq, so no output-name change applies (heavy
  `geojsonl` keeps `layerName`-or-geom-col for the internal layer name).

## Cross-tier parity

Identical option names (`geomCol` / `sridCol` / `projCol`), identical resolution
rules and required-ness, and identical CRS handling. A `geojsonl` write with the
same options + frame produces the same output on either tier (the existing heavy
↔ light geojsonl round-trip continues to hold).

## Testing

**Light:**
- Resolution: explicit options override; defaults used when omitted; arbitrary
  names (`geomCol="the_geom", sridCol="epsg"`) round-trip.
- geom required → error when it cannot resolve; srid required → error when it
  cannot resolve (no option, no `<geom>_srid`).
- proj optional → CRS from srid alone; proj4 fallback exercised when srid is
  `"0"`.
- Output geometry name per format (e.g. a GPKG write produces a `geom` geometry
  column); geojsonl round-trips via `geojson_gbx`.
- Uniform behavior across the writers (the shared `_writer_col_roles` covers all
  five).

**Heavy** (Scala, `GeoJSONLWriterTest`):
- `resolveRoles` unit cases: explicit overrides; convention defaults when
  omitted; geom/srid required errors.
- A `geojsonl` write driven by `.option("geomCol", ...)` etc. on a frame with
  non-convention column names, round-tripped via the `geojson_ogr` reader.

## Delivery

Light commits + the heavy (Scala) change land on `beta/0.4.0` (flows into the
open PR #46); pushed on the user's go. Heavy work builds + tests in the
`geobrix-dev` Docker container (Maven); a JAR rebuild is needed for cluster use.
