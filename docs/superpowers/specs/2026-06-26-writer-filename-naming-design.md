# `fileName` Option + Adaptive Output Naming for Single-File Writers — Design

**Date:** 2026-06-26
**Status:** Approved in brainstorm → spec for review
**Tiers:** light (Python/PySpark `*_gbx`) and heavy (Scala/JVM DataSource V2), with **one shared contract**.

---

## 1. Problem

GeoBrix writers that emit a **single file or single-unit archive** (a `.gpkg`, a `.geojson`, a zipped shapefile `.shp.zip`, a FileGDB `.gdb`/`.gdb.zip`, and later a `.pmtiles`) have no consistent, intuitive output-naming behavior:

- A user calling `.save("/out/shapefile-heavy")` (a stem) expects the writer to produce `/out/shapefile-heavy.shp.zip` — it does not auto-complete the extension.
- A user pointing `.save()` at an **existing directory** expects a sensible default name, not an error or a confusing failure.
- There is no way to **name** the output unit explicitly.
- These behaviors differ (or are absent) across writers and across tiers.

Most Spark writers emit a *directory of shards*; single-file/single-unit writers are the exception and need a more adaptive naming model. (The original trigger: a user ran `df.write.format("shapefile_ogr")…` — a **read-only** heavy format — and got a cryptic `stageHeadForSchemaSpark` failure instead of a clear "use the light writer" message. See §6.)

## 2. Goal

A single, tier-agnostic **output-naming contract** for single-file/single-unit writers:

1. A `.option("fileName", "<name>")` that, when present, names the output unit (extension auto-completed) and creates parent directories as needed.
2. Adaptive defaults when `fileName` is absent: complete the extension on a stem path, or derive a name when given an existing directory.
3. **Identical semantics in both tiers** wherever such a writer exists.

## 3. The contract

Inputs: the `.save(path)` argument, an optional `.option("fileName", name)`, and the writer's **canonical extension** `EXT` (per §4).

**Resolution rules** (evaluated in order):

1. **`fileName` provided** → treat `path` as the **parent directory**. Create it (and parents) if missing. Output = `path / complete(fileName)`.
2. **`fileName` absent AND `path` is an existing directory** → output = `path / complete(basename(path))` — a unit named after the directory, written **under** it.
3. **`fileName` absent AND `path` does not exist / is file-like** → output = `complete(path)`; create `path`'s parent directory if missing. (`path`'s last segment is the target name.)

**`complete(name)`** (extension completion):
- If `name` already ends with `EXT` (case-insensitive) → use as-is.
- Else append the missing part(s). For multi-part extensions this is incremental: `roads` → `roads.shp.zip`; `roads.shp` → `roads.shp.zip`; `roads.shp.zip` → unchanged.

**Validation:** if `name` ends with a **different recognized geo extension** (e.g. `.gpkg` passed to the shapefile writer, or `.geojson` to the gpkg writer), raise a clear error naming the expected `EXT` — rather than silently appending and producing `roads.gpkg.shp.zip`.

## 4. Canonical extensions (`EXT`) per writer

| Writer (light `_gbx` / heavy) | `EXT` | Notes |
|---|---|---|
| `gpkg_gbx` | `.gpkg` | single file |
| `geojson_gbx` | `.geojson` | single file |
| `shapefile_gbx` (`zip=true`) | `.shp.zip` | single archive; **non-zip shapefile stays a directory bundle — not in scope** |
| `file_gdb_gbx` | `.gdb` (or `.gdb.zip` when `zip=true`) | `.gdb` is a directory treated as a named unit |
| PMTiles (light + heavy) | `.pmtiles` | single archive — **same contract, applied when PMTiles writers adopt it** |

Sharded/directory writers (`geojsonl_gbx`, `geojsonl_ogr`, the raster `gdal`/`gtiff_gdal` tile-dir writers) are **out of scope** — `fileName` is a single-unit concept; per-tile naming there is the existing `nameCol` option.

## 5. Applicability across tiers (the "consistency" answer)

- **Light single-file vector writers** (`gpkg_gbx`, `geojson_gbx`, `shapefile_gbx`+zip, `file_gdb_gbx`): implement the contract **now**.
- **PMTiles** (exists in both tiers, single archive): the **same contract** governs `fileName`/naming; applied when the PMTiles writers take this treatment (tracked with the PMTiles writer work, not implemented here, but the contract is fixed now so both tiers match).
- **Heavy single-file *vector* writers do not exist** (heavy OGR is read-only). For those formats, "both tiers" is satisfied by: the **light** writer does the naming, and the **heavy** read-only format returns a clear error directing to the light writer (§6) — not by adding heavy vector writers (explicitly out of scope, §8).

## 6. Heavy read-only-format clear error (folded in)

`df.write.format("shapefile_ogr"|"gpkg_ogr"|"file_gdb_ogr"|"geojson_ogr"|"ogr")` currently fails confusingly: `OGR_DataSource.supportsExternalMetadata=true` makes Spark call `inferSchema` on the write path, which reads the nonexistent target → cryptic `stageHeadForSchemaSpark` `NoSuchFileException`/"Is a directory". 

**Fix:** these read-only formats should reject a write attempt with a clear, actionable message, e.g. *"`shapefile_ogr` is a read-only reader; write with the light `shapefile_gbx` writer (or `geojsonl_ogr` for sharded GeoJSONL)."* Reads are unaffected. (Exact mechanism — overriding the write path / capability surface so Spark raises before `inferSchema` — to be finalized in the plan.)

## 7. Implementation

- **Light:** one shared helper `_resolve_single_file_output(path: str, file_name: str | None, ext: str) -> str` in `python/geobrix/src/databricks/labs/gbx/ds/vector.py`, applying §3 exactly. Each of the four single-file writers calls it to compute its output target; parent-dir creation centralized there. Pure-function core (path-string logic) is unit-testable without Spark; the FS touch (exists-dir check, mkdirs) is the only IO.
- **Heavy:** a Scala mirror of the same 3-case logic for any heavy single-file writer (PMTiles), kept behaviorally identical to the Python helper — same rule order, same `complete()` semantics — so cross-tier output names match for the same inputs.
- **DRY + reuse:** the helper is the single source of naming truth; designed for reuse by PMTiles (both tiers) so the contract is defined once.

## 8. Out of scope

- **Adding heavy single-file vector writers** (`shapefile_ogr`/`gpkg_ogr`/`file_gdb_ogr`/`geojson_ogr` write). Heavy vector stays read-only; the clear error (§6) is the UX there.
- **Single-`.shp`-path *reader* sidecar staging** (the `.shx`-not-found error) — a related but separate **reader** fix, tracked independently (light + heavy shapefile readers).
- **Implementing PMTiles `fileName`** — the contract is fixed here; the application lands with the PMTiles writer work.
- Sharded/directory writers' naming.

## 9. Testing

- **Unit (light):** `_resolve_single_file_output` across the full matrix — 3 path cases × `fileName` present/absent × each `EXT` (incl. multi-part `.shp.zip`, `.gdb.zip`); the wrong-extension validation error; parent-dir creation.
- **Round-trip (light):** for each of the four writers, write with (a) a stem path, (b) an existing dir, (c) an explicit `fileName`, on a UC Volume; assert the resolved output path matches the contract and the result reads back.
- **Heavy clear-error:** asserting each read-only OGR format raises the actionable message on `.save()` (reads still succeed).
- **Cross-tier (when PMTiles adopts):** identical inputs → identical resolved names in light vs heavy.

## 10. Success criteria

- `.save("/out/roads")` on any single-file `_gbx` writer produces `/out/roads.<EXT>`.
- `.save("/out/existing_dir")` produces `/out/existing_dir/<existing_dir>.<EXT>`.
- `.option("fileName","roads")` produces `<save-dir>/roads.<EXT>`, creating dirs as needed.
- Wrong-extension `fileName` fails with a clear message.
- Writing a read-only heavy OGR format fails with a clear "use the `_gbx` writer" message.
- The naming contract is defined once and reused (light now; heavy/PMTiles to the same contract).
