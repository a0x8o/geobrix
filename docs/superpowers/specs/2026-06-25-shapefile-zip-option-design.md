# Shapefile writer `zip` option — Design

**Date:** 2026-06-25
**Branch:** `beta/0.4.0`
**File:** `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`VectorGbxWriter`)

## Purpose

A shapefile is a multi-file bundle (`.shp/.shx/.dbf/.prj/.cpg`), and
`shapefile_gbx` currently writes it as a **directory** of those sidecars. Add a
`zip` write option so the bundle can be emitted as a **single, portable
`.shp.zip` file** instead. The readers already open a zipped shapefile, so the
output round-trips with no reader change.

## Option

- **`zip`** — boolean, default **`False`** (off). Parsed case-insensitively like
  the other write options (`driverName`, `layerName`, `maxRecordsPerFile`, the
  `geomCol`/`sridCol`/`projCol` set).
- **Shapefile only.** It is ignored / not offered for the other writers:
  `gpkg`/`geojson` are already single files; `geojsonl` is a directory of shards
  (zipping would defeat its splittable/parallel-read design); `file_gdb` is a
  directory but goes through the native `osgeo` path and is a **separate
  follow-up spec** (TBD from the lessons here).

## Behavior

- **`zip=False` (default):** unchanged — a **directory** holding the
  `.shp/.shx/.dbf/.prj/.cpg` bundle.
- **`zip=True`:** a **single `.shp.zip` file** containing those sidecars at the
  zip root, so a reader opens it via `/vsizip/<name>.shp.zip` (GDAL finds the
  `.shp` inside). Round-trips through `shapefile_gbx` unchanged — the reader maps
  a `.zip`-suffixed path to `/vsizip/` and the ESRI reader already accepts
  `.shp` / `.shz` / `.zip`, so `.shp.zip` is recognized.

## Output naming

`.save(path)` with `zip=True` produces `<stem>.shp.zip`:
- `.save(".../roads")` → `.../roads.shp.zip`
- `.save(".../roads.shp")` → `.../roads.shp.zip`
- `.save(".../roads.shp.zip")` → used as-is

So `zip=True` yields one file, not a directory — consistent with the
"What `.save()` produces" table on the writers overview.

## Implementation

`VectorGbxWriter.commit` already assembles the shapefile bundle on local disk and
then copies the result to the Volume target. The zip option plugs in there:

- **Preferred:** write the shapefile directly to a local path ending
  `.shp.zip`. GDAL's ESRI Shapefile driver (3.1+) writes a single compressed
  shapefile for a `.shp.zip` path, so `pyogrio.write_arrow(local_out, driver="ESRI
  Shapefile", ...)` with `local_out = "<stem>.shp.zip"` produces the single file;
  the existing copy step then byte-copies that one file to the Volume (FUSE-safe,
  via `_copy_file_to_fuse`).
- **Fallback** (if the bundled GDAL build does not write `.shp.zip`): write the
  directory bundle as today, then zip its sidecar files **flat at the zip root**
  into `<stem>.shp.zip` and copy that single file. The TDD step determines which
  path the bundled GDAL supports.

The final Volume target is `<stem>.shp.zip` (per the naming rule). Only the
`ESRI Shapefile` driver consults `zip`; other drivers ignore it.

## Testing

- `zip=True` produces a **single `.shp.zip` file** at the target (not a
  directory).
- The `.shp.zip` contains `.shp/.shx/.dbf/.prj` at the archive root.
- A `zip=True` write **round-trips** via `shapefile_gbx` to the same row count
  and geometry.
- `zip=False` still yields the directory bundle (no regression).
- Output naming: `.save("roads")` + `zip=True` → `roads.shp.zip`.

## Docs (required deliverable)

- Update the **"What `.save(path)` produces"** table in
  `docs/docs/writers/overview.mdx` to note that `shapefile_gbx` with `zip=True`
  writes a single `<name>.shp.zip` file (vs. the default directory bundle).
- Add a one-line `zip` option note on the shapefile writer page
  (`docs/docs/writers/shapefile.mdx`).

These land **with** the implementation so the docs match shipped behavior.

## Delivery

Commits to `beta/0.4.0` (flows into PR #46); pushed on the user's go. Light tier
only (the heavy tier has no shapefile writer). A `file_gdb` zip option is a
separate future spec.
