# Light Raster Writer (Python DataSource V2) + Raster I/O Docs Design

**Date:** 2026-06-11
**Branch:** `light-readers`
**Status:** Approved (design); ready for implementation plan

## Summary

Bring the pure-Python `pyrx` raster **writer** to full functional parity with the
heavy Scala `gdal`/`gtiff_gdal` writer, on both light formats (`raster_gbx`
catch-all + `gtiff_gbx` named), built on PySpark DataSource V2. Then close the
raster I/O **documentation** gap across both tiers: net-new light reader+writer
docs, and an audit of the heavy `gdal`/`gtiff_gdal` reader+writer docs so every
option (including the tile-metadata-driven encoding settings) is documented.

Companion to the reader spec `2026-06-11-light-readers-raster-design.md` (same
`pyrx/ds/` subpackage, same parity philosophy: pixel-level not byte-level).

**Implementation is split into two plans** (this one design covers both):
1. **Writer plan (first):** the light writer code parity — Architecture, Write
   contract, and Testing sections below.
2. **Docs plan (follow-up):** the Documentation section below (light reader+writer
   pages + heavy `gdal`/`gtiff_gdal` reader+writer option audit), written after the
   writer lands so the docs/doc-tests exercise the finished writer.

## Heavy writer contract (verified parity target)

Read directly from the Scala write path (`GDAL_RowWriter`, `GDAL_BatchWrite`,
`OperatorOptions.appendOptions`, `GDALTranslate`):

- **Schema:** exact `(source: string, tile: struct<cellid: long, raster: binary,
  metadata: map<string,string>>)`. Extras or missing both fail.
- **Writer `.option()`s:** `path` (required), `nameCol` (optional), `ext`
  (default `"tif"`). **That is the entire writer-option surface.**
- **Encoding comes from `tile.metadata`, NOT writer options.** `GDAL_RowWriter`
  calls `GDALTranslate.executeTranslate(localPath, ds, "gdal_translate", mtd)`
  where `mtd` is the tile's metadata map. `OperatorOptions.appendOptions` then
  reads from `mtd`: `format` (default `GTiff`), `compression` (default
  `DEFLATE`), `blocksize` (default `512`, floored to mult-of-16, clamped ≥64 and
  ≤min(w,h)), `zlevel` (DEFLATE, default `6`), `zstd_level` (ZSTD, default `9`),
  plus a `PREDICTOR` chosen by dtype (3 for float, else 2). So the on-disk
  format/compression are a property of the tile, set when it was read/produced.
- **Always re-encodes via `gdal_translate`** (never writes `tile.raster`
  verbatim), then stamps `RASTERX_<key>` for each metadata entry + `RASTERX_CELL`
  = cellid, then `FlushCache`.
- **Filenames:** `nameCol` set → `{row[nameCol]}.{ext}`; else
  `{MurmurHash3(tile)}_{pid}_{tid}.{ext}`. Flat directory under `path`, one file
  per row. (`nameCol` must be an existing column — in practice overwrite
  `source`, since the schema is fixed at 2 columns.)
- **Mode/commit:** append-only; writes directly to `path` via Hadoop copy from a
  per-row local temp; batch `commit`/`abort` are no-ops; partial writes remain on
  failure. No staging dir.
- **Format strings:** `gdal` + `gtiff_gdal` (same for read and write);
  `gtiff_gdal` = `gdal` + `dsExtraMap(driver="GTiff")`.

## Architecture & components

Rework `pyrx/ds/writer.py` into a full writer and wire it to **both** light
formats (mirroring the heavy `gdal`/`gtiff_gdal` pair):

- **`raster_gbx`** (`RasterGbxDataSource`, catch-all) gains `writer()` → the
  catch-all writer; output driver derives from `tile.metadata` (default GTiff).
- **`gtiff_gbx`** (`GTiffGbxDataSource`) `writer()` presets `driver="GTiff"` (the
  `dsExtraMap` mirror — same pattern as the reader). For the writer this means
  GTiff is the assumed/forced output driver.
- **`pyrx/ds/writer.py`** — `RasterGbxWriter(DataSourceWriter)` + a picklable
  `RasterCommitMessage`. Thin: schema check, filename derivation, mode handling,
  delegating per-tile bytes to the helper.
- **`pyrx/ds/_write.py`** (new) — pure-Python per-tile byte production
  (`tile_to_bytes(...)`): the hybrid verbatim/re-encode logic + `RASTERX_*`
  stamping. Unit-testable without Spark (parallels `_encode.py` on the read side).

## Write contract (light)

- **Schema:** exact `(source, tile)` enforced up front — extras *or* missing both
  fail (`assert_write_schema`, already present, kept).
- **Writer options:** `path` (req), `nameCol` (optional), `ext` (default `"tif"`)
  — matching heavy. No format/compression writer options.
- **Output encoding from `tile.metadata`:** driver from `metadata["driver"]`/
  `metadata["format"]` (default `GTiff`); `compression`, `blocksize`, `zlevel`,
  `zstd_level` read from metadata with the same defaults as heavy. `gtiff_gbx`
  forces driver GTiff regardless.
- **Hybrid bytes (`_write.tile_to_bytes`):**
  - Target driver **GTiff** (dominant case — `raster_gbx`/`gtiff_gbx` tiles are
    already GTiff) → write `tile.raster` **verbatim**. Pixel-identical to heavy;
    heavy's specific creation-options (TILED/BLOCKSIZE/PREDICTOR/ZLEVEL) differ,
    but our contract is decoded-pixel parity, which holds.
  - Target driver **non-GTiff** (e.g. `COG`, `PNG`) → **rasterio re-encode**:
    decode `tile.raster`, write via the target driver applying the
    metadata-derived `compress`/`blocksize`/`zlevel`/`zstd_level`, and stamp
    `RASTERX_<key>` (from `tile.metadata`) + `RASTERX_CELL` (from `cellid`) via
    `dataset.update_tags`.
  - **Verify-during-impl:** confirm rasterio's GTiff/COG creation-option names
    (`COMPRESS`, `ZLEVEL`, `PREDICTOR`, `BLOCKXSIZE/BLOCKYSIZE`, `BIGTIFF`) match
    what `appendOptions` emits, so re-encoded output is faithful.
- **Filenames:** `nameCol` set → `{row[nameCol]}.{ext}`; else an opaque unique
  name `{content-hash}_{uuid}.{ext}`. **Verify-during-impl:** whether PySpark's
  `DataSourceWriter` exposes a partition id (Scala uses `pid_tid`); if not, the
  uuid keeps names collision-free across partitions. Documented as *not*
  byte-identical to heavy's `MurmurHash3_pid_tid` — use `nameCol` for control.
- **Mode/commit:** flat dir, no staging. `append` adds files; `overwrite` clears
  the output dir on the driver (in `writer()`/`__init__`, runs once) before tasks.
  `commit` no-op; `abort` best-effort removes this run's files. (Heavy is
  append-only no-op; light adds clean `overwrite` handling.)

## Testing (TDD — tests are the contract)

Unit (`_write.tile_to_bytes`, no Spark) + integration (local Spark, `ds/`
fixtures):

- **Verbatim (GTiff):** `raster_gbx` read → `gtiff_gbx` write, no options →
  output bytes **byte-identical** to input `tile.raster`; round-trip pixels equal.
- **Re-encode (non-GTiff):** a tile whose `metadata["driver"]="COG"` (or `format`)
  → output bytes differ, decode to **same pixels** (within tol), and carry
  `RASTERX_CELL` + `RASTERX_<key>` tags.
- **`nameCol`:** `withColumn("source", …)` + `option("nameCol","source")` →
  filenames are the column values (`{name}.{ext}`).
- **`ext`:** `option("ext","tiff")` → suffix honored.
- **Strict schema:** extra/missing column fails (kept).
- **Catch-all vs named:** `raster_gbx` writer honors `tile.metadata` driver;
  `gtiff_gbx` forces GTiff.
- **Mode:** `overwrite` replaces (no stale accumulation — write-twice test);
  `append` adds.
- **Serverless guard:** `_write.py` auto-covered by the path-based scan; extend
  the explicit file list.
- **Light-vs-heavy round-trip parity (Docker/integration, skip-if-heavy-
  unavailable):** `gtiff_gbx` write → re-read decodes to the same pixels as the
  heavy `gtiff_gdal` write path.

## Documentation (both tiers — closes the flagged gaps)

Per the repo convention, **doc-tests are the documentation source** (code in
`docs/tests/python/...`, imported into `.mdx` via raw-loader; run in Docker).

**Light tier (net-new):**
- Doc-tests `docs/tests/python/readers/raster_gbx_examples.py` and
  `docs/tests/python/writers/raster_gbx_examples.py` — real reads/writes/round-
  trips on sample data, both `raster_gbx` and `gtiff_gbx`, all options.
- `.mdx` pages `docs/docs/readers/raster_gbx.mdx` + `docs/docs/writers/raster_gbx.mdx`
  importing that code; add both to `readers/overview.mdx` and `writers/overview.mdx`.
- Update `docs/docs/api/execution-tiers.mdx` — the line "native lightweight Python
  Data Source readers are not yet available" is now stale; document
  `raster_gbx`/`gtiff_gbx` read+write as available in the light tier.

**Heavy tier (option audit + fill):**
- **Readers** (`readers/gdal.mdx`, `readers/gtiff.mdx`): ensure every option is
  documented — `path`, `sizeInMB` (default 16, the tiling threshold), `filterRegex`
  (default `.*`, recursive listing), `driver`. Verify the options table is complete.
- **Writers** (`writers/gdal.mdx`): document that `compression`, `blocksize`,
  `zlevel`, `zstd_level`, `format`/`driver` are read from **`tile.metadata`** (not
  writer options), with their defaults and how to influence them (upstream
  transforms / `RST_AsFormat`), and add an explicit **`gtiff_gdal` writer**
  example (today `gtiff_gdal` appears only as a read-back format). Keep the
  existing accurate "driver comes from the tile" framing.

## Out of scope (this spec)

- Vector readers/writers (`vector_gbx`, `*_ogr`).
- Byte-identical filename parity with heavy's `MurmurHash3_pid_tid` (use
  `nameCol` for controlled names).
- New write **modes** beyond `append`/`overwrite`.

## Verify-during-impl checklist

1. rasterio creation-option names vs `OperatorOptions.appendOptions` output
   (`COMPRESS`, `PREDICTOR`, `ZLEVEL`, `ZSTD_LEVEL`, `BLOCKXSIZE/Y`, `BIGTIFF`,
   `TILED`, COG `BLOCKSIZE`).
2. Whether PySpark `DataSourceWriter` exposes a partition id (else uuid uniquifier).
3. `tile.metadata` key used for driver: `"driver"` vs `"format"` (heavy reads
   `format`; the reader emits both = "GTiff"). Honor `driver` then `format`.
4. rasterio `update_tags` lands `RASTERX_*` such that a re-read recovers them
   (parity with heavy's `SetMetadataItem`).
5. Confirm `docs/docs/writers/gdal.mdx` "ext does not change format" framing stays
   correct after documenting the tile-metadata encoding keys.
