# Lightweight `gbx_pmtiles_agg` — Design

**Date:** 2026-06-14
**Branch:** `pygx-light` (add-on; not part of pygx Phase 1/2)
**Status:** Approved design — ready for implementation plan.

## Goal

Add a lightweight (pure-Python / Serverless-safe) implementation of `gbx_pmtiles_agg`
so the function — today heavyweight-only — works in the lightweight tier as a one-line
behavioural swap. A grouped aggregate that folds a group of `(bytes, z, x, y)` map tiles
into a single PMTiles v3 archive (BINARY), reusing the archive-assembly machinery already
built for the light `pmtiles_gbx` writer.

## Background — the heavy contract (parity target)

Heavy `gbx_pmtiles_agg` is a Scala `TypedImperativeAggregate`
(`src/main/scala/com/databricks/labs/gbx/pmtiles/PMTiles_Agg.scala`):

- **Args:** `(bytes BINARY, z INT, x INT, y INT, metadata_json STRING = "{}")` — 4-arg and
  5-arg forms. `z/x/y` are coerced from `Long` (PySpark sends Python `int` as `LongType`).
- **Output:** `BINARY` — a PMTiles v3 archive.
- **Build (`PMTilesV3Encoder`):** accumulate `(z,x,y,bytes)` (100 MiB cap) → sort by Hilbert
  TileID (`hilbertId(z,x,y)`) → SHA-256 dedup identical payloads → RLE root directory
  (varint delta-encoded) → 127-byte header + root dir + metadata JSON + tile data. Internal
  (directory) compression is **NONE**. No leaf directories (throws if the root directory
  would exceed 16,257 bytes). Tile type auto-detected from the first non-null payload's magic
  bytes (PNG / JPEG / WebP / else MVT).
- **Python binding** (`python/geobrix/src/databricks/labs/gbx/pmtiles/functions.py`):
  `register(spark)` registers the JVM aggregate via the `register_ds` DataSource;
  `pmtiles_agg(bytes_col, z, x, y, metadata_json=None)` returns
  `f.call_function("gbx_pmtiles_agg", ...)`.

`gbx_pmtiles_agg` is **already** in `docs/tests-function-info/registered_functions.txt`
(line 157). The light tier introduces **no new SQL name** — so binding-parity
(`gbx:test:bindings`) and `function-info.json` need no new entry. This is purely a second
implementation + register path behind the same name.

## The reuse asset

A complete light PMTiles archive assembler already exists under
`databricks.labs.gbx.ds.tiles` (it powers the `pmtiles_gbx` DataSource writer):

- `ds/tiles/backend.py` — `PMTilesBackend.assemble(sorted_tiles, header_info, out_path)`
  drives `pmtiles.writer.Writer` (`write_tile(tileid, data)` → `finalize(header_dict, metadata)`).
- `ds/tiles/_header.py` — `sniff_tile_type(data)`, `build_header_info(...)`, and
  `HeaderInfo.header_dict()` (sets `internal_compression=Compression.GZIP`).
- `ds/tiles/shard.py` — `stream_sorted(entries)` sorts `(tileid, bytes)` ascending.
- `pmtiles.tile.zxy_to_tileid` gives the spec Hilbert TileID (same ordering as heavy's
  `hilbertId`).
- `pmtiles>=3.4,<4` is already in the `[light]` extra.

`pmtiles.writer.Writer` accepts any file-like object, so it writes to an `io.BytesIO`
for an in-memory BINARY result (no temp file on the worker for the *archive* — the lib
still uses a `tempfile` internally for its tile-data buffer before finalize, which is fine).

## Architecture

PMTiles is **format-agnostic** — it archives raster *or* vector tiles (PNG/JPEG/WebP/MVT) —
so the light implementation does **not** belong to RasterX/`pyrx` or VectorX/`pyvx`.

1. **Tier-neutral home.** The light implementation lives in the existing
   `databricks.labs.gbx.pmtiles` package — the same package as the heavy binding — in a new
   submodule `_agg_light.py`. The user-facing Column wrapper `pmtiles_agg` stays in
   `databricks.labs.gbx.pmtiles.functions` (one import path for both tiers; register is the
   only difference).

2. **Shared light register helper.** `register_pmtiles_agg(spark)` (in the light module)
   does `spark.udf.register("gbx_pmtiles_agg", _pmtiles_agg_udf)`. It is called by **both**
   `pyrx.functions.register` and `pyvx.functions.register`, so registering either lightweight
   tier installs `gbx_pmtiles_agg`. It is also exposed standalone
   (`from databricks.labs.gbx.pmtiles import register_pmtiles_agg`) for users who want only it.
   `pygx` is **not** wired (grid cells, not tiles). The heavy `pmtiles.functions.register`
   (JVM `register_ds` path) is untouched.

3. **Idempotent.** Registering both pyrx and pyvx in one session re-registers the same name
   harmlessly (`spark.udf.register` overwrites). No double-registration error.

### Component / file map

| File | Responsibility |
|---|---|
| `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py` (new) | `_pmtiles_agg_udf` grouped-agg `pandas_udf` + `_assemble_archive(tiles, metadata)` BytesIO assembler + `register_pmtiles_agg(spark)` |
| `python/geobrix/src/databricks/labs/gbx/pmtiles/__init__.py` (modify) | re-export `register_pmtiles_agg` |
| `python/geobrix/src/databricks/labs/gbx/pmtiles/functions.py` (modify) | keep tier-neutral `pmtiles_agg` Column wrapper (see contingency below) |
| `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (modify) | call `register_pmtiles_agg(spark)` at end of `register` |
| `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` (modify) | call `register_pmtiles_agg(spark)` at end of `register` |
| `python/geobrix/test/pmtiles/test_agg_light_core.py` (new) | Spark-free assembler unit tests |
| `python/geobrix/test/pmtiles/test_agg_light_udf.py` (new) | registered-UDF tests via spark fixture |
| `python/geobrix/test/pmtiles/test_parity_pmtiles_agg.py` (new) | JAR-gated cross-tier decoded parity |

## The grouped-agg UDF

Mirror the pyvx `_asmvt_udf` GROUPED_AGG pattern (`(pd.Series, ...) -> bytes`, detected by
PySpark as Series-to-Scalar):

```python
@pandas_udf(BinaryType())
def _pmtiles_agg_udf(
    data: pd.Series, z: pd.Series, x: pd.Series, y: pd.Series, metadata_json: pd.Series
) -> Optional[bytes]:
    return _assemble_archive(data, z, x, y, metadata_json)
```

`_assemble_archive`:
1. Drop rows where `data` is null (mirror heavy's non-null handling). If none remain → return `None`.
2. Resolve metadata: first non-null `metadata_json` in the group, parsed JSON; default `{}`.
3. Sniff tile type from the first non-null payload (`ds.tiles._header.sniff_tile_type`).
4. Build `(zxy_to_tileid(z, x, y), bytes)` tuples; sort ascending by tileid (== Hilbert order).
5. Build `HeaderInfo` via the existing `ds.tiles` helpers (zoom range, bbox, tile type,
   `internal_compression=GZIP`).
6. Write to `io.BytesIO` through `pmtiles.writer.Writer` (the same call sequence
   `PMTilesBackend.assemble` uses); return `buf.getvalue()`.

**Memory cap.** Mirror heavy's 100 MiB accumulation cap with the same failure semantics:
raise a clear error if the group's total payload bytes exceed the cap, so the failure mode
matches across tiers. (A grouped aggregate materialises the whole group on one worker; the
cap is the guardrail.)

**Dedup.** Heavy SHA-256-dedups identical payloads; this is an internal size optimisation that
does not change decoded output. The light tier relies on the `pmtiles` lib's directory RLE and
does not add explicit dedup — decoded-tile parity is unaffected.

**Leaf directories (light advantage).** The `pmtiles` lib supports leaf directories
(`build_roots_leaves`/`optimize_directories`), so the light tier is **not** subject to heavy's
16,257-byte root-directory ceiling — it handles archives with more tiles than heavy can. Parity
is asserted only over the range where heavy succeeds; this is documented as a light-tier
capability, not a divergence to "fix."

## Column wrapper (tier-neutral)

The existing wrapper does `f.call_function("gbx_pmtiles_agg", ...)`. **Preferred:** keep it
unchanged — once the light UDF is registered under that SQL name, `call_function` invokes it in
a `groupBy().agg(...)` context for both tiers, so one wrapper serves both.

**Contingency (decided by a TDD test in Task 1 of the plan):** if `call_function` does **not**
compose with a registered pandas grouped-agg UDF inside `.agg()`, fall back to the pyvx
`quadbin_cellunion_agg` pattern — `register_pmtiles_agg` stashes the udf object and the light
wrapper calls it directly (`_pmtiles_agg_udf(...)`), while heavy keeps `call_function`. The
wrapper picks the path based on which tier is registered. The plan must verify the wrapper works
in `.agg()` on both tiers before declaring done.

## Parity contract

**Decoded-tile parity, not byte-identical** — the established contract from the `pmtiles_gbx`
writer parity test (`test_pmtiles_parity.py`). Decode both archives with
`pmtiles.reader.Reader` and compare:
- the `{(z, x, y): bytes}` tile dictionaries are equal, and
- the metadata round-trips equal.

Byte-level archives differ by design: heavy uses NONE internal (directory) compression, light
uses GZIP (the `pmtiles` lib default). Both are spec-valid and decode identically.

## Serverless / Connect safety

Light code uses only `pmtiles` + standard library + the `ds.tiles` helpers behind a
`spark.udf.register` + Column expressions. No `_jvm`, `sparkContext`, `.rdd`, or
`spark.conf.set`. Covered by the existing Serverless guard test pattern.

## Testing (TDD)

1. **Spark-free core** (`test_agg_light_core.py`): `_assemble_archive` on hand-built tile lists
   — single tile, multiple tiles across zooms, MVT and PNG payloads, metadata round-trip,
   null-payload handling, empty group → `None`, cap-exceeded raises. Decode with
   `pmtiles.reader.Reader` and assert the tile dict + metadata.
2. **Registered UDF** (`test_agg_light_udf.py`, spark fixture, Docker): register via the light
   helper; `df.groupBy(...).agg(pmtiles_agg(...))`; decode the result; assert wrapper works in
   `.agg()` (the `call_function` contingency check); assert `pyrx.register` and `pyvx.register`
   both install `gbx_pmtiles_agg`.
3. **Cross-tier parity** (`test_parity_pmtiles_agg.py`, JAR-gated): register light then heavy on
   a shared corpus of MVT tiles (include a multi-zoom group and a group with a POLYGON-derived
   MVT, not just points); assert decoded tile-dict + metadata equality.

## Bindings & docs

- **Bindings:** no new SQL name → `registered_functions.txt` and `function-info.json`
  unchanged. (`gbx:test:bindings` already passes for `gbx_pmtiles_agg`.) Confirm the
  function-info example still applies to both tiers.
- **Docs (per-function tiering):** `docs/docs/api/pmtiles-functions.mdx` is page-level
  `<Tier heavy/>` today. Change `gbx_pmtiles_agg` to `<Tier both/> <Impl groupedAgg/>` with a
  `:::note Lightweight tier` lib-attribution admonition (Powered by the **pmtiles** package),
  matching the raster/quadbin convention; leave any genuinely heavy-only pmtiles entries as
  `<Tier heavy/>` (the page can no longer be page-level heavy).
- `docs/docs/api/execution-tiers.mdx`: move `gbx_pmtiles_agg` out of the heavy-only column.
- `docs/docs/api/performance.mdx` + `benchmarking.mdx`: add the pmtiles_agg light-vs-heavy
  result (see bench) — per the standing "bench changes must update benchmarking.mdx" rule.
- Reflect light pmtiles_agg availability wherever the lightweight package set is enumerated
  (the install/quick-start tier framing already covers RasterX/VectorX/GridX; pmtiles_agg rides
  along via the pyrx/pyvx register and needs no separate landing-page bullet unless one exists).

## Benchmark

Add a `pmtiles_agg` light-vs-heavy leg to the bench harness (mirror the existing PMTiles
writer bench): a corpus of MVT tiles grouped into archives; measure grouped-agg wall time per
tier and assert decoded parity. Capture on the cluster, update `benchmarking.mdx`.

## Out of scope

- Spatial sharding / multi-archive catalog output (separate PMTiles sharding effort).
- A light PMTiles **reader** SQL function (readers live in the DataSource layer).
- `pygx` wiring (grid, not tiles).
- Any change to the heavy encoder or the heavy `register` path.

## Open / risk items

- **`call_function` vs grouped-agg pandas UDF in `.agg()`** — resolved by the Task-1 TDD test;
  contingency documented above.
- **Tile-type heterogeneity** — heavy sniffs one type for the archive from the first payload; the
  light tier follows the same single-type assumption. Mixed tile types in one group are not a
  supported input on either tier.
