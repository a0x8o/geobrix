# pyvx (light VectorX) — MVT tier design

**Status:** approved design (brainstormed 2026-06-13). Next: writing-plans → implementation.
**Branch:** `pyvx-light` (from `beta/0.4.0`).
**Survey:** `prompts/features/2026-06-12-pyvx-light-tier-survey.md`.

## Goal

Deliver the **MVT** slice of a pure-Python / PySpark **light VectorX tier** (`pyvx`) — `st_asmvt`
(aggregator) and `st_asmvt_pyramid` (generator) — that is a drop-in swap for the heavyweight
`vectorx` MVT functions and runs where the heavyweight tier can't (Serverless, ARM, standard/shared,
Lakeflow — no JAR, no init script, no native GDAL). As part of the same effort, upgrade **both
tiers** to encode MVT feature attributes with **native protobuf value types** (not stringified).

## Scope

**In scope**
- `pyvx.st_asmvt` — light MVT aggregator.
- `pyvx.st_asmvt_pyramid` — light MVT pyramid generator.
- **Native attribute typing in both tiers** — light encoder + an upgrade to the heavyweight Scala
  `MvtWriter` / `st_asmvt` / `st_asmvt_pyramid` paths (currently stringify all attributes).
- Light-vs-heavy parity + perf benches for both functions; the Benchmarking page **Vector** tab.
- `pyvx` docs page mirroring the readers/writers doc template.

**Out of scope (separate later specs)**
- TIN / elevation generators (`st_triangulate`, `st_interpolateelevationbbox`,
  `st_interpolateelevationgeom`) — the constrained-Delaunay / breakline trade-off is its own design.
- `st_legacyaswkb` (light) — deferred; revisit when the migration path is taken on.

## Why native attributes (decision record)

The MVT protobuf `Value` is a typed union (`string_value`, `int_value`, `uint_value`, `sint_value`,
`float_value`, `double_value`, `bool_value`). Stringifying everything (`pop="42"`) is valid MVT but:
(1) loses the format's typed-value efficiency (larger tiles), and (2) forces type-sensitive clients
(MapLibre/Mapbox GL data-driven styles, filters, numeric expressions) to add `to-number` casts.
Native typing (`pop=42`) is best practice. These MVT functions are **net-new, unreleased capabilities
since v0.3.0**, so changing the heavyweight behavior carries **no back-compat surface** — we make both
tiers native together, which keeps the swap invisible *and* the parity gate meaningful.

## Architecture & package layout

New light package `python/geobrix/src/databricks/labs/gbx/pyvx/`, mirroring `pyrx` / `vectorx`:

- `functions.py` — Column-API wrappers with signatures **identical to heavy `vectorx.functions`**
  (`st_asmvt(geom_wkb, attrs, layer_name)`, `st_asmvt_pyramid(geom_wkb, attrs, min_z, max_z,
  layer_name=None, extent=None)`) + `register(spark)`.
- `_mvt.py` — pure-Python encode helpers (tile-local encode + per-tile clip/encode) over
  `mapbox-vector-tile`.
- `_serde.py` — geometry WKB ↔ `shapely`; the attrs-struct → MVT-`Value` type mapping.
- `register(spark)` — **Serverless-safe wiring only**: `spark.udf.register(...)` for the aggregator,
  `spark.udtf.register(...)` for the pyramid. **No `_jvm` / `spark.conf.set` / `sparkContext` /
  `.rdd`** anywhere (hard Serverless constraint). A `test_serverless_no_spark_config`-style guard
  asserts this, as for `pyrx`.

Output tiles compose with the existing `gbx_pmtiles_agg` writer for end-to-end publishing.

## Native attribute typing contract (both tiers)

`attrs` is a Spark struct column; each field maps to the matching MVT `Value` field:

| Spark field type | MVT `Value` field |
|---|---|
| `IntegerType` / `LongType` | `int_value` (signed → `sint_value`) |
| `FloatType` | `float_value` |
| `DoubleType` | `double_value` |
| `BooleanType` | `bool_value` |
| `StringType` | `string_value` |
| any other (date, timestamp, binary, decimal, array, struct, null) | `string_value` fallback |

- Governs **both** the light encoder and the upgraded heavy `MvtWriter`, so the tiers emit
  byte-equivalent typed tiles.
- **Heavy change (in scope):** `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtWriter.scala`
  (and the `st_asmvt` aggregate + `st_asmvt_pyramid` generator paths) stop stringifying and read
  typed struct fields; update the Scala MVT tests/docs that assert stringified output.
- **Parity** is measured at the **decoded-feature level** (geometry + typed properties), like the
  PMTiles gate — not raw bytes (encoders differ in byte layout: key ordering, value dedup,
  geometry quantization).

## `st_asmvt` aggregator (grouped-agg pandas UDF)

- **Call site, identical to heavy:** `df.groupBy("z","x","y").agg(pyvx.st_asmvt(geom_wkb, attrs, "layer"))`.
- **Input contract mirrors heavy:** `geom_wkb` per-row geometry in WKB, already in **tile-local
  coordinates**; `attrs` per-row struct; `layer_name` constant.
- **Impl:** Arrow-backed grouped-agg pandas UDF — group columns arrive as pandas Series → decode WKB
  via `shapely` → build features with native-typed properties → `mapbox-vector-tile` encodes one layer
  (default extent 4096) → return MVT `BINARY`. One blob per group. Non-partial (whole group, one
  post-shuffle stage), as with the `pyrx` `*_agg` functions; a group is one tile's features.

## `st_asmvt_pyramid` generator (Python UDTF)

- **Impl:** a Python UDTF whose `eval` **`yield`s rows incrementally** (avoids fan-out OOM — do not
  build a large list) — one `(z, x, y, mvt_bytes)` per intersecting tile. Inputs EPSG:4326; for each
  feature × zoom in `[min_z, max_z]`, compute intersecting tiles, clip (`shapely`), reproject to
  tile-local extent (`pyproj` transformer built per-call/partition, not global), encode, yield.
- **Caps mirror heavy:** `max_z ≤ 20`; total tiles across the zoom range `≤ 10^6` — enforced, with a
  clear raised error on breach.
- **Output schema matches heavy** (`z, x, y, mvt_bytes`) so it feeds `gbx_pmtiles_agg` identically.
- **Call site:** registered via `spark.udtf.register("pyvx_st_asmvt_pyramid", …)`, invoked as a
  table/lateral function (`… FROM features, LATERAL pyvx_st_asmvt_pyramid(geom, attrs, min_z, max_z,
  layer, extent)`). Differs from heavy's generator-in-`select` — documented; output + composition are
  identical.
- **De-risk (first plan task):** verify the UDTF registers and runs on **Serverless + Spark Connect**
  (and the bench cluster). If unsupported, fall back to **2A** — `pandas_udf(ArrayType(tile_struct))`
  + caller `explode` — with the same output schema. The rest of the design is unchanged either way.

## Error handling & edge cases

- Empty/invalid geometry → zero rows / null (mirror heavy); a feature not intersecting a tile at a
  zoom → no row for that tile.
- Unsupported attr field types → `string_value` fallback, never an error.
- Cap breach (`max_z`, 10^6 tiles) → explicit raised error, matching heavy.
- No process-global mutable state; UDF/UDTF are pure functions of inputs (Serverless-safe).

## Testing & bench

- **TDD, real data, no mocks:** encode known features → decode with `mapbox-vector-tile` → assert
  geometry + native-typed properties.
- **Light-vs-heavy decoded-feature parity** tests per function (both tiers native): same input →
  decode both outputs → features (geometry + typed props) match.
- **Heavy-side:** update the Scala MVT tests/docs to assert native-typed values (were stringified).
- **Bench:** extend the bench harness (like the vector readers/writers) — light-vs-heavy timing +
  parity for `st_asmvt` and `st_asmvt_pyramid`; populate the Benchmarking page **Vector** tab.
- **Binding parity:** `gbx_st_*` already registered; keep `pyvx` bindings + `function-info`
  consistent (`gbx:test:bindings`).

## Dependencies

- Add `mapbox-vector-tile` (pure Python, attribute-preserving) to the `light` extra. `shapely>=2.0`
  and `pyproj` are already present.

## Risks

- **UDTF on Serverless/Connect** — **RESOLVED (2026-06-13): use approach 2B.**
  Spike (Task 1) confirmed both conditions of the decision rule:
  1. **Local run passed** — trivial `Fan` UDTF registered via `spark.udtf.register`, executed via
     `LATERAL`, yielded all expected rows under PySpark 4.0.0 / Python 3.12 (`PYSPARK_PYTHON` must
     match driver version; worker picked up system Python 3.10 until env vars were set).
  2. **Platform support confirmed** — Databricks docs list Python UDTFs as Public Preview on
     Serverless (DBR 14.3+) and supported over Databricks Connect / Spark Connect (16.4+); Unity
     Catalog UDTFs supported from DBR 17.1+. Our target (DBR 17.3 LTS) clears both thresholds.
     Sources: https://docs.databricks.com/aws/en/udf/python-udtf and
     https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/udf
  Task 5 will implement `st_asmvt_pyramid` as a Python UDTF (2B). The 2A fallback is retired.
- **Encoder byte differences** — handled by decoded-feature parity (not raw-byte) comparison.
- **Heavy MVT test churn** — switching heavy to native types will change existing Scala MVT
  assertions; updating them is in scope.

## Future exploration (note — not this phase)

Once the `st_asmvt_pyramid` UDTF proves out **solid performance and confirmed Serverless-safety**,
audit other functions for the same incremental-`yield` UDTF pattern, where it would be a memory-safer
and closer-to-heavy alternative to the current `pandas_udf(ArrayType(...))` + `explode` (array-buffering)
generators:

- **This vector phase (future specs):** the TIN/elevation generators (`st_triangulate`,
  `st_interpolateelevationbbox`, `st_interpolateelevationgeom`) are heavyweight `CollectionGenerator`s →
  prime UDTF candidates when those specs are taken on.
- **Existing raster (`pyrx`):** audit any light generator-style / fan-out functions currently realized
  as `pandas_udf(ArrayType)` + `explode` (e.g. tiling/subdivision producers) — incremental-`yield`
  UDTFs avoid buffering the whole fan-out array in the Python worker.
- **Reputational angle:** incremental-`yield`, Arrow-backed UDTFs are a documentable best practice for
  memory-safe fan-out at scale — a point to elevate in the docs/benchmarking narrative once measured.

Keep the focus on the MVT slice now; this is a tracked follow-up, not in-scope work.
