# Benchmark Phase 4 — buckets A (aggregators) + D (geometry-in) — design spec

**Date:** 2026-06-08 · **Branch:** `beta/0.4.0` · **Status:** design (pre-plan).
**Goal:** Cover the final **10** registered `rst_` functions (coverage **97 → 107/107**) — the 7 `*_agg` aggregators via a **real Spark `groupBy().agg()` harness** (user-chosen, more faithful than pure-core for the deprecation decision) and the 3 geometry-in functions via a new geometry corpus — classify divergences per best-practice, and perf-review the bucket.

## 1. The 10 functions

| Bucket | Functions | Input | Output | Heavy | Light |
|---|---|---|---|---|---|
| **A — tile aggregators (4)** | `combineavg_agg`, `merge_agg`, `frombands_agg`, `derivedband_agg` | grouped tile rows | tile | `RST_*Agg` (`TypedImperativeAggregate`, `expressions/agg/`) | pandas_udf reducers in `.agg()` (`core/agg.py` + `functions.py:2632+`) |
| **A — geometry aggregators (3)** | `rasterize_agg`, `gridfrompoints_agg`, `dtmfromgeoms_agg` | grouped geometry rows | tile | `RST_*Agg` | pandas_udf reducers (`core/agg.py`,`core/tin.py`) |
| **D — geometry-in (3)** | `rasterize`, `gridfrompoints`, `dtmfromgeoms` | one geometry / geometry-array | tile | `RST_*` | `core/features.py`,`core/tin.py` |

## 2. Key insight — leverage the EXISTING spark-path harness
- `bench/runner.py:307-468` `run_spark_path()` already: creates a `local[2]` arrow-enabled Spark session, loads a tile DataFrame on the `(cellid LONG, raster BINARY, metadata MAP)` struct schema, invokes `col_fn()` over N rows (10/100/1000/10000), times via `.write.format("noop").save()`.
- Heavy `HeavyRunner.runSparkPath(spark)` on `SilentSparkSession` (`HeavyBenchSuite extends SilentSparkSession`) registers SQL UDFs via `functions.register(spark)` and runs the columnar path.
- The synth recipes (`synth.py`: `frombands`/`combineavg`/`merge`) already produce deterministic multi-tile groups written-once / read-by-both.

**So Phase 4 EXTENDS this, it doesn't rebuild it.** New work = (a) groupBy-aggregate invocation, (b) geometry corpus, (c) `geometry` input_kind, (d) 10 FnSpecs.

## 3. Consistency vs perf — two signals, one harness
The bench's value is heavy-vs-light **consistency** AND **perf**. For aggregators both still apply (the recon's "no fingerprinting" note is rejected — consistency is the whole point):
- **Consistency** = aggregate a FIXED, deterministic group (small N) on both tiers → ONE output tile → fingerprint (raster kind) → exact/within_tol. The group must be identical + deterministic across tiers (reuse synth recipes for tile-aggs; the new geometry corpus for geometry-aggs). This answers "does light's aggregate match heavy's?"
- **Perf** = time the real `groupBy(key).agg(col_fn(...))` at scale (the existing N-row scaling + noop-write timing), on both tiers. This is the distributed-aggregation timing the user wants.

Implementation: the spark-path aggregate mode runs `df.groupBy(key).agg(col_fn(...))`, (a) collects the single output tile for the fixed-group consistency fingerprint, (b) times the scaled groupBy for perf.

## 4. New pieces (the actual work)

### 4a. Geometry corpus (new — biggest piece)
`datagen.py`/`synth.py`: synthesize deterministic geometry sets from tile extents (seeded, written-once, read-by-both — same pattern as tiles):
- **boxes** (for `rasterize`/`rasterize_agg`): N axis-aligned boxes derived from a tile's bounds (shrunk/offset for variety), each with a burn value. WKB BINARY + value DOUBLE.
- **points** (for `gridfrompoints`/`gridfrompoints_agg`): N points scattered across a tile extent (seeded), value DOUBLE.
- **z-points** (for `dtmfromgeoms`/`dtmfromgeoms_agg`): N 3D points (Z sampled from the tile raster), breaklines = NULL/empty.
- All in the **output CRS** (geometry WKB carries no CRS; must match the target `srid`). Extent/size/srid args derived from the source tile.
- A `corpus.json`-style manifest entry so both tiers read identical geometry.

### 4b. `input_kind="geometry"`
New input kind in `spec.py`/`runner.py`: feeds a geometry set (the corpus above) to a function. Bucket D uses it (single geometry / geometry-array → raster); geometry-aggregators use a grouped geometry DataFrame.

### 4c. groupBy-aggregate invocation
- **Light** (`run_spark_path`): an aggregate branch — build the N-row DataFrame (tile rows from the synth group, or geometry rows from the geometry corpus) + a group key column, `df.groupBy(key).agg(col_fn(...).alias("out"))`, collect for fingerprint + time at scale.
  - `frombands_agg`: add a `band_index` INT column (0,1,…) — heavy + light both sort ascending by it.
  - `combineavg_agg`: aligned tiles (synth `combineavg` recipe).
  - `merge_agg`: offset tiles (synth `merge` recipe).
  - `derivedband_agg`: reuse the hardcoded `_DERIVEDBAND_PYFUNC` (mean-bands) from `spec.py`.
  - `gridfrompoints_agg`: power=2.0, max_pts=12; `dtmfromgeoms_agg`: breaklines=NULL, tolerances=0.0.
- **Heavy** (`HeavyRunner.runSparkPath`/`BenchDispatch`): the matching `df.groupBy(key).agg(expr("gbx_rst_*_agg(...)"))` (the `TypedImperativeAggregate` runs as a real Catalyst aggregate). Same group key + N rows + columns.

### 4d. 10 FnSpec entries (`spec.py`)
- 7 aggregators: `modes=("spark-path",)` (aggregate-mode), `core=False`, `sources` (pyrx core/agg.py|tin.py + functions.py binding + the Scala RST_*Agg + shared serde), fingerprint = raster (fixed-group output).
- 3 geometry-in: `input_kind="geometry"`, pure-core + spark-path, fingerprint = raster.
- Bump `BenchDispatchTest` size +10 → 107.

## 5. v0.3.0 membership
Check each of the 10 against `git show v0.3.0:docs/tests-function-info/registered_functions.txt`. We are NOT changing heavy behavior (only benchmarking it), so no release-notes entries are expected — UNLESS a divergence forces a heavy fix (then document per the v0.3.0 rule; light fixes are v0.4.0-exempt). Several of these (TIN/IDW `dtmfromgeoms`/`gridfrompoints`, and likely `rasterize`) are v0.4.0-new.

## 6. Perf-review (standing directive — [[perf-parity-light-vs-heavy]])
Flag any of the 10 where light is meaningfully slower than heavy (≥~1ms AND ≥~1.5×). Distributed groupBy timing may surprise (pandas_udf serialization overhead vs Catalyst). Also run `gbx:perf:vectorscan` ([[pyrx-vectorization-standing-check]]) over any new pyrx core touched. Add findings to the perf backlog.

## 7. Risks
- **Geometry corpus is net-new** — deterministic, CRS-correct, read-by-both. Highest-effort piece.
- **pandas_udf serialization** in light groupBy (Arrow tile-struct round-trip) — may dominate light timing; that's a real, fair signal to capture.
- **Consistency group must be byte-identical across tiers** — reuse the write-once/read-both synth path; extend it to geometry.
- **CRS alignment** geometry WKB ↔ target srid.
- **Aligned-tiles / band_index / breaklines** contract details (per §4c) — get them exact or consistency diverges spuriously (cf. the Phase-3 lesson: bench-arg mismatches masquerade as divergences).

## 8. Validation (controller-orchestrated, backgrounded + 30s heartbeat)
Re-seed/scoped-bench the 10 → store; `gbx:bench:status` → **107/107**, stale=0; classify each fn's consistency (fix the wrong tier per best-practice, or document a v0.3.0 heavy change); perf-review; gates (`gbx:test:bindings`, `gbx:lint:python`, `gbx:lint:scalastyle`). **Re-bench is the verdict** (Phase-3 lesson: don't trust unit-test/hypothesis "done").

## 9. Sequencing
P4.1 geometry corpus + `geometry` input_kind → P4.2 bucket D (3 geometry-in) → P4.3 groupBy-aggregate harness + bucket A (7) → P4.4 validate (107/107) + perf-review. Out of scope: the queued tessellate mosaic-mode (#101) + viewshed reimpl (#106).
