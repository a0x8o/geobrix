# GeoBrix benchmark suite — design spec

**Date:** 2026-06-05 · **Branch:** `pyrx-0.4.0` · **Status:** design approved, pre-implementation.
**Goal:** a benchmark suite that compares the **heavyweight** (Scala/JNI RasterX, `gbx_rst_*`) and **lightweight** (pyrx, rasterio-backed) APIs on compute time, at various scales, runnable (1) locally — heavyweight in the `geobrix-dev` Docker container, lightweight in an isolated `uv` venv — and (2) on a Databricks cluster for same-hardware comparison. The suite **generates its own data** at scale (compute time matters more than pretty output), keeping each tile valid (controlled nodata, plus variety in SRID, pixel values, resolution, extent, bands).

Related prior work: [`2026-05-29-rasterio-lightweight-api.md`](2026-05-29-rasterio-lightweight-api.md) (pyrx design + the pure-Python core), [`2026-06-05-bundling-gdal-natives-assessment.md`](2026-06-05-bundling-gdal-natives-assessment.md) (why pyrx stays pure-Python).

---

## 1. Scope & key decisions

- **Both timing models, reported separately:**
  - **pure-core** — the raster algorithm only, no Spark. pyrx: call `pyrx.core.<fn>(ds)` on a rasterio dataset. heavyweight: construct the `rst_*` Catalyst expression and call `eval` on a single deserialized tile `InternalRow` in-JVM (no Spark scheduler). Asymmetric mechanism, but **both read the identical generated tile bytes**, so the algorithm comparison is fair.
  - **spark-path** — the `rst_*` Column on a DataFrame of tiles, materialized: heavyweight JVM Catalyst expression vs pyrx pandas/Arrow UDF. Captures serialization + framework overhead.
- **Function coverage:** **all `rst_*` functions present in both APIs** (~100), driven by a registry, with `--functions`/`--category` filters to run a slice.
- **Scale = two independent ladders, not a full matrix:**
  - pure-core: tile dimensions `256² → 512² → 1024² → 2048² → 4096²` × bands `{1, 4, 13}`.
  - spark-path: row count `10 → 100 → 1k → 10k` at a fixed mid tile size (default `1024²`; cluster pushes higher via `--rows`).
- **Lightweight isolation:** a dedicated `uv` venv (no system-site-packages), locked from the `pyrx` optional-deps in `pyproject.toml`. This becomes the canonical runtime for pyrx **benchmarks and other pyrx tests** (existing `gbx:test:pyrx` is routed through it).
- **Compute-time first:** machine-readable results (JSONL local, Parquet/Delta on cluster); a comparison table + short markdown summary, **no charts by default**.
- **Honesty invariants** (encoded in the data, not just docs): same corpus/args/iteration-counts across runners; `where ∈ {docker, venv, cluster}` so laptop Docker-vs-venv numbers are never mistaken for same-hardware truth; `na_by_design` rows emitted (not dropped) so coverage gaps are legible; runners executed **sequentially** so they don't contend for CPU and skew timings.

## 2. Architecture (Approach A — two native runners over a shared spec)

```
                 ┌───────────────────────────────────────────────┐
                 │  shared spec (language-neutral)               │
                 │   • function registry: name, category,        │
                 │     input_shape, args, modes(pure-core|        │
                 │     spark-path|both), applies_to               │
                 │   • corpus manifest (corpus.json):            │
                 │     every tile's path + properties + scale pt │
                 └───────────────────────────────────────────────┘
                       ▲                              ▲
        reads same     │                              │   reads same
        corpus + spec  │                              │   corpus + spec
   ┌───────────────────┴────────┐        ┌────────────┴───────────────────┐
   │ Scala runner (geobrix-dev  │        │ Python runner (uv venv)         │
   │ Docker, JVM/JNI)           │        │                                 │
   │  pure-core: expression.eval│        │  pure-core: pyrx.core.fn(ds)    │
   │  spark-path: local Spark   │        │  spark-path: local PySpark+UDF  │
   └───────────────┬────────────┘        └───────────────┬─────────────────┘
                   │  result rows (common schema)         │
                   └──────────────┬───────────────────────┘
                                  ▼
                   orchestrator (gbx:bench:*) → merge → comparison.csv + summary.md
                                  │
                                  └── cluster phase: same runners, notebook job,
                                      results → bench_results Delta table
```

The corpus is generated **once** and read by both runners — the same-bytes invariant is the foundation of fairness. No cross-runner coupling beyond the shared spec + result schema.

## 3. Data generator

**Module:** `python/geobrix/src/databricks/labs/gbx/bench/datagen.py` · **Command:** `gbx:bench:gen-data` (runs in the venv, rasterio-based).

Deterministic and **seeded** — re-running with the same seed + generator version produces identical bytes. Emits a `corpus.json` manifest listing every tile's path, properties, and which scale point/axis it belongs to. Runners read the manifest and never re-derive.

Per-tile validity knobs:

| Property | Behavior |
|---|---|
| **NoData** | `--nodata-frac` accepts the full range (≈0.0–0.9) and a **list to sweep** (e.g. `0.02,0.25,0.5`), so the corpus spans minimal-nodata and nodata-heavy tiles. Placement modes: thin border / sparse mask / clustered holes. Default minimizes nodata; high fractions are allowed on purpose. |
| **SRID** | Cycled across EPSG:4326, 3857, 32618 (UTM 18N), 27700 (BNG); each tile gets a correct affine for that CRS's units. `--srids` to override. |
| **Pixel values** | Seeded-RNG generative patterns: gradients, noise, sinusoids, and **band-correlated spectral-like** values so NDVI/indices produce non-degenerate output. Range matched to dtype. |
| **Dtype** | Swept `uint8`, `int16`, `float32` (`--dtypes`). |
| **Resolution** | Varied per CRS, affine kept consistent with extent + dimensions. |
| **Extent** | Realistic per-CRS placements; deliberate overlap included for `merge`/`combineavg` aggregators. |
| **Bands** | `1 / 4 / 13` (`--bands`). |

**Scale wiring:** `--tile-px 256,512,1024,2048,4096`, `--bands 1,4,13`, `--rows 10,100,1000,10000`, `--seed`, `--out`.

**Validity gate (post-gen):** assert every tile opens, has the declared CRS/bands/dtype, and nodata fraction is within a configurable warn-threshold (warn, not hard-fail, so intentional high-nodata tiles pass). Fail fast before benchmarking on bad data.

**Locations:** corpus under `sample-data/Volumes/.../bench-corpus/` locally; pushed/generated to a UC Volume for cluster runs.

## 4. The two runners

Both speak the shared spec, run **N warmup + M measured iterations** per (function × scale point), and report `median/min/p90` to absorb JIT and cache effects. Each function's registry `modes` field marks it `pure-core` / `spark-path` / `both`; aggregators (`_agg`) and multi-output tiling generators are `spark-path` only, and a skipped mode produces a `na_by_design` row recording why.

**Python runner** — `python/geobrix/src/databricks/labs/gbx/bench/runner.py`, executed inside the `uv` venv:
- **pure-core:** `_serde.open_tile(bytes)` → `pyrx.core.<module>.<fn>(ds, **args)`. No Spark.
- **spark-path:** local `SparkSession`, DataFrame of N tiles, apply the `rst_*` Column wrapper (pandas/Arrow UDF), force materialization, time end-to-end.

**Scala runner** — `src/main/scala/com/databricks/labs/gbx/bench/Runner.scala` (a `main`, invoked via Maven/`spark-submit` inside `geobrix-dev`):
- **pure-core:** deserialize the manifest tile to the heavyweight tile `InternalRow`, construct the `rst_*` expression, call `eval` directly — algorithm + GDAL-JNI time, no Spark scheduler.
- **spark-path:** local single-node `SparkSession`, DataFrame of N tiles, registered `gbx_rst_*` expression, materialize, time it.

Each runner writes a JSONL/Parquet shard; no cross-runner coupling.

## 5. Results schema & metrics

One flat, language-neutral schema (JSONL local, Parquet/Delta cluster) so local and cluster rows stack into the same table.

| Field | Meaning |
|---|---|
| `run_id` | one id per orchestrator invocation |
| `api` | `heavyweight` \| `lightweight` |
| `fn`, `category` | e.g. `rst_slope`, `terrain` |
| `mode` | `pure-core` \| `spark-path` |
| `tile_px`, `bands`, `dtype`, `srid` | input scale/variety coordinates |
| `rows` | tiles processed (1 for pure-core; ladder value for spark-path) |
| `nodata_frac` | actual nodata fraction of the input |
| `warmup_iters`, `measured_iters` | iteration counts |
| `median_ms`, `min_ms`, `p90_ms` | timing distribution |
| `throughput_mpix_s`, `throughput_rows_s` | derived |
| `peak_rss_mb` | coarse peak memory (best-effort per-process sampling) |
| `status` | `ok` \| `na_by_design` \| `error` |
| `note` | reason for `na`/`error` |
| `output_fingerprint` | JSON: a cheap, deterministic summary of the function's output, captured **outside** the timed loop (pure-core mode only). Scalar fns → the value(s); tile-returning fns → per-band `{shape, dtype, nodata_count, min, max, mean, std}`. Enables heavy-vs-light consistency comparison. Empty for spark-path rows. |
| `env_arch`, `env_cpu_model`, `env_cpu_count`, `env_os`, `env_gbx_version`, `env_gdal_version`, `env_runtime_version`, `env_where` | environment; `where ∈ docker\|venv\|cluster` |

**Metrics philosophy:** primary = `median_ms` + `throughput_mpix_s` (median over min: GDAL block-cache/JIT make min optimistic; p90 retained for tail/variance). Default output is a **comparison table** joining heavyweight vs lightweight on `(fn, mode, tile_px, bands, rows)` with a `speedup = hw_median / lw_median` column → `comparison.csv` + a short `summary.md` (slowest fns, biggest divergences, error rows). A `--plot` flag (later) can render PNGs; off by default. Cross-run/scale/arch/version comparison is a `groupBy`/filter over accumulated Parquet/Delta — no bespoke diff.

## 5b. Output consistency (heavy vs light agreement)

Beyond compute time, the suite captures whether the two APIs produce **consistent output** on the **same input tile** (the seeded corpus guarantees identical bytes in). Because heavyweight (GDAL) and lightweight (numpy/rasterio) run different algorithms, "consistency" is defined as **numeric agreement**, not byte-equality.

- **Capture (in the runner, pure-core mode only):** one **untimed** call per (function × input tile) produces the actual output; an `output_fingerprint` is computed and stored in the result row. Scalar fns (`width`, `srid`, `avg`…) store the value(s); tile-returning fns (`slope`, `ndvi`, `transform`…) store per-band `{shape, dtype, nodata_count, min, max, mean, std}`. The fingerprint is taken outside the timed iterations so it never affects timing. Spark-path rows leave it empty (consistency is an algorithm-output property, cleanest to capture from the pure-core path).
- **Default comparison (in `compare`, Plan 1b):** join heavyweight vs lightweight rows on `(fn, tile_px, bands, dtype, srid, nodata_frac)` and classify each as **exact-match** (scalars; integer/count stats), **within-tolerance** (float stats within configurable rel/abs tolerance), or **divergent** — reporting the largest stat delta. Output: a `consistency.csv` + a section in `summary.md` (per-fn agreement class, worst deltas, any divergences).
- **Deep per-pixel opt-in (`--deep-parity <fns>`):** for named functions, both APIs' raw outputs are aligned and compared per-pixel (max-abs-error, RMSE, %-pixels-within-tol). Heavier (needs grid alignment when CRS/resolution differ, e.g. after `transform`); used as a targeted parity audit, not the broad sweep.
- **Tolerances are explicit and recorded**, so "consistent" is a defined, reproducible claim — not a vibe. Divergences are surfaced, not hidden (same honesty principle as `na_by_design`).

## 6. Orchestrator & commands

Two new command categories under `scripts/commands/` (`.md`+`.sh` pairs, source `common.sh`, support `--help`/`--log`, fail-fast `check_docker` where needed).

**Venv foundation (shared):**
- `gbx:venv:sync` — create/refresh an isolated `uv` venv at a fixed gitignored path (`.venv-pyrx/`), installing the `pyrx` optional-deps locked; assert no-system-site-packages.
- `common.sh` helper `run_in_pyrx_venv "<cmd>"`.
- **Existing `gbx:test:pyrx` (and pyrx paths in `gbx:test:python`) routed through this venv** — fixing the command, not working around it, so lightweight tests are host-isolated everywhere.

**Bench commands:**

| Command | Where | Does |
|---|---|---|
| `gbx:bench:gen-data` | venv | datagen → corpus + `corpus.json` → validity gate. `--tile-px --bands --rows --nodata-frac --srids --dtypes --seed --out` |
| `gbx:bench:heavyweight` | Docker | Scala runner over the corpus. `--functions --category --mode --iters --warmup --out --log` |
| `gbx:bench:lightweight` | venv | Python runner over the **same** corpus, same option surface |
| `gbx:bench:compare` | host | merge result shards → `comparison.csv` + `summary.md` |
| `gbx:bench:all` | orchestrates | one-shot local entry point (below) |
| `gbx:bench:cluster` | Databricks | cluster phase (§7) |

**`gbx:bench:all` flow:** `gbx:venv:sync` (idempotent) → ensure corpus exists at the shared path (generate if missing or `--regen`; the **one** path passed to both runners) → run **heavyweight, then lightweight, sequentially** (CPU-contention honesty rule) → `gbx:bench:compare`.

**Result layout (gitignored, under `test-logs/`):** `test-logs/bench/<run_id>/{heavyweight.parquet, lightweight.parquet, comparison.csv, summary.md}`.

**Usage note:** the heavyweight Docker/Maven leg is minutes-long; when driven via Claude it's dispatched as a Task subagent with periodic progress updates, but the commands themselves are plain shell for any contributor or CI.

## 7. Cluster phase

Reuses the **same two runners, the same corpus, the same schema**; only packaging, submission, and a results table are new. This is where the comparison is honest in absolute terms (same hardware).

**Single `--cluster-id` with isolation flags:**
- Default `gbx:bench:cluster --cluster-id <id>` runs **both** APIs on that cluster (true same-hardware comparison on an x86 heavyweight-configured cluster).
- `--heavyweight-only` / `--lightweight-only` to isolate — e.g. operator installs the pyrx wheel on an **ARM cluster** and runs `--lightweight-only`.
- Pre-flight **verifies the target API imports** on the cluster and fails fast otherwise (clear "heavyweight not available here" rather than a crash).

**ARM asymmetry is a headline result, not a forced comparison.** Heavyweight deploys as a CI-built, sha256-verified bundle whose init script **refuses `aarch64`** (x86-only by design); lightweight runs on Serverless/Standard/Lakeflow DLT **and ARM**. So:
- x86 cluster → both APIs run.
- ARM cluster → lightweight only; heavyweight recorded as `na_by_design`, note `"heavyweight: x86-only (init script refuses aarch64)"`. ARM-vs-x86 compares lightweight-on-ARM vs lightweight-on-x86 (with heavyweight-on-x86 as reference).

**Operator owns artifact provisioning; the benchmark runs against what's installed and records it.** No artifact pushing or `--artifact-source` flag. The operator sets up the cluster per the installation docs (release bundle + init script + wheel library on x86 heavyweight; or just the `[pyrx]` wheel on any/ARM cluster). The benchmark **detects and records** `gbx_version`, `gdal_version`, `arch`, `runtime_version` from the live session, so "what was tested" reflects a real, operator-validated deployment.

**Submission:** reuse the existing one-off notebook-job mechanism (`push_and_run_bundle_on_cluster.py`, `notebooks/tests/databricks_cluster_config.env` for host/token/cluster, `GBX_BUNDLE_VOLUME_*`). The notebook ensures the corpus exists on the Volume, runs the matching runner(s) — spark-path is the headline at scale; pure-core still runs on the driver — and appends rows to a **`bench_results` Delta table** keyed by `run_id + scale coords + env`.

**Corpus on cluster:** generated once to a UC Volume (tiny seed in, big corpus out; `numpy` pinned in venv + cluster for reproducibility), then both runners read that one path.

**Bench code delivery:** the Python bench module ships inside the installed wheel (importable once the operator's wheel is present). The Scala runner's on-cluster delivery (in the JAR vs notebook-attached) is settled in the plan.

**Guardrails:** launching a cluster job is shared-state and costs money — `gbx:bench:cluster` requires an explicit `--cluster-id`, never auto-provisions or attaches init scripts, and confirms target + corpus path before submitting. When driven via Claude, confirm with the user before any cluster submission.

## 8. File layout (new)

```
python/geobrix/src/databricks/labs/gbx/bench/
  __init__.py
  spec.py        # function registry + corpus manifest model
  datagen.py     # seeded corpus generator + validity gate
  runner.py      # Python runner (pure-core + spark-path)
  results.py     # result-row schema, JSONL/Parquet IO, compare/summary
src/main/scala/com/databricks/labs/gbx/bench/
  Runner.scala   # Scala runner (eval pure-core + local Spark spark-path)
scripts/commands/
  gbx-venv-sync.{md,sh}
  gbx-bench-gen-data.{md,sh}
  gbx-bench-heavyweight.{md,sh}
  gbx-bench-lightweight.{md,sh}
  gbx-bench-compare.{md,sh}
  gbx-bench-all.{md,sh}
  gbx-bench-cluster.{md,sh}
python/geobrix/test/bench/   # unit tests for datagen, spec, results, compare
```

## 9. Out of scope (YAGNI)

- Charts/dashboards (machine-readable + comparison table only; `--plot` is a later add).
- Auto-provisioning clusters or owning the heavyweight deploy chain (sha256 sidecar, init-script staging) — operator's job per install docs.
- Benchmarking functions that exist in only one API (parity set only).
- Vector (pyvx) / grid (pygx) benchmarks — raster (RasterX/pyrx) only for v1.
- Micro-profiling (flame graphs, per-line); this measures end-to-end op time.

## 10. Open implementation details (for the plan)

1. Scala runner on-cluster delivery: include the bench `Runner` in the assembly JAR vs ship as a notebook-attached jar (avoid bloating the production JAR — possibly a separate bench classifier artifact or a `src/test` main submitted explicitly).
2. Exact registry seed: enumerate the ~100 parity functions from `docs/tests-function-info/registered_functions.txt` with their `input_shape`/`args`/`modes` — generated semi-automatically, hand-verified.
3. `peak_rss_mb` sampling mechanism per runtime (psutil in venv; JVM `OperatingSystemMXBean`/RSS read in Scala).
4. Default warmup/measured iteration counts per mode (pure-core can afford more iters; spark-path fewer).
5. Whether `gbx:bench:all` should also accept a `--quick` profile (small ladder) for CI smoke vs the full sweep.

## 11. Refinements surfaced during Plan 1a execution (carry into 1b/2)

Building the local suite (Plan 1a, committed on `pyrx-0.4.0`) surfaced several real refinements — capture here so 1b/2 incorporate them rather than rediscover them:

1. **NoData → fingerprint accuracy (consistency-critical).** pyrx terrain (e.g. `slope`) computes over NoData **sentinel** pixels rather than masking them, so a terrain output fingerprint on a nodata-bearing tile summarizes "slope-of-sentinel" values. The cross-API consistency compare (1b) must either **pre-mask NoData before fingerprinting** or **assert both APIs share identical NoData semantics** — otherwise the agreement check compares garbage on nodata tiles. (Also informs whether the deep-parity per-pixel mode masks nodata.)
2. **`min_bands` on FnSpec → `na_by_design`, not `error`.** Band-math fns (`ndvi`/`ndwi`/`nbr`) on 1-band tiles error because band 2 doesn't exist. Add a `min_bands` field to `FnSpec`; the runner skips tiles below it and records `na_by_design` (with note), keeping the `error` column meaningful (true breakage only). Applies to both runners.
3. **`peak_rss_mb` attribution.** Current value is process-wide `ru_maxrss` (monotonic high-water mark) — it can't attribute memory to a specific fn/tile and only ever rises. Either drop it from per-row output or measure a per-call delta (`tracemalloc` peak around the timed call). Decide in 1b.
4. **spark-path placeholder fields.** spark-path rows carry `srid=0` / `nodata_frac=0.0` as sentinels (the row pool mixes srids, nodata 0.0). Document these as sentinels (comment or a dedicated marker) so a downstream reader never treats `srid=0` as a real projection.
5. **`na_by_design` emission for skipped modes.** `run_pure_core` currently `continue`s past fns lacking `pure-core` mode (silent). Once spark-path-only fns (aggregators) enter the registry in 1b, emit `na_by_design` rows instead so coverage gaps stay legible (design §1 honesty rule).
6. **Coverage gaps to exercise before 1b sign-off:** a `--bands 1` sweep (exercises the band-math na/error path) and at least one `int16`/`uint8` terrain tile (1a e2e was float32-heavy).
7. **Lint gate not yet run.** Implementers couldn't run `flake8` in the venv; **`gbx:lint:python` (Docker) must run before any push** — it's the CI gate (isort/black/flake8). Manual scans were clean.
8. **Local spark-path is heap-bounded** (confirmed: large tiles OOM the local driver at default heap). `gbx:bench:lightweight` exposes `--driver-mem` (default 4g); the full row ladder (1000/10000 rows at large tile sizes) is **cluster-scope** (Plan 2), not laptop. Keep local `--row-counts`/tile sizes modest.
9. **Compare semantics (for Plan 1c) — confirmed during 1b fingerprint build.** The heavy (Scala/GDAL) and light (numpy) fingerprints are semantically aligned (schema, `[h,w]` shape, population std `÷N`, nodata-filtered stats, null-on-empty), but three representational divergences mean the `compare` step MUST: (a) **exclude `dtype` from the agreement gate** — GDAL emits `Float32`/`Byte`/`UInt16`, numpy emits `float32`/`uint8`/`int16`; treat dtype as informational/display only; (b) compare numeric band stats and scalar `value` with a **float tolerance** (parse-and-compare as numbers), never string/exact equality — a scalar can serialize `256` (int) on one side vs `256.0` (float) on the other; (c) compare **parsed JSON**, not raw strings (Python `sort_keys=True` vs Jackson insertion order differ). Also: terrain/band-math on nodata-bearing tiles will legitimately diverge (light computes over sentinels, heavy/GDAL may mask) — surface as a real consistency finding with a "likely nodata-handling" note (ref §11.1). **Confirmed in the 1b e2e:** even on **nodata-free** tiles, neighborhood ops diverge in `nodata_count` — heavyweight `rst_slope` marks the 1-px slope-kernel border as nodata (1020 px on a 256² tile) while lightweight does not (0); min/max/mean/std still agree to ~3 decimals. So the compare must (i) treat `nodata_count` as an expected divergence for neighborhood/terrain ops (gate agreement on the value stats with tolerance, report nodata_count delta separately as informational), and (ii) never expect fingerprints to be byte-equal across APIs — always parse-and-tolerance-compare. **Confirmed in the 1c e2e:** the value-stat impact of the border-nodata difference is **tile-size-dependent** — `rst_slope` is `within_tol` at 512² but `divergent` at 256² (max_rel_delta ~0.006), because the border is a larger fraction of a small tile, shifting min/max/mean/std past the 1e-3 tolerance. This is a real, expected divergence (don't loosen tolerance to hide it); the compare now tags such divergent cells with a "divergence likely nodata/border-handling (nodata_count differs by N)" note so the cause is self-evident in `summary.md`. Headline perf finding from the same run: lightweight `rst_ndvi` is ~900× faster than heavyweight (heavy shells out to a `gdal_calc` subprocess).
10. **Pure-core timing asymmetry (heavy vs light).** The Scala `HeavyRunner` pure-core timed body runs `BenchDispatch.pureCore` which *includes* building the output fingerprint string each iteration, whereas the Python runner times the core op only (fingerprint is captured once, untimed). Minor for expensive ops (terrain/warp dominate), but for cheap accessors the heavy median carries a small fingerprint-serialization tax the light side doesn't. If strict pure-core timing parity matters, split `BenchDispatch.pureCore` into "run op" vs "fingerprint output" so the timed closure excludes fingerprinting (mirrors Python). Low priority; note when interpreting accessor pure-core deltas in 1c.
11. **Heavyweight spark-path not yet exercised end-to-end (Plan 1c must).** Plan 1b validated the heavyweight runner's **pure-core** path e2e (8/8 ok, schema-parity confirmed) but only compiled `runSparkPath` — no real-data run. Plan 1c's `gbx:bench:all` (or a dedicated smoke) must run `gbx:bench:heavyweight --modes spark-path` on a real corpus before trusting those rows. (Low residual risk: the gdal_calc rootPath NPE that hit pure-core can't recur on spark-path, which goes through the production spectral-eval path that mkdir's independently; untested surface is the cache/warm-up/noop-sink timing harness + per-fn row emission.) Note also: heavyweight band-math (`rst_ndvi`/etc.) pure-core is ~30–70× slower than terrain because it shells out to a `gdal_calc` subprocess — a real perf signal to highlight in the compare.
12. **Spark-path JVM/Spark warm-up skews the first timed job.** Observed in a demo run: `rst_avg` measured 589 ms @ 2 rows but 138 ms @ 4 rows — the *first* spark job in the process pays one-time JVM/Spark spin-up that the per-`(fn,rows)` warmup loop doesn't absorb (warmup is per-call, but the process-level JIT/Spark init only happens once, on whichever `(fn,rows)` runs first). Fix in 1b: run a **dedicated throwaway spark warm-up job** (one trivial materialized job on the tile DataFrame) before the timing loop in `run_spark_path`, so steady-state timings aren't contaminated by interpreter/JVM init. Both runners' spark-path legs need this (heavyweight JVM JIT warm-up is the same hazard, arguably larger). Until fixed, spark-path absolute numbers at the smallest row count are unreliable — note it in `summary.md` or discard the first sample.

---
*Design approved 2026-06-05 via brainstorming; Plan 1a implemented + reviewed the same day (commits on `pyrx-0.4.0`, all 15 tasks, 26 bench tests green). Next: Plan 1b (Scala heavyweight runner + cross-API consistency compare + `gbx:bench:all`), then Plan 2 (cluster). Sources: installation.mdx (current deploy model), recon of existing test/data/cluster harness, pyrx core/_serde/_udf, gbx command conventions.*
