# gbx:bench:all

Run the full local benchmark pipeline **sequentially**: generate the corpus, run the heavyweight (Scala/JNI, Docker) then the lightweight (pyrx, venv) benchmarks on the *same* corpus, then compare them. Heavy and light run one-after-the-other (never concurrently) so they don't contend for CPU and skew each other's timings.

Outputs land in `test-logs/bench/<run-id>/`: `heavyweight.jsonl`, `lightweight.jsonl`, `comparison.csv`, `summary.md`.

**Note:** `--modes` accepts `pure-core|spark-path|both`. Local defaults are laptop-modest; the full row ladder / large tiles are cluster-scope (Plan 2).

**Usage:** `bash scripts/commands/gbx-bench-all.sh [options]`

**Options:** `--run-id`, `--functions`, `--set core|full`, `--modes`, `--tile-px`, `--bands`, `--dtypes`, `--srids`, `--nodata-frac`, `--row-rows`, `--row-counts`, `--warmup`, `--measured`, `--driver-mem`, `--seed`, `--log`, `--help`.

`--set` chooses the benchmark tier: `core` (the fast default representative set) or `full` (every function in the registry). An explicit `--functions` list overrides `--set`. Defaults to `core`, preserving the fast default run.

**Examples:**
- `bash scripts/commands/gbx-bench-all.sh --run-id full1 --functions rst_width,rst_slope,rst_ndvi --modes both --log all.log`
- `bash scripts/commands/gbx-bench-all.sh --run-id fullcov --set full --log all-full.log`
