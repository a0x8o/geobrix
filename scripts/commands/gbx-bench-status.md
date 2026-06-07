# gbx:bench:status

Print a coverage/parity/performance scorecard from the authoritative benchmark store.

Read-only over `test-logs/bench/authoritative/` — never benchmarks, never needs Docker. Aggregates every stored record into benchmark coverage (`N / 107`), parity over compared cells (exact / within_tol / divergent + the divergent function names, plus timing-only cells), the performance win split (lightweight at least as fast vs heavyweight faster), the computed functional-parity gap (registered minus pyrx-implemented), the registered functions with no store record, and a per-function table that marks **STALE** any record whose sources changed since it was validated. Divergent / stale functions sort first. Calls `python -m databricks.labs.gbx.bench.store status`.

`--stale-only` prints just the aggregate lines plus the stale/missing function list (omits the per-function table) — handy as a quick pre-push check.

**Usage:** `bash scripts/commands/gbx-bench-status.sh [options]`

**Options:** `--stale-only`, `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-status.sh` — full scorecard.
- `bash scripts/commands/gbx-bench-status.sh --stale-only` — aggregate + stale/missing only.
