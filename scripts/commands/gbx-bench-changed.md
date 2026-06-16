# gbx:bench:changed

Benchmark **only** the functions affected by the current changes, then write authoritative per-function store records from that run.

Resolution maps changed files to functions through each `FnSpec.sources`: a changed source file selects every registered function that declares it (so editing a shared module like `_nodata.py` re-benchmarks all of its dependents). By default it inspects the working tree (`git diff --name-only HEAD` + untracked files); `--base <ref>` diffs against a ref instead.

`--set` controls the registry tier consulted during mapping and defaults to **`full`** (not `core`) so any changed registered function is caught — change-awareness should consider the whole registry, not just the fast default set.

`--functions <csv>` bypasses diff-resolution and benchmarks exactly the listed registered functions — for re-validating a known set, or when a change is in bench infra (e.g. `compare.py`) that maps to no function's `sources` but still affects how a function compares. Mutually informative with `--list` (a `--functions` list with `--list` just echoes the set).

`--list` is a dry-run: it prints the affected functions and any unmapped changed paths (files in no function's `sources` — candidate forgotten sources or non-source edits), then exits without benchmarking. Without `--list`, it runs `gbx:bench:all --set <set> --functions <affected> --modes pure-core` on a fresh `changed-<epoch>` run id and persists one store record per affected function (tagged with the current commit — prefixed `dirty:` when the tree is dirty — the corpus seed, and the run's rows + comparison cells).

**Usage:** `bash scripts/commands/gbx-bench-changed.sh [options]`

**Options:** `--base <ref>`, `--functions <csv>`, `--set core|full` (default `full`), `--list`, `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-changed.sh --list` — show which functions the current edits affect.
- `bash scripts/commands/gbx-bench-changed.sh --base main --log changed.log` — benchmark everything changed since `main` and update the store.
- `bash scripts/commands/gbx-bench-changed.sh --functions rst_polygonize,rst_contour,rst_h3_tessellate --log validate.log` — re-benchmark an explicit set and refresh their store records.
