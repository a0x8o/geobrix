# gbx:bench:seed

Bootstrap (or rebuild) the authoritative per-function benchmark store from a full run.

Benchmarks **every** function in the selected tier — heavyweight (Docker) + lightweight (venv) + compare — and writes one authoritative store record per function from that run. This is `gbx:bench:changed` for the whole set instead of the working-tree diff; both commands share the same store-write entry (`python -m databricks.labs.gbx.bench.store write-run`).

`--set` chooses the tier and defaults to **`full`** (every registered function); `--set core` seeds only the fast core set. Records are tagged with the current commit (prefixed `dirty:` when the working tree is dirty), the corpus seed, and the run's rows + comparison cells. Prints how many records were written and the store directory.

**Usage:** `bash scripts/commands/gbx-bench-seed.sh [options]`

**Options:** `--set core|full` (default `full`), `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-seed.sh --log seed.log` — seed the whole registry.
- `bash scripts/commands/gbx-bench-seed.sh --set core` — seed only the fast core set.
