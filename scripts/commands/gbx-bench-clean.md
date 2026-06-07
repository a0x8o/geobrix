# gbx:bench:clean

Prune `test-logs/bench/` — ad-hoc run directories, orphaned store records, or everything.

By default (`--runs`) it removes every entry directly under `test-logs/bench/` **except** the `authoritative/` store: the ad-hoc `<run-id>/` directories left by `gbx:bench:all` / `changed` / `seed`, plus any stray files. The authoritative store is kept.

`--orphans` does the `--runs` prune **and** deletes `authoritative/<fn>.json` for functions no longer in the registry (`spec.select(set="full")`) — e.g. a function that was renamed or dropped but left a record behind. The orphan set is computed by `store.orphan_records`.

`--all` removes **everything** under `test-logs/bench/`, including the authoritative store. It is destructive and requires the explicit flag (there is no default wipe). Each mode prints what it removed.

**Usage:** `bash scripts/commands/gbx-bench-clean.sh [options]`

**Options:** `--runs` (default), `--orphans`, `--all`, `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-clean.sh` — drop ad-hoc run dirs, keep the store.
- `bash scripts/commands/gbx-bench-clean.sh --orphans` — also prune records for removed functions.
- `bash scripts/commands/gbx-bench-clean.sh --all` — wipe everything under `test-logs/bench/`.
