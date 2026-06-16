# gbx:bench:stalecheck

Advisory pre-push warning for changed functions whose benchmark record is missing or stale.

For the functions whose `sources` changed (working tree vs HEAD, or the diff vs `--base <ref>`), checks whether their authoritative store record is **missing** or **stale** (`is_stale`) and warns — listing them and suggesting `gbx:bench:changed` to re-validate. Read-only and cheap: it **never benchmarks**, **never blocks** (always exits 0), and never needs Docker. Calls `store.stale_changed_functions`.

**Usage:** `bash scripts/commands/gbx-bench-stalecheck.sh [options]`

**Options:** `--base <ref>` (default: working tree vs HEAD), `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-stalecheck.sh` — warn about anything changed in the working tree.
- `bash scripts/commands/gbx-bench-stalecheck.sh --base origin/beta/0.4.0` — warn about anything changed since the remote branch tip.
