# gbx:bench:compare

Join a run's heavyweight + lightweight JSONL shards into a heavy-vs-light **speedup + output-consistency** report (`comparison.csv` + `summary.md`), in the isolated pyrx venv.

Consistency uses tolerance-based fingerprint agreement (parsed, not byte-equal): `dtype` casing and `nodata_count` (neighborhood-op border) are informational, not divergences.

**Usage:** `bash scripts/commands/gbx-bench-compare.sh [options]`

**Options:** `--run-id <id>`, `--heavyweight <path>`, `--lightweight <path>`, `--out-dir <dir>`, `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-compare.sh --run-id finalcmp`
- `bash scripts/commands/gbx-bench-compare.sh --heavyweight a.jsonl --lightweight b.jsonl --out-dir /tmp/cmp`
