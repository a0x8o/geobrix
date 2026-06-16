# gbx:bench:heavyweight

Run the heavyweight (Scala/JNI RasterX) benchmark in the `geobrix-dev` container over a generated corpus, writing a `heavyweight.jsonl` shard (same schema as the lightweight runner). Pure-core opens each tile with GDAL and calls `RST_*.execute`; spark-path times the registered `gbx_rst_*` Column on a local Spark DataFrame.

**Prereq:** generate the corpus first with `gbx:bench:gen-data` (its default output lands under the container mount at `/Volumes/main/default/bench-corpus`).

**Usage:** `bash scripts/commands/gbx-bench-heavyweight.sh [options]`

**Options:** `--corpus <dir>` (container path), `--out <path>` (container path), `--run-id <id>`, `--functions <list>`, `--set core|full`, `--modes pure-core|spark-path|both`, `--row-counts <list>`, `--warmup <n>`, `--measured <n>`, `--log <path>`, `--help`.

`--set` chooses the tier (`core` default, or `full`). The Scala heavy runner reads an explicit function list, so the selected tier is resolved to concrete `rst_*` names via the pyrx registry on the host and passed as `gbx.bench.functions`. An explicit `--functions` overrides `--set`.

**Examples:**
- `bash scripts/commands/gbx-bench-heavyweight.sh --run-id r1 --log hw.log`
- `bash scripts/commands/gbx-bench-heavyweight.sh --functions rst_slope,rst_ndvi --modes pure-core`
- `bash scripts/commands/gbx-bench-heavyweight.sh --set full --modes pure-core`
