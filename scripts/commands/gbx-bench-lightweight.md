# gbx:bench:lightweight

Run the lightweight (pyrx) benchmark runner in the isolated venv, over a generated corpus, in pure-core and/or spark-path mode. Writes a JSONL result shard (with output fingerprints for consistency comparison on pure-core rows).

**Usage:** `bash scripts/commands/gbx-bench-lightweight.sh [options]`

**Options:** `--corpus <dir>`, `--out <path>`, `--run-id <id>`, `--functions <list>`, `--categories <list>`, `--mode pure-core|spark-path|both`, `--row-counts <list>`, `--warmup <n>`, `--measured <n>`, `--driver-mem <m>` (Spark driver heap for the spark-path leg, default 4g), `--log <path>`, `--help`.

**Scaling note:** on a laptop the spark-path leg is bounded by driver memory. Keep `--row-counts` and the corpus tile size modest locally; the full row ladder (1000/10000 rows at large tile sizes) is intended for the cluster phase. Raise `--driver-mem` if the spark-path leg OOMs.

**Examples:**
- `bash scripts/commands/gbx-bench-lightweight.sh --run-id r1 --log lw.log`
- `bash scripts/commands/gbx-bench-lightweight.sh --functions rst_slope,rst_ndvi --mode pure-core`
- `bash scripts/commands/gbx-bench-lightweight.sh --mode spark-path --row-counts 10,50,100 --driver-mem 6g`
