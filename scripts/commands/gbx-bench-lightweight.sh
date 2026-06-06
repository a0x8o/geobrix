#!/bin/bash
# gbx:bench:lightweight — run the pyrx benchmark runner in the isolated venv.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

CORPUS="${PROJECT_ROOT}/sample-data/Volumes/main/default/bench-corpus"
RUN_ID="local"
OUT=""
FUNCTIONS=""
CATEGORIES=""
MODE="both"
ROW_COUNTS="10,100,1000,10000"
WARMUP="2"
MEASURED="5"
DRIVER_MEM="4g"
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:lightweight"
    cat <<'EOF'
Run the lightweight (pyrx) benchmark runner in the isolated venv.

Usage: bash scripts/commands/gbx-bench-lightweight.sh [options]
Options:
  --corpus <dir>      Corpus root (default sample-data/.../bench-corpus)
  --out <path>        JSONL output (default test-logs/bench/<run-id>/lightweight.jsonl)
  --run-id <id>       Run id (default local)
  --functions <list>  Comma-separated rst_* names (default: all in registry)
  --categories <list> Comma-separated categories
  --mode <m>          pure-core | spark-path | both (default both)
  --row-counts <l>    Spark-path row ladder (default 10,100,1000,10000)
  --warmup <n>        Warmup iters (default 2)
  --measured <n>      Measured iters (default 5)
  --driver-mem <m>    Spark driver memory for spark-path leg (default 4g)
  --log <path>        Tee output under test-logs/
  --help, -h          Show help

Note: on a laptop the spark-path leg is bounded by driver memory — keep
--row-counts and the corpus tile size modest locally. The full ladder
(1000/10000 rows at large tile sizes) is intended for the cluster phase.
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --functions) FUNCTIONS="$2"; shift 2 ;;
    --categories) CATEGORIES="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --row-counts) ROW_COUNTS="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --measured) MEASURED="$2"; shift 2 ;;
    --driver-mem) DRIVER_MEM="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:lightweight"
setup_log_file "$LOG_PATH"

[[ -z "$OUT" ]] && OUT="${PROJECT_ROOT}/test-logs/bench/${RUN_ID}/lightweight.jsonl"
mkdir -p "$(dirname "$OUT")"

# Size the local Spark driver heap for the spark-path leg (see scaling note in --help).
export PYSPARK_SUBMIT_ARGS="--driver-memory ${DRIVER_MEM} pyspark-shell"

run_in_pyrx_venv "python -m databricks.labs.gbx.bench.runner \
    --corpus '$CORPUS' --out '$OUT' --run-id '$RUN_ID' --mode '$MODE' \
    --functions '$FUNCTIONS' --categories '$CATEGORIES' \
    --row-counts '$ROW_COUNTS' --warmup '$WARMUP' --measured '$MEASURED'"
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ results: $OUT"
    PRETTY="${OUT%.jsonl}.json"
    [[ -f "$PRETTY" ]] && echo "   pretty:  $PRETTY"
fi
exit $EXIT_CODE
