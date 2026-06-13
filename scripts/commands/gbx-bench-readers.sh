#!/bin/bash
# gbx:bench:readers — time the light raster reader (raster_gbx) in the isolated venv.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

CORPUS=""
MODE="pure-local"
RUN_ID="local"
SIZE_MIB="16"
WARMUP="1"
MEASURED="3"
OUT=""
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:readers"
    cat <<'EOF'
Time the light raster reader (raster_gbx) per-file in pure-local mode, or
over a corpus directory via the Spark data source in spark-path mode.

Usage: bash scripts/commands/gbx-bench-readers.sh [OPTIONS]
Options:
  --corpus <dir>    Directory containing *.tif files to benchmark (required)
  --mode <m>        pure-local | spark-path | both (default: pure-local)
  --run-id <id>     Run ID label embedded in result rows (default: local)
  --size-mib <n>    Tile size budget in MiB (default: 16)
  --warmup <n>      Warmup iterations per file/path (default: 1)
  --measured <n>    Measured iterations per file/path (default: 3)
  --out <path>      Write results to this JSONL file (default: print only)
  --log <path>      Tee output to test-logs/<path>
  --help, -h        Show help

Examples:
  bash scripts/commands/gbx-bench-readers.sh \
    --corpus sample-data/Volumes/main/default/bench-corpus \
    --mode pure-local --warmup 1 --measured 3

  bash scripts/commands/gbx-bench-readers.sh \
    --corpus /tmp/bench-tifs --mode both \
    --out test-logs/bench/local/readers.jsonl --log bench-readers.log
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --corpus) CORPUS="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --size-mib) SIZE_MIB="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --measured) MEASURED="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

if [[ -z "$CORPUS" ]]; then
    echo "ERROR: --corpus is required" >&2
    show_help
    exit 1
fi

cd "$PROJECT_ROOT"
show_banner "gbx:bench:readers"
setup_log_file "$LOG_PATH"

CMD="python -m databricks.labs.gbx.bench.readers \
    --corpus '$CORPUS' --mode '$MODE' --run-id '$RUN_ID' \
    --size-mib '$SIZE_MIB' --warmup '$WARMUP' --measured '$MEASURED'"

if [[ -n "$OUT" ]]; then
    CMD="$CMD --out '$OUT'"
fi

run_in_pyrx_venv "$CMD"
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 0 && -n "$OUT" ]]; then
    echo "results: $OUT"
fi
exit $EXIT_CODE
