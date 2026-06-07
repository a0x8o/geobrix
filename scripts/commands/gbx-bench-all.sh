#!/bin/bash
# gbx:bench:all — full local benchmark pipeline: gen-data -> heavyweight -> lightweight -> compare.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

RUN_ID="local"
FUNCTIONS=""
MODES="both"
TILE_PX="256,512"
BANDS="2"
DTYPES="float32"
SRIDS="4326,32618"
NODATA_FRAC="0.0"
ROW_ROWS="4"
ROW_COUNTS="2,4"
WARMUP="2"
MEASURED="5"
DRIVER_MEM="4g"
SEED="11"
LOG_PATH=""
HOST_CORPUS="sample-data/Volumes/main/default/bench-corpus"
CONTAINER_CORPUS="/Volumes/main/default/bench-corpus"

show_help() {
    show_banner "gbx:bench:all"
    cat <<'EOF'
Full local benchmark pipeline (sequential): gen-data -> heavyweight (Docker) ->
lightweight (venv) -> compare. Heavy and light run sequentially (never concurrent)
so they don't contend for CPU and skew timings.

Usage: bash scripts/commands/gbx-bench-all.sh [options]
Options:
  --run-id <id>        Run id (default local)
  --functions <list>   rst_* names (default: all registry fns)
  --modes <m>          pure-core | spark-path | both (default both)
  --tile-px <list>     Size sweep (default 256,512)
  --bands <list>       Band counts (default 2)
  --dtypes <list>      Dtypes (default float32)
  --srids <list>       SRIDs (default 4326,32618 -- geographic + projected, so
                       CRS-auto-scaled terrain ops exercise both branches)
  --nodata-frac <list> NoData fraction(s) (default 0.0)
  --row-rows <n>       Row-pool size (default 4)
  --row-counts <list>  Spark-path row ladder (default 2,4)
  --warmup <n>         Warmup iters (default 2)
  --measured <n>       Measured iters (default 5)
  --driver-mem <m>     Lightweight spark driver mem (default 4g)
  --seed <n>           Corpus seed (default 11)
  --log <path>         Tee output under test-logs/
  --help, -h           Show help

Note: local defaults are laptop-modest. The full row ladder / large tiles are
cluster-scope (Plan 2). Outputs: test-logs/bench/<run-id>/{heavyweight,lightweight}.jsonl,
comparison.csv, summary.md.
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --functions) FUNCTIONS="$2"; shift 2 ;;
    --modes) MODES="$2"; shift 2 ;;
    --tile-px) TILE_PX="$2"; shift 2 ;;
    --bands) BANDS="$2"; shift 2 ;;
    --dtypes) DTYPES="$2"; shift 2 ;;
    --srids) SRIDS="$2"; shift 2 ;;
    --nodata-frac) NODATA_FRAC="$2"; shift 2 ;;
    --row-rows) ROW_ROWS="$2"; shift 2 ;;
    --row-counts) ROW_COUNTS="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --measured) MEASURED="$2"; shift 2 ;;
    --driver-mem) DRIVER_MEM="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:all"
check_docker
setup_log_file "$LOG_PATH"

set -e
echo "▶ [1/4] gen-data"
bash "$SCRIPT_DIR/gbx-bench-gen-data.sh" --out "$HOST_CORPUS" \
    --tile-px "$TILE_PX" --bands "$BANDS" --dtypes "$DTYPES" --srids "$SRIDS" \
    --nodata-frac "$NODATA_FRAC" --row-rows "$ROW_ROWS" --seed "$SEED"

echo "▶ [2/4] heavyweight (Docker) — runs first, sequentially"
bash "$SCRIPT_DIR/gbx-bench-heavyweight.sh" --run-id "$RUN_ID" --corpus "$CONTAINER_CORPUS" \
    --functions "$FUNCTIONS" --modes "$MODES" --row-counts "$ROW_COUNTS" \
    --warmup "$WARMUP" --measured "$MEASURED"

echo "▶ [3/4] lightweight (venv) — runs after heavyweight, sequentially"
bash "$SCRIPT_DIR/gbx-bench-lightweight.sh" --run-id "$RUN_ID" --corpus "$HOST_CORPUS" \
    --functions "$FUNCTIONS" --mode "$MODES" --row-counts "$ROW_COUNTS" \
    --warmup "$WARMUP" --measured "$MEASURED" --driver-mem "$DRIVER_MEM"

echo "▶ [4/4] compare"
bash "$SCRIPT_DIR/gbx-bench-compare.sh" --run-id "$RUN_ID"
set +e

echo "✅ bench:all complete — test-logs/bench/${RUN_ID}/{heavyweight,lightweight}.jsonl, comparison.csv, summary.md"
