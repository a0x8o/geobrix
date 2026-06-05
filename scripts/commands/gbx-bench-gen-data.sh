#!/bin/bash
# gbx:bench:gen-data — generate the seeded benchmark corpus (in the pyrx venv).
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

OUT="${PROJECT_ROOT}/sample-data/Volumes/main/default/bench-corpus"
TILE_PX="256,512,1024,2048,4096"
BANDS="1,4,13"
DTYPES="uint8,int16,float32"
SRIDS="4326,3857,32618,27700"
NODATA_FRAC="0.02"
SEED="1234"
ROW_ROWS="10000"
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:gen-data"
    cat <<'EOF'
Generate the seeded benchmark corpus + corpus.json manifest in the pyrx venv.

Usage: bash scripts/commands/gbx-bench-gen-data.sh [options]
Options:
  --out <dir>          Corpus output dir (default sample-data/.../bench-corpus)
  --tile-px <list>     Tile-size sweep (default 256,512,1024,2048,4096)
  --bands <list>       Band counts (default 1,4,13)
  --dtypes <list>      Dtypes (default uint8,int16,float32)
  --srids <list>       SRIDs to cycle (default 4326,3857,32618,27700)
  --nodata-frac <list> NoData fraction(s), e.g. 0.02,0.25,0.5 (default 0.02)
  --row-rows <n>       Row-pool size for spark-path sweep (default 10000)
  --seed <n>           RNG seed (default 1234)
  --log <path>         Tee output under test-logs/
  --help, -h           Show help
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --out) OUT="$2"; shift 2 ;;
    --tile-px) TILE_PX="$2"; shift 2 ;;
    --bands) BANDS="$2"; shift 2 ;;
    --dtypes) DTYPES="$2"; shift 2 ;;
    --srids) SRIDS="$2"; shift 2 ;;
    --nodata-frac) NODATA_FRAC="$2"; shift 2 ;;
    --row-rows) ROW_ROWS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:gen-data"
setup_log_file "$LOG_PATH"

run_in_pyrx_venv "python -m databricks.labs.gbx.bench.datagen \
    --out '$OUT' --tile-px '$TILE_PX' --bands '$BANDS' --dtypes '$DTYPES' \
    --srids '$SRIDS' --nodata-frac '$NODATA_FRAC' --row-rows '$ROW_ROWS' --seed '$SEED'"
EXIT_CODE=$?
exit $EXIT_CODE
