#!/bin/bash
# gbx:bench:compare — join heavyweight + lightweight shards into comparison.csv + summary.md.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

RUN_ID="local"
HW=""
LW=""
OUT_DIR=""
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:compare"
    cat <<'EOF'
Join the heavyweight + lightweight JSONL shards for a run into a heavy-vs-light
speedup + consistency report (comparison.csv + summary.md), in the pyrx venv.

Usage: bash scripts/commands/gbx-bench-compare.sh [options]
Options:
  --run-id <id>     Run id; defaults paths to test-logs/bench/<run-id>/ (default local)
  --heavyweight <p> Heavyweight shard (default test-logs/bench/<run-id>/heavyweight.jsonl)
  --lightweight <p> Lightweight shard (default test-logs/bench/<run-id>/lightweight.jsonl)
  --out-dir <dir>   Output dir (default test-logs/bench/<run-id>)
  --log <path>      Tee output under test-logs/
  --help, -h        Show help
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --heavyweight) HW="$2"; shift 2 ;;
    --lightweight) LW="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:compare"
setup_log_file "$LOG_PATH"

[[ -z "$HW" ]] && HW="${PROJECT_ROOT}/test-logs/bench/${RUN_ID}/heavyweight.jsonl"
[[ -z "$LW" ]] && LW="${PROJECT_ROOT}/test-logs/bench/${RUN_ID}/lightweight.jsonl"
[[ -z "$OUT_DIR" ]] && OUT_DIR="${PROJECT_ROOT}/test-logs/bench/${RUN_ID}"

if [[ ! -f "$HW" ]]; then echo "ERROR: heavyweight shard not found: $HW" >&2; exit 1; fi
if [[ ! -f "$LW" ]]; then echo "ERROR: lightweight shard not found: $LW" >&2; exit 1; fi

run_in_pyrx_venv "python -m databricks.labs.gbx.bench.compare \
    --heavyweight '$HW' --lightweight '$LW' --out-dir '$OUT_DIR'"
EXIT_CODE=$?
[[ $EXIT_CODE -eq 0 ]] && echo "✅ comparison: $OUT_DIR/comparison.csv + summary.md"
exit $EXIT_CODE
