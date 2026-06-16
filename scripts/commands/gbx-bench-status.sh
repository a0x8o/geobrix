#!/bin/bash
# gbx:bench:status — print the authoritative-store scorecard (read-only, no Docker).
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

STALE_ONLY=0
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:status"
    cat <<'EOF'
Print a neutral coverage/parity/performance scorecard aggregated from the
authoritative per-function benchmark store. Read-only: never benchmarks, never
needs Docker. Reports benchmark coverage (N / 107), parity over compared cells
(exact / within_tol / divergent + divergent fn names, plus timing-only), the
performance win split, the computed functional-parity gap, the registered
functions with no store record, and a per-function table with a STALE marker for
records whose sources changed since validation.

Usage: bash scripts/commands/gbx-bench-status.sh [options]
Options:
  --stale-only   Print only the aggregate lines + the stale/missing function list
                 (omit the full per-function table).
  --log <path>   Tee output under test-logs/
  --help, -h     Show help

Reads test-logs/bench/authoritative/. Run gbx:bench:seed or gbx:bench:changed to
populate the store.
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --stale-only) STALE_ONLY=1; shift ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:status"
setup_log_file "$LOG_PATH"

STATUS_ARGS="status"
[[ $STALE_ONLY -eq 1 ]] && STATUS_ARGS="status --stale-only"

run_in_pyrx_venv "python -m databricks.labs.gbx.bench.store $STATUS_ARGS"
exit $?
