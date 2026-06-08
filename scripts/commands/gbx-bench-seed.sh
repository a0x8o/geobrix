#!/bin/bash
# gbx:bench:seed — bootstrap/rebuild the authoritative store from a full (or core) run.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

SET="full"
LOG_PATH=""
HOST_CORPUS="sample-data/Volumes/main/default/bench-corpus"

show_help() {
    show_banner "gbx:bench:seed"
    cat <<'EOF'
Bootstrap (or rebuild) the authoritative per-function benchmark store from a
FULL (or --set core) run: benchmark every function in the selected tier —
heavyweight (Docker) + lightweight (venv) + compare — and write one authoritative
store record for each from that run.

This is gbx:bench:changed for the WHOLE set instead of the working-tree diff;
both commands share the same store-write entry.

Usage: bash scripts/commands/gbx-bench-seed.sh [options]
Options:
  --set <core|full> Registry tier to seed (default full: every registered fn).
  --log <path>      Tee output under test-logs/
  --help, -h        Show help

Outputs: test-logs/bench/seed-<yyyymmdd_hhmmss>-<set>/{heavyweight,lightweight}.jsonl,
comparison.csv, summary.md, and one record per selected fn under
test-logs/bench/authoritative/ (tagged with the current commit — prefixed
dirty: when the tree is dirty — the corpus seed, and the run's rows + cells).
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --set) SET="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:seed"
validate_set "$SET" || exit 1
check_docker
setup_log_file "$LOG_PATH"

# The whole selected set is the function list to validate. Resolve it in the venv.
SELECTED=$(run_in_pyrx_venv "python -m databricks.labs.gbx.bench.store selected-names '$SET'")
RESOLVE_RC=$?
if [[ $RESOLVE_RC -ne 0 ]]; then
    echo -e "${RED}❌ resolving selected functions failed${NC}"
    echo "$SELECTED"
    exit $RESOLVE_RC
fi
SELECTED=$(printf '%s' "$SELECTED" | tr -d '\n')

if [[ -z "$SELECTED" ]]; then
    echo -e "${RED}❌ no functions selected for set=$SET — nothing to seed${NC}"
    exit 1
fi

N_SEL=$(printf '%s' "$SELECTED" | tr ',' '\n' | grep -c .)
echo -e "${CYAN}Seeding $N_SEL functions (set=$SET)${NC}"

COMMIT=$(git rev-parse HEAD)
if [[ -n "$(git status --porcelain)" ]]; then
    COMMIT="dirty:$COMMIT"
fi
VALIDATED_AT=$(date -u +%FT%TZ)
RUN_ID="seed-$(date +%Y%m%d_%H%M%S)-$SET"

echo -e "${CYAN}▶ benchmarking the whole set (set=$SET, run-id=$RUN_ID)${NC}"
# No --functions -> gbx:bench:all benchmarks the entire selected set.
bash "$SCRIPT_DIR/gbx-bench-all.sh" --set "$SET" --modes "pure-core" --run-id "$RUN_ID"
BENCH_RC=$?
if [[ $BENCH_RC -ne 0 ]]; then
    echo -e "${RED}❌ benchmark run failed (rc=$BENCH_RC) — no store records written${NC}"
    exit $BENCH_RC
fi

RUN_DIR="${PROJECT_ROOT}/test-logs/bench/${RUN_ID}"
CORPUS_JSON="${PROJECT_ROOT}/${HOST_CORPUS}/corpus.json"

echo -e "${CYAN}▶ writing authoritative store records${NC}"
# Shared store-write entry (DRY — same CLI gbx:bench:changed uses).
run_in_pyrx_venv "python -m databricks.labs.gbx.bench.store write-run '$RUN_DIR' '$SELECTED' '$COMMIT' '$VALIDATED_AT' '$SET' '$CORPUS_JSON'"
WRITE_RC=$?
if [[ $WRITE_RC -ne 0 ]]; then
    echo -e "${RED}❌ writing store records failed (rc=$WRITE_RC)${NC}"
    exit $WRITE_RC
fi

STORE_DIR="${PROJECT_ROOT}/test-logs/bench/authoritative"
N_RECORDS=$(find "$STORE_DIR" -maxdepth 1 -name '*.json' 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo -e "${GREEN}✅ bench:seed complete${NC}"
echo "   records written: $N_RECORDS"
echo "   store dir: $STORE_DIR"
exit 0
