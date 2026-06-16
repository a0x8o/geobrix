#!/bin/bash
# gbx:bench:clean — prune test-logs/bench/ (run dirs, orphan records, or everything).
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

MODE="runs"   # runs (default) | orphans | all
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:clean"
    cat <<'EOF'
Prune test-logs/bench/. By default removes ad-hoc run directories (everything
directly under test-logs/bench/ EXCEPT the authoritative/ store).

Usage: bash scripts/commands/gbx-bench-clean.sh [options]
Options:
  --runs        (DEFAULT) Delete every entry directly under test-logs/bench/
                except authoritative/ (the <run-id>/ dirs + any stray files).
                Keeps the authoritative store.
  --orphans     Like --runs, AND delete authoritative/<fn>.json for functions no
                longer in the registry (spec.select(set="full")).
  --all         Remove EVERYTHING under test-logs/bench/, including authoritative/.
                Destructive — requires this explicit flag.
  --log <path>  Tee output under test-logs/
  --help, -h    Show help
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --runs) MODE="runs"; shift ;;
    --orphans) MODE="orphans"; shift ;;
    --all) MODE="all"; shift ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:clean"
setup_log_file "$LOG_PATH"

BENCH_DIR="${PROJECT_ROOT}/test-logs/bench"
STORE_DIR="${BENCH_DIR}/authoritative"

if [[ ! -d "$BENCH_DIR" ]]; then
    echo "test-logs/bench/ does not exist — nothing to clean."
    exit 0
fi

if [[ "$MODE" == "all" ]]; then
    N=$(find "$BENCH_DIR" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')
    rm -rf "${BENCH_DIR:?}"/*
    echo -e "${GREEN}✅ removed everything under test-logs/bench/ ($N entries, including authoritative/)${NC}"
    exit 0
fi

# --runs and --orphans both prune run dirs / stray files (keep authoritative/).
REMOVED_RUNS=0
for entry in "$BENCH_DIR"/*; do
    [[ -e "$entry" ]] || continue            # no glob match -> skip the literal
    [[ "$entry" == "$STORE_DIR" ]] && continue  # keep the authoritative store
    rm -rf "$entry"
    REMOVED_RUNS=$((REMOVED_RUNS + 1))
done
echo -e "${CYAN}removed $REMOVED_RUNS run dir(s)/stray file(s) under test-logs/bench/${NC}"

if [[ "$MODE" == "orphans" ]]; then
    REMOVED_ORPHANS=0
    if [[ -d "$STORE_DIR" ]]; then
        ORPHANS=$(run_in_pyrx_venv "python -m databricks.labs.gbx.bench.store orphans")
        ORPH_RC=$?
        if [[ $ORPH_RC -ne 0 ]]; then
            echo -e "${RED}❌ resolving orphan records failed${NC}"
            echo "$ORPHANS"
            exit $ORPH_RC
        fi
        while IFS= read -r fn; do
            [[ -z "$fn" ]] && continue
            rm -f "${STORE_DIR}/${fn}.json"
            echo "  removed orphan record: ${fn}"
            REMOVED_ORPHANS=$((REMOVED_ORPHANS + 1))
        done <<< "$ORPHANS"
    fi
    echo -e "${CYAN}removed $REMOVED_ORPHANS orphan store record(s)${NC}"
fi

echo -e "${GREEN}✅ bench:clean complete (authoritative/ kept)${NC}"
exit 0
