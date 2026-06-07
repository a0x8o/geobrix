#!/bin/bash
# gbx:bench:stalecheck — advisory pre-push warning for changed fns with a
# missing/stale authoritative record. NEVER blocks (always exits 0), NEVER
# benchmarks, never needs Docker.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

BASE=""
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:stalecheck"
    cat <<'EOF'
Cheap, advisory staleness warning: for the functions whose sources changed
(working tree vs HEAD, or the diff vs --base <ref>), check whether their
authoritative store record is MISSING or STALE and WARN — listing them and
suggesting `gbx:bench:changed`. Read-only: NEVER benchmarks, NEVER blocks (always
exits 0), never needs Docker.

Usage: bash scripts/commands/gbx-bench-stalecheck.sh [options]
Options:
  --base <ref>   Diff vs <ref> (git diff --name-only <ref>); default: working
                 tree vs HEAD + untracked files.
  --log <path>   Tee output under test-logs/
  --help, -h     Show help
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --base) BASE="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:stalecheck"
setup_log_file "$LOG_PATH"

STALE_PY="
import sys
from databricks.labs.gbx.bench import store
base = sys.argv[1] or None
stale = store.stale_changed_functions(base=base)
print('STALE=' + ','.join(stale))
"

OUT=$(run_in_pyrx_venv "python -c \"$STALE_PY\" '$BASE'")
RC=$?
if [[ $RC -ne 0 ]]; then
    # Advisory only: surface the error but never block a push.
    echo -e "${YELLOW}⚠️  stalecheck could not run (rc=$RC) — skipping (advisory only)${NC}"
    echo "$OUT"
    exit 0
fi

STALE=$(printf '%s\n' "$OUT" | sed -n 's/^STALE=//p')

if [[ -z "$STALE" ]]; then
    echo -e "${GREEN}✅ no stale functions — every changed function's benchmark record is up to date${NC}"
    exit 0
fi

echo -e "${YELLOW}⚠️  changed functions with a missing or stale benchmark record:${NC}"
echo "   $STALE"
echo -e "   ${CYAN}re-validate them with:${NC} bash scripts/commands/gbx-bench-changed.sh"
echo "   (advisory only — this never blocks a push)"
exit 0
