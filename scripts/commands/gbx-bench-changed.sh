#!/bin/bash
# gbx:bench:changed — benchmark only the functions affected by the current changes.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

BASE=""
SET="full"
LIST=0
LOG_PATH=""
HOST_CORPUS="sample-data/Volumes/main/default/bench-corpus"

show_help() {
    show_banner "gbx:bench:changed"
    cat <<'EOF'
Resolve which registered functions are affected by the current working-tree
changes (or by the diff vs --base <ref>), then benchmark only those — heavyweight
(Docker) + lightweight (venv) + compare — and write authoritative store records
for each from that run. Uses each FnSpec.sources to map changed files -> functions.

Usage: bash scripts/commands/gbx-bench-changed.sh [options]
Options:
  --base <ref>     Diff vs <ref> (git diff --name-only <ref>); default: working
                   tree vs HEAD + untracked files.
  --set <core|full> Registry tier consulted when mapping changes -> functions
                   (default full: any changed registered function is caught).
  --list           Dry-run: print affected functions + unmapped-path warnings and
                   exit 0. No benchmarking, no store writes.
  --log <path>     Tee output under test-logs/
  --help, -h       Show help

Outputs (non --list): test-logs/bench/changed-<epoch>/{heavyweight,lightweight}.jsonl,
comparison.csv, summary.md, and one record per affected fn under
test-logs/bench/authoritative/.
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --base) BASE="$2"; shift 2 ;;
    --set) SET="$2"; shift 2 ;;
    --list) LIST=1; shift ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:changed"
validate_set "$SET" || exit 1
setup_log_file "$LOG_PATH"

# Resolve affected functions in the venv. Emit a machine-readable AFFECTED= /
# UNMAPPED= / CHANGED= triple plus a human summary, so the shell can branch on it.
RESOLVE_PY="
import sys
from databricks.labs.gbx.bench import spec as S, store
base = sys.argv[1] or None
which = sys.argv[2]
specs = S.select(set=which)
changed, affected, unmapped = store.resolve_changed(base=base, specs=specs)
print('CHANGED=' + ','.join(changed))
print('AFFECTED=' + ','.join(affected))
print('UNMAPPED=' + ','.join(unmapped))
"

RESOLVE_OUT=$(run_in_pyrx_venv "python -c \"$RESOLVE_PY\" '$BASE' '$SET'")
RESOLVE_RC=$?
if [[ $RESOLVE_RC -ne 0 ]]; then
    echo -e "${RED}❌ change resolution failed${NC}"
    echo "$RESOLVE_OUT"
    exit $RESOLVE_RC
fi

CHANGED=$(printf '%s\n' "$RESOLVE_OUT" | sed -n 's/^CHANGED=//p')
AFFECTED=$(printf '%s\n' "$RESOLVE_OUT" | sed -n 's/^AFFECTED=//p')
UNMAPPED=$(printf '%s\n' "$RESOLVE_OUT" | sed -n 's/^UNMAPPED=//p')

echo ""
echo -e "${CYAN}Changed paths:${NC} ${CHANGED:-(none)}"
echo -e "${CYAN}Affected functions:${NC} ${AFFECTED:-(none)}"
if [[ -n "$UNMAPPED" ]]; then
    echo -e "${YELLOW}⚠️  Unmapped changed paths (in no function's sources):${NC} $UNMAPPED"
fi
echo ""

if [[ -z "$AFFECTED" ]]; then
    echo "no benchmarked functions affected by current changes"
    exit 0
fi

if [[ $LIST -eq 1 ]]; then
    echo "(--list) dry-run only — no benchmarking performed."
    exit 0
fi

# Real run: needs Docker for the heavyweight side.
check_docker

COMMIT=$(git rev-parse HEAD)
if [[ -n "$(git status --porcelain)" ]]; then
    COMMIT="dirty:$COMMIT"
fi
VALIDATED_AT=$(date -u +%FT%TZ)
RUN_ID="changed-$(date +%s)"

echo -e "${CYAN}▶ benchmarking affected functions (set=$SET, run-id=$RUN_ID)${NC}"
bash "$SCRIPT_DIR/gbx-bench-all.sh" --set "$SET" --functions "$AFFECTED" \
    --modes "pure-core" --run-id "$RUN_ID"
BENCH_RC=$?
if [[ $BENCH_RC -ne 0 ]]; then
    echo -e "${RED}❌ benchmark run failed (rc=$BENCH_RC) — no store records written${NC}"
    exit $BENCH_RC
fi

RUN_DIR="${PROJECT_ROOT}/test-logs/bench/${RUN_ID}"
CORPUS_JSON="${PROJECT_ROOT}/${HOST_CORPUS}/corpus.json"

echo -e "${CYAN}▶ writing authoritative store records${NC}"
WRITE_PY="
import json, sys
from pathlib import Path
from databricks.labs.gbx.bench import spec as S, store
run_dir, fns_csv, commit, validated_at, which, corpus_json = sys.argv[1:7]
fns = [f for f in fns_csv.split(',') if f]
specs_by_name = {s.name: s for s in S.select(set=which)}
corpus = 'unknown'
cp = Path(corpus_json)
if cp.exists():
    d = json.loads(cp.read_text())
    corpus = 'seed=%s' % d.get('seed', 'unknown')
written = store.write_records_from_run(
    run_dir, fns, commit=commit, validated_at=validated_at,
    which=which, corpus=corpus, specs_by_name=specs_by_name,
)
for fn, path in sorted(written.items()):
    print('  validated -> %s (%s)' % (fn, path))
"

run_in_pyrx_venv "python -c \"$WRITE_PY\" '$RUN_DIR' '$AFFECTED' '$COMMIT' '$VALIDATED_AT' '$SET' '$CORPUS_JSON'"
WRITE_RC=$?
if [[ $WRITE_RC -ne 0 ]]; then
    echo -e "${RED}❌ writing store records failed (rc=$WRITE_RC)${NC}"
    exit $WRITE_RC
fi

echo ""
echo -e "${GREEN}✅ bench:changed complete${NC}"
echo "   affected fns validated -> store: $AFFECTED"
[[ -n "$UNMAPPED" ]] && echo -e "   ${YELLOW}unmapped changed paths: $UNMAPPED${NC}"
exit 0
