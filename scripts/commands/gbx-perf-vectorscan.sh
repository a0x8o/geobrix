#!/bin/bash
# gbx:perf:vectorscan — scan pyrx core for vectorizable anti-patterns.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

STRICT=0
LOG_PATH=""

show_help() {
    show_banner "gbx:perf:vectorscan"
    cat <<'EOF'
Scan the pyrx core (python/.../pyrx/core/*.py) for vectorizable anti-patterns —
per-pixel Python work that belongs in NumPy/SciPy. Runs INDEPENDENT of the
heavy-vs-light benchmark, so it catches functions where both tiers are slow or
where light beats a slow heavy but still has easy headroom.

Flags: generic_filter, np.vectorize/frompyfunc/.apply(, scalar-lib calls in a
comprehension over coordinate arrays, and pixel-scale range loops. Ignores
benign per-band/per-zoom/per-stop loops. Annotate a genuinely-unavoidable line
with '# vectorscan: ok <reason>' to allowlist it.

Usage: bash scripts/commands/gbx-perf-vectorscan.sh [options]
Options:
  --strict        Exit non-zero if any non-allowlisted finding remains
                  (use as a perf-review / pre-push gate).
  --log <path>    Tee output under test-logs/
  --help, -h      Show help
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --strict) STRICT=1; shift ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:perf:vectorscan"
setup_log_file "$LOG_PATH"

ARGS=(--root "$PROJECT_ROOT")
[[ $STRICT -eq 1 ]] && ARGS+=(--strict)

python3 "$PROJECT_ROOT/scripts/perf/vectorscan.py" "${ARGS[@]}"
exit $?
