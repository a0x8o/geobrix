#!/bin/bash
# gbx:bench:heavyweight — run the heavyweight (Scala/JNI) benchmark in the geobrix-dev container.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

CORPUS="/Volumes/main/default/bench-corpus"   # container-side path
RUN_ID="local"
OUT=""
FUNCTIONS=""
MODES="both"
ROW_COUNTS="10,100,1000,10000"
WARMUP="2"
MEASURED="5"
LOG_PATH=""

show_help() {
    show_banner "gbx:bench:heavyweight"
    cat <<'EOF'
Run the heavyweight (Scala/JNI RasterX) benchmark in the geobrix-dev container.
Reads the corpus from the container mount, writes a heavyweight JSONL shard.

Usage: bash scripts/commands/gbx-bench-heavyweight.sh [options]
Options:
  --corpus <dir>      Container-side corpus dir (default /Volumes/main/default/bench-corpus)
  --out <path>        Container-side JSONL out (default /root/geobrix/test-logs/bench/<run-id>/heavyweight.jsonl)
  --run-id <id>       Run id (default local)
  --functions <list>  Comma-separated rst_* names (default: all 19)
  --modes <m>         pure-core | spark-path | both (default both)
  --row-counts <l>    Spark-path row ladder (default 10,100,1000,10000)
  --warmup <n>        Warmup iters (default 2)
  --measured <n>      Measured iters (default 5)
  --log <path>        Tee output under test-logs/
  --help, -h          Show help
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --functions) FUNCTIONS="$2"; shift 2 ;;
    --modes) MODES="$2"; shift 2 ;;
    --row-counts) ROW_COUNTS="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --measured) MEASURED="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:heavyweight"
check_docker
setup_log_file "$LOG_PATH"

[[ -z "$OUT" ]] && OUT="/root/geobrix/test-logs/bench/${RUN_ID}/heavyweight.jsonl"

MVN="mvn test -PskipScoverage -DskipTests=false \
    -Dsuites='com.databricks.labs.gbx.bench.HeavyBenchSuite' \
    -Dgbx.bench.corpus='$CORPUS' -Dgbx.bench.out='$OUT' -Dgbx.bench.runId='$RUN_ID' \
    -Dgbx.bench.functions='$FUNCTIONS' -Dgbx.bench.modes='$MODES' \
    -Dgbx.bench.rowCounts='$ROW_COUNTS' -Dgbx.bench.warmup='$WARMUP' -Dgbx.bench.measured='$MEASURED'"

docker exec geobrix-dev /bin/bash -c "$DOCKER_MAVEN_ENV && cd /root/geobrix && $MVN"
EXIT_CODE=$?
HOST_OUT="${OUT/\/root\/geobrix\//$PROJECT_ROOT/}"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ heavyweight results: $HOST_OUT"
    # Symmetric with lightweight: emit heavyweight.summary.md via the venv python.
    run_in_pyrx_venv "python -m databricks.labs.gbx.bench.results --in '$HOST_OUT'"
    SUMMARY="${HOST_OUT%.jsonl}.summary.md"
    [[ -f "$SUMMARY" ]] && echo "   summary: $SUMMARY"
fi
exit $EXIT_CODE
