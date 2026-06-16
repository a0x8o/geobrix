#!/bin/bash
# gbx:lint:python - Run isort, black, flake8 on Python package (same as CI)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PY_DIR="$PROJECT_ROOT/python/geobrix"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Lint: Python (isort, black, flake8)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:lint:python${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--check${NC}         Check only (no edits). Default when run in CI or without --fix."
    echo -e "  ${GREEN}--fix${NC}            Apply isort and black, then run flake8 (runs on host so files are updated)."
    echo -e "  ${GREEN}--log <path>${NC}     Write output to log file."
    echo -e "  ${GREEN}--help${NC}           Show this help."
    echo ""
    echo -e "${CYAN}Modes:${NC}"
    echo -e "  ${YELLOW}--check${NC}  Runs in Docker (same as CI). Fails if imports or format are not clean."
    echo -e "  ${YELLOW}--fix${NC}    Runs on host. If isort/black/flake8 are missing, uses a venv at ${CYAN}python/geobrix/.venv${NC} and installs dev deps there."
    echo ""
}

MODE="check"
LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --check)
            MODE="check"
            shift
            ;;
        --fix)
            MODE="fix"
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "Lint: Python (isort, black, flake8)"
setup_log_file "$LOG_PATH"

run_check_docker() {
    check_docker
    echo -e "${CYAN}Running isort/black/flake8 in Docker (check only)...${NC}"
    echo ""
    show_separator
    # flake8 does NOT read pyproject.toml [tool.flake8] natively — CI relies on the
    # flake8-pyproject plugin (pinned in requirements-ci.txt) to honor its ignore list
    # (E203,E266,E501,W503) and max-line-length=88. The dev container's lockfile omits it,
    # so without this ensure-step flake8 falls back to defaults (79 cols, no ignores) and
    # floods false E501s that DON'T match CI. Install the CI-pinned version idempotently so
    # --check actually matches CI. (Pin in sync with python/geobrix/requirements-ci.txt.)
    docker exec geobrix-dev /bin/bash -c "cd /root/geobrix/python/geobrix && \
        { pip show flake8-pyproject >/dev/null 2>&1 || pip install -q 'flake8-pyproject==1.2.4' --break-system-packages; } && \
        isort --check-only src test && black --check src test && flake8 src test"
}

run_fix_host() {
    local need_venv=0
    local isort_cmd="isort"
    local black_cmd="black"
    local flake8_cmd="flake8"
    local venv_dir="$PY_DIR/.venv"

    if ! command -v isort &>/dev/null || ! command -v black &>/dev/null || ! command -v flake8 &>/dev/null; then
        need_venv=1
    fi

    if [ "$need_venv" -eq 1 ]; then
        if [ ! -d "$venv_dir" ] || [ ! -x "$venv_dir/bin/isort" ] || [ ! -x "$venv_dir/bin/black" ] || [ ! -x "$venv_dir/bin/flake8" ]; then
            echo -e "${CYAN}Creating venv at ${YELLOW}$venv_dir${NC} and installing dev deps..."
            python3 -m venv "$venv_dir" || { echo -e "${RED}Failed to create venv.${NC}"; exit 1; }
            # Pin bootstrap to the shared Ubuntu 24.04 noble base (DBR 17.3 / 18 LTS) — keep in sync with .github/actions/{scala,python}_build/action.yml,
            # scripts/docker/Dockerfile, scripts/geobrix-gdal-init.sh.
            "$venv_dir/bin/pip" install -q --upgrade pip==25.0.1 setuptools==74.0.0 wheel==0.45.1
            (cd "$PY_DIR" && "$venv_dir/bin/pip" install -q -e ".[dev]") || { echo -e "${RED}Failed to install python/geobrix[dev].${NC}"; exit 1; }
            echo -e "${GREEN}Venv ready.${NC}"
        fi
        isort_cmd="$venv_dir/bin/isort"
        black_cmd="$venv_dir/bin/black"
        flake8_cmd="$venv_dir/bin/flake8"
    fi

    echo -e "${CYAN}Applying isort and black, then running flake8 (on host)...${NC}"
    echo ""
    show_separator
    (cd "$PY_DIR" && "$isort_cmd" src test && "$black_cmd" src test && "$flake8_cmd" src test)
}

if [ "$MODE" = "fix" ]; then
    run_fix_host
else
    run_check_docker
fi

EXIT_CODE=$?
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Python lint passed.${NC}"
else
    echo -e "${RED}Python lint failed (exit code: $EXIT_CODE).${NC}"
    [ "$MODE" = "check" ] && echo -e "${CYAN}Tip: run with ${YELLOW}--fix${NC}${CYAN} (on host with dev deps) to auto-fix isort/black.${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
