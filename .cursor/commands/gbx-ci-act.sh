#!/bin/bash
# gbx:ci:act - Run GitHub Actions workflows locally via `act`, against a
# Databricks-aware runner image. The real .github/ tree is NEVER modified —
# the jfrog-auth composite is bind-mounted with a no-op stub at runtime.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Local CI dry-run with act"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:ci:act${NC} ${YELLOW}[act-arguments...]${NC}"
    echo ""
    echo -e "${CYAN}Common invocations:${NC}"
    echo -e "  ${GREEN}gbx:ci:act -l${NC}                                    List jobs"
    echo -e "  ${GREEN}gbx:ci:act -W .github/workflows/build_main.yml -j build${NC}    Run a specific job"
    echo -e "  ${GREEN}gbx:ci:act push${NC}                                  Simulate a push event"
    echo -e "  ${GREEN}gbx:ci:act pull_request${NC}                          Simulate a PR event"
    echo -e "  ${GREEN}gbx:ci:act --help${NC}                                Pass-through to act --help"
    echo ""
    echo -e "${CYAN}First run (one-time, ~5 min):${NC}"
    echo -e "  Builds ${YELLOW}geobrix-ci-runner:local${NC} Docker image."
    echo -e "  Requires act: ${YELLOW}brew install act${NC}"
    echo ""
    echo -e "${CYAN}What's pre-baked into the runner image:${NC}"
    echo -e "  • pip → ${YELLOW}pypi-proxy.dev.databricks.com${NC}"
    echo -e "  • Maven → ${YELLOW}maven-proxy.dev.databricks.com${NC}"
    echo -e "  • npm → ${YELLOW}npm-proxy.dev.databricks.com${NC}"
    echo ""
    echo -e "${CYAN}Caveats:${NC}"
    echo -e "  • JFrog OIDC is mocked (no real token); pip/maven/npm flow via corp proxy"
    echo -e "  • ${YELLOW}runs-on: larger-runners${NC} is treated as a label only — no actual"
    echo -e "    larger machine; uses your local Docker resources"
    echo -e "  • Real .github/ files are never modified — see scripts/ci-local/README.md"
    echo ""
    echo -e "${CYAN}Iteration loop:${NC}"
    echo -e "  1. Edit a workflow in .github/workflows/"
    echo -e "  2. ${GREEN}gbx:ci:act -W <workflow> -j <job>${NC}"
    echo -e "  3. When clean, push and let real CI exercise OIDC + larger runners"
    echo ""
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    show_help
    exit 0
fi

show_banner "Local CI dry-run with act"
exec bash "$PROJECT_ROOT/scripts/ci-local/run-act.sh" "$@"
