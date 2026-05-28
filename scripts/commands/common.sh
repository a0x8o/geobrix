#!/bin/bash
# Common helper functions for GeoBrix Cursor commands

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Maven env for Docker runs: unset conflicting opts, set Jupyter dirs, and tune Maven/JVM for coverage
# MAVEN_OPTS speeds up builds and scoverage (G1GC + 4G heap when running in geobrix-dev)
export DOCKER_MAVEN_ENV="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && export MAVEN_OPTS=\"-Xmx4G -XX:+UseG1GC\""

check_docker() {
    if ! docker ps &> /dev/null; then
        echo -e "${RED}❌ Error: Docker is not running${NC}"
        echo "Start Docker and try again."
        exit 1
    fi
    
    if ! docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${RED}❌ Error: geobrix-dev container not found${NC}"
        echo "Start the development container first:"
        echo -e "  ${YELLOW}./scripts/docker/start_docker.sh${NC}"
        exit 1
    fi
    
    if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${YELLOW}⚠️  Container is not running. Starting...${NC}"
        docker start geobrix-dev
        sleep 2
    fi
}

resolve_log_path() {
    local log_arg="$1"
    
    if [ -z "$log_arg" ]; then
        echo ""
        return
    fi
    
    # Check if absolute path (starts with /)
    if [[ "$log_arg" == /* ]]; then
        echo "$log_arg"
        return
    fi
    
    # Check if it's just a filename (no directory separator)
    if [[ "$log_arg" != */* ]]; then
        echo "test-logs/$log_arg"
        return
    fi
    
    # It's a relative path - prepend test-logs/
    echo "test-logs/$log_arg"
}

# Central logging: truncate log on each run so every command gets a fresh file.
# Commands that use --log should call this (or setup_log); the only exception is
# scripts that tee a subprocess only—those must truncate explicitly (: > "$LOG_PATH").
#
# Tees all subsequent script output to BOTH the terminal and the log file, reliably under
# `bash` and `sh` alike. The previous implementation used bash-only process substitution
# (`exec > >(tee ...)`) which (a) is a parse error under POSIX sh, so it fell back to a
# file-only redirect that left the terminal silent, and (b) even under bash races the shell
# exit — bash does not wait for the tee in `>(...)`, so the last lines could be truncated.
#
# Mechanism here: a private FIFO drained by a backgrounded `tee`, plus an EXIT trap that
# closes the write end (so tee sees EOF and flushes) and waits for tee before the script
# exits. No process substitution → identical behavior in both shells, no lost tail output.
# Uses `printf '%b'` rather than `echo -e` (which prints a literal "-e" under /bin/sh).
setup_log_file() {
    local log_path="$1"
    [ -n "$log_path" ] || return 0

    mkdir -p "$(dirname "$log_path")"
    : > "$log_path"
    printf '%b\n' "${CYAN}📝 Logging to: ${YELLOW}${log_path}${NC}"

    # Private FIFO. If FIFOs are unavailable, degrade to file-only logging rather than fail.
    local fifo
    fifo="$(mktemp -u "${TMPDIR:-/tmp}/gbx-log.XXXXXX")" || { exec >>"$log_path" 2>&1; return 0; }
    if ! mkfifo "$fifo" 2>/dev/null; then
        exec >>"$log_path" 2>&1
        return 0
    fi

    exec 3>&1                            # save the real terminal stdout on fd 3
    tee -a "$log_path" <"$fifo" >&3 &    # tee drains the FIFO -> log file + terminal
    GBX_TEE_PID=$!                       # global on purpose: the EXIT trap below reads it
    exec >"$fifo" 2>&1                   # route all stdout+stderr into the FIFO
    rm -f "$fifo"                        # unlink now; open fds keep it alive until closed

    # Flush-safe teardown: capture the real exit code, restore stdout/stderr (closing the
    # FIFO write end so tee reaches EOF), wait for tee to finish, then exit with that code.
    # `exit` inside an EXIT trap does not re-run the trap, so this is not recursive.
    trap 'rc=$?; exec 1>&3 2>&3 3>&-; [ -n "${GBX_TEE_PID:-}" ] && wait "${GBX_TEE_PID}" 2>/dev/null; exit $rc' EXIT
}

show_banner() {
    local title="$1"
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${CYAN}$title${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_separator() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Print a clickable file:// URL for the report. Plain URL is clickable in most terminals/IDEs.
# Usage: print_report_link "/absolute/path/to/index.html"
print_report_link() {
    local report_path="$1"
    local abs_path
    abs_path="$(cd "$(dirname "$report_path")" 2>/dev/null && pwd)/$(basename "$report_path")"
    [ -n "$abs_path" ] || abs_path="$report_path"
    # file:// URL (three slashes for absolute path) - most UIs make this clickable
    printf 'file://%s\n' "$abs_path"
}

open_report() {
    local report_path="$1"
    
    if [ ! -f "$report_path" ]; then
        echo -e "${YELLOW}⚠️  Report file not found: $report_path${NC}"
        return 1
    fi
    
    echo -e "${CYAN}📊 Opening report: ${YELLOW}$report_path${NC}"
    
    # Detect OS and open accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$report_path"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        xdg-open "$report_path" &>/dev/null || echo -e "${YELLOW}⚠️  Could not open browser. Open manually: $report_path${NC}"
    else
        echo -e "${YELLOW}⚠️  Unsupported OS. Open manually: $report_path${NC}"
    fi
}

generate_timestamp() {
    date +%Y%m%d-%H%M%S
}

# Warn if the assembly JAR that Spark tests load via spark.jars is stale relative to Scala
# sources. A stale JAR silently tests old behavior and surfaces as UNRESOLVED_ROUTINE for
# functions added since the last `mvn package`. Non-fatal — prints a hint and returns.
# Usage: warn_if_jar_stale "$PROJECT_ROOT"
warn_if_jar_stale() {
    local project_root="$1"
    local rebuild='gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"'
    local jar
    jar=$(ls -t "$project_root"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
    if [ -z "$jar" ]; then
        echo -e "${YELLOW}⚠️  No assembly JAR in target/ — Spark tests load geobrix-*-jar-with-dependencies.jar via spark.jars.${NC}"
        echo -e "${YELLOW}   Build it first: ${rebuild}${NC}"
        echo ""
        return
    fi
    local newer
    newer=$(find "$project_root/src/main/scala" -name '*.scala' -newer "$jar" -print 2>/dev/null | head -n 1)
    if [ -n "$newer" ]; then
        echo -e "${YELLOW}⚠️  Assembly JAR is older than Scala sources — tests may fail with UNRESOLVED_ROUTINE on newly added functions.${NC}"
        echo -e "${YELLOW}   Stale JAR: $(basename "$jar")${NC}"
        echo -e "${YELLOW}   Rebuild:   ${rebuild}${NC}"
        echo ""
    fi
}

# Aliases for backward compatibility
print_banner() { show_banner "$@"; }
print_separator() { show_separator "$@"; }
setup_log() { setup_log_file "$@"; }

# Export helpers + color vars so they survive any subshell that doesn't re-source this file
# (observed on macOS bash 3.2: "show_separator: command not found" mid-script).
export RED GREEN YELLOW BLUE CYAN NC DOCKER_MAVEN_ENV
export -f check_docker resolve_log_path setup_log_file show_banner show_separator \
          print_report_link open_report generate_timestamp warn_if_jar_stale \
          print_banner print_separator setup_log 2>/dev/null || true
