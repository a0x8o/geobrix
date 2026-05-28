#!/bin/bash
# Starts the geobrix-dev container with the repo bind-mounted at /root/geobrix.
# The mount source is resolved from git (not $PWD) so the container is always bound
# to the repository root regardless of which subdirectory you launch from, and so a
# stale/deleted CWD can't dangle the mount. Run from anywhere inside the repo:
#   sh scripts/docker/start_docker.sh

# Resolve the repository top-level via git. Errors clearly if not inside a git work tree.
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$REPO_ROOT" ] || [ ! -d "$REPO_ROOT" ]; then
  echo "❌ Not inside a git work tree (or top-level not found). cd into the geobrix repo and retry." >&2
  exit 1
fi

# Guard against binding to an ephemeral agent worktree (.claude/worktrees/*), which get
# auto-cleaned out from under a long-lived container and dangle the mount -> exec fails with
# "current working directory is outside of container mount namespace root".
case "$REPO_ROOT" in
  */.claude/worktrees/*)
    echo "⚠️  Top-level resolves to a temporary worktree:" >&2
    echo "      $REPO_ROOT" >&2
    echo "    These get auto-cleaned and will dangle the container mount." >&2
    echo "    cd into the main checkout before starting the dev container." >&2
    exit 1
    ;;
esac

docker run --platform linux/amd64 --name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v "$REPO_ROOT":/root/geobrix -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd geobrix-dev:ubuntu24-gdal311-spark /bin/bash
