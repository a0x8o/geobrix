#!/bin/bash
# gbx:data:push-jar - mvn clean package -DskipTests then upload BOTH jars: product *-jar-with-dependencies.jar -> GBX_ARTIFACT_VOLUME/, and bench *-tests.jar -> bundle volroot (overwrite if exists); GBX_BUNDLE_SKIP_JAR_UPLOAD=1 skips all, GBX_BUNDLE_SKIP_TESTS_JAR_UPLOAD=1 skips just the tests.jar

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT" || exit 1

# Prefer the project venv interpreter -- it has databricks-sdk (the bare `python` on PATH
# often does not, and the upload uses WorkspaceClient). Fall back to PATH python.
PY="python"
if [ -x "$PROJECT_ROOT/.venv-pyrx/bin/python" ]; then
  PY="$PROJECT_ROOT/.venv-pyrx/bin/python"
fi
"$PY" notebooks/tests/push_jar_to_volume.py
exit $?
