#!/usr/bin/env python3
"""
Push the heavy-vs-light benchmark notebook to the workspace and run it on a configured cluster.

Both APIs (heavyweight Scala/Spark + lightweight pyrx) run on the SAME cluster, giving a
true same-hardware comparison. The script builds a bench notebook (via the bench cluster
notebook builder), uploads it, and runs it as a one-off job. Results append to the
bench_results Delta table and land comparison.csv / summary.md under the out_dir on the
configured Volume.

The cluster + artifacts must be provisioned by the operator (see the installation docs):
- heavyweight: x86 DBR 17.3 LTS with the init script + bundle + geobrix wheel + the bench
  geobrix-*-tests.jar staged on a Volume (the tests.jar is attached here as a job library;
  the production fat JAR is installed by the heavyweight init script, NOT attached here).
- lightweight (incl. ARM): just the [pyrx] wheel (installed by the notebook's %pip cell).
  On ARM clusters use --lightweight-only (heavyweight is x86-only by design).

Requires: databricks-sdk, and env config (see databricks_cluster_config.example.env).

Usage:
  1. Copy databricks_cluster_config.example.env to databricks_cluster_config.env.
  2. Set DATABRICKS_HOST, DATABRICKS_TOKEN (or profile), CLUSTER_ID, GBX_BUNDLE_VOLUME_*.
  3. Optional: set GBX_BENCH_CORPUS, GBX_BENCH_RESULTS_TABLE, GBX_BENCH_TESTS_JAR_VOLUME_PATH,
     GBX_BUNDLE_WHEEL_VOLUME_PATH.
  4. Run: python push_and_run_bench_on_cluster.py [options]
     Options: --no-wait, --heavyweight-only, --lightweight-only, --run-id, --functions,
              --modes, --row-counts, --warmup, --measured
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Load config from .env in same dir
TESTS_DIR = Path(__file__).resolve().parent
_env_file = TESTS_DIR / "databricks_cluster_config.env"


def _strip_invisible(s: str) -> str:
    """Remove BOM and common invisible Unicode so env-derived paths are clean."""
    s = (s or "").strip()
    for c in ("﻿", "​", "‌", "‍", "\r"):
        s = s.replace(c, "")
    return s.strip()


if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = _strip_invisible(v)


def _geobrix_version() -> str:
    """Read version from python package __init__.py (avoid heavy imports)."""
    init_py = TESTS_DIR.parent.parent / "python" / "geobrix" / "src" / "databricks" / "labs" / "gbx" / "__init__.py"
    if init_py.exists():
        with open(init_py) as f:
            for line in f:
                line = line.strip()
                if line.startswith("__version__"):
                    # __version__ = "0.4.0"
                    if "=" in line:
                        v = line.split("=", 1)[1].strip().strip("'\"").strip()
                        if v:
                            return v
    return "0.4.0"


def _arg(flag: str, default: str) -> str:
    """Read --flag <value> from argv, else default."""
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def main() -> int:
    do_wait = "--no-wait" not in sys.argv
    heavyweight = "--lightweight-only" not in sys.argv
    lightweight = "--heavyweight-only" not in sys.argv

    run_id = _arg("--run-id", "cluster")
    functions = _arg("--functions", "")
    modes = _arg("--modes", "both")
    row_counts = _arg("--row-counts", "10,100,1000,10000")
    warmup = int(_arg("--warmup", "2"))
    measured = int(_arg("--measured", "5"))

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE", file=sys.stderr)
        return 2

    cluster_id = _strip_invisible(os.environ.get("CLUSTER_ID") or "")
    if not cluster_id:
        print("Set CLUSTER_ID (existing cluster to run the benchmark on)", file=sys.stderr)
        return 2

    catalog = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main")
    schema = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default")
    volume = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples")
    volroot = f"/Volumes/{catalog}/{schema}/{volume}"
    ver = _geobrix_version()

    # Wheel path: explicit or derived under the Volume root.
    wheel = _strip_invisible(os.environ.get("GBX_BUNDLE_WHEEL_VOLUME_PATH") or "").strip()
    if not wheel:
        wheel = f"{volroot}/geobrix-{ver}-py3-none-any.whl"

    corpus = _strip_invisible(os.environ.get("GBX_BENCH_CORPUS") or "").strip() or f"{volroot}/bench-corpus"
    table = _strip_invisible(os.environ.get("GBX_BENCH_RESULTS_TABLE") or "").strip() or f"{catalog}.{schema}.bench_results"
    tests_jar = _strip_invisible(os.environ.get("GBX_BENCH_TESTS_JAR_VOLUME_PATH") or "").strip() or f"{volroot}/geobrix-{ver}-tests.jar"
    out_dir = f"{volroot}/bench-out/{run_id}"

    cfg = dict(
        wheel=wheel,
        corpus=corpus,
        out_dir=out_dir,
        table=table,
        run_id=run_id,
        functions=functions,
        modes=modes,
        row_counts=row_counts,
        warmup=warmup,
        measured=measured,
        heavyweight=heavyweight,
        lightweight=lightweight,
    )

    # Import the notebook builder from the repo source (this runs on the HOST, not the cluster).
    sys.path.insert(0, "python/geobrix/src")
    try:
        from databricks.labs.gbx.bench.cluster import build_bench_notebook
    except ImportError as e:
        print(f"Could not import build_bench_notebook from repo source (python/geobrix/src): {e}", file=sys.stderr)
        print("Run this from the repo root so 'python/geobrix/src' resolves.", file=sys.stderr)
        return 2

    try:
        import io
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import compute, jobs
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    # Pre-flight: show the operator exactly what will run before submitting.
    print("=" * 64)
    print("gbx:bench:cluster pre-flight")
    print(f"  cluster_id : {cluster_id}")
    print(f"  scope      : heavyweight={heavyweight}  lightweight={lightweight}")
    print(f"  run_id     : {run_id}")
    print(f"  functions  : {functions or '(all)'}")
    print(f"  modes      : {modes}   row_counts={row_counts}")
    print(f"  warmup/meas: {warmup}/{measured}")
    print(f"  corpus     : {corpus}")
    print(f"  table      : {table}")
    print(f"  out_dir    : {out_dir}")
    print(f"  wheel      : {wheel}")
    if heavyweight:
        print(f"  tests.jar  : {tests_jar}  (attached as job library; fat JAR via cluster init script)")
    print("  NOTE: this submits a job to a real cluster and consumes compute.")
    print("=" * 64)

    nb = build_bench_notebook(cfg)
    nb_bytes = json.dumps(nb, indent=1).encode("utf-8")

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient(host=host, token=token)

    # Notebook path: env override or per-user default.
    notebook_path_from_env = _strip_invisible(os.environ.get("GBX_BENCH_RUNNER_NOTEBOOK_PATH") or "").strip() or None
    me = w.current_user.me()
    default_path = f"/Users/{me.user_name}/geobrix_bench_runner.ipynb"
    notebook_path = notebook_path_from_env or default_path
    if not notebook_path.endswith(".ipynb"):
        notebook_path = notebook_path.rstrip("/") + ".ipynb"
    # Ensure it doesn't have the /Workspace prefix for the SDK calls.
    notebook_path = "/" + notebook_path.strip().removeprefix("/Workspace").lstrip("/")

    # Ensure parent directory exists (workspace API).
    notebook_parent = Path(notebook_path).parent
    try:
        w.workspace.mkdirs(str(notebook_parent))
    except Exception:
        pass

    print(f"Uploading bench notebook to {notebook_path}...")
    # CRITICAL: ImportFormat.JUPYTER creates a NOTEBOOK, not a FILE.
    w.workspace.upload(
        notebook_path,
        io.BytesIO(nb_bytes),
        format=ImportFormat.JUPYTER,
        overwrite=True,
    )

    # Attach the bench tests.jar only for the heavyweight leg (it carries the bench Scala classes).
    libraries = [compute.Library(jar=tests_jar)] if heavyweight else None

    print("Submitting one-off benchmark run on cluster...")
    submit_waiter = w.jobs.submit(
        run_name=f"geobrix-bench-{run_id}",
        timeout_seconds=7200,
        tasks=[
            jobs.SubmitTask(
                task_key="run_bench",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    source=jobs.Source.WORKSPACE,
                ),
                libraries=libraries,
            )
        ],
    )

    if not do_wait:
        # Fire-and-forget: submit returns a waiter; we don't call .result().
        print("Run submitted. Use the Databricks UI to check status, "
              "or run without --no-wait to wait here.")
        return 0

    print("Waiting for run to finish...")
    run = submit_waiter.result()
    run_id_remote = run.run_id
    state = run.state
    if state and getattr(state, "life_cycle_state", None):
        lc = state.life_cycle_state.value if hasattr(state.life_cycle_state, "value") else str(state.life_cycle_state)
        if lc == "TERMINATED":
            result_state = (state.result_state.value if state.result_state and hasattr(state.result_state, "value") else str(state.result_state)) if state.result_state else "UNKNOWN"
            # Try to surface the notebook's dbutils.notebook.exit() payload.
            try:
                out = w.jobs.get_run_output(run.tasks[0].run_id) if run.tasks else None
                if out and getattr(out, "notebook_output", None) and out.notebook_output.result:
                    print("Notebook output:", out.notebook_output.result)
            except Exception:
                pass
            if result_state == "SUCCESS":
                print(f"Run {run_id_remote} finished successfully.")
                print(f"Results: Delta table {table} | out_dir {out_dir} (comparison.csv / summary.md).")
                return 0
            print(f"Run {run_id_remote} finished with result_state={result_state}", file=sys.stderr)
            print(f"Check the Delta table {table} and out_dir {out_dir} for partial results.", file=sys.stderr)
    else:
        print(f"Run state: {state}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
