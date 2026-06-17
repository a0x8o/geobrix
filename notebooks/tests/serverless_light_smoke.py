#!/usr/bin/env python3
"""Serverless (v5, ARM) lightweight-API install smoke.

Submits a one-off **serverless** notebook run that (1) %pip-installs the
GeoBrix wheel with a parameterizable spec, (2) restarts Python, (3) probes
Spark-Connect health + key package versions + the pyrx import, and exits a
JSON verdict. Lets us iterate on the Serverless "halo" install without a
cluster.

Auth: mints an oauth-fe bearer token (the SDK's profile refresh is flaky on
some networks) and uses WorkspaceClient(host, token).

Usage (run under .venv-pyrx which has databricks-sdk):
    .venv-pyrx/bin/python notebooks/tests/serverless_light_smoke.py \
        --spec 'geobrix[light] @ file:///Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl' \
        --env-version 5
"""
import argparse
import json
import os
import subprocess
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, workspace

HOST = os.environ.get(
    "DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
NB_NAME = "geobrix_serverless_light_smoke"
DEFAULT_SPEC = (
    "geobrix[light] @ file:///Volumes/geospatial_docs/geobrix/sample-data/"
    "geobrix-0.4.0-py3-none-any.whl"
)


def token():
    out = subprocess.run(
        ["databricks", "auth", "token", "-p", "oauth-fe"],
        capture_output=True, text=True, timeout=60,
    ).stdout
    return json.loads(out)["access_token"]


def notebook_source(spec: str) -> str:
    # Databricks SOURCE-format Python notebook. %pip + restartPython, then probe.
    probe = '''
import json, importlib.metadata as md, warnings, traceback
r = {}
# 1) Spark-Connect health AFTER the install (protobuf bump can break Connect)
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    r["spark_range_count"] = int(spark.range(5).count())
    r["spark_ok"] = True
except Exception as e:
    r["spark_ok"] = False
    r["spark_error"] = repr(e)
# 2) Key package versions (confirm what the install upgraded)
for p in ["protobuf","idna","numpy","pandas","pyarrow","grpcio","ipython",
          "rasterio","shapely","rio-tiler","mapbox-vector-tile","pyogrio",
          "pyproj","scikit-image","xarray-spatial","h3","quadbin","scipy","numexpr"]:
    try:
        r["ver_"+p] = md.version(p)
    except Exception:
        r["ver_"+p] = "MISSING"
# 3) Import + register the lightweight raster API and run a tiny op
# 2b) MVT encode via the exact pyvx 2.x API (default_options + typed props)
try:
    import mapbox_vector_tile as mvt
    from shapely.geometry import Point
    b = mvt.encode(
        {"name": "t", "features": [
            {"geometry": Point(10, 10), "properties": {"id": 1, "name": "x", "v": 1.5}}]},
        default_options={"extents": 4096, "y_coord_down": True})
    r["mvt_encode_bytes"] = len(b)
    r["mvt_ok"] = len(b) > 0
except Exception as e:
    r["mvt_ok"] = False
    r["mvt_error"] = repr(e)
# 3) Import + register the lightweight raster API and run a tiny op
try:
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    r["pyrx_import"] = "ok"
    # a trivially small light op end-to-end via SQL would need data; just
    # confirm a registered function resolves in the catalog.
    try:
        funcs = [f.name for f in spark.catalog.listFunctions() if f.name.startswith("gbx_rst_")]
        r["gbx_rst_registered_count"] = len(funcs)
    except Exception as e:
        r["catalog_error"] = repr(e)
except Exception as e:
    r["pyrx_import"] = "ERROR"
    r["pyrx_error"] = repr(e)
    r["pyrx_trace"] = traceback.format_exc()[-1500:]
dbutils.notebook.exit(json.dumps(r))
'''
    return (
        "# Databricks notebook source\n"
        f"# MAGIC %pip install {spec}\n"
        "\n# COMMAND ----------\n"
        "dbutils.library.restartPython()\n"
        "\n# COMMAND ----------\n"
        + probe
    )


def diagnose_source(spec: str, dry_run: bool = True) -> str:
    # No %pip magic — run pip via subprocess to capture the FULL output (the
    # resolver/install error) under the v5 immutable constraints. With
    # dry_run=False this performs the REAL install so install-time failures
    # (uninstall-protected core packages, etc.) surface in the captured output.
    dr = '"--dry-run", ' if dry_run else ""
    body = f'''
import json, subprocess, sys
CONSTRAINTS = "/databricks/.core_packages/immutable-package-constraints.txt"
r = {{}}
reqs = {spec!r}.split(" ||| ")
import tempfile, os
rf = os.path.join(tempfile.gettempdir(), "gbx_reqs.txt")
open(rf, "w").write("\\n".join(reqs) + "\\n")
args = [sys.executable, "-m", "pip", "install", {dr}"--no-input",
        "-c", CONSTRAINTS, "-r", rf]
p = subprocess.run(args, capture_output=True, text=True)
r["rc"] = p.returncode
r["stdout"] = p.stdout[-7000:]
r["stderr"] = p.stderr[-7000:]
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def isolate_register_source() -> str:
    # Deps come from the job Environment spec. Wrap udf/udtf.register to log
    # which exact registration raises (the SUM:DOUBLE parse error).
    body = '''
import json, traceback
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from databricks.labs.gbx.pyrx import functions as F
log = []
orig_udf = spark.udf.register
orig_udtf = spark.udtf.register
def wrap_udf(name, f=None, *a, **k):
    try:
        return orig_udf(name, f, *a, **k)
    except Exception as e:
        log.append({"kind":"udf","name":name,"err":repr(e)[:400]}); raise
def wrap_udtf(name, cls=None, *a, **k):
    try:
        return orig_udtf(name, cls, *a, **k)
    except Exception as e:
        log.append({"kind":"udtf","name":name,"err":repr(e)[:400]}); raise
try:
    spark.udf.register = wrap_udf
    spark.udtf.register = wrap_udtf
    patched = True
except Exception as e:
    patched = False
r = {"patched": patched}
try:
    F.register(spark)
    r["register"] = "ok"
except Exception as e:
    r["register"] = "ERROR"
    r["error"] = repr(e)[:400]
    r["first_failed_call"] = log[-1] if log else "n/a (failure not in a wrapped register call)"
r["all_failures"] = log
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def probe_only_source() -> str:
    # No install in the notebook — deps come from the job Environment spec
    # (provisioned before the notebook runs; ephemeral env on the path first).
    body = '''
import json, traceback, importlib.metadata as md
r = {}
for pkg in ["protobuf","idna","typing_extensions","mapbox-vector-tile","rasterio",
            "rio-tiler","shapely","numpy","pyarrow"]:
    try: r["ver_"+pkg] = md.version(pkg)
    except Exception: r["ver_"+pkg] = "MISSING"
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    r["spark_ok"] = True; r["spark_range_count"] = int(spark.range(5).count())
except Exception as e:
    r["spark_ok"] = False; r["spark_error"] = repr(e)
try:
    import mapbox_vector_tile as mvt
    from shapely.geometry import Point
    b = mvt.encode({"name":"t","features":[{"geometry":Point(10,10),"properties":{"id":1,"name":"x","v":1.5}}]},
                   default_options={"extents":4096,"y_coord_down":True})
    r["mvt_ok"] = len(b) > 0; r["mvt_bytes"] = len(b)
except Exception as e:
    r["mvt_ok"] = False; r["mvt_error"] = repr(e)
try:
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    r["pyrx_import"] = "ok"
    r["gbx_rst_count"] = len([f.name for f in spark.catalog.listFunctions() if f.name.startswith("gbx_rst_")])
except Exception as e:
    r["pyrx_import"] = "ERROR"; r["pyrx_error"] = repr(e); r["pyrx_trace"] = traceback.format_exc()[-1000:]
try:
    from databricks.labs.gbx.pyvx import functions as vx
    vx.register(spark); r["pyvx_register"] = "ok"
except Exception as e:
    r["pyvx_register"] = "ERROR"; r["pyvx_error"] = repr(e)
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def funcval_source(spec: str) -> str:
    # Real subprocess install (bypasses the strict job-%pip magic) THEN exercise
    # the API in the same session: confirms the fixed wheel installs and the
    # lightweight API works (Spark-Connect health, versions, MVT encode, pyrx).
    body = f'''
import json, subprocess, sys, importlib, traceback
CONSTRAINTS = "/databricks/.core_packages/immutable-package-constraints.txt"
r = {{}}
p = subprocess.run([sys.executable,"-m","pip","install","--no-input","-c",CONSTRAINTS,{spec!r}],
                   capture_output=True, text=True)
r["install_rc"] = p.returncode
r["install_conflict_report"] = ("dependency conflicts" in (p.stdout+p.stderr))
r["install_tail"] = (p.stdout+p.stderr)[-1200:]
importlib.invalidate_caches()
import importlib.metadata as md
for pkg in ["protobuf","idna","typing_extensions","mapbox-vector-tile","rasterio","shapely","numpy","pyarrow"]:
    try: r["ver_"+pkg] = md.version(pkg)
    except Exception: r["ver_"+pkg] = "MISSING"
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    r["spark_ok"] = True; r["spark_range_count"] = int(spark.range(5).count())
except Exception as e:
    r["spark_ok"] = False; r["spark_error"] = repr(e)
try:
    import mapbox_vector_tile as mvt
    from shapely.geometry import Point
    b = mvt.encode({{"name":"t","features":[{{"geometry":Point(10,10),"properties":{{"id":1,"name":"x","v":1.5}}}}]}},
                   default_options={{"extents":4096,"y_coord_down":True}})
    r["mvt_ok"] = len(b) > 0; r["mvt_bytes"] = len(b)
except Exception as e:
    r["mvt_ok"] = False; r["mvt_error"] = repr(e)
try:
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    r["pyrx_import"] = "ok"
    r["gbx_rst_count"] = len([f.name for f in spark.catalog.listFunctions() if f.name.startswith("gbx_rst_")])
except Exception as e:
    r["pyrx_import"] = "ERROR"; r["pyrx_error"] = repr(e); r["pyrx_trace"] = traceback.format_exc()[-1200:]
try:
    from databricks.labs.gbx.pyvx import functions as vx
    vx.register(spark)
    r["pyvx_register"] = "ok"
except Exception as e:
    r["pyvx_register"] = "ERROR"; r["pyvx_error"] = repr(e)
dbutils.notebook.exit(json.dumps(r))
'''
    return "# Databricks notebook source\n" + body


def probe_mvt_source() -> str:
    body = '''
import json, subprocess, sys, re
CONSTRAINTS = "/databricks/.core_packages/immutable-package-constraints.txt"
out = {}
idx = subprocess.run([sys.executable,"-m","pip","index","versions","mapbox-vector-tile"],
                     capture_output=True, text=True)
out["index"] = (idx.stdout + idx.stderr)[-1500:]
res = {}
for ver in ["2.0.0","2.0.1","2.1.0","2.2.0"]:
    p = subprocess.run(
        [sys.executable,"-m","pip","install","--dry-run","--no-input","-c",CONSTRAINTS,
         "mapbox-vector-tile=="+ver],
        capture_output=True, text=True)
    txt = p.stdout + p.stderr
    prot = re.search(r"protobuf-([0-9][0-9.]*)", txt)
    res[ver] = {
        "rc": p.returncode,
        "protobuf_would_install": prot.group(1) if prot else "none (base 5.29.4 kept)",
        "conflict": "ERROR" in txt and "dependency conflicts" in txt,
        "tail": txt[-500:],
    }
out["results"] = res
dbutils.notebook.exit(json.dumps(out))
'''
    return "# Databricks notebook source\n" + body


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spec", default=DEFAULT_SPEC, help="pip install target")
    ap.add_argument("--env-version", default="5")
    ap.add_argument("--run-name", default="geobrix-serverless-light-smoke")
    ap.add_argument("--diagnose", action="store_true",
                    help="pip --dry-run resolver-conflict capture (no install)")
    ap.add_argument("--real-install", action="store_true",
                    help="with --diagnose: REAL subprocess install, captured output")
    ap.add_argument("--probe-mvt", action="store_true",
                    help="probe mapbox-vector-tile 2.x versions for protobuf requirement")
    ap.add_argument("--func-validate", action="store_true",
                    help="real subprocess install + exercise the API in-session")
    ap.add_argument("--env-deps", action="store_true",
                    help="install via the job Environment spec deps (not %pip); probe-only notebook")
    ap.add_argument("--isolate-register", action="store_true",
                    help="env-deps + wrap register to pinpoint the failing UDF registration")
    args = ap.parse_args()

    w = WorkspaceClient(host=HOST, token=token())
    if args.isolate_register:
        src = isolate_register_source()
        args.env_deps = True  # provision deps via the Environment spec
    elif args.env_deps:
        src = probe_only_source()
    elif args.func_validate:
        src = funcval_source(args.spec)
    elif args.probe_mvt:
        src = probe_mvt_source()
    elif args.diagnose:
        src = diagnose_source(args.spec, dry_run=not args.real_install)
    else:
        src = notebook_source(args.spec)
    nb_path = f"/Users/{w.current_user.me().user_name}/{NB_NAME}"
    print(f"Install spec: {args.spec}")
    print(f"Uploading smoke notebook to {nb_path} ...")
    import base64
    w.workspace.import_(
        path=nb_path, format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=base64.b64encode(src.encode()).decode(), overwrite=True,
    )

    print(f"Submitting serverless run (env v{args.env_version}) ...")
    run = w.jobs.submit(
        run_name=args.run_name,
        tasks=[jobs.SubmitTask(
            task_key="smoke",
            notebook_task=jobs.NotebookTask(notebook_path=nb_path),
            environment_key="light_env",
        )],
        environments=[jobs.JobEnvironment(
            environment_key="light_env",
            spec=compute.Environment(
                environment_version=args.env_version,
                dependencies=[args.spec] if args.env_deps else None,
            ),
        )],
    )
    run_id = run.bind()["run_id"] if hasattr(run, "bind") else run.run_id
    # run is a Wait; get the run_id
    try:
        run_id = run.run_id
    except Exception:
        pass
    print(f"Run submitted: run_id={run_id}")
    # poll
    final = None
    for i in range(60):
        r = w.jobs.get_run(run_id)
        life = str(r.state.life_cycle_state) if r.state else "?"
        res = str(r.state.result_state) if (r.state and r.state.result_state) else ""
        print(f"poll {i}: life={life} result={res}", flush=True)
        if "TERMINATED" in life or "INTERNAL_ERROR" in life or "SKIPPED" in life:
            final = r
            break
        time.sleep(20)
    if not final:
        print("POLL_TIMEOUT")
        return 1
    # fetch task output
    task_run_id = final.tasks[0].run_id
    out = w.jobs.get_run_output(task_run_id)
    print("=== run result_state:", final.state.result_state, "===")
    if final.state and final.state.state_message:
        print("state_message:", final.state.state_message[:500])
    if getattr(out, "notebook_output", None) and out.notebook_output.result:
        print("=== notebook exit JSON ===")
        try:
            print(json.dumps(json.loads(out.notebook_output.result), indent=2))
        except Exception:
            print(out.notebook_output.result)
    if getattr(out, "error", None):
        print("=== ERROR ===\n", out.error)
    if getattr(out, "error_trace", None):
        print("=== ERROR TRACE (tail) ===\n", out.error_trace[-2000:])
    print("run_page_url:", final.run_page_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
