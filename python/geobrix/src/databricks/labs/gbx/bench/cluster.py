"""Cluster-side helpers: persist bench rows to a Delta table + build the bench notebook."""

from __future__ import annotations

from dataclasses import asdict, replace
from typing import List

from databricks.labs.gbx.bench.results import ResultRow


def rows_to_dataframe(rows: List[ResultRow], spark, where: str = "cluster"):
    """Build a Spark DataFrame from ResultRows, re-tagging env_where (e.g. 'cluster')."""
    import pandas as pd

    retagged = [replace(r, env_where=where) for r in rows]
    return spark.createDataFrame(pd.DataFrame([asdict(r) for r in retagged]))


def to_delta(rows: List[ResultRow], spark, table: str, where: str = "cluster") -> int:
    """Append ResultRows to the bench_results Delta table. Returns row count. (Cluster-only.)"""
    if not rows:
        return 0
    df = rows_to_dataframe(rows, spark, where=where)
    df.write.format("delta").mode("append").saveAsTable(table)
    return len(rows)


def _cell(source: str, kind: str = "code") -> dict:
    return {
        "cell_type": kind,
        "metadata": {},
        "outputs": [],
        "execution_count": None,
        "source": source.splitlines(keepends=True),
    }


_PREAMBLE = """import json
import os

from databricks.labs.gbx.bench import compare, results, runner
from databricks.labs.gbx.bench import cluster as _cl
from databricks.labs.gbx.bench import manifest as _m
from databricks.labs.gbx.bench import spec as _s

CORPUS = {corpus!r}
OUT = {out_dir!r}
TABLE = {table!r}
RUN_ID = {run_id!r}
FUNCTIONS = {functions!r}
SET = {set!r}
MODES = {modes!r}
ROW_COUNTS = [int(x) for x in {row_counts!r}.split(",") if x]
WARMUP, MEASURED = {warmup}, {measured}
TRUNCATE = {truncate!r}
TRUNCATE_ALL = {truncate_all!r}
LIGHTWEIGHT, HEAVYWEIGHT = {lightweight!r}, {heavyweight!r}

os.makedirs(OUT, exist_ok=True)
corpus = _m.Corpus.read(f"{{CORPUS}}/corpus.json")
fnspecs = _s.select(functions=[x for x in FUNCTIONS.split(",") if x] or None, set=SET)
lw, hw, all_rows = [], [], []
"""

# Truncate up-front + define the incremental Delta sink. Kept OUT of _PREAMBLE (which
# is .format()-processed) so the runtime f-strings here use single braces. The sink
# appends each function's rows the moment that function finishes, so the run can be
# polled / queried in real time via SELECT ... WHERE run_id = RUN_ID. (With incremental
# flushing the table is truncated up-front, NOT at the end -- a late truncate would wipe
# the rows we just streamed in.)
_SINK = """
if TRUNCATE_ALL:
    # Whole-table reset: only THIS run's rows remain afterwards. Use for a clean table;
    # NOT for coexisting light+heavy run separately (the 2nd invocation would wipe the
    # 1st) -- use --truncate-results for that.
    try:
        spark.sql(f"TRUNCATE TABLE {TABLE}")
        print(f"TRUNCATED {TABLE} (whole table) -- only this run's rows will remain")
    except Exception as _e:  # table doesn't exist yet -> the first append creates it
        print(f"truncate-all skipped ({TABLE} absent): {_e}")
elif TRUNCATE:
    # Scope the clear to THIS run_id + the tier(s) this invocation writes, so the paired
    # tier of the same benchmark run is NOT wiped: a --heavyweight-only run clears only
    # its heavyweight rows for run_id and leaves the lightweight rows a separate
    # --lightweight-only run wrote (and vice versa); --modes both clears both. Other
    # run_ids are untouched. (Whole-table TRUNCATE would clobber the paired tier.)
    _apis = [a for a, on in (("lightweight", LIGHTWEIGHT), ("heavyweight", HEAVYWEIGHT)) if on]
    _in = ", ".join(f"'{a}'" for a in _apis)
    try:
        spark.sql(f"DELETE FROM {TABLE} WHERE run_id = '{RUN_ID}' AND api IN ({_in})")
        print(f"cleared prior rows: run_id={RUN_ID} api in [{_in}] (other runs/tiers kept)")
    except Exception as _e:  # table doesn't exist yet -> the first append creates it
        print(f"truncate skipped ({TABLE} absent/empty): {_e}")

_delta = [0]


def _sink(batch):
    _delta[0] += _cl.to_delta(batch, spark, TABLE, where="cluster")


def _show_md(title, text):
    # Render a generated summary inline in the run notebook (visible in the Databricks
    # run UI) the moment its tier finishes. displayHTML keeps the md pipe-tables aligned
    # in a monospace block; falls back to print if displayHTML isn't available.
    try:
        import html as _h
        displayHTML(
            "<h3 style='font-family:sans-serif;margin:8px 0'>" + _h.escape(title) + "</h3>"
            "<pre style='font-family:ui-monospace,SFMono-Regular,Menlo,monospace;"
            "font-size:12px;line-height:1.4;white-space:pre;overflow:auto;"
            "background:#f6f8fa;padding:12px;border-radius:6px;border:1px solid #d0d7de'>"
            + _h.escape(text) + "</pre>")
    except Exception:
        print("\\n===== " + title + " =====\\n" + text)


def _show_table(api):
    # Show this tier's streamed rows as an interactive table (display) right before its
    # summary, so the run notebook surfaces the raw bench_results rows too.
    _df = spark.sql(f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND api = '{api}'")
    try:
        display(_df)
    except Exception:
        _df.show(300, truncate=False)
"""

_LIGHT = """
if MODES in ("pure-core", "both"):
    lw += runner.run_pure_core(CORPUS, corpus, fnspecs, RUN_ID, WARMUP, MEASURED, "cluster", sink=_sink)
if MODES in ("spark-path", "both"):
    lw += runner.run_spark_path(spark, CORPUS, corpus, fnspecs, RUN_ID, ROW_COUNTS, WARMUP, MEASURED, "cluster", sink=_sink)
results.write_jsonl(lw, f"{OUT}/lightweight.jsonl")
# Always write a per-tier summary so every run (incl. lightweight-only, which
# has no heavy-vs-light comparison summary.md) leaves a readable summary file.
_lw_md = results.summarize(lw)
with open(f"{OUT}/lightweight.summary.md", "w") as fh:
    fh.write(_lw_md)
_show_table("lightweight")  # raw rows, then the summary
_show_md(f"Lightweight summary -- {RUN_ID}", _lw_md)  # (a) shown once light completes
all_rows += lw
"""

_HEAVY = """
# Heavy runs in the JVM (reads the Volume corpus via Spark; see HeavyRunner). It writes
# its shard to a LOCAL path ONE ROW AT A TIME, fsync'd per row (BenchIO.JsonlAppender) --
# the JVM can't write the /Volumes object-storage mount. To make the heavy run pollable
# live (like the lightweight sink) WITHOUT changing the JVM, run HeavyBenchMain in a
# thread and TAIL that shard: stream each newly-flushed row to the SAME Delta sink as it
# lands. The jsonl shard stays the durable artifact; all Delta writes go through the
# Python sink so heavy rows share the lightweight schema (safe for --modes both).
import threading
import time as _time

_local_hw_out = "/local_disk0/heavyweight.jsonl"
if os.path.exists(_local_hw_out):
    os.remove(_local_hw_out)
_hw_err = {}


def _run_heavy():
    try:
        spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(
            spark._jsparkSession, CORPUS, _local_hw_out, FUNCTIONS, MODES,
            ",".join(str(x) for x in ROW_COUNTS), WARMUP, MEASURED, RUN_ID)
    except Exception as _e:  # re-raised after the join
        _hw_err["e"] = _e


def _complete_lines(path):
    # Only lines the JVM has fully written: each append is text + "\\n" + flush + fsync,
    # so split("\\n")[:-1] drops a trailing partial line until it's complete.
    if not os.path.exists(path):
        return []
    with open(path) as _fh:
        return _fh.read().split("\\n")[:-1]


_hw_thread = threading.Thread(target=_run_heavy, daemon=True)
_hw_thread.start()
_hw_seen = 0
while True:
    _alive = _hw_thread.is_alive()
    _lines = _complete_lines(_local_hw_out)
    if len(_lines) > _hw_seen:
        _batch = [results.ResultRow(**json.loads(_l)) for _l in _lines[_hw_seen:] if _l.strip()]
        _sink(_batch)          # interim Delta append -> pollable live
        hw += _batch
        _hw_seen = len(_lines)
    if not _alive:             # thread done -> the read above already drained the tail
        break
    _time.sleep(2)
_hw_thread.join()
if _hw_err.get("e"):
    raise _hw_err["e"]
# Per-tier heavy summary (mirrors lightweight.summary.md) + show it once heavy completes.
_hw_md = results.summarize(hw)
with open(f"{OUT}/heavyweight.summary.md", "w") as fh:
    fh.write(_hw_md)
_show_table("heavyweight")  # raw rows, then the summary
_show_md(f"Heavyweight summary -- {RUN_ID}", _hw_md)  # (b) shown once heavy completes
all_rows += hw
"""

_EPILOGUE = """
# Rows were appended incrementally by _sink as each function finished, so there is no
# bulk append here -- delta_rows is the running count the sink accumulated.
delta_rows = _delta[0]

# Status-led result payload. Lead with a human status + counts; only surface
# `compared` (the heavy-vs-light comparison cell count) when BOTH tiers ran, so a
# lightweight-only run doesn't report a bare `compared: 0` that reads like failure.
ok = sum(1 for r in all_rows if getattr(r, "status", "ok") == "ok")
errors = sum(1 for r in all_rows if getattr(r, "status", "ok") == "error")
result = dict(
    status=("error" if errors else "success"),
    run_id=RUN_ID,
    rows=len(all_rows), ok=ok, errors=errors,
    delta_rows=delta_rows, table=TABLE, out=OUT,
)
if hw and lw:
    cells, unmatched = compare.compare_cells(hw, lw)
    compare.write_csv(cells, f"{OUT}/comparison.csv")
    _cmp_md = compare.summarize_compare(cells, unmatched, hw, lw)
    with open(f"{OUT}/summary.md", "w") as fh:
        fh.write(_cmp_md)
    _show_md(f"Heavy vs light comparison -- {RUN_ID}", _cmp_md)  # (c) final, both tiers ran
    result["compared"] = len(cells)
    result["summary"] = f"{OUT}/summary.md"
elif lw:
    result["summary"] = f"{OUT}/lightweight.summary.md"
else:
    result["summary"] = f"{OUT}/heavyweight.summary.md"

dbutils.notebook.exit(json.dumps(result))
"""


def build_bench_notebook(cfg: dict) -> dict:
    sel = cfg.get("set", "core")
    functions = cfg["functions"]
    # The Scala heavy runner reads an explicit FUNCTIONS list, not the Python
    # registry. When no explicit functions are named, resolve the selected tier
    # to concrete names so the heavy path honors --set core|full too.
    if not functions:
        from databricks.labs.gbx.bench import spec as _s

        functions = ",".join(f.name for f in _s.select(set=sel))
    body = _PREAMBLE.format(
        corpus=cfg["corpus"],
        out_dir=cfg["out_dir"],
        table=cfg["table"],
        run_id=cfg["run_id"],
        functions=functions,
        set=sel,
        modes=cfg["modes"],
        row_counts=cfg["row_counts"],
        warmup=cfg["warmup"],
        measured=cfg["measured"],
        truncate=cfg.get("truncate_results", False),
        truncate_all=cfg.get("truncate_all", False),
        lightweight=bool(cfg.get("lightweight")),
        heavyweight=bool(cfg.get("heavyweight")),
    )
    body += _SINK  # truncate up-front + define the incremental Delta sink
    if cfg.get("lightweight"):
        body += _LIGHT
    if cfg.get("heavyweight"):
        body += _HEAVY
    body += _EPILOGUE
    cells = [
        _cell(f"%pip install --quiet '{cfg['wheel']}[pyrx]'"),
        _cell("dbutils.library.restartPython()"),
        _cell(body),
    ]
    return {"cells": cells, "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
