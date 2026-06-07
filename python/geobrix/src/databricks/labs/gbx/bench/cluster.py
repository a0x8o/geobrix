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

os.makedirs(OUT, exist_ok=True)
corpus = _m.Corpus.read(f"{{CORPUS}}/corpus.json")
fnspecs = _s.select(functions=[x for x in FUNCTIONS.split(",") if x] or None, set=SET)
lw, hw, all_rows = [], [], []
"""

_LIGHT = """
if MODES in ("pure-core", "both"):
    lw += runner.run_pure_core(CORPUS, corpus, fnspecs, RUN_ID, WARMUP, MEASURED, "cluster")
if MODES in ("spark-path", "both"):
    lw += runner.run_spark_path(spark, CORPUS, corpus, fnspecs, RUN_ID, ROW_COUNTS, WARMUP, MEASURED, "cluster")
results.write_jsonl(lw, f"{OUT}/lightweight.jsonl")
all_rows += lw
"""

_HEAVY = """
hw_out = f"{OUT}/heavyweight.jsonl"
spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(
    spark._jsparkSession, CORPUS, hw_out, FUNCTIONS, MODES,
    ",".join(str(x) for x in ROW_COUNTS), WARMUP, MEASURED, RUN_ID)
hw = results.read_jsonl(hw_out)
all_rows += hw
"""

_EPILOGUE = """
delta_rows = _cl.to_delta(all_rows, spark, TABLE, where="cluster")
compared = 0
if hw and lw:
    cells, unmatched = compare.compare_cells(hw, lw)
    compare.write_csv(cells, f"{OUT}/comparison.csv")
    with open(f"{OUT}/summary.md", "w") as fh:
        fh.write(compare.summarize_compare(cells, unmatched, hw, lw))
    compared = len(cells)

dbutils.notebook.exit(json.dumps(dict(
    rows=len(all_rows), delta_rows=delta_rows, compared=compared, out=OUT, table=TABLE)))
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
    )
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
