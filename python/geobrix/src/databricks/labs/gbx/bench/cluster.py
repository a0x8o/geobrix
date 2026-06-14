"""Cluster-side helpers: persist bench rows to a Delta table + build the bench notebook."""

from __future__ import annotations

import dataclasses
from dataclasses import asdict, replace
from typing import List

from databricks.labs.gbx.bench.results import ResultRow

# Explicit on-write column order for the bench_results Delta table. The ResultRow
# dataclass field order keeps its defaults last (a dataclass constraint), but the
# table reads better with the timing/throughput metrics grouped and the per-iter
# distribution (iter_*) trailing. rows_to_dataframe .select()s to this order.
ORDER = [
    "run_event_num",
    "run_id",
    "api",
    "fn",
    "category",
    "mode",
    # Headline timing metrics sit right after `mode` for at-a-glance reading (seconds; per-
    # tile also in ms). The rest of the dims/throughput/env follow.
    "avg_wall_clock_s",
    "per_tile_avg_s",
    "per_tile_avg_ms",
    "tile_px",
    "bands",
    "dtype",
    "srid",
    "rows",
    "nodata_frac",
    "warmup_iters",
    "measured_iters",
    "throughput_mpix_s",
    "throughput_rows_s",
    "peak_rss_mb",
    "status",
    "note",
    "output_fingerprint",
    "env_arch",
    "env_cpu_model",
    "env_cpu_count",
    "env_os",
    "env_gbx_version",
    "env_gdal_version",
    "env_runtime_version",
    "env_where",
    "iter_median_s",
    "iter_min_s",
    "iter_p90_s",
    "iter_total_wall_clock_s",
]

# Guard against drift: ORDER must cover exactly the ResultRow fields, no more, no less.
_RR_FIELDS = {f.name for f in dataclasses.fields(ResultRow)}
if set(ORDER) != _RR_FIELDS:
    raise RuntimeError(
        "bench.cluster.ORDER out of sync with ResultRow fields: "
        f"missing={_RR_FIELDS - set(ORDER)} extra={set(ORDER) - _RR_FIELDS}"
    )


def rows_to_dataframe(rows: List[ResultRow], spark, where: str = "cluster"):
    """Build a Spark DataFrame from ResultRows, re-tagging env_where (e.g. 'cluster').

    Columns are emitted in the explicit ``ORDER`` (not dataclass field order) so the
    Delta table column layout is the human-readable grouping, with the per-iter
    distribution (iter_*) trailing.
    """
    import pandas as pd

    retagged = [replace(r, env_where=where) for r in rows]
    df = spark.createDataFrame(pd.DataFrame([asdict(r) for r in retagged]))
    return df.select(*ORDER)


def to_delta(rows: List[ResultRow], spark, table: str, where: str = "cluster") -> int:
    """Append ResultRows to the bench_results Delta table. Returns row count. (Cluster-only.)"""
    if not rows:
        return 0
    df = rows_to_dataframe(rows, spark, where=where)
    # mergeSchema so new ResultRow columns (e.g. iter_total/avg_wall_clock_s) append to an
    # existing bench_results table instead of failing on a schema mismatch.
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        table
    )
    return len(rows)


def _remap_heavy_iter_to_seconds(d: dict) -> dict:
    """Map a heavy Scala jsonl row (MILLISECOND timing keys) onto ResultRow second-scale
    fields, in place. The Scala BenchRow still emits ms under the old key names (Scala is
    unchanged); ResultRow now stores SECONDS, so rename each ms key to its iter_*_s / *_s
    field and divide by 1000. per_tile_avg_{s,ms} are derived from the median + rows (Scala
    emits neither). The heavy run notebook calls this so the conversion is shared + testable.
    """
    for _old, _new in (
        ("median_ms", "iter_median_s"),
        ("min_ms", "iter_min_s"),
        ("p90_ms", "iter_p90_s"),
        ("total_wall_clock_ms", "iter_total_wall_clock_s"),
        ("avg_wall_clock_ms", "avg_wall_clock_s"),
    ):
        if _old in d:
            d[_new] = d.pop(_old) / 1000.0
    _n = d.get("rows") or 0
    _ms = d.get("iter_median_s", 0.0) * 1000.0  # back to the original ms median
    d["per_tile_avg_ms"] = (_ms / _n) if (_ms and _n) else 0.0
    d["per_tile_avg_s"] = (_ms / _n / 1000.0) if (_ms and _n) else 0.0
    return d


def _cell(source: str, kind: str = "code", collapsed: bool = False) -> dict:
    # collapsed: hide this cell's SOURCE by default. Sets the JupyterLab standard
    # (`jupyter.source_hidden`) plus the legacy `collapsed` flag; the Databricks notebook /
    # job-run viewer may or may not honor either (jobs render a static notebook), so this is
    # best-effort -- it cleanly collapses in Jupyter/.ipynb viewers regardless.
    meta = {}
    if collapsed:
        meta = {"jupyter": {"source_hidden": True}, "collapsed": True}
    return {
        "cell_type": kind,
        "metadata": meta,
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
# Spark-path uses its own (usually smaller) iteration counts -- the N-tile sweep is the
# averaging, so 1 warm-up + 1 measured is the efficient default; pure-core keeps WARMUP/MEASURED.
SPARK_WARMUP, SPARK_MEASURED = {spark_warmup}, {spark_measured}
# Tiles per partition for the spark-path row DataFrame; 0 = auto (n / (slots*4), i.e.
# oversubscribe slots ~4x so finished slots grab pending tasks instead of idling on the
# straggler tail). Set via --override-partition-size.
PARTITION_SIZE = {partition_size}
TRUNCATE = {truncate!r}
TRUNCATE_ALL = {truncate_all!r}
RESUME = {resume!r}
# On --resume, FIX_ERRORS (default) re-runs fns whose only row is an error (e.g. after a
# code/JAR fix) by purging those error rows first; --no-fix-errors keeps them as-is.
FIX_ERRORS = {fix_errors!r}
# --redo-functions: force re-run this explicit list of fns for the selected (api, mode) by
# DELETING their existing rows first (whatever the status), so they re-run while every OTHER
# fn's rows stay. It is INDEPENDENT of the run scope (--set/--functions), so one run can
# resume-run the never-ran fns AND force-redo this named subset. Unlike --resume (runs only
# MISSING) or --truncate-* (clears broadly). CSV string of fn names ("" = redo nothing).
REDO_FUNCTIONS = {redo_functions!r}
LIGHTWEIGHT, HEAVYWEIGHT = {lightweight!r}, {heavyweight!r}
# --explain-only: build each spark-path fn's DataFrame and print/persist its physical plan
# WITHOUT timing or writing to Delta. Plans are also teed to EXPLAIN_DIR on the Volume so
# they can be harvested after the run. Diagnostic only -- no rows are produced.
EXPLAIN_ONLY = {explain_only!r}
EXPLAIN_DIR = OUT + "/explain"
# --benchmark-readers: also run the reader benchmark (raster_gbx vs gdal) on the cluster.
# --readers-only: ONLY run the reader benchmark, skip all fn benchmarks.
BENCHMARK_READERS = {benchmark_readers!r}
READERS_ONLY = {readers_only!r}
# --benchmark-pmtiles: also run the pmtiles writer benchmark (pmtiles_gbx vs pmtiles).
# --pmtiles-only: ONLY run the pmtiles benchmark, skip all fn benchmarks.
BENCHMARK_PMTILES = {benchmark_pmtiles!r}
PMTILES_ONLY = {pmtiles_only!r}
# --benchmark-vector: also run the vector reader benchmark (light *_gbx vs heavy *_ogr).
# --vector-only: ONLY run the vector reader benchmark, skip all fn benchmarks.
BENCHMARK_VECTOR = {benchmark_vector!r}
VECTOR_ONLY = {vector_only!r}
# --benchmark-mvt: also run the st_asmvt benchmark (light pyvx vs heavy vectorx).
# --mvt-only: ONLY run the MVT benchmark, skip all fn benchmarks.
BENCHMARK_MVT = {benchmark_mvt!r}
MVT_ONLY = {mvt_only!r}
# --benchmark-vector-tin: also run the TIN + legacy benchmark (light pyvx vs heavy vectorx).
# --vector-tin-only: ONLY run the TIN + legacy benchmark, skip all fn benchmarks.
BENCHMARK_VECTOR_TIN = {benchmark_vector_tin!r}
VECTOR_TIN_ONLY = {vector_tin_only!r}
# --benchmark-grid-quadbin: also run the quadbin grid benchmark (light pygx vs heavy gridx.quadbin).
# --grid-quadbin-only: ONLY run the quadbin grid benchmark, skip all fn benchmarks.
BENCHMARK_GRID_QUADBIN = {benchmark_grid_quadbin!r}
GRID_QUADBIN_ONLY = {grid_quadbin_only!r}
# --benchmark-fanout: also run the fan-out UDTF benchmark (all 8 streaming UDTFs).
# --fanout-only: ONLY run the fanout benchmark, skip all fn benchmarks.
BENCHMARK_FANOUT = {benchmark_fanout!r}
FANOUT_ONLY = {fanout_only!r}
# --fanout-scale: dial the synthetic fan-out size (default 1.0 -> meaningful but ~couple
# minutes on ~20 workers). Larger = more output rows per function.
FANOUT_SCALE = {fanout_scale}
# --vector-scale: read the scaled 1M-seed corpus (copies dir + seed file) instead of the
# tiny 4-file corpus. Requires the scaled corpus to have been generated first via
# gbx:bench:generate-vector-corpus and staged at {{CORPUS}}/vector-scale/<fmt>/.
VECTOR_SCALE = {vector_scale!r}
WRITER_ROWS = {writer_rows}
# --vector-legs reader|writer|both : run only the reader-ingest legs, only the writer-export
# leg, or both. Lets each reader/writer be benchmarked as its own isolated cluster job (a
# struggling reader can't block the writers, and the 14M writer-source table is only
# materialized for writer runs). Default both.
VECTOR_LEGS = {vector_legs!r}
# --vector-formats csv : restrict the scaled vector run to these light formats (e.g.
# "geojson_gbx"). Empty = all four. Combined with --vector-legs and --lightweight-only/
# --heavyweight-only, this runs ONE (format x tier x leg) per job for true cold isolation.
VECTOR_FORMATS = {vector_formats!r}

os.makedirs(OUT, exist_ok=True)
# Disable AQE so it can't coalesce the spark-path repartition back toward
# defaultParallelism (~slots) -- which reintroduces the straggler idle. The runner sets an
# explicit partition count per fn; with AQE off it's respected. Set both the Apache and the
# Databricks switches at runtime, then echo the effective values so the run log shows it took.
for _ck, _cv in (
    ("spark.sql.adaptive.enabled", "false"),
    ("spark.databricks.optimizer.adaptive.enabled", "false"),
):
    try:
        spark.conf.set(_ck, _cv)
    except Exception as _e:
        print(f"could not set {{_ck}}: {{_e}}")
print(
    "AQE: adaptive.enabled="
    + str(spark.conf.get("spark.sql.adaptive.enabled", "?"))
    + " databricks.adaptive="
    + str(spark.conf.get("spark.databricks.optimizer.adaptive.enabled", "?"))
)
corpus = _m.Corpus.read(f"{{CORPUS}}/corpus.json")
fnspecs = _s.select(functions=[x for x in FUNCTIONS.split(",") if x] or None, set=SET)
# --redo-functions FORCES its fns into the run scope even when they fall outside --set /
# --functions, so `--redo-functions X` actually RE-RUNS X. Purging alone (the old behavior)
# would DELETE X's rows and re-run nothing when X is not in the selected set -- a silent
# data-loss footgun. Union any redo fn missing from the base scope into BOTH fnspecs (the
# lightweight scope + the heavy _mode_names gate) AND FUNCTIONS (the csv run_heavy splits).
_redo_names = [x for x in REDO_FUNCTIONS.split(",") if x]
if _redo_names:
    _base_names = [f.name for f in fnspecs]
    _redo_missing = [x for x in _redo_names if x not in _base_names]
    if _redo_missing:
        fnspecs = list(fnspecs) + list(_s.select(functions=_redo_missing))
        FUNCTIONS = ",".join([x for x in FUNCTIONS.split(",") if x] + _redo_missing)
# Per-mode expected row counts: how many fns run in each mode. Used by --resume to tell a
# COMPLETE (tier x mode) section (already in the table for this run_id) from a partial one.
_PC_EXP = len([f for f in fnspecs if "pure-core" in f.modes])
_SP_EXP = len([f for f in fnspecs if "spark-path" in f.modes])
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

if RESUME:
    print(
        f"[resume] run_id={RUN_ID}: keeping existing rows; complete (tier x mode) "
        f"sections will be loaded + skipped. NOTE: only valid on the SAME cluster "
        f"config -- spark-path timings depend on cluster size (pure-core does not)."
    )

_delta = [0]

# Monotonic per-run event counter. Continue from the run's current max so --resume keeps
# numbering where it left off (loaded rows keep their numbers; new events get higher ones).
import dataclasses as _dc


def _max_event():
    try:
        _v = spark.sql(
            f"SELECT max(run_event_num) FROM {TABLE} WHERE run_id = '{RUN_ID}'"
        ).collect()[0][0]
        return int(_v) if _v is not None else 0
    except Exception:  # column/table absent on a fresh run
        return 0


_event = [_max_event()]


def _sink(batch):
    # Stamp each row's run_event_num in execution order (the sink is the single chokepoint
    # all rows flow through, in completion order) just before the Delta append.
    _numbered = []
    for _r in batch:
        _event[0] += 1
        _numbered.append(_dc.replace(_r, run_event_num=_event[0]))
    _delta[0] += _cl.to_delta(_numbered, spark, TABLE, where="cluster")


def _done_fns(api, mode):
    # Function-granular resume: the set of fn names already in the table for this
    # (run_id, api, mode). --resume LOADS these and runs only the fns NOT in this set, so
    # a re-run completes exactly what's missing -- the unfinished spark-path fns, or a
    # single fn whose row you DELETEd to force its re-run (e.g. the fixed rst_fillnodata).
    if not RESUME:
        return set()
    _rows = spark.sql(
        f"SELECT DISTINCT fn FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}'"
    ).collect()
    return {_r[0] for _r in _rows}


def _load_section(api, mode):
    # Reconstruct ResultRows already in the table for this (api, mode), so the final
    # comparison includes the kept (skipped) fns without re-running them. Columns are the
    # ResultRow field names (the table was written from ResultRow), so map by field name;
    # missing column -> dataclass default.
    import dataclasses

    _fields = {f.name for f in dataclasses.fields(results.ResultRow)}
    _rows = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}'"
    ).collect()
    _out = []
    for _r in _rows:
        _d = _r.asDict()
        _out.append(results.ResultRow(**{_k: _d[_k] for _k in _fields if _k in _d}))
    return _out


def _purge_errors(api, mode):
    # FIX_ERRORS default: on resume, DELETE prior error rows for this (api, mode) so those
    # fns count as MISSING and get re-run (and don't leave a stale error row beside the new
    # ok row). --no-fix-errors skips this -> errored fns stay 'done' and are not retried.
    if not (RESUME and FIX_ERRORS):
        return
    _n = spark.sql(
        f"SELECT count(*) FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}' AND status='error'"
    ).collect()[0][0]
    if int(_n) > 0:
        spark.sql(
            f"DELETE FROM {TABLE} WHERE run_id='{RUN_ID}' "
            f"AND api='{api}' AND mode='{mode}' AND status='error'"
        )
        print(f"[resume] {api} {mode}: purged {int(_n)} error row(s) -> will re-run them")


def _purge_functions(api, mode):
    # --redo-functions: DELETE the named fns' rows for this (api, mode) -- any status -- so
    # they count as MISSING and re-run, while every other fn's rows stay. Independent of the
    # run scope, so a resume run also force-redoes this subset. Targeted, run_id-scoped.
    _fns = [f for f in REDO_FUNCTIONS.split(",") if f]
    if not _fns:
        return
    _in = ",".join("'" + f + "'" for f in _fns)
    _n = spark.sql(
        f"SELECT count(*) FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}' AND fn IN ({_in})"
    ).collect()[0][0]
    if int(_n) > 0:
        spark.sql(
            f"DELETE FROM {TABLE} WHERE run_id='{RUN_ID}' "
            f"AND api='{api}' AND mode='{mode}' AND fn IN ({_in})"
        )
        print(f"[redo] {api} {mode}: purged {int(_n)} row(s) for {len(_fns)} fn(s) -> re-running")


def _show_md(title, text, path=None):
    # Render a generated summary inline in the run notebook (visible in the Databricks
    # run UI) as RENDERED markdown -- headings + GFM pipe-tables become real HTML.
    # Databricks' job-run UI does NOT render IPython.display.Markdown (that path only
    # renders in Jupyter), so convert markdown -> HTML and use displayHTML, which IS the
    # Databricks primitive for inline rich output. Fall back to IPython, then print.
    # path: when given, show the Volume location of the .md FILE as a line above the
    # rendered body so the run links to the artifact (e.g. .../bench-out/<run_id>/summary.md).
    _loc = ("\\n\\n**Summary file:** `" + path + "`") if path else ""
    _full = "### " + title + _loc + "\\n\\n" + text
    try:
        import markdown as _mdlib

        _html = _mdlib.markdown(_full, extensions=["tables", "fenced_code", "sane_lists"])
        displayHTML(_html)  # noqa: F821  (Databricks notebook global)
        return
    except Exception:
        pass
    try:
        from IPython.display import Markdown
        from IPython.display import display as _md_display

        _md_display(Markdown(_full))
        return
    except Exception:
        pass
    print("\\n===== " + title + " =====")
    if path:
        print("Summary file: " + path)
    print(text)


def show_section(api, mode, rows):
    # One (tier x mode) section: show its raw bench_results rows as an interactive table,
    # then render + persist a summary for just those rows. Each section lives in its OWN
    # notebook cell, so this output renders the moment the cell finishes -- the run UI is
    # a live, preserved view of progress (no need to download md from the Volume).
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND api = '{api}' AND mode = '{mode}'"
    )
    try:
        display(_df)
    except Exception:
        _df.show(300, truncate=False)
    _md = results.summarize(rows, pool_size=len(corpus.row_pool.tiles))
    _path = f"{OUT}/{api}.{mode}.summary.md"
    with open(_path, "w") as fh:
        fh.write(_md)
    _show_md(f"{api} {mode} summary -- {RUN_ID}", _md, path=_path)
"""

# Lightweight helper: pure Python/PySpark. Emitted only when --lightweight, so a
# heavyweight-only run never references run_pure_core / run_spark_path.
_LIGHT_HELPERS = """
def run_light(mode):
    # Run one lightweight mode over the selected fns, streaming each fn's rows to the Delta
    # sink as it finishes. Function-granular --resume: load the fns already in the table and
    # run ONLY the missing ones, so a re-run completes just what's left. Returns this
    # section's rows (loaded + newly run) for the section cell to summarize.
    # --explain-only is a spark-path-only diagnostic: print/persist plans, produce no rows.
    if EXPLAIN_ONLY:
        if mode != "spark-path":
            return []
        runner.run_spark_path(
            spark, CORPUS, corpus, fnspecs, RUN_ID, ROW_COUNTS, SPARK_WARMUP,
            SPARK_MEASURED, "cluster", sink=None, partition_size=PARTITION_SIZE,
            explain_only=True, explain_dir=EXPLAIN_DIR,
        )
        return []
    _purge_errors("lightweight", mode)
    _purge_functions("lightweight", mode)
    _done = _done_fns("lightweight", mode)
    _loaded = _load_section("lightweight", mode) if _done else []
    if _loaded:
        lw.extend(_loaded)
    # Scope the to-do to fns that actually HAVE this mode -- e.g. the *_agg aggregators are
    # spark-path-only (no pure-core form), so without this filter a pure-core resume would
    # forever report them as "missing" and call run_pure_core on them for zero rows.
    _todo = [f for f in fnspecs if f.name not in _done and mode in f.modes]
    if _loaded or _done:
        print(f"[resume] lightweight {mode}: loaded {len(_loaded)} existing fn(s), "
              f"running {len(_todo)} missing")
    _new = []
    if _todo:
        if mode == "pure-core":
            _new = runner.run_pure_core(
                CORPUS, corpus, _todo, RUN_ID, WARMUP, MEASURED, "cluster", sink=_sink
            )
        else:
            _new = runner.run_spark_path(
                spark, CORPUS, corpus, _todo, RUN_ID, ROW_COUNTS, SPARK_WARMUP, SPARK_MEASURED, "cluster", sink=_sink, partition_size=PARTITION_SIZE
            )
        lw.extend(_new)
    return _loaded + _new
"""

# Heavyweight helper: drives the Scala HeavyBenchMain. Emitted only when --heavyweight.
_HEAVY_HELPERS = """
def run_heavy(mode):
    # Run one heavy mode in the JVM. The JVM writes its shard to a LOCAL path one row at a
    # time (it can't write the /Volumes object-storage mount), so run HeavyBenchMain in a
    # thread and TAIL the shard, streaming each newly-flushed row to the SAME Delta sink as
    # the lightweight tier. Pure-core opens tiles via GDAL on the driver, which can't read
    # /Volumes -> stage a LOCAL corpus copy (dbutils is UC-aware) for pure-core; spark-path
    # reads the Volume directly (binaryFile, UC-aware) so it keeps CORPUS.
    # Function-granular --resume: load fns already in the table; run ONLY the missing ones
    # by passing a filtered FUNCTIONS list to the JVM (e.g. a single fn to re-run).
    _purge_errors("heavyweight", mode)
    _purge_functions("heavyweight", mode)
    _done = _done_fns("heavyweight", mode)
    _loaded = _load_section("heavyweight", mode) if _done else []
    if _loaded:
        hw.extend(_loaded)
    # Scope to fns that HAVE this mode (the *_agg aggregators are spark-path-only); otherwise
    # a pure-core resume perpetually reports the 7 aggregators as "missing" + dispatches them
    # to the JVM pure-core path for zero rows.
    _mode_names = {f.name for f in fnspecs if mode in f.modes}
    _todo_fns = [
        f for f in FUNCTIONS.split(",") if f and f not in _done and f in _mode_names
    ]
    if _loaded or _done:
        print(f"[resume] heavyweight {mode}: loaded {len(_loaded)} existing fn(s), "
              f"running {len(_todo_fns)} missing")
    if not _todo_fns:
        return _loaded
    _fns_csv = ",".join(_todo_fns)
    import threading
    import time as _time

    _out = f"/local_disk0/heavyweight.{mode}.jsonl"
    if os.path.exists(_out):
        os.remove(_out)
    _err = {}
    _root = CORPUS
    if mode == "pure-core":
        _root = "/local_disk0/bench-corpus-pc"
        if not os.path.exists(_root):
            dbutils.fs.cp(CORPUS, "file:" + _root, recurse=True)

    # Spark-path uses the (smaller) spark iteration counts; pure-core keeps WARMUP/MEASURED.
    _wu, _ms = (SPARK_WARMUP, SPARK_MEASURED) if mode == "spark-path" else (WARMUP, MEASURED)

    def _go():
        try:
            spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(
                spark._jsparkSession, CORPUS, _root, _out, _fns_csv, mode,
                ",".join(str(x) for x in ROW_COUNTS), _wu, _ms, RUN_ID)
        except Exception as _e:  # re-raised after the join
            _err["e"] = _e

    def _complete_lines(path):
        # Each append is text + "\\n" + flush + fsync, so split("\\n")[:-1] drops a
        # trailing partial line until it's complete.
        if not os.path.exists(path):
            return []
        with open(path) as _fh:
            return _fh.read().split("\\n")[:-1]

    # ms->s conversion of the heavy Scala jsonl rows uses the shared, unit-tested
    # _cl._remap_heavy_iter_to_seconds (imported in the preamble) so the cluster path and
    # the host tests exercise the SAME logic.
    _remap_iter = _cl._remap_heavy_iter_to_seconds

    _th = threading.Thread(target=_go, daemon=True)
    _th.start()
    _seen = 0
    _new = []
    while True:
        _alive = _th.is_alive()
        _lines = _complete_lines(_out)
        if len(_lines) > _seen:
            _batch = [results.ResultRow(**_remap_iter(json.loads(_l))) for _l in _lines[_seen:] if _l.strip()]
            _sink(_batch)          # interim Delta append -> pollable live
            hw.extend(_batch)
            _new.extend(_batch)
            _seen = len(_lines)
        if not _alive:             # thread done -> the read above drained the tail
            break
        _time.sleep(2)
    _th.join()
    if _err.get("e"):
        raise _err["e"]
    return _loaded + _new
"""

# One cell per (tier x mode) section. Each is emitted only when its tier+mode is selected,
# and renders its table + summary as soon as the cell completes.
_CELL_LIGHT_PURE = """# (a) Lightweight pure-core
show_section("lightweight", "pure-core", run_light("pure-core"))
"""
_CELL_HEAVY_PURE = """# (b) Heavyweight pure-core
show_section("heavyweight", "pure-core", run_heavy("pure-core"))
"""
_CELL_LIGHT_SPARK = """# (c) Lightweight spark-path
show_section("lightweight", "spark-path", run_light("spark-path"))
"""
_CELL_HEAVY_SPARK = """# (d) Heavyweight spark-path
show_section("heavyweight", "spark-path", run_heavy("spark-path"))
"""

_CELL_READERS = """# Reader benchmark: light raster_gbx vs heavy gdal (both on-cluster)
from databricks.labs.gbx.bench import readers as _rd
_rows_dir = f"{CORPUS}/rows"
_reader_rows = []
if LIGHTWEIGHT:
    _r = _rd.run_format_read(spark, _rows_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="lightweight", fmt="raster_gbx",
                             options={"filterRegex": r".*\\.tif$"}, where="cluster")
    _sink([_r]); lw.append(_r); _reader_rows.append(_r)
if HEAVYWEIGHT:
    _r = _rd.run_format_read(spark, _rows_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="heavyweight", fmt="gdal", where="cluster")
    _sink([_r]); hw.append(_r); _reader_rows.append(_r)
# Writer benchmark: light gtiff_gbx vs heavy gtiff_gdal (same raster_gbx-read input)
import shutil as _sh
_wsrc = f"{CORPUS}/rows"
if LIGHTWEIGHT:
    _wl = "/local_disk0/bench_writer_light"
    _sh.rmtree(_wl, ignore_errors=True)
    _wr = _rd.run_format_write(spark, _wsrc, _wl, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="raster_gbx", write_fmt="gtiff_gbx",
                               options={"filterRegex": r".*\\.tif$"}, where="cluster")
    _sink([_wr]); lw.append(_wr); _reader_rows.append(_wr)
if HEAVYWEIGHT:
    _wh = "/local_disk0/bench_writer_heavy"
    _sh.rmtree(_wh, ignore_errors=True)
    _wr = _rd.run_format_write(spark, _wsrc, _wh, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="heavyweight", read_fmt="raster_gbx", write_fmt="gtiff_gdal",
                               mode="append",  # heavy gdal writer is append-only (overwrite -> truncate error)
                               options={"filterRegex": r".*\\.tif$"}, where="cluster")
    _sink([_wr]); hw.append(_wr); _reader_rows.append(_wr)
if _reader_rows:
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category IN ('reader', 'writer')"
    )
    try:
        display(_df)
    except Exception:
        _df.show(100, truncate=False)
    _md = results.summarize(_reader_rows)
    _show_md(f"reader benchmark -- {RUN_ID}", _md)
"""

_CELL_PMTILES = """# PMTiles benchmark: light pmtiles_gbx vs heavy pmtiles (both on-cluster) + parity check
from databricks.labs.gbx.bench import readers as _rd
from pmtiles.reader import MemorySource, Reader as _PMReader
import pmtiles.reader as _pmr
import glob as _glob
import gzip as _gz
import os as _os
import shutil as _sh
# Both pmtiles writers are two-phase (executor-write -> driver-merge), so their
# intermediates MUST be on a filesystem shared across driver+executors. Node-local
# /local_disk0 fails the driver merge on a multi-node cluster. Use DBFS: light
# (pure-Python) via the /dbfs FUSE mount (its single-archive finalize is now
# rename-free, so sequential FUSE writes are safe); heavy (JVM) via the dbfs:
# scheme (it cannot use UC /Volumes by direct API). Parity decodes both via FUSE.
_DBFS_FUSE = "/dbfs/tmp/gbx_bench"
_os.makedirs(_DBFS_FUSE, exist_ok=True)
_pmtiles_rows = []
_pl = _ph = None
if LIGHTWEIGHT:
    _pl = _DBFS_FUSE + "/pmtiles_light.pmtiles"
    if _os.path.isdir(_pl):
        _sh.rmtree(_pl, ignore_errors=True)
    elif _os.path.exists(_pl):
        _os.remove(_pl)
    _r = _rd.run_pmtiles_write(spark, _pl, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               n_tiles=1000, shard_zoom=0, write_fmt="pmtiles_gbx",
                               where="cluster")
    _sink([_r]); lw.append(_r); _pmtiles_rows.append(_r)
if HEAVYWEIGHT:
    _ph = _DBFS_FUSE + "/pmtiles_heavy"            # FUSE path (for parity decode)
    _sh.rmtree(_ph, ignore_errors=True)
    _ph_save = "dbfs:/tmp/gbx_bench/pmtiles_heavy"  # dbfs: scheme (for the JVM writer)
    _r = _rd.run_pmtiles_write(spark, _ph_save, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               n_tiles=1000, shard_zoom=0, write_fmt="pmtiles",
                               where="cluster")
    _sink([_r]); hw.append(_r); _pmtiles_rows.append(_r)
# Parity check: decode all tiles from both archives and compare (z,x,y) keys+bytes.
if LIGHTWEIGHT and HEAVYWEIGHT and _pl and _ph:
    # The Python pmtiles Reader gzip-decompresses directories unconditionally, but
    # PMTiles directories may be uncompressed (the heavy Scala writer uses
    # internal_compression=NONE). Patch deserialize_directory to fall back to the
    # raw bytes (re-gzip so the lib's own parser still runs) -> reads both tiers.
    _orig_dd = _pmr.deserialize_directory
    def _dd_compat(_buf):
        try:
            return _orig_dd(_buf)
        except Exception:
            return _orig_dd(_gz.compress(bytes(_buf)))
    _pmr.deserialize_directory = _dd_compat

    def _decode_any(path):
        # Return {(z,x,y): bytes} for every tile in path (file or dir of *.pmtiles).
        # Read the whole archive into memory (sequential) and use MemorySource:
        # the DBFS/Volumes paths are cloud object storage and do not support the
        # mmap/seek that MmapSource needs.
        import os as _os2
        if _os2.path.isfile(path):
            files = [path]
        else:
            files = sorted(_glob.glob(_os2.path.join(path, "**", "*.pmtiles"), recursive=True))
        tiles = {}
        for _pf in files:
            with open(_pf, "rb") as _fh:
                _data = _fh.read()
            _rdr = _PMReader(MemorySource(_data))
            # min/max zoom live in the PMTiles header (not metadata); the Reader
            # tile accessor is .get(z, x, y) (returns None when absent).
            _hdr = _rdr.header()
            _zmin = int(_hdr["min_zoom"])
            _zmax = int(_hdr["max_zoom"])
            for _z in range(_zmin, _zmax + 1):
                _side = 2 ** _z
                for _x in range(_side):
                    for _y in range(_side):
                        _b = _rdr.get(_z, _x, _y)
                        if _b is not None:
                            tiles[(_z, _x, _y)] = bytes(_b)
        return tiles
    _verdict_path = "/dbfs/tmp/gbx_bench/parity_verdict.txt"
    try:
        _lt = _decode_any(_pl)
        _ht = _decode_any(_ph)
        _lk = set(_lt.keys())
        _hk = set(_ht.keys())
        if _lk == _hk and all(_lt[_k] == _ht[_k] for _k in _lk):
            _verdict = f"PMTILES PARITY: PASS ({len(_lk)} tiles, keys+bytes equal)"
        elif _lk == _hk:
            _bad = [_k for _k in _lk if _lt[_k] != _ht[_k]]
            _verdict = (f"PMTILES PARITY: FAIL bytes differ on {len(_bad)} tile(s) "
                        f"e.g. {sorted(_bad)[:5]} ({len(_lk)} tiles)")
        else:
            _verdict = (f"PMTILES PARITY: FAIL keyset differs -- light-only "
                        f"{sorted(_lk - _hk)[:5]}, heavy-only {sorted(_hk - _lk)[:5]} "
                        f"(light={len(_lk)}, heavy={len(_hk)})")
    except Exception as _pe:
        _verdict = f"PMTILES PARITY: ERROR -- {type(_pe).__name__}: {_pe}"
    print(_verdict)
    try:
        with open(_verdict_path, "w") as _vf:
            _vf.write(_verdict)
    except Exception as _we:
        print(f"(could not write verdict file: {_we})")
    # Hard gate: a parity mismatch or decode error fails the bench run.
    assert _verdict.startswith("PMTILES PARITY: PASS"), _verdict
if _pmtiles_rows:
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'writer' "
        f"AND fn IN ('pmtiles_gbx', 'pmtiles')"
    )
    try:
        display(_df)
    except Exception:
        _df.show(100, truncate=False)
    _md = results.summarize(_pmtiles_rows)
    _show_md(f"pmtiles benchmark -- {RUN_ID}", _md)
"""

_CELL_MVT = """# MVT benchmark: light pyvx st_asmvt vs heavy vectorx st_asmvt (+ decoded-feature parity)
from databricks.labs.gbx.bench import readers as _rd
import mapbox_vector_tile as _mvt_lib
_mvt_rows = []
_mvt_light_blobs = None  # {(z,x,y): bytes} decoded from light run
_mvt_heavy_blobs = None  # {(z,x,y): bytes} decoded from heavy run
_N_FEATURES = 500
_N_TILES = 10
if LIGHTWEIGHT:
    _r = _rd.run_mvt_agg(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                         api="lightweight", n_features=_N_FEATURES, n_tiles=_N_TILES,
                         where="cluster")
    _sink([_r]); lw.append(_r); _mvt_rows.append(_r)
    # Capture decoded blobs for parity: re-run the agg once (untimed) and collect.
    if _r.status == "ok":
        try:
            import pyspark.sql.functions as _F_mvt
            from shapely.geometry import box as _b
            from shapely import to_wkb as _wkb
            from pyspark.sql.types import (StructType, StructField, IntegerType,
                                           BinaryType, DoubleType, StringType)
            _z3 = 3
            _addrs = [(_z3, i % 8, (i // 8) % 8) for i in range(_N_TILES)]
            _rd2 = []
            for _i in range(_N_FEATURES):
                _tz, _tx, _ty = _addrs[_i % _N_TILES]
                # Mirror run_mvt_agg's coordinate spread: squares on a 16x16 grid over
                # the full [0,4096] extent so they survive the heavy MVT driver's
                # quantization (a packed band collapses to sub-pixel -> empty heavy tile).
                _slot = (_i // _N_TILES) % 256
                _cx = 128 + (_slot % 16) * 256; _cy = 128 + (_slot // 16) * 256
                _g = bytes(_wkb(_b(_cx-32, _cy-32, _cx+32, _cy+32)))
                _rd2.append((_tz, _tx, _ty, _g, _i, float(_i)*0.1, f"feat_{_i}"))
            _sch = StructType([
                StructField("z", IntegerType(), False), StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False), StructField("geom", BinaryType(), True),
                StructField("id", IntegerType(), True), StructField("score", DoubleType(), True),
                StructField("label", StringType(), True),
            ])
            _df2 = spark.createDataFrame(_rd2, schema=_sch).select(
                "z","x","y","geom",
                _F_mvt.struct(_F_mvt.col("id"), _F_mvt.col("score"), _F_mvt.col("label")).alias("attrs"),
            ).cache(); _df2.count()
            from databricks.labs.gbx.pyvx import functions as _vx
            _vx.register(spark)
            _lrows = (_df2.groupBy("z","x","y")
                .agg(_vx.st_asmvt(_F_mvt.col("geom"), _F_mvt.col("attrs"), _F_mvt.lit("layer")).alias("mvt"))
                .collect())
            _mvt_light_blobs = {(r["z"], r["x"], r["y"]): bytes(r["mvt"]) for r in _lrows if r["mvt"]}
        except Exception as _pe:
            print(f"MVT light parity capture error: {type(_pe).__name__}: {_pe}")
if HEAVYWEIGHT:
    _r = _rd.run_mvt_agg(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                         api="heavyweight", n_features=_N_FEATURES, n_tiles=_N_TILES,
                         where="cluster")
    _sink([_r]); hw.append(_r); _mvt_rows.append(_r)
    # Capture decoded blobs for parity.
    if _r.status == "ok":
        try:
            import pyspark.sql.functions as _F_mvth
            from shapely.geometry import box as _bh
            from shapely import to_wkb as _wkbh
            from pyspark.sql.types import (StructType, StructField, IntegerType,
                                           BinaryType, DoubleType, StringType)
            _z3h = 3
            _addrsh = [(_z3h, i % 8, (i // 8) % 8) for i in range(_N_TILES)]
            _rdh = []
            for _i in range(_N_FEATURES):
                _tz, _tx, _ty = _addrsh[_i % _N_TILES]
                # Mirror run_mvt_agg's coordinate spread: squares on a 16x16 grid over
                # the full [0,4096] extent so they survive the heavy MVT driver's
                # quantization (a packed band collapses to sub-pixel -> empty heavy tile).
                _slot = (_i // _N_TILES) % 256
                _cx = 128 + (_slot % 16) * 256; _cy = 128 + (_slot // 16) * 256
                _g = bytes(_wkbh(_bh(_cx-32, _cy-32, _cx+32, _cy+32)))
                _rdh.append((_tz, _tx, _ty, _g, _i, float(_i)*0.1, f"feat_{_i}"))
            _schh = StructType([
                StructField("z", IntegerType(), False), StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False), StructField("geom", BinaryType(), True),
                StructField("id", IntegerType(), True), StructField("score", DoubleType(), True),
                StructField("label", StringType(), True),
            ])
            _dfh = spark.createDataFrame(_rdh, schema=_schh).select(
                "z","x","y","geom",
                _F_mvth.struct(_F_mvth.col("id"), _F_mvth.col("score"), _F_mvth.col("label")).alias("attrs"),
            ).cache(); _dfh.count()
            from databricks.labs.gbx.vectorx import functions as _hx
            _hx.register(spark)
            _hrows = (_dfh.groupBy("z","x","y")
                .agg(_hx.st_asmvt(_F_mvth.col("geom"), _F_mvth.col("attrs"), _F_mvth.lit("layer")).alias("mvt"))
                .collect())
            _mvt_heavy_blobs = {(r["z"], r["x"], r["y"]): bytes(r["mvt"]) for r in _hrows if r["mvt"]}
        except Exception as _pe:
            print(f"MVT heavy parity capture error: {type(_pe).__name__}: {_pe}")
# Parity check: decode both tiers' MVT output and compare geometry+property counts.
if LIGHTWEIGHT and HEAVYWEIGHT and _mvt_light_blobs is not None and _mvt_heavy_blobs is not None:
    try:
        _lk = set(_mvt_light_blobs.keys())
        _hk = set(_mvt_heavy_blobs.keys())
        _parity_ok = True
        _parity_msg = []
        if _lk != _hk:
            _parity_ok = False
            _parity_msg.append(
                f"tile-key mismatch: light-only={sorted(_lk-_hk)[:3]} "
                f"heavy-only={sorted(_hk-_lk)[:3]} (light={len(_lk)}, heavy={len(_hk)})"
            )
        else:
            _feat_mismatches = []
            for _k in sorted(_lk):
                _ld = _mvt_lib.decode(_mvt_light_blobs[_k])
                _hd = _mvt_lib.decode(_mvt_heavy_blobs[_k])
                _ll = _ld.get("layer", {}).get("features", [])
                _hl = _hd.get("layer", {}).get("features", [])
                if len(_ll) != len(_hl):
                    _feat_mismatches.append(f"{_k}: light={len(_ll)} heavy={len(_hl)}")
                else:
                    # Order-independent: the two encoders may emit features in a different
                    # order, so compare the SET of feature ids per tile, not positionally.
                    _lids = {_f.get("properties", {}).get("id") for _f in _ll}
                    _hids = {_f.get("properties", {}).get("id") for _f in _hl}
                    if _lids != _hids:
                        _diff = sorted((_lids ^ _hids), key=lambda v: (v is None, v))[:5]
                        _feat_mismatches.append(f"{_k}: id-set differs (sym-diff {_diff})")
            if _feat_mismatches:
                _parity_ok = False
                _parity_msg.append("feature mismatches: " + "; ".join(_feat_mismatches[:5]))
        if _parity_ok:
            _verdict = f"MVT PARITY: PASS ({len(_lk)} tiles, feature counts + ids match)"
        else:
            _verdict = "MVT PARITY: FAIL -- " + "; ".join(_parity_msg)
    except Exception as _pe:
        _verdict = f"MVT PARITY: ERROR -- {type(_pe).__name__}: {_pe}"
    print(_verdict)
    assert _verdict.startswith("MVT PARITY: PASS"), _verdict
if _mvt_rows:
    _df_mvt = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'mvt'"
    )
    try:
        display(_df_mvt)
    except Exception:
        _df_mvt.show(100, truncate=False)
    _md = results.summarize(_mvt_rows)
    _show_md(f"mvt benchmark -- {RUN_ID}", _md)
"""

_CELL_VECTOR_TIN = """# TIN + legacy benchmark: light pyvx vs heavy vectorx, decoded-output parity.
# 4 functions: st_legacyaswkb (legacy migration) + st_triangulate /
# st_interpolateelevationbbox / st_interpolateelevationgeom (constrained-Delaunay TIN).
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from shapely import wkb as _shp_wkb
import pyspark.sql.functions as _F_tin
_tin_rows = []
_TIN_N_ROWS = 5
_TIN_N_POINTS = 25
_LEG_N_ROWS = 1000
if LIGHTWEIGHT:
    for _fn_run in (_rd.run_legacy_aswkb, _rd.run_triangulate,
                    _rd.run_interp_bbox, _rd.run_interp_geom):
        _kw = dict(api="lightweight", where="cluster")
        if _fn_run is _rd.run_legacy_aswkb:
            _kw["n_rows"] = _LEG_N_ROWS
        else:
            _kw["n_rows"] = _TIN_N_ROWS; _kw["n_points"] = _TIN_N_POINTS
        _r = _fn_run(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, **_kw)
        _sink([_r]); lw.append(_r); _tin_rows.append(_r)
if HEAVYWEIGHT:
    for _fn_run in (_rd.run_legacy_aswkb, _rd.run_triangulate,
                    _rd.run_interp_bbox, _rd.run_interp_geom):
        _kw = dict(api="heavyweight", where="cluster")
        if _fn_run is _rd.run_legacy_aswkb:
            _kw["n_rows"] = _LEG_N_ROWS
        else:
            _kw["n_rows"] = _TIN_N_ROWS; _kw["n_points"] = _TIN_N_POINTS
        _r = _fn_run(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, **_kw)
        _sink([_r]); hw.append(_r); _tin_rows.append(_r)
# Decoded-output parity (hard gate) -- rebuild the SAME deterministic corpus the run_*
# legs timed, then collect each tier through its native surface and compare decoded output.
if LIGHTWEIGHT and HEAVYWEIGHT:
    _verdicts = []
    # --- legacy: decoded-geometry equality (light collected BEFORE heavy registers the
    #     same SQL name; the later registration overwrites it). ---
    try:
        _ldata, _lschema = _cv.generate_legacy_structs(_LEG_N_ROWS)
        _ldf = spark.createDataFrame(_ldata, schema=_lschema)
        _ldf.createOrReplaceTempView("_leg_parity_v")
        from databricks.labs.gbx.pyvx import functions as _vx_leg
        _vx_leg.register(spark)
        _light_w = [bytes(r["w"]) for r in spark.sql(
            "SELECT gbx_st_legacyaswkb(g) AS w FROM _leg_parity_v").collect()]
        from databricks.labs.gbx.vectorx.jts.legacy import functions as _hx_leg
        _hx_leg.register(spark)
        _heavy_w = [bytes(r["w"]) for r in spark.sql(
            "SELECT gbx_st_legacyaswkb(g) AS w FROM _leg_parity_v").collect()]
        _leg_ok = len(_light_w) == len(_heavy_w) and len(_light_w) > 0
        if _leg_ok:
            for _lw, _hw in zip(_light_w, _heavy_w):
                _lg = _shp_wkb.loads(_lw); _hg = _shp_wkb.loads(_hw)
                if not (_lg.equals(_hg) and _lg.has_z and _hg.has_z):
                    _leg_ok = False; break
        _v = ("LEGACY PARITY: PASS (%d geometries, decoded equality + Z)" % len(_light_w)
              if _leg_ok else "LEGACY PARITY: FAIL -- decoded geometry mismatch")
    except Exception as _pe:
        _v = "LEGACY PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("LEGACY PARITY: PASS"), _v
    # --- TIN: register both tiers (different catalog paths -> coexist). ---
    from databricks.labs.gbx.pyvx import functions as _vx_tin
    from databricks.labs.gbx.vectorx import functions as _hx_tin
    _vx_tin.register(spark); _hx_tin.register(spark)
    _tdata, _tschema = _cv.generate_tin_points(_TIN_N_ROWS, n_points=_TIN_N_POINTS)
    _tdf = spark.createDataFrame(_tdata, schema=_tschema)
    _tdf.createOrReplaceTempView("_tin_parity_v")
    def _centroid(_blob):
        _g = _shp_wkb.loads(bytes(_blob)); _c = _g.centroid
        return (round(_c.x, 6), round(_c.y, 6))
    def _unmatched(_a, _b):
        for _cx, _cy in _a:
            if not any(abs(_cx-_bx) < 1e-6 and abs(_cy-_by) < 1e-6 for _bx, _by in _b):
                return (_cx, _cy)
        return None
    # triangulate: count + centroid-set match within 1e-6.
    try:
        _lt = spark.sql("SELECT t.triangle FROM _tin_parity_v, LATERAL "
                        "gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t").collect()
        _ht = _tdf.select(_F_tin.call_function(
            "gbx_st_triangulate", _F_tin.col("pts"), _F_tin.col("bl"), _F_tin.col("mt"),
            _F_tin.col("st"), _F_tin.col("spf"), _F_tin.lit("constrained")
        ).alias("triangle")).collect()
        _lc = sorted(_centroid(r["triangle"]) for r in _lt)
        _hc = sorted(_centroid(r["triangle"]) for r in _ht)
        _tri_ok = (len(_lt) == len(_ht) > 0 and _unmatched(_lc, _hc) is None
                   and _unmatched(_hc, _lc) is None)
        _v = ("TIN TRIANGULATE PARITY: PASS (light=heavy=%d triangles, centroids match)" % len(_lt)
              if _tri_ok else "TIN TRIANGULATE PARITY: FAIL -- light=%d heavy=%d (centroid/count mismatch)"
              % (len(_lt), len(_ht)))
    except Exception as _pe:
        _v = "TIN TRIANGULATE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("TIN TRIANGULATE PARITY: PASS"), _v
    # interp bbox + geom: same in-hull cell keyset + per-cell |dz| < 1e-6.
    def _grid_dict(_rows, _col):
        _out = {}
        for _r in _rows:
            _g = _shp_wkb.loads(bytes(_r[_col]))
            _out[(round(_g.x, 6), round(_g.y, 6))] = _g.z
        return _out
    # bbox
    try:
        _lb = _grid_dict(spark.sql(
            "SELECT t.elevation_point AS p FROM _tin_parity_v, LATERAL "
            "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, "
            "w, h, srid, 'constrained') t").collect(), "p")
        _hb = _grid_dict(_tdf.select(_F_tin.call_function(
            "gbx_st_interpolateelevationbbox", _F_tin.col("pts"), _F_tin.col("bl"),
            _F_tin.col("mt"), _F_tin.col("st"), _F_tin.col("spf"), _F_tin.col("xmin"),
            _F_tin.col("ymin"), _F_tin.col("xmax"), _F_tin.col("ymax"), _F_tin.col("w"),
            _F_tin.col("h"), _F_tin.col("srid"), _F_tin.lit("constrained")
        ).alias("p")).collect(), "p")
        _bb_ok = (_lb.keys() == _hb.keys() and len(_lb) > 0
                  and all(abs(_lb[_k] - _hb[_k]) < 1e-6 for _k in _lb))
        _v = ("TIN INTERP BBOX PARITY: PASS (%d cells, |dz|<1e-6)" % len(_lb)
              if _bb_ok else "TIN INTERP BBOX PARITY: FAIL -- cell/Z mismatch (light=%d heavy=%d)"
              % (len(_lb), len(_hb)))
    except Exception as _pe:
        _v = "TIN INTERP BBOX PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("TIN INTERP BBOX PARITY: PASS"), _v
    # geom
    try:
        _lg2 = _grid_dict(spark.sql(
            "SELECT t.elevation_point AS p FROM _tin_parity_v, LATERAL "
            "gbx_st_interpolateelevationgeom(pts, bl, mt, st, spf, origin, cols, rows_n, "
            "cell_x, cell_y, 'constrained') t").collect(), "p")
        _hg2 = _grid_dict(_tdf.select(_F_tin.call_function(
            "gbx_st_interpolateelevationgeom", _F_tin.col("pts"), _F_tin.col("bl"),
            _F_tin.col("mt"), _F_tin.col("st"), _F_tin.col("spf"), _F_tin.col("origin"),
            _F_tin.col("cols"), _F_tin.col("rows_n"), _F_tin.col("cell_x"),
            _F_tin.col("cell_y"), _F_tin.lit("constrained")
        ).alias("p")).collect(), "p")
        _gg_ok = (_lg2.keys() == _hg2.keys() and len(_lg2) > 0
                  and all(abs(_lg2[_k] - _hg2[_k]) < 1e-6 for _k in _lg2))
        _v = ("TIN INTERP GEOM PARITY: PASS (%d cells, |dz|<1e-6)" % len(_lg2)
              if _gg_ok else "TIN INTERP GEOM PARITY: FAIL -- cell/Z mismatch (light=%d heavy=%d)"
              % (len(_lg2), len(_hg2)))
    except Exception as _pe:
        _v = "TIN INTERP GEOM PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("TIN INTERP GEOM PARITY: PASS"), _v
if _tin_rows:
    _df_tin = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category IN ('tin','legacy')"
    )
    try:
        display(_df_tin)
    except Exception:
        _df_tin.show(100, truncate=False)
    _md = results.summarize(_tin_rows)
    _show_md(f"TIN + legacy benchmark -- {RUN_ID}", _md)
"""

_CELL_GRID_QUADBIN = """# Quadbin grid benchmark: light pygx vs heavy gridx.quadbin, exact-output parity.
# 4 representative shapes: pointascell (scalar) / polyfill (geom->ARRAY<cell>) /
# tessellate (struct-array) / cellunion_agg (grouped aggregate). Both tiers expose the
# SAME gbx_quadbin_* SQL names, so light is collected BEFORE heavy re-registers (the later
# registration overwrites the UDF) -- the same ordering trick as the legacy parity cell.
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from shapely import wkb as _shp_wkb
_quadbin_rows = []
_QB_N_ROWS = 1000
_QB_PAC_RES = 12   # pointascell resolution
_QB_GEOM_RES = 8   # polyfill / tessellate resolution
_QB_AGG_RES = 8    # cellunion_agg cell resolution
if LIGHTWEIGHT:
    _quadbin_rows.append(_rd.run_quadbin_pointascell(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_PAC_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_polyfill(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_tessellate(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_cellunion_agg(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_AGG_RES, where="cluster"))
    for _r in _quadbin_rows[-4:]:
        _sink([_r]); lw.append(_r)
if HEAVYWEIGHT:
    _hw_qb = []
    _hw_qb.append(_rd.run_quadbin_pointascell(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_PAC_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_polyfill(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_tessellate(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_cellunion_agg(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_AGG_RES, where="cluster"))
    for _r in _hw_qb:
        _sink([_r]); hw.append(_r); _quadbin_rows.append(_r)
# Exact-output parity (hard gate): rebuild the SAME deterministic corpora, collect each tier
# through its native SQL surface, and compare. Cells: exact equality. Decoded geometry: 1e-6.
if LIGHTWEIGHT and HEAVYWEIGHT:
    import pyspark.sql.functions as _F_qb
    _verdicts = []
    def _centroid_qb(_blob):
        _g = _shp_wkb.loads(bytes(_blob)); _c = _g.centroid
        return (round(_c.x, 6), round(_c.y, 6))
    # --- pointascell: exact cell-id equality (light BEFORE heavy: shared SQL name). ---
    try:
        _pdata, _pschema = _cv.generate_quadbin_points(_QB_N_ROWS)
        _pdf = spark.createDataFrame(_pdata, schema=_pschema)
        _pdf.createOrReplaceTempView("_qb_pac_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pac
        _gx_pac.register(spark)
        _light_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_quadbin_pointascell(lon, lat, %d) AS cell FROM _qb_pac_parity_v"
            % _QB_PAC_RES).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_pac
        _hx_pac.register(spark)
        _heavy_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_quadbin_pointascell(lon, lat, %d) AS cell FROM _qb_pac_parity_v"
            % _QB_PAC_RES).collect()]
        _pac_ok = (len(_light_cells) == len(_heavy_cells) > 0
                   and _light_cells == _heavy_cells)
        _v = ("QUADBIN POINTASCELL PARITY: PASS (%d cells, exact id equality)" % len(_light_cells)
              if _pac_ok else "QUADBIN POINTASCELL PARITY: FAIL -- cell id mismatch")
    except Exception as _pe:
        _v = "QUADBIN POINTASCELL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN POINTASCELL PARITY: PASS"), _v
    # --- polyfill: exact per-row cell-SET equality. ---
    try:
        _gdata, _gschema = _cv.generate_quadbin_polygons(_QB_N_ROWS)
        _gdf = spark.createDataFrame(_gdata, schema=_gschema)
        _gdf.createOrReplaceTempView("_qb_geom_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pf
        _gx_pf.register(spark)
        _light_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_quadbin_polyfill(geom, %d) AS cells FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_pf
        _hx_pf.register(spark)
        _heavy_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_quadbin_polyfill(geom, %d) AS cells FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        _pf_ok = (len(_light_pf) == len(_heavy_pf) > 0 and _light_pf == _heavy_pf)
        _ncells = sum(len(c) for c in _light_pf)
        _v = ("QUADBIN POLYFILL PARITY: PASS (%d rows, %d cells, exact set equality)"
              % (len(_light_pf), _ncells)
              if _pf_ok else "QUADBIN POLYFILL PARITY: FAIL -- cell set mismatch")
    except Exception as _pe:
        _v = "QUADBIN POLYFILL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN POLYFILL PARITY: PASS"), _v
    # --- tessellate: exact (cell, centroid) set per row; cells exact, centroid within 1e-6. ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ts
        _gx_ts.register(spark)
        def _chip_set(_chips):
            return sorted((int(c["cell"]), _centroid_qb(c["geom"])) for c in _chips)
        _light_ts = [_chip_set(r["chips"]) for r in spark.sql(
            "SELECT gbx_quadbin_tessellate(geom, %d) AS chips FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_ts
        _hx_ts.register(spark)
        _heavy_ts = [_chip_set(r["chips"]) for r in spark.sql(
            "SELECT gbx_quadbin_tessellate(geom, %d) AS chips FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        _ts_ok = len(_light_ts) == len(_heavy_ts) > 0
        if _ts_ok:
            for _lrow, _hrow in zip(_light_ts, _heavy_ts):
                if len(_lrow) != len(_hrow):
                    _ts_ok = False; break
                for (_lc, (_lx, _ly)), (_hc, (_hx2, _hy)) in zip(_lrow, _hrow):
                    if _lc != _hc or abs(_lx - _hx2) >= 1e-6 or abs(_ly - _hy) >= 1e-6:
                        _ts_ok = False; break
                if not _ts_ok:
                    break
        _nchips = sum(len(c) for c in _light_ts)
        _v = ("QUADBIN TESSELLATE PARITY: PASS (%d rows, %d chips, cells exact + centroid<1e-6)"
              % (len(_light_ts), _nchips)
              if _ts_ok else "QUADBIN TESSELLATE PARITY: FAIL -- chip/cell/centroid mismatch")
    except Exception as _pe:
        _v = "QUADBIN TESSELLATE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN TESSELLATE PARITY: PASS"), _v
    # --- cellunion_agg: per-group decoded-union geometry equality (within 1e-6). ---
    try:
        _adata, _aschema = _cv.generate_quadbin_cellid_arrays(_QB_N_ROWS, res=_QB_AGG_RES)
        _adf = spark.createDataFrame(_adata, schema=_aschema)
        _adf.createOrReplaceTempView("_qb_agg_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_ag
        _gx_ag.register(spark)
        _light_ag = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _qb_agg_parity_v GROUP BY group").collect()}
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_ag
        _hx_ag.register(spark)
        _heavy_ag = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _qb_agg_parity_v GROUP BY group").collect()}
        _ag_ok = (_light_ag.keys() == _heavy_ag.keys() and len(_light_ag) > 0)
        if _ag_ok:
            for _k in _light_ag:
                _lg = _shp_wkb.loads(_light_ag[_k]); _hg = _shp_wkb.loads(_heavy_ag[_k])
                # Decoded-geometry equality within tolerance. shapely union_all and JTS
                # union pick different vertex orders / multipolygon member orders, so
                # equals()/equals_exact() report False on identical coverage. The
                # geometrically meaningful 1e-6 bar is "area of disagreement < tol":
                # symmetric_difference().area collapses to floating-point noise (~1e-13)
                # when the two unions cover the same region.
                if _lg.symmetric_difference(_hg).area >= 1e-6:
                    _ag_ok = False; break
        _v = ("QUADBIN CELLUNION_AGG PARITY: PASS (%d groups, decoded union equality)"
              % len(_light_ag)
              if _ag_ok else "QUADBIN CELLUNION_AGG PARITY: FAIL -- union geometry mismatch")
    except Exception as _pe:
        _v = "QUADBIN CELLUNION_AGG PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN CELLUNION_AGG PARITY: PASS"), _v
if _quadbin_rows:
    _df_qb = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'grid'"
    )
    try:
        display(_df_qb)
    except Exception:
        _df_qb.show(100, truncate=False)
    _md = results.summarize(_quadbin_rows)
    _show_md(f"quadbin grid benchmark -- {RUN_ID}", _md)
"""

_CELL_FANOUT = """# Fan-out UDTF benchmark: light pyrx (streaming UDTF) vs heavy rasterx (generator/array),
# flatten-BOTH parity -- each tier's output is flattened to comparable flat rows, then the
# flat row counts are compared per function (hard gate).
from databricks.labs.gbx.bench import readers as _rd
_fanout_rows = []
_fanout_fns = list(_rd.FANOUT_FUNCTIONS)  # all 8 streaming UDTFs
# For parity: collect (fn, api, flat_output_count) to compare light vs heavy per-fn.
_fanout_counts = {}  # (fn, api) -> flat row count
for _ffn in _fanout_fns:
    if LIGHTWEIGHT:
        _r = _rd.run_fanout_udtf(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                  api="lightweight", fn=_ffn, scale=FANOUT_SCALE,
                                  where="cluster")
        _sink([_r]); lw.append(_r); _fanout_rows.append(_r)
        _fanout_counts[(_ffn, "lightweight")] = _r.rows
    if HEAVYWEIGHT:
        _r = _rd.run_fanout_udtf(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                  api="heavyweight", fn=_ffn, scale=FANOUT_SCALE,
                                  where="cluster")
        _sink([_r]); hw.append(_r); _fanout_rows.append(_r)
        _fanout_counts[(_ffn, "heavyweight")] = _r.rows
# Flatten-both parity check: compare light vs heavy FLAT output row counts per fn.
if LIGHTWEIGHT and HEAVYWEIGHT:
    _parity_ok = True
    _parity_msgs = []
    for _ffn in _fanout_fns:
        _lc = _fanout_counts.get((_ffn, "lightweight"), None)
        _hc = _fanout_counts.get((_ffn, "heavyweight"), None)
        if _lc is None or _hc is None:
            continue
        if _lc == _hc and _lc > 0:
            _parity_msgs.append(f"{_ffn}: PASS (light=heavy={_lc} flat rows)")
        else:
            _parity_ok = False
            _parity_msgs.append(f"{_ffn}: FAIL light={_lc} heavy={_hc}")
    _verdict = "FANOUT PARITY: " + (
        "PASS" if _parity_ok else "FAIL"
    ) + " -- " + "; ".join(_parity_msgs)
    print(_verdict)
    assert _verdict.startswith("FANOUT PARITY: PASS"), _verdict
if _fanout_rows:
    _df_fanout = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'fanout'"
    )
    try:
        display(_df_fanout)
    except Exception:
        _df_fanout.show(100, truncate=False)
    _md = results.summarize(_fanout_rows)
    _show_md(f"fanout UDTF benchmark -- {RUN_ID}", _md)
"""

# Cell-set constants: a "fanout-only" run uses just _CELL_FANOUT.
FANOUT_ONLY = {"fanout"}
BENCHMARK_FANOUT = {"fanout"}

_CELL_VECTOR = """# Vector reader + writer benchmark: light *_gbx vs heavy *_ogr (+ row-count parity)
# Two-leg pipeline (scaled branch):
#   Leg 1 (reader): spark.read.format(fmt).load(copies/) -> Delta ingest table (forces
#     materialization of the full multi-file corpus into one queryable table per format).
#   Leg 2 (writer): read from the shared ~WRITER_ROWS Delta table -> single-file export
#     via write.format(fmt) (no coalesce; the two-phase writer merges fragments on commit).
from databricks.labs.gbx.bench import readers as _rd
if VECTOR_SCALE:
    # Scaled corpus: 1M-row seed per format.  The READ path is the copies/ directory so
    # BOTH tiers enumerate N copies and read them in parallel -- a fair all-format
    # light-vs-heavy comparison.  Shapefile and FileGDB copies are self-contained zips
    # (.shp.zip / .gdb.zip) so the heavy OGR dir-read sees each copy as one file.
    # Writer source is the shared pre-materialized Delta table (WRITER_ROWS polygons).
    _vscale_base = f"{CORPUS}/vector-scale"
    # Fresh Delta state each run: drop the writer-source + per-format ingest tables so each
    # timed ingest is a clean CREATE (not an overwrite of a prior version) and nothing lingers
    # in the catalog between repeat benchmarks. Dropped again at the end (cleanup).
    _bench_tbls = ["geospatial_docs.geobrix.bench_vec_wsrc"] + [
        f"geospatial_docs.geobrix.bench_vec_ingest_{_f}"
        for _f in (
            "geojson_gbx", "geojson_ogr", "shapefile_gbx", "shapefile_ogr",
            "gpkg_gbx", "gpkg_ogr", "file_gdb_gbx", "file_gdb_ogr",
        )
    ]
    for _t in _bench_tbls:
        spark.sql(f"DROP TABLE IF EXISTS {_t}")
    _vcases = [
        # (light_fmt, heavy_fmt, read_path, heavy_options)
        ("geojson_gbx",  "geojson_ogr",  _vscale_base + "/geojson_gbx/copies",   {"multi": "false"}),
        ("shapefile_gbx", "shapefile_ogr", _vscale_base + "/shapefile_gbx/copies", {}),
        ("gpkg_gbx",     "gpkg_ogr",     _vscale_base + "/gpkg_gbx/copies",      {}),
        # FileGDB reads the single seed.gdb.zip for BOTH tiers: the heavy OGR reader opens
        # one FileGDB datasource, not a directory of them. (light *_gbx CAN dir-read a folder
        # of .gdb.zip -- a light-only capability noted in the docs.) Single-archive keeps the
        # light-vs-heavy comparison fair and avoids the heavy dir-read error.
        ("file_gdb_gbx", "file_gdb_ogr", _vscale_base + "/file_gdb_gbx/seed.gdb.zip",  {}),
    ]
    if VECTOR_FORMATS:
        _sel = set(f.strip() for f in VECTOR_FORMATS.split(",") if f.strip())
        _vcases = [c for c in _vcases if c[0] in _sel]
    _do_read = VECTOR_LEGS in ("both", "reader")
    _do_write = VECTOR_LEGS in ("both", "writer")
    _wsrc_tbl = "geospatial_docs.geobrix.bench_vec_wsrc"
    # Materialize the writer-source Delta table once (untimed) so all writer legs share it.
    # Only for writer runs -- reader-only jobs skip the 14M materialization.
    if LIGHTWEIGHT and _do_write:
        from databricks.labs.gbx.bench.corpus_vector import generate_polygon_seed
        generate_polygon_seed(spark, WRITER_ROWS).write.format("delta").mode("overwrite").saveAsTable(_wsrc_tbl)
    _vrows = []
    for _lfmt, _hfmt, _vp, _hopts in _vcases:
        if LIGHTWEIGHT and _do_read:
            _it = f"geospatial_docs.geobrix.bench_vec_ingest_{_lfmt}"
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="lightweight", fmt=_lfmt, ingest_table=_it,
                                     where="cluster")
            _sink([_r]); lw.append(_r); _vrows.append(_r)
        # The heavy OGR FileGDB reader reads native Esri .gdb/.gdb.zip but not the GeoBrix-
        # generated .gdb.zip archive -- skip its heavy leg (FileGDB heavy-read is reported as
        # native-archive-only; light reads the generated corpus, incl. a directory of them).
        _heavy_ok = _hfmt != "file_gdb_ogr"
        if HEAVYWEIGHT and _heavy_ok and _do_read:
            _it = f"geospatial_docs.geobrix.bench_vec_ingest_{_hfmt}"
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="heavyweight", fmt=_hfmt, options=(_hopts or None),
                                     ingest_table=_it, where="cluster")
            _sink([_r]); hw.append(_r); _vrows.append(_r)
        if LIGHTWEIGHT and HEAVYWEIGHT and _heavy_ok and _do_read:
            # Non-fatal parity: a single format's mismatch/failure must NOT abort the whole
            # vector bench -- record it and continue so the other formats + the writer leg run.
            try:
                _lc = spark.read.format(_lfmt).load(_vp).count()
                _hr = spark.read.format(_hfmt)
                for _k, _v in (_hopts or {}).items():
                    _hr = _hr.option(_k, _v)
                _hc = _hr.load(_vp).count()
                print(f"VECTOR PARITY {_lfmt}: light={_lc} heavy={_hc} {'PASS' if _lc==_hc else 'FAIL'}")
            except Exception as _e:  # noqa: BLE001
                print(f"VECTOR PARITY {_lfmt}: ERROR {type(_e).__name__}: {str(_e)[:120]}")
        if LIGHTWEIGHT and _do_write:
            _w = _rd.run_vector_write(spark, _wsrc_tbl, f"{OUT}/vecwrite/{_lfmt}", RUN_ID,
                                      SPARK_WARMUP, SPARK_MEASURED, fmt=_lfmt,
                                      src_is_table=True, where="cluster")
            _sink([_w]); lw.append(_w); _vrows.append(_w)
    # Cleanup: drop the bench Delta tables so nothing lingers in the catalog between runs.
    for _t in _bench_tbls:
        spark.sql(f"DROP TABLE IF EXISTS {_t}")
else:
    _vbase = f"{CORPUS}/vector"
    # (light_fmt, heavy_fmt, read_path, seed_path, heavy_options).
    # The heavy geojson_ogr reader defaults to the GeoJSONSeq driver, so force multi=false
    # to read the standard FeatureCollection corpus file (matching the light geojson_gbx
    # GeoJSON-driver reader) for a fair comparison.
    _vcases = [
        ("geojson_gbx",  "geojson_ogr",  _vbase + "/nyc_boroughs.geojson", _vbase + "/nyc_boroughs.geojson", {"multi": "false"}),
        ("shapefile_gbx", "shapefile_ogr", _vbase + "/nyc_subway.shp.zip",  _vbase + "/nyc_subway.shp.zip",   {}),
        ("gpkg_gbx",     "gpkg_ogr",     _vbase + "/nyc_complete.gpkg",    _vbase + "/nyc_complete.gpkg",    {}),
        ("file_gdb_gbx", "file_gdb_ogr", _vbase + "/NYC_Sample.gdb.zip",   _vbase + "/NYC_Sample.gdb.zip",   {}),
    ]
    _vrows = []
    for _lfmt, _hfmt, _vp, _vseed, _hopts in _vcases:
        if LIGHTWEIGHT:
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="lightweight", fmt=_lfmt, where="cluster")
            _sink([_r]); lw.append(_r); _vrows.append(_r)
        if HEAVYWEIGHT:
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="heavyweight", fmt=_hfmt, options=(_hopts or None),
                                     where="cluster")
            _sink([_r]); hw.append(_r); _vrows.append(_r)
        if LIGHTWEIGHT and HEAVYWEIGHT:
            # Non-fatal parity: a single format's mismatch/failure must NOT abort the whole
            # vector bench -- record it and continue so the other formats + the writer leg run.
            try:
                _lc = spark.read.format(_lfmt).load(_vp).count()
                _hr = spark.read.format(_hfmt)
                for _k, _v in (_hopts or {}).items():
                    _hr = _hr.option(_k, _v)
                _hc = _hr.load(_vp).count()
                print(f"VECTOR PARITY {_lfmt}: light={_lc} heavy={_hc} {'PASS' if _lc==_hc else 'FAIL'}")
            except Exception as _e:  # noqa: BLE001
                print(f"VECTOR PARITY {_lfmt}: ERROR {type(_e).__name__}: {str(_e)[:120]}")
        if LIGHTWEIGHT:
            _w = _rd.run_vector_write(spark, _vseed, f"{OUT}/vecwrite/{_lfmt}", RUN_ID,
                                      SPARK_WARMUP, SPARK_MEASURED, fmt=_lfmt, where="cluster")
            _sink([_w]); lw.append(_w); _vrows.append(_w)
if _vrows:
    _md = results.summarize(_vrows)
    _show_md(f"vector reader+writer benchmark -- {RUN_ID}", _md)
"""

_EPILOGUE = """# Wrap-up: durable jsonl shards + per-tier summaries + heavy-vs-light comparison.
all_rows = lw + hw
if lw:
    results.write_jsonl(lw, f"{OUT}/lightweight.jsonl")
    with open(f"{OUT}/lightweight.summary.md", "w") as fh:
        fh.write(results.summarize(lw, pool_size=len(corpus.row_pool.tiles)))
if hw:
    results.write_jsonl(hw, f"{OUT}/heavyweight.jsonl")
    with open(f"{OUT}/heavyweight.summary.md", "w") as fh:
        fh.write(results.summarize(hw, pool_size=len(corpus.row_pool.tiles)))

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
    _cmp_md = compare.summarize_compare(
        cells, unmatched, hw, lw, pool_size=len(corpus.row_pool.tiles)
    )
    with open(f"{OUT}/summary.md", "w") as fh:
        fh.write(_cmp_md)
    # final render, both tiers ran: show the compare md + link to the summary.md artifact.
    _show_md(f"Heavy vs light comparison -- {RUN_ID}", _cmp_md, path=f"{OUT}/summary.md")
    result["compared"] = len(cells)
    result["summary"] = f"{OUT}/summary.md"
elif lw:
    result["summary"] = f"{OUT}/lightweight.summary.md"
else:
    result["summary"] = f"{OUT}/heavyweight.summary.md"
"""

# The notebook exit MUST be its own trailing cell. dbutils.notebook.exit() ends the run
# immediately, so when it shares a cell with the compare _show_md(), the job-run UI keeps
# only the exit value (the JSON) and DROPS that cell's displayHTML output -- which is why the
# final heavy-vs-light summary never rendered inline (only its path showed in the JSON), while
# the per-section summaries (their own cells) did. Splitting it lets the render cell complete
# and commit its HTML output before this cell exits. `result` persists across cells (shared
# notebook globals).
_EXIT = """# Emit the status-led JSON exit payload (separate cell -- see _EPILOGUE note).
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
    setup = _PREAMBLE.format(
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
        spark_warmup=cfg.get("spark_warmup", 1),
        spark_measured=cfg.get("spark_measured", 1),
        partition_size=int(cfg.get("partition_size", 0)),
        truncate=cfg.get("truncate_results", False),
        truncate_all=cfg.get("truncate_all", False),
        resume=bool(cfg.get("resume")),
        fix_errors=bool(cfg.get("fix_errors", True)),
        redo_functions=str(cfg.get("redo_functions", "") or ""),
        lightweight=bool(cfg.get("lightweight")),
        heavyweight=bool(cfg.get("heavyweight")),
        explain_only=bool(cfg.get("explain_only")),
        benchmark_readers=bool(cfg.get("benchmark_readers")),
        readers_only=bool(cfg.get("readers_only")),
        benchmark_pmtiles=bool(cfg.get("benchmark_pmtiles")),
        pmtiles_only=bool(cfg.get("pmtiles_only")),
        benchmark_vector=bool(cfg.get("benchmark_vector")),
        vector_only=bool(cfg.get("vector_only")),
        vector_scale=bool(cfg.get("vector_scale")),
        writer_rows=int(cfg.get("writer_rows", 14000000)),
        vector_legs=str(cfg.get("vector_legs", "both")),
        vector_formats=str(cfg.get("vector_formats", "") or ""),
        benchmark_mvt=bool(cfg.get("benchmark_mvt")),
        mvt_only=bool(cfg.get("mvt_only")),
        benchmark_vector_tin=bool(cfg.get("benchmark_vector_tin")),
        vector_tin_only=bool(cfg.get("vector_tin_only")),
        benchmark_grid_quadbin=bool(cfg.get("benchmark_grid_quadbin")),
        grid_quadbin_only=bool(cfg.get("grid_quadbin_only")),
        benchmark_fanout=bool(cfg.get("benchmark_fanout")),
        fanout_only=bool(cfg.get("fanout_only")),
        fanout_scale=float(cfg.get("fanout_scale", 1.0)),
    )
    setup += (
        _SINK  # truncate up-front + define the incremental Delta sink + show_section
    )
    light = bool(cfg.get("lightweight"))
    heavy = bool(cfg.get("heavyweight"))
    modes = cfg["modes"]
    do_pure = modes in ("pure-core", "both")
    do_spark = modes in ("spark-path", "both")
    if light:
        setup += _LIGHT_HELPERS  # run_light (references run_pure_core / run_spark_path)
    if heavy:
        setup += _HEAVY_HELPERS  # run_heavy (references HeavyBenchMain)

    benchmark_readers = bool(cfg.get("benchmark_readers"))
    readers_only = bool(cfg.get("readers_only"))
    benchmark_pmtiles = bool(cfg.get("benchmark_pmtiles"))
    pmtiles_only = bool(cfg.get("pmtiles_only"))
    benchmark_vector = bool(cfg.get("benchmark_vector"))
    vector_only = bool(cfg.get("vector_only"))
    benchmark_mvt = bool(cfg.get("benchmark_mvt"))
    mvt_only = bool(cfg.get("mvt_only"))
    benchmark_vector_tin = bool(cfg.get("benchmark_vector_tin"))
    vector_tin_only = bool(cfg.get("vector_tin_only"))
    benchmark_grid_quadbin = bool(cfg.get("benchmark_grid_quadbin"))
    grid_quadbin_only = bool(cfg.get("grid_quadbin_only"))
    benchmark_fanout = bool(cfg.get("benchmark_fanout"))
    fanout_only = bool(cfg.get("fanout_only"))

    # Setup is one cell; then ONE cell per selected (tier x mode) section so each renders
    # its table + summary the moment it finishes; then the wrap-up cell. Order: pure-core
    # (light, heavy) then spark-path (light, heavy).
    cells = [
        # Ensure BOTH fresh geobrix code AND the full [light] dep set every run. The wheel
        # version is a fixed 0.4.0 string, so on a WARM cluster that already has geobrix
        # installed, a bare `pip install '<wheel>[light]'` no-ops: pip sees geobrix==0.4.0
        # satisfied and skips the install ENTIRELY -- including resolving the [light] extra
        # deps. So the cluster can end up running STALE code (e.g. a freshly added DataSource
        # -> DATA_SOURCE_NOT_FOUND) OR missing a [light] dep (e.g. shapely -> ModuleNotFound
        # at `import bench.spec`). The old fix (`--force-reinstall --no-deps`) swapped the
        # code but, by skipping deps, LEFT geobrix present without its extras -> the next
        # warm run's [light] install then no-ops on those deps. Uninstalling first forces the
        # install to actually run: fresh code from the wheel FILE + resolved [light] extras
        # (shapely/rasterio/pyogrio/...). Deps come from pip's cache (fast); only the small
        # geobrix wheel is re-read. `markdown` powers the displayHTML summaries (_show_md).
        _cell("%pip uninstall -y geobrix"),
        _cell(f"%pip install --quiet '{cfg['wheel']}[light]' markdown"),
        _cell("dbutils.library.restartPython()"),
        # Cmd 3 -- the big setup cell (preamble + sink + helpers). Collapsed by default so the
        # run view leads with the per-section result cells, not this wall of setup code.
        _cell(setup, collapsed=True),
    ]
    if (
        not readers_only
        and not pmtiles_only
        and not vector_only
        and not mvt_only
        and not vector_tin_only
        and not grid_quadbin_only
        and not fanout_only
    ):
        if light and do_pure:
            cells.append(_cell(_CELL_LIGHT_PURE))
        if heavy and do_pure:
            cells.append(_cell(_CELL_HEAVY_PURE))
        if light and do_spark:
            cells.append(_cell(_CELL_LIGHT_SPARK))
        if heavy and do_spark:
            cells.append(_cell(_CELL_HEAVY_SPARK))
    if benchmark_readers or readers_only:
        cells.append(_cell(_CELL_READERS))
    if benchmark_pmtiles or pmtiles_only:
        cells.append(_cell(_CELL_PMTILES))
    if benchmark_vector or vector_only:
        cells.append(_cell(_CELL_VECTOR))
    if benchmark_mvt or mvt_only:
        cells.append(_cell(_CELL_MVT))
    if benchmark_vector_tin or vector_tin_only:
        cells.append(_cell(_CELL_VECTOR_TIN))
    if benchmark_grid_quadbin or grid_quadbin_only:
        cells.append(_cell(_CELL_GRID_QUADBIN))
    if benchmark_fanout or fanout_only:
        cells.append(_cell(_CELL_FANOUT))
    cells.append(_cell(_EPILOGUE))
    cells.append(
        _cell(_EXIT)
    )  # exit in its OWN cell so the compare render isn't truncated
    return {"cells": cells, "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
