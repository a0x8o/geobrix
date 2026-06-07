import time

import pytest

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import runner as rn
from databricks.labs.gbx.bench import spec as s


def test_time_iters_returns_distribution():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        time.sleep(0.001)

    stats = rn.time_iters(fn, warmup=2, measured=5)
    assert calls["n"] == 7  # warmup + measured
    assert stats["measured_iters"] == 5
    assert stats["median_ms"] >= 0.5
    assert stats["min_ms"] <= stats["median_ms"] <= stats["p90_ms"] + 1e-6


def test_capture_env_has_required_fields():
    env = rn.capture_env(where="venv")
    for k in (
        "env_arch",
        "env_os",
        "env_cpu_count",
        "env_gdal_version",
        "env_gbx_version",
        "env_where",
    ):
        assert k in env
    assert env["env_where"] == "venv"


def test_run_pure_core_produces_ok_rows(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=9,
        tile_px=[32, 64],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width", "rst_avg"])
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        warmup=1,
        measured=2,
        where="venv",
    )
    assert rows, "expected result rows"
    assert all(r.status == "ok" for r in rows)
    assert {r.fn for r in rows} == {"rst_width", "rst_avg"}
    assert all(r.mode == "pure-core" and r.rows == 1 for r in rows)
    # consistency fingerprint captured for every pure-core row
    assert all(r.output_fingerprint for r in rows)
    # one row per (fn x size_sweep tile)
    assert len(rows) == 2 * len(corpus.size_sweep)


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    from pyspark.sql import SparkSession

    # Pin worker + driver Python to this interpreter so local executors match the
    # driver minor version (otherwise PYTHON_VERSION_MISMATCH on Python UDFs).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    sess = (
        SparkSession.builder.master("local[2]")
        .appName("bench-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield sess


def test_run_spark_path_produces_ok_rows(tmp_path, spark):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=4,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=6,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_width"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        row_counts=[2, 4],
        warmup=1,
        measured=2,
        where="venv",
    )
    assert rows and all(r.status == "ok" for r in rows)
    assert all(r.mode == "spark-path" and r.fn == "rst_width" for r in rows)
    assert sorted({r.rows for r in rows}) == [2, 4]


def test_runner_main_writes_shard(tmp_path):
    dg.generate_corpus(
        out_dir=tmp_path,
        seed=2,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    out = tmp_path / "lw.jsonl"
    rn.main(
        [
            "--corpus",
            str(tmp_path),
            "--out",
            str(out),
            "--functions",
            "rst_width",
            "--mode",
            "pure-core",
            "--row-counts",
            "2,4",
            "--warmup",
            "1",
            "--measured",
            "2",
            "--run-id",
            "cli",
        ]
    )
    from databricks.labs.gbx.bench import results as r

    rows = r.read_jsonl(out)
    assert rows and all(x.fn == "rst_width" for x in rows)


def test_spark_path_runs_a_warmup_before_timing(tmp_path, spark, monkeypatch):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=5,
        tile_px=[32],
        bands=[2],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
    )
    calls = {"noop_saves": 0}
    import pyspark.sql.readwriter

    orig_save = pyspark.sql.readwriter.DataFrameWriter.save

    def counting_save(self, *a, **k):
        calls["noop_saves"] += 1
        return orig_save(self, *a, **k)

    monkeypatch.setattr(pyspark.sql.readwriter.DataFrameWriter, "save", counting_save)
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=s.select(functions=["rst_width"]),
        run_id="t",
        row_counts=[2],
        warmup=1,
        measured=1,
        where="venv",
    )
    # 1 warm-up + (warmup 1 + measured 1) timed saves for the single (fn,rows) cell = 3
    assert calls["noop_saves"] >= 3
    assert rows and all(r.status == "ok" for r in rows)


def test_pure_core_emits_na_by_design_for_low_band_count(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=3,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=1,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_ndvi"])  # band-math needs 2 bands
    rows = rn.run_pure_core(
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="t",
        warmup=1,
        measured=1,
        where="venv",
    )
    assert rows and all(r.status == "na_by_design" for r in rows)
    assert all("band" in r.note.lower() for r in rows)
