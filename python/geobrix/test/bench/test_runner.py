import time

import pytest

from databricks.labs.gbx.bench import runner as rn
from databricks.labs.gbx.bench import datagen as dg, manifest as m, spec as s


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
    for k in ("env_arch", "env_os", "env_cpu_count", "env_gdal_version",
              "env_gbx_version", "env_where"):
        assert k in env
    assert env["env_where"] == "venv"


def test_run_pure_core_produces_ok_rows(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path, seed=9, tile_px=[32, 64], bands=[2], dtypes=["float32"],
        srids=[4326], nodata_fracs=[0.0], row_rows=2, row_tile_px=32,
        row_bands=2, row_dtype="float32",
    )
    fns = s.select(functions=["rst_width", "rst_avg"])
    rows = rn.run_pure_core(
        corpus_root=tmp_path, corpus=corpus, fnspecs=fns,
        run_id="t", warmup=1, measured=2, where="venv",
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
    sess = (SparkSession.builder.master("local[2]").appName("bench-tests")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
    yield sess


def test_run_spark_path_produces_ok_rows(tmp_path, spark):
    corpus = dg.generate_corpus(
        out_dir=tmp_path, seed=4, tile_px=[32], bands=[2], dtypes=["float32"],
        srids=[4326], nodata_fracs=[0.0], row_rows=6, row_tile_px=32,
        row_bands=2, row_dtype="float32",
    )
    fns = s.select(functions=["rst_width"])
    rows = rn.run_spark_path(
        spark=spark, corpus_root=tmp_path, corpus=corpus, fnspecs=fns,
        run_id="t", row_counts=[2, 4], warmup=1, measured=2, where="venv",
    )
    assert rows and all(r.status == "ok" for r in rows)
    assert all(r.mode == "spark-path" and r.fn == "rst_width" for r in rows)
    assert sorted({r.rows for r in rows}) == [2, 4]
