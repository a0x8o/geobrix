import pytest

from databricks.labs.gbx.bench import cluster as cl
from databricks.labs.gbx.bench import results as R


def _row(**kw):
    base = dict(
        run_id="r",
        api="lightweight",
        fn="rst_width",
        category="accessor",
        mode="pure-core",
        tile_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.0,
        warmup_iters=1,
        measured_iters=2,
        median_ms=1.0,
        min_ms=1.0,
        p90_ms=1.0,
        throughput_mpix_s=1.0,
        throughput_rows_s=1.0,
        peak_rss_mb=0.0,
        status="ok",
        note="",
        env_arch="x",
        env_cpu_model="x",
        env_cpu_count=1,
        env_os="x",
        env_gbx_version="0.4.0",
        env_gdal_version="3.12.1",
        env_runtime_version="x",
        env_where="venv",
        output_fingerprint="",
    )
    base.update(kw)
    return R.ResultRow(**base)


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    s = SparkSession.builder.master("local[2]").appName("cluster-test").getOrCreate()
    yield s


def test_rows_to_dataframe_schema_and_where(spark):
    df = cl.rows_to_dataframe([_row(), _row(fn="rst_avg")], spark, where="cluster")
    assert len(df.columns) == 30
    assert "output_fingerprint" in df.columns
    vals = {r["fn"]: r["env_where"] for r in df.collect()}
    assert vals == {"rst_width": "cluster", "rst_avg": "cluster"}


def test_build_bench_notebook_cells():
    cfg = dict(
        wheel="/Volumes/c/s/v/geobrix-0.4.0-py3-none-any.whl",
        corpus="/Volumes/c/s/v/bench-corpus",
        out_dir="/Volumes/c/s/v/bench-out/run1",
        table="main.default.bench_results",
        run_id="run1",
        functions="rst_width,rst_slope",
        modes="both",
        row_counts="10,100",
        warmup=2,
        measured=5,
        heavyweight=True,
        lightweight=True,
    )
    nb = cl.build_bench_notebook(cfg)
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "geobrix-0.4.0-py3-none-any.whl[pyrx]" in src
    assert "restartPython" in src
    assert "HeavyBenchMain" in src and "_jvm" in src
    assert "run_spark_path" in src or "run_pure_core" in src
    assert "bench_results" in src
    assert "dbutils.notebook.exit" in src
    assert nb["nbformat"] == 4


def test_build_bench_notebook_lightweight_only_omits_heavyweight():
    cfg = dict(
        wheel="w.whl",
        corpus="c",
        out_dir="o",
        table="t",
        run_id="r",
        functions="",
        modes="both",
        row_counts="10",
        warmup=1,
        measured=1,
        heavyweight=False,
        lightweight=True,
    )
    nb = cl.build_bench_notebook(cfg)
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "HeavyBenchMain" not in src  # heavyweight leg genuinely absent
    assert (
        "run_pure_core" in src or "run_spark_path" in src
    )  # lightweight still present


def test_build_bench_notebook_heavyweight_only_omits_lightweight():
    cfg = dict(
        wheel="w.whl",
        corpus="c",
        out_dir="o",
        table="t",
        run_id="r",
        functions="",
        modes="pure-core",
        row_counts="10",
        warmup=1,
        measured=1,
        heavyweight=True,
        lightweight=False,
    )
    nb = cl.build_bench_notebook(cfg)
    src = "\n".join("".join(c.get("source", [])) for c in nb["cells"])
    assert "HeavyBenchMain" in src
    assert "run_pure_core" not in src and "run_spark_path" not in src
