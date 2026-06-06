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
