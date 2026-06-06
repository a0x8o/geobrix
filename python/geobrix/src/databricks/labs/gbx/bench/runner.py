"""Python benchmark runner: pure-core and spark-path timing over a corpus."""

from __future__ import annotations

import platform
import statistics
import time
from pathlib import Path
from typing import Callable, Dict, List

from databricks.labs.gbx.bench import manifest as m
from databricks.labs.gbx.bench.fingerprint import fingerprint_output
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.spec import FnSpec
from databricks.labs.gbx.pyrx import _serde


def time_iters(fn: Callable[[], None], warmup: int, measured: int) -> Dict:
    """Run fn warmup+measured times; return ms distribution over measured runs."""
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(measured):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    samples.sort()
    p90_idx = max(0, min(len(samples) - 1, int(round(0.9 * (len(samples) - 1)))))
    return {
        "warmup_iters": warmup,
        "measured_iters": measured,
        "median_ms": statistics.median(samples),
        "min_ms": samples[0],
        "p90_ms": samples[p90_idx],
    }


def capture_env(where: str) -> Dict:
    try:
        import rasterio

        gdal_version = rasterio.__gdal_version__
    except Exception:  # noqa: BLE001
        gdal_version = "unknown"
    try:
        from importlib.metadata import version as _pkg_version

        gbx_version = _pkg_version("geobrix")
    except Exception:  # noqa: BLE001
        gbx_version = "unknown"
    import os

    return {
        "env_arch": platform.machine(),
        "env_cpu_model": platform.processor() or "unknown",
        "env_cpu_count": os.cpu_count() or 0,
        "env_os": platform.system(),
        "env_gbx_version": str(gbx_version),
        "env_gdal_version": str(gdal_version),
        "env_runtime_version": "py" + platform.python_version(),
        "env_where": where,
    }


def peak_rss_mb() -> float:
    try:
        import resource

        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes; Linux reports kilobytes.
        return (kb / (1024 * 1024)) if platform.system() == "Darwin" else (kb / 1024)
    except Exception:  # noqa: BLE001
        return 0.0


def _mpix(tile_px: int, bands: int, rows: int) -> float:
    return (tile_px * tile_px * bands * rows) / 1e6


def run_pure_core(
    corpus_root,
    corpus: m.Corpus,
    fnspecs: List[FnSpec],
    run_id: str,
    warmup: int,
    measured: int,
    where: str,
) -> List[ResultRow]:
    root = Path(corpus_root)
    env = capture_env(where)
    out: List[ResultRow] = []
    for fs in fnspecs:
        if "pure-core" not in fs.modes:
            continue
        for te in corpus.size_sweep:
            raster = (root / te.path).read_bytes()
            try:
                # Untimed: capture the actual output once for consistency fingerprinting.
                with _serde.open_tile(raster) as ds:
                    _out = fs.core_fn(ds, fs.args)
                fingerprint = fingerprint_output(_out)

                def call(_b=raster, _fs=fs):
                    with _serde.open_tile(_b) as ds:
                        _fs.core_fn(ds, _fs.args)

                stats = time_iters(call, warmup, measured)
                ms = stats["median_ms"]
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="pure-core",
                        tile_px=te.tile_px,
                        bands=te.bands,
                        dtype=te.dtype,
                        srid=te.srid,
                        rows=1,
                        nodata_frac=te.nodata_frac,
                        warmup_iters=stats["warmup_iters"],
                        measured_iters=stats["measured_iters"],
                        median_ms=ms,
                        min_ms=stats["min_ms"],
                        p90_ms=stats["p90_ms"],
                        throughput_mpix_s=(
                            (_mpix(te.tile_px, te.bands, 1) / (ms / 1000.0))
                            if ms
                            else 0.0
                        ),
                        throughput_rows_s=(1.0 / (ms / 1000.0)) if ms else 0.0,
                        peak_rss_mb=peak_rss_mb(),
                        status="ok",
                        note="",
                        output_fingerprint=fingerprint,
                        **env,
                    )
                )
            except Exception as e:  # noqa: BLE001
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="pure-core",
                        tile_px=te.tile_px,
                        bands=te.bands,
                        dtype=te.dtype,
                        srid=te.srid,
                        rows=1,
                        nodata_frac=te.nodata_frac,
                        warmup_iters=warmup,
                        measured_iters=0,
                        median_ms=0.0,
                        min_ms=0.0,
                        p90_ms=0.0,
                        throughput_mpix_s=0.0,
                        throughput_rows_s=0.0,
                        peak_rss_mb=0.0,
                        status="error",
                        note=str(e)[:300],
                        output_fingerprint="",
                        **env,
                    )
                )
    return out


def run_spark_path(
    spark,
    corpus_root,
    corpus: m.Corpus,
    fnspecs: List[FnSpec],
    run_id: str,
    row_counts: List[int],
    warmup: int,
    measured: int,
    where: str,
) -> List[ResultRow]:
    """Time each fn as a Spark Column over N tile rows (serialization + UDF overhead)."""
    from pyspark.sql import functions as F

    root = Path(corpus_root)
    env = capture_env(where)
    pool = corpus.row_pool

    # Build the tile DataFrame once at the max row count; subselect with limit(n).
    max_rows = max(row_counts)
    tiles = pool.tiles[:max_rows]
    payload = []
    for te in tiles:
        d = _serde.build_tile((root / te.path).read_bytes(), "GTiff", te.cellid)
        payload.append((d["cellid"], d["raster"], d["metadata"]))
    base = spark.createDataFrame(payload, schema=_serde.TILE_SCHEMA)
    # Wrap the 3 columns into the tile struct the prx.rst_* wrappers expect.
    df_all = base.select(F.struct("cellid", "raster", "metadata").alias("tile")).cache()
    df_all.count()  # materialize the cache so it isn't part of timing

    out: List[ResultRow] = []
    for fs in fnspecs:
        if "spark-path" not in fs.modes:
            continue
        for n in sorted(row_counts):
            df = df_all.limit(n)
            try:

                def job(_df=df, _fs=fs):
                    c = _fs.col_fn(_df["tile"], _fs.args)
                    _df.select(c.alias("out")).write.format("noop").mode(
                        "overwrite"
                    ).save()

                stats = time_iters(job, warmup, measured)
                ms = stats["median_ms"]
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="spark-path",
                        tile_px=pool.tile_px,
                        bands=pool.bands,
                        dtype=pool.dtype,
                        srid=0,
                        rows=n,
                        nodata_frac=0.0,
                        warmup_iters=stats["warmup_iters"],
                        measured_iters=stats["measured_iters"],
                        median_ms=ms,
                        min_ms=stats["min_ms"],
                        p90_ms=stats["p90_ms"],
                        throughput_mpix_s=(
                            (_mpix(pool.tile_px, pool.bands, n) / (ms / 1000.0))
                            if ms
                            else 0.0
                        ),
                        throughput_rows_s=(n / (ms / 1000.0)) if ms else 0.0,
                        peak_rss_mb=peak_rss_mb(),
                        status="ok",
                        note="",
                        output_fingerprint="",
                        **env,
                    )
                )
            except Exception as e:  # noqa: BLE001
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="spark-path",
                        tile_px=pool.tile_px,
                        bands=pool.bands,
                        dtype=pool.dtype,
                        srid=0,
                        rows=n,
                        nodata_frac=0.0,
                        warmup_iters=warmup,
                        measured_iters=0,
                        median_ms=0.0,
                        min_ms=0.0,
                        p90_ms=0.0,
                        throughput_mpix_s=0.0,
                        throughput_rows_s=0.0,
                        peak_rss_mb=0.0,
                        status="error",
                        note=str(e)[:300],
                        output_fingerprint="",
                        **env,
                    )
                )
    df_all.unpersist()
    return out


def main(argv=None):
    import argparse

    from databricks.labs.gbx.bench import manifest as _m
    from databricks.labs.gbx.bench import results as _r
    from databricks.labs.gbx.bench import spec as _s

    ap = argparse.ArgumentParser(prog="bench.runner")
    ap.add_argument("--corpus", required=True, help="corpus root dir (has corpus.json)")
    ap.add_argument("--out", required=True, help="output JSONL shard")
    ap.add_argument("--functions", default="")
    ap.add_argument("--categories", default="")
    ap.add_argument(
        "--mode", default="both", choices=["pure-core", "spark-path", "both"]
    )
    ap.add_argument("--row-counts", default="10,100,1000,10000")
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--measured", type=int, default=5)
    ap.add_argument("--run-id", default="local")
    ap.add_argument("--where", default="venv")
    a = ap.parse_args(argv)

    corpus = _m.Corpus.read(Path(a.corpus) / "corpus.json")
    fnspecs = _s.select(
        functions=[x for x in a.functions.split(",") if x] or None,
        categories=[x for x in a.categories.split(",") if x] or None,
    )
    row_counts = [int(x) for x in a.row_counts.split(",") if x]
    rows: List[ResultRow] = []
    if a.mode in ("pure-core", "both"):
        rows += run_pure_core(
            a.corpus, corpus, fnspecs, a.run_id, a.warmup, a.measured, a.where
        )
    if a.mode in ("spark-path", "both"):
        import os
        import sys

        from pyspark.sql import SparkSession

        # Pin Spark workers to this interpreter (avoids PYTHON_VERSION_MISMATCH when
        # local executors would otherwise pick up a different system python).
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("bench-runner")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        rows += run_spark_path(
            spark,
            a.corpus,
            corpus,
            fnspecs,
            a.run_id,
            row_counts,
            a.warmup,
            a.measured,
            a.where,
        )
    _r.write_jsonl(rows, a.out)
    print(f"wrote {len(rows)} rows -> {a.out}")
    summary_path = (
        a.out[:-6] + ".summary.md"
        if a.out.endswith(".jsonl")
        else a.out + ".summary.md"
    )
    Path(summary_path).write_text(_r.summarize(rows))
    print(f"summary -> {summary_path}")


if __name__ == "__main__":
    main()
