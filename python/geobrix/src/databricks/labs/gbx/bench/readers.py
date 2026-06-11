"""Reader benchmark mode: time the light raster reader (raster_gbx) per-file.

Pure-local path: open each file with rasterio, split into tiles via pyrx
core tiling, re-encode each tile — measures the end-to-end reader cost on
the local filesystem without Spark overhead.

Spark-path: register the raster_gbx data source and time
spark.read.format("raster_gbx").load(path).count() over a corpus directory.

Cluster format-read: generic spark.read.format(...).load(path).count() wrapper
for comparing light (raster_gbx) vs heavy (gdal) readers on the same cluster.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, List, Optional

from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.runner import capture_env, peak_rss_mb, time_iters


def _read_one_file_light(file_path: str, size_mib: int) -> int:
    """Open a raster file, compute tiles, return tile count."""
    import rasterio

    from databricks.labs.gbx.pyrx.core import tiling as core_tiling

    size_bytes = os.path.getsize(file_path)
    with rasterio.open(file_path) as ds:
        tiles = core_tiling.make_tiles(ds, size_in_mb=size_mib, size_bytes=size_bytes)
        return len(tiles)


def run_pure_local_reader(
    files: List[str],
    run_id: str,
    warmup: int,
    measured: int,
    size_mib: int = 16,
    where: str = "venv",
) -> List[ResultRow]:
    """Time the light raster reader on a list of local file paths.

    One ResultRow is emitted per file. ``iter_median_s`` is the median
    wall-clock over ``measured`` iterations for that single file.
    """
    env = capture_env(where)
    out: List[ResultRow] = []
    for file_path in files:
        try:
            stats = time_iters(
                lambda f=file_path: _read_one_file_light(f, size_mib),
                warmup,
                measured,
            )
            ms = stats["iter_median_ms"]
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn="raster_read",
                    category="reader",
                    mode="pure-core",
                    tile_px=0,
                    bands=0,
                    dtype="",
                    srid=0,
                    rows=1,
                    nodata_frac=0.0,
                    warmup_iters=stats["warmup_iters"],
                    measured_iters=stats["measured_iters"],
                    iter_median_s=ms / 1000.0,
                    iter_min_s=stats["iter_min_ms"] / 1000.0,
                    iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                    iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                    avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                    throughput_mpix_s=0.0,
                    throughput_rows_s=(1.0 / (ms / 1000.0)) if ms else 0.0,
                    peak_rss_mb=peak_rss_mb(),
                    status="ok",
                    note=os.path.basename(file_path),
                    output_fingerprint="",
                    **env,
                )
            )
        except Exception as e:  # noqa: BLE001
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn="raster_read",
                    category="reader",
                    mode="pure-core",
                    tile_px=0,
                    bands=0,
                    dtype="",
                    srid=0,
                    rows=1,
                    nodata_frac=0.0,
                    warmup_iters=warmup,
                    measured_iters=0,
                    iter_median_s=0.0,
                    iter_min_s=0.0,
                    iter_p90_s=0.0,
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


def run_spark_path_reader(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    size_mib: int = 16,
    where: str = "venv",
) -> List[ResultRow]:
    """Time the raster_gbx Spark data source over a corpus directory.

    Registers the light DS, then times
    ``spark.read.format("raster_gbx").option("sizeInMB", ...).load(path).count()``.
    One ResultRow is emitted covering the whole directory.
    """
    from databricks.labs.gbx.pyrx.ds.register import register

    register(spark)
    env = capture_env(where)

    def _job():
        return (
            spark.read.format("raster_gbx")
            .option("sizeInMB", str(size_mib))
            .load(path)
            .count()
        )

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        # Count the actual row count from one call so we can record it.
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        out = [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=int(actual_rows),
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(
                    (ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0
                ),
                per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(
                    (actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0
                ),
                peak_rss_mb=peak_rss_mb(),
                status="ok",
                note=os.path.basename(path.rstrip("/\\")),
                output_fingerprint="",
                **env,
            )
        ]
    except Exception as e:  # noqa: BLE001
        out = [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="error",
                note=str(e)[:300],
                output_fingerprint="",
                **env,
            )
        ]
    return out


def run_format_read(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    fmt: str,
    options: Optional[Dict[str, str]] = None,
    where: str = "venv",
    size_mib: int = 16,
) -> "ResultRow":
    """Time spark.read.format(fmt).load(path).count() on-cluster.

    For fmt=="raster_gbx": registers the light data source first.
    For fmt=="gdal": ensures the heavyweight GDAL driver is initialised.
    Returns a single ResultRow (mode="spark-path", category="reader").
    """
    env = capture_env(where)

    if fmt == "raster_gbx":
        from databricks.labs.gbx.pyrx.ds.register import register

        register(spark)
    elif fmt == "gdal":
        try:
            from databricks.labs.gbx.rasterx import functions as _rx

            _rx.register(spark)
        except Exception:  # noqa: BLE001
            pass  # best-effort GDAL init; failure surfaces in the timed call

    def _job():
        reader = spark.read.format(fmt)
        if options:
            for k, v in options.items():
                reader = reader.option(k, str(v))
        if fmt == "raster_gbx":
            reader = reader.option("sizeInMB", str(size_mib))
        return reader.load(path).count()

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        actual_rows = int(actual_rows)
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="raster_read",
            category="reader",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=actual_rows,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0,
            per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(
                (actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0
            ),
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{fmt} over {os.path.basename(path.rstrip('/\\'))}",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="raster_read",
            category="reader",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[:200],
            output_fingerprint="",
            **env,
        )


def run_format_write(
    spark,
    input_path: str,
    out_path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    write_api: str,
    read_fmt: str = "raster_gbx",
    write_fmt: str = "gtiff_gbx",
    mode: str = "overwrite",
    options: Optional[Dict[str, str]] = None,
    where: str = "venv",
) -> "ResultRow":
    """Time spark.write.format(write_fmt).save(out_path) on a pre-read input DataFrame.

    ``mode`` is the Spark write mode: the light gtiff_gbx writer supports
    "overwrite"; the heavy gtiff_gdal writer is append-only ("overwrite" raises
    UNSUPPORTED_FEATURE truncate), so pass mode="append" for it.

    Reads the input directory once via ``read_fmt`` (same reader for both tiers so
    write cost is isolated), caches it, then times repeated ``write.format(write_fmt)``
    calls. Returns a single ResultRow (mode="spark-path", category="writer").
    """
    env = capture_env(where)

    # Register light DS (always needed for raster_gbx reader/writer).
    from databricks.labs.gbx.pyrx.ds.register import register

    register(spark)

    # Best-effort heavy init when either format is a heavyweight GDAL format.
    _heavy_fmts = {"gdal", "gtiff_gdal"}
    if read_fmt in _heavy_fmts or write_fmt in _heavy_fmts:
        try:
            from databricks.labs.gbx.rasterx import functions as _rx

            _rx.register(spark)
        except Exception:  # noqa: BLE001
            pass

    # Read the input once and cache — isolates write cost from read cost.
    try:
        reader = spark.read.format(read_fmt)
        if options:
            for k, v in options.items():
                reader = reader.option(k, str(v))
        df = reader.load(input_path)
        df = df.cache()
        n = int(df.count())
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=write_api,
            fn="raster_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[:200],
            output_fingerprint="",
            **env,
        )

    def _job():
        w = df.write.format(write_fmt).mode(mode)
        if options:
            for k, v in options.items():
                w = w.option(k, str(v))
        w.save(out_path)

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api=write_api,
            fn="raster_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
            per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{write_fmt} write of {n} tiles",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=write_api,
            fn="raster_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[:200],
            output_fingerprint="",
            **env,
        )


def _list_tifs(corpus_dir: str) -> List[str]:
    """Return all *.tif / *.tiff paths under corpus_dir."""
    import glob

    tifs = sorted(glob.glob(os.path.join(corpus_dir, "**", "*.tif"), recursive=True))
    tifs += sorted(glob.glob(os.path.join(corpus_dir, "**", "*.tiff"), recursive=True))
    return tifs


def _print_summary(rows: List[ResultRow]) -> None:
    """Print a compact results table to stdout."""
    if not rows:
        print("(no results)")
        return
    print(
        f"\n{'file/note':<40} {'mode':<12} {'status':<8} {'median_s':>10} {'rows':>8}"
    )
    print("-" * 82)
    for r in rows:
        print(
            f"{r.note:<40} {r.mode:<12} {r.status:<8} "
            f"{r.iter_median_s:>10.4f} {r.rows:>8}"
        )
    ok = [r for r in rows if r.status == "ok"]
    if ok:
        import statistics

        med = statistics.median(r.iter_median_s for r in ok)
        print(f"\nMedian iter_median_s across {len(ok)} file(s): {med:.4f} s")


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(prog="databricks.labs.gbx.bench.readers")
    ap.add_argument(
        "--mode",
        default="pure-local",
        choices=["pure-local", "spark-path", "both"],
        help="Benchmark mode (default: pure-local)",
    )
    ap.add_argument(
        "--corpus",
        required=True,
        help="Directory containing *.tif files to benchmark",
    )
    ap.add_argument("--run-id", default="local", help="Run ID label (default: local)")
    ap.add_argument(
        "--warmup", type=int, default=1, help="Warmup iterations (default: 1)"
    )
    ap.add_argument(
        "--measured", type=int, default=3, help="Measured iterations (default: 3)"
    )
    ap.add_argument(
        "--size-mib", type=int, default=16, help="Tile size budget in MiB (default: 16)"
    )
    ap.add_argument(
        "--out",
        default="",
        help="Output JSONL path (default: print summary only)",
    )
    ap.add_argument("--where", default="venv", help="env_where label (default: venv)")
    a = ap.parse_args(argv)

    rows: List[ResultRow] = []

    if a.mode in ("pure-local", "both"):
        files = _list_tifs(a.corpus)
        if not files:
            print(f"WARNING: no .tif/.tiff files found under {a.corpus}", flush=True)
        else:
            print(f"pure-local: {len(files)} file(s)", flush=True)
            rows += run_pure_local_reader(
                files=files,
                run_id=a.run_id,
                warmup=a.warmup,
                measured=a.measured,
                size_mib=a.size_mib,
                where=a.where,
            )

    if a.mode in ("spark-path", "both"):
        import sys

        from pyspark.sql import SparkSession

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("bench-readers")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        print(f"spark-path: corpus={a.corpus}", flush=True)
        rows += run_spark_path_reader(
            spark=spark,
            path=a.corpus,
            run_id=a.run_id,
            warmup=a.warmup,
            measured=a.measured,
            size_mib=a.size_mib,
            where=a.where,
        )

    _print_summary(rows)

    if a.out:
        from databricks.labs.gbx.bench.results import write_jsonl

        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        write_jsonl(rows, a.out)
        print(f"wrote {len(rows)} rows -> {a.out}")


if __name__ == "__main__":
    main()
