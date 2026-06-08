"""Python benchmark runner: pure-core and spark-path timing over a corpus."""

from __future__ import annotations

import platform
import statistics
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Callable, Dict, List

from databricks.labs.gbx.bench import manifest as m
from databricks.labs.gbx.bench import spec as _spec
from databricks.labs.gbx.bench import synth as _synth
from databricks.labs.gbx.bench.fingerprint import (
    fingerprint_collection,
    fingerprint_dggs_grid,
    fingerprint_output,
    fingerprint_vector,
)
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.spec import FnSpec
from databricks.labs.gbx.pyrx import _serde


def _synthesized_paths(corpus_root, tile_rel_path: str, fn: str) -> List[str]:
    """Synthesize (write-once) the multi-tile input for a `tile_array` fn.

    Both engines compute the SAME output dir via ``synth.synth_dir`` (from corpus
    root + tile path + fn) and read the identical files. The pyrx runner writes
    them here; the heavy runner reads the same paths. Idempotent: re-runs reuse the
    existing files. Returns the synthesized GTiff paths in consumption order.
    """
    src = str(Path(corpus_root) / tile_rel_path)
    out_dir = _synth.synth_dir(corpus_root, tile_rel_path, _spec.synth_recipe(fn))
    return _synth.synthesize(src, _spec.synth_recipe(fn), out_dir)


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


def _open_ds_list(paths):
    """Open synthesized tile paths into a list of datasets under one ExitStack."""
    stack = ExitStack()
    dss = [stack.enter_context(_serde.open_tile(Path(p).read_bytes())) for p in paths]
    return stack, dss


def _capture_and_call(fs, input_kind, raster, tile_path, synth_paths):
    """Build (fingerprint, call) for a pure-core function via its input_kind adapter.

    The fingerprint is the untimed output summary (empty for timing-only fns); the
    `call` closure runs the core_fn once per timed iteration. Both branch on the
    same input_kind: "bytes" (raw raster bytes), "path" (corpus file path),
    "tile_array" (a list of open datasets from the synthesized multi-tile input),
    or "tile" (the default: a single opened DatasetReader).
    """
    if input_kind == "bytes":
        feed = lambda: raster  # noqa: E731
        call = lambda _b=raster, _fs=fs: _fs.core_fn(_b, _fs.args)  # noqa: E731
    elif input_kind == "path":
        feed = lambda: tile_path  # noqa: E731
        call = lambda _p=tile_path, _fs=fs: _fs.core_fn(_p, _fs.args)  # noqa: E731
    elif input_kind == "tile_array":

        def _run_array(_fs=fs, _paths=synth_paths):
            stack, dss = _open_ds_list(_paths)
            try:
                return _fs.core_fn(dss, _fs.args)
            finally:
                stack.close()

        feed = None
        call = _run_array
    else:

        def _run_tile(_fs=fs, _b=raster):
            with _serde.open_tile(_b) as ds:
                return _fs.core_fn(ds, _fs.args)

        feed = None
        call = _run_tile

    if not getattr(fs, "fingerprint", True):
        return "", call
    # bytes/path feed a value; tile/tile_array reuse the timed `call` (which
    # returns the output) for the one untimed fingerprint pass.
    out = fs.core_fn(feed(), fs.args) if feed is not None else call()
    return _fingerprint_for(fs, out), call


def _fingerprint_for(fs, out):
    """Fingerprint a core_fn output, honoring the FnSpec's fingerprint_kind.

    Most functions use ``"auto"`` -- ``fingerprint_output`` inspects the value
    (bytes -> raster, list-of-bytes -> raster_collection, list-of-scalars ->
    scalar_list, else scalar). Functions whose output shape the auto-detector
    cannot classify declare an explicit kind: ``"dggs_grid"`` (a per-band list of
    cell records the auto path would mis-read as a scalar_list), ``"vector"``, or
    ``"collection"``.
    """
    kind = getattr(fs, "fingerprint_kind", "auto")
    if kind == "dggs_grid":
        return fingerprint_dggs_grid(out)
    if kind == "vector":
        return fingerprint_vector(out)
    if kind == "collection":
        return fingerprint_collection(out)
    return fingerprint_output(out)


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
            if te.bands < getattr(fs, "min_bands", 1):
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
                        status="na_by_design",
                        note=f"requires >= {getattr(fs, 'min_bands', 1)} bands",
                        output_fingerprint="",
                        **env,
                    )
                )
                continue
            raster = (root / te.path).read_bytes()
            tile_path = str(root / te.path)
            # The `input_kind` adapter decides what the core_fn is fed:
            #   "bytes": the raw raster bytes (reader/constructor fns whose input
            #            is content, not an open ds -- rst_tryopen/fromcontent).
            #   "path":  the corpus tile's file path (rst_fromfile).
            #   "tile_array": a LIST of open datasets, synthesized from the corpus
            #            tile (write-once-read-both) -- the multi-tile fns
            #            (rst_frombands/combineavg/merge).
            #   "tile"  (default): an opened rasterio DatasetReader -- every
            #            pre-existing function takes this path unchanged.
            input_kind = getattr(fs, "input_kind", "tile")
            # For tile_array, synthesize the multi-tile input ONCE (idempotent) and
            # reuse the same files for the fingerprint pass and every timed call, so
            # the heavy runner reads byte-identical inputs from the same paths.
            synth_paths = (
                _synthesized_paths(root, te.path, fs.name)
                if input_kind == "tile_array"
                else None
            )

            try:
                # Capture the untimed output fingerprint (consistency) and build the
                # timed `call` closure, both keyed off the input_kind adapter.
                fingerprint, call = _capture_and_call(
                    fs, input_kind, raster, tile_path, synth_paths
                )
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

    # tile_array adapter (spark-path): the multi-tile fns (rst_frombands/
    # combineavg/merge) consume an ARRAY<tile> column, not a single tile. Build a
    # CONSTANT array literal from the SAME synthesized files the pure-core path
    # writes (write-once-read-both), broadcast across every row. This times the
    # array serialization + UDF overhead, which is the point of the spark-path
    # measurement. The first row_pool tile is the representative source.
    _array_root = pool.tiles[0].path if pool.tiles else None

    def _synth_array_col(fn: str):
        """ARRAY<tile> literal column of the synthesized tiles for a tile_array fn."""
        paths = _synthesized_paths(root, _array_root, fn)
        elems = []
        for p in paths:
            d = _serde.build_tile(Path(p).read_bytes(), "GTiff", 0)
            elems.append(
                F.struct(
                    F.lit(d["cellid"]).cast("long").alias("cellid"),
                    F.lit(d["raster"]).alias("raster"),
                    F.create_map(
                        *[
                            x
                            for k, v in d["metadata"].items()
                            for x in (F.lit(k), F.lit(v))
                        ]
                    ).alias("metadata"),
                )
            )
        return F.array(*elems)

    def _input_col(fn: str, kind: str):
        """The column fed to col_fn: an array literal for tile_array, else the tile."""
        return _synth_array_col(fn) if kind == "tile_array" else df_all["tile"]

    # Spark warm-up: one throwaway job so JVM/Spark spin-up isn't charged to the first
    # timed cell. Band-aware (mirrors the Scala HeavyRunner warm-up) + guarded so a
    # warm-up failure can never abort timing.
    _spark_fns = [f for f in fnspecs if "spark-path" in f.modes]
    _warm = next(
        (f for f in _spark_fns if getattr(f, "min_bands", 1) <= pool.bands), None
    )
    if _warm is None and _spark_fns:
        _warm = _spark_fns[0]
    if _warm is not None:
        try:
            _wc = _warm.col_fn(
                _input_col(_warm.name, getattr(_warm, "input_kind", "tile")),
                _warm.args,
            )
            df_all.limit(1).select(_wc.alias("warmup")).write.format("noop").mode(
                "overwrite"
            ).save()
        except Exception:  # noqa: BLE001 — warm-up failures must never abort timing
            pass

    out: List[ResultRow] = []
    for fs in fnspecs:
        if "spark-path" not in fs.modes:
            continue
        _kind = getattr(fs, "input_kind", "tile")
        for n in sorted(row_counts):
            df = df_all.limit(n)
            try:

                def job(_df=df, _fs=fs, _k=_kind):
                    c = _fs.col_fn(_input_col(_fs.name, _k), _fs.args)
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
    ap.add_argument("--set", default="core", choices=["core", "full"])
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
        set=getattr(a, "set"),
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
