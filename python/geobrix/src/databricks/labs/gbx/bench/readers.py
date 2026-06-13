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
    from databricks.labs.gbx.ds.register import register

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
    ingest_table: Optional[str] = None,
) -> "ResultRow":
    """Time spark.read.format(fmt).load(path).count() on-cluster.

    For fmt=="raster_gbx": registers the light data source first.
    For fmt=="gdal": ensures the heavyweight GDAL driver is initialised.
    When ``ingest_table`` is set, the timed job writes the read DataFrame to
    that Delta table (mode="overwrite") and returns its row count -- a real
    ingest that forces materialization.  When None the behavior is unchanged
    (plain .count()).
    Returns a single ResultRow (mode="spark-path", category="reader").
    """
    env = capture_env(where)

    if fmt.endswith("_gbx"):
        # register() installs ALL light DataSources (raster_gbx, gtiff_gbx, pmtiles_gbx,
        # vector_gbx + the vector *_gbx). Registering only for fmt=="raster_gbx" left vector
        # formats (geojson_gbx, shapefile_gbx, ...) unregistered -> DATA_SOURCE_NOT_FOUND.
        from databricks.labs.gbx.ds.register import register

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
        df = reader.load(path)
        if ingest_table:
            # Write to a managed table.  On Databricks the default format is Delta;
            # on local Spark it defaults to Parquet.  Either satisfies the row-count
            # assertion -- we avoid hardcoding format("delta") so local tests work
            # without the Delta connector.
            df.write.mode("overwrite").saveAsTable(ingest_table)
            return spark.table(ingest_table).count()
        return df.count()

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        actual_rows = int(actual_rows)
        _note = (
            f"{fmt} -> {ingest_table}"
            if ingest_table
            else f"{fmt} over {os.path.basename(path.rstrip('/\\'))}"
        )
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=f"read_{fmt}",
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
            note=_note,
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=f"read_{fmt}",
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
            note=str(e)[-500:],
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
    from databricks.labs.gbx.ds.register import register

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
            note=str(e)[-500:],
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
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_pmtiles_write(
    spark,
    out_path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    n_tiles: int = 1000,
    shard_zoom: int = 0,
    write_fmt: str = "pmtiles_gbx",
    where: str = "venv",
) -> "ResultRow":
    """Time a PMTiles write of ``n_tiles`` synthetic PNG tiles.

    ``write_fmt`` is ``'pmtiles_gbx'`` (light) or ``'pmtiles'`` (heavy).
    Generates distinct (z, x, y) tiles, caches the DataFrame, then times
    repeated ``write.format(write_fmt)`` calls. Returns a single ResultRow
    (mode="spark-path", category="writer").
    """
    env = capture_env(where)

    if write_fmt == "pmtiles_gbx":
        from databricks.labs.gbx.ds.register import register

        register(spark)

    # Build n_tiles distinct (z, x, y) synthetic PNG tiles.
    # z is chosen so that side*side >= n_tiles (no duplicate addresses).
    png_header = b"\x89PNG\r\n\x1a\n"
    z = max(1, (max(1, n_tiles) - 1).bit_length() // 2 + 1)
    # Ensure side^2 covers n_tiles.
    while (2**z) ** 2 < n_tiles:
        z += 1
    side = 2**z
    rows_data = []
    for i in range(n_tiles):
        x = i % side
        y = (i // side) % side
        rows_data.append((z, x, y, bytearray(png_header + i.to_bytes(4, "big"))))
    df = spark.createDataFrame(
        rows_data, schema="z int, x int, y int, bytes binary"
    ).cache()
    n = int(df.count())

    def _write():
        writer = df.write.format(write_fmt).mode("overwrite")
        if write_fmt == "pmtiles_gbx":
            writer = writer.option("shardZoom", str(shard_zoom))
        writer.save(out_path)

    try:
        stats = time_iters(_write, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api="lightweight" if write_fmt == "pmtiles_gbx" else "heavyweight",
            fn=write_fmt,
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
            throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{write_fmt} write of {n} tiles",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api="lightweight" if write_fmt == "pmtiles_gbx" else "heavyweight",
            fn=write_fmt,
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
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_vector_write(
    spark,
    src_path: str,
    out_dir: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    fmt: str,
    where: str = "venv",
    src_is_table: bool = False,
) -> "ResultRow":
    """Time write.format(fmt) for a light vector writer, with read-back parity.

    Light-only: there is no heavy vector writer tier.  When ``src_is_table``
    is True, reads the source from a pre-existing Spark table (``src_path`` is
    the table name); otherwise reads ``src_path`` via the ``fmt`` light reader.
    Caches the source DataFrame, then times repeated
    ``write.format(fmt).mode("overwrite").save(target)`` calls (no coalesce --
    the two-phase writer merges fragments on commit), writing to a distinct
    ``out_dir/iter.m<i>`` per iteration to avoid append/overwrite contention.
    After timing, reads back the last written target and asserts that the
    non-null geometry count equals the source count.

    Returns a single ResultRow with category="writer", mode="spark-path",
    fn="write_<fmt>", api="lightweight".  On any error returns status="error"
    with the exception in ``note`` (does not raise).
    """
    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    # Read source once and count to establish the feature count for parity.
    try:
        if src_is_table:
            df = spark.table(src_path)
        else:
            df = spark.read.format(fmt).load(src_path)
        df = df.cache()
        n = int(df.count())
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn=f"write_{fmt}",
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
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )

    # Build per-iteration target paths so repeated writes go to fresh directories.
    # Use a format-appropriate extension -- OpenFileGDB (file_gdb_gbx) requires a
    # `.gdb` path or GDAL's CreateDataSource returns None; the others are lenient but
    # a natural extension keeps the round-trip realistic.
    _ext = {
        "geojson_gbx": ".geojson",
        "shapefile_gbx": ".shp",
        "gpkg_gbx": ".gpkg",
        "file_gdb_gbx": ".gdb",
        "vector_gbx": ".geojson",
        "ogr_gbx": ".geojson",
    }.get(fmt, "")
    _targets = [f"{out_dir}/iter.m{i}{_ext}" for i in range(max(1, measured))]
    _iter_idx = [0]

    def _job():
        target = _targets[_iter_idx[0] % len(_targets)]
        _iter_idx[0] += 1
        df.write.format(fmt).mode("overwrite").save(target)

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]

        # Read-back parity: last written target (index measured-1, clamped to len).
        _last = _targets[(max(1, measured) - 1) % len(_targets)]
        try:
            back = spark.read.format(fmt).load(_last)
            # Derive geometry column name from the schema: the geom col has a sibling
            # "<col>_srid" field.  Use the first such pair found.
            _srid_fields = [
                f.name for f in back.schema.fields if f.name.endswith("_srid")
            ]
            if _srid_fields:
                _gcol = _srid_fields[0][: -len("_srid")]
                import pyspark.sql.functions as _F

                _back_n = int(back.filter(_F.col(_gcol).isNotNull()).count())
            else:
                _back_n = int(back.count())
            if _back_n != n:
                return ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn=f"write_{fmt}",
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
                    throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
                    peak_rss_mb=peak_rss_mb(),
                    status="error",
                    note=f"parity FAIL: wrote {n}, read back {_back_n} ({fmt})",
                    output_fingerprint="",
                    **env,
                )
        except Exception as _pe:  # noqa: BLE001
            return ResultRow(
                run_id=run_id,
                api="lightweight",
                fn=f"write_{fmt}",
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
                throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
                peak_rss_mb=peak_rss_mb(),
                status="error",
                note=f"readback error: {str(_pe)[-450:]}",
                output_fingerprint="",
                **env,
            )

        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn=f"write_{fmt}",
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
            throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{fmt} write+readback of {n} features",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn=f"write_{fmt}",
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
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_mvt_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_features: int = 500,
    n_tiles: int = 10,
    where: str = "cluster",
) -> "ResultRow":
    """Time a grouped st_asmvt aggregation over synthetic in-memory features.

    Builds ``n_features`` features distributed across ``n_tiles`` (z,x,y) keys,
    each with a WKB polygon (tile-local coordinates) plus a mixed-type attrs struct
    (int id, double score, str label). Registers the chosen tier, caches the
    DataFrame, then times a ``groupBy("z","x","y").agg(st_asmvt(...))`` job to
    completion. Returns a single ResultRow (mode="spark-path", category="mvt").

    ``api`` controls which tier is registered and timed:
        "lightweight"  → ``databricks.labs.gbx.pyvx.functions``
        "heavyweight"  → ``databricks.labs.gbx.vectorx.functions``
    """
    env = capture_env(where)

    # Register the tier.
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
            asmvt_fn = vx.st_asmvt
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
            asmvt_fn = hx.st_asmvt
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
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
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"register error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Build a synthetic features DataFrame with n_features rows across n_tiles keys.
    # Each tile gets roughly equal features.  Geometries are small squares in
    # tile-local [0, 4096] coordinates (WKB); attrs is a struct with native types.
    try:
        from shapely.geometry import box as _box
        from shapely import to_wkb as _to_wkb
        import pyspark.sql.functions as _F
        from pyspark.sql.types import (
            StructType, StructField, IntegerType, BinaryType,
            DoubleType, StringType,
        )

        # Build tile addresses: a small z=3 grid so (z,x,y) is always valid.
        z = 3
        tile_addresses = [(z, i % 8, (i // 8) % 8) for i in range(n_tiles)]

        rows_data = []
        for i in range(n_features):
            tz, tx, ty = tile_addresses[i % n_tiles]
            # A tiny square near the centre of the tile in pixel space.
            cx = 2048 + (i % 32) * 4
            cy = 2048 + (i % 32) * 4
            geom = _box(cx - 10, cy - 10, cx + 10, cy + 10)
            wkb = bytes(_to_wkb(geom))
            rows_data.append((tz, tx, ty, wkb, i, float(i) * 0.1, f"feat_{i}"))

        schema = StructType([
            StructField("z", IntegerType(), False),
            StructField("x", IntegerType(), False),
            StructField("y", IntegerType(), False),
            StructField("geom", BinaryType(), True),
            StructField("id", IntegerType(), True),
            StructField("score", DoubleType(), True),
            StructField("label", StringType(), True),
        ])
        raw_df = spark.createDataFrame(rows_data, schema=schema)
        # Pack id/score/label into a attrs struct so the aggregator gets a struct column.
        df = raw_df.select(
            "z", "x", "y", "geom",
            _F.struct(
                _F.col("id"),
                _F.col("score"),
                _F.col("label"),
            ).alias("attrs"),
        ).cache()
        n = int(df.count())
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
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
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"dataframe build error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    def _job():
        import pyspark.sql.functions as _F2

        return (
            df.groupBy("z", "x", "y")
            .agg(asmvt_fn(_F2.col("geom"), _F2.col("attrs"), _F2.lit("layer")).alias("mvt"))
            .count()
        )

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        n_tile_groups = min(n, n_tiles)
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
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
            per_tile_avg_s=(ms / n_tile_groups / 1000.0) if (ms and n_tile_groups) else 0.0,
            per_tile_avg_ms=(ms / n_tile_groups) if (ms and n_tile_groups) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n_tile_groups / (ms / 1000.0)) if (ms and n_tile_groups) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"st_asmvt {api} {n} features -> {n_tile_groups} tiles",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
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
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=str(e)[-500:],
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
