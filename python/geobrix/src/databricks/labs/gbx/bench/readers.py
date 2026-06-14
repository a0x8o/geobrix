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
        import pyspark.sql.functions as _F
        from pyspark.sql.types import (
            BinaryType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
        from shapely import to_wkb as _to_wkb
        from shapely.geometry import box as _box

        # Build tile addresses: a small z=3 grid so (z,x,y) is always valid.
        z = 3
        tile_addresses = [(z, i % 8, (i // 8) % 8) for i in range(n_tiles)]

        rows_data = []
        for i in range(n_features):
            tz, tx, ty = tile_addresses[i % n_tiles]
            # Spread each tile's squares across the FULL [0, 4096] tile extent on a
            # coarse grid. The heavy MVT driver (OGR, EPSG:3857, single 0/0/0 tile)
            # quantizes the whole layer to EXTENT=4096, so squares packed into a tiny
            # coordinate band collapse to sub-pixel and the driver drops them (empty
            # tile). Light keeps them (it treats the coords as already tile-local), so
            # a packed band silently breaks light-vs-heavy parity. A 16x16 grid over
            # the extent (step 256) keeps every square distinct + above the
            # quantization floor in both tiers.
            slot = (i // n_tiles) % 256
            cx = 128 + (slot % 16) * 256
            cy = 128 + (slot // 16) * 256
            geom = _box(cx - 32, cy - 32, cx + 32, cy + 32)
            wkb = bytes(_to_wkb(geom))
            rows_data.append((tz, tx, ty, wkb, i, float(i) * 0.1, f"feat_{i}"))

        schema = StructType(
            [
                StructField("z", IntegerType(), False),
                StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False),
                StructField("geom", BinaryType(), True),
                StructField("id", IntegerType(), True),
                StructField("score", DoubleType(), True),
                StructField("label", StringType(), True),
            ]
        )
        raw_df = spark.createDataFrame(rows_data, schema=schema)
        # Pack id/score/label into a attrs struct so the aggregator gets a struct column.
        df = raw_df.select(
            "z",
            "x",
            "y",
            "geom",
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
            .agg(
                asmvt_fn(_F2.col("geom"), _F2.col("attrs"), _F2.lit("layer")).alias(
                    "mvt"
                )
            )
            .count()
        )

    try:
        # Guard against a tier that "succeeds" (10 groups counted) but emits all-NULL or
        # all-empty MVT blobs -- e.g. the heavy OGR MVT driver dropping every feature to a
        # sub-pixel collapse. Counting groups alone masks that, so validate (once, untimed)
        # that at least one group produced a non-empty blob; a collapse becomes a status=
        # "error" row (via the except below), not a misleading "ok".
        import pyspark.sql.functions as _F3

        _validation = (
            df.groupBy("z", "x", "y")
            .agg(
                asmvt_fn(_F3.col("geom"), _F3.col("attrs"), _F3.lit("layer")).alias(
                    "mvt"
                )
            )
            .collect()
        )
        _nonempty = sum(
            1 for _r in _validation if _r["mvt"] and len(bytes(_r["mvt"])) > 0
        )
        if _nonempty == 0:
            raise RuntimeError(
                f"st_asmvt {api} produced {len(_validation)} group(s) but every MVT blob "
                "is NULL/empty -- features collapsed (check coordinate extent vs the "
                "encoder's quantization)."
            )
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
            per_tile_avg_s=(
                (ms / n_tile_groups / 1000.0) if (ms and n_tile_groups) else 0.0
            ),
            per_tile_avg_ms=(ms / n_tile_groups) if (ms and n_tile_groups) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(
                (n_tile_groups / (ms / 1000.0)) if (ms and n_tile_groups) else 0.0
            ),
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


def _tin_result_row(
    *,
    run_id: str,
    api: str,
    fn: str,
    category: str,
    env: dict,
    rows: int,
    status: str,
    note: str,
    stats: Optional[dict] = None,
    warmup: int = 0,
) -> "ResultRow":
    """Compact ResultRow builder for the TIN/legacy spark-path legs.

    When ``stats`` is None (error path) the timing fields are zeroed and
    ``measured_iters`` is 0; otherwise they are filled from ``time_iters``
    output and per-row metrics are amortized over ``rows`` (output rows --
    triangles / interpolated points / decoded geometries)."""
    if stats is None:
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category=category,
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=rows,
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
            status=status,
            note=note[-500:],
            output_fingerprint="",
            **env,
        )
    ms = stats["iter_median_ms"]
    return ResultRow(
        run_id=run_id,
        api=api,
        fn=fn,
        category=category,
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype="",
        srid=0,
        rows=rows,
        nodata_frac=0.0,
        warmup_iters=stats["warmup_iters"],
        measured_iters=stats["measured_iters"],
        iter_median_s=ms / 1000.0,
        iter_min_s=stats["iter_min_ms"] / 1000.0,
        iter_p90_s=stats["iter_p90_ms"] / 1000.0,
        iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
        avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
        per_tile_avg_s=(ms / rows / 1000.0) if (ms and rows) else 0.0,
        per_tile_avg_ms=(ms / rows) if (ms and rows) else 0.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=(rows / (ms / 1000.0)) if (ms and rows) else 0.0,
        peak_rss_mb=peak_rss_mb(),
        status=status,
        note=note[-500:],
        output_fingerprint="",
        **env,
    )


def run_legacy_aswkb(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_legacyaswkb decode over ``n_rows`` synthetic legacy structs.

    Light (``api="lightweight"``) registers ``databricks.labs.gbx.pyvx`` and
    times ``SELECT gbx_st_legacyaswkb(g) FROM v``. Heavy (``api="heavyweight"``)
    registers ``databricks.labs.gbx.vectorx.jts.legacy`` -- the SAME SQL name --
    and times the same query. (The shared name means a light+heavy parity cell
    must collect light BEFORE registering heavy; that ordering lives in the
    cluster cell, not here -- each call registers exactly one tier.)
    Returns a single ResultRow (mode="spark-path", category="legacy").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_legacy_structs

    env = capture_env(where)
    fn = "st_legacyaswkb"
    cat = "legacy"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx.jts.legacy import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_legacy_structs(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_legacy_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_st_legacyaswkb(g) AS w FROM _legacy_bench_v"
        ).count()

    try:
        # Untimed validation: confirm the decode produces non-null WKB.
        _val = spark.sql("SELECT gbx_st_legacyaswkb(g) AS w FROM _legacy_bench_v").head(
            1
        )
        if not _val or _val[0]["w"] is None or len(bytes(_val[0]["w"])) == 0:
            raise RuntimeError("st_legacyaswkb produced null/empty WKB")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"st_legacyaswkb {api} decoded {n} geometries",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_triangulate(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 5,
    n_points: int = 25,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_triangulate over ``n_rows`` synthetic point arrays.

    Light registers pyvx UDTFs and times the SQL ``LATERAL`` TVF; heavy
    registers vectorx and times the JVM generator-Column form (the surfaces
    occupy different catalog paths and coexist). Records ``rows`` = number of
    output triangles. Returns one ResultRow (mode="spark-path", category="tin").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_tin_points

    env = capture_env(where)
    fn = "st_triangulate"
    cat = "tin"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_tin_points(n_rows, n_points=n_points)
        df = spark.createDataFrame(data, schema=schema).cache()
        df.count()
        df.createOrReplaceTempView("_tin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    if api == "lightweight":

        def _job():
            return spark.sql(
                "SELECT t.triangle FROM _tin_bench_v, LATERAL "
                "gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t"
            ).count()

    else:
        import pyspark.sql.functions as _f

        def _job():
            return df.select(
                _f.call_function(
                    "gbx_st_triangulate",
                    _f.col("pts"),
                    _f.col("bl"),
                    _f.col("mt"),
                    _f.col("st"),
                    _f.col("spf"),
                    _f.lit("constrained"),
                ).alias("triangle")
            ).count()

    try:
        n_out = int(_job())
        if n_out <= 0:
            raise RuntimeError("st_triangulate produced no triangles")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n_out,
            status="ok",
            note=f"st_triangulate {api} -> {n_out} triangles",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_interp_bbox(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 5,
    n_points: int = 25,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_interpolateelevationbbox over ``n_rows`` synthetic point arrays.

    Light = SQL ``LATERAL`` UDTF; heavy = JVM generator-Column. Records
    ``rows`` = number of interpolated grid points. Returns one ResultRow
    (mode="spark-path", category="tin").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_tin_points

    env = capture_env(where)
    fn = "st_interpolateelevationbbox"
    cat = "tin"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_tin_points(n_rows, n_points=n_points)
        df = spark.createDataFrame(data, schema=schema).cache()
        df.count()
        df.createOrReplaceTempView("_tin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    if api == "lightweight":

        def _job():
            return spark.sql(
                "SELECT t.elevation_point AS p FROM _tin_bench_v, LATERAL "
                "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, "
                "xmin, ymin, xmax, ymax, w, h, srid, 'constrained') t"
            ).count()

    else:
        import pyspark.sql.functions as _f

        def _job():
            return df.select(
                _f.call_function(
                    "gbx_st_interpolateelevationbbox",
                    _f.col("pts"),
                    _f.col("bl"),
                    _f.col("mt"),
                    _f.col("st"),
                    _f.col("spf"),
                    _f.col("xmin"),
                    _f.col("ymin"),
                    _f.col("xmax"),
                    _f.col("ymax"),
                    _f.col("w"),
                    _f.col("h"),
                    _f.col("srid"),
                    _f.lit("constrained"),
                ).alias("p")
            ).count()

    try:
        n_out = int(_job())
        if n_out <= 0:
            raise RuntimeError("st_interpolateelevationbbox produced no points")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n_out,
            status="ok",
            note=f"st_interpolateelevationbbox {api} -> {n_out} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_interp_geom(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 5,
    n_points: int = 25,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_interpolateelevationgeom over ``n_rows`` synthetic point arrays.

    Light = SQL ``LATERAL`` UDTF (arg order: pts, bl, mt, st, spf, origin, cols,
    rows, cell_x, cell_y, mode); heavy = JVM generator-Column (same arg order).
    Records ``rows`` = number of interpolated grid points. Returns one ResultRow
    (mode="spark-path", category="tin").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_tin_points

    env = capture_env(where)
    fn = "st_interpolateelevationgeom"
    cat = "tin"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_tin_points(n_rows, n_points=n_points)
        df = spark.createDataFrame(data, schema=schema).cache()
        df.count()
        df.createOrReplaceTempView("_tin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    if api == "lightweight":

        def _job():
            return spark.sql(
                "SELECT t.elevation_point AS p FROM _tin_bench_v, LATERAL "
                "gbx_st_interpolateelevationgeom(pts, bl, mt, st, spf, "
                "origin, cols, rows_n, cell_x, cell_y, 'constrained') t"
            ).count()

    else:
        import pyspark.sql.functions as _f

        def _job():
            return df.select(
                _f.call_function(
                    "gbx_st_interpolateelevationgeom",
                    _f.col("pts"),
                    _f.col("bl"),
                    _f.col("mt"),
                    _f.col("st"),
                    _f.col("spf"),
                    _f.col("origin"),
                    _f.col("cols"),
                    _f.col("rows_n"),
                    _f.col("cell_x"),
                    _f.col("cell_y"),
                    _f.lit("constrained"),
                ).alias("p")
            ).count()

    try:
        n_out = int(_job())
        if n_out <= 0:
            raise RuntimeError("st_interpolateelevationgeom produced no points")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n_out,
            status="ok",
            note=f"st_interpolateelevationgeom {api} -> {n_out} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def _register_quadbin(spark, api: str) -> None:
    """Register exactly one quadbin tier.

    light  -> ``databricks.labs.gbx.pygx`` (gx.register: spark.udf only).
    heavy  -> ``databricks.labs.gbx.gridx.quadbin`` (JVM Scala UDFs).

    Both tiers expose the SAME ``gbx_quadbin_*`` SQL names, so registering one
    overwrites the other in the session catalog.  Each ``run_quadbin_*`` call
    therefore registers a single tier; the light-vs-heavy parity gate (which
    must collect light BEFORE re-registering heavy) lives in the cluster cell,
    not here.
    """
    if api == "lightweight":
        from databricks.labs.gbx.pygx import functions as gx

        gx.register(spark)
    else:
        from databricks.labs.gbx.gridx.quadbin import functions as hx

        hx.register(spark)


def run_quadbin_pointascell(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_pointascell (scalar lon/lat -> cell) over ``n_rows`` points.

    Light registers pygx (``gbx_quadbin_pointascell`` via spark.udf); heavy
    registers gridx.quadbin (the SAME SQL name, JVM). Records ``rows`` = number
    of cells produced. Returns one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_points

    env = capture_env(where)
    fn = "quadbin_pointascell"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_points(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_pointascell(lon, lat, {res}) AS cell "
            "FROM _quadbin_bench_v"
        ).count()

    try:
        # Untimed validation: confirm non-null cell ids are produced.
        _val = spark.sql(
            f"SELECT gbx_quadbin_pointascell(lon, lat, {res}) AS cell "
            "FROM _quadbin_bench_v WHERE lon IS NOT NULL LIMIT 1"
        ).head(1)
        if not _val or _val[0]["cell"] is None:
            raise RuntimeError("quadbin_pointascell produced null cell")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_pointascell {api} encoded {n} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_polyfill(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_polyfill (geom -> ARRAY<cell>) over ``n_rows`` WKT polygons.

    Light registers pygx; heavy registers gridx.quadbin (same SQL name). Records
    ``rows`` = number of input polygons (each producing a cell array). Returns one
    ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_polygons

    env = capture_env(where)
    fn = "quadbin_polyfill"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_polyfill(geom, {res}) AS cells "
            "FROM _quadbin_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty cell array.
        _val = spark.sql(
            f"SELECT gbx_quadbin_polyfill(geom, {res}) AS cells "
            "FROM _quadbin_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["cells"]:
            raise RuntimeError("quadbin_polyfill produced empty cell array")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_polyfill {api} filled {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_tessellate(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_tessellate (geom -> ARRAY<STRUCT<cell,geom>>) over polygons.

    Light registers pygx; heavy registers gridx.quadbin (same SQL name). Records
    ``rows`` = number of input polygons. Returns one ResultRow (mode="spark-path",
    category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_polygons

    env = capture_env(where)
    fn = "quadbin_tessellate"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_tessellate(geom, {res}) AS chips "
            "FROM _quadbin_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty chip array with bytes.
        _val = spark.sql(
            f"SELECT gbx_quadbin_tessellate(geom, {res}) AS chips "
            "FROM _quadbin_bench_v LIMIT 1"
        ).head(1)
        _chips = _val[0]["chips"] if _val else None
        if (
            not _chips
            or _chips[0]["geom"] is None
            or len(bytes(_chips[0]["geom"])) == 0
        ):
            raise RuntimeError("quadbin_tessellate produced empty/null chips")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_tessellate {api} tessellated {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_cellunion_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_cellunion_agg (grouped aggregate) over ``n_rows`` cell ids.

    Streams one cell id per row, grouped by a small key set, unioning each
    group's cell boundaries into one EWKB MultiPolygon.  Light registers pygx
    (a GROUPED_AGG pandas UDF); heavy registers gridx.quadbin (the SAME SQL
    name, a JVM TypedImperativeAggregate). Records ``rows`` = number of input
    cells. Returns one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cellid_arrays

    env = capture_env(where)
    fn = "quadbin_cellunion_agg"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cellid_arrays(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_agg_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT group, gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _quadbin_agg_v GROUP BY group"
        ).count()

    try:
        # Untimed validation: confirm each group produced a non-empty union blob.
        _val = spark.sql(
            "SELECT gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _quadbin_agg_v GROUP BY group"
        ).collect()
        _nonempty = sum(1 for _r in _val if _r["u"] and len(bytes(_r["u"])) > 0)
        if _nonempty == 0:
            raise RuntimeError("quadbin_cellunion_agg produced 0 non-empty union blobs")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_cellunion_agg {api} unioned {n} cells -> {_nonempty} groups",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_resolution(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_resolution (scalar cell -> INT) over ``n_rows`` cell ids.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): exact INT equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_resolution"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_resolution(cell) AS r FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm the resolution comes back as the input res.
        _val = spark.sql(
            "SELECT gbx_quadbin_resolution(cell) AS r FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["r"] != res:
            raise RuntimeError("quadbin_resolution produced unexpected resolution")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_resolution {api} resolved {n} cells",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_kring(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_kring (scalar cell -> ARRAY<LONG>) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): exact sorted
    cell-set per row at a fixed ``k``.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_kring"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_kring(cell, {k}) AS ring FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring (k=1 -> up to 9 cells).
        _val = spark.sql(
            f"SELECT gbx_quadbin_kring(cell, {k}) AS ring FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("quadbin_kring produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_kring {api} ringed {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_distance(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_distance (scalar (cell_a, cell_b) -> INT) over ``n_rows`` pairs.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input pairs. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): exact INT equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cell_pairs

    env = capture_env(where)
    fn = "quadbin_distance"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cell_pairs(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_pair_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_distance(cell_a, cell_b) AS d FROM _quadbin_pair_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null integer distance comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_distance(cell_a, cell_b) AS d FROM _quadbin_pair_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["d"] is None:
            raise RuntimeError("quadbin_distance produced null distance")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_distance {api} measured {n} pairs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_aswkb(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_aswkb (scalar cell -> EWKB polygon) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): decoded geometry
    within 1e-6 + SRID 4326.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_aswkb"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_aswkb(cell) AS g FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty EWKB polygon comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_aswkb(cell) AS g FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("quadbin_aswkb produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_aswkb {api} encoded {n} cell polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_centroid(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_centroid (scalar cell -> EWKB point) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): decoded point
    within 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_centroid"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_centroid(cell) AS g FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty EWKB point comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_centroid(cell) AS g FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("quadbin_centroid produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_centroid {api} encoded {n} cell centroids",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_cellunion(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_cellunion (scalar ARRAY<cell> -> EWKB) over grouped cell arrays.

    Reuses the cellunion_agg corpus (``generate_quadbin_cellid_arrays``):
    collect each group's cells into an ARRAY<LONG>, then call the scalar
    ``gbx_quadbin_cellunion`` on the array. Light registers pygx; heavy registers
    gridx.quadbin (the SAME SQL name). Records ``rows`` = number of unioned arrays
    (one per group). Returns one ResultRow (mode="spark-path", category="grid").
    Parity (cluster cell): decoded union geometry via symmetric-difference-area
    < 1e-6 (member-ordering-robust, like the cellunion_agg leg).
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cellid_arrays

    env = capture_env(where)
    fn = "quadbin_cellunion"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cellid_arrays(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema)
        df.createOrReplaceTempView("_quadbin_cellsrc_v")
        # Collapse the streamed (group, cell) rows into one ARRAY<cell> per group
        # so the scalar cellunion gets the same cell sets the agg unions.
        arr_df = spark.sql(
            "SELECT group, collect_list(cell) AS cells "
            "FROM _quadbin_cellsrc_v GROUP BY group"
        ).cache()
        n = int(arr_df.count())
        arr_df.createOrReplaceTempView("_quadbin_cellarr_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_cellunion(cells) AS u FROM _quadbin_cellarr_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty union blob comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_cellunion(cells) AS u FROM _quadbin_cellarr_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["u"] is None or len(bytes(_val[0]["u"])) == 0:
            raise RuntimeError("quadbin_cellunion produced empty/null union")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_cellunion {api} unioned {n} cell arrays",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def _make_synthetic_geotiff(
    n_regions: int = 4,
    size: int = 64,
    *,
    bands: int = 1,
    bounds: tuple = (-0.5, 51.3, 0.5, 51.7),
    checkerboard: int = 0,
) -> bytes:
    """Build a minimal in-memory GeoTIFF with distinct value structure.

    The tile is ``size x size`` pixels, ``bands``-band float32, EPSG:4326.

    Value structure (drives the fan-out of each consuming function):
      * ``checkerboard > 0``: paint a ``checkerboard x checkerboard`` grid of
        alternating integer values -- yields MANY connected components for
        rst_polygonize and many distinct cell measures for the grid counters.
      * otherwise: split into ``n_regions`` horizontal bands of distinct value
        (the original behaviour) -- a handful of large components.

    ``bounds`` is the WGS84 (minx, miny, maxx, maxy) extent; a wider extent
    spans more H3 cells / XYZ tiles. ``bands`` > 1 drives rst_separatebands.

    Returns raw GTiff bytes suitable for the ``raster`` field of a tile struct.
    """
    import io as _io

    import numpy as np
    from rasterio.crs import CRS
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    def _one_band(seed: float) -> "np.ndarray":
        arr = np.zeros((size, size), dtype=np.float32)
        if checkerboard and checkerboard > 0:
            # Paint a checkerboard of distinct values: adjacent cells differ so
            # polygonize sees ``checkerboard^2`` separate connected components.
            cell = max(1, size // checkerboard)
            last = checkerboard - 1
            for by in range(checkerboard):
                r0 = by * cell
                r1 = size if by == last else (by + 1) * cell
                for bx in range(checkerboard):
                    c0 = bx * cell
                    c1 = size if bx == last else (bx + 1) * cell
                    # Distinct value per block; +seed so bands differ.
                    val = float((by * checkerboard + bx) % 251 + 1) + seed
                    arr[r0:r1, c0:c1] = val
        else:
            step = max(1, size // n_regions)
            for i in range(n_regions):
                row_start = i * step
                row_end = (i + 1) * step if i < n_regions - 1 else size
                arr[row_start:row_end, :] = float(i + 1) + seed
        return arr

    minx, miny, maxx, maxy = bounds
    transform = from_bounds(minx, miny, maxx, maxy, size, size)
    crs = CRS.from_epsg(4326)

    buf = _io.BytesIO()
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            dtype="float32",
            width=size,
            height=size,
            count=max(1, bands),
            crs=crs,
            transform=transform,
        ) as ds:
            for b in range(max(1, bands)):
                ds.write(_one_band(seed=float(b) * 0.0), b + 1)
        buf.write(mf.read())
    return buf.getvalue()


# Fan-out functions covered by run_fanout_udtf.  Order is stable for parity loops.
FANOUT_FUNCTIONS = [
    "rst_polygonize",
    "rst_h3_rastertogridcount",
    "rst_xyzpyramid",
    "rst_h3_tessellate",
    "rst_retile",
    "rst_tooverlappingtiles",
    "rst_maketiles",
    "rst_separatebands",
]

# Per-function synthetic-input + invocation spec.  ``scale`` (default 1.0) is the
# tunable that dials the fan-out up/down; sizes below are the scale=1.0 defaults
# chosen to be meaningful yet finish in a couple of minutes on ~20 workers.
#
# Each entry returns (tile_kwargs, light_lateral, heavy_lateral) where the LATERAL
# fragments are the part AFTER "LATERAL" in the SQL, and ``heavy_lateral`` already
# flattens the heavy tier to the SAME granularity as the light flat UDTF rows:
#   * polygonize  -> heavy ARRAY<struct>           -> explode  (single)
#   * gridcount   -> heavy ARRAY<ARRAY<struct>>    -> explode∘explode (double)
#   * 5 tilers    -> heavy CollectionGenerator     -> LATERAL VIEW gbx_.. (no explode)
#   * xyzpyramid  -> heavy CollectionGenerator emits flat rows -> LATERAL VIEW (no explode)


def _fanout_spec(fn: str, scale: float):
    """Return (tile_kwargs, light_sql, heavy_sql) for a fan-out function.

    ``light_sql`` / ``heavy_sql`` are full SQL strings over the temp view
    ``_fanout_bench_ras`` (column ``tile``) that each produce FLAT rows so the
    two row counts are directly comparable (the flatten-both parity gate).
    """
    s = max(0.1, float(scale))

    if fn == "rst_polygonize":
        # Many connected components -> large polygon fan-out.
        cb = max(2, int(round(16 * (s**0.5))))
        size = max(64, int(round(256 * (s**0.5))))
        tile_kwargs = dict(size=size, checkerboard=cb)
        light = "SELECT t.* FROM _fanout_bench_ras, LATERAL gbx_rst_polygonize(tile, 1, 4) t"
        # Heavy returns ARRAY<struct> -> single explode.
        heavy = (
            "SELECT p.* FROM _fanout_bench_ras "
            "LATERAL VIEW explode(gbx_rst_polygonize(tile, 1, 4)) e AS p"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_h3_rastertogridcount":
        # Fine H3 resolution + wide extent -> many cells.
        res = 9 if s <= 1.0 else 10
        # Wider extent at higher scale -> more cells.
        span = 0.5 * (s**0.5)
        bounds = (-span, 51.5 - span, span, 51.5 + span)
        tile_kwargs = dict(size=max(128, int(round(256 * (s**0.5)))), bounds=bounds)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_h3_rastertogridcount(tile, {res}) t"
        )
        # Heavy returns ARRAY<ARRAY<struct>> (bands x cells) -> DOUBLE explode.
        heavy = (
            "SELECT c.* FROM _fanout_bench_ras "
            f"LATERAL VIEW explode(gbx_rst_h3_rastertogridcount(tile, {res})) eb AS band_cells "
            "LATERAL VIEW explode(band_cells) ec AS c"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_xyzpyramid":
        # Deep zoom range over a multi-degree extent -> thousands of tiles.
        min_z = 4
        max_z = 9 if s <= 1.0 else 10
        span = 1.5 * (s**0.5)
        bounds = (-span, 51.5 - span, span, 51.5 + span)
        tile_kwargs = dict(size=max(128, int(round(256 * (s**0.5)))), bounds=bounds)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_xyzpyramid(tile, {min_z}, {max_z}, 'PNG', 256, 'bilinear') t"
        )
        # Heavy generator emits flat rows directly -> LATERAL VIEW, NO explode.
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            f"LATERAL VIEW gbx_rst_xyzpyramid(tile, {min_z}, {max_z}, 'PNG', 256, 'bilinear') t AS tile"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_h3_tessellate":
        res = 8 if s <= 1.0 else 9
        span = 0.5 * (s**0.5)
        bounds = (-span, 51.5 - span, span, 51.5 + span)
        tile_kwargs = dict(size=max(128, int(round(256 * (s**0.5)))), bounds=bounds)
        # Pass explicit mode='covering' (the default) so the bench leg is
        # unambiguous and a future 'centroid' variant can be added by changing
        # this one argument. Heavy uses LATERAL VIEW (CollectionGenerator, flat
        # rows, no explode) -- same pattern as rst_xyzpyramid.
        mode = "covering"
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_h3_tessellate(tile, {res}, '{mode}') t"
        )
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            f"LATERAL VIEW gbx_rst_h3_tessellate(tile, {res}, '{mode}') t AS tile"
        )
        return tile_kwargs, light, heavy

    if fn in ("rst_retile", "rst_tooverlappingtiles"):
        # Large raster with small tile size -> many tiles.
        size = max(512, int(round(1024 * (s**0.5))))
        tw = th = 64
        tile_kwargs = dict(size=size)
        if fn == "rst_retile":
            light = (
                "SELECT t.* FROM _fanout_bench_ras, "
                f"LATERAL gbx_rst_retile(tile, {tw}, {th}) t"
            )
            heavy = (
                "SELECT t.* FROM _fanout_bench_ras "
                f"LATERAL VIEW gbx_rst_retile(tile, {tw}, {th}) t AS tile"
            )
        else:
            overlap = 8
            light = (
                "SELECT t.* FROM _fanout_bench_ras, "
                f"LATERAL gbx_rst_tooverlappingtiles(tile, {tw}, {th}, {overlap}) t"
            )
            heavy = (
                "SELECT t.* FROM _fanout_bench_ras "
                f"LATERAL VIEW gbx_rst_tooverlappingtiles(tile, {tw}, {th}, {overlap}) t AS tile"
            )
        return tile_kwargs, light, heavy

    if fn == "rst_maketiles":
        # Large raster + small per-tile MB budget -> many power-of-4 sub-tiles.
        size = max(512, int(round(1024 * (s**0.5))))
        size_mb = 1
        tile_kwargs = dict(size=size)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_maketiles(tile, {size_mb}) t"
        )
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            f"LATERAL VIEW gbx_rst_maketiles(tile, {size_mb}) t AS tile"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_separatebands":
        # MANY bands (hyperspectral / large-band case) -> large per-row fan-out.
        nbands = max(8, int(round(64 * s)))
        tile_kwargs = dict(size=64, bands=nbands)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, LATERAL gbx_rst_separatebands(tile) t"
        )
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            "LATERAL VIEW gbx_rst_separatebands(tile) t AS tile"
        )
        return tile_kwargs, light, heavy

    raise ValueError(f"unknown fanout fn: {fn}")


def run_fanout_udtf(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    fn: str,
    scale: float = 1.0,
    where: str = "cluster",
) -> "ResultRow":
    """Time one of the 8 fan-out functions, light (UDTF) vs heavy (generator/array).

    Builds a per-function synthetic GeoTIFF tile sized to drive ``fn`` into a
    LARGE fan-out (the regime where streaming UDTFs help -- see ``_fanout_spec``),
    wraps it in a one-row DataFrame matching the tile struct schema, then times
    the flattened invocation to completion. Returns a single ResultRow
    (mode="spark-path", category="fanout").

    Both tiers are invoked via SQL and flattened to the SAME granularity so the
    output row counts are directly comparable (flatten-both parity):
        * light = streaming UDTF via ``LATERAL gbx_<fn>(...)`` -> already flat.
        * heavy = its Scala counterpart, flattened to match:
            - ARRAY<struct>        (polygonize)  -> single ``explode``
            - ARRAY<ARRAY<struct>> (gridcount)   -> double ``explode``
            - CollectionGenerator  (5 tilers)    -> ``LATERAL VIEW gbx_.. (no explode)``
            - CollectionGenerator emitting flat rows (xyzpyramid) -> ``LATERAL VIEW``

    ``api`` controls which tier is timed:
        "lightweight"  -> registers pyrx UDTFs
        "heavyweight"  -> registers rasterx (needs the JAR -> cluster-only)
    ``fn`` is one of ``FANOUT_FUNCTIONS``.
    ``scale`` dials the synthetic fan-out up/down (default 1.0).
    """
    env = capture_env(where)

    # Resolve the per-function synthetic-input + invocation spec up front so a bad
    # fn name fails loudly rather than silently benching nothing.
    try:
        tile_kwargs, light_sql, heavy_sql = _fanout_spec(fn, scale)
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
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
            note=f"spec error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Register the tier.
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyrx import functions as prx

            prx.register(spark)
        else:
            from databricks.labs.gbx.rasterx import functions as rx

            rx.register(spark)
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
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

    # Build a synthetic tile DataFrame (one row).
    try:
        from pyspark.sql.types import (
            BinaryType,
            LongType,
            MapType,
            StringType,
            StructField,
            StructType,
        )

        tile_bytes = _make_synthetic_geotiff(**tile_kwargs)
        _w = int(tile_kwargs.get("size", 64))
        _bands = int(tile_kwargs.get("bands", 1))
        tile_schema = StructType(
            [
                StructField("cellid", LongType(), nullable=False),
                StructField("raster", BinaryType(), nullable=False),
                StructField(
                    "metadata",
                    MapType(StringType(), StringType()),
                    nullable=True,
                ),
            ]
        )
        tile_row = (
            0,
            bytearray(tile_bytes),
            {
                "driver": "GTiff",
                "width": str(_w),
                "height": str(_w),
                "count": str(_bands),
            },
        )
        df = spark.createDataFrame([tile_row], schema=tile_schema)
        import pyspark.sql.functions as _F

        # Wrap as a struct column named "tile" matching the gbx_rst_* UDTF expectation.
        df = df.select(_F.struct("cellid", "raster", "metadata").alias("tile")).cache()
        df.count()  # materialise

        # Register as a temp view for SQL path.
        df.createOrReplaceTempView("_fanout_bench_ras")
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
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
            note=f"dataframe build error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Build the job closure: both tiers go through SQL and are flattened to the
    # SAME granularity (flatten-both parity).  ``light_sql`` is the streaming UDTF
    # LATERAL form; ``heavy_sql`` flattens the Scala counterpart per _fanout_spec.
    try:
        sql = light_sql if api == "lightweight" else heavy_sql

        def _job():
            return spark.sql(sql).count()

        # Validate once (untimed): guard against 0-row / all-empty output.
        actual_rows = int(_job())
        if actual_rows == 0:
            raise RuntimeError(
                f"{fn} ({api}) produced 0 output rows -- check tile content or "
                "registration (all-empty/null guard)."
            )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
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
            note=f"job build/validate error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=_w,
            bands=_bands,
            dtype="float32",
            srid=4326,
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
            note=f"{fn} {api} -> {actual_rows} output rows",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=_w,
            bands=_bands,
            dtype="float32",
            srid=4326,
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
