"""OvertureClient — distributed, AOI-driven Overture Maps GeoParquet data source.

Mirrors gbx.stac.StacClient: a static-catalog discovery step (driver-side,
metadata-only), then DISTRIBUTED asset I/O. Default I/O path is a distributed
Spark read of Overture GeoParquet over the cloud path with bbox-struct predicate
pushdown (AOI rows only), written to a UC Volume + an optional metadata Delta
table; an HTTP-href whole-file download is the fallback. Serverless-safe:
parallelism only via repartition(N, col); no spark.conf/cache/persist/.rdd.

Injection seams (offline tests): _catalog_opener (returns a pystac.Catalog-shaped
object) and _get_fn (an HTTP fetcher passed to the fallback downloader).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

OVERTURE_CATALOG = "https://stac.overturemaps.org/catalog.json"

# Spark schema for discover() output — pinned; SP2/SP3 depend on it.
_DISCOVER_COLS = ["theme", "type", "href", "asset_bbox", "release"]

# Columns for the download() metadata output — pinned; Tasks 6-10 + SP3 depend on it.
_META_COLS = [
    "theme",
    "type",
    "source",
    "path",
    "out_file_sz",
    "is_out_file_valid",
    "last_update",
    "asset_bbox",
    "release",
    "href",
]


def _meta_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("source", StringType()),
            StructField("out_file_sz", LongType()),
            StructField("is_out_file_valid", BooleanType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
            StructField("href", StringType()),
        ]
    )


def _meta_dataframe(spark, meta_rows, partitions):
    """Build the download metadata DataFrame.

    Repartitions by (theme, type, source) using column-hash (NOT number-only) so
    the result stays distributed on Serverless (AQE cannot coalesce a column-hash
    repartition to 1). Aliases `source` as `path` and adds `last_update`.
    """
    from pyspark.sql import functions as F

    cols = [
        "theme",
        "type",
        "source",
        "out_file_sz",
        "is_out_file_valid",
        "asset_bbox",
        "release",
        "href",
    ]
    if not meta_rows:
        df = spark.createDataFrame([], _meta_schema())
    else:
        df = spark.createDataFrame(
            [tuple(r[c] for c in cols) for r in meta_rows], _meta_schema()
        )
    n = max(1, partitions or 1)
    # Column-hash repartition: on Serverless, number-only repartition(N) is
    # AQE-coalesced to 1 (serial); hashing by real columns keeps it distributed.
    return (
        df.repartition(n, F.col("theme"), F.col("type"), F.col("source"))
        .withColumn("path", F.col("source"))
        .withColumn("last_update", F.current_timestamp())
        .select(
            "theme",
            "type",
            "source",
            "path",
            "out_file_sz",
            "is_out_file_valid",
            "last_update",
            "asset_bbox",
            "release",
            "href",
        )
    )


def _is_valid_parquet(path: str) -> bool:
    """True iff the parquet opens (pyarrow). Validity = opens, not raster-decodable."""
    try:
        import pyarrow.parquet as pq

        pq.ParquetFile(path).metadata  # touch metadata to force a read
        return True
    except Exception:  # noqa: BLE001
        return False


def _discover_schema():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )


class OvertureClient:
    def __init__(
        self,
        catalog: str = OVERTURE_CATALOG,
        release: Optional[str] = None,
        _catalog_opener=None,
        _get_fn=None,
    ):
        self.catalog = catalog
        self.release = release
        self._catalog_opener = _catalog_opener
        self._get_fn = _get_fn

    def _open_catalog(self):
        import pystac

        return pystac.Catalog.from_file(self.catalog)

    def _opener(self):
        return (
            self._catalog_opener
            if self._catalog_opener is not None
            else self._open_catalog
        )

    def _download_distributed(
        self, assets_df, out_dir, *, bbox, validate, partitions
    ) -> "DataFrame":
        """Performant default: distributed read of each asset's GeoParquet with a
        bbox-struct predicate pushdown, AOI subset written to the Volume per
        (theme, type). Returns the metadata rows. Serverless-safe: repartition by
        column only; no spark.conf/cache/persist/.rdd.

        Parameters
        ----------
        assets_df:
            DataFrame from discover() — columns: theme, type, href, asset_bbox, release.
        out_dir:
            Root output directory (UC Volume path or local path).
        bbox:
            AOI as (minx, miny, maxx, maxy).
        validate:
            If True, read-back one row from the written parquet to confirm it is valid.
        partitions:
            Target partition count for repartition(N, col). Must be >= 1.
        """
        import os

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        from databricks.labs.gbx.sample._overture_discover import normalize_bbox

        spark = SparkSession.getActiveSession()
        minx, miny, maxx, maxy = normalize_bbox(bbox)
        assets = assets_df.select(*_DISCOVER_COLS).collect()

        meta_rows = []
        for a in assets:
            df = spark.read.parquet(a["href"])
            # bbox-struct predicate pushdown when the Overture `bbox` struct is present.
            if "bbox" in df.columns:
                df = df.filter(
                    (F.col("bbox.xmin") <= F.lit(maxx))
                    & (F.col("bbox.xmax") >= F.lit(minx))
                    & (F.col("bbox.ymin") <= F.lit(maxy))
                    & (F.col("bbox.ymax") >= F.lit(miny))
                )
            target = os.path.join(out_dir, a["theme"], a["type"])
            # Hash-by-column repartition (NOT number-only): on Serverless a
            # round-robin repartition(N) is AQE-coalesced to 1 (serial). Hash by a
            # real source column so the per-asset row groups spread across cores.
            # Prefer the Overture `id` column; else hash by the first column.
            key = "id" if "id" in df.columns else df.columns[0]
            (
                df.repartition(partitions, F.col(key))
                .write.mode("overwrite")
                .parquet(target)
            )
            valid = True
            if validate:
                try:
                    spark.read.parquet(target).limit(1).count()
                except Exception:
                    valid = False
            try:
                sz = sum(
                    os.path.getsize(os.path.join(target, f))
                    for f in os.listdir(target)
                    if f.endswith(".parquet")
                )
            except OSError:
                sz = None
            meta_rows.append(
                {
                    "theme": a["theme"],
                    "type": a["type"],
                    "source": target,
                    "out_file_sz": sz,
                    "is_out_file_valid": valid,
                    "asset_bbox": a["asset_bbox"],
                    "release": a["release"],
                    "href": a["href"],
                }
            )

        return _meta_dataframe(spark, meta_rows, partitions)

    def _download_fallback(
        self, assets_df, out_dir, *, validate, max_tries, partitions
    ) -> "DataFrame":
        """Fallback: whole-file HTTP download fanned out by href (column-hash
        repartition, Serverless-safe). Temp-file then sequential copy (Volume-safe).
        Returns the same metadata schema as the distributed path."""
        import os
        import shutil
        import tempfile

        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        get_fn = self._get_fn  # None in production; injectable for tests
        _validate = validate
        _max_tries = max_tries

        row_schema = StructType(
            [
                StructField("theme", StringType()),
                StructField("type", StringType()),
                StructField("source", StringType()),
                StructField("out_file_sz", LongType()),
                StructField("is_out_file_valid", BooleanType()),
                StructField("asset_bbox", ArrayType(DoubleType())),
                StructField("release", StringType()),
                StructField("href", StringType()),
            ]
        )

        @F.udf(row_schema)
        def _fetch(theme, type_, href, asset_bbox, release):
            getter = get_fn
            if getter is None:
                import requests

                getter = requests.get
            target_dir = os.path.join(out_dir, theme, type_)
            os.makedirs(target_dir, exist_ok=True)
            basename = os.path.basename(href.split("?")[0]) or "asset.parquet"
            outpath = os.path.join(target_dir, basename)
            # idempotent skip: a present, openable file is left as-is
            if os.path.exists(outpath) and _is_valid_parquet(outpath):
                sz = os.path.getsize(outpath)
                return (theme, type_, outpath, sz, True, asset_bbox, release, href)
            for _ in range(max(1, _max_tries)):
                tmpd = tempfile.mkdtemp(prefix="gbx_overture_")
                try:
                    local = os.path.join(tmpd, basename)
                    resp = getter(href, timeout=100, stream=True)
                    resp.raise_for_status()
                    with open(local, "wb") as fh:
                        for chunk in resp.iter_content(1024 * 1024):
                            if chunk:
                                fh.write(chunk)
                    ok = (not _validate) or _is_valid_parquet(local)
                    if ok:
                        shutil.copyfile(local, outpath)
                        sz = os.path.getsize(outpath)
                        return (
                            theme,
                            type_,
                            outpath,
                            sz,
                            True,
                            asset_bbox,
                            release,
                            href,
                        )
                except Exception:  # noqa: BLE001
                    pass
                finally:
                    shutil.rmtree(tmpd, ignore_errors=True)
            return (theme, type_, None, None, False, asset_bbox, release, href)

        n = max(1, partitions or 1)
        fetched = (
            assets_df.select(*_DISCOVER_COLS)
            # column-hash repartition by href (NOT number-only) for Serverless.
            .repartition(n, F.col("href"))
            .withColumn(
                "_m",
                _fetch("theme", "type", "href", "asset_bbox", "release"),
            )
            .select("_m.*")
            .withColumn("path", F.col("source"))
            .withColumn("last_update", F.current_timestamp())
            .select(
                "theme",
                "type",
                "source",
                "path",
                "out_file_sz",
                "is_out_file_valid",
                "last_update",
                "asset_bbox",
                "release",
                "href",
            )
        )
        return fetched

    def _merge_metadata(self, meta_df, table):
        """Create or UPSERT the metadata Delta table keyed by (theme, type, source)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if not spark.catalog.tableExists(table):
            meta_df.write.format("delta").mode("overwrite").saveAsTable(table)
            return meta_df

        from delta.tables import DeltaTable

        dt = DeltaTable.forName(spark, table)
        (
            dt.alias("t")
            .merge(
                meta_df.alias("u"),
                "t.theme = u.theme AND t.type = u.type AND t.source = u.source",
            )
            .whenMatchedUpdate(
                set={
                    "path": "u.path",
                    "out_file_sz": "u.out_file_sz",
                    "is_out_file_valid": "u.is_out_file_valid",
                    "last_update": "u.last_update",
                    "asset_bbox": "u.asset_bbox",
                    "release": "u.release",
                    "href": "u.href",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        return meta_df

    _CLOUD_SCHEMES = ("s3://", "s3a://", "abfs://", "abfss://", "gs://", "wasbs://")

    def download(
        self,
        assets_df,
        out_dir,
        *,
        bbox=None,
        table=None,
        validate=True,
        max_tries=5,
        partitions=None,
    ) -> "DataFrame":
        """Distributed download of discovered assets to out_dir (a Volume).

        Cloud predicate — routes to the distributed read path when ALL hrefs:
        - start with a cloud object-store scheme (``s3://``, ``s3a://``, ``abfs://``,
          ``abfss://``, ``gs://``, ``wasbs://``), OR
        - start with ``/`` (FUSE-mounted UC Volumes such as ``/Volumes/...``, which
          Spark can read directly without HTTP).

        Any other scheme (``http://``, ``https://``, etc.) falls through to the
        whole-file HTTP download fallback (``_download_fallback``).

        When ``table=<name>`` is set, the per-asset metadata DataFrame is
        persisted to a Delta table via idempotent MERGE keyed by
        ``(theme, type, source)``: the first call creates the table; subsequent
        calls update the volatile columns (``path``, ``out_file_sz``,
        ``is_out_file_valid``, ``last_update``, ``asset_bbox``, ``release``,
        ``href``) so the catalog stays queryable and re-runnable without
        accumulating duplicate rows. The returned DataFrame is unchanged.

        Serverless-safe; idempotent skip on valid existing targets.
        """
        assets = assets_df.select(*_DISCOVER_COLS)
        n = partitions if partitions is not None else max(1, assets.count())

        hrefs = [r["href"] for r in assets.select("href").distinct().collect()]
        is_cloud = bool(hrefs) and all(
            any(h.startswith(s) for s in self._CLOUD_SCHEMES)
            or h.startswith("/")  # local Volume / FUSE path is Spark-readable
            for h in hrefs
        )

        if is_cloud:
            meta = self._download_distributed(
                assets, out_dir, bbox=bbox, validate=validate, partitions=n
            )
        else:
            meta = self._download_fallback(
                assets, out_dir, validate=validate, max_tries=max_tries, partitions=n
            )

        if table is not None:
            meta = self._merge_metadata(meta, table)
        return meta

    def discover(self, bbox, themes=None, release=None) -> "DataFrame":
        """One row per intersecting GeoParquet asset for the AOI.

        Columns: theme, type, href, asset_bbox, release. themes=None => ALL.
        Driver-side + metadata-only (lightweight); asset I/O happens in download().
        """
        from pyspark.sql import SparkSession

        from databricks.labs.gbx.sample._overture_discover import (
            cli_discover,
            expand_themes,
            resolve_release,
            traverse_catalog,
        )

        opener = self._opener()
        rel = resolve_release(opener, release or self.release)
        pairs = expand_themes(themes)

        rows = None
        # CLI fast-path only in production (no injected opener); offline tests force traversal.
        if self._catalog_opener is None:
            rows = cli_discover(bbox, pairs, rel)
        if not rows:
            rows = traverse_catalog(opener, bbox, pairs)

        for r in rows:
            r["release"] = rel

        spark = SparkSession.getActiveSession()
        schema = _discover_schema()
        if not rows:
            return spark.createDataFrame([], schema)
        ordered = [tuple(r[c] for c in _DISCOVER_COLS) for r in rows]
        return spark.createDataFrame(ordered, schema)
