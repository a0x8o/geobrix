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
