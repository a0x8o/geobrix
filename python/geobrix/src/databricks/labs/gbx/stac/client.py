"""StacClient — catalog-agnostic, Serverless-safe STAC search/download/repair.

Parallelism is via DataFrame.repartition(N, col) — hash by a column, since a
number-only repartition(N) is AQE-coalesced toward 1 partition on Serverless (NOT
spark.conf, which is a no-op there). No caching or persistence calls — Serverless-safe.
Asset download is
resilient (read-validated + retried). The catalog opener is injectable
(_catalog_opener) for unit tests.

Note on item_properties: values are stringified into a MapType(String, String).
Downstream numeric filters must cast (e.g. ``CAST(item_properties['eo:cloud_cover']
AS DOUBLE) < 20``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"


def _spark_types():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    asset_schema = ArrayType(
        StructType(
            [
                StructField("asset_name", StringType()),
                StructField("href", StringType()),
            ]
        )
    )
    item_schema = StructType(
        [
            StructField("item_id", StringType()),
            StructField("date", StringType()),
            StructField("item_bbox", ArrayType(DoubleType())),
            StructField("item_properties", MapType(StringType(), StringType())),
        ]
    )
    return asset_schema, item_schema


def _search_driver(
    catalog, df: "DataFrame", geojson_col: str, collections: List[str], datetime: str
) -> "DataFrame":
    """Run search on the driver (for test injection; no UDF pickling required)."""
    from databricks.labs.gbx.stac._search import extract_assets, parse_item, search_one

    rows = []
    carried = [c for c in df.columns if c != geojson_col]
    for r in df.collect():
        geojson = r[geojson_col]
        items = search_one(catalog, list(collections), datetime, geojson)
        for item_json in items:
            p = parse_item(item_json)
            props = {k: str(v) for k, v in (p["item_properties"] or {}).items()}
            for asset in extract_assets(item_json):
                row_dict = {c: r[c] for c in carried}
                row_dict.update(
                    {
                        "item_id": p["item_id"],
                        "date": p["date"],
                        "item_bbox": p["item_bbox"],
                        "item_properties": props,
                        "asset_name": asset["asset_name"],
                        "href": asset["href"],
                    }
                )
                rows.append(row_dict)

    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    spark = SparkSession.getActiveSession()
    if not rows:
        schema = StructType(
            [StructField(c, df.schema[c].dataType) for c in carried]
            + [
                StructField("item_id", StringType()),
                StructField("date", StringType()),
                StructField("item_bbox", ArrayType(DoubleType())),
                StructField("item_properties", MapType(StringType(), StringType())),
                StructField("asset_name", StringType()),
                StructField("href", StringType()),
            ]
        )
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows)


class StacClient:
    def __init__(
        self,
        catalog=PLANETARY_COMPUTER,
        sign="planetary_computer",
        _catalog_opener=None,
    ):
        self.catalog = catalog
        self.sign = sign
        self._catalog_opener = _catalog_opener

    def _open_catalog(self):
        if self._catalog_opener is not None:
            return self._catalog_opener()
        import pystac_client

        from databricks.labs.gbx.stac._sign import resolve_modifier

        return pystac_client.Client.open(
            self.catalog, modifier=resolve_modifier(self.sign)
        )

    def search(
        self,
        df: "DataFrame",
        geojson_col: str,
        collections: List[str],
        datetime: str,
        partitions: int = 512,
    ) -> "DataFrame":
        """Search a STAC catalog for all AOIs in df[geojson_col].

        Returns one row per (AOI, item, asset). Carried columns (all columns except
        geojson_col) are preserved. Duplicate (item_id, asset_name) rows that arise
        when multiple AOIs overlap the same item are collapsed by .distinct().

        item_properties values are stringified to MapType(String, String); downstream
        numeric filters must cast (e.g. CAST(item_properties['eo:cloud_cover'] AS DOUBLE)).
        """
        # When a catalog opener is injected (test mode) run on the driver to avoid
        # pickling test-local callables into the Spark UDF worker.
        if self._catalog_opener is not None:
            return _search_driver(
                self._catalog_opener(), df, geojson_col, collections, datetime
            )

        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, StringType

        _ASSET_SCHEMA, _ITEM_SCHEMA = _spark_types()
        catalog_url = (
            self.catalog
        )  # search stores raw hrefs (no modifier); signing is at download

        @F.udf(ArrayType(StringType()))
        def _items(geojson):
            import pystac_client

            from databricks.labs.gbx.stac._search import search_one

            # Open without modifier so asset hrefs are stored raw (unsigned).
            # Signing happens once in _fetch at download time via resolve_signer.
            cat = pystac_client.Client.open(catalog_url)
            return search_one(cat, list(collections), datetime, geojson)

        @F.udf(_ASSET_SCHEMA)
        def _assets(item_json):
            from databricks.labs.gbx.stac._search import extract_assets

            return [(a["asset_name"], a["href"]) for a in extract_assets(item_json)]

        @F.udf(_ITEM_SCHEMA)
        def _item_fields(item_json):
            from databricks.labs.gbx.stac._search import parse_item

            p = parse_item(item_json)
            props = {k: str(v) for k, v in (p["item_properties"] or {}).items()}
            return (p["item_id"], p["date"], p["item_bbox"], props)

        carried = [c for c in df.columns if c != geojson_col]
        return (
            # Hash-by-column repartition (NOT number-only): on Serverless a round-robin
            # repartition(N) is AQE-coalesced back toward 1 partition (serial); repartition by
            # the AOI geometry column is respected and spreads ~one search task per AOI.
            df.repartition(partitions, F.col(geojson_col))
            .withColumn("_item", F.explode(_items(F.col(geojson_col))))
            .withColumn("_f", _item_fields("_item"))
            .withColumn("_a", F.explode(_assets("_item")))
            .select(
                *carried,
                F.col("_f.item_id").alias("item_id"),
                F.col("_f.date").alias("date"),
                F.col("_f.item_bbox").alias("item_bbox"),
                F.col("_f.item_properties").alias("item_properties"),
                F.col("_a.asset_name").alias("asset_name"),
                F.col("_a.href").alias("href"),
            )
            .dropDuplicates(["item_id", "asset_name"])
        )

    def download(
        self,
        df: "DataFrame",
        out_dir: str,
        asset_names: Optional[List[str]] = None,
        name: str = "{asset_name}_{item_id}.tif",
        validate: bool = True,
        max_tries: int = 5,
        partitions: Optional[int] = None,
        _get_fn=None,
    ) -> "DataFrame":
        """Download STAC assets to out_dir.

        validate=True (default): publish only if rasterio can decode a window of the
            file; is_out_file_valid reflects decode success.
        validate=False: publish without rasterio decode; is_out_file_valid reflects
            download+publish success only.

        Already-valid files (idempotency): if the target path exists and passes the
        validity check, the asset is skipped (no re-download).

        Returns a DataFrame with columns: item_id, asset_name, out_file_path,
        out_file_sz, is_out_file_valid, last_update.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        if asset_names:
            df = df.filter(F.col("asset_name").isin(list(asset_names)))
        # Carry raw href so _fetch can sign it once (avoids get_item which requires
        # a collection parameter on the PC STAC API). Fail LOUDLY if it's missing —
        # silently null-filling it makes every download invalid with no explanation
        # (the exact symptom that hid this in eo-series nb02). The href comes from
        # StacClient.search output; band tables drop it, so download/repair off a band
        # table won't work — feed a cell_assets-derived df instead.
        missing = [c for c in ("item_id", "asset_name", "href") if c not in df.columns]
        if missing:
            raise ValueError(
                f"download() input is missing required column(s) {missing}; it needs "
                f"item_id, asset_name, href (the StacClient.search output). Got {df.columns}. "
                "Note: band tables carry band_name and no href — pass a cell_assets-derived "
                "DataFrame (or use build_band_table(force_rebuild=True))."
            )
        targets = df.select("item_id", "asset_name", "href").distinct()
        n = partitions if partitions is not None else max(1, targets.count())
        sign = self.sign
        _validate = validate
        _injected_get = _get_fn  # None in production; injectable for tests

        @F.udf(StringType())
        def _fetch(item_id, asset_name, href):
            from databricks.labs.gbx.stac._download import fetch_validate_publish
            from databricks.labs.gbx.stac._sign import resolve_signer

            signer = resolve_signer(sign)

            def href_fn():
                if not href:
                    raise ValueError(
                        f"no href available for item_id={item_id!r} asset={asset_name!r}"
                    )
                return signer(href)

            filename = name.format(asset_name=asset_name, item_id=item_id)
            kwargs = {} if _injected_get is None else {"get": _injected_get}
            return fetch_validate_publish(
                href_fn,
                out_dir,
                filename,
                max_tries=max_tries,
                validate=_validate,
                **kwargs,
            )

        @F.udf("long")
        def _size(path):
            import os

            return os.path.getsize(path) if path and os.path.exists(path) else None

        return (
            # Hash-by-column repartition (NOT number-only): on Serverless a round-robin
            # repartition(N) is AQE-coalesced back toward 1 partition (serial download).
            # (item_id, asset_name) is unique per target, so hashing by it spreads
            # downloads evenly across n.
            targets.repartition(n, F.col("item_id"), F.col("asset_name"))
            .withColumn("out_file_path", _fetch("item_id", "asset_name", "href"))
            .withColumn("out_file_sz", _size("out_file_path"))
            .withColumn("is_out_file_valid", F.col("out_file_path").isNotNull())
            .withColumn("last_update", F.current_timestamp())
        )

    def repair(
        self,
        target,
        where: str = "is_out_file_valid = false",
        spark=None,
        out_dir: Optional[str] = None,
    ) -> "DataFrame":
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        is_table = isinstance(target, str)
        df = spark.table(target) if is_table else target
        invalid = df.filter(where)
        # Fail loudly with an actionable message: repair re-downloads, which needs the raw
        # href to re-sign. A band table (band_name, no asset_name/href) can't be repaired —
        # rebuild it from cell_assets instead. (Previously this raised a bare UNRESOLVED_COLUMN.)
        missing = [
            c for c in ("item_id", "asset_name", "href") if c not in invalid.columns
        ]
        if missing:
            raise ValueError(
                f"repair() target is missing required column(s) {missing}; it needs "
                f"item_id, asset_name, href to re-download. Got {invalid.columns}. A band table "
                "carries band_name and no href and cannot be repaired directly — re-run "
                "build_band_table(force_rebuild=True) (which reads cell_assets), or call repair() "
                "on a cell_assets-derived table/DataFrame."
            )
        repaired = self.download(
            invalid.select("item_id", "asset_name", "href"),
            out_dir or _common_dir(invalid),
        )
        if is_table:
            from delta.tables import DeltaTable

            dt = DeltaTable.forName(spark, target)
            (
                dt.alias("t")
                .merge(
                    repaired.alias("u"),
                    "t.item_id = u.item_id AND t.asset_name = u.asset_name",
                )
                .whenMatchedUpdate(
                    set={
                        "out_file_path": "u.out_file_path",
                        "out_file_sz": "u.out_file_sz",
                        "is_out_file_valid": "u.is_out_file_valid",
                        "last_update": "u.last_update",
                    }
                )
                .execute()
            )
        return repaired


def _common_dir(df: "DataFrame") -> str:
    """Infer the output dir from existing out_file_path values (repair convenience)."""
    import os

    from pyspark.sql import functions as F

    row = df.filter(F.col("out_file_path").isNotNull()).select("out_file_path").first()
    if row is None:
        raise ValueError("repair: cannot infer out_dir; pass out_dir=...")
    return os.path.dirname(row["out_file_path"])
