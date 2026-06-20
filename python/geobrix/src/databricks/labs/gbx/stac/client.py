"""StacClient — catalog-agnostic, Serverless-safe STAC search/download/repair.

Parallelism is via DataFrame.repartition(N) (NOT spark.conf, which is a no-op on
Serverless). No caching or persistence calls — Serverless-safe. Asset download is
resilient (read-validated + retried). The catalog opener is injectable
(_catalog_opener) for unit tests.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"


def _spark_types():
    from pyspark.sql.types import (
        ArrayType, DoubleType, MapType, StringType, StructField, StructType,
    )
    asset_schema = ArrayType(StructType([
        StructField("asset_name", StringType()),
        StructField("href", StringType()),
    ]))
    item_schema = StructType([
        StructField("item_id", StringType()),
        StructField("date", StringType()),
        StructField("item_bbox", ArrayType(DoubleType())),
        StructField("item_properties", MapType(StringType(), StringType())),
    ])
    return asset_schema, item_schema


def _search_driver(catalog, df: "DataFrame", geojson_col: str, collections: List[str],
                   datetime: str) -> "DataFrame":
    """Run search on the driver (for test injection; no UDF pickling required)."""
    from pyspark.sql import Row
    from databricks.labs.gbx.stac._search import search_one, parse_item, extract_assets

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
                row_dict.update({
                    "item_id": p["item_id"],
                    "date": p["date"],
                    "item_bbox": p["item_bbox"],
                    "item_properties": props,
                    "asset_name": asset["asset_name"],
                    "href": asset["href"],
                })
                rows.append(row_dict)

    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if not rows:
        from pyspark.sql.types import ArrayType, DoubleType, MapType, StringType, StructField, StructType
        _ASSET_SCHEMA, _ITEM_SCHEMA = _spark_types()
        schema = StructType(
            [StructField(c, df.schema[c].dataType) for c in carried] + [
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
    def __init__(self, catalog=PLANETARY_COMPUTER, sign="planetary_computer", _catalog_opener=None):
        self.catalog = catalog
        self.sign = sign
        self._catalog_opener = _catalog_opener

    def _open_catalog(self):
        if self._catalog_opener is not None:
            return self._catalog_opener()
        import pystac_client
        from databricks.labs.gbx.stac._sign import resolve_modifier

        return pystac_client.Client.open(self.catalog, modifier=resolve_modifier(self.sign))

    def search(self, df: "DataFrame", geojson_col: str, collections: List[str],
               datetime: str, partitions: int = 512) -> "DataFrame":
        # When a catalog opener is injected (test mode) run on the driver to avoid
        # pickling test-local callables into the Spark UDF worker.
        if self._catalog_opener is not None:
            return _search_driver(self._catalog_opener(), df, geojson_col, collections, datetime)

        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, StringType

        _ASSET_SCHEMA, _ITEM_SCHEMA = _spark_types()
        catalog_url, sign = self.catalog, self.sign

        @F.udf(ArrayType(StringType()))
        def _items(geojson):
            from databricks.labs.gbx.stac._search import search_one
            from databricks.labs.gbx.stac._sign import resolve_modifier
            import pystac_client
            cat = pystac_client.Client.open(catalog_url, modifier=resolve_modifier(sign))
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
            df.repartition(partitions)
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
        )

    def download(self, df: "DataFrame", out_dir: str, asset_names: Optional[List[str]] = None,
                 name: str = "{asset_name}_{item_id}.tif", validate: bool = True,
                 max_tries: int = 5, partitions: Optional[int] = None) -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        if asset_names:
            df = df.filter(F.col("asset_name").isin(list(asset_names)))
        targets = df.select("item_id", "asset_name").distinct()
        n = partitions if partitions is not None else max(1, targets.count())
        catalog_url, sign = self.catalog, self.sign

        @F.udf(StringType())
        def _fetch(item_id, asset_name):
            from databricks.labs.gbx.stac._download import fetch_validate_publish
            from databricks.labs.gbx.stac._sign import resolve_modifier, resolve_signer
            import pystac_client
            cat = pystac_client.Client.open(catalog_url, modifier=resolve_modifier(sign))
            signer = resolve_signer(sign)

            def href_fn():
                item = cat.get_item(item_id)
                return signer(item.assets[asset_name].href)

            filename = name.format(asset_name=asset_name, item_id=item_id)
            return fetch_validate_publish(href_fn, out_dir, filename, max_tries=max_tries)

        @F.udf("long")
        def _size(path):
            import os
            return os.path.getsize(path) if path and os.path.exists(path) else None

        return (
            targets.repartition(n)
              .withColumn("out_file_path", _fetch("item_id", "asset_name"))
              .withColumn("out_file_sz", _size("out_file_path"))
              .withColumn("is_out_file_valid", F.col("out_file_path").isNotNull())
        )

    def repair(self, target, where: str = "is_out_file_valid = false",
               spark=None, out_dir: Optional[str] = None) -> "DataFrame":
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        is_table = isinstance(target, str)
        df = spark.table(target) if is_table else target
        invalid = df.filter(where)
        repaired = self.download(
            invalid.select("item_id", "asset_name"),
            out_dir or _common_dir(invalid),
        )
        if is_table:
            from delta.tables import DeltaTable

            dt = DeltaTable.forName(spark, target)
            (dt.alias("t").merge(
                repaired.alias("u"),
                "t.item_id = u.item_id AND t.asset_name = u.asset_name")
             .whenMatchedUpdate(set={
                 "out_file_path": "u.out_file_path",
                 "out_file_sz": "u.out_file_sz",
                 "is_out_file_valid": "u.is_out_file_valid",
             }).execute())
        return repaired


def _common_dir(df: "DataFrame") -> str:
    """Infer the output dir from existing out_file_path values (repair convenience)."""
    import os
    from pyspark.sql import functions as F

    row = df.filter(F.col("out_file_path").isNotNull()).select("out_file_path").first()
    if row is None:
        raise ValueError("repair: cannot infer out_dir; pass out_dir=...")
    return os.path.dirname(row["out_file_path"])
