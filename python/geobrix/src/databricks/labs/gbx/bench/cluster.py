"""Cluster-side helpers: persist bench rows to a Delta table + build the bench notebook."""

from __future__ import annotations

from dataclasses import asdict, replace
from typing import List

from databricks.labs.gbx.bench.results import ResultRow


def rows_to_dataframe(rows: List[ResultRow], spark, where: str = "cluster"):
    """Build a Spark DataFrame from ResultRows, re-tagging env_where (e.g. 'cluster')."""
    import pandas as pd

    retagged = [replace(r, env_where=where) for r in rows]
    return spark.createDataFrame(pd.DataFrame([asdict(r) for r in retagged]))


def to_delta(rows: List[ResultRow], spark, table: str, where: str = "cluster") -> int:
    """Append ResultRows to the bench_results Delta table. Returns row count. (Cluster-only.)"""
    if not rows:
        return 0
    df = rows_to_dataframe(rows, spark, where=where)
    df.write.format("delta").mode("append").saveAsTable(table)
    return len(rows)
