"""Serverless-compatibility guard for the light tier (pyrx + gbx.ds).

Serverless / Spark Connect FORBIDS mutating Spark configuration at runtime
(`spark.conf.set(...)`) and does not expose the JVM bridge (`spark._jvm`,
`sparkContext`, `.rdd`). Serverless is a target environment for the light
product, so the pyrx PRODUCT and the gbx.ds DataSources must never do any of
those — they may only register UDFs / DataSources and build Column expressions.
(The bench *harness* under `gbx.bench` legitimately tunes configs from a repo
checkout; it is NOT shipped/run as the product, so it is out of scope here.)

This is a static source guard: if someone adds a forbidden call, this test
fails with the exact file:line, before it ships and breaks on Serverless.
"""

import re
from pathlib import Path

import databricks.labs.gbx.ds as gbx_ds
import databricks.labs.gbx.pyrx as pyrx

# Each pattern is a runtime Spark-config mutation or a JVM-bridge access that
# Serverless / Spark Connect rejects. `getOrCreate`, `getActiveSession`, and
# `spark.udf.register` / `spark.dataSource.register` are NOT forbidden.
_FORBIDDEN = {
    "spark config mutation": re.compile(r"\.conf\.set\s*\("),
    "SparkConf": re.compile(r"\bSparkConf\b"),
    "setConf": re.compile(r"\.setConf\s*\("),
    "setSystemProperty": re.compile(r"\bsetSystemProperty\b"),
    "JVM bridge (_jvm)": re.compile(r"\._jvm\b"),
    "JVM bridge (_jsc)": re.compile(r"\._jsc\b"),
    "sparkContext access": re.compile(r"\.sparkContext\b"),
    "RDD API": re.compile(r"\.rdd\b"),
}

_ROOTS = (
    Path(pyrx.__file__).resolve().parent,
    Path(gbx_ds.__file__).resolve().parent,
)


def _source_files():
    for root in _ROOTS:
        for p in root.rglob("*.py"):
            if "__pycache__" in p.parts or p.name == Path(__file__).name:
                continue
            yield p


def test_light_product_never_mutates_spark_config_or_uses_jvm_bridge():
    violations = []
    for path in _source_files():
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("#", 1)[0]  # ignore comments/docstring-prose mentions
            for label, pat in _FORBIDDEN.items():
                if pat.search(code):
                    violations.append(f"{path.name}:{i} [{label}] -> {line.strip()}")
    assert not violations, (
        "light product code must be Serverless-safe (no Spark config mutation / "
        "JVM bridge). Found:\n  " + "\n  ".join(violations)
    )


def test_serverless_scan_includes_ds_modules():
    """The migrated DataSource modules must be in scope of the scan."""
    files = {p.name for p in _source_files()}
    for required in (
        "raster.py",
        "gtiff.py",
        "writer.py",
        "_write.py",
        "register.py",
        "_encode.py",
        "_listing.py",
        "pmtiles.py",
        "grid.py",
        "_header.py",
        "backend.py",
        "catalog.py",
        "shard.py",
        "vector.py",
    ):
        assert required in files, f"{required} not covered by Serverless scan"
