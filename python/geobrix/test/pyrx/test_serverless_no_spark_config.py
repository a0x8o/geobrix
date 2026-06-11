"""Serverless-compatibility guard for the pyrx (lightweight) tier.

Serverless / Spark Connect FORBIDS mutating Spark configuration at runtime
(`spark.conf.set(...)`) and does not expose the JVM bridge (`spark._jvm`,
`sparkContext`, `.rdd`). Serverless is a target environment for pyrx, so the
pyrx PRODUCT must never do any of those — it may only register UDFs and build
Column expressions. (The bench *harness* under `gbx.bench` legitimately tunes
configs from a repo checkout; it is NOT shipped/run as the product, so it is
out of scope here.)

This is a static source guard: if someone adds a forbidden call to pyrx, this
test fails with the exact file:line, before it ships and breaks on Serverless.
"""

import re
from pathlib import Path

import databricks.labs.gbx.pyrx as pyrx

# Each pattern is a runtime Spark-config mutation or a JVM-bridge access that
# Serverless / Spark Connect rejects. `getOrCreate`, `getActiveSession`, and
# `spark.udf.register` are NOT forbidden — those are Connect-compatible.
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


def _pyrx_source_files():
    root = Path(pyrx.__file__).resolve().parent
    for p in root.rglob("*.py"):
        if "__pycache__" in p.parts or p.name == Path(__file__).name:
            continue
        yield p


def test_pyrx_never_mutates_spark_config_or_uses_jvm_bridge():
    violations = []
    for path in _pyrx_source_files():
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("#", 1)[0]  # ignore comments/docstring-prose mentions
            for label, pat in _FORBIDDEN.items():
                if pat.search(code):
                    violations.append(f"{path.name}:{i} [{label}] -> {line.strip()}")
    assert not violations, (
        "pyrx must be Serverless-safe (no Spark config mutation / JVM bridge). "
        "Found:\n  " + "\n  ".join(violations)
    )
