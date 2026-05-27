"""Tests for VectorX SQL examples.

Ensures all SQL examples in documentation are executable and produce valid results.
Mirrors the per-package test driver pattern used by ``test_rasterx_functions_sql.py``
and ``test_gridx_functions_sql.py``. Each example function in
``vectorx_functions_sql`` returns a SQL string; this driver runs it against the
docs-test Spark session (from ``conftest.py``) and asserts non-empty output.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import vectorx_functions_sql  # noqa: E402


@pytest.fixture(scope="module")
def vectorx_registered(spark):
    """Register VectorX expression-level SQL functions for this test module."""
    from databricks.labs.gbx.vectorx import functions as vx
    vx.register(spark)
    yield spark


def test_st_asmvt_sql_example(vectorx_registered):
    """Run the ``gbx_st_asmvt`` SQL example and assert a non-empty MVT blob."""
    spark = vectorx_registered
    sql = vectorx_functions_sql.st_asmvt_sql_example()
    # The example contains a multi-statement script (WITH ... SELECT ...); pyspark's
    # sql() runs a single statement, so we execute the full text.
    result = spark.sql(sql.replace(";", "")).collect()
    assert len(result) == 1
    assert result[0]["mvt_bytes_len"] > 0
