"""The light pmtiles_agg module must be Serverless/Connect-safe."""

import ast
import inspect

from databricks.labs.gbx.pmtiles import _agg_light

_FORBIDDEN = ("_jvm", "sparkContext", ".rdd", "spark.conf.set", "_jsc")


def test_no_spark_internal_access():
    src = inspect.getsource(_agg_light)
    # Strip docstrings (the module docstring legitimately names _jvm/spark.conf
    # in its "Serverless-safe" note) before scanning the real code for forbidden
    # Spark-internal access.
    tree = ast.parse(src)
    docstring_ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ) and isinstance(ast.get_docstring(node), str):
            body0 = node.body[0]
            docstring_ranges.append((body0.lineno, body0.end_lineno))

    src_lines = src.splitlines()
    filtered = []
    for i, line in enumerate(src_lines, start=1):
        in_docstring = any(start <= i <= end for start, end in docstring_ranges)
        if in_docstring:
            continue
        filtered.append(line)
    code_src = "\n".join(filtered)

    for bad in _FORBIDDEN:
        assert (
            bad not in code_src
        ), f"Serverless-unsafe access: {bad!r} found in non-docstring source"
