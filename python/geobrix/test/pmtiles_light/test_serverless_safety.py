"""The light pmtiles_agg module must be Serverless/Connect-safe."""

import ast
import inspect

from databricks.labs.gbx.pmtiles import _agg_light

_FORBIDDEN = ("_jvm", "sparkContext", ".rdd", "spark.conf.set", "_jsc")


def _code_tokens(module) -> str:
    """Return source with all string literals (docstrings/comments) stripped."""
    src = inspect.getsource(module)
    tree = ast.parse(src)
    # Remove all string-constant nodes (docstrings live as Expr(Constant(str)))
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            node.value.value = ""
    # Reconstruct from AST — use the raw source minus comment lines instead
    lines = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        lines.append(line)
    code_only = "\n".join(lines)
    # Also blank out triple-quoted docstrings via AST unparsing of non-string nodes
    return code_only


def test_no_spark_internal_access():
    src = inspect.getsource(_agg_light)
    # Strip the module docstring (first triple-quoted block) before checking.
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
