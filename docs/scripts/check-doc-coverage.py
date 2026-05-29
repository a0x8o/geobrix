#!/usr/bin/env python3
"""Verify doc coverage for every registered GeoBrix function.

Two checks run together:

  D2 — every registered function is documented on its Functions page
       (docs/docs/api/{rasterx,gridx,vectorx,pmtiles}-functions.mdx).

  D3 — no registered function has a placeholder-only example output
       constant in its SQL example file.  A placeholder is a table whose
       only data rows consist solely of ``...`` and/or empty cells with
       no real values anywhere (no [BINARY], no [GTiff, no <WKB, no real
       numbers, no WKT, etc.).  A data row that contains at least one
       non-empty, non-dot cell is not a placeholder even if other cells
       are dots.

Exit code: 0 when both checks pass, 1 if either finds problems.

Run directly on the host — pure stdlib, no Docker needed.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"

# prefix → docs page
PAGE_MAP: dict[str, Path] = {
    "gbx_rst_": REPO_ROOT / "docs/docs/api/rasterx-functions.mdx",
    "gbx_bng_": REPO_ROOT / "docs/docs/api/gridx-functions.mdx",
    "gbx_quadbin_": REPO_ROOT / "docs/docs/api/gridx-functions.mdx",
    "gbx_custom_": REPO_ROOT / "docs/docs/api/gridx-functions.mdx",
    "gbx_st_": REPO_ROOT / "docs/docs/api/vectorx-functions.mdx",
    "gbx_pmtiles_": REPO_ROOT / "docs/docs/api/pmtiles-functions.mdx",
}

# prefix → SQL example file
SQL_FILE_MAP: dict[str, Path] = {
    "gbx_rst_": REPO_ROOT / "docs/tests/python/api/rasterx_functions_sql.py",
    "gbx_bng_": REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    "gbx_quadbin_": REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    "gbx_custom_": REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    "gbx_st_": REPO_ROOT / "docs/tests/python/api/vectorx_functions_sql.py",
    "gbx_pmtiles_": REPO_ROOT / "docs/tests/python/api/pmtiles_functions_sql.py",
}

# Regex to find _sql_example_output constants and their triple-quoted values
OUTPUT_CONST_RE = re.compile(
    r'(\w+_sql_example_output)\s*=\s*"""(.*?)"""',
    re.DOTALL,
)

# Cells that signal a real (non-placeholder) value even when surrounded by dots
REAL_CELL_RE = re.compile(
    r"""
    \[BINARY\]          # binary tile descriptor
    | \[GTiff           # GTiff variant
    | <WKB              # WKB prefix
    | POLYGON\s*\(      # WKT polygon
    | POINT\s*\(        # WKT point
    | LINESTRING\s*\(   # WKT linestring
    | \{                # JSON / struct
    | \[<digit          # array of digit
    | \[\d              # array starting with digit
    | \[STRUCT          # STRUCT array abbreviation
    """,
    re.VERBOSE | re.IGNORECASE,
)


def canonical_sql() -> list[str]:
    names = []
    for line in REGISTERED_TXT.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.append(line)
    return names


def page_for(name: str) -> Path | None:
    for prefix, page in PAGE_MAP.items():
        if name.startswith(prefix):
            return page
    return None


def sql_file_for(name: str) -> Path | None:
    for prefix, sql_file in SQL_FILE_MAP.items():
        if name.startswith(prefix):
            return sql_file
    return None


def bare_name(gbx_name: str) -> str:
    """Strip the leading 'gbx_' prefix: 'gbx_rst_foo' → 'rst_foo'."""
    return gbx_name[len("gbx_"):]


def is_documented(name: str, page_text: str) -> bool:
    """Return True if *name* appears in *page_text* in any recognised form.

    Accepted matches (all case-sensitive substring):
      1. The full SQL name:         gbx_rst_foo
      2. The bare name:             rst_foo  (used in ### headings)
      3. functionName="<bare>_sql_example"
      4. outputConstant="<bare>_sql_example_output"
    """
    full = name                   # e.g. gbx_rst_foo
    bare = bare_name(name)        # e.g. rst_foo

    if full in page_text:
        return True
    if bare in page_text:
        return True
    if f'functionName="{bare}_sql_example"' in page_text:
        return True
    if f'outputConstant="{bare}_sql_example_output"' in page_text:
        return True
    return False


def _all_output_constants(path: Path) -> dict[str, str]:
    """Return {constant_name: value} for every _sql_example_output in *path*."""
    text = path.read_text()
    return {m.group(1): m.group(2) for m in OUTPUT_CONST_RE.finditer(text)}


def _is_placeholder_output(value: str) -> bool:
    """Return True if *value* is a placeholder-only table.

    A placeholder is a table whose data rows (rows after the header / first
    separator pair) ALL consist solely of ``...`` and/or whitespace/empty cells
    and contain no recognisably real value.

    Table structure:
        +---+      <- separator 0
        |col|      <- header row(s)
        +---+      <- separator 1
        |val|      <- data rows  <-- these are what we inspect
        +---+

    Non-table strings (no ``+---+``/``|`` structure) are also flagged if they
    are just a column label or ``...``.
    """
    stripped = value.strip()
    if not stripped:
        return False

    lines = stripped.splitlines()
    sep_indices = [i for i, l in enumerate(lines) if l.strip().startswith("+")]
    has_table = len(sep_indices) >= 2

    if not has_table:
        # Bare (non-table) string: flag if it's just '...' or a single word
        # column label with no numerics / WKT / etc.
        single = stripped.replace("...", "").replace("|", "").strip()
        if not single or single.isidentifier():
            return True
        return False

    # Data rows are pipe-rows that come AFTER the second separator line.
    # (Rows between separator 0 and separator 1 are header/column-name rows.)
    second_sep = sep_indices[1]
    data_rows = [
        l for i, l in enumerate(lines)
        if i > second_sep and l.strip().startswith("|") and not l.strip().startswith("+")
    ]
    if not data_rows:
        return False

    def row_has_real_value(row: str) -> bool:
        cells = [c.strip() for c in row.strip("|").split("|")]
        for cell in cells:
            if not cell or cell == "...":
                continue
            # Check for real-value patterns
            if REAL_CELL_RE.search(cell):
                return True
            # Any cell that is not empty, not pure dots, and contains
            # something other than '...' is considered real
            cleaned = cell.replace("...", "").strip()
            if cleaned:
                return True
        return False

    # The output is a placeholder only if NO data row has a real value
    return not any(row_has_real_value(r) for r in data_rows)


# ---------------------------------------------------------------------------
# Check D2 — every registered function is documented on its page
# ---------------------------------------------------------------------------

def check_d2(names: list[str]) -> tuple[bool, str]:
    """Return (passed, report_text)."""
    # Pre-load page texts (one read per unique page)
    page_texts: dict[Path, str] = {}
    for page in set(PAGE_MAP.values()):
        if page.exists():
            page_texts[page] = page.read_text()
        else:
            page_texts[page] = ""

    # Group missing by page
    missing_by_page: dict[Path, list[str]] = {}
    for name in names:
        page = page_for(name)
        if page is None:
            continue
        text = page_texts.get(page, "")
        if not is_documented(name, text):
            missing_by_page.setdefault(page, []).append(name)

    if not missing_by_page:
        total = len(names)
        return True, f"✅ D2 doc-coverage OK — all {total} registered functions are documented on their pages."

    lines = ["❌ D2 doc-coverage FAILED — registered functions with no documentation on their mapped page:"]
    for page in sorted(str(p) for p in missing_by_page):
        page_path = Path(page)
        rel = page_path.relative_to(REPO_ROOT)
        lines.append(f"\n  {rel}:")
        for fn in missing_by_page[page_path]:
            lines.append(f"     - {fn}")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D3 — no placeholder-only example outputs
# ---------------------------------------------------------------------------

def check_d3(names: list[str]) -> tuple[bool, str]:
    """Return (passed, report_text)."""
    # Pre-load SQL files
    sql_texts: dict[Path, dict[str, str]] = {}
    for sql_file in set(SQL_FILE_MAP.values()):
        if sql_file.exists():
            sql_texts[sql_file] = _all_output_constants(sql_file)
        else:
            sql_texts[sql_file] = {}

    placeholders: list[str] = []
    for name in names:
        sql_file = sql_file_for(name)
        if sql_file is None:
            continue
        constants = sql_texts.get(sql_file, {})
        bare = bare_name(name)  # e.g. rst_foo
        const_name = f"{bare}_sql_example_output"
        if const_name not in constants:
            # No output constant at all — out of scope for this check
            continue
        value = constants[const_name]
        if _is_placeholder_output(value):
            placeholders.append(name)

    if not placeholders:
        return True, "✅ D3 placeholder-output OK — no registered function has a placeholder-only example output."

    lines = ["❌ D3 placeholder-output FAILED — registered functions with placeholder-only example outputs:"]
    for fn in placeholders:
        lines.append(f"     - {fn}")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    if not REGISTERED_TXT.exists():
        print(f"❌ missing required file: {REGISTERED_TXT}", file=sys.stderr)
        return 1

    names = canonical_sql()
    print(f"Canonical registered functions: {len(names)}")
    print()

    d2_ok, d2_report = check_d2(names)
    d3_ok, d3_report = check_d3(names)

    print(d2_report)
    print()
    print(d3_report)
    print()

    if d2_ok and d3_ok:
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
