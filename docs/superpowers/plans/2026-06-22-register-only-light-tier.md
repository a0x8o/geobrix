# register(only=[...]) — Selective SQL Registration (Light Tiers) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `only` parameter to the lightweight `register()` functions (`pyrx`, `pygx`, `pyvx`) so a session can register a subset of a tier's SQL functions instead of the whole set.

**Architecture:** A shared `_register.py` helper normalizes/validates requested names (case-insensitive, both `gbx_`/short forms) and runs a per-package "grouped registrar map" — an ordered list of `(guard_thunk, {sql_name: register_fn})` groups. `register(only=None)` registers everything in today's order; `register(only=[...])` registers exactly the requested names and runs a group's availability guard only when ≥1 of its functions is selected.

**Tech Stack:** Python 3.12, PySpark (`spark.udf.register` / `spark.udtf.register`), pytest. Pure-Python — no JAR, no Docker. Tests run via `gbx:test:python --path python/geobrix/test/<pkg>/`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-22-register-only-light-tier-design.md`.
- Scope is **light tiers only** (`pyrx`, `pygx`, `pyvx`). Do NOT touch heavy (`rasterx`/Scala/JAR) registration — heavy `only=` is a deferred follow-up.
- `only=None` (default) MUST be behavior-identical to today: every function registered, in the same order, with all availability guards run.
- `only=[]` registers nothing (no-op, no error).
- Name handling: strip + `.lower()`, then prepend `gbx_` if absent. Accept both SQL (`gbx_rst_slope`) and short (`rst_slope`, `RST_Slope`) forms.
- Unknown name (after normalization) → raise `ValueError` listing the offending name(s) with up to 3 `difflib` close matches. Never silently skip.
- A group's availability guard (`_env.assert_*_available()`) runs only if ≥1 of its functions is selected.
- New public param signature: `register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None`.
- One canonical name per function; no aliases. No emojis. Match surrounding code style (4-space indent, existing import ordering).

---

### Task 1: Shared `_register.py` helper (normalize + validate + run groups)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/_register.py`
- Test: `python/geobrix/test/test_register_helper.py`

**Interfaces:**
- Produces:
  - `normalize_name(name: str) -> str` — strip+lower, prepend `gbx_` if absent (SQL functions).
  - `normalize_datasource_name(name: str) -> str` — strip+lower, append `_gbx` if absent (DataSource format names: `raster` → `raster_gbx`).
  - `resolve_only(only: Iterable[str], valid: Iterable[str], normalizer: Callable[[str], str] = normalize_name) -> Set[str]` — normalize all (via `normalizer`) + validate against `valid`; raise `ValueError` on unknown.
  - `run_groups(groups: List[Tuple[Callable[[], None], Dict[str, Callable[[Any], None]]]], spark, only: Optional[Iterable[str]]) -> None` — register selected functions; run each group's guard only if it has a selected function. Validation uses the union of all group names (via the default `normalize_name`).

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/test_register_helper.py
"""Unit tests for the shared selective-registration helper."""
import pytest

from databricks.labs.gbx import _register


def test_normalize_name_short_and_full_and_case():
    assert _register.normalize_name("rst_slope") == "gbx_rst_slope"
    assert _register.normalize_name("gbx_rst_slope") == "gbx_rst_slope"
    assert _register.normalize_name("RST_Slope") == "gbx_rst_slope"
    assert _register.normalize_name("GBX_RST_Slope") == "gbx_rst_slope"
    assert _register.normalize_name("  BNG_Polyfill  ") == "gbx_bng_polyfill"


def test_normalize_datasource_name_suffix_and_case():
    assert _register.normalize_datasource_name("raster") == "raster_gbx"
    assert _register.normalize_datasource_name("raster_gbx") == "raster_gbx"
    assert _register.normalize_datasource_name("RASTER_GBX") == "raster_gbx"
    assert _register.normalize_datasource_name("  Shapefile  ") == "shapefile_gbx"


def test_resolve_only_with_datasource_normalizer():
    valid = {"raster_gbx", "gtiff_gbx", "shapefile_gbx"}
    assert _register.resolve_only(
        ["raster", "GTIFF_GBX"], valid, normalizer=_register.normalize_datasource_name
    ) == {"raster_gbx", "gtiff_gbx"}


def test_resolve_only_returns_canonical_subset():
    valid = {"gbx_rst_slope", "gbx_rst_clip", "gbx_rst_width"}
    assert _register.resolve_only(["rst_slope", "GBX_RST_Clip"], valid) == {
        "gbx_rst_slope",
        "gbx_rst_clip",
    }


def test_resolve_only_empty_returns_empty_set():
    assert _register.resolve_only([], {"gbx_rst_slope"}) == set()


def test_resolve_only_unknown_raises_with_name_and_suggestion():
    with pytest.raises(ValueError) as ei:
        _register.resolve_only(["rst_slpe"], {"gbx_rst_slope", "gbx_rst_clip"})
    msg = str(ei.value)
    assert "rst_slpe" in msg
    assert "gbx_rst_slope" in msg  # close-match suggestion


def test_run_groups_only_registers_selected_and_runs_only_their_guards():
    calls = {"guardA": 0, "guardB": 0}
    registered = []

    def guardA():
        calls["guardA"] += 1

    def guardB():
        calls["guardB"] += 1

    groups = [
        (guardA, {"gbx_a_one": lambda s: registered.append("a_one"),
                  "gbx_a_two": lambda s: registered.append("a_two")}),
        (guardB, {"gbx_b_one": lambda s: registered.append("b_one")}),
    ]
    _register.run_groups(groups, spark=None, only=["a_one"])
    assert registered == ["a_one"]
    assert calls == {"guardA": 1, "guardB": 0}  # guardB not run — no b fn selected


def test_run_groups_none_registers_all_and_runs_all_guards():
    calls = []
    registered = []
    groups = [
        (lambda: calls.append("gA"), {"gbx_a_one": lambda s: registered.append("a_one")}),
        (lambda: calls.append("gB"), {"gbx_b_one": lambda s: registered.append("b_one")}),
    ]
    _register.run_groups(groups, spark=None, only=None)
    assert registered == ["a_one", "b_one"]
    assert calls == ["gA", "gB"]


def test_run_groups_validates_against_union_of_groups():
    groups = [
        (lambda: None, {"gbx_a_one": lambda s: None}),
        (lambda: None, {"gbx_b_one": lambda s: None}),
    ]
    # b_one is valid (other group); typo is not
    with pytest.raises(ValueError):
        _register.run_groups(groups, spark=None, only=["a_one", "nope_x"])


def test_run_groups_empty_only_registers_nothing_and_no_guards():
    calls = []
    registered = []
    groups = [
        (lambda: calls.append("gA"), {"gbx_a_one": lambda s: registered.append("a_one")}),
    ]
    _register.run_groups(groups, spark=None, only=[])
    assert registered == []
    assert calls == []  # no function selected => guard not run
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/test_register_helper.py`
Expected: FAIL — `ModuleNotFoundError: ... _register` (module doesn't exist yet).

- [ ] **Step 3: Write the helper**

```python
# python/geobrix/src/databricks/labs/gbx/_register.py
"""Shared helpers for selective SQL registration: register(spark, only=[...]).

Used by the lightweight register() functions (pyrx, pygx, pyvx) so each can
register a subset of its gbx_* SQL functions. Names are case-insensitive and
accept either the short form (rst_slope) or the full SQL name (gbx_rst_slope).
"""
from __future__ import annotations

import difflib
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

# A group = (availability-guard thunk, {canonical_sql_name: register_fn(spark)}).
Group = Tuple[Callable[[], None], Dict[str, Callable[[Any], None]]]


def normalize_name(name: str) -> str:
    """Normalize one requested name to its canonical gbx_ SQL name.

    Strips whitespace, lowercases (SQL names are lowercase; Scala classes are
    CamelCase, so users may type RST_Slope / BNG_Polyfill), and prepends gbx_
    if absent. 'rst_slope', 'RST_Slope', 'gbx_rst_slope' -> 'gbx_rst_slope'.
    """
    n = name.strip().lower()
    return n if n.startswith("gbx_") else f"gbx_{n}"


def normalize_datasource_name(name: str) -> str:
    """Normalize one DataSource format name to its canonical form.

    DataSource formats use a `_gbx` suffix (not a `gbx_` prefix). Strips +
    lowercases, then appends `_gbx` if absent. 'raster', 'RASTER',
    'raster_gbx' -> 'raster_gbx'.
    """
    n = name.strip().lower()
    return n if n.endswith("_gbx") else f"{n}_gbx"


def resolve_only(
    only: Iterable[str],
    valid: Iterable[str],
    normalizer: Callable[[str], str] = normalize_name,
) -> Set[str]:
    """Normalize requested names (via `normalizer`) and validate against `valid`.

    Returns the set of canonical names to register. Raises ValueError that lists
    any name not matching a registerable target (after normalization), with up
    to 3 difflib close matches each.
    """
    valid_set = set(valid)
    requested = [(orig, normalizer(orig)) for orig in only]
    unknown = [(orig, norm) for orig, norm in requested if norm not in valid_set]
    if unknown:
        lines = []
        for orig, norm in unknown:
            matches = difflib.get_close_matches(norm, valid_set, n=3)
            hint = f" -> did you mean: {', '.join(matches)}?" if matches else ""
            lines.append(f"  {orig!r}{hint}")
        raise ValueError(
            "register(only=...) got unrecognized name(s):\n"
            + "\n".join(lines)
            + "\nPass a registerable name (or its short form) for this tier."
        )
    return {norm for _, norm in requested}


def run_groups(groups: List[Group], spark: Any, only: Optional[Iterable[str]]) -> None:
    """Register the selected functions across `groups`.

    only=None registers every function in every group (guards all run, in order).
    only=[...] registers exactly the named functions; a group's guard runs only
    when >=1 of its functions is selected. Validation is against the union of all
    group names.
    """
    all_names: Set[str] = set()
    for _guard, entries in groups:
        all_names |= set(entries)
    wanted = None if only is None else resolve_only(only, all_names)
    for guard, entries in groups:
        selected = [fn for name, fn in entries.items() if wanted is None or name in wanted]
        if not selected:
            continue
        guard()
        for fn in selected:
            fn(spark)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/test_register_helper.py`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/_register.py python/geobrix/test/test_register_helper.py
git commit -m "feat(register): shared only= normalize/validate/run-groups helper"
```

---

### Task 2: pyrx `register(only=[...])`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (the `register` function, ~lines 50-105)
- Test: `python/geobrix/test/pyrx/test_register_only.py`

**Interfaces:**
- Consumes: `_register.run_groups`.
- Produces: `pyrx.functions.register(spark=None, only=None)`; a module-level builder `_registrar_groups() -> List[Group]`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/pyrx/test_register_only.py
"""register(only=[...]) selective registration for the pyrx tier."""
import pytest

from databricks.labs.gbx.pyrx import functions as prx


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_registers_just_those(spark):
    for n in ("gbx_rst_slope", "gbx_rst_clip"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    prx.register(spark, only=["rst_slope"])
    assert _exists(spark, "gbx_rst_slope")
    assert not _exists(spark, "gbx_rst_clip")


def test_only_accepts_both_name_forms(spark):
    for n in ("gbx_rst_width", "gbx_rst_height"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    prx.register(spark, only=["gbx_rst_width", "RST_Height"])
    assert _exists(spark, "gbx_rst_width")
    assert _exists(spark, "gbx_rst_height")


def test_only_selects_udtf_and_pmtiles_agg(spark):
    for n in ("gbx_rst_retile", "gbx_pmtiles_agg"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    prx.register(spark, only=["gbx_rst_retile", "gbx_pmtiles_agg"])
    assert _exists(spark, "gbx_rst_retile")
    assert _exists(spark, "gbx_pmtiles_agg")


def test_only_unknown_name_raises(spark):
    with pytest.raises(ValueError) as ei:
        prx.register(spark, only=["rst_slpe"])
    assert "rst_slpe" in str(ei.value)


def test_only_none_registers_full_set(spark):
    prx.register(spark)
    for n in ("gbx_rst_width", "gbx_rst_slope", "gbx_rst_retile", "gbx_pmtiles_agg"):
        assert _exists(spark, n)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_register_only.py`
Expected: FAIL — `register()` rejects the `only` keyword (`TypeError: register() got an unexpected keyword argument 'only'`).

- [ ] **Step 3: Refactor `register` to the grouped registrar map**

Replace the current `register` (the `for name, udf_obj in SQL_REGISTRY.items(): ...` loop, the explicit `spark.udtf.register(...)` calls, and the trailing `register_pmtiles_agg(spark)`) with a builder + `run_groups`. The builder transcribes exactly the names registered today: every `SQL_REGISTRY` entry, the 14 UDTFs (verbatim names below), and `gbx_pmtiles_agg`.

```python
from typing import List, Optional

from databricks.labs.gbx import _register


def _registrar_groups() -> List[_register.Group]:
    """One group for pyrx (rasterio guard): scalar/agg UDFs (from SQL_REGISTRY),
    UDTFs, and the format-agnostic pmtiles aggregate. Insertion order matches the
    pre-only register() ordering so only=None is behavior-identical."""
    entries = {}
    for name, udf_obj in SQL_REGISTRY.items():
        entries[name] = lambda s, n=name, u=udf_obj: s.udf.register(n, u)

    udtfs = [
        ("gbx_rst_polygonize", _RstPolygonizeUDTF),
        ("gbx_rst_h3_rastertogridavg", _RstH3RasterToGridAvgUDTF),
        ("gbx_rst_h3_rastertogridcount", _RstH3RasterToGridCountUDTF),
        ("gbx_rst_h3_rastertogridmax", _RstH3RasterToGridMaxUDTF),
        ("gbx_rst_h3_rastertogridmin", _RstH3RasterToGridMinUDTF),
        ("gbx_rst_h3_rastertogridmedian", _RstH3RasterToGridMedianUDTF),
        ("gbx_rst_quadbin_rastertogridavg", _RstQuadbinRasterToGridAvgUDTF),
        ("gbx_rst_quadbin_rastertogridcount", _RstQuadbinRasterToGridCountUDTF),
        ("gbx_rst_quadbin_rastertogridmax", _RstQuadbinRasterToGridMaxUDTF),
        ("gbx_rst_quadbin_rastertogridmin", _RstQuadbinRasterToGridMinUDTF),
        ("gbx_rst_quadbin_rastertogridmedian", _RstQuadbinRasterToGridMedianUDTF),
        ("gbx_rst_separatebands", _RstSeparateBandsUDTF),
        ("gbx_rst_retile", _RstRetileUDTF),
        ("gbx_rst_tooverlappingtiles", _RstToOverlappingTilesUDTF),
        ("gbx_rst_maketiles", _RstMakeTilesUDTF),
        ("gbx_rst_h3_tessellate", _RstH3TessellateUDTF),
        ("gbx_rst_xyzpyramid", _RstXyzPyramidUDTF),
    ]
    for name, cls in udtfs:
        entries[name] = lambda s, n=name, c=cls: s.udtf.register(n, c)

    def _reg_pmtiles(s):
        from databricks.labs.gbx.pmtiles import register_pmtiles_agg

        register_pmtiles_agg(s)

    entries["gbx_pmtiles_agg"] = _reg_pmtiles
    return [(lambda: _env.assert_rasterio_available(), entries)]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Explicitly register the pyrx functions as Spark SQL functions.

    Installs the same ``gbx_rst_*`` SQL names the heavyweight rasterx package
    uses, but powered by the pyspark/rasterio implementation (no JAR). Call this
    once when you want the functions from SQL. The Python Column API
    (``prx.rst_width(col)``) works WITHOUT this call.

    You register the lightweight OR the heavyweight package in a given session;
    they share the ``gbx_rst_*`` names, so the last registration wins.

    Args:
        spark: Spark session (uses the active session if not provided).
        only: Optional list of function names to register (instead of all).
            Accepts SQL names (``gbx_rst_slope``) or short names (``rst_slope``),
            case-insensitively. ``None`` registers everything; ``[]`` registers
            nothing. An unrecognized name raises ``ValueError``.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    _register.run_groups(_registrar_groups(), spark, only)
```

Note: the count check that's audited in Task 2 Step 5 relies on the UDTF list above being complete — verify it matches the `spark.udtf.register(...)` calls present in `register` before the refactor (17 UDTFs total across the two comment blocks; transcribe all, do not drop any).

**Important — there is no separate `_fromfile_udf` registration in pyrx `register` to preserve** (the fromfile UDF is registered by the *heavy* `rasterx.register`, not here). Do not add it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_register_only.py`
Expected: PASS (5 tests).

- [ ] **Step 5: Run the full pyrx registration suite (no regression)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_sql_registration.py`
Expected: PASS — `only=None` path is unchanged, all existing SQL registration tests still pass.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_register_only.py
git commit -m "feat(pyrx): register(only=[...]) selective SQL registration"
```

---

### Task 3: pygx `register(only=[...])` (multi-guard: quadbin / bng / custom)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/functions.py` (the `register` function, ~lines 621-680)
- Test: `python/geobrix/test/pygx/test_register_only.py`

**Interfaces:**
- Consumes: `_register.run_groups`, `pygx._env` guards.
- Produces: `pygx.functions.register(spark=None, only=None)`; `_registrar_groups() -> List[Group]` with three guarded groups.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/pygx/test_register_only.py
"""register(only=[...]) selective registration for the pygx tier."""
import pytest

from databricks.labs.gbx.pygx import functions as pgx


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_quadbin(spark):
    for n in ("gbx_quadbin_polyfill", "gbx_bng_polyfill"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    pgx.register(spark, only=["quadbin_polyfill"])
    assert _exists(spark, "gbx_quadbin_polyfill")
    assert not _exists(spark, "gbx_bng_polyfill")


def test_only_accepts_camelcase(spark):
    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS gbx_bng_polyfill")
    pgx.register(spark, only=["BNG_Polyfill"])
    assert _exists(spark, "gbx_bng_polyfill")


def test_only_unknown_raises(spark):
    with pytest.raises(ValueError) as ei:
        pgx.register(spark, only=["quadbin_polifyll"])
    assert "quadbin_polifyll" in str(ei.value)


def test_only_does_not_trip_unselected_subgroup_guard(spark, monkeypatch):
    # Selecting only a quadbin fn must NOT call the bng/custom availability guards.
    from databricks.labs.gbx.pygx import _env

    def _boom():
        raise RuntimeError("guard should not be called")

    monkeypatch.setattr(_env, "assert_bng_available", _boom)
    monkeypatch.setattr(_env, "assert_custom_available", _boom)
    pgx.register(spark, only=["gbx_quadbin_resolution"])  # must not raise
    assert _exists(spark, "gbx_quadbin_resolution")


def test_only_none_registers_all_subgroups(spark):
    pgx.register(spark)
    for n in ("gbx_quadbin_polyfill", "gbx_bng_polyfill", "gbx_custom_polyfill"):
        assert _exists(spark, n)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_register_only.py`
Expected: FAIL — `TypeError: register() got an unexpected keyword argument 'only'`.

- [ ] **Step 3: Refactor `register` to three guarded groups**

Replace the body of `register` with a builder that transcribes each existing `spark.udf.register(...)` / `spark.udtf.register(...)` call into a `name: thunk` entry, **preserving the return-type argument** where present (e.g. `ArrayType(LongType())`, `ArrayType(StringType())`, `ArrayType(QUADBIN_CELL_SCHEMA)`, `ArrayType(BNG_CHIP_SCHEMA)`). The guard for the group must close over `_env` so it resolves at call time (monkeypatchable). Each group's thunk is `lambda: _env.assert_<sub>_available()`.

```python
from typing import List, Optional

from databricks.labs.gbx import _register
from databricks.labs.gbx.pygx import _env


def _registrar_groups() -> List[_register.Group]:
    quadbin = {
        "gbx_quadbin_pointascell": lambda s: s.udf.register("gbx_quadbin_pointascell", _pointascell_udf),
        "gbx_quadbin_resolution": lambda s: s.udf.register("gbx_quadbin_resolution", _resolution_udf),
        "gbx_quadbin_distance": lambda s: s.udf.register("gbx_quadbin_distance", _distance_udf),
        "gbx_quadbin_aswkb": lambda s: s.udf.register("gbx_quadbin_aswkb", _aswkb_udf),
        "gbx_quadbin_centroid": lambda s: s.udf.register("gbx_quadbin_centroid", _centroid_udf),
        "gbx_quadbin_cellunion": lambda s: s.udf.register("gbx_quadbin_cellunion", _cellunion_udf),
        "gbx_quadbin_kring": lambda s: s.udf.register("gbx_quadbin_kring", _kring, ArrayType(LongType())),
        "gbx_quadbin_polyfill": lambda s: s.udf.register("gbx_quadbin_polyfill", _polyfill, ArrayType(LongType())),
        "gbx_quadbin_tessellate": lambda s: s.udf.register("gbx_quadbin_tessellate", _tessellate, ArrayType(QUADBIN_CELL_SCHEMA)),
        "gbx_quadbin_cellunion_agg": lambda s: s.udf.register("gbx_quadbin_cellunion_agg", _cellunion_agg_udf),
    }
    bng = {
        "gbx_bng_pointascell": lambda s: s.udf.register("gbx_bng_pointascell", _bng_pointascell_udf),
        "gbx_bng_eastnorthasbng": lambda s: s.udf.register("gbx_bng_eastnorthasbng", _bng_eastnorthasbng_udf),
        "gbx_bng_cellarea": lambda s: s.udf.register("gbx_bng_cellarea", _bng_cellarea_udf),
        "gbx_bng_distance": lambda s: s.udf.register("gbx_bng_distance", _bng_distance_udf),
        "gbx_bng_euclideandistance": lambda s: s.udf.register("gbx_bng_euclideandistance", _bng_euclideandistance_udf),
        "gbx_bng_aswkb": lambda s: s.udf.register("gbx_bng_aswkb", _bng_aswkb_udf),
        "gbx_bng_aswkt": lambda s: s.udf.register("gbx_bng_aswkt", _bng_aswkt_udf),
        "gbx_bng_centroid": lambda s: s.udf.register("gbx_bng_centroid", _bng_centroid_udf),
        "gbx_bng_cellintersection": lambda s: s.udf.register("gbx_bng_cellintersection", _bng_cellintersection_udf),
        "gbx_bng_cellunion": lambda s: s.udf.register("gbx_bng_cellunion", _bng_cellunion_udf),
        "gbx_bng_kring": lambda s: s.udf.register("gbx_bng_kring", _bng_kring, ArrayType(StringType())),
        "gbx_bng_kloop": lambda s: s.udf.register("gbx_bng_kloop", _bng_kloop, ArrayType(StringType())),
        "gbx_bng_polyfill": lambda s: s.udf.register("gbx_bng_polyfill", _bng_polyfill, ArrayType(StringType())),
        "gbx_bng_geomkring": lambda s: s.udf.register("gbx_bng_geomkring", _bng_geomkring, ArrayType(StringType())),
        "gbx_bng_geomkloop": lambda s: s.udf.register("gbx_bng_geomkloop", _bng_geomkloop, ArrayType(StringType())),
        "gbx_bng_tessellate": lambda s: s.udf.register("gbx_bng_tessellate", _bng_tessellate, ArrayType(BNG_CHIP_SCHEMA)),
        "gbx_bng_kringexplode": lambda s: s.udtf.register("gbx_bng_kringexplode", _BngKRingExplode),
        "gbx_bng_kloopexplode": lambda s: s.udtf.register("gbx_bng_kloopexplode", _BngKLoopExplode),
        "gbx_bng_geomkringexplode": lambda s: s.udtf.register("gbx_bng_geomkringexplode", _BngGeomKRingExplode),
        "gbx_bng_geomkloopexplode": lambda s: s.udtf.register("gbx_bng_geomkloopexplode", _BngGeomKLoopExplode),
        "gbx_bng_tessellateexplode": lambda s: s.udtf.register("gbx_bng_tessellateexplode", _BngTessellateExplode),
        "gbx_bng_cellunion_agg": lambda s: s.udf.register("gbx_bng_cellunion_agg", _bng_cellunion_agg_udf),
        "gbx_bng_cellintersection_agg": lambda s: s.udf.register("gbx_bng_cellintersection_agg", _bng_cellintersection_agg_udf),
    }
    custom = {
        "gbx_custom_grid": lambda s: s.udf.register("gbx_custom_grid", _custom_grid_udf),
        "gbx_custom_pointascell": lambda s: s.udf.register("gbx_custom_pointascell", _custom_pointascell_udf),
        "gbx_custom_cellaswkb": lambda s: s.udf.register("gbx_custom_cellaswkb", _custom_cellaswkb_udf),
        "gbx_custom_cellaswkt": lambda s: s.udf.register("gbx_custom_cellaswkt", _custom_cellaswkt_udf),
        "gbx_custom_centroid": lambda s: s.udf.register("gbx_custom_centroid", _custom_centroid_udf),
        "gbx_custom_polyfill": lambda s: s.udf.register("gbx_custom_polyfill", _custom_polyfill, ArrayType(LongType())),
        "gbx_custom_kring": lambda s: s.udf.register("gbx_custom_kring", _custom_kring, ArrayType(LongType())),
    }
    return [
        (lambda: _env.assert_quadbin_available(), quadbin),
        (lambda: _env.assert_bng_available(), bng),
        (lambda: _env.assert_custom_available(), custom),
    ]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Register the pygx grid SQL functions (Serverless-safe: udf/udtf only).

    Args:
        spark: Spark session (uses the active session if not provided).
        only: Optional list of function names to register (instead of all).
            Accepts SQL names (``gbx_bng_polyfill``) or short names
            (``bng_polyfill``), case-insensitively. ``None`` registers everything;
            ``[]`` registers nothing. An unrecognized name raises ``ValueError``.
            A sub-module's availability guard runs only when >=1 of its functions
            is selected.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    _register.run_groups(_registrar_groups(), spark, only)
```

Verify every name/return-type matches the pre-refactor `register` body exactly (10 quadbin + 23 bng + 7 custom = 40 entries). Keep the existing `from pyspark.sql.types import ArrayType, LongType, StringType` (and `QUADBIN_CELL_SCHEMA`, `BNG_CHIP_SCHEMA`) imports the module already has.

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_register_only.py`
Expected: PASS (5 tests).

- [ ] **Step 5: Run the existing pygx UDF suites (no regression)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/`
Expected: PASS — existing quadbin/bng/custom tests unaffected by the `only=None` path.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/functions.py python/geobrix/test/pygx/test_register_only.py
git commit -m "feat(pygx): register(only=[...]) selective SQL registration"
```

---

### Task 4: pyvx `register(only=[...])` (guards: mvt / legacy / tin + pmtiles)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` (the `register` function, ~lines 267-283)
- Test: `python/geobrix/test/pyvx/test_register_only.py`

**Interfaces:**
- Consumes: `_register.run_groups`, `pyvx._env` guards.
- Produces: `pyvx.functions.register(spark=None, only=None)`; `_registrar_groups() -> List[Group]`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/pyvx/test_register_only.py
"""register(only=[...]) selective registration for the pyvx tier."""
import pytest

from databricks.labs.gbx.pyvx import functions as pvx


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_mvt(spark):
    for n in ("gbx_st_asmvt", "gbx_st_legacyaswkb"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    pvx.register(spark, only=["st_asmvt"])
    assert _exists(spark, "gbx_st_asmvt")
    assert not _exists(spark, "gbx_st_legacyaswkb")


def test_only_selects_udtf_and_pmtiles(spark):
    for n in ("gbx_st_asmvt_pyramid", "gbx_pmtiles_agg"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    pvx.register(spark, only=["gbx_st_asmvt_pyramid", "gbx_pmtiles_agg"])
    assert _exists(spark, "gbx_st_asmvt_pyramid")
    assert _exists(spark, "gbx_pmtiles_agg")


def test_only_does_not_trip_unselected_guard(spark, monkeypatch):
    from databricks.labs.gbx.pyvx import _env

    def _boom():
        raise RuntimeError("guard should not be called")

    monkeypatch.setattr(_env, "assert_tin_available", _boom)
    monkeypatch.setattr(_env, "assert_legacy_available", _boom)
    pvx.register(spark, only=["gbx_st_asmvt"])  # must not raise
    assert _exists(spark, "gbx_st_asmvt")


def test_only_unknown_raises(spark):
    with pytest.raises(ValueError) as ei:
        pvx.register(spark, only=["st_asmtv"])
    assert "st_asmtv" in str(ei.value)


def test_only_none_registers_all(spark):
    pvx.register(spark)
    for n in ("gbx_st_asmvt", "gbx_st_legacyaswkb", "gbx_st_triangulate", "gbx_pmtiles_agg"):
        assert _exists(spark, n)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_register_only.py`
Expected: FAIL — `TypeError: register() got an unexpected keyword argument 'only'`.

- [ ] **Step 3: Refactor `register` to guarded groups**

Preserve the `BinaryType()` return-type arg on `gbx_st_legacyaswkb`. The pmtiles group has no `_env` guard, so use a no-op guard (`lambda: None`).

```python
from typing import List, Optional

from databricks.labs.gbx import _register
from databricks.labs.gbx.pyvx import _env


def _registrar_groups() -> List[_register.Group]:
    mvt = {
        "gbx_st_asmvt": lambda s: s.udf.register("gbx_st_asmvt", _asmvt_udf),
        "gbx_st_asmvt_pyramid": lambda s: s.udtf.register("gbx_st_asmvt_pyramid", _AsMvtPyramidUDTF),
    }
    legacy = {
        "gbx_st_legacyaswkb": lambda s: s.udf.register("gbx_st_legacyaswkb", _legacyaswkb_impl, BinaryType()),
    }
    tin = {
        "gbx_st_triangulate": lambda s: s.udtf.register("gbx_st_triangulate", _TriangulateUDTF),
        "gbx_st_interpolateelevationbbox": lambda s: s.udtf.register("gbx_st_interpolateelevationbbox", _InterpElevBBoxUDTF),
        "gbx_st_interpolateelevationgeom": lambda s: s.udtf.register("gbx_st_interpolateelevationgeom", _InterpElevGeomUDTF),
    }

    def _reg_pmtiles(s):
        from databricks.labs.gbx.pmtiles import register_pmtiles_agg

        register_pmtiles_agg(s)

    pmtiles = {"gbx_pmtiles_agg": _reg_pmtiles}
    return [
        (lambda: _env.assert_mvt_available(), mvt),
        (lambda: _env.assert_legacy_available(), legacy),
        (lambda: _env.assert_tin_available(), tin),
        (lambda: None, pmtiles),
    ]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Register the pyvx VectorX SQL functions (Serverless-safe: udf/udtf only).

    Args:
        spark: Spark session (uses the active session if not provided).
        only: Optional list of function names to register (instead of all).
            Accepts SQL names (``gbx_st_asmvt``) or short names (``st_asmvt``),
            case-insensitively. ``None`` registers everything; ``[]`` registers
            nothing. An unrecognized name raises ``ValueError``. A sub-module's
            availability guard runs only when >=1 of its functions is selected.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    _register.run_groups(_registrar_groups(), spark, only)
```

Keep the module's existing `from pyspark.sql.types import BinaryType` import.

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_register_only.py`
Expected: PASS (5 tests).

- [ ] **Step 5: Run existing pyvx suites (no regression)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/test/pyvx/test_register_only.py
git commit -m "feat(pyvx): register(only=[...]) selective SQL registration"
```

---

### Task 5: `ds.register.register(spark, only=[...])` — readers/writers by format name

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/register.py` (the `register` function)
- Test: `python/geobrix/test/ds/test_register_only.py`

**Interfaces:**
- Consumes: `_register.resolve_only`, `_register.normalize_datasource_name`.
- Produces: `ds.register.register(spark=None, only=None)`.

The 9 light DataSources select by **format name** (`name()` classmethod): `raster_gbx`, `gtiff_gbx`, `pmtiles_gbx`, `vector_gbx`, `shapefile_gbx`, `geojson_gbx`, `geojsonl_gbx`, `gpkg_gbx`, `file_gdb_gbx`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/ds/test_register_only.py
"""register(only=[...]) selective registration for the light DataSources."""
import pytest

from databricks.labs.gbx.ds import register as ds_register


def _format_ok(spark, fmt):
    """A format is registered if .format(fmt) builds a reader without an
    'unsupported data source' error. Loading an empty path raises a DIFFERENT
    (path/IO) error, so we treat only the unsupported-format error as 'absent'."""
    try:
        spark.read.format(fmt).load("/tmp/__nonexistent_gbx_probe__")
        return True
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if "unable to find" in msg or "data source" in msg and "not" in msg:
            return False
        return True  # some other error => the format WAS resolved


def test_only_subset_registers_just_those(spark):
    ds_register.register(spark, only=["raster_gbx", "gtiff_gbx"])
    assert _format_ok(spark, "raster_gbx")
    assert _format_ok(spark, "gtiff_gbx")


def test_only_accepts_bare_name_without_suffix(spark):
    ds_register.register(spark, only=["raster"])  # -> raster_gbx
    assert _format_ok(spark, "raster_gbx")


def test_only_unknown_format_raises(spark):
    with pytest.raises(ValueError) as ei:
        ds_register.register(spark, only=["raster_gpx"])
    assert "raster_gpx" in str(ei.value)


def test_only_none_registers_all(spark):
    ds_register.register(spark)
    for fmt in ("raster_gbx", "gtiff_gbx", "shapefile_gbx", "geojson_gbx"):
        assert _format_ok(spark, fmt)
```

Note: a `spark` fixture must exist for `python/geobrix/test/ds/`. If `python/geobrix/test/ds/conftest.py` has no `spark` fixture, add a module-scoped one mirroring `python/geobrix/test/pyrx/conftest.py` (a plain `SparkSession.builder.master("local[2]")` session, no JARs).

- [ ] **Step 2: Run tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_register_only.py`
Expected: FAIL — `TypeError: register() got an unexpected keyword argument 'only'`.

- [ ] **Step 3: Add `only` to `ds.register.register`**

```python
# python/geobrix/src/databricks/labs/gbx/ds/register.py  (replace the register function)
from typing import List, Optional

from databricks.labs.gbx import _register


def register(spark: Optional[SparkSession] = None, only: Optional[List[str]] = None) -> None:
    """Register the light DataSources (raster_gbx, gtiff_gbx, pmtiles_gbx, and the
    vector readers/writers). Uses the active session if not given.

    Args:
        spark: Spark session (active session if not provided).
        only: Optional list of format names to register (instead of all 9).
            Accepts the format name with or without the ``_gbx`` suffix
            (``raster`` or ``raster_gbx``), case-insensitively. ``None`` registers
            everything; ``[]`` registers nothing. An unrecognized format raises
            ``ValueError``.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    by_name = {src.name(): src for src in _SOURCES}
    if only is None:
        selected = list(_SOURCES)
    else:
        wanted = _register.resolve_only(
            only, by_name.keys(), normalizer=_register.normalize_datasource_name
        )
        selected = [by_name[n] for n in wanted]
    for source in selected:
        spark.dataSource.register(source)
```

Keep `_SOURCES`, the existing imports, and `_try_register_on_import()` unchanged. `_SOURCES` order is preserved for `only=None` (iterate `_SOURCES`, not the resolved set).

- [ ] **Step 4: Run tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_register_only.py`
Expected: PASS (4 tests).

- [ ] **Step 5: Run existing ds suites (no regression)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/`
Expected: PASS — `only=None` path unchanged.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/register.py python/geobrix/test/ds/test_register_only.py
git commit -m "feat(ds): register(only=[...]) selective reader/writer registration"
```

---

### Task 6: Docs — "Registering a subset" in execution-tiers.mdx

**Files:**
- Modify: `docs/docs/api/execution-tiers.mdx` (add a subsection after "The one-line swap", ~line 25)

**Interfaces:** none (docs only).

- [ ] **Step 1: Add the subsection**

Insert after the `:::warning Heavyweight needs more than the wheel` admonition (before `## Tradeoffs`):

````markdown
## Registering a subset (`only=`)

`register()` installs every `gbx_*` SQL name for the tier. To register just the functions a session uses, pass `only=` (lightweight tiers — `pyrx`, `pygx`, `pyvx`):

```python
from databricks.labs.gbx.pyrx import functions as rx

rx.register(spark, only=["rst_slope", "rst_clip"])   # just these two
rx.register(spark)                                   # all (default)
```

Names are case-insensitive and accept either the SQL name (`gbx_rst_slope`) or the short form (`rst_slope`). An unrecognized name raises `ValueError` (typo guard). `only=[]` registers nothing.

**Readers and writers** register through a separate entry point and take `only=` too — selected by **format name** (with or without the `_gbx` suffix):

```python
from databricks.labs.gbx.ds import register as ds_register

ds_register.register(spark, only=["raster_gbx", "gtiff_gbx"])  # just these formats
ds_register.register(spark, only=["shapefile"])               # 'shapefile' -> 'shapefile_gbx'
ds_register.register(spark)                                    # all readers/writers (default)
```

**Mixing tiers per function.** Because both tiers share the `gbx_*` names (last registration wins), you can register the heavyweight set and then override individual functions with the lightweight implementation:

```python
from databricks.labs.gbx.rasterx import functions as heavy
from databricks.labs.gbx.pyrx    import functions as light

heavy.register(spark)                       # all heavy gbx_rst_*
light.register(spark, only=["rst_slope"])   # gbx_rst_slope now lightweight
```

The reverse — re-registering a few **heavy** functions over a lightweight session — is not yet available; `only=` is currently a lightweight-tier feature (heavy registers its full set). Mixing works because both tiers use the same tile struct and GTiff payload, so a tile produced by one tier flows into a function from the other.
````

- [ ] **Step 2: Verify the docs build references resolve**

Run: `grep -n "Registering a subset" docs/docs/api/execution-tiers.mdx`
Expected: the new heading is present. (No doc-test executes this MDX prose; the code blocks are illustrative, consistent with the page's existing `## The one-line swap` block.)

- [ ] **Step 3: Commit**

```bash
git add docs/docs/api/execution-tiers.mdx
git commit -m "docs(tiers): document register(only=[...]) and per-function tier mixing"
```

---

## Notes for the implementer

- The `spark` fixture in each light package's `conftest.py` is **module-scoped** and shared, so SQL temp functions accumulate within a test module. The `only=` tests therefore `DROP TEMPORARY FUNCTION IF EXISTS` the names they assert on (both present- and absent-checks) so they're order-independent. This `DROP TEMPORARY FUNCTION` pattern is already used in `python/geobrix/test/pmtiles_light/test_agg_light_udf.py`.
- If `spark.catalog.functionExists("gbx_...")` does not report a temp UDTF in this Spark build, fall back to `"<name>" in [f.name for f in spark.catalog.listFunctions()]` — but try `functionExists` first.
- Late-binding in `lambda`: the pyrx scalar/UDTF closures bind `name`/`udf`/`cls` as default args (`lambda s, n=name, u=udf_obj: ...`) to avoid the classic loop-variable capture bug. The pygx/pyvx maps hard-code each name literal in the lambda body, so they don't need default-arg binding. Keep it that way.
- Run `gbx:lint:python --check` (isort/black/flake8) before the final commit of each task; reformat in-container if host black differs.
