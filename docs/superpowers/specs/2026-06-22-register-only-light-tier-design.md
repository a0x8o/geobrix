# `register(spark, only=[...])` — selective SQL registration (light tiers)

**Date:** 2026-06-22
**Status:** Approved (design)
**Scope:** Lightweight tiers only — `pyrx`, `pygx`, `pyvx`. Heavyweight `only=` is a documented future follow-up (out of scope here).

## Goal

Add an optional `only` parameter to the lightweight `register()` functions so a session can register a **subset** of a tier's SQL functions instead of the full set:

```python
from databricks.labs.gbx.pyrx import functions as rx
rx.register(spark, only=["rst_slope", "gbx_rst_clip"])   # just these two
rx.register(spark)                                        # all (unchanged)
```

## Motivation

`register()` today is all-or-nothing per package: it installs every `gbx_*` SQL name for that tier. The two tiers share SQL names, so cross-tier composition is last-registration-wins at whole-package granularity. `only=` gives finer control. The primary beneficiary is the lightweight tier:

- **Light-only cluster:** register just the functions a session will actually use, rather than the whole surface.
- **Tier mixing:** with heavy installed and auto-registered, `light.register(spark, only=[...])` overrides just those names with the lightweight implementation, leaving the rest heavy. (The reverse — heavy re-registering a few over light — needs the deferred heavy `only=`.)

## API

Add `only: Optional[List[str]] = None` to `register()` in:
- `databricks.labs.gbx.pyrx.functions`
- `databricks.labs.gbx.pygx.functions`
- `databricks.labs.gbx.pyvx.functions`

Semantics:
- `only=None` (default) → register **everything** — identical to today's behavior, in today's order.
- `only=[...]` → register **exactly** the named functions, nothing else.
- `only=[]` → register **nothing** (valid, no-op registration). Documented explicitly.

### Name handling

Accept **both** the SQL name and the short Python name, **case-insensitively**. Normalization: the input is `.lower()`-cased first (SQL names are all lowercase by convention, and the Scala classes are CamelCase — `RST_Slope`, `BNG_Polyfill` — so users naturally type mixed case), then `gbx_` is prepended if absent. This is uniform across every prefix (`rst_`, `bng_`, `quadbin_`, `custom_`, `st_`):

| Input | Normalized |
|---|---|
| `rst_slope` | `gbx_rst_slope` |
| `gbx_rst_slope` | `gbx_rst_slope` |
| `RST_Slope` | `gbx_rst_slope` |
| `st_asmvt` | `gbx_st_asmvt` |
| `BNG_Polyfill` | `gbx_bng_polyfill` |
| `GBX_RST_Slope` | `gbx_rst_slope` |

Lowercasing relaxes only the case dimension — a name that still doesn't match a registerable function after lowercasing is treated as unknown and raises (the typo guard below). Leading/trailing whitespace is stripped before normalizing.

### Validation

Validate every normalized name against the package's full registerable set (the union of all groups). On any unrecognized name, raise `ValueError` that lists the unrecognized name(s) and, for each, up to 3 `difflib.get_close_matches` suggestions from the valid set. Rationale: a silently-unregistered function would otherwise surface much later at call time as `UNRESOLVED_ROUTINE` — fail fast at registration with an actionable message.

Example message:
```
register(only=...) got unknown function name(s): ['rst_slpe'].
  rst_slpe -> did you mean: rst_slope?
Valid names are the gbx_* SQL names (or their short forms) for this tier.
```

## Mechanism — grouped registrar map

Each light `register()` is currently a flat sequence of `spark.udf.register(name, udf)` / `spark.udtf.register(name, cls)` calls, with `pygx` and `pyvx` interleaving per-sub-module availability guards (`_env.assert_quadbin_available()`, `assert_bng_available()`, `assert_custom_available()`; `assert_mvt_available()`, `assert_legacy_available()`, `assert_tin_available()`).

Refactor each `register()` to build an **ordered list of groups**, each a pair:

```
(assert_available_fn, { sql_name: register_fn(spark) -> None, ... })
```

- `pyrx`: one group, guard `assert_rasterio_available`, mapping every scalar/agg UDF (derived from `SQL_REGISTRY`), every UDTF (the ~20 `spark.udtf.register` names), and `gbx_pmtiles_agg` (via `register_pmtiles_agg`) to its registration closure.
- `pygx`: three groups — `quadbin` (guard `assert_quadbin_available`), `bng` (guard `assert_bng_available`), `custom` (guard `assert_custom_available`) — each mapping its `gbx_quadbin_*` / `gbx_bng_*` / `gbx_custom_*` names.
- `pyvx`: groups for `mvt` (`assert_mvt_available`: `gbx_st_asmvt`, `gbx_st_asmvt_pyramid`), `legacy` (`assert_legacy_available`: `gbx_st_legacyaswkb`), `tin` (`assert_tin_available`: `gbx_st_triangulate`, `gbx_st_interpolateelevationbbox`, `gbx_st_interpolateelevationgeom`), and `pmtiles` (no guard / its own: `gbx_pmtiles_agg`).

`register(spark, only=None)` algorithm:
1. `spark = spark or SparkSession.builder.getOrCreate()`.
2. If `only` is not None: normalize + validate (raise on unknown) → `wanted: set[str]`.
3. For each group in order:
   - `selected = {n: fn for n, fn in group.entries.items() if only is None or n in wanted}`
   - if `selected` is non-empty: call the group's `assert_available_fn()`, then call each `fn(spark)` in the group's defined order.

Key property: with `only=None`, every group runs its guard and registers every name **in the same order as today** — a behavior-preserving refactor. A guard for a sub-module with no selected functions is **not** invoked (so `pygx` `only=['gbx_quadbin_polyfill']` never asserts bng/custom availability).

A small shared helper module (e.g. `databricks/labs/gbx/_register.py`) holds `normalize_only(names) -> set[str]` and `resolve_only(names, valid) -> set[str]` (validation + close-match error) so the three packages don't duplicate the logic. The grouped registrar structures stay per-package (they reference package-local UDF/UDTF objects).

## Testing (TDD)

Per package (`pyrx`, `pygx`, `pyvx`), using the existing Python test session fixture:
1. `only` with a subset registers exactly those functions (each present in `spark.catalog`/callable) and at least one omitted function is **absent**.
2. Both name forms resolve: `only=['rst_slope']` and `only=['gbx_rst_slope']` register the same function.
3. Unknown name raises `ValueError` whose message contains the offending name.
4. `only=None` registers the full set (count/spot-check parity with the pre-refactor behavior).
5. A UDTF is selectable by name (e.g. `pyrx` `only=['gbx_rst_retile']`; `pyvx` `only=['gbx_st_asmvt_pyramid']`), and the pmtiles agg is selectable (`only=['gbx_pmtiles_agg']`).
6. `pygx` `only=['gbx_quadbin_polyfill']` does **not** raise from the bng/custom availability guards (sub-module isolation). Where practical, assert the guard isn't tripped (e.g. monkeypatch the bng/custom `assert_*` to raise and confirm it is not called).
7. `only=[]` registers nothing and does not error.

Tests are pure-Python (no Docker/JAR) and run via `gbx:test:python --path python/geobrix/test/<pkg>/`.

## Out of scope (future follow-up)

Heavyweight `only=`. It is feasible but requires a JAR change + cluster re-validation: an optional `only: Set[String]` on `RegistryDelegate` (skip `register(companion)` when `only` is non-empty and the companion name is absent), threaded from a new `RegisterBatch` `"only"` option through each Scala package `functions.register(spark, only)` signature, with the heavy Python `register(only=...)` passing `.option("only", ",".join(...))`. Tracked separately.

## Docs

Add a short **"Registering a subset"** subsection to `docs/docs/api/execution-tiers.mdx`: the `only=` signature, both-name-forms note, the light-over-heavy mixing pattern, and a note that heavy `only=` is not yet available (use whole-tier registration order for heavy).
