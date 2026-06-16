# Benchmark full coverage + core/full sets + deprecation scorecard — design spec

**Date:** 2026-06-07 · **Branch:** `beta/0.4.0` · **Status:** design approved (scope + core-set + capture-motivation), pre-plan.

**Goal:** Grow the heavy-vs-light benchmark from a 19-function representative set to **full coverage of the 107 registered `rst_*` functions**, while preserving a fast **`core`** set for routine runs. Make the suite the **evidence instrument** for a possible heavyweight-API deprecation.

## 1. Motivation (internal)

A surprising benchmark finding: lightweight (pyrx) performance is **at least comparable to, and in several cases far better than**, heavyweight (rasterx) — e.g. band-math is hundreds of times faster pure-core, terrain/reductions a few times faster, with **0 divergent** output. Combined with the fact that **pyrx already implements all 107 registered `rst_*` functions** (full functional parity — `registered_functions.txt` rst_ = 107, pyrx rst_ = 107, **0 heavy-only**), this opens the door to **eventually deprecating the heavyweight tier entirely**.

That makes the benchmark a decision instrument, not just a comparison. To responsibly retire heavyweight we must show, across **every** function, that lightweight (a) produces consistent output and (b) performs acceptably. Any unbenchmarked function is an unanswered "is light safe here?". So **completeness is the objective**, and the suite must surface coverage/parity/performance as a **scorecard**.

> **Voice boundary:** the deprecation framing lives in this internal (gitignored) doc only. User-facing docs (`docs/docs/`) and the generated `summary.md` stay neutral — they report coverage/parity/performance numbers; they do **not** say "we plan to delete heavyweight." The data supports the decision without the loaded framing.

## 2. Scope

- **This round (Phase 1):** the core/full selection mechanism + **bucket E (standard ds-in, ~51 functions)** + the scorecard/coverage section in `summary.md`.
- **Committed follow-on phases (not optional — required to complete the deprecation evidence base):**
  - Phase 2 — **C: multi-input / constructors / tiling / readers** (14): `frombands`, `fromcontent`, `fromfile`, `combineavg`, `merge`, `maketiles`, `retile`, `tooverlappingtiles`, `xyzpyramid`, `tryopen`, `separatebands`, `buildoverviews`, `getsubdataset`, `subdatasets`. Needs multi-tile corpus + adapters.
  - Phase 3 — **B: DGGS grid / vector-out** (13): `contour`, `polygonize`, `h3_tessellate`, `h3_rastertogrid{avg,count,max,median,min}`, `quadbin_rastertogrid{avg,count,max,median,min}`. Needs new fingerprint kind + consistency semantics for cell/geometry output.
  - Phase 4 — **A: aggregators** (7, `*_agg`) + **D: geometry-in** (3): `rasterize`, `dtmfromgeoms`, `gridfrompoints`. Needs a group-by spark-path harness and a geometry corpus.
- The 107 = 19 (current) + 88 (missing). Buckets: E 51 · C 14 · B 13 · A 7 · D 3 = 88.

## 3. Core vs full selection

- Add `core: bool = False` to `FnSpec`. Mark the **current 19** as `core=True` (kept as-is per decision — they already span accessor / reduction / terrain / band-math / warp).
- Extend `spec.select(functions=None, categories=None, set="core")` with a `set` parameter:
  - `set="core"` (default) → only `core=True` specs.
  - `set="full"` → entire `REGISTRY`.
  - `functions=`/`categories=` still filter within the chosen set (explicit `functions` overrides the set filter, as today).
- Thread `--set core|full` through the `gbx:bench:*` commands (`gen-data` picks tiles for whatever functions are selected; `lightweight`/`heavyweight`/`all`/`cluster` pass it to `select`). Default `core`. `full` is the on-demand / periodic-job run.

## 4. Bucket E — standard ds-in (Phase 1)

All take one tile in. Three sub-shapes, each a known pattern:

1. **Scalar / list accessors** → `scalar` or `scalar_list` fingerprint (full consistency). E.g. `band`, `srid`, `type`, `format`, `memsize`, `isempty`, `rotation`, `scalex/y`, `skewx/y`, `upperleftx/y`, `pixelwidth/height`, `tilexyz`, `worldtorastercoord(+x/y)`, `rastertoworldcoord(+x/y)` (the coord ones take scalar args). Heavy: `BenchFingerprint.ofScalar/ofArray(RST_X.execute(ds[, args]))`.
2. **Map / struct outputs** → **pure-core timing only** (`modes=("pure-core",)`, no consistency assertion): `metadata`, `bandmetadata`, `georeference`, `boundingbox`, `summary`, `histogram`, `getnodata`. Their outputs (maps/structs/variable-length) have no clean cross-engine fingerprint; forcing one would be noise. Timed, not compared. Documented as such in the scorecard.
3. **Tile-out transforms** → `raster` fingerprint (full consistency), same pattern as terrain/warp: `clip`, `threshold`, `initnodata`, `setsrid`, `updatetype`, `fillnodata`, `filter`, `convolve`, `proximity`, `viewshed`, `color_relief`, `resample`, `resample_to_res`, `resample_to_size`, `derivedband`, `mapalgebra`, `index`, `evi`, `savi`, `cog_convert`, `asformat`, `sample`. Heavy: `fpDerived(RST_X.execute(ds, args))`.

**Per-function cost (mechanical, mirrors existing 19):**
- Python `FnSpec` in `spec.py`: `name`, `sql_name`, `category`, `modes`, `args`, `core_fn=lambda ds,a: <pyrx core>`, `col_fn=lambda t,a: prx.rst_x(t,...)`, `core=False`.
- Scala `BenchDispatch.scala`: a `pure-core` case `case "rst_x" => <fp>(RST_X.execute(ds, <args>))` and a `column` case `case "rst_x" => rst_x(tile, <args>)`. **Heavy signatures are read from each `RST_X.scala`** (don't guess; the `execute`/registration shows the arg order).
- Fingerprint kind chosen by output type (scalar/scalar_list/raster). Map/struct → pure-core-only, fingerprint omitted/`na`.

`min_bands` set where a function needs ≥2 bands (e.g. band-math, `band` index 2).

## 5. Scorecard / coverage section (Phase 1)

Add to `summarize_compare` (combined `summary.md`) an aggregate **Coverage & parity** block, computed from the compared cells + the registry + the canonical 107 list:

- **Coverage:** `benchmarked N / 107 registered rst_ functions` (and which `set` produced this run).
- **Parity:** of the consistency-comparable cells, counts of `exact` / `within_tol` / `divergent` (and the divergent function names).
- **Performance:** count where lightweight ≥ heavyweight (speedup ≥ 1) vs where heavyweight wins.
- **Functional parity gap:** registered rst_ functions with no pyrx implementation — **currently 0** (state it explicitly; it's a positive, and the check guards against future regressions).
- **Not yet covered:** the explicit list/count of registered functions absent from this run's registry (the A–D buckets in Phase 1) — **no silent omission**; the reader sees exactly what remains.

Framed neutrally (coverage/parity/performance), no deprecation language.

## 6. Architecture / files (Phase 1)

- `python/.../bench/spec.py` — `FnSpec.core` field; `select(set=...)`; ~51 new `FnSpec` entries; a `REGISTERED_RST` loader (read `docs/tests-function-info/registered_functions.txt`) for coverage math.
- `src/test/scala/.../bench/BenchDispatch.scala` — ~51 new `pure-core` + `column` cases (signatures from each `RST_*.scala`). New fingerprint helpers only if a new output shape needs one (reuse `ofScalar`/`ofArray`/`fpDerived`).
- `python/.../bench/compare.py` — the Coverage & parity block in `summarize_compare`; coverage math (registry vs 107).
- `scripts/commands/gbx-bench-*.sh` — `--set core|full` option (default core), passed to `select`.
- `python/.../bench/spec.py` `dump_functions_json` — include the `core` flag + `set` membership.

## 7. Testing & validation

- **Unit (venv):** `test_spec.py` — `select(set="core")` returns only the 19; `select(set="full")` returns all registered; `functions=`/`categories=` still filter; new FnSpecs are well-formed (have core_fn/col_fn, valid modes, fingerprint-appropriate). `test_compare.py` — the scorecard block renders coverage/parity/performance counts and the not-yet-covered list; `exact` cells unannotated; parity-gap=0 line present.
- **Scala (Docker):** the new `BenchDispatch` cases compile and dispatch (extend the bench Scala test to exercise a sample of new functions pure-core + column).
- **Cross-API acceptance (authoritative):** `gbx:bench:all --set full --modes pure-core` runs clean; the scorecard shows coverage 70/107 (Phase 1 = 19 + 51) and the A–D not-yet-covered list. Spot-check that newly-covered tile-out transforms are `within_tol` (and investigate any `divergent` as a real light-vs-heavy finding, not loosened tolerance).
- **`core` run unchanged:** `gbx:bench:all` (default core) still produces the 19-function comparison, fast.

## 8. Out of scope (this round)
- Buckets A–D (committed to Phases 2–4).
- Any change to the consistency tolerance or `_close` semantics.
- Removing/altering heavyweight functions (deprecation is a future decision this evidence base informs — not an action here).

## 9. Risks
- **Heavy signature drift:** each `RST_X.execute` arg order must be read from source, not guessed — a wrong arg silently benchmarks the wrong thing. Mitigation: implementers read each `RST_*.scala`; the acceptance sweep's consistency check catches gross mismatches.
- **Map/struct fingerprint temptation:** resist forcing consistency on metadata/struct outputs; pure-core-timing-only is correct, and the scorecard must label them so coverage isn't overstated as "parity-verified."
- **Scale:** ~51 functions × (FnSpec + 2 Scala cases) is large but mechanical — subagent-driven, grouped by sub-shape (scalar accessors / coord transforms / tile-out transforms / map-struct), one task per group + mechanism + scorecard.

---
*Design approved 2026-06-07. Next: implementation plan (writing-plans), Phase 1. Sources: `bench/spec.py` (registry + select), `BenchDispatch.scala` (heavy dispatch), `registered_functions.txt` (107 canonical), pyrx `functions.py` (107 parity).*
