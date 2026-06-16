# Benchmark Phase 3 — bucket B (DGGS grid / vector-out) — design spec

**Date:** 2026-06-08 · **Branch:** `beta/0.4.0` · **Status:** design (recommended basis approved), pre-plan.
**Goal:** Add the 13 bucket-B functions to the heavy-vs-light benchmark (coverage **84 → 97 / 107**) with two new fingerprint kinds for cell/geometry output, classify divergences per best-practice, and **perf-review** the bucket (flag light fns meaningfully slower than heavy → the perf backlog). Each declares `FnSpec.sources`; results land in the authoritative store.

## 1. The 13 functions, by output shape

| Group | Functions | Output | pyrx core | heavy |
|---|---|---|---|---|
| **B-grid (DGGS)** | `h3_tessellate`(tile,res), `h3_rastertogrid{avg,count,max,median,min}`(tile,res), `quadbin_rastertogrid{avg,count,max,median,min}`(tile,res) — 11 | array of **cells** `(cell_id[, value])` | `tessellate.tessellate_h3`, `gridagg.raster_to_grid` | `RST_H3_Tessellate`, `RST_H3_RasterToGrid*`, `RST_Quadbin_RasterToGrid*` |
| **B-vec (vector-out)** | `contour`(tile,levels,…), `polygonize`(tile,band,connectedness) — 2 | array of **geometry features** `(geom, level/value)` | `analysis.contour`, `features.polygonize` | `RST_Contour`, `RST_Polygonize` |

All `input_kind="tile"` (single ds in); the novelty is **output** fingerprinting.

## 2. New fingerprint kinds

- **`dggs_grid`** (the 11 cell-output fns): `{"kind":"dggs_grid","count":N,"cells_hash":<sha of sorted cell-id set>,"agg":{min,max,mean,std over cell values}}`. For `tessellate` / `*count`, the "value" is the per-cell count (or 1); the agg + count still apply.
  - **Consistency = cell COUNT exact + value `agg` within tolerance** (the pass criterion). Additionally **report cell-set overlap** (Jaccard of the two `cells_hash` sets) as an informational note — exact cell-set equality across two H3/quadbin implementations on raster edges is unlikely, so it's reported, not gating. A `cells_hash` match → exact; count+agg match → within_tol; count mismatch → divergent.
- **`vector`** (contour, polygonize): `{"kind":"vector","count":N,"measure":<total length (contour) / total area (polygonize)>,"attr_agg":{min,max,mean,std over the level/value attribute}}`.
  - **Consistency = feature COUNT + `measure` within tolerance** + attr agg within tol. **NOT** exact geometry — GDAL vs pyrx contour/polygonize place vertices differently; count + aggregate length/area is the meaningful swap-safety signal (same as the `raster_collection` philosophy).

Both kinds are order-independent (aggregate over the whole set). Add to Scala `BenchFingerprint` (`ofDggsGrid`, `ofVector`) + pyrx (`fingerprint_dggs_grid`, `fingerprint_vector`) + `compare.py` branches reusing `_close(rel_tol,abs_tol)`.

## 3. H3 / quadbin parity (first plan step — gate before fingerprinting)

pyrx uses the `h3` + `quadbin` Python libs; heavy uses the GridX H3/quadbin. **Verify they share the standard cell-id definitions** (same H3 index for a given lat/lon/res; same quadbin index) so `cell_id`s are directly comparable. Quick check: pick a known lat/lon/res, compute the cell id in both (a pyrx `_h3_cell`/`_quadbin_cell` call vs the heavy GridX call) and confirm equality. If they match → cell-set overlap is meaningful. If they diverge inherently (different cell conventions) → fall back to **count + value-agg only** (drop `cells_hash` from the pass) and record it as a finding (per best-practice: fix whichever tier is non-standard, or document).

## 4. Per-function notes / known forks
- **rastertogrid sampling:** each engine maps pixels → cells (pixel centroid → H3/quadbin cell, then aggregate by the op). If both sample pixel centroids identically → same cells + same per-cell aggregates. Edge/resolution handling may differ → cell-set differences (reported via overlap). The **value agg** (e.g. mean of all cells' means) is the robust comparison.
- **`*count`:** value = per-cell pixel count; agg over those counts + total cell count.
- **`contour`:** fixed `levels` arg (e.g. evenly-spaced over the band's range); measure = total LineString length.
- **`polygonize`:** `band=1, connectedness=4` (or 8 — match heavy's default); measure = total polygon area; attr = the polygon value.
- **v0.3.0 status:** check each bucket-B fn's v0.3.0 membership; for any **heavy** behavior change document per [[commit-message-hygiene]]-adjacent release-notes discipline (light changes never need v0.3.0 docs — pyrx is v0.4.0-new).

## 5. Perf-review (per the standing directive — [[perf-parity-light-vs-heavy]])
As part of validation, after the bucket-B functions land in the store, **flag any light fn meaningfully slower than heavy** (light slower by ≥~1 ms AND ≥~1.5×) and add to the perf backlog (task #90). DGGS tessellation + contour/polygonize in pyrx may be slow vs heavy GDAL — capture it. This is a first-class output of the phase alongside consistency, not an afterthought.

## 6. Architecture / files
- `bench/spec.py`: 13 FnSpecs (`core=False`, `sources`, args), grouped B-grid/B-vec; the runner fingerprints list-returning DGGS/vector core_fns via the new kinds.
- `bench/results.py`/`fingerprint.py`: `fingerprint_dggs_grid`, `fingerprint_vector` (pyrx side).
- `src/main/scala/.../bench/BenchFingerprint.scala`: `ofDggsGrid`, `ofVector`.
- `src/test/.../bench/BenchDispatch.scala`: 13 dispatch cases wrapping the heavy RST_* outputs in the new fingerprints; `BenchDispatchTest` size +13 → 97.
- `bench/compare.py`: `dggs_grid` + `vector` branches.

## 7. Testing & validation
- **Unit (venv):** the two new fingerprint kinds round-trip + compare correctly (count mismatch → divergent; agg within tol → within_tol; cell-set overlap reported); H3/quadbin parity check; per-fn registry well-formedness.
- **Scala test-compile** for the dispatch cases.
- **Cross-API acceptance (Docker, controller-orchestrated, backgrounded + ~30s status):** `gbx:bench:changed`/scoped run → store; `gbx:bench:status` → **97/107**; classify each bucket-B fn's consistency (count/agg) — record divergences (likely cell-edge membership or geometry-count differences) per best-practice (fix the wrong tier, or document if a v0.3.0 heavy change).
- **Perf-review:** flag meaningfully-slower-light bucket-B fns → task #90.

## 8. Sequencing & out of scope
- B-grid (11) then B-vec (2) — or together if the fingerprint infra lands first. Then Phase 4 (bucket A aggregators + D geometry-in) → 107/107.
- Out of scope: exact geometry/cell-set equality (use count + aggregate measures + overlap report); the heavy `RST_TileXYZ` RGBA-render follow-up; the perf optimizations themselves (queued, task #90 — Phase 3 only *flags* them).

## 9. Risks
- **H3/quadbin cell-edge membership** differs between implementations → cell-set overlap <100% even when value-agg matches; handled by making value-agg+count the pass criterion, overlap informational.
- **contour/polygonize feature counts** may differ (GDAL vs pyrx algorithms produce different # of features at the same levels/connectedness) → a real divergence to classify (fix wrong tier or document), not auto-pass.
- **Perf:** DGGS tessellation / vectorization in pyrx may be slow → captured by the perf-review, not blocking coverage.

---
*Design 2026-06-08 (recommended basis). Next: implementation plan (writing-plans). Sources: pyrx `core/{gridagg,tessellate,features,analysis}.py` + bindings, heavy `RST_H3_*`/`RST_Quadbin_*`/`RST_Contour`/`RST_Polygonize`, bench `spec.py`/`compare.py`/`BenchFingerprint.scala`. Plugs into the store/changed/status lifecycle + [[perf-parity-light-vs-heavy]].*
