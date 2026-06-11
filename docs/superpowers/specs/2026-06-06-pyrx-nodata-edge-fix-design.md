# pyrx NoData / edge-handling fix — design spec

**Date:** 2026-06-06 · **Branch:** `pyrx-0.4.0` · **Status:** design approved, pre-plan.
**Goal:** Fix lightweight (pyrx) raster functions that diverge from the heavyweight (Scala/GDAL) on **NoData and kernel-edge handling**, so a `rx.rst_* → prx.rst_*` swap gives consistent results. Heavyweight is the reference (unchanged).

**Evidence:** The benchmark consistency sweep (full pure-core, 19 ds-in fns × a nodata-free 256² tile + a 25%-nodata 512² tile) + a code read of both implementations. See [[pyrx-nodata-edge-divergences]] memory and the design spec §11 of the benchmark suite. Confirmed divergences, with the heavy target semantics quoted from `NDVI.compute`/`gdal_calc`, `GDALBlock`, and `RST_DEMProcessingHelper`.

## 1. Scope & decisions

- **Families fixed now (confirmed divergent on both sides):**
  - **Terrain (6):** `slope`, `aspect`, `hillshade`, `tri`, `tpi`, `roughness`.
  - **Band-math (4):** `ndvi`, `ndwi`, `nbr`, `mapalgebra` (and the shared `index`/spectral helpers `savi`/`evi` ride along since they use the same read+emit path).
- **Deferred (fast-follow):** focal (`filter`, `convolve`) — heavy behavior confirmed (mask-aware *skip* + window-shrink, **no** border ring), but not in the bench registry, so there's no lightweight consistency evidence yet. The follow-up first adds `filter`/`convolve` to the bench registry, then applies a mask-aware-skip fix (distinct from terrain's border-ring rule).
- **Unchanged:** all reductions/accessors (avg/min/max/median/pixelcount/numbands/width/height — already mask via `read_masks`), summary, histogram, contour (explicitly masks), clip/threshold/init_nodata/band/setsrid (manage nodata correctly), warp (transform/to_webmercator — exact). The heavyweight is **not** touched.
- **Terrain edge convention (decided):** **match GDAL exactly** — the undefined 1-px kernel border becomes NoData (as `gdal.DEMProcessing` does by default, no `-compute_edges`). This is a user-visible change (existing pyrx terrain calls currently return computed edge values); accepted for swap-consistency.
- **Justification:** swap-consistency + correctness ([[pyrx-robustness-over-checkbox]]), not Mosaic parity ([[justify-by-utility-not-mosaic]]). Computing slope/NDVI over a `-9999` sentinel produces meaningless output — the input-NoData masking is a genuine correctness fix; the terrain border-ring is the one convention change.
- **Declared-nodata contingency:** heavy masks input NoData **only when the input band has a *declared* NoData value** (it's `gdal_calc.py`/GDAL default behavior, not GeoBrix code). pyrx mirrors this automatically by keying off `ds.read_masks` (all-valid when no nodata is declared).

## 2. Architecture (Approach A — shared masked-read core + explicit per-family edge rule)

One new focused helper module isolates the single new concept (mask-aware reads + neighborhood propagation); each family stays thin and applies its *own* edge rule (the heavy semantics genuinely differ per family). The already-consistent families are untouched.

**New: `python/geobrix/src/databricks/labs/gbx/pyrx/core/_nodata.py`**

```python
read_masked(ds, band=1) -> (data: np.ndarray[float64], valid: np.ndarray[bool])
    # data = ds.read(band).astype("float64")
    # valid = ds.read_masks(band) != 0
    # When the band has no declared nodata, read_masks() is all-255 → valid all-True
    # → no masking. This mirrors heavy's "mask only when nodata is declared".

nodata_value(ds, default=-9999.0) -> float
    # ds.nodata if set, else `default` (the sentinel to write into output).

propagate_invalid(valid: np.ndarray[bool], size=3) -> invalid: np.ndarray[bool]
    # invalid where ANY pixel in the size×size window is invalid OR out-of-array:
    #   invalid = ~scipy.ndimage.binary_erosion(valid, np.ones((size, size)), border_value=0)
    # border_value=0 (out-of-bounds treated invalid) yields BOTH the input-nodata
    # propagation AND the 1-px border ring in ONE call — matching gdal.DEMProcessing.

emit(template_ds, result: np.ndarray, nodata: float, invalid: np.ndarray[bool],
     dtype: str) -> bytes
    # result = result.copy(); result[invalid | ~isfinite(result)] = nodata
    # write a GTiff from template_ds's transform/crs with the given dtype + profile
    # nodata=nodata. Generalizes the current _emit/_emit_float32.
```

Dependencies: `scipy.ndimage` (already a pyrx dep, used by focal/proximity) and `ds.read_masks` (already used by `accessors._valid_values`). No new third-party deps.

**Unit boundaries:** `propagate_invalid` is pure array→array (testable against hand-built masks); `read_masked` depends only on a rasterio dataset; `emit` is a write round-trip. Each is independently testable and has one responsibility.

## 3. Terrain family (`pyrx/core/terrain.py`)

The math (Horn/Wilson formulas, `_horn_gradients`/`_neighbors`) and the function signatures/args are **unchanged**. Only the read and emit change:

1. `data, valid = read_masked(ds)` (was `ds.read(1).astype("float64")`).
2. Compute the op on `data` exactly as today — keep the internal `np.pad(..., mode="edge")` used to compute interior-adjacent gradients; the padded edge values are discarded by the mask in step 4.
3. `invalid = propagate_invalid(valid)` — the entire behavioral fix: input-NoData propagation through the 3×3 window **and** the 1-px border ring, in one erosion.
4. `emit(ds, result, nodata, invalid, dtype)`.

Per-op nodata/dtype:
- `slope`/`aspect`/`tri`/`tpi`/`roughness` — Float32, `nodata = -9999.0` (matches GDAL DEMProcessing's Float32 nodata).
- `hillshade` — uint8, `nodata = 0` (matches GDAL hillshade; was `nodata=None`).

Result: a nodata-free tile → today's interior values + a NoData border ring; a nodata-bearing tile additionally masks sentinel neighborhoods — both matching heavy.

## 4. Band-math family (`pyrx/core/indices.py`, `pyrx/core/mapalgebra.py`)

Per-pixel (no neighborhood) → uses `read_masked` but **not** `propagate_invalid`.

**`indices.py`** (ndvi/ndwi/nbr/savi/evi + the `index` dispatcher):
1. For each contributing band: `data_i, valid_i = read_masked(ds, idx_i)`.
2. Compute the formula on `data_i` as today (`_normalized_diff`, EVI/SAVI exprs, the numexpr `index` registry).
3. `invalid = (~valid_a) | (~valid_b) | …` (OR of contributing bands' invalid; `emit` additionally folds in non-finite results).
4. `emit(ds, result, nodata=-9999.0, invalid, dtype="float32")`.

**`mapalgebra.py`** (`rst_mapalgebra`): read inputs via `read_masked`, OR their `~valid` into the invalid mask, run numexpr, and `emit` with a declared nodata — fixing **two** gaps the investigation found: (a) it computed over sentinels, and (b) it never set an output nodata at all.

Signatures, the `index` named-formula registry, and numexpr usage are unchanged. The declared-nodata contingency is automatic via `read_masked`.

## 5. Testing & validation

- **New helper tests** (`pyrx/test/pyrx/test_core_nodata.py`): `propagate_invalid` (single interior invalid → its 3×3 neighborhood invalid; border ring always invalid; all-valid interior loses only the ring); `read_masked` (declared-nodata tile → False at sentinels; no-declared-nodata tile → all-True); `emit` round-trip (nodata written, reopens correctly).
- **Per-family behavioral tests** (extend `test_core_*`): terrain (slope) — declared-nodata tile + planted sentinel → output NoData on the border ring *and* the sentinel's 3×3 neighborhood, interior values away from edges/nodata unchanged (regression-pinned); no-declared-nodata tile → only the border ring NoData; hillshade nodata=0. Band-math (ndvi) — planted sentinel → NoData at exactly those pixels (no spread); no-declared-nodata → no masking; `(B+A)=0` → NoData.
- **Update existing goldens** that legitimately change: pyrx terrain/band-math assertions checking edge/nodata values → updated to the new contract (interior-value assertions stay). Doc-test goldens for terrain/band-math examples → regenerated via the Docker doc-test path (`gbx:test:python-docs`), per-package, narrowed to changed nodes.
- **Cross-API acceptance gate:** re-run the consistency sweep (`gbx:bench:gen-data` nodata-free + nodata-heavy tiles → `gbx:bench:heavyweight` + `gbx:bench:lightweight --mode pure-core` → `gbx:bench:compare`). The sweep validates the **9 bench-registered fns**: 6 terrain (slope/aspect/hillshade/tri/tpi/roughness) + 3 band-math (ndvi/ndwi/nbr). **Done = those 9 cells flip `divergent` → `within_tol`/`exact`** on both tiles. The other touched band-math fns (`savi`, `evi`, `index`, `mapalgebra`) share the same `indices.py`/`mapalgebra.py` read+emit path but are **not in the bench registry**, so they're validated by **unit tests only** (their fix is the identical mechanism). Local unit tests (same masking contract as heavy) are the fast TDD loop; the sweep is the authoritative cross-API confirmation for the registered subset.
- **Residual caveat (pre-agreed):** if a terrain cell stays `divergent` after masking is verified correct, that's an interior-algorithm difference (pyrx Horn vs GDAL Horn rounding) — a separate finding, not chased here.
- **Execution:** unit tests in the `uv` venv (`gbx:test:pyrx`); doc-tests + the bench sweep in Docker.

## 6. Out of scope (YAGNI / deferred)
- Focal (`filter`/`convolve`) — fast-follow: add to bench registry, then mask-aware-skip fix (different edge rule: window-shrink, no border ring).
- Any heavyweight change (heavy is the reference; e.g. the band-math `gdal_calc` subprocess slowness is a separate perf item, not behavioral).
- proximity/viewshed/derivedband/clip/threshold — already consistent or out of the NoData-divergence set.
- A `compute_edges` flag (rejected — strict swap contract; heavy has no such flag).
- Masked-array (`numpy.ma`/rioxarray) refactor across the read path (rejected — YAGNI, risks the stable families).

## 7. Risks
- **Interior-algorithm residual:** masking may leave terrain `within_tol` but not `exact` (or, worst case, still `divergent` if pyrx Horn diverges from GDAL Horn beyond the nodata cause). Surfaced by the sweep; handled as a separate finding.
- **Golden churn:** terrain/band-math doc examples + unit goldens change (intended). Bounded to those two families; reductions/warp goldens untouched.
- **hillshade nodata=0 collision:** if a legitimate hillshade value is 0, it would read as NoData. GDAL has the same property (hillshade nodata=0 is GDAL's own default), so this matches heavy — acceptable.

---
*Design approved 2026-06-06 via brainstorming. Next: implementation plan (writing-plans). Sources: the benchmark consistency sweep (`test-logs/bench/sweep/`), code reads of pyrx `terrain.py`/`indices.py`/`mapalgebra.py`/`accessors.py` and heavyweight `NDVI.compute`/`RST_MapAlgebra`/`GDALBlock`/`RST_DEMProcessingHelper`.*
