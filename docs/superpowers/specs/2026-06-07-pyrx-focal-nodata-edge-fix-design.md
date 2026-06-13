# pyrx focal NoData + edge fix — design spec

**Date:** 2026-06-07 · **Branch:** `beta/0.4.0` · **Status:** design approved, pre-plan.
**Goal:** Make pyrx focal ops (`filter` = min/max/median/mean; `convolve`) match the heavyweight (rasterx/`GDALBlock`) on **NoData handling** and **kernel-edge handling**, so `rx.rst_filter/convolve → prx.*` swaps are consistent. Heavyweight is the reference.

## 1. Motivation

The full-coverage benchmark left `rst_convolve` `divergent` (max_rel_delta ~0.012–0.057, `nodata_count_delta=0`). Root cause is **edge handling**, not NoData poisoning (the default corpus tiles are effectively nodata-free, so the NoData path isn't even exercised). `rst_filter` reads `within_tol` only because the corpus is nodata-free and median is edge-robust — on a NoData-bearing tile pyrx would poison while heavy skips. This is the deferred focal item from `2026-06-06-pyrx-nodata-edge-fix-design.md` §6, now root-caused.

## 2. Heavy target semantics (verbatim from `GDALBlock.scala`)

**`valuesAt(x,y,kw,kh)`** (backs avg/min/max/median/mode): iterate the kw×kh window; include a neighbor **only if** `xIndex/yIndex in [0,width/height)` **AND** `mask≠0` **AND** `value≠nodata`. Out-of-bounds neighbors are **skipped** → the window **shrinks at edges** (no padding, no reflect). Aggregate over the collected valid values:
- `avg` = `sum/length` (mean of valid); `min`/`max`/`median`/`mode` over valid.
- If **no** valid neighbor → output **nodata**.

**`convolveAt(x,y,kernel)`**: `sum += value·kernel(i)(j)` **only** for in-bounds valid neighbors; out-of-bounds & invalid contribute **0**; weights are **not** renormalized → edge is effectively **zero-padded**. Kernel is applied **un-flipped** (`kernel(i)(j)` indexed in window reading order → correlation, not convolution).

## 3. pyrx today (`pyrx/core/focal.py`)

- `filt`: `ndimage.{minimum,maximum,median}_filter` / `uniform_filter` on raw arrays — default boundary (`reflect`); no mask.
- `convolve`: `ndimage.convolve(data, k, mode="nearest")` — edge **replication**; kernel **flipped** (scipy `convolve` flips); no mask.

Divergences vs heavy: edge (reflect/nearest vs shrink/zero-pad), kernel orientation (flip vs none), and NoData (poisons vs skips).

## 4. Fix — rewrite `focal.py` to match GDALBlock (reuse `_nodata.py`)

Per band, `data, valid = read_masked(ds, band)` (`_nodata.read_masked` → float64 data + bool valid mask). Then:

- **`convolve`** → `scipy.ndimage.correlate(data * valid, kernel, mode="constant", cval=0.0)`.
  - `correlate` (not `convolve`) → un-flipped kernel, matching `convolveAt`'s `kernel(i)(j)`.
  - `data*valid` zeros invalid pixels; `mode="constant", cval=0` zeros out-of-bounds → both contribute 0, no renormalization. Matches `convolveAt` exactly.
  - Output dtype Float64 (unchanged). NoData: a pixel is output-nodata only where it has **no valid neighbor** — compute `valid_count = correlate(valid, ones_like(kernel!=0), mode="constant", cval=0)`; where `valid_count==0` → nodata. (Heavy `convolveAt` returns 0 for all-invalid, but its mask path marks it; emit nodata there for a clean fingerprint.)
- **`mean`** → `num = correlate(data*valid, box, mode="constant", cval=0)`, `cnt = correlate(valid, box, mode="constant", cval=0)`; `result = num/cnt` where `cnt>0`, else nodata. `box = np.ones((size,size))`. Window-shrinks at edges; skips NoData. Output Float32 (unchanged).
- **`min`/`max`/`median`/`mode`** → set invalid → `np.nan`; `scipy.ndimage.generic_filter(arr, func, size=size, mode="constant", cval=np.nan)` with NaN-aware `func` (`np.nanmin`/`np.nanmax`/`np.nanmedian`/a nan-aware mode). All-NaN window → nodata. Output dtype = input (unchanged) for min/max/median; mode = input dtype.
  - `generic_filter` is slower than the built-ins but correct; acceptable for benchmark/typical tiles. (If a built-in fast path is needed later, optimize then — YAGNI now.)
- **Emit** via `_nodata.emit(ds, result, nodata, invalid, dtype)` (or the equivalent: set profile nodata + write), so the output declares NoData where all-invalid, consistent with terrain/band-math.

Signatures unchanged: `filt(ds, kernel_size, operation)`, `convolve(ds, kernel)`. Bindings (`prx.rst_filter`/`rst_convolve`) and the bench FnSpecs need **no** change.

## 5. Testing & validation

- **Unit (venv, `test_core_focal.py`):**
  - **Edge (nodata-free):** convolve/mean/median on a small tile → border pixels match the GDALBlock rule (zero-pad for convolve; shrink-renormalized mean; shrunk-window median). Pin against hand-computed expected for a 4×4 tile + 3×3 kernel.
  - **NoData skip:** plant a NoData pixel; assert its neighbors' outputs exclude it (mean renormalizes over valid; convolve zero-contributes); a pixel whose entire window is NoData → output NoData.
  - **Kernel orientation:** an **asymmetric** kernel convolve matches correlation (un-flipped), distinguishing the new behavior from the old `convolve` flip.
  - Existing focal tests that assumed reflect/nearest edges get updated to the new (correct) GDALBlock contract — recompute expected, don't weaken.
- **Cross-API acceptance:** re-run the bench for `rst_convolve`/`rst_filter` (pure-core). `rst_convolve` must flip `divergent → within_tol`/`exact` on both tiles. Add a **NoData-bearing focal tile** to the validation (a small nodata sweep, or assert via the unit NoData test) so the NoData-skip path is actually exercised — currently the default sweep is nodata-free and would not catch poisoning.
- **No regression:** `rst_filter` stays `within_tol`/`exact`.

## 6. Out of scope
- Other terrain/band-math (already fixed).
- A fast built-in path for min/max/median (use `generic_filter`; optimize only if a real perf need arises).
- `mode` filter is heavy-only in the bench sense (pyrx `filt` doesn't expose mode today) — keep pyrx's current operation set (min/max/median/mean); only align the shared ones.

## 7. Risks
- **`generic_filter` perf**: slower for large tiles; acceptable for the corpus, and focal isn't a hot path. Flag if a sweep tile is huge.
- **NoData not exercised by default corpus**: must add a nodata tile/test or the fix's NoData half is unverified by the sweep — covered by the unit NoData tests as the authoritative check.
- **Output-nodata for convolve all-invalid**: heavy `convolveAt` returns 0 (mask handled separately); pyrx emitting nodata there is the cleaner/consistent choice and shouldn't disagree on valid interior pixels — verify the fingerprint (stats over valid pixels) still matches.

---
*Design approved 2026-06-07 via investigation (GDALBlock.scala read + bench root-cause). Next: implementation plan (writing-plans). Sources: `GDALBlock.scala` valuesAt/convolveAt, `KernelFilter.scala`, pyrx `focal.py`, bench `comparison.csv` (rst_convolve divergent). See [[pyrx-nodata-edge-divergences]].*
