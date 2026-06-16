# Terrain CRS-scale alignment (GDAL-normal) — design spec

**Date:** 2026-06-06 · **Branch:** `pyrx-0.4.0` · **Status:** design approved (substance + 2 key decisions), pre-plan.

**Goal:** Make the gradient-based terrain functions — `slope`, `aspect`, `hillshade` — produce **GDAL-3.11-normal** output in *both* engines (heavyweight Scala/gdaldem and lightweight pyrx/numpy), so that (a) a `rx.rst_* → prx.rst_*` swap is consistent and (b) both match standalone `gdaldem`. The heavyweight is **not** a frozen reference here — these functions are all new and may change to align to GDAL-normal (user direction, 2026-06-06).

## 1. Background / root cause

A consistency sweep + code reads (see [[pyrx-nodata-edge-divergences]]) found `rst_hillshade` diverges heavy-vs-light (~0.99) and `rst_aspect` diverges on the geographic tile (~1e-3, was mis-attributed to float rounding). The unifying root cause:

- **GDAL 3.11+** (heavy native libgdal **3.11.4**; light rasterio bundles **3.12.1**): *"if none of -scale, -xscale and -yscale are specified, and the CRS is a geographic or projected CRS, gdaldem will automatically determine the appropriate ratio from the units of the CRS."*
- The corpus tile is **EPSG:4326**, res **0.0001°**. On the heavy side:
  - `RST_Hillshade`/`RST_Aspect` omit `-s` → GDAL auto-scales (degree→metre) → correct relief.
  - `RST_Slope` **forces `-s 1.0`** → suppresses auto-scale → saturates (~90° everywhere). This is a wart.
- pyrx `slope`/`aspect`/`hillshade` hand-roll Horn on **raw pixel-size gradients** with **no CRS awareness** → on a degree grid the gradients are ~10⁵× too large.
- **Why the sweep looked the way it did:** `slope` *falsely agreed* (both engines degree-naive/saturated — heavy `-s 1.0`, pyrx scale=1.0); `hillshade` diverged (heavy auto-scales, pyrx doesn't); `aspect` diverged anisotropically on the geographic tile only. **TRI/TPI/Roughness are difference-based (no division by pixel size) → scale-invariant → already consistent** (out of scope).

## 2. Target behavior — "GDAL normal" (GDAL 3.11 auto-scale)

**Exact GDAL 3.11.4 formula** (verbatim from `apps/gdaldem_lib.cpp` lines 3652–3710, the `GDALDEMProcessing` defaulting block triggered when `std::isnan(psOptions->xscale)`):

```c
xscale = 1; yscale = 1;
double zunit = 1;                          // from band GetUnitType(): "m"→1, "ft"→0.3048,
                                           // "us-ft"→US-foot-conv, ""→1 (assume metre), else warn+1
if (poSrcSRS && poSrcSRS->IsGeographic()) {
    const double dfAngUnits = poSrcSRS->GetAngularUnits();        // degrees → π/180 ≈ 0.0174532925
    yscale = dfAngUnits * poSrcSRS->GetSemiMajor() / zunit;       // WGS84: π/180·6378137 = 111319.4908
    const double dfMeanLat = (adfGT[3] + nYSize * adfGT[5] / 2) * dfAngUnits;   // centre lat, RADIANS
    // (warns if |meanLat| > 80°)
    xscale = yscale * cos(dfMeanLat);                            // anisotropic
} else if (poSrcSRS && poSrcSRS->IsProjected()) {
    xscale = poSrcSRS->GetLinearUnits() / zunit;                 // metre → 1.0; foot → 0.3048
    yscale = xscale;                                             // isotropic
}
// else (no/unknown CRS): xscale = yscale = 1.0
```

GDAL **applies** the scale as `gradient = (weighted Horn sum) / (pixel_size · scale)` (see `inv_ewres_xscale = 1/(adfGeoTransform[1]·xscale)`, `inv_nsres_yscale = 1/(adfGeoTransform[5]·yscale)` in the alg-data setup). Since pyrx's `_horn_gradients` already divides by `pixel_size`, pyrx applies the scale as an **extra divisor**: `dzdx /= xscale`, `dzdy /= yscale`.

**pyrx reimplementation (numpy/rasterio + pyproj):**
- `dfAngUnits`: degrees → `π/180` (virtually all geographic CRS; can read from pyproj if non-degree).
- `GetSemiMajor()`: `pyproj.CRS(ds.crs).ellipsoid.semi_major_metre` (EPSG:4326 → 6378137.0).
- `dfMeanLat` (radians) = `(ds.transform.f + ds.height * ds.transform.e / 2) * (π/180)` — `transform.f` = y-origin (deg), `transform.e` = y pixel size (negative). Then `xscale = yscale * cos(dfMeanLat)`.
- Projected: `GetLinearUnits()` = `pyproj.CRS(ds.crs).axis_info[0].unit_conversion_factor` (metre → 1.0).
- `zunit` from band unit (rasterio `ds.units[band-1]`); default 1.0 (metre) when unset.

(Anisotropy — `xscale ≠ yscale` on geographic — is why **aspect** shifts: `dzdx`/`dzdy` are divided by different scales. This is the real cause of the aspect "1e-3 rounding" residual.)

## 3. Decisions (approved)

1. **pyrx scale API:** auto-scale by default (CRS-derived, GDAL-normal). Expose optional `xscale: float | None = None`, `yscale: float | None = None` overrides on `slope`/`aspect`/`hillshade`, mirroring gdaldem's `-xscale`/`-yscale` (anisotropic-capable). **Drop** slope's current `scale: float = 1.0` parameter/default (degree-naive). When both overrides are `None` → auto-derive; when given → use as-is.
2. **Heavy `RST_Slope`:** **drop scale from the default registration.** The 1-arg `rst_slope(tile)` and 2-arg `rst_slope(tile, unit)` emit **no** `-s` (GDAL auto-scales). Only the explicit 3-arg `rst_slope(tile, unit, scale)` emits `-s <scale>`. The Scala `builder()` already branches on arity; adjust so the 1/2-arg paths construct without a scale literal (or with a sentinel that the helper omits).

## 4. Architecture

### 4.1 Light (pyrx) — `python/.../pyrx/core/terrain.py`
- **New shared helper** `_gdaldem_scale(ds) -> (xscale, yscale)`: replicate §2. Reads `ds.crs` (rasterio CRS): `is_geographic` → anisotropic lat formula (centre lat from `ds.transform`/`ds.bounds`, semi-major from the CRS via pyproj/`ds.crs` — pin the WGS84 `a` and confirm projected-units path against GDAL source); `is_projected` → linear-units scale; else `(1.0, 1.0)`.
- `_horn_gradients(ds)` stays as-is (raw per-pixel-size gradients). Each of `slope`/`aspect`/`hillshade`:
  - resolve `(xs, ys)` = explicit overrides if both given, else `_gdaldem_scale(ds)`.
  - divide: `dzdx /= xs`, `dzdy /= ys` (GDAL applies scale as a divisor of the gradient), then run the existing slope/aspect/hillshade math on the scaled gradients.
- `slope`: remove `scale` param, add `xscale`/`yscale`. `aspect`/`hillshade`: add `xscale`/`yscale`. Signatures otherwise unchanged. NoData/edge handling (the `_nodata.py` `read_masked`/`propagate_invalid`/`emit` path) unchanged.
- `tri`/`tpi`/`roughness`: **untouched** (scale-invariant).

### 4.2 Heavy (Scala) — `RST_Slope.scala`
- `execute(ds, unit, scaleOpt)`: only append `Seq("-s", scale.toString)` when scale is explicitly provided. `builder()`: arity 1/2 → construct slope without forcing a scale (omit `-s`); arity 3 → pass the user scale. `functions.scala` `rst_slope(tileExpr)` / `rst_slope(tile, unit)` convenience overloads must NOT inject `lit(1.0)` as a scale that emits `-s`.
- aspect/hillshade/tri/tpi/roughness Scala: **no change** (already omit `-s`). Verify by re-reading.

### 4.3 Bench — `python/.../bench/spec.py` + `src/test/.../bench/BenchDispatch.scala`
- Remove the `scale: 1.0` passed to pyrx `slope` and the `-s 1.0`/scale=1.0 on heavy slope (both were masking the divergence). Both engines now use auto-scale on slope, matching aspect/hillshade.
- Keep a projected-CRS tile in the corpus so the sweep exercises both auto-scale branches (geographic + projected). If the corpus lacks a metre-CRS DEM tile, add one in datagen.

## 5. Testing & validation

- **Unit (venv, pyrx):** new `test_gdaldem_scale_*` — geographic 4326 tile → `(xscale≈yscale·cos(lat), yscale≈111319.49)`; projected metre tile → `(1.0, 1.0)`; no-CRS → `(1.0, 1.0)`. Per-fn: `slope`/`aspect`/`hillshade` on a geographic tile now divide gradients by the auto-scale (assert the scaled-gradient result, not the raw); explicit `xscale`/`yscale` override path; metre tile unchanged. Update existing slope/aspect/hillshade goldens that change (compute new expected, don't weaken). TRI/TPI/roughness tests unchanged.
- **Heavy (Docker):** Scala suite for `RST_Slope` — 1/2-arg omit `-s` (auto-scale), 3-arg emits `-s`. Re-read aspect/hillshade tests still green.
- **Cross-API acceptance sweep (authoritative):** re-run pure-core sweep. **Done =** `slope`, `aspect`, `hillshade` all flip to `within_tol`/`exact` on the **geographic** tile AND stay `within_tol` on the **projected** tile; tri/tpi/roughness/band-math stay `within_tol`; `nodata_count_delta=0` throughout. (Formula is pinned exactly in §2; the sweep against real gdaldem 3.11.4 is the final confirmation.)
- **Doc tests:** regenerate any terrain doc-example goldens (Docker, per-package, narrowed to changed nodes).

## 6. Out of scope
- TRI/TPI/Roughness (scale-invariant).
- Focal (`filter`/`convolve`) NoData fix (separate fast-follow, already tracked).
- High-latitude accuracy beyond GDAL's own `cos(lat)` approximation (GDAL itself documents this limitation).
- Any non-terrain function.

## 7. Risks
- **Exact GDAL formula:** `meanLat` definition + projected linear-units path must match GDAL 3.11.4 precisely or the sweep won't hit `within_tol`. Mitigation: extract from in-container GDAL source; bench is the oracle; iterate.
- **Breaking change:** default `slope`/`hillshade`/`aspect` output changes on geographic rasters in BOTH engines (they become auto-scaled/correct). Beta, no aliases — document in `docs/docs/beta-release-notes.mdx`.
- **pyrx CRS access:** rasterio `ds.crs` semi-major axis — confirm pyproj is available in the pyrx venv (it ships with rasterio's deps) or derive `a` from the CRS WKT; fall back to WGS84 `a` for EPSG:4326.
- **Cross-language naming/parity:** removing slope's `scale` arg touches the Scala signature, `functions.py` binding, `registered_functions.txt`, and `function-info.json` — run `gbx:test:bindings`.

---
*Design approved 2026-06-06. Next: implementation plan (writing-plans). Sources: GDAL 3.11 gdaldem docs (auto-scale note), `gdaldem_lib.cpp`, root-cause investigation (`test-logs/bench/round2/`), code reads of pyrx `terrain.py` + heavy `RST_Slope`/`RST_Hillshade`/`RST_Aspect`/`RST_DEMProcessingHelper`.*
