# Benchmark Phase 2 — bucket C (multi-input / constructors / tiling / readers) — design spec

**Date:** 2026-06-07 · **Branch:** `beta/0.4.0` · **Status:** design (recommended basis), pre-plan.
**Goal:** Add the 14 bucket-C `rst_*` functions to the heavy-vs-light benchmark, taking coverage **70 → 84 / 107**. These are the "special-shaped" functions the harness deferred (multi-tile inputs, collection outputs, byte/path readers). Each registers `FnSpec.sources` and is validated via the new `gbx:bench:changed` into the authoritative store.

## 1. The 14 functions, by shape (from binding signatures)

| Group | Functions | In | Out | Harness need |
|---|---|---|---|---|
| **C1 readers / single-tile** | `fromcontent`(bytes,driver), `fromfile`(path,driver), `tryopen`(tile→bool), `buildoverviews`(tile,levels,resampling) | bytes / path / one tile | tile / bool | byte/path input adapter; `tryopen`→scalar, `fromcontent`/`fromfile`/`buildoverviews`→raster |
| **C2 subdataset metadata** | `subdatasets`(tile→MAP), `getsubdataset`(tile,name→tile) | tile | map / tile | GTiff has **no subdatasets** → timing-only |
| **C3 multi-tile IN** | `frombands`(ARRAY→tile), `combineavg`(ARRAY→tile), `merge`(ARRAY→tile) | **array of tiles** | tile | **array-input adapter** + synthesized multi-tile inputs |
| **C4 multi-tile OUT** | `maketiles`(tile,mb→ARRAY), `retile`(tile,w,h→ARRAY), `tooverlappingtiles`(tile,w,h,ov→ARRAY), `separatebands`(tile→ARRAY), `xyzpyramid`(tile,minz,maxz→ARRAY) | one tile | **array of tiles** | **collection fingerprint** |

## 2. Decisions (recommended basis)

- **C4 → collection fingerprint** (not timing-only). New fingerprint kind `raster_collection`: `{count: N, bands_total, agg: {min,max,mean,std} over all output tiles' band-0 (or all-band) pixels}`. Consistency compares `count` (exact) + the aggregate stats (tolerance) — a real swap-safety signal. Heavy `RST_*.execute` returns a tile array; pyrx returns a list of tile bytes — both reduce to the same collection fingerprint.
- **C3 → synthesize inputs from the corpus** (no new corpus fixture): `frombands` ← the 2 bands of the corpus tile split into 2 single-band tiles; `combineavg` ← 2–3 copies of the corpus tile (aligned); `merge` ← 2 variants with **offset extents** (shift the geotransform origin so the mosaic spans a union). Synthesize inside the bench adapter from the one corpus tile the runner already has. New **array-input adapter**: core_fn receives the synthesized list of open ds; col_fn builds an `ARRAY<tile>` Spark column; heavy dispatch builds the tile array.
- **C2 → timing-only** (`fingerprint=False`, pure-core): `subdatasets` returns an empty MAP on plain GTiff (times, not compared); `getsubdataset` has no subdataset to extract on GTiff — register **timing-only with a note**, and if it errors on the corpus, mark it `na`/skip with the reason surfaced (don't fake a subdataset). A dedicated NetCDF/HDF fixture is disproportionate for 2 functions (revisit only if subdataset coverage is later required).
- **C1 → fits the harness with a byte/path adapter:** `fromcontent`(corpus tile bytes + "GTiff"), `fromfile`(corpus tile path + "GTiff") → both read the same source → raster fingerprint should match. `tryopen`(valid tile bytes) → scalar bool (1.0 both). `buildoverviews`(tile, levels=[2,4], "average") → raster fingerprint of the **base band** (overviews are internal; base band unchanged → exact/within_tol).
- **Sequencing within Phase 2:** **C1 → C3 → C4** (C1 fits the harness fastest; C3 adds the array-input adapter; C4 adds the collection fingerprint). C2 timing-only throughout. Each sub-step lands its functions in the store via `gbx:bench:changed`.

## 3. Architecture / harness additions

- **Byte/path input adapter (C1):** the bench currently hands `core_fn` an open `ds` (a tile). For `fromcontent`/`fromfile`/`tryopen`, the input is bytes/path, not a derived tile. Extend `FnSpec` with an `input_kind` (default `"tile"`; new `"bytes"`, `"path"`) so the runner passes the corpus tile's raw bytes / file path instead of an opened ds. Heavy dispatch + col_fn mirror (heavy `RST_FromContent.execute(bytes, driver)` etc.).
- **Array-input adapter (C3):** `input_kind="tile_array"` → the runner synthesizes the input list (a small `_synthesize(ds, fn)` helper: split-bands / copies / offset-variants) and passes a list of ds to core_fn; col_fn wraps an `ARRAY<tile>` column; heavy builds the Scala tile array. Synthesis is deterministic from the corpus tile.
- **Collection-output fingerprint (C4):** add `BenchFingerprint.ofCollection(tiles)` (Scala) + the pyrx equivalent → serialize `{kind:"raster_collection", count, agg-stats}`. `compare.py` `compare_fingerprints` gains a `raster_collection` branch: `count` must match exactly (else divergent), agg-stats compared with the existing tolerance. `_STATS` reused.
- **`sources` for all 14** (per the lifecycle design): pyrx core module (`agg.py`/`tiling.py`/`ops.py`/`accessors.py`/`xyz.py`) + heavy `RST_<Name>.scala` (+ shared `PixelCombineRasters.scala` for frombands/combineavg/merge; tiling helpers as applicable).
- **Registry/scorecard:** all 14 `core=False`, added to `select(set="full")`; `gbx:bench:status` then shows **84/107**; the not-yet-covered list drops to the 23 bucket-B + bucket-A/D functions (Phases 3–4).

## 4. Per-function fingerprint summary

- **Compared (raster / scalar / collection):** `fromcontent`, `fromfile` (raster — re-read of same source), `tryopen` (scalar bool), `buildoverviews` (raster base band), `frombands`/`combineavg`/`merge` (raster — deterministic synthesized inputs), `maketiles`/`retile`/`tooverlappingtiles`/`separatebands`/`xyzpyramid` (raster_collection).
- **Timing-only (`fingerprint=False`):** `subdatasets`, `getsubdataset` (no subdatasets on the GTiff corpus). Any C-function whose synthesized input/op can't be made cross-engine-identical is downgraded to timing-only **with an explicit note**, not forced.

## 5. Testing & validation

- **Unit (venv):** registry well-formedness for the 14 (sources exist, modes/fingerprint valid); the synthesis helper (`_synthesize` produces the expected band-split/copies/offset-variants); the `raster_collection` fingerprint round-trips and `compare_fingerprints` flags a count mismatch as divergent + tolerates agg-stat noise; `tryopen` scalar path.
- **Cross-API acceptance (Docker, via the lifecycle commands):** `gbx:bench:changed` (or scoped `gbx:bench:all --functions <C set>`) over the 14 → write store records; `gbx:bench:status` shows **84/107**, the C functions' consistency (most within_tol; record + classify any divergent as a finding, e.g. `merge` mosaic edge or `combineavg` NoData — investigate, don't loosen). Run **backgrounded with ~30s status** (Docker).
- **Store integration:** each C function ends with an authoritative record + `sources`; `gbx:bench:status` stale=0 after validation.

## 6. Sequencing & out of scope
- C1 (4) → C3 (3) → C4 (5); C2 (2) timing-only alongside. Then Phases 3 (bucket B: DGGS/vector-out, 13) and 4 (bucket A aggregators + D geometry-in, 10) reach 107/107.
- Out of scope: a NetCDF/HDF subdataset fixture (C2 stays timing-only); the `_agg` aggregators (Phase 4); DGGS/vector-out fingerprints (Phase 3).

## 7. Risks
- **Collection fingerprint semantics:** tile ordering may differ heavy-vs-light → aggregate over the *whole collection* (order-independent stats), and compare `count` exactly + stats by tolerance. Don't rely on per-tile pairing.
- **C3 synthesis identity:** the synthesized inputs must be byte-identical across engines (same split/copies/offsets) or the comparison is meaningless — synthesize from the same corpus tile with deterministic transforms; if heavy can't receive the identical synthesized array cleanly, downgrade that fn to timing-only + note.
- **`merge`/`combineavg` may surface real divergences** (mosaic edge / NoData averaging) — those are findings for the scorecard (candidate consistency fixes), not tolerance loosening.
- **`getsubdataset` on GTiff** likely errors/empty — keep timing-only and surface the reason; do not fabricate a subdataset.

---
*Design (recommended basis) 2026-06-07. Next: implementation plan (writing-plans), sequenced C1→C3→C4 with C2 timing-only; validate via `gbx:bench:changed` into the store. Sources: bench `spec.py`/`compare.py`/`BenchDispatch.scala`/`store.py`, pyrx `functions.py` bindings, bucket-C investigation. See [[benchmark-lifecycle-design]] (the store/changed/status infra this plugs into).*
