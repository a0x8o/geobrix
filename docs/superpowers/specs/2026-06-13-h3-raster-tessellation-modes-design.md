# H3 raster tessellation — pedigree, current behavior, and multi-mode design notes

> **Status:** APPROVED design (2026-06-13). Sections 1–5 are the grounding background (pedigree +
> current behavior); sections 6–10 are the approved design. Much of §1–5 is institutional history
> (Mosaic → Databricks-native) — preserve it. Next step after this doc: writing-plans.

## 1. Why this matters — lineage / pedigree

GeoBrix's H3 raster functions descend from a technique **pioneered in DBLabs Mosaic** that has since
**inspired Databricks-native product functions**. The lineage:

- **Mosaic** (vector + raster grid tessellation):
  - vector `grid_tessellate` — https://databrickslabs.github.io/mosaic/api/spatial-indexing.html#grid-tessellate
  - raster `rst_tessellate` — https://databrickslabs.github.io/mosaic/api/raster-functions.html#rst-tessellate
  - raster `rst_rastertogridavg` (and `*count/max/min/median`) — https://databrickslabs.github.io/mosaic/api/raster-functions.html#rst-rastertogridavg
- **Databricks-native (product) H3 functions** that the Mosaic technique inspired:
  - `h3_coverash3` — the **covering set** of a geometry: every H3 cell that *overlaps* it.
    https://docs.databricks.com/aws/en/sql/language-manual/functions/h3_coverash3
  - `h3_tessellateaswkb` — **tessellation**: for each covering-set cell, the geometry **clipped to
    the cell** (the intersection), returned as a WKB "chip".
    https://docs.databricks.com/aws/en/sql/language-manual/functions/h3_tessellateaswkb
- **GeoBrix** deliberately did **NOT** re-port the H3 functions now built into the product (the
  `h3_*` vector functions). It carries the **raster** H3 functions (`rst_h3_tessellate`,
  `rst_h3_rastertogrid*`) and the discrete-grid families. Positioning: GeoBrix is an on-ramp that
  offers the *same pioneered technique* (in **both** light and heavy tiers) and **complements** the
  product's native `h3_*` functions rather than competing with them.

**Implication for docs:** when we surface this, frame GeoBrix as the origin of (and complement to)
the native `h3_coverash3` / `h3_tessellateaswkb` technique — factual, lineage-grounded.

## 2. The reference technique (raster H3), per MLJ

For a raster, the canonical "tessellate-as-WKB" technique is:
- **(a)** Take the tile's extent as a **bbox polygon** ("bbox geom").
- **(b)** Project bbox geom to **EPSG:4326** if needed (or require the caller to pass 4326).
- **(c)** Get the **COVERING SET** of bbox geom — every H3 cell that **overlaps** it. This is NOT
  centroid-`polyfill` (the "is the cell's center inside?" half-in rule).
- **(d)** **Vector-intersect** each covering-set cell's hexagon with bbox geom → a per-cell WKB
  **"chip"** (the clipped piece). Some functions keep the chip; others reduce it to a measure.

H3 v4 primitives:
- **Covering set** = `polygonToCellsExperimental(poly, res, ContainmentOverlapping)` — exact overlap.
- **Centroid polyfill** = classic `polygonToCells` / `polyfill` — center-in-polygon containment.

## 3. Current GeoBrix HEAVY behavior (grounded from code)

(From a read of `src/main/scala/com/databricks/labs/gbx/rasterx/...`; file:line where cited.)

### `gbx_rst_h3_tessellate` — `RST_H3_Tessellate` → `RasterTessellate.tessellateH3Iter`
- **(a) bbox geom: YES** — `BoundingBox.bbox(ds, GDAL.WSG84)` builds a 4-corner extent polygon.
- **(b) projection: reprojects to 4326 internally** (raster-CRS→4326 for the extent; hexagons
  reprojected 4326→raster-CRS for clipping in `ClipToGeom`).
- **(c) cell selection: centroid-`polyfill` on `bbox.buffer(bufR)` — NOT a true covering set.**
  `H3.polyfill(bbox.buffer(bufR), res)`. Uber `h3.polyfill` is centroid-containment; the bbox is
  dilated by `getBufferRadius` (≈ one cell circumradius) to recover fringe cells. An *approximation*
  of a covering set.
- **(d) per-cell output: hexagon-clipped WKB chip** — true H3 hexagon used as a `gdalwarp -cutline
  … -crop_to_cutline` cutline with `CUTLINE_ALL_TOUCHED=TRUE`. **Keep-test = NoData-mask "any valid
  pixel after the cutline"** (`RasterAccessors.isEmpty`), NOT a hexagon-area-coverage threshold.
- **Consequence (measured):** over-includes a **disjoint fringe** (~188–284 cells, *zero* geometric
  overlap with the raster) — an artifact of buffer + bbox-snapped warp + nodata keep-test. Those
  cells are beyond even the true covering set.

### `gbx_rst_{h3,quadbin}_rastertogrid{avg,count,max,min,median}` (10) — diverges from the reference
- **(a) bbox geom: NO** — pure per-pixel walk, no polygon.
- **(b) projection: assumes/requires 4326, no reprojection** (geotransform fed straight to
  `pointToCellID`; non-4326 silently wrong; quadbin docs say "callers reproject via RST_Transform").
- **(c) cell selection: pixel-centroid point sampling** — each valid pixel's 0.5-offset center maps
  to exactly ONE cell (`geoToH3` / `Quadbin.pointToCell`). Emergent set ("cells containing ≥1 valid
  pixel centroid"). Neither covering set nor polyfill+buffer. **This is inherently
  single-assignment per pixel.**
- **(d) per-cell output: scalar MEASURE, no clip** — valid pixels bucketed per cell, then `fAgg`
  (avg/count/max/min/median). No area / partial-pixel weighting; a boundary cell gets whole pixels.

### `gbx_rst_gridfrompoints(+agg)` — inverse direction (points→raster IDW), N/A to the lens.

### Heavy inconsistencies to carry into the design
- **Cell selection differs across the family:** tessellate = polyfill-on-buffered-bbox
  (geometry-driven, approximate covering); rastertogrid = pixel-centroid-to-cell (data-driven,
  single-assignment). **Neither uses a true `all_touched`/overlap covering set.**
- **CRS handling differs:** tessellate reprojects internally; rastertogrid hard-assumes 4326.
- **`getBufferRadius` has only Polygon/MultiPolygon match arms — no default** (MatchError risk).
- The tessellate keep-test is nodata-mask, not hexagon coverage → the disjoint-fringe over-inclusion.

## 4. Current GeoBrix LIGHT behavior

- **`rst_h3_tessellate` (light)** — bbox→4326, `h3.h3shape_to_cells` (centroid polyfill) **+ a
  one-ring `grid_disk` buffer + an `all_touched` pixel-coverage prune** → lands on the **true
  all-touched / overlapping set** (verified `== oracle`: zero misses, zero extras, on both a 4326
  SRTM tile and a reprojected UTM tile). Emits a hexagon-clipped chip via `rasterio.mask`.
  **Caveat:** the prune uses `all_touched=True` but the actual clip uses `all_touched=False`
  (a touch-semantics asymmetry to reconcile).
- **`rst_*_rastertogrid*` (light)** — mirrors HEAVY exactly (pixel-centroid binning, scalar measure,
  no clip, assumes 4326). Light and heavy agree with each other here; both differ from the
  covering-set+clip reference (by design — this is the data-driven binning family).

## 5. The tessellate divergence (root cause)

Measured: light **11958** vs heavy **12242** cells on the same tile (~2.4%, heavy more). All the
heavy-extra cells are **fully disjoint** (zero overlap) — heavy's buffer + bbox-snapped warp +
nodata keep-test admits a fringe ring beyond the covering set. **Light is the correct all-touched
set.** This is a heavy correctness issue, *not* caused by the recent UDTF conversion (which only
changed the call form, not the cell math).

## 6. Approved design — `rst_h3_tessellate` modes (light + heavy aligned)

Scope (MLJ): `rst_h3_tessellate` ONLY — the one diverging function. `rst_h3_rastertogrid*` already
agrees light↔heavy and is out of scope. Two named modes, **identical in both tiers by construction**
(both tiers call the same H3 v4 primitive per mode); the alignment deletes both tiers' hand-rolled
approximations rather than patching them separately.

### 6.1 The `mode` parameter
- Optional trailing **string** param `mode ∈ {"covering", "centroid"}`, **default `"covering"`** —
  matching geobrix's string-enum convention for multi-choice params (`algorithm`, `operation`,
  `split_point_finder`, `format`). A boolean (`useCentroid`) was rejected: modes may grow (e.g.
  area-weighted) and a boolean can't extend without a breaking change.
- **Backward compatible.** SQL is positional-only: heavy `FunctionBuilder` registers arity **2**
  (default `Literal("covering")`) **and 3**, so existing `(tile, resolution)` calls keep working;
  `(tile, resolution, 'centroid')` selects the new mode. Python wrappers (light + heavy):
  `mode: ColLike = "covering"` (positional or `mode=` kwarg; SQL positional only).
- **Validation** follows the rasterx pattern: Scala `require(AllowedSet.contains(...))` + Python
  `ValueError` on a `{"covering","centroid"}` miss, message listing the valid values.

### 6.2 `covering` mode (default) — the pioneered tessellate-as-WKB technique
- **Cell selection:** the **true covering set** of the tile's 4326 bbox via H3 v4
  `polygonToCellsExperimental(bbox, res, ContainmentOverlapping)` — every cell that *overlaps* the tile.
- **Per-cell output:** raster **clipped to the cell's hexagon** with **`all_touched=True`** (boundary
  pixels included), applied consistently in any prune AND the clip (fixes light's prune-vs-clip
  asymmetry; matches heavy's `CUTLINE_ALL_TOUCHED=TRUE`). One tile-struct chip per cell.
- **Semantics:** full coverage of the tile; border cells/pixels are **shared with neighboring tiles**
  (overlap accepted — union across tiles reconstructs a full cell).
- Replaces heavy's polyfill-on-buffered-bbox + nodata keep-test (removing the disjoint-fringe
  over-inclusion) AND light's seed+grid_disk+prune approximation → identical cells by construction.

### 6.3 `centroid` mode (new, additive) — pixel-centroid single-assignment
- **Assignment:** each **pixel** → the single H3 cell whose hexagon contains the pixel's centroid
  (per-pixel `pointToCellID`/`latlng_to_cell` — the **same selection `rst_h3_rastertogrid*` already
  uses**).
- **Per-cell output:** one tile-struct chip per cell holding **only its assigned pixels** (others
  nodata). The cell set emerges from the pixels (cells with ≥1 assigned pixel); no bbox/covering step.
- **Semantics:** a **partition** — every pixel assigned exactly once, **nothing dropped, no
  double-count across tiles** (a pixel belongs to exactly one hexagon globally). The de-duped binning
  case ("assign a set of rasters/tiles to H3 cells without double-counting").
- This is **pixel**-centroid, NOT cell-centroid selection (which would drop border pixels — rejected).

### 6.4 CRS
- Both modes, both tiers reproject the tile extent / pixel coords to **EPSG:4326** internally for the
  H3 lookups (current tessellate behavior — kept). (`rastertogrid`'s hard-4326 assumption is separate
  and out of scope.)

### 6.5 Cross-tier alignment + "no harm"
- Both tiers call the **same H3 v4 primitives** per mode → identical cell sets + chips by construction.
- `covering` (default) is the **corrected** existing behavior — heavy drops its disjoint fringe, light
  drops its approximation. The 0.4.0 H3 capabilities are unreleased WIP, so this is a fix, not a
  back-compat break. `centroid` is purely **additive**. Existing capability is **fixed + extended**.

## 7. Implementation scope

- **Heavy (Scala):** `RST_H3_Tessellate` (+ `RasterTessellate` / `H3`) — add `modeExpr`; covering path
  → `ContainmentOverlapping` covering set (replace polyfill+buffer; the `getBufferRadius` MatchError
  risk disappears); centroid path → per-pixel `pointToCellID` assignment → per-cell chip;
  `FunctionBuilder` arity 2+3; Scala API + heavy Python binding `mode="covering"`; validation.
  **Verify the bundled H3-Java version exposes `polygonToCellsExperimental(ContainmentOverlapping)`**
  (H3 v4). JAR rebuild + tessellate re-bench.
- **Light (pyrx):** `pyrx/core/tessellate.py` + the `rst_h3_tessellate` UDTF/wrapper — add `mode`;
  covering path → the h3-py v4 overlapping-containment call (verify exact API:
  `polygon_to_cells_experimental(..., contain='overlap')` or equivalent) replacing seed+grid_disk+prune;
  centroid path → per-pixel `latlng_to_cell` → per-cell chip; fix the `all_touched` asymmetry; validation.
- `registered_functions.txt` name unchanged (`gbx_rst_h3_tessellate`); update the `function-info.json`
  usage example + docstrings to show the `mode` arg.

## 8. Testing

- **Per-mode light-vs-heavy parity** on a border-containing tile (the regime that exposed the
  divergence): for EACH mode, assert light and heavy produce the **same cell set** AND the **same
  per-cell chip pixels** — passing by construction (same H3 primitive). Replaces the strict-equality
  fan-out bench leg that the divergence tripped.
- **Covering:** cell set == the true overlapping set (vs a `ContainmentOverlapping` oracle); no
  disjoint cells.
- **Centroid:** assert a **partition** — every input pixel appears in exactly one cell's chip; the
  union of chips == all valid pixels; no pixel in two chips.
- Spark-free light core unit tests for both modes; the fan-out bench's `h3_tessellate` leg compares
  per-mode (default covering).

## 9. Docs — H3 explainer page (deliverable; outline to refine with MLJ)

A dedicated H3 page explaining how H3 raster handling works. Draft outline (to refine before writing):
- **Lineage:** Mosaic → Databricks-native `h3_coverash3` / `h3_tessellateaswkb`; GeoBrix as
  origin-of / complement-to the native technique.
- **The two tessellation modes** — `covering` (full coverage, shareable across tiles; the pioneered
  chip technique) vs `centroid` (pixel-centroid single-assignment partition, de-duped) — with a
  "when to use which" guide and a visual of the border behavior (overlap vs partition).
- **Relationship to `rst_h3_rastertogrid*`** — same centroid selection, measure vs chip output.
- **CRS expectations** (4326 internally for tessellate; the rastertogrid 4326 contract).
- **Cross-tier parity** (light ≡ heavy by construction).

## 10. Status / next

**Approved design (2026-06-13).** Next: **writing-plans** → subagent-driven implementation (heavy
Scala + JAR + light + per-mode parity tests + the explainer page).
