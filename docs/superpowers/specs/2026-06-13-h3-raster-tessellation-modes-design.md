# H3 raster tessellation — pedigree, current behavior, and multi-mode design notes

> **Status:** background + design notes (2026-06-13), captured before brainstorm/spec. This is
> foundational pedigree for GeoBrix's H3 raster functions and the basis for a planned multi-mode
> tessellation design. Preserve it — much of this is institutional history (Mosaic → Databricks
> native) that is otherwise easily lost.

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

## 6. Design direction — MULTIPLE MODES, in BOTH tiers

The decision (MLJ): don't pick "one right answer" — **expose multiple tessellation modes, available
in both the light and heavy tiers** (light's mode added to heavy; heavy's pioneered mode added to
light), swappable and consistently named. Candidate modes:

1. **Covering-set + hexagon-clip ("tessellate-as-WKB", the pioneered technique)** — the **true
   covering set** (every overlapping cell) × each cell **clipped to its hexagon** → WKB chip. The
   distinguished, Databricks-native-aligned mode (`h3_coverash3` + `h3_tessellateaswkb`). Likely the
   default.
2. **Polyfill / centroid (single-assignment)** — **explicitly NOT ruled out** (MLJ). Centroid-
   containment so a cell is owned by **exactly one** tile, based on where its centroid lands across
   adjacent tiles → **no double-counting at tile boundaries**. Many users (not familiar with the
   pioneered covering technique) will *expect* this standard polyfill behavior; it's the natural
   single-assignment model. This is also already the *de facto* selection model of the
   `rastertogrid` family (pixel-centroid → one cell).
3. (The existing light "all-touched covering" set is mode #1's selection; the rastertogrid
   data-driven pixel-centroid binning is its own family — relate it to #2's single-assignment idea.)

**Cross-tier goal:** both tiers offer the same modes (a `mode`/`containment` parameter on
`rst_h3_tessellate`, same names/values both tiers), so light↔heavy stays a drop-in swap and parity
tests assert per-mode equality.

## 7. Correctness items to fold into the mode work

- For the **covering** mode, use the **true covering set** (H3 v4 `ContainmentOverlapping`) rather
  than: heavy's polyfill-on-buffered-bbox (over-includes disjoint fringe) OR light's
  ring+prune approximation. That makes both tiers **identical by construction** and removes the
  ~2.4% divergence + the buffer hack.
- Reconcile light's `all_touched` **prune-vs-clip asymmetry** (prune True, clip False).
- `rastertogrid` **CRS handling** (hard-assumes 4326) — document the requirement and/or reproject.
- `getBufferRadius` **MatchError risk** (no default arm) — only relevant if any buffer path remains.

## 8. Open questions (for the brainstorm/spec)

- Exact mode set, parameter name/values, and **default** (lean: covering-set+clip, to match the
  pioneered technique + native `h3_tessellateaswkb`).
- Does the **polyfill/centroid** mode apply to `rst_h3_tessellate` only, or also define a
  single-assignment variant relevant to the `rastertogrid` family?
- **Per-cell output per mode**: hexagon-clipped chip (WKB/raster) vs scalar measure vs cell-id only.
- Should `rastertogrid` gain covering-set / area-weighted variants, or stay pixel-centroid binning?
- Scope of heavy changes (Scala + JAR rebuild + tessellate re-bench) and light changes; **per-mode
  cross-tier parity tests**. Quadbin equivalents (`Quadbin.polyfillBbox` exists but is unused).

## 9. Next step

Take this through a short **brainstorm → spec** (it's a cross-tier behavior change + new option),
then implement subagent-driven. This notes doc is the grounding input.
