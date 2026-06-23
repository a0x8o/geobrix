# H3 cell rasterizer (`rst_h3_rasterize_agg` + `rst_h3_gridspec`) — Design

**Date:** 2026-06-23
**Branch:** `beta/0.4.0`
**Status:** design (pending user review)

## Goal

Add a **DGGS-cell rasterizer** to RasterX: rasterize a set of H3 cell ids (each
carrying a value) into a raster tile — the inverse of the existing
`rst_h3_rastertogrid*`. Scope is **step (2)** of the customer pipeline only; step
(1) polyfill is done upstream, and step (3) band-stacking reuses the existing
`rst_frombands_agg`.

## Motivation

Customer (telco) pipeline: transmitter coverage multipolygons over ~80
signal-strength thresholds → `h3_polyfill` at res 12 (done) → **rasterize the
cellids grouped by `(TxId, SourceYear, SourceMonth, Threshold)`** → stack the
per-threshold rasters as bands. GeoBrix has rich raster→DGGS support
(`rst_h3_rastertogrid*`, `rst_h3_tessellate`) and vector→raster (`rst_rasterize`,
`rst_rasterize_agg`), but **no cell→raster primitive in either tier** — confirmed
gap. This feature fills it, symmetric with the raster→DGGS path.

## Scope

In scope:
- `rst_h3_rasterize_agg` — grouped aggregator: H3 cells (+ optional value) → tile.
- `rst_h3_gridspec` — grouped helper: H3 cells → the complete shared output grid
  (snapped origin + pixel size + dims + srid) so per-threshold bands stack aligned.
- Both tiers: the rasterizer is a heavyweight Scala UDAF + a lightweight pyrx grouped
  `pandas_udf`; the grid-spec helper is a scalar bbox + native `min/max` + snap in both tiers.
- Validation (CI synthetic round-trip + a committed FCC fixture) and a
  DEM-isoband notebook example.

Out of scope (separate follow-ups, after H3 proves out):
- `rst_quadbin_rasterize_agg` / `rst_quadbin_gridspec`.
- `rst_bng_rasterize_agg` / `rst_bng_gridspec`.
- A scalar (non-agg) or scalar-over-array form — agg-only for now.
- A boundary-polygon-burn mode (centroid-only for now; could be a future `mode=`).
- Step (1) polyfill and step (3) `rst_frombands_agg` stacking (already exist).

## Algorithm: pixel-centroid burn (inverse of `rst_h3_rastertogrid`)

For each output pixel, take its **center**, convert to lon/lat (unproject if the
output CRS is projected), call `h3.latlng_to_cell(lat, lon, resolution)`, and burn
the cell's value if that cell is in the group's set; otherwise NoData (-9999.0).

- This is the exact inverse of `rst_h3_rastertogrid*` (which sends each pixel
  centroid *to* a cell), so a `rastertogrid → rasterize` round-trip is near-lossless
  at a matching grid/resolution.
- H3 hexagons tile without gaps/overlap, so every pixel center maps to exactly one
  cell — a clean partition, no edge double-counting.
- **Resolution is inferred from the cells** (`h3.get_resolution`); all cells in a
  group must share one resolution (validate; error if mixed). This makes the
  function resolution-agnostic (res-8 FCC data and res-12 customer data exercise the
  same code path).
- **Implementation note (perf):** do not H3-index every pixel in a large grid
  blindly. Build the `cell→value` map once per group; for the output grid compute
  pixel-center lon/lat via the affine transform, then index per pixel (the existing
  `gridagg._h3_cells` scalar-loop pattern). Bound the work to the cells' bbox.
  The heavy tier mirrors `RST_H3_RasterToGrid`'s centroid math
  (`xGeo = gt[0] + (px+0.5)*gt[1] + (py+0.5)*gt[2]`).

## Output grid (extent, pixel size, CRS)

- **Default — fully auto:** extent = union bounding box of the group's cell
  boundaries; pixel size derived from the H3 resolution (so each cell maps to ≥1
  pixel — e.g. pixel ≈ average hexagon edge length at that resolution). One-call
  convenience.
- **Overrides (full control):** caller may supply any of `pixel_size`, or an explicit
  `xmin, ymin, xmax, ymax, width, height` (like `rst_rasterize`). For aligned band
  stacking, feed every threshold the *same* grid from `rst_h3_gridspec` (see below).
- **CRS:** default **EPSG:4326** (H3 is natively WGS84; simplest, no reprojection).
  Optional projected `srid` → pixels are uniform **meters** (square on the ground);
  pixel centers are unprojected to lon/lat for the H3 lookup. A 4326 result reprojects
  to any CRS post-hoc via the existing `rst_transform(tile, target_srid)` (both tiers).
  Note: a fixed-degree pixel in 4326 is uniform in degrees, not meters (longitude
  degrees shrink with `cos(lat)`); use a projected `srid` when metric uniformity matters.

## Functions

### `rst_h3_rasterize_agg`

Grouped aggregator. One H3 cell id per row; accumulate per group; burn on eval.

**Signature (SQL / both tiers):**
```
rst_h3_rasterize_agg(
  cellid     BIGINT,           -- H3 cell id (one per row)
  value      DOUBLE,           -- optional; omitted/null -> 1.0 (presence mask)
  srid       INT,              -- optional; default 4326
  pixel_size DOUBLE,           -- optional; default auto from H3 resolution
  xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,  -- optional explicit extent
  width INT, height INT,       -- optional explicit dims (alt to pixel_size)
  mode STRING, kring_pad INT   -- optional auto-extent controls (default 'centroids', 1)
) [GROUP BY ...] -> tile
```
- **Grid:** when an explicit extent (`xmin…height`) is supplied (e.g. from
  `rst_h3_gridspec` for aligned stacking), it is used as-is. Otherwise the extent is
  derived per `mode` + `kring_pad` exactly as in `rst_h3_gridspec`, on the same snapped
  global lattice — so a standalone call and a per-cell-then-merge call agree.
- **Value:** burns the per-cell `value`; default `1.0` over covered cells with
  NoData elsewhere (presence mask) when `value` is omitted/null. Cells of one
  resolution don't overlap, so there is no within-group burn conflict; if two rows
  carry the same cell with different values, last-wins after a deterministic sort
  (mirrors `rst_rasterize_agg`'s canonical ordering).
- **Heavy:** UDAF modeled on `RST_RasterizeAgg` (accumulate cell ids + values, cap
  buffer at the same 200 MiB guard, build the grid via
  `VectorRasterBridge.buildEmptyRaster`, burn by centroid, return the
  `STRUCT<cellid, raster, metadata>` tile with `cellid=0`).
- **Light:** new grouped `pandas_udf` in pyrx. Per the established light-agg
  convention, the **light SQL aggregate returns `BINARY`** (raster bytes), and the
  Python wrapper `rx.rst_h3_rasterize_agg(...)` composes the full tile struct
  (BINARY → `rst_fromcontent`-style wrap). Document the SQL return-type deviation as
  an orange `:::warning` (both return types + the PySpark `StructType`-in-grouped-agg
  reason + the GROUP-BY recovery example), exactly as the other `rst_*_agg` light
  functions do.

### `rst_h3_gridspec` (shared-grid / canvas definer)

Defines the **complete shared output grid** over a cell set — the "canvas" every
band paints onto — so all thresholds of a transmitter rasterize to a
**byte-identical transform** and the per-threshold bands stack cleanly via
`rst_frombands_agg`. It returns not just a bounding box but the full affine grid
(origin + pixel size + dims + srid); its job is to *negotiate one shared reference
frame* for the bands, not to process pixel data.

**Return — the full grid spec:**
```
STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
       pixel_size DOUBLE, width INT, height INT, srid INT>
```

**Bounds mode + padding.**
- `mode='centroids'` (default) — bounds = bbox of the cell **centroids**, then
  ±half-pixel so the extreme centroids land on corner pixel *centers*. Compact;
  ≈1 pixel per cell at the default pixel size.
- `mode='spatial_envelope'` — bounds = OGC **envelope** of the cell geometries
  (full hexagon footprints). Use with sub-cell pixels when you want complete cell
  shapes rendered.
- `kring_pad` (int, default `1`) — expand the cell set by N H3 rings (`h3.grid_disk`)
  **before** computing bounds, in *both* modes. The pad cells are **NoData** — they
  only enlarge the canvas. Purpose: (a) preserve each cell's full *value* footprint
  instead of chopping it at its centroid; (b) provide a margin for the per-cell
  independent-rasterize→merge pattern (below); (c) de-degenerate the single-cell
  case (a lone cell's centroid is a point → `kring_pad=1` uses its 6 neighbors'
  centroids). `kring_pad=0` → tight bounds (centroids: 1 px / chop at centroid;
  spatial_envelope: the tight single-hexagon envelope). For large sets the ring
  expansion may be approximated by padding the bbox by `N × cell_spacing` rather
  than materializing every kring cell.

**Grid alignment / snapping (prevents half-pixel shifts when stacking):**
- Compute the bounds per `mode` + `kring_pad` above (in `srid`).
- Choose `pixel_size` (auto from the H3 resolution — e.g. a clean fraction of the
  cell edge so each cell maps to a consistent integer pixel count — or caller-given).
- **Snap the origin to the pixel grid:** `xmin = floor(bounds.xmin / pixel_size) *
  pixel_size`, `ymax = ceil(bounds.ymax / pixel_size) * pixel_size`; then
  `width = ceil((bounds.xmax - xmin)/pixel_size)`, `height = ceil((ymax - bounds.ymin)/pixel_size)`,
  and `xmax = xmin + width*pixel_size`, `ymin = ymax - height*pixel_size`.
- Result: a deterministic, pixel-aligned grid on a **global lattice** (origin is a
  multiple of `pixel_size`, independent of which cells are present). Two consequences:
  every threshold of a Tx handed the *same* struct shares one origin/resolution/extent
  (clean band stacking), **and** cells rasterized *independently* land on the same
  lattice — so per-cell tiles abut/overlap exactly and `rst_merge_agg` mosaics them
  losslessly (see the merge pattern below).

**Implementation (both tiers, no custom UDAF):** a *scalar* per-cell bbox plus
native Spark `min/max` group aggregates, then the snap arithmetic. This avoids the
grouped-`pandas_udf` `StructType`-return limitation (see
[[light-agg-struct-return-convention]]) and is identical in both tiers.
- Scalar per-cell bbox `h3_cell_bbox(cellid, srid)` (centroid point in `mode='centroids'`,
  hexagon envelope in `mode='spatial_envelope'`) — a scalar UDF from
  `h3.cell_to_boundary` / `cell_to_latlng` / `H3.cellIdToGeometry`, reprojected to `srid`;
  `kring_pad` applied via `h3.grid_disk`.
- Union bounds = `groupBy(...).agg(F.min(...), F.max(...))` in native Spark SQL; the
  snap step produces the final grid struct.
- Exposed as a **Python/DataFrame helper** `rx.rst_h3_gridspec(df, cell_col="cellid",
  *group_cols, srid=4326, pixel_size=None, mode="centroids", kring_pad=1)` returning the
  grouped DataFrame with a `grid STRUCT<...>` column; SQL users build the same recipe
  from the scalar `gbx_h3_cell_bbox`. (Final name is a plan-time decision; the `_agg`
  suffix is intentionally NOT used — this is a spatial reduction / grid definer, not a
  Spark aggregator UDAF.)
- Usage for stacking: compute the grid over `(TxId, SourceYear, SourceMonth)`
  (union across all thresholds) → join the `grid` struct back onto each
  `(…, Threshold)` group → pass `grid.xmin/ymin/xmax/ymax/width/height/srid` into
  `rst_h3_rasterize_agg`. All thresholds then share one canvas.

**Per-cell independent rasterize → merge (first-class pattern).** Because the snapped
grid lives on a global lattice, you can rasterize each cell (or small cell group)
*independently* — `rst_h3_rasterize_agg` with `kring_pad ≥ 1` so each cell's full
footprint plus a margin is captured — and then mosaic the resulting tiles with the
existing `rst_merge_agg`. The per-cell tiles are lattice-aligned, so the merge is
lossless and order-independent. This is the embarrassingly-parallel alternative to
rasterizing a whole group against one big canvas.

## Data flow (customer + example)

```
cells(Threshold, TxId, Year, Month, cellid_12)
  │  -- per-transmitter common grid (union across thresholds)
  ├─► rx.rst_h3_gridspec(cells, "cellid", "TxId","Year","Month") -> grid(shared canvas struct)
  │
  └─► join grid  ─►  groupBy(TxId,Year,Month,Threshold)
                      .agg(rst_h3_rasterize_agg(cellid, lit(1),
                           srid=grid.srid, xmin=grid.xmin, ..., width=grid.width, height=grid.height))
                      AS band_tile                      -- one aligned band per threshold
        │
        └─► groupBy(TxId,Year,Month)
              .agg(rst_frombands_agg(band_tile, Threshold_rank))  -- stack bands
              AS coverage_stack
```

## Error handling

- Mixed resolutions within a group → clear error naming the offending resolutions.
- Empty group → null tile (no rows to burn).
- Invalid/0 cell id → skipped with a warning (don't fail the whole group).
- Auto extent on a single cell → that cell's bbox (degenerate but valid).
- Projected `srid` with an out-of-range cell (antimeridian/pole) → fall back to the
  cell boundary's lon/lat bbox; document the limitation.

## Testing

Real assertions on real data; matplotlib `Agg`; no mocking of h3/rasterio.

**CI (committed, no external download, exact oracle):**
- **Round-trip vs the inverse:** sample DEM `srtm_n40w073.tif` →
  `rst_h3_rastertogridavg(res)` → `(cellid, measure)` →
  `rst_h3_rasterize_agg(cellid, measure, <same grid>)` → assert the burned values
  match the source's centroid-sampled values within tolerance (toraster is the
  centroid inverse of rastertogrid).
- **Partition property:** `h3.polyfill` a synthetic polygon at a resolution →
  `rst_h3_rasterize_agg` → assert every burned pixel's centroid re-indexes
  (`latlng_to_cell`) to a cell *in* the set, and every NoData pixel's centroid does
  *not* — proving the clean partition.
- **Extent helper:** `rst_h3_gridspec` over a known cell set equals the union of
  `h3.cell_to_boundary` bboxes (4326), and in a projected `srid` equals the
  reprojected bbox.
- **Presence-mask default / value passthrough:** omitted `value` → covered=1.0,
  elsewhere NoData; explicit `value` → that value burned.
- **Tier parity:** heavy and light produce the same cell-set → same covered-pixel
  set (JAR-gated parity test, per repo convention).
- **Binding parity / light-agg convention:** light SQL returns BINARY, Python wrapper
  returns the tile struct; add the three bindings (Scala `override def name`,
  Python `functions.py`, `function-info.json`) + `registered_functions.txt`.

**Realistic fixture (committed, public FCC data):**
- Curate a small subset of `bdc_12_UnlicensedFixedWireless_fixed_broadband_*.csv`
  (already res-8 H3 via `h3_res8_id`; one provider, one FL county via `block_geoid`
  prefix `12086` = Miami-Dade, a few `max_advertised_download_speed` tiers) → a
  few-hundred-cell CSV committed under the test fixtures (FCC data is open/public).
  Maps to the customer flow: `provider_id`≈`TxId`, speed tier≈`Threshold`,
  `h3_res8_id`≈the polyfilled cellid. Rasterize per (provider, speed-tier), stack the
  tiers; assert covered cells match the fixture. This validates step (2) on real,
  pre-celled coverage data (no polyfill needed).

## Notebook example (full flow from polygons — DEM isobands)

A standalone example notebook demonstrating the **complete** flow (for context;
the *function* scope remains step 2):
- Start from the sample DEM `srtm_n40w073.tif`; quantize into N filled elevation
  isobands (e.g. every 100 m) via `rasterio.features.shapes` on the banded array →
  multipolygons over a range of thresholds (elevation bands stand in for signal
  thresholds).
- `h3_polyfill` each band → cells with `(band_level, cellid)`.
- `rst_h3_gridspec` for a common grid → `rst_h3_rasterize_agg` per band →
  `rst_frombands_agg` stack.
- Render with `gbx.viz.plot_raster`; the stacked bands visibly reconstruct the
  terrain — demonstrating *and* validating the rasterize→stack flow with **no
  external data**.
- Telco-authentic variant (optional, external): FCC **Mobile** coverage polygons
  (per provider/technology) stacked by technology tier. Dataset choice deferred to
  the notebook build; DEM isobands is the headline.

## Docs

- New entries on `docs/docs/api/raster-functions.mdx`: `rst_h3_rasterize_agg`
  (Aggregator section, `<Impl groupedAgg/>`, with the light-tier BINARY `:::warning`)
  and `rst_h3_gridspec`. Cross-reference `rst_h3_rastertogrid*` as the inverse and
  `rst_frombands_agg` for stacking. No internal/"wave" vocabulary (QC gate).
- Notebook example page under `docs/docs/notebooks/` when the notebook lands.
