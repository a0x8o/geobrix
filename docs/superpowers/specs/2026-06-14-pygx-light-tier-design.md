# pygx Light GridX Tier — Design (quadbin + BNG)

**Date:** 2026-06-14
**Branch:** `pygx-light`
**Status:** Approved design (pending user review)

## Goal

Bring the heavy-tier **GridX** functions to a lightweight, pure-Python/PySpark tier `databricks.labs.gbx.pygx`, the next sibling after `pyrx` (RasterX, complete) and `pyvx` (VectorX, complete), so GridX is a genuine **exit from heavy** for discrete-global-grid work — full function parity, exact result parity, competitive performance. Two grid systems, designed together, **implemented phased**:

- **Phase 1 — quadbin** (CARTO quadbin, 10 functions). Net-new in 0.4.0.
- **Phase 2 — BNG** (British National Grid, 23 functions).

Goals carried from prior lightweight work: (a) function parity, (b) exact result parity, (c) performance wins, (d) special attention wherever light is slower than heavy, (e) coherence (functions genuinely useful as written), (f) captured benchmarking, (g) consistent docs, (h) prominent-surface visibility (README, docs landing, intro, execution-tiers, performance).

## Architecture

Pure-Python/PySpark, **Serverless/Spark-Connect safe**: only `spark.udf.register` / `spark.udtf.register` + Column expressions — never `spark.conf.set`, `_jvm`, `sparkContext`, or `.rdd`. Mirrors the pyrx/pyvx package shape. Function names are identical to the heavy tier (`bng_*`, `quadbin_*`) so swapping tiers is a one-line import change; SQL names (`gbx_quadbin_*`, `gbx_bng_*`) register pyspark-backed under the same names as heavy.

**Files:**

| File | Responsibility | Phase |
|---|---|---|
| `python/geobrix/src/databricks/labs/gbx/pygx/__init__.py` | package marker | 1 |
| `pygx/functions.py` | `register(spark)` + Column wrappers + UDF/UDTF classes (public surface) | 1 (extended in 2) |
| `pygx/_quadbin.py` | quadbin cell math (wraps the `quadbin` lib) + shapely geometry build | 1 |
| `pygx/_geom.py` | shared `parse_geom` (WKB/EWKB/WKT/EWKT) for geometry-input functions — mirrors the pyvx contract (cross-ST geom-input consistency) | 1 |
| `pygx/_serde.py` | output struct schemas (tessellate cell/geom structs) | 1 |
| `pygx/_bng.py` | pure-Python port of `BNG.scala` (codec + geometry + neighborhood + coverage). Split into `_bng_codec.py` + `_bng_geom.py` if it grows unwieldy | 2 |
| `pygx/_env.py` | dependency guard (`quadbin` + `shapely`) | 1 |
| `python/geobrix/test/pygx/…` | Spark-free core unit tests + registered-fn tests + JAR-gated cross-tier parity | 1, 2 |

**No new dependencies.** `quadbin>=0.2,<0.3` and `shapely` are already in the `[light]` extra; BNG is pure Python (`numpy`/`shapely` already present). The `quadbin` lib is already used by `pyrx/core/gridagg.py` (a vectorized `point_to_cell` reference).

## Parity bar (both phases)

GridX is **deterministic** integer/coordinate math, so the bar is stronger than pyvx's TIN:

- **Cell IDs and cell sets: bit-exact.** `pointascell`/`eastnorthasbng` produce identical IDs; `polyfill`/`kring`/`kloop`/`tessellate`/`geomkring`/`geomkloop` produce identical cell *sets* (no tolerance).
- **Geometry outputs (WKB): within ~1e-6.** `aswkb`/`aswkt`/`centroid`/`cellunion`/`cellintersection`/tessellate-chips go through shapely (light) vs JTS (heavy); coordinates match to a relative/absolute tolerance of 1e-6, not byte-identical.
- **BNG `tessellate`/`polyfill` held to exact cell-set parity.** Heavy uses a `buffer`-erode core/border split (0.1 m tolerance) + JTS `contains`/`intersects`; a shapely-vs-JTS boundary disagreement on a knife-edge cell is treated as a **bug to fix**, not a tolerated divergence.

---

## Phase 1 — quadbin (10 functions)

Heavy source: `src/main/scala/com/databricks/labs/gbx/gridx/quadbin/` + `gridx/grid/Quadbin.scala`. The `quadbin` lib does all cell math.

| Function (SQL) | Shape | Output | Light implementation |
|---|---|---|---|
| `gbx_quadbin_pointascell` | scalar | BIGINT | `quadbin.point_to_cell(lon, lat, res)` (res ∈ [0,26]) |
| `gbx_quadbin_resolution` | scalar | INT | `quadbin.get_resolution(cell)` |
| `gbx_quadbin_kring` | scalar | ARRAY\<BIGINT\> | `quadbin.k_ring(cell, k)` |
| `gbx_quadbin_distance` | scalar | INT | `quadbin.cell_distance(a, b)` (same-resolution; error otherwise, mirror heavy) |
| `gbx_quadbin_polyfill` | scalar | ARRAY\<BIGINT\> | `quadbin.polyfill_bbox(geom.bounds, res)` — bbox/envelope semantics, matching heavy (res ∈ [0,20]) |
| `gbx_quadbin_aswkb` | scalar | BINARY (EWKB polygon, SRID 4326) | cell → bbox (lib) → shapely polygon → `to_wkb(..., include_srid=True)` |
| `gbx_quadbin_centroid` | scalar | BINARY (EWKB point) | cell bbox center → shapely Point → EWKB |
| `gbx_quadbin_cellunion` | scalar | BINARY (EWKB MultiPolygon) | each cell → shapely polygon → `unary_union` → EWKB |
| `gbx_quadbin_tessellate` | scalar | ARRAY\<STRUCT\<cell:BIGINT, geom:BINARY\>\> | polyfill bbox → per-cell shapely intersection with the input geom |
| `gbx_quadbin_cellunion_agg` | aggregator | BINARY (EWKB MultiPolygon) | **grouped-agg `pandas_udf` returning BINARY directly** (atomic output — no struct-compose workaround); same union logic as `cellunion` |

Geometry-input functions (`polyfill`, `tessellate`) accept WKB/EWKB/WKT/EWKT via `_geom.parse_geom`.

---

## Phase 2 — BNG (23 functions)

Heavy source: `src/main/scala/com/databricks/labs/gbx/gridx/bng/` + the **canonical algorithm** `gridx/grid/BNG.scala` (≈816 lines). **No PyPI BNG library exists** — `_bng.py` is a faithful pure-Python port of `BNG.scala`, validated bit-exact against it. Cell IDs are STRING (mirror heavy). All register under the `gbx_bng_*` prefix (incl. `gbx_bng_pointascell`). Resolution = integer index ±1..±6 (1=100km … 6=1m; negatives = quadrants) or a `resolutionMap` string key (`"1km"`, `"100m"`, …); **never** metres-as-Int. Coordinates are EPSG:27700 eastings/northings.

Implemented in dependency order (each layer testable before the next):

1. **Codec** — `encode`/`decode`/`format`/`parse`, `resolutionMap`/`sizeMap`, the `letterMap` 2-letter-prefix grid, and the ±quadrant logic. Functions: `bng_pointascell`, `bng_eastnorthasbng`, `bng_cellarea` (`(edgeSize/1000)²` km²), `bng_distance` (Manhattan), `bng_euclideandistance` (Chebyshev/max).
2. **Cell → geometry** — `cellIdToGeometry` (decode → x/y/edgeSize → shapely polygon), `cellIdToCenter`. Functions: `bng_aswkb` (WKB polygon, **no SRID** — heavy uses `toWKB` not `toEWKB`), `bng_aswkt`, `bng_centroid`, `bng_cellintersection`, `bng_cellunion`.
3. **Neighborhood (cell-centric)** — `kRing`/`kLoop` square-grid walks. Functions: `bng_kring`, `bng_kloop`, and the explode UDTFs `bng_kringexplode`, `bng_kloopexplode` (SQL-`LATERAL`-only in light).
4. **Coverage** — `bng_polyfill` (BFS flood-fill from boundary + centroid via `kLoop`, shapely `contains` per candidate centroid).
5. **Tessellation** — `bng_tessellate` / `bng_tessellateexplode` (buffer-erode core/border split, separate polyfill per region, per-border-cell shapely intersection, 0.1 m core tolerance) → `ARRAY<STRUCT<cellid:STRING, core:BOOL, chip:BINARY>>`. The hardest functions.
6. **Geometry-centric neighborhood** — `bng_geomkring`, `bng_geomkloop` and their explode UDTFs (depend on the tessellate/`lineFill`/`lineDecompose` machinery — BFS walk along LineStrings via `kRing`).
7. **Aggregators** — `bng_cellintersection_agg`, `bng_cellunion_agg` (grouped-agg `pandas_udf` returning BINARY directly).

### BNG known-issue validation (Mosaic lineage)

Two BNG bugs were flagged in upstream Mosaic and may have been inherited by the heavy port. Because pygx holds **exact cell-set / geometry parity** with heavy, a bug present in heavy would otherwise be *propagated* into light. Stance (same as the pyvx legacy holes/Z fixes): during Phase 2, **validate each against current `gridx/grid/BNG.scala`**; if still present, **fix in BOTH tiers** so parity holds on *correct* behavior, and **reference the Mosaic issue** in the commit message + the beta release notes. If already fixed or not applicable in the geobrix port, record that finding (no change).

- **[mosaic#434](https://github.com/databrickslabs/mosaic/issues/434)** — affects `bng_aswkb` / `bng_aswkt` / `bng_centroid` / `bng_cellarea`: a **100km** grid reference whose two-letter code is `"NE"` / `"NW"` / `"SE"` / `"SW"` is misread as a *quadrant* (negative) resolution and given a half-dimension geometry. The codec (`format`/`parse`/`cellIdToGeometry`) must disambiguate a genuine 100km two-letter reference from a quadrant suffix. Parity test must include these four cells (expect full 100km × 100km = 10¹⁰ m² area).
- **[mosaic#423](https://github.com/databrickslabs/mosaic/issues/423)** — affects `bng_tessellate` / `bng_tessellateexplode`: when the input polygon is **aligned to the BNG grid** at the resolution, the per-border-cell intersection yields spurious **POINT/LINESTRING** chips at shared vertices/edges alongside the correct polygon chips. Tessellation must **filter intersection results to areal geometries** (Polygon/MultiPolygon) only. Parity test must include a grid-aligned polygon and assert no degenerate (non-areal) chips in either tier.

## Data flow

```
quadbin: (lon,lat,res) ─pointascell→ BIGINT ─resolution/aswkb/centroid→ INT / EWKB
         geom (WKB/EWKB/WKT/EWKT) ─polyfill→ ARRAY<BIGINT> ; ─tessellate→ ARRAY<STRUCT<cell,geom>>
         ARRAY<BIGINT> / grouped rows ─cellunion(_agg)→ EWKB MultiPolygon

BNG: (point|east,north)+res ─encode→ STRING cellid ─cellIdToGeometry→ WKB / WKT / centroid
     cellid ─kring/kloop(+explode)→ ARRAY<STRING> / rows
     geom+res ─polyfill→ ARRAY<STRING> ; ─tessellate(+explode)→ STRUCT<cellid,core,chip>
     geom+res+k ─geomkring/geomkloop(+explode)→ ARRAY<STRING> / rows
     cells ─cellintersection/cellunion(_agg)→ WKB
```

## Error handling

- Resolution validation mirrors heavy: quadbin [0,26] (pointascell) / [0,20] (polyfill); BNG ±1..±6 or `resolutionMap` keys — reject metres-as-Int with a clear error.
- `quadbin_distance` / `bng` distances on different resolutions → error (mirror heavy).
- Unknown/malformed cell IDs → clear `ValueError`.
- Empty/null geometry inputs → empty array / null, matching heavy.
- Explode UDTFs (light) have no Python Column form → `NotImplementedError` pointing to SQL `LATERAL` (like pyvx pyramid).

## Testing

- **Spark-free core** (`_quadbin.py`, `_bng.py`): the BNG **codec round-trips** (encode↔decode↔format↔parse) and is checked **bit-exact against `BNG.scala`** across every resolution including ±quadrants; quadbin cell math via the lib. Geometry construction, kring/kloop walks, polyfill BFS, tessellate core/border split — unit-tested with known fixtures.
- **Registered-function tests** (Docker, Spark): each `gbx_*` UDF/UDTF/agg via the spark fixture.
- **Cross-tier parity** (JAR-gated, `test/pygx/test_parity_*`): exact cell-ID/set equality light-vs-heavy per function; geometry WKB decoded-equality within 1e-6. Register light, then heavy (same SQL name, last-wins), as in the pyvx parity tests.
- **Binding parity**: every `gbx_quadbin_*` / `gbx_bng_*` present in `registered_functions.txt`, Python `functions.py`, and `function-info.json`.

## Performance (goals c, d)

Pure cell-math functions (`pointascell`, `resolution`, `distance`, `kring`/`kloop`) should be **competitive-to-faster** than heavy (no JVM/JTS overhead). The geometry-returning functions (`aswkb`, `centroid`, `cellunion`, `tessellate`) cross the WKB UDF boundary — the same JVM↔Python ser/de tax measured for pyvx; the bench quantifies it, and any function where light is materially slower than heavy gets explicit attention (vectorization where possible) and an honest note in the docs (goal d). Cell IDs are scalar LONG/STRING — cheap across the boundary.

## Benchmarking (goal f)

Extend the `gbx:*` bench harness mirroring the vector-tin bench: `bench/corpus_*` generators (points + geometries + cell-id arrays), `bench/readers.py` `run_*` legs, a `bench/cluster.py` cell, and launcher flags `--grid-quadbin-only` / `--grid-bng-only`. Cluster light-vs-heavy timing + **exact-parity** verdicts. Fill the **`benchmarking.mdx` "Grid (soon)" tab** per phase. Terminate any cluster started for the run after capture.

## Docs (goals g, h) — every surface, per phase

- `docs/docs/api/gridx-functions.mdx` — flip each function's Tier badge heavy→both as it lands; per-function lib-attribution notes (quadbin lib / pure-Python BNG / shapely); document the BNG resolution-index + `resolutionMap` convention and the EPSG:27700 expectation.
- `docs/docs/api/execution-tiers.mdx` — move quadbin (phase 1) then BNG (phase 2) out of the "heavyweight-only" list and the `gbx_bng_*`/quadbin mentions; update the GridX framing.
- `docs/docs/api/performance.mdx` — add pygx to the execution-shape tabs, the modules table (`pygx/_quadbin.py`, `pygx/_bng.py`), the libraries table (`quadbin`), and the perf narrative.
- `docs/docs/api/benchmarking.mdx` — fill the Grid tab with light-vs-heavy numbers + exact-parity verdicts.
- `README.md` — flip the GridX bullet from "Heavyweight Scala tier (lightweight `pygx` planned)" to its availability (by phase).
- Docs landing (`docs/src/pages/index.js`) — GridX card + the heavyweight-only line.
- `docs/docs/intro.mdx` — note pygx alongside pyrx/pyvx.
- `function-info` examples for the registered functions.

Voice: no internal/planning vocabulary; justify by user utility, not Mosaic parity. Quadbin is net-new in 0.4.0 — no interim/back-compat details surface publicly.

## Phasing

**One spec** (this document, both grid systems). **Two implementation plans**: the **quadbin plan executes first** and ships fully (10 functions + bench + all docs surfaces) before the **BNG plan** starts. Each phase flips its functions' Tier badges and updates every doc surface above on completion.

## Out of scope

- **Custom gridding (`gbx_custom_*`) — stays heavyweight-only, explicitly NOT ported to pygx.** The 7 custom user-defined grid functions — `gbx_custom_grid`, `gbx_custom_pointascell`, `gbx_custom_cellaswkb`, `gbx_custom_cellaswkt`, `gbx_custom_centroid`, `gbx_custom_polyfill`, `gbx_custom_kring` — remain a heavyweight-only capability. See the [Custom Grid Functions](https://databrickslabs.github.io/geobrix/docs/api/gridx-functions#custom-grid-functions) section (`docs/docs/api/gridx-functions.mdx#custom-grid-functions`). pygx covers **quadbin + BNG only**; custom grids stay heavy, and the docs/execution-tiers updates must keep listing `gbx_custom_*` as heavyweight-only.
- The `h3` GridX subpackage (Databricks-native H3 already covers hex; GeoBrix raster H3 is RasterX, already in pyrx).
- Any heavy-tier behavior change (GridX heavy is the parity reference; light conforms to it).
