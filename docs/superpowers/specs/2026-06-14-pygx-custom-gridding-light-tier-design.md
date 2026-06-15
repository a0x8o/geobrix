# pygx Light Custom-Gridding Tier — Design

**Date:** 2026-06-14
**Branch:** `pygx-light`
**Status:** **DRAFT — pending user review.** This is a design *proposal* drafted while the user was away; the brainstorming approval gate is **not** yet satisfied. Do **not** start an implementation plan or write any custom-gridding code until the user reviews and approves. Open questions at the bottom must be resolved first.

## Goal / context

Port the 7 heavyweight **custom-gridding** functions (`gbx_custom_*`) to the lightweight, pure-Python/PySpark `databricks.labs.gbx.pygx` tier so that **GridX reaches full 1:1 light↔heavy parity** — every GridX function (quadbin, BNG, **and custom**) available in both tiers with exact result parity. This removes the last GridX caveat: the "light vs heavy not 1:1 (custom gridding heavy-only)" note that pygx, the execution-tiers page, the gridx-functions reference, and the docs landing all currently carry.

This is the **third and final pygx phase**, after quadbin (phase 1, done) and BNG (phase 2, done). It was originally declared **out of scope** in the pygx light-tier spec (`2026-06-14-pygx-light-tier-design.md`, "Out of scope" → custom gridding stays heavyweight-only). The user (2026-06-14) reversed that decision to close the parity gap **in this branch, after BNG**. BNG phase 2 is now complete, so this is the immediate next pygx work.

**Net-new-in-0.4.0 status.** The `gbx_custom_*` family is net-new in 0.4.0 (it has no 0.3.0 predecessor), so there is no back-compat obligation; heavy custom-gridding behavior is the parity reference and stays fixed (except any validated bug fix — see open questions). No interim/planning vocabulary surfaces in user-facing docs.

Goals carried from all prior lightweight work: (a) function parity, (b) exact result parity, (c) competitive performance, (d) explicit attention wherever light is slower than heavy, (e) coherence, (f) captured benchmarking, (g) consistent docs on every surface, (h) prominent-surface visibility.

## What custom gridding *is* (heavy recon)

Unlike quadbin (a fixed global CARTO grid) and BNG (a fixed British grid in EPSG:27700), a **custom grid is a user-defined, arbitrary regular rectangular grid** — the caller defines the grid's extent, root cell size, recursive split factor, and (optionally) a CRS. There is no global singleton: every operation is parameterized by a **grid-spec struct** the user builds first with `gbx_custom_grid`.

Heavy source (read for this spec):
- `gridx/grid/CustomGridSystem.scala` — the canonical grid math (≈340 lines): cell-ID bit-packing, cell↔coordinate mapping, polyfill, kRing/kLoop, cell→geometry, centroid, distance.
- `gridx/grid/GridConf.scala` — the grid config case class + derived quantities (`bitsPerResolution`, `maxResolution`, `rootCellCountX/Y`).
- `gridx/custom/Custom_GridSpec.scala` — the 8-field STRUCT schema + `systemFromRow` decoder + Int/Long-tolerant coercion.
- `gridx/custom/Custom_*.scala` (7 expression classes) + `gridx/custom/functions.scala` (registration).

### The grid model (from `GridConf` + `CustomGridSystem`)

A grid is fully described by 8 integer parameters (the `gbx_custom_grid` STRUCT):

| Field | Type | Meaning | Validation (heavy `Custom_Grid.eval`) |
|---|---|---|---|
| `bound_x_min` | LONG | grid extent min X (native CRS units) | — |
| `bound_x_max` | LONG | grid extent max X | `> bound_x_min` |
| `bound_y_min` | LONG | grid extent min Y | — |
| `bound_y_max` | LONG | grid extent max Y | `> bound_y_min` |
| `cell_splits` | INT | subdivisions per axis per resolution level | `>= 2` |
| `root_cell_size_x` | INT | root (resolution-0) cell width in CRS units | `> 0` |
| `root_cell_size_y` | INT | root cell height in CRS units | `> 0` |
| `srid` | INT | EPSG SRID of the grid CRS; **-1 == no CRS** | (defaulted to -1 when 7 args) |

Derived (in `GridConf`):
- `resBits = 8` (top 8 bits of the cell ID hold the resolution), `idBits = 56` (low 56 bits hold the cell position).
- `subCellsCount = cell_splits²`; `bitsPerResolution = ceil(log2(subCellsCount))`; `maxResolution = min(20, floor(56 / bitsPerResolution))`.
- `rootCellCountX = ceil((bound_x_max - bound_x_min) / root_cell_size_x)`; `rootCellCountY` analogously.

**Resolution** is an integer level `0..maxResolution` (0 = root cells of size `root_cell_size_{x,y}`; each level divides cell size by `cell_splits`). This is unlike BNG's ±1..±6 index/`resolutionMap` keys and unlike quadbin's 0..26 — custom resolution is a plain level index validated against `maxResolution`.

### Cell-ID encoding (must port bit-exact)

```
cellId = (resolution.toLong << idBits) | cellPosition          # idBits = 56
cellPosition = cellPosY * totalCellsX(res) + cellPosX          # row-major
totalCellsX(res) = rootCellCountX * cell_splits^res            # (Y analogous)
getCellResolution(cellId) = (cellId >> 56).toInt
getCellPosition(cellId)   = cellId & 0x00ffffffffffffffL
cellWidth(res)  = root_cell_size_x / cell_splits^res           # (height analogous)
```

Coordinate → cell position: `cellPosX = floor((x - bound_x_min) / cellWidth(res))` (Y analogous). Cell position → cell origin: `x = cellPosX * cellWidth + bound_x_min`. Cell → polygon: the axis-aligned rectangle `[x, x+cellWidth] × [y, y+cellHeight]` as a closed 5-point ring. Centroid: the geometric center (`x + cellWidth/2`, `y + cellHeight/2`).

`pointToCellID` enforces bounds: `bound_x_min <= x < bound_x_max`, `bound_y_min <= y < bound_y_max`, `res <= maxResolution`, no-NaN — raising on violation (light must match these as `ValueError`).

## The 7 functions — signatures, return types, light impl approach

All cell IDs are **BIGINT** (`LongType`). Geometry outputs use `JTS.toWKB`/`JTS.toWKT` — **the 2D, no-SRID variant** (`JTS.scala:159`, *not* `toEWKB`). So custom geometry WKB carries **no SRID**, like BNG (and unlike quadbin's EWKB). The `srid` field in the grid spec is metadata only — it is **not** stamped into output geometries by the heavy code.

| # | Function (SQL) | Heavy signature | Return type | Light shape | Light implementation |
|---|---|---|---|---|---|
| 1 | `gbx_custom_grid` | `(xMin, xMax, yMin, yMax, cellSplits, rootCellSizeX, rootCellSizeY[, srid])` — 7 or 8 INT/LONG args | `STRUCT` (8 fields, see schema above) | **plain `@udf` / direct Column** (foldable struct builder) | Build the identical 8-field struct with the **same validation** (`xMax>xMin`, `yMax>yMin`, `cell_splits>=2`, root sizes `>0`). No geometry, no cell math. See "grid-spec construction" below. |
| 2 | `gbx_custom_pointascell` | `(point: BINARY\|STRING, grid: STRUCT, resolution: INT\|LONG)` | `BIGINT` | **`pandas_udf`** (scalar, bounded; vectorizable) | parse_geom → coordinate → `point_to_cell_id(x, y, res, conf)` (bit-pack). NULL point/grid → NULL. |
| 3 | `gbx_custom_cellaswkb` | `(cell: BIGINT, grid: STRUCT)` | `BINARY` (WKB polygon, **no SRID**) | **`pandas_udf`** (bounded scalar) | `cell_id_to_polygon(cell, conf)` → shapely → `to_wkb()` (no SRID). |
| 4 | `gbx_custom_cellaswkt` | `(cell: BIGINT, grid: STRUCT)` | `STRING` (WKT polygon) | **`pandas_udf`** (bounded scalar) | same polygon → `to_wkt()`. |
| 5 | `gbx_custom_centroid` | `(cell: BIGINT, grid: STRUCT)` | `BINARY` (WKB point, **no SRID**) | **`pandas_udf`** (bounded scalar) | cell center point → shapely Point → `to_wkb()`. |
| 6 | `gbx_custom_polyfill` | `(geom: BINARY\|STRING, grid: STRUCT, resolution: INT\|LONG)` | `ARRAY<BIGINT>` | **plain `@udf`** (variable-length array, row-by-row, scale-safe) | bbox of geom → candidate cell-center grid → keep centers with `geom.contains(Point)` → map to cell IDs. **Centroid-containment semantics** (exact port of `CustomGridSystem.polyfill`). NULL geom → NULL. |
| 7 | `gbx_custom_kring` | `(cell: BIGINT, grid: STRUCT, k: INT\|LONG)` | `ARRAY<BIGINT>` | **plain `@udf`** (variable-length array, row-by-row) | decode cell → posX/posY → Chebyshev square `[posX±k]×[posY±k]` **clamped to `[0, totalCells]`** → map back to cell IDs (exact port of `CustomGridSystem.kRing`). |

**No explode UDTFs and no aggregators in the custom family** — the heavy `gbx_custom_*` set has none (confirmed against `Custom_*.scala` + `registered_functions.txt`). So there is **no grouped-agg BINARY-vs-STRUCT deviation** to document for custom (unlike BNG/quadbin), and no `*_agg` / `*explode` light path.

## Architecture

Mirrors the established pygx shape (pure-Python/PySpark, **Serverless / Spark-Connect safe** — only `spark.udf.register` + Column expressions; never `spark.conf.set`, `_jvm`, `sparkContext`, or `.rdd`). Function names are identical to heavy (`custom_*`) and SQL names register under the same `gbx_custom_*` names, so swapping tiers is a one-line import change.

**Files:**

| File | Action | Responsibility |
|---|---|---|
| `pygx/_custom.py` | **new** | Pure-Python port of `CustomGridSystem`/`GridConf`: a `CustomGridConf` dataclass (8 fields + derived `maxResolution`, `rootCellCount{X,Y}`, `idBits=56`), and the grid-math functions `point_to_cell_id`, `cell_id_to_polygon`, `cell_id_to_centroid`, `polyfill`, `k_ring`, plus the bit-pack/unpack helpers. Spark-free, shapely for geometry. |
| `pygx/functions.py` | **extend** | Add the 7 `gbx_custom_*` UDF definitions, their `spark.udf.register(...)` calls in `register()`, and the 7 `custom_*` Column wrappers. Reuse the existing `_col` / `ColLike` helpers. |
| `pygx/_geom.py` | **reuse** | `parse_geom` for the `point`/`geom` inputs of `pointascell` and `polyfill` (WKB/EWKB/WKT/EWKT both tiers — cross-ST geom-input consistency). |
| `pygx/_serde.py` | **extend** | Add `CUSTOM_GRID_SCHEMA` (the 8-field STRUCT, matching `Custom_GridSpec.gridStructType` field names/types/nullability exactly: `bound_x_min/x_max/y_min/y_max` LONG non-null, `cell_splits`/`root_cell_size_x`/`root_cell_size_y`/`srid` INT non-null). |
| `pygx/_env.py` | **reuse / maybe extend** | No new dependency (shapely already required). Add an `assert_custom_available()` only if a guard is wanted for symmetry; custom needs nothing quadbin/BNG don't already pull in. |
| `python/geobrix/test/pygx/…` | **new tests** | Spark-free core unit tests → registered-fn tests → JAR-gated cross-tier exact parity (see Testing). |

**No new dependencies.** Custom gridding is integer/coordinate arithmetic plus shapely (already in the `[light]` extra) for the rectangle/point geometry and `contains` test. The grid math is a direct port — no library equivalent exists or is needed.

### Grid-spec construction (`gbx_custom_grid`) — the one structural difference from quadbin/BNG

quadbin and BNG have no config struct; custom does. `gbx_custom_grid` is a **pure foldable struct builder** with the same 8-field schema and the same validation as `Custom_Grid.eval`. Two implementation options (decide in the plan):

- **(A) Pure Column expression** — `custom_grid(...)` returns `f.struct(f.lit(...).alias("bound_x_min"), ...)` with the validation pushed into the consuming UDFs (they already decode the struct). Pro: no UDF, foldable, cheap, Serverless-trivial. Con: validation surfaces at consume time, not build time (heavy validates at build/eval time).
- **(B) Thin `@udf`** returning `CUSTOM_GRID_SCHEMA` that validates eagerly and raises on bad bounds/splits/sizes, matching heavy's eager `require(...)`. Pro: error parity (fails at `grid(...)` time). Con: a UDF call where a Column expression would do.

**Leaning (B)** for behavioral parity with heavy's eager validation, but the struct field names/types/nullability **must** match `CUSTOM_GRID_SCHEMA` either way so the same struct flows into both light and heavy consumers. (Open question Q1.)

The consuming light UDFs (`pointascell`, `cellaswkb`, `cellaswkt`, `centroid`, `polyfill`, `kring`) receive the grid spec as a **struct column** → arrives in the UDF as a Row / dict; reconstruct a `CustomGridConf` from its 8 fields (mirroring `Custom_GridSpec.systemFromRow`, Int/Long tolerant since PySpark may send Long for INT literals).

## Impl-shape assignment (per the established pygx rule)

The rule (documented in `functions.py`): **scalar/bounded-output → `pandas_udf`** (numpy-vectorized or batched-Arrow win); **variable-length array output → plain `@udf`** (row-by-row, OOM-safe at scale — a scalar `pandas_udf` would buffer a whole Arrow batch of arrays); **explode → `@udtf`** (none here); **grouped-agg → grouped-agg `pandas_udf` returning BINARY** (none here).

- `gbx_custom_grid` → **plain `@udf` returning the struct** (option B) or a Column-expression builder (option A). Bounded fixed-size struct; no per-row geometry.
- `gbx_custom_pointascell` → **`pandas_udf`** → `LongType`. Bounded scalar; the bit-packing is vectorizable.
- `gbx_custom_cellaswkb` / `gbx_custom_cellaswkt` / `gbx_custom_centroid` → **`pandas_udf`** → `BinaryType` / `StringType` / `BinaryType`. One geometry per row (bounded); the win is the batched Arrow transfer.
- `gbx_custom_polyfill` → **plain `@udf`** → `ArrayType(LongType())`. Variable-length (a large bbox at fine resolution can emit many cells) → row-by-row for scale safety.
- `gbx_custom_kring` → **plain `@udf`** → `ArrayType(LongType())`. Variable-length `(2k+1)²` output → row-by-row.

## Parity bar

GridX custom gridding is **deterministic integer/coordinate math**, so the bar matches quadbin/BNG (stronger than pyvx's TIN):

- **Cell IDs and cell sets: bit-exact.** `pointascell` produces the identical `Long` ID; `polyfill`/`kring` produce identical cell *sets* (no tolerance) — the bit-packing (`res << 56 | pos`), the `totalCellsX/Y` growth, the `floor`-based coordinate→position mapping, and the centroid-containment / Chebyshev-clamp semantics must be ported exactly from `CustomGridSystem`.
- **Geometry outputs (WKB/WKT): within ~1e-6.** `cellaswkb`/`cellaswkt`/`centroid` go through shapely (light) vs JTS (heavy); coordinates match to a relative/absolute tolerance of 1e-6, not byte-identical (ring orientation / coordinate formatting may differ harmlessly).
- **No SRID stamped.** Confirm light `to_wkb()` is called **without** `include_srid` (heavy uses `JTS.toWKB`, the 2D no-SRID variant) so the WKB byte layout class matches heavy — i.e. light must **not** stamp the grid's `srid` into the geometry. The `srid` field is grid metadata only.
- **Bounds/validation errors match.** Out-of-bounds coordinate, `res > maxResolution`, `cell_splits < 2`, `xMax <= xMin`, etc. raise in both tiers (light `ValueError` / `IllegalArgument`-equivalent message).

## Serverless-safety

`udf` + Column expressions only. No `spark.conf.set`, `_jvm`, `sparkContext`, `.rdd`, or repartition in the product path (those are bench-harness-only, as for the rest of pygx). The existing `test_serverless_no_spark_config.py` guard already covers `functions.py`; the custom additions live in the same module and inherit it.

## Testing

Mirror the quadbin/BNG test layering:

1. **Spark-free core** (`test/pygx/test_custom_core.py` against `_custom.py`): bit-pack/unpack round-trips (`cell_id_to_(res,posX,posY)` ↔ `(res,posX,posY)_to_cell_id`) across resolutions; `point_to_cell_id` on known fixtures; `cell_id_to_polygon`/`centroid` coordinates; `polyfill` centroid-containment on a known polygon; `k_ring` Chebyshev + boundary clamp; the `GridConf`-derived `maxResolution`/`rootCellCount` formulas; validation raises.
2. **Registered-function tests** (Docker, Spark — `test/pygx/test_custom_functions.py`): each `gbx_custom_*` via the spark fixture, including `gbx_custom_grid` struct shape, NULL propagation, and a `pointascell → cellaswkb → polyfill → kring` round-trip on a small grid.
3. **Cross-tier exact parity** (JAR-gated — `test/pygx/test_parity_custom.py`, mirroring `test_parity_bng.py` / `test_parity_quadbin.py`): register light then heavy (same SQL name, last-wins), build the **same grid spec** in both tiers, assert exact cell-ID / cell-set equality for `pointascell`/`polyfill`/`kring`, and decoded-geometry equality within 1e-6 for `cellaswkb`/`cellaswkt`/`centroid`. Include edge cells (origin cell, max-corner cell), a multi-resolution grid (`cell_splits` 2 and 4), and a grid **with** and **without** a `srid`.
4. **Binding parity** (`gbx:test:bindings`): every `gbx_custom_*` present in `registered_functions.txt` (already is), Python `functions.py` (the new light wrappers), and `function-info.json`.

## Bindings + docs surfaces to flip (heavy-only → both)

- **`registered_functions.txt`** — already lists the 7 `gbx_custom_*` names (no change; binding parity already expects them).
- **`function-info.json`** / `gbx:docs:function-info` — examples already exist for the custom functions (heavy); confirm they still validate after the light registration (no placeholder/empty usage).
- **`docs/docs/api/gridx-functions.mdx`** — flip the custom-grid section's `<Tier heavy/>` → `<Tier both/>` (lines ~15, ~1027, and each `### gbx_custom_*` per-function badge); update the top-of-page "partially lightweight" sentence to say GridX is **fully** lightweight; add per-function lib-attribution notes (pure-Python port of `CustomGridSystem` + shapely geometry; no SRID stamped).
- **`docs/docs/api/execution-tiers.mdx`** — remove custom-grid from the heavyweight-only list (lines ~45, ~47); GridX becomes fully lightweight; update the "remaining heavyweight-only surfaces" sentence (custom-grid drops off; OGR readers / `conforming` triangulation / heavy `pmtiles` writer remain).
- **`docs/docs/api/performance.mdx`** — add `pygx/_custom.py` to the modules table; note custom gridding in the pygx perf narrative (cell-math competitive; geometry-returning crosses the WKB UDF boundary).
- **`docs/docs/api/benchmarking.mdx`** — fill the Grid tab with light-vs-heavy custom numbers + exact-parity verdict (per the "bench changes → update docs" rule).
- **`README.md`** — flip the GridX bullet to full lightweight availability (quadbin + BNG + custom).
- **Docs landing** (`docs/src/pages/index.js`) — GridX card / heavyweight-only line: custom no longer heavy-only.
- **`docs/docs/intro.mdx`** — if it enumerates GridX tier coverage, include custom.
- **The pygx light-tier spec** (`2026-06-14-pygx-light-tier-design.md`) — its "Out of scope" custom-gridding note is superseded by this spec; add a forward-reference there on completion (or note here that this spec supersedes that bullet).

Voice: no internal/planning vocabulary; justify by user utility, not Mosaic parity; custom is net-new in 0.4.0 so no back-compat details surface publicly.

## Bench

Add a `--grid-custom-only` launcher leg mirroring `--grid-bng-only` / `--grid-quadbin-only` (`bench/cluster.py` ~line 198): generate a custom grid spec + a points corpus + geometries within the grid extent, run the 7 light-vs-heavy legs (timing + exact parity), and fill the `benchmarking.mdx` Grid tab. Pure cell-math (`pointascell`, `polyfill`, `kring`) should be competitive-to-faster (no JVM/JTS); the geometry-returning functions cross the WKB UDF boundary (the same ser/de tax measured for the rest of pygx) — quantify and note honestly. Terminate any cluster started for the run after capture; if reusing the standing bench cluster, suggest (don't auto) termination.

## Open questions for the user

1. **`gbx_custom_grid` build vs consume validation (option A vs B).** Heavy validates eagerly at `grid(...)` eval time (`require(xMax>xMin, ...)` etc.). A pure Column-expression struct builder (A) is cheaper/Serverless-trivial but defers validation to the consuming UDFs; a thin validating `@udf` (B) preserves error-at-build-time parity. I lean **B** for parity — confirm, or accept (A) and treat build-time vs consume-time error timing as an allowed divergence.
2. **SRID convention confirmation.** Heavy `cellaswkb`/`centroid` use `JTS.toWKB` (2D, **no SRID**) and the grid's `srid` is *not* stamped into output geometries (it's metadata only). Light will match (plain `to_wkb()`, no SRID). Confirm that's the intended contract — i.e. we do **not** want light to start stamping `srid` as EWKB (which would be a heavy-behavior change, out of scope unless you say so).
3. **Heavy bugs to validate?** quadbin/BNG had known upstream issues to validate-and-maybe-fix-in-both-tiers. Custom gridding is geobrix-original (no Mosaic lineage), so there's no external issue list — but during the port I'll watch for: (a) the `polyfill` `firstCellPos..lastCellPos + 1` off-by-one bbox padding (intentional over-scan, then centroid-filtered — port as-is); (b) `pointToCellID`'s `require(!x.isNaN && !x.isNaN, ...)` (duplicate `x` check — Y NaN is *not* guarded; likely a typo, harmless since callers pass finite coords — port the *behavior*, flag it). Want me to fix (b) in both tiers, or port the heavy behavior verbatim for strict parity?
4. **Resolution domain.** Custom resolution is `0..maxResolution` where `maxResolution = min(20, floor(56 / bitsPerResolution))` and depends on `cell_splits`. Confirm light should compute `maxResolution` identically and reject `res > maxResolution` (rather than e.g. a fixed cap).

## Out of scope

- The **`h3` GridX subpackage** (Databricks-native H3 covers hex; GeoBrix raster H3 is RasterX, already in pyrx).
- Any **heavy-tier behavior change** — heavy custom gridding is the parity reference and stays fixed, except a bug fix explicitly approved under open question Q3.
- **No new aggregators or explode UDTFs** — the heavy custom family has none; light adds none.
- **No EWKB/SRID stamping** for custom geometry outputs unless Q2 reverses the no-SRID contract.
