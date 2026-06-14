# pyvx VectorX TIN + Legacy Light Tier — Design

**Date:** 2026-06-13
**Branch:** `pyvx-light`
**Status:** Approved design (pending user review)

## Goal

Bring the remaining heavy-tier VectorX `gbx_st_*` functions to light-tier parity in `databricks.labs.gbx.pyvx`, so the light tier is a genuine **exit from heavy** for surveying/DTM and Mosaic-migration workloads — not a partial port. This covers the **TIN block** (`st_triangulate`, `st_interpolateelevationbbox`, `st_interpolateelevationgeom`) and **legacy-geometry migration** (`st_legacyaswkb`).

Because 0.4.0 VectorX is unreleased, greenfield WIP (no back-compat owed), the design also **modifies the heavy tier** where alignment makes the cross-tier swap seamless: a new `mode` parameter on the TIN functions, and two bug-fixes in `st_legacyaswkb` (preserve Z, preserve polygon holes).

## Architecture

Pure-Python/PySpark light tier, Serverless/Spark-Connect safe (only `udf`/`udtf` registration + Column expressions — never `spark.conf.set`, `_jvm`, `sparkContext`, or `.rdd`). Heavy compute primitives stay JVM/JTS; the light tier reimplements them on `scipy` + `shapely` + `numpy`.

The triangulation engine is the crux: heavy uses JTS `ConformingDelaunayTriangulator` (a **conforming** Delaunay that inserts Steiner points to keep triangles Delaunay while honoring breakline constraints). No permissively-licensed Python library provides constrained/conforming Delaunay over sites + segments:

- `triangle` (Shewchuk) — non-commercial license, disqualified for Databricks Labs.
- `shapely.constrained_delaunay_triangles` / `mapbox_earcut` — polygon-**interior** tessellators; empirically return *empty* for scattered point sets and ignore interior breakline segments (verified: shapely 2.1.2 / GEOS 3.13.1). Wrong tool for mass-point TIN.
- VTK — permissive and capable, but a ~100MB+ dependency that defeats a "light" tier.

So the light tier uses **scipy `Delaunay` + a hand-rolled Sloan constraint-recovery step** (forced constraint edges via edge-flipping — a **constrained**, no-Steiner Delaunay). To keep the two tiers aligned by default, the **heavy tier gains the same constrained mode** and the conforming (Steiner) behavior becomes an explicit opt-in.

## Tech Stack

- Light: Python 3.12, `scipy.spatial.Delaunay`, `shapely` (2.x, WKB/EWKB/WKT I/O), `numpy`, PySpark `@udf`/`@udtf`.
- Heavy: Scala 2.13 / Spark 4.0 / JTS (existing); constrained path on JTS `QuadEdgeSubdivision` / `IncrementalDelaunayTriangulator`.
- New `[light]` dependency: **`scipy`** (BSD-3, permissive). `shapely`/`numpy` already present.

---

## Component 1 — `mode` parameter (cross-tier TIN alignment)

A new trailing `mode: String` parameter on `st_triangulate`, `st_interpolateelevationbbox`, and `st_interpolateelevationgeom`, in **both tiers**, following the established H3-modes playbook (backward-compatible arity — existing call arities keep working; `mode` defaults to `"constrained"`).

| `mode` | Semantics | Light | Heavy |
|---|---|---|---|
| `"constrained"` **(default)** | Forced constraint edges, **no Steiner points** | ✅ scipy + Sloan | ✅ JTS QuadEdge + constraint recovery |
| `"conforming"` | JTS conforming Delaunay (**adds Steiner points**); `splitPointFinder` is meaningful | ❌ raises `NotImplementedError` (points to heavy) | ✅ `ConformingDelaunayTriangulator` (today's behavior) |

Rationale: `"constrained"` is producible in **both** tiers, so the default swap is seamless. `"conforming"` is the richer JVM-only capability, now an explicit opt-in rather than a silent tier difference.

`splitPointFinder` (`MIDPOINT`/`NONENCROACHING`) only affects Steiner placement, so it is meaningful **only** under `"conforming"`. Under `"constrained"` it is accepted for signature compatibility and documented as a no-op.

Validation: unknown `mode` raises `IllegalArgumentException` (heavy) / `ValueError` (light) listing the valid values, mirroring `rst_h3_tessellate`.

---

## Component 2 — Light TIN backend (`pyvx/_tin.py`)

Pure-Python, Spark-free, the heavily-tested core. Geometry I/O via shapely.

**Triangulation (`mode="constrained"`):**
1. Parse mass points (`ARRAY<BINARY|STRING>` → shapely geometries; collect XYZ vertices). Apply `mergeTolerance` vertex merge/dedup (snap near-coincident vertices).
2. `scipy.spatial.Delaunay(points_xy)` → base unconstrained triangulation.
3. **Sloan constraint recovery** for each breakline segment not already an edge:
   - Robust `orient2d` predicate (determinant form).
   - Walk the triangulation from segment start to end via `Delaunay.find_simplex` / `.neighbors` to find intersected edges.
   - Iteratively flip intersected edges (only across convex quads) until the segment is an edge sequence.
   - Termination guard (bounded flip count; raise on non-termination rather than loop).
   - Handle degenerate/cocircular/near-collinear cases explicitly.
4. **Z-snap (`snapTolerance`)**: vertices within `snapTolerance` of a constraint line get Z overwritten by linear interpolation along that line (matches heavy `LengthIndexedLine` post-process).

**Interpolation:** barycentric Z within the (constrained) TIN triangle containing each query point. Query points outside the convex hull → **no output row** (matches heavy's silent drop). NaN Z (degenerate triangle) → dropped.

**Grid generation (replicate heavy exactly):**
- **bbox**: `xRes=(xmax-xmin)/widthPx`, `yRes=(ymax-ymin)/heightPx`; center `x=xmin+(i+0.5)*xRes`, `y=ymin+(j+0.5)*yRes`; **column-major** order (`i` over `[0,widthPx)` slowest, `j` over `[0,heightPx)` fastest); points carry the `srid` param.
- **geom**: `x=originX+(i+0.5)*cellSizeX`, `y=originY+(j+0.5)*cellSizeY`; `cellSizeY` may be negative (y-down raster convention); column-major; SRID taken from the `gridOrigin` geometry (EWKB/EWKT → non-zero, plain → 0).

**Output encoding:** triangles → 2D OGC WKB (`shapely.to_wkb`, `output_dimension=2`). Elevation points → 3D `POINT Z` ISO WKB (`shapely.to_wkb`, `output_dimension=3`), SRID set on the geometry but **not** embedded (matches heavy `toWKB3` = ISO WKB-Z, not EWKB).

`mode="conforming"` → `NotImplementedError` with guidance to use the heavy tier.

## Component 3 — Light TIN functions (`pyvx/functions.py`, registered via `register(spark)`)

All generators are `@udtf` (matching heavy's `CollectionGenerator` contract and the existing pyvx UDTF pattern; invoked via SQL `LATERAL`). The per-row input array **is** the bounded local point set, so no internal spatial grouping is needed.

- `st_triangulate(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, mode="constrained")` → UDTF, output `STRUCT<triangle BINARY>` (one row per triangle).
- `st_interpolateelevationbbox(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, xmin, ymin, xmax, ymax, width_px, height_px, srid, mode="constrained")` → UDTF, output `STRUCT<elevation_point BINARY>`.
- `st_interpolateelevationgeom(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y, mode="constrained")` → UDTF, output `STRUCT<elevation_point BINARY>`.

Null/empty `points` → empty iterator. Non-empty `breaklines` with `mode="conforming"` → `NotImplementedError`.

## Component 4 — Legacy decode (`pyvx/_legacy.py` + `st_legacyaswkb`)

Input legacy Mosaic struct:
```
STRUCT<
  typeId     INT,                                 -- 1 POINT, 2 MULTIPOINT, 3 LINESTRING,
                                                  --   4 MULTILINESTRING, 5 POLYGON, 6 MULTIPOLYGON,
                                                  --   7 LINEARRING; 8 GEOMETRYCOLLECTION → error
  srid       INT,
  boundaries ARRAY<ARRAY<ARRAY<DOUBLE>>>,         -- rings; each coord len 2 (XY) or 3 (XYZ)
  holes      ARRAY<ARRAY<ARRAY<ARRAY<DOUBLE>>>>   -- interior rings per polygon
>
```

`_legacy.py` decodes the struct into a shapely geometry, then emits **ISO WKB preserving Z** (`shapely.to_wkb`, default `output_dimension=3` / `flavor="iso"` — Z written when present, 2D otherwise) and **preserving interior rings (holes)**. This fixes the two heavy bugs: `toWKB` (2D) dropped Z, and a `// TODO` silently dropped holes.

Single function, `st_legacyaswkb`, a **scalar UDF**, both tiers:

| Function | Output | Notes |
|---|---|---|
| `st_legacyaswkb` | ISO WKB, **Z preserved**, **holes preserved** | SRID **not** embedded — it lives in the source struct's `srid` field and the migrator applies it at ingestion (`ST_GeomFromWKB(wkb, srid)` / set CRS) |

No EWKB variant: Z (the only extra dimension the legacy format carries) is preserved by plain ISO WKB; embedding SRID-in-bytes would be the sole reason for EWKB and is deferred as YAGNI (the SRID is available separately). M does not exist in the source format and is unsupported by GEOS — explicitly out of scope.

`GEOMETRYCOLLECTION` (typeId 8) raises (matches heavy). Null input → null output.

## Component 5 — Heavy-tier changes (Scala)

- **`ST_Triangulate.scala`, `ST_InterpolateElevationBBox.scala`, `ST_InterpolateElevationGeom.scala`**: add the `mode` param (FunctionBuilder arity arms for backward-compatible defaulting to `"constrained"`); implement the `"constrained"` path via JTS `QuadEdgeSubdivision` / `IncrementalDelaunayTriangulator` + constraint recovery (no Steiner); keep `"conforming"` on `ConformingDelaunayTriangulator`. **Default behavior changes** to constrained (acceptable — unreleased WIP).
- **`InternalGeometry.scala`** (`jts/legacy`): fix the dropped-holes TODO for POLYGON/MULTIPOLYGON in `toJTS`.
- **`ST_LegacyAsWKB.scala`** (`jts/legacy`): switch the encoder from `JTS.toWKB` (2D) to `JTS.toWKB3` (Z-preserving ISO WKB), so heavy matches light. SRID still not embedded.

## Data flow

```
mass points (ARRAY<geom>) ─┐
breaklines (ARRAY<line>)  ─┼─→ _tin.triangulate(mode) ─→ constrained TIN
tolerances, mode          ─┘                               │
                                                           ├─ st_triangulate: emit each triangle (2D WKB)
grid spec (bbox | origin) ────────────────────────────────┴─→ interpolate Z at cell centers
                                                              → emit in-hull POINT Z (3D WKB), drop outside-hull

legacy struct ─→ _legacy.decode (Z + holes preserved) ─→ shapely geom ─→ st_legacyaswkb (ISO WKB, Z preserved)
```

## Error handling

- Unknown `mode` → `ValueError`/`IllegalArgumentException` listing valid values.
- `mode="conforming"` in light → `NotImplementedError` pointing to heavy.
- Non-LineString breaklines for the interpolate functions → error (matches heavy's runtime type check).
- Sloan non-termination → raise (never silently return an unconstrained result).
- `GEOMETRYCOLLECTION` legacy input → raise.
- Empty/null points → empty iterator (generators) / null (scalar).

## Testing strategy

**Light unit (`_tin.py`, `_legacy.py` — Spark-free):**
- Delaunay correctness on scattered points; degenerate (collinear, cocircular, single point, duplicates), empty.
- Sloan recovery: every constraint segment is present as a triangle-edge sequence; flip termination; Z-snap correctness.
- Grid generation: exact center coordinates + column-major ordering for both bbox and geom specs; negative `cellSizeY`.
- Interpolation: known barycentric values; outside-hull drop; NaN drop.
- Legacy decode: each typeId; holes preserved; Z preserved (ISO WKB dim-3 round-trips XYZ); GEOMETRYCOLLECTION raises.

**Cross-tier parity (JAR-gated, `test/pyvx/`):**
- **Legacy**: light↔heavy decoded-geometry equality for `st_legacyaswkb` incl. a **holed polygon** and a **Z-valued (XYZ)** geometry (both post-fix).
- **No-breakline TIN**: triangle-set / interpolated-surface equality within float tolerance (Delaunay ~unique).
- **With-breakline TIN** (`mode="constrained"` both tiers): assert (a) each constraint segment present as triangle edges, (b) interpolated Z at sample points within tolerance — **not** triangle-identity (Qhull vs JTS cocircular tie-breaks differ).
- **`mode="conforming"`**: heavy behavior test; light raises.

**Heavy Scala:** constrained-vs-conforming mode tests; legacy holes-preserved + Z-preserved (`toWKB3`) tests.

**Binding parity:** `st_legacyaswkb`, `st_triangulate`, `st_interpolateelevationbbox`, `st_interpolateelevationgeom` present in `registered_functions.txt`, Python `functions.py` (both tiers), and `function-info.json`.

## Phasing

1. **Legacy first** — `st_legacyaswkb` (Z-preserve + holes fixes), both tiers. Independent, lower-risk, establishes the non-MVT scalar-UDF pattern.
2. **TIN block second** — `_tin.py` backend (TDD the Sloan core hardest) → 3 light UDTFs → heavy `mode` (constrained path) → cross-tier parity tests.

## Out of scope

- M dimension (absent from the legacy format; unsupported by GEOS).
- `"conforming"` mode in the light tier (heavy-only opt-in by design).
- Byte-identical triangle parity with breaklines (different-but-valid triangulations; surface-closeness is the contract).
- Vector file readers/writers (a separate phase).

## Documentation

- pyvx VectorX functions page: add the 4 functions with light/heavy tabs, the `mode` param, and the constrained-vs-conforming + breakline divergence explainer (defensible-divergence framing, like the H3-Java 3.7.0 note).
- Legacy section: migration framing for Mosaic customers; `st_legacyaswkb` preserves Z + holes; SRID applied separately at ingestion; M out of scope.
- `function-info.json` examples for all 5.
- No internal vocabulary (no "wave N"); justify by user utility, not Mosaic parity.
