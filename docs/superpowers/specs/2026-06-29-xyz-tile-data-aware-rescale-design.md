# Data-aware 8-bit rescaling for XYZ raster tiling (both tiers)

**Date:** 2026-06-29
**Branch:** beta/0.4.0
**Status:** Design — pending user review

## Problem

`rst_tilexyz` / `rst_xyzpyramid` encode PNG/JPEG/WEBP tiles by rescaling pixel
values to 8-bit using the **full data-type range**, ignoring the raster's actual
data distribution:

- **Light tier** (`pyrx/core/xyz.py::render_tile`): rio-tiler
  `ImageData.render(...)` is called with no `dataset_statistics`, so non-`uint8`
  dtypes rescale against `dtype_ranges[dtype]` (e.g. uint16 → divide by 65535).
- **Heavy tier** (`RST_TileXYZ.scala` → `OperatorOptions` PNG branch):
  `gdal_translate -of PNG -ot Byte -a_nodata none`, with **no `-scale`**, so GDAL
  rescales from the full dtype range.

Both tiers do this identically, so they currently **match** — this is a shared
product behavior, not a tier divergence. For narrow-range imagery (uint16 NAIP /
EO in e.g. `[8000, 12000]` of `[0, 65535]`, DEMs, float indices) the data is
crushed into a few dark 8-bit values **at encode time** (proven empirically:
uint16 `[8000,12000]` → 8-bit `[31,46]`, ~18% of range). The contrast is gone
before any viewer sees the tile; downstream rendering cannot recover it.

This surfaced in Helios NB02 section 7: the static raster PMTiles basemap looks
washed-out and flat compared to the source preview (`plot_file`), which applies
its own per-band percentile stretch and so looks correct.

**Out of scope / not the cause:** the static-preview zoom selection in
`vizx/_maplibre.py::_decode_pmtiles_for_static` (a separate, already-made change
that mosaics the finest zoom rather than the coarsest — reduces blur, orthogonal
to this contrast defect). It is noted here only to disambiguate; it is not part
of this design's parity surface.

## Goal

Make the **default** tile output look right for the common cases with **zero
customer configuration**, without harming already-correct imagery, without
introducing tile-to-tile contrast seams, and while keeping the two tiers at
parity. Provide an explicit override for power users and for back-compatibility.

## Solution

Add a single `rescale` parameter to `rst_tilexyz` and `rst_xyzpyramid` in **both
tiers**, default `"auto"`:

- **`"auto"` (default):**
  - **uint8 source → pass through unchanged.** Already display-ready (RGB / NAIP
    byte imagery); never touched. Protects the cases that are correct today.
  - **non-8-bit source → rescale to Byte using whole-dataset per-band min/max,**
    computed **once per source** (not per tile). Recovers contrast AND guarantees
    every tile shares one mapping → **no tile-to-tile seams.**
- **`"none"`:** today's raw full-dtype-range behavior. Explicit escape hatch for
  anyone depending on current output.
- **`(min, max)` explicit pair:** use exactly these bounds, skip the stats read.
  (Per-band uniform; a single pair applied to all bands — matches the common
  EO/single-sensor case and keeps the SQL/argument surface simple.)

### Why min/max (not percentile) for the auto path

We chose dtype-gated **whole-dataset min/max** over a 2–98 percentile stretch for
the auto default because (a) it is one unambiguous statistic both engines compute
the same way (`ComputeStatistics` / `dataset_statistics`), keeping the parity
contract trivial — a single `(min,max)` pair fed to `-scale min max 0 255` (heavy)
and `in_range=[(min,max),...]` (light); and (b) it never clips real data. Outlier
hot-pixels are the known weakness of min/max; if that proves a problem on real
EO data, the `(min,max)` override and a future `percentile` mode are the escape
valves. Percentile was considered and deferred (YAGNI for the default).

## Parity contract

- Both tiers derive the **same per-band `(min, max)`** for a given source and feed
  it identically:
  - Heavy: `gdal_translate -scale <min> <max> 0 255 -ot Byte ...`
  - Light: rio-tiler `img.render(..., in_range=[(min, max), ...])`
- Tiling parity is asserted at the **pixel / value-distribution level, not byte
  level** — heavy re-encodes a GTiff per tile and the PNG encoders differ between
  GDAL and rio-tiler, so exact-byte equality is not guaranteed (consistent with
  the established "light-readers" pixel-parity note). uint8 pass-through is the
  one path asserted byte-identical within a tier (no rescale applied).

## Components touched

### Light tier (Python)
- `pyrx/core/xyz.py`:
  - `render_tile(ds, z, x, y, fmt, size, resampling, rescale="auto")` — resolve the
    effective `(min,max)` per band (or pass-through for uint8 / `"none"`) and pass
    `in_range` into `img.render`.
  - Compute whole-dataset stats **once** and thread the resolved range through
    `pyramid(...)` so every tile in a pyramid uses the same mapping (no per-tile
    recompute, no seams).
  - `_validate(...)` extended to validate `rescale` (`"auto"` / `"none"` / a
    2-tuple of numbers).
- `pyrx/functions.py`: add `rescale` (default `"auto"`) to `rst_tilexyz` and
  `rst_xyzpyramid` public signatures + docstrings; wire it through the scalar UDF
  and the `_RstXyzPyramidUDTF` LATERAL signature.

### Heavy tier (Scala)
- `rasterx/expressions/web/RST_TileXYZ.scala`: accept a `rescale` argument; when it
  resolves to a `(min,max)` (auto-on-non-byte or explicit pair), compute band
  statistics once and inject `-scale min max 0 255` into the translate step.
- `rasterx/expressions/web/RST_XYZPyramid.scala`: thread `rescale` through to the
  per-tile `RST_TileXYZ.execute`; compute source stats once for the pyramid.
- `OperatorOptions` (PNG/JPEG/WEBP branches): allow the `-scale` flag to be
  supplied; default unchanged when no scale resolved.

### Bindings / parity (all four must carry the new arg — `gbx:test:bindings`)
- `docs/tests-function-info/registered_functions.txt`
- `function-info.json` (regenerated via `gbx:docs:function-info`)
- Python `functions.py` binding (above)
- `function-info` SQL example in
  `docs/tests/python/api/rasterx_functions_sql.py` (`*_sql_example()`)

### Helios NB02
- No notebook change required (auto default fixes the washed-out basemap).
  Confirm visually via a Serverless re-run after the light wheel is restaged.

## Testing (definition of done)

TDD, light tier first (fast local loop in `.venv-pyrx`), then heavy, then a
cross-tier Docker parity gate:

1. **Light unit tests (local):** synthetic uint16 raster, data in a known narrow
   range (e.g. `[8000,12000]`).
   - `rescale="auto"` → decoded tile spans (near) full 8-bit range, not `[31,46]`.
   - `rescale="none"` → reproduces today's crushed `[31,46]` (back-compat proof).
   - `rescale=(min,max)` → exact expected mapping.
   - uint8 source + `"auto"` → byte-identical to `"none"` (pass-through proof).
   - Pyramid: all tiles across the zoom range share one mapping (no seams) — assert
     two adjacent tiles' shared overlap/edge statistics are consistent.
2. **Heavy unit/expression tests (Docker):** mirror the light assertions on the
   Scala path against the same fixture.
3. **Cross-tier pixel-parity test (Docker, gates completion):** same fixture
   tiled by both tiers; assert equivalent per-band value distribution (within a
   tolerance) for `"auto"`, and identical uint8 pass-through behavior.

Local TDD drives implementation; the Docker heavy + parity tests are the gate
before declaring done.

## Risks & mitigations

- **Default-behavior change.** `"auto"` changes non-8-bit tile output vs today.
  Mitigation: `"none"` is the documented, exact escape hatch; uint8 (the
  most common web-imagery case) is untouched; release notes call out the change.
- **Outlier hot-pixels under min/max.** Mitigation: `(min,max)` override; future
  percentile mode if real data demands it (deferred).
- **Parity drift between engines.** Mitigation: a single resolved `(min,max)` fed
  to both; pixel-level (not byte) parity test in Docker as the gate.
- **Stats read cost.** One `ComputeStatistics` / `dataset_statistics` per source
  (not per tile); negligible vs the tiling work; skipped entirely for uint8 and
  for the explicit `(min,max)` path.
