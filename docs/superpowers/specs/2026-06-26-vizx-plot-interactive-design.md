# vizx.plot_interactive — design

**Goal:** Add `plot_interactive`, the interactive (folium) twin of `plot_static`, to `gbx.vizx`.
It must be **scale-safe** (folium hangs on millions of vertices) and **Databricks-safe** (folium
does not auto-render; it needs `displayHTML`). Promotes the validated helper from the XPlore
customer notebook into the library.

## Why
Today vizx has only static rendering (`plot_static`) and gdf adapters; docs tell users to call
raw geopandas `.explore()`, which (a) **hangs at scale** (3.4M-vertex coverage footprints →
20-min blank) and (b) **does not render in Databricks** (must go through `displayHTML`). Every
notebook hits this.

## API
```python
plot_interactive(
    data, *, column=None, mode="auto",
    grid_system=None, geom_col=None,
    max_vertices=60_000,   # auto's detailed->fast crossover + "detailed may be slow" trigger
    max_px=1400,           # fast/overlay raster resolution
    opacity=0.65,
    debug_level=1,         # 0 silent · 1 key decisions+warnings · 2+ verbose internals
    **explore_kw,
)
```

`data` is a GeoDataFrame **or** a Spark DataFrame. For a Spark DataFrame, convert to a gdf the
same way `plot_static` does: `grid_system` set → `cells_as_gdf`; else geometry column via
`geom_col`/auto-detect → `as_gdf`. (Reuse `_static_map`'s detection where practical.)

## Modes (intent-oriented)
- **`auto`** (default): use **detailed** if total vertex count `<= max_vertices`, else **fast**.
  Announce the chosen path + reason at `debug_level >= 1`.
- **`detailed`**: geopandas `.explore()` — full vector, hover tooltips/popups. If vertex count
  `> max_vertices`, emit (at `debug_level >= 1`) `"detailed mode: {n:,} vertices > {max_vertices:,}
  — may be slow to render. (set debug_level=0 to silence)"` and **proceed** (honor the choice).
- **`fast`**: raster image overlay — rasterize polygons to a PNG (`max_px` resolution) and lay it
  on a folium `ImageOverlay`. Complete (every polygon burned, nothing dropped), scales to millions
  of vertices, but a flat image (no per-feature hover). The proven `_raster_overlay` below.

Invalid `mode` → `ValueError` listing the valid modes.

## Tunable thresholds (conservative Serverless defaults; raise to tune)
- `max_vertices` (default 60_000) — auto crossover + detailed-slow trigger.
- `max_px` (default 1400) — overlay raster resolution (higher = sharper, slower/bigger PNG).

## debug_level (default 1)
- **0** — silent.
- **1** — key decisions only: auto's chosen path + why; the detailed-over-threshold warning. Every
  level-1 message ends with `" (set debug_level=0 to silence)"`.
- **2+** — verbose internals: vertex counts always, raster px used, render timing.

Example level-1 lines:
- `auto -> fast (image overlay): 3,377,195 vertices > max_vertices=60,000; per-feature hover unavailable at this scale. (set debug_level=0 to silence)`
- `detailed mode: 3,377,195 vertices > 60,000 — may be slow to render. (set debug_level=0 to silence)`

## Databricks vs Jupyter rendering
Build the folium map `m`, then render it as the function's **last statement**:
```python
try:
    html = m._repr_html_()
except Exception:
    html = m.get_root().render()
try:
    displayHTML(html)   # Databricks: render via side effect; function returns None
except NameError:
    return m            # plain Jupyter: return the map so it auto-renders
```

## Proven `_raster_overlay` (basis — adapt into the module)
rasterize polygons (numeric column → value; categorical → integer codes; none → 1..n) to a
`max_px`-wide grid in EPSG:4326, viridis-colormap to an RGBA PNG (NoData transparent), folium
`ImageOverlay` over the bounds, `fit_bounds`. (Verbatim logic validated on Serverless: 3.4M-vertex
rings → ~29s, 10 KB HTML, renders.)

## Tests (TDD, `python/geobrix/test/vizx/test_interactive.py`)
Mock `displayHTML` (inject into builtins / module globals) and assert it is called in Databricks
mode and not in Jupyter mode (NameError path returns the map). Use a small GeoDataFrame fixture.
- `mode="fast"` → `_raster_overlay` path; output is a folium Map with an ImageOverlay; complete.
- `mode="detailed"` small gdf → `.explore()` path (can mock `gdf.explore`).
- `mode="auto"` picks detailed under threshold, fast over it (drive with `max_vertices`).
- `detailed` over threshold emits the warning at `debug_level=1`, silent at `0` (capture stdout).
- `debug_level=0/1/2` output gating.
- invalid `mode` → ValueError.
- Spark DataFrame input path (can be a light unit with a tiny spark df, or mock the adapter).
- categorical vs numeric `column` in `_raster_overlay`.

## Wiring & follow-ups
- Export `plot_interactive` from `vizx/__init__.py` and add to `__all__`.
- (Follow-up, separate change) update docs/example that recommend raw `.explore()` →
  `vizx.plot_interactive`; retrofit the XPlore notebook to import it instead of the inline helper.

---

## Addendum — representative sampling (`sample_seed`) + fast-path truncation warning

**Motivation:** the only place rows are dropped is the Spark→gdf collection cap (`max_rows`),
currently `.limit(N)` = **first N**, which is partition-order arbitrary (spatially biased) and
unstable across runs. Add an opt-in **reproducible sample**.

**`sample_seed` (new param, default `None`):**
- `None` → first `max_rows` via `.limit` (current behaviour; deterministic, cheapest; **non-breaking**).
- `<int>` → reproducible sample via **`pyspark.sql.DataFrame.sample`** (NOT the pandas-on-Spark
  API): compute `frac = min(1.0, (max_rows * 1.3) / df.count())`, then
  `df.sample(withReplacement=False, fraction=frac, seed=sample_seed).limit(max_rows)`. SQL-native,
  reproducible by `seed`. Note `.sample` is Bernoulli/**approximate-N**, so the 1.3× headroom + the
  trailing `.limit(max_rows)` yield up to `max_rows` rows; same seed → same sample. Costs one extra
  `count()` job (only on the opt-in sampling path).

**Where it lives:** add `sample_seed=None` to the collection adapters **`as_gdf`** and
**`cells_as_gdf`** (they own the `max_rows` cap), and thread it through `plot_static` and
`plot_interactive`. One implementation, both plotters benefit. Default `None` keeps all existing
behaviour/tests intact.

**Fast-path truncation warning:** in `plot_interactive`, when the collected gdf was actually
truncated (collected row count `== max_rows` and the source had more), emit at `debug_level >= 1`:
`"fast: showing {max_rows:,} of {n:,} geometries — pre-aggregate (st_union_agg / rst_h3_rasterize_agg) for complete coverage. (set debug_level=0 to silence)"`.
Rationale: a *sampled* "complete-coverage" raster is a contradiction; for true completeness the
caller pre-aggregates so the gdf is few-rows-many-vertices and the cap never fires (the validated
XPlore path: 8 dissolved footprints).

**Added tests:** `sample_seed=None` → first-N (unchanged); `sample_seed=int` → reproducible
(same seed twice → identical rows; different seed → different); adapter-level sampling in
`as_gdf`/`cells_as_gdf`; the fast-path truncation warning fires only when truncated and is gated by
`debug_level`.
