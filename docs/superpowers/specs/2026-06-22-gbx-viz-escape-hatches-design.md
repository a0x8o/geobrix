# gbx.viz + pyrx escape-hatches — Design

**Date:** 2026-06-22
**Branch:** `beta/0.4.0`
**Status:** design (pending user review)

## Goal

Promote the reusable EO-series notebook helpers into the package as first-class,
tested, documented APIs: a tier-agnostic visualization module `databricks.labs.gbx.viz`
(behind a new `[viz]` extra), plus two lightweight escape-hatches in `pyrx` for users
whose needs fall outside the canonical `rst_*` surface.

## Motivation

`notebooks/examples/eo-series/library.py` and `config_nb.ipynb` contain genuinely
useful, non-trivial helpers (a proper EO render pipeline; Spark→GeoDataFrame
adapters) that every notebook reimplements or `%run`-imports. They belong in the
package so users get them by `pip install` rather than copy-paste. A survey of both
files classified each helper as promote / leave-local (see "Helper disposition").

## Scope

In scope:
- `gbx.viz`: `plot_raster`, `plot_file`, `as_gdf`, `cells_as_gdf` (+ private render helpers).
- `pyrx` escape-hatches: `tile_to_numpy`, `rst_apply`.
- New `[viz]` extra (matplotlib + geopandas + folium + mapclassify), pinned + hashed
  in the lightweight CI lock; viz tests run in the light CI phase.
- Drop `generate_cells` (heavyweight-only, unused).

Out of scope (separate follow-ups):
- A `rst_h3_tessellate_df` DataFrame wrapper hiding the LATERAL-UDTF + tile-rebuild
  boilerplate — folded into the queued "UDTF generalization" audit.
- Migrating the eo-series notebooks to import from the package + re-executing them.
- `set_conf_safe` — stays notebook-local (the lightweight tier must never
  `spark.conf.set`; shipping even a "safe" wrapper would signal otherwise).

## Architecture

Approach A: visualization is **tier-agnostic** (operates on raster bytes / Spark
DataFrames, independent of pyrx vs rasterx), so it lives at the top level
`databricks.labs.gbx.viz`. The escape-hatches operate on the pyrx tile struct and
rasterio, so they live in `pyrx`.

```
databricks/labs/gbx/
  viz/
    __init__.py        # public: plot_raster, plot_file, as_gdf, cells_as_gdf
    _raster.py         # plot_raster/plot_file + _decimated_read/_percentile_stretch/
                       #   _needs_percentile_stretch/_render (private)
    _vector.py         # as_gdf, cells_as_gdf
    _env.py            # assert_viz_available() — lazy-dep guard (mirrors pyrx/_env.py)
  pyrx/
    functions.py       # + tile_to_numpy, rst_apply re-exported on the public surface
    core/
      escape.py        # tile_to_numpy, rst_apply implementations
```

All heavy `[viz]` deps are **lazy-imported inside functions**, guarded by
`viz/_env.py::assert_viz_available()`, which raises a clear
`pip install 'geobrix[viz]'` message when a dep is missing — mirroring
`pyrx/_env.py::assert_rasterio_available()`. Matplotlib is set to the `Agg` backend
when no display is available, so headless/cluster use never errors on a missing GUI.

The tile struct is the canonical `struct<cellid: bigint, raster: binary,
metadata: map<string,string>>` (`pyrx/_serde.py::TILE_SCHEMA`); the escape-hatches
use the existing `_serde.open_tile(raster_bytes)` context manager.

## Components

### `gbx.viz._raster`

```python
def plot_raster(raster_bytes, *, fig_w=10, fig_h=10, max_pixels=2000): ...
def plot_file(path, *, fig_w=10, fig_h=10, max_pixels=2000): ...
```
- Pipeline (ported verbatim from `library.py`, kept as private helpers):
  - `_decimated_read(src, max_pixels)` — bilinear downsample so `max(w,h) <= max_pixels`;
    `masked=True` so nodata is honored; returns `(data, transform, scale)`.
  - `_needs_percentile_stretch(data)` — True for integer dtypes whose max > 255.
  - `_percentile_stretch(data, lo_pct=2, hi_pct=98)` — per-band 2–98th percentile
    stretch to `[0,1]` float32, ignoring masked pixels; mask preserved.
  - `_render(data, transform, *, title, fig_w, fig_h, scale)` — apply stretch when
    needed; single-band → `viridis`, multi-band → RGB via `rasterio.plot.show`; title
    suffixed with decimation factor when downsampled.
- `plot_raster` opens bytes via `rasterio.io.MemoryFile`; `plot_file` via `rasterio.open`.
- Both return `None` (side-effect: a matplotlib figure). Headless-safe via `Agg`.

### `gbx.viz._vector`

```python
def as_gdf(df, wkt_col="wkt", *, max_rows=10_000): ...
def cells_as_gdf(df, cell_col="cellid", extra_cols=(), *, max_rows=10_000): ...
```
- `as_gdf`: Spark DataFrame with a WKT column → `geopandas.GeoDataFrame` (EPSG:4326).
  Collects with `df.limit(max_rows + 1).toPandas()` (single collect); if the result
  has `> max_rows` rows, truncate to `max_rows` and `warnings.warn` that output was
  truncated for driver-side viz. `max_rows=None` opts out of the limit. Geometry built
  with `geopandas.GeoSeries.from_wkt(..., crs=4326)`; non-geometry columns preserved.
- `cells_as_gdf`: H3 cell ids → boundary polygons via the **`h3` lib** (already a light
  dep) computed on the collected pandas frame (portable, offline-testable — replaces
  the notebook's Databricks-native `h3_boundaryaswkt`); carries `extra_cols`; delegates
  to `as_gdf`. `max_rows` applied here (before the per-row boundary computation).
  Note: gbx H3 `cellid` is a `bigint`; the h3 v4 API (`h3.cell_to_boundary`) takes a
  string index, so convert per-cell with `h3.int_to_str(cellid)` before building the
  boundary polygon (shapely `Polygon` from the lng/lat ring).

### `pyrx` escape-hatches (`pyrx/core/escape.py`, re-exported in `functions.py`)

```python
def tile_to_numpy(tile_or_bytes) -> "np.ndarray": ...
def rst_apply(tile_col, fn, returnType=DoubleType()) -> Column: ...
```
- `tile_to_numpy`: accepts a tile struct (a `Row`/dict with a `raster` field) **or**
  raw `bytes`/`bytearray`; reads all bands via `_serde.open_tile(...).read()` → ndarray.
  The "drop to numpy" hatch (from `library.py::to_numpy_arr`, generalized to accept a
  tile struct). No new deps (rasterio/numpy already light).
- `rst_apply`: returns a `Column`; builds a per-row scalar UDF that opens each tile's
  `raster` bytes via `_serde.open_tile` and calls `fn(rasterio_dataset)`, returning one
  value of `returnType` per row (default `DoubleType()`; any Spark `DataType` accepted).
  Null/empty tile → null. Generalizes `library.py::rasterio_lambda`'s hardcoded
  `DoubleType`. The documented "GeoBrix lacks function X — run your own rasterio per
  tile" path. **Scalar return only** (raster→raster transforms remain the domain of
  `rst_mapalgebra`/`rst_derivedband`).

Both escape-hatches are **Python-API-only**: `tile_to_numpy` returns a host object
and `rst_apply` takes a Python callable, so neither is SQL-registerable. They are
exposed on the `pyrx.functions` import surface but are **not** added to the SQL
registry or `registered_functions.txt` — binding-parity and `function-info.json`
are unaffected (the QC `binding-parity` gate stays green).

## Dependencies / `[viz]` extra

`pyproject.toml`:
```
viz = [
    "matplotlib>=3.7,<4",
    "geopandas>=1.0,<2",
    "folium>=0.16,<1",
    "mapclassify>=2.6,<3",   # geopandas .explore() choropleth classifier
]
```
- geopandas 1.x uses `pyogrio` (already a light dep); `shapely`/`pyproj` already light.
- Versions match `requirements-dev-container.in` where present; pinned + hash-locked in
  `requirements-pyrx-ci.{in,txt}` (regenerated with `--generate-hashes`).
- The `h3` lib is already in the light lock (used by `cells_as_gdf`).

## Error handling

- Missing `[viz]` deps → `assert_viz_available()` raises `ModuleNotFoundError`-style
  message: `gbx.viz requires the [viz] extra: pip install 'geobrix[viz]'`.
- `plot_*`: an unreadable raster surfaces the underlying rasterio error (not swallowed).
- `as_gdf`/`cells_as_gdf`: missing `wkt_col`/`cell_col` → `KeyError`-style ValueError
  naming the column; oversized DF → warn + truncate (not an error).
- `rst_apply`: null/empty tile → null row (no crash); a `fn` exception propagates as the
  UDF's task failure (not silently swallowed — the escape-hatch is the user's code).

## Testing

New `python/geobrix/test/viz/` dir (real assertions; matplotlib `Agg`; no pixel compare):
- `_percentile_stretch`: known UInt16 array → output in `[0,1]`, masked pixels excluded
  from percentile stats, mask preserved.
- `_decimated_read`: source larger than `max_pixels` → output `max(w,h) <= max_pixels`
  and `scale > 1`; small source → untouched, `scale == 1`.
- `plot_raster`/`plot_file`: a synthesized GTiff renders without error and produces a
  figure/axes (assert on the returned/active figure, not pixels).
- `as_gdf`: result CRS == EPSG:4326, geometries valid, non-geom columns preserved;
  `> max_rows` input → exactly `max_rows` rows + a truncation warning; `max_rows=None`
  → full collect.
- `cells_as_gdf`: a known H3 id → expected boundary polygon (h3 lib), `extra_cols`
  carried through.

New `python/geobrix/test/pyrx/test_escape.py`:
- `tile_to_numpy`: synthesized tile → expected shape/dtype; bytes-input and
  struct-input agree.
- `rst_apply`: small DataFrame of tiles → expected scalar per row with a **non-default**
  `returnType` (e.g. `IntegerType()`), proving the return-type generalization; null tile
  → null.

## CI / supply-chain

Follows the maintained light-tier condition (`test/conftest.py` docstring):
1. Add `"viz"` to `_LIGHT_TEST_DIRS` in `python/geobrix/test/conftest.py` (heavy phase
   skips it — no rasterio/geopandas there).
2. Add `test/viz` to the light pytest dir list in `.github/actions/pyrx_build/action.yml`.
3. Add the `[viz]` deps to `requirements-pyrx-ci.in`; regenerate `requirements-pyrx-ci.txt`
   with `UV_INDEX_URL=https://pypi-proxy.dev.databricks.com/simple uv pip compile
   --generate-hashes --python-version 3.12 -o requirements-pyrx-ci.txt requirements-pyrx-ci.in`.
4. Verify in a **clean venv built only from the lock** (`pip install --require-hashes -r
   requirements-pyrx-ci.txt && pip install --no-deps .`) running the full light selection,
   to catch any missing transitive dep before pushing (the dev container's pre-installed
   extras mask gaps).

## Docs

- New `docs/docs/api/viz.mdx` documenting `plot_raster`/`plot_file`/`as_gdf`/`cells_as_gdf`
  with the `[viz]` install note and runnable examples (doc-test-backed, per the repo's
  single-source convention).
- Escape-hatches documented in `docs/docs/api/raster-functions.mdx` (an "Escape hatches"
  section) — `tile_to_numpy` + `rst_apply` framed as the path for gaps in `rst_*` coverage.
- No internal/wave vocabulary in any `docs/docs/` page (QC `internals-leak` gate).

## Helper disposition (full survey of library.py + config_nb)

| Helper | Source | Disposition |
|---|---|---|
| `plot_raster`, `plot_file` (+ render pipeline) | library.py | → `gbx.viz._raster` |
| `to_numpy_arr` | library.py | → `pyrx` `tile_to_numpy` |
| `rasterio_lambda` | library.py | → `pyrx` `rst_apply` (generalized return type) |
| `as_gdf`, `cells_as_gdf` | config_nb | → `gbx.viz._vector` (h3-lib boundaries) |
| `generate_cells` | library.py | **dropped** (heavyweight-only, unused) |
| `set_conf_safe` | both | leave notebook-local |
| `file_size`, `timestamp_filename`, `get_now_formatted` | config_nb | leave local (trivial; validity already in STAC pkg) |
| `finalize_tiled_band_tbl`, `gen_tessellate_tiled_band` | config_nb | leave local (eo-series Delta-schema + Databricks-SQL-coupled ETL) |
| (LATERAL UDTF + tile-rebuild boilerplate inside `gen_tessellate_tiled_band`) | config_nb | follow-up: `rst_h3_tessellate_df` under the queued UDTF-generalization audit |
