# gbx.viz + pyrx escape-hatches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote the EO-series notebook helpers into the package as a tier-agnostic `databricks.labs.gbx.viz` module (new `[viz]` extra) plus two Python-only pyrx escape-hatches (`tile_to_numpy`, `rst_apply`).

**Architecture:** `gbx.viz` is tier-agnostic (operates on raster bytes / Spark DataFrames) and lives at the package top level; its heavy deps (matplotlib, geopandas) are lazy-imported behind a `viz/_env.py` guard mirroring `pyrx/_env.py`. The escape-hatches operate on the pyrx tile struct via `pyrx/_serde.open_tile` and live in `pyrx/core/escape.py`, re-exported on `pyrx.functions`. They are Python-API-only (never SQL-registered).

**Tech Stack:** Python 3.12, PySpark 4.0, rasterio (light), matplotlib + geopandas + folium + mapclassify (new `[viz]`), h3 (light), pytest.

## Global Constraints

- Target branch `beta/0.4.0`. Spec: `docs/superpowers/specs/2026-06-22-gbx-viz-escape-hatches-design.md`.
- Serverless-safe: NO `spark.conf.set`, `.cache()/.persist()/.localCheckpoint()`, `_jvm`/`sparkContext`/`.rdd` anywhere in package code.
- `[viz]` deps are pinned+hash-locked in `requirements-pyrx-ci.{in,txt}` (regen with `--generate-hashes`); versions match `requirements-dev-container.in` where present.
- Package code imports only matplotlib + geopandas (+ already-light shapely/h3) — NOT folium/mapclassify (those are user-side `.explore()` deps shipped in the extra). The dep guard checks only what our code imports.
- Escape-hatches are Python-API-only: NOT added to the SQL registry or `registered_functions.txt`; binding-parity / `function-info.json` stay unchanged.
- Heavy deps lazy-imported inside functions, guarded by `assert_viz_available()` raising `pip install 'geobrix[viz]'`. Matplotlib forced to `Agg` when no display.
- Tests use real assertions on real synthesized rasters (reuse `make_geotiff_bytes` from `test/pyrx/conftest.py`); matplotlib `Agg`; no pixel comparison. No mocking of geopandas/rasterio.
- `gbx:lint:python --check` (isort/black/flake8) must pass; run black/isort IN the dev container (host black may differ).
- No internal/wave vocabulary in any `docs/docs/` page (QC `internals-leak` gate).
- All test runs happen in the `geobrix-dev` Docker container (`bash scripts/commands/gbx-docker-start.sh`; run via `scripts/commands/gbx-test-python.sh --path <p>` or `docker exec geobrix-dev bash -lc '...pytest...'`).

---

## File Structure

- Create `python/geobrix/src/databricks/labs/gbx/viz/__init__.py` — public exports.
- Create `.../viz/_env.py` — `assert_viz_available()` lazy-dep guard.
- Create `.../viz/_raster.py` — `plot_raster`, `plot_file` + private render pipeline.
- Create `.../viz/_vector.py` — `as_gdf`, `cells_as_gdf`.
- Create `.../pyrx/core/escape.py` — `tile_to_numpy`, `rst_apply`.
- Modify `.../pyrx/functions.py` — re-export `tile_to_numpy`, `rst_apply`.
- Modify `python/geobrix/pyproject.toml` — add `[viz]` extra.
- Modify `python/geobrix/requirements-pyrx-ci.in` + regenerate `.txt`.
- Modify `python/geobrix/test/conftest.py` — add `"viz"` to `_LIGHT_TEST_DIRS`.
- Modify `.github/actions/pyrx_build/action.yml` — add `test/viz` to the light dir list.
- Create `python/geobrix/test/viz/__init__.py`, `.../test/viz/test_raster.py`, `.../test/viz/test_vector.py`.
- Create `python/geobrix/test/pyrx/test_escape.py`.
- Create `docs/docs/api/viz.mdx`; modify `docs/docs/api/raster-functions.mdx`.

---

### Task 1: `[viz]` extra + lightweight CI lock + viz package skeleton + dep guard

**Files:**
- Modify: `python/geobrix/pyproject.toml` (after the `stac = [...]` block, ~line 129)
- Modify: `python/geobrix/requirements-pyrx-ci.in` (regen `.txt`)
- Create: `python/geobrix/src/databricks/labs/gbx/viz/__init__.py`
- Create: `python/geobrix/src/databricks/labs/gbx/viz/_env.py`
- Test: `python/geobrix/test/viz/__init__.py`, `python/geobrix/test/viz/test_env.py`

**Interfaces:**
- Produces: `databricks.labs.gbx.viz._env.assert_viz_available() -> None` (raises `ImportError` with `[viz]` guidance if matplotlib or geopandas missing). `viz/__init__.py` exports `plot_raster, plot_file, as_gdf, cells_as_gdf` (added in later tasks; in this task `__init__.py` is created empty-but-importable).

- [ ] **Step 1: Add the `[viz]` extra to pyproject.toml**

Insert after the `stac = [...]` block:

```toml
# Visualization helpers (gbx.viz): matplotlib raster rendering + geopandas/folium
# map adapters. Optional so [light] users who don't visualize don't pull the GUI
# stack. Package code imports only matplotlib + geopandas; folium + mapclassify are
# for the user's GeoDataFrame.explore() maps.
viz = [
    "matplotlib>=3.7,<4",
    "geopandas>=1.0,<2",
    "folium>=0.16,<1",
    "mapclassify>=2.6,<3",
]
```

- [ ] **Step 2: Create the viz package skeleton**

`viz/__init__.py`:
```python
"""gbx.viz — tier-agnostic visualization helpers (requires the [viz] extra).

Raster rendering (plot_raster / plot_file) and Spark DataFrame -> GeoDataFrame
adapters (as_gdf / cells_as_gdf) for interactive maps. Install with
``pip install 'geobrix[viz]'``.
"""
```
(Exports are added by later tasks; keep importable now.)

`viz/_env.py`:
```python
"""Lazy-dependency guard for gbx.viz (the [viz] extra).

Visualization deps are heavy and optional. Package code imports them only inside
functions, after calling assert_viz_available(), which raises a clear install
hint when they are absent — mirroring pyrx/_env.py::assert_rasterio_available().
"""


def assert_viz_available() -> None:
    """Raise ImportError with [viz] guidance if matplotlib or geopandas is missing.

    Only the deps gbx.viz code actually imports are checked (matplotlib for raster
    rendering, geopandas for the GeoDataFrame adapters). folium / mapclassify are
    user-side GeoDataFrame.explore() deps and are not imported by this package.
    """
    missing = []
    for mod in ("matplotlib", "geopandas"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        raise ImportError(
            "gbx.viz requires the [viz] extra (missing: "
            + ", ".join(missing)
            + "). Install with: pip install 'geobrix[viz]'"
        )
```

- [ ] **Step 3: Write the failing test**

`test/viz/__init__.py`: empty file.

`test/viz/test_env.py`:
```python
import builtins

import pytest

from databricks.labs.gbx.viz._env import assert_viz_available


def test_assert_viz_available_passes_when_present():
    # matplotlib + geopandas are installed in the light/dev/CI env.
    assert assert_viz_available() is None


def test_assert_viz_available_raises_actionable_error(monkeypatch):
    real_import = builtins.__import__

    def fake(name, *a, **k):
        if name == "geopandas":
            raise ImportError("No module named 'geopandas'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)
    with pytest.raises(ImportError) as ei:
        assert_viz_available()
    msg = str(ei.value)
    assert "geopandas" in msg and "geobrix[viz]" in msg
```

- [ ] **Step 4: Add `[viz]` deps to the light CI lock**

Edit `requirements-pyrx-ci.in` — insert before the `# --- test runner ---` block:
```
# --- visualization ([viz] extra): gbx.viz raster rendering + GeoDataFrame map
# adapters. Package code imports matplotlib + geopandas; folium + mapclassify are
# for user GeoDataFrame.explore() maps. Matches requirements-dev-container.in. ---
matplotlib==3.10.9
geopandas==1.1.1
folium==0.21.0
mapclassify==2.11.0
```
(If `requirements-dev-container.in` pins different versions, match those exact versions instead — check with `grep -iE "^matplotlib==|^geopandas==|^folium==|^mapclassify==" requirements-dev-container.txt`.)

- [ ] **Step 5: Regenerate the hash-pinned lock (in the container, via the proxy)**

Run:
```
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && UV_INDEX_URL=https://pypi-proxy.dev.databricks.com/simple uv pip compile --generate-hashes --python-version 3.12 -o requirements-pyrx-ci.txt requirements-pyrx-ci.in'
```
Expected: `requirements-pyrx-ci.txt` gains matplotlib/geopandas/folium/mapclassify + transitives, each with `--hash=sha256:` lines; no pre-existing top-level pin changes version.

- [ ] **Step 6: Run the tests**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_env.py`
Expected: 2 passed. (If matplotlib/geopandas aren't yet in the dev container, `pip install matplotlib geopandas folium mapclassify` in the container first; the lock guarantees CI has them.)

- [ ] **Step 7: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/viz test/viz && isort src/databricks/labs/gbx/viz test/viz'
git add python/geobrix/pyproject.toml python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-pyrx-ci.txt python/geobrix/src/databricks/labs/gbx/viz/ python/geobrix/test/viz/
git commit -m "feat(viz): [viz] extra + package skeleton + dep guard"
```

---

### Task 2: `viz._raster` percentile-stretch + decimation pipeline (pure functions)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/viz/_raster.py`
- Test: `python/geobrix/test/viz/test_raster.py`

**Interfaces:**
- Produces: `_needs_percentile_stretch(data) -> bool`; `_percentile_stretch(data, lo_pct=2, hi_pct=98) -> np.ndarray`; `_decimated_read(src, max_pixels) -> (data, transform, scale)`. Consumed by `plot_raster`/`plot_file` (Task 3).

- [ ] **Step 1: Write the failing tests**

`test/viz/test_raster.py`:
```python
import numpy as np
import pytest

from databricks.labs.gbx.viz import _raster


def test_needs_stretch_true_for_uint16_over_255():
    data = np.array([[0, 300], [1000, 65535]], dtype="uint16")
    assert _raster._needs_percentile_stretch(data) is True


def test_needs_stretch_false_for_float_and_small_int():
    assert _raster._needs_percentile_stretch(np.array([[0.1, 0.9]], dtype="float32")) is False
    assert _raster._needs_percentile_stretch(np.array([[0, 200]], dtype="uint8")) is False


def test_percentile_stretch_scales_to_unit_range_ignoring_mask():
    band = np.arange(100, dtype="uint16").reshape(1, 10, 10) * 10  # 0..9900
    masked = np.ma.MaskedArray(band, mask=np.zeros_like(band, dtype=bool))
    masked.mask[0, 0, 0] = True  # exclude an outlier-free pixel
    out = _raster._percentile_stretch(masked)
    assert out.dtype == np.float32
    assert float(out.min()) >= 0.0 and float(out.max()) <= 1.0
    assert isinstance(out, np.ma.MaskedArray)
    assert out.mask[0, 0, 0]  # mask preserved
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_raster.py`
Expected: FAIL (module/functions not defined).

- [ ] **Step 3: Implement the pipeline (ported verbatim from library.py)**

`viz/_raster.py`:
```python
"""Raster rendering pipeline for gbx.viz (decimation + percentile stretch).

Ported from notebooks/examples/eo-series/library.py. matplotlib/rasterio are
lazy-imported inside the public plotters (Task 3); the numeric helpers here use
only numpy and the rasterio dataset passed in.
"""

import numpy as np


def _decimated_read(src, max_pixels):
    """Read `src` (rasterio DatasetReader) decimated so max(width,height)<=max_pixels.

    Returns (data, transform, scale). masked=True so nodata is honored downstream.
    """
    import rasterio

    scale = max(src.width, src.height) / max_pixels
    if scale > 1:
        out_shape = (src.count, int(src.height // scale), int(src.width // scale))
        data = src.read(
            out_shape=out_shape,
            resampling=rasterio.enums.Resampling.bilinear,
            masked=True,
        )
        transform = src.transform * src.transform.scale(
            src.width / data.shape[-1],
            src.height / data.shape[-2],
        )
    else:
        data = src.read(masked=True)
        transform = src.transform
    return data, transform, scale


def _needs_percentile_stretch(data):
    """True when data is integer-typed with a max above matplotlib's RGB int 255."""
    if not np.issubdtype(data.dtype, np.integer):
        return False
    mx = np.ma.max(data) if isinstance(data, np.ma.MaskedArray) else data.max()
    if mx is np.ma.masked:
        return False
    return int(mx) > 255


def _percentile_stretch(data, lo_pct=2, hi_pct=98):
    """Per-band 2-98th percentile stretch to [0,1] float32; masked pixels excluded."""
    if data.ndim == 2:
        data = data[np.newaxis, ...]
    is_masked = isinstance(data, np.ma.MaskedArray)
    out = np.empty(data.shape, dtype=np.float32)
    for b in range(data.shape[0]):
        band = data[b]
        valid = band.compressed() if is_masked else np.asarray(band).ravel()
        if valid.size == 0:
            out[b] = 0.0
            continue
        lo, hi = np.percentile(valid, (lo_pct, hi_pct))
        rng = max(float(hi - lo), 1e-9)
        out[b] = np.clip((np.asarray(band, dtype=np.float32) - lo) / rng, 0.0, 1.0)
    return np.ma.MaskedArray(out, mask=data.mask) if is_masked else out
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_raster.py`
Expected: 3 passed.

- [ ] **Step 5: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/viz/_raster.py test/viz/test_raster.py && isort src/databricks/labs/gbx/viz/_raster.py test/viz/test_raster.py'
git add python/geobrix/src/databricks/labs/gbx/viz/_raster.py python/geobrix/test/viz/test_raster.py
git commit -m "feat(viz): raster decimation + percentile-stretch pipeline"
```

---

### Task 3: `viz._raster` plot_raster / plot_file + `_render`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/viz/_raster.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/viz/__init__.py`
- Test: `python/geobrix/test/viz/test_raster.py`

**Interfaces:**
- Consumes: `_decimated_read`, `_needs_percentile_stretch`, `_percentile_stretch` (Task 2); `assert_viz_available` (Task 1); `make_geotiff_bytes` (test fixture, `test/pyrx/conftest.py`).
- Produces: `plot_raster(raster_bytes, *, fig_w=10, fig_h=10, max_pixels=2000) -> None`; `plot_file(path, *, fig_w=10, fig_h=10, max_pixels=2000) -> None`.

- [ ] **Step 1: Write the failing tests**

Append to `test/viz/test_raster.py`:
```python
import matplotlib

matplotlib.use("Agg")  # headless: no display needed
import matplotlib.pyplot as plt  # noqa: E402

from databricks.labs.gbx.viz import plot_file, plot_raster  # noqa: E402
from test.pyrx.conftest import make_geotiff_bytes  # noqa: E402


def test_plot_raster_produces_a_figure():
    plt.close("all")
    plot_raster(make_geotiff_bytes(width=8, height=8, count=1))
    assert len(plt.get_fignums()) == 1
    plt.close("all")


def test_plot_file_produces_a_figure(tmp_path):
    p = tmp_path / "t.tif"
    p.write_bytes(make_geotiff_bytes(width=8, height=8, count=3))
    plt.close("all")
    plot_file(str(p))
    assert len(plt.get_fignums()) == 1
    plt.close("all")
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_raster.py`
Expected: FAIL (`plot_raster`/`plot_file` not importable from `gbx.viz`).

- [ ] **Step 3: Implement `_render` + the public plotters**

Append to `viz/_raster.py`:
```python
def _render(data, transform, *, title, fig_w, fig_h, scale):
    """Stretch when needed, then plot via rasterio.plot.show (Agg-safe)."""
    import matplotlib

    if matplotlib.get_backend().lower() != "agg":
        try:
            matplotlib.get_current_fig_manager()
        except Exception:
            matplotlib.use("Agg")
    from matplotlib import pyplot
    from rasterio.plot import show

    if _needs_percentile_stretch(data):
        data = _percentile_stretch(data)
    fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
    if data.shape[0] == 1:
        show(data, ax=ax, transform=transform, cmap="viridis")
    else:
        show(data, ax=ax, transform=transform)
    full_title = f"{title} (scale 1/{round(scale, 1)}x)" if scale > 1 else title
    ax.set_title(full_title)
    pyplot.show()


def plot_raster(raster_bytes, *, fig_w=10, fig_h=10, max_pixels=2000):
    """Render a raster from in-memory bytes (e.g. a tile's `raster` field).

    Auto-decimates above max_pixels; integer rasters whose values exceed 255
    (typical EO UInt16) get a per-band 2-98% percentile stretch. Single-band ->
    viridis; multi-band -> RGB. Requires the [viz] extra.
    """
    from databricks.labs.gbx.viz._env import assert_viz_available

    assert_viz_available()
    from rasterio.io import MemoryFile

    with MemoryFile(bytes(raster_bytes)) as mf:
        with mf.open() as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(data, transform, title="tile.raster", fig_w=fig_w, fig_h=fig_h, scale=scale)


def plot_file(path, *, fig_w=10, fig_h=10, max_pixels=2000):
    """Render a raster from disk (TIF, VRT, ...) with the plot_raster pipeline."""
    from databricks.labs.gbx.viz._env import assert_viz_available

    assert_viz_available()
    import rasterio

    with rasterio.open(path) as src:
        data, transform, scale = _decimated_read(src, max_pixels)
        _render(
            data,
            transform,
            title=f"File: {str(path).split('/')[-1]}",
            fig_w=fig_w,
            fig_h=fig_h,
            scale=scale,
        )
```

Update `viz/__init__.py` to export them:
```python
from databricks.labs.gbx.viz._raster import plot_file, plot_raster

__all__ = ["plot_raster", "plot_file"]
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_raster.py`
Expected: 5 passed.

- [ ] **Step 5: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/viz test/viz && isort src/databricks/labs/gbx/viz test/viz'
git add python/geobrix/src/databricks/labs/gbx/viz/_raster.py python/geobrix/src/databricks/labs/gbx/viz/__init__.py python/geobrix/test/viz/test_raster.py
git commit -m "feat(viz): plot_raster + plot_file"
```

---

### Task 4: `viz._vector` as_gdf / cells_as_gdf

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/viz/_vector.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/viz/__init__.py`
- Test: `python/geobrix/test/viz/test_vector.py`

**Interfaces:**
- Consumes: `assert_viz_available` (Task 1); a Spark session (test fixture pattern from `test/pyrx/conftest.py`).
- Produces: `as_gdf(df, wkt_col="wkt", *, max_rows=10_000) -> geopandas.GeoDataFrame`; `cells_as_gdf(df, cell_col="cellid", extra_cols=(), *, max_rows=10_000) -> geopandas.GeoDataFrame`.

- [ ] **Step 1: Write the failing tests**

`test/viz/test_vector.py`:
```python
import logging
import warnings

import pytest


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-vector-tests")
        .getOrCreate()
    )
    yield s


def test_as_gdf_crs_geometry_and_columns(spark):
    from databricks.labs.gbx.viz import as_gdf

    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = as_gdf(df)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"]
    assert "wkt" not in gdf.columns
    assert all(gdf.geometry.is_valid)


def test_as_gdf_truncates_and_warns_over_max_rows(spark):
    from databricks.labs.gbx.viz import as_gdf

    df = spark.range(5).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gdf = as_gdf(df, max_rows=2)
    assert len(gdf) == 2
    assert any("truncated" in str(w.message).lower() for w in caught)


def test_cells_as_gdf_boundary_from_h3_lib(spark):
    import h3

    from databricks.labs.gbx.viz import cells_as_gdf

    cell_int = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 5))
    df = spark.createDataFrame([(cell_int, 7)], ["cellid", "count"])
    gdf = cells_as_gdf(df, extra_cols=["count"])
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["count"]) == [7]
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_vector.py`
Expected: FAIL (`as_gdf`/`cells_as_gdf` not importable).

- [ ] **Step 3: Implement `_vector.py`**

```python
"""Spark DataFrame -> GeoDataFrame adapters for gbx.viz interactive maps.

Collect to the driver (single-node viz); guarded by max_rows so a large frame
does not OOM the driver. Boundaries for H3 cells use the h3 lib (portable), not
the Databricks-native h3_boundaryaswkt.
"""

import warnings


def as_gdf(df, wkt_col="wkt", *, max_rows=10_000):
    """Spark DataFrame with a WKT column -> geopandas.GeoDataFrame (EPSG:4326).

    Collects to the driver. With max_rows set (default 10_000) the frame is
    truncated to max_rows and a warning is emitted; pass max_rows=None to opt out.
    """
    from databricks.labs.gbx.viz._env import assert_viz_available

    assert_viz_available()
    import geopandas as gpd

    if wkt_col not in df.columns:
        raise ValueError(
            f"as_gdf: column {wkt_col!r} not in DataFrame columns {df.columns}"
        )
    if max_rows is None:
        pdf = df.toPandas()
    else:
        pdf = df.limit(max_rows + 1).toPandas()
        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]
            warnings.warn(
                f"as_gdf: output truncated to max_rows={max_rows} for driver-side "
                "viz; pass max_rows=None to collect all rows.",
                stacklevel=2,
            )
    geometry = gpd.GeoSeries.from_wkt(pdf[wkt_col], crs=4326)
    pdf = pdf.drop(columns=[wkt_col])
    pdf["geometry"] = geometry.values
    return gpd.GeoDataFrame(pdf, geometry="geometry", crs=4326)


def cells_as_gdf(df, cell_col="cellid", extra_cols=(), *, max_rows=10_000):
    """H3 cell ids (bigint) -> boundary polygons as a GeoDataFrame (EPSG:4326).

    Boundaries come from the h3 lib (h3 v4 takes a string index, so each bigint
    cellid is converted via h3.int_to_str). extra_cols are carried through.
    """
    from databricks.labs.gbx.viz._env import assert_viz_available

    assert_viz_available()
    import h3
    from shapely.geometry import Polygon

    cols = [cell_col, *extra_cols]
    if max_rows is None:
        pdf = df.select(*cols).toPandas()
    else:
        pdf = df.select(*cols).limit(max_rows + 1).toPandas()
        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]
            warnings.warn(
                f"cells_as_gdf: output truncated to max_rows={max_rows} for "
                "driver-side viz; pass max_rows=None to collect all rows.",
                stacklevel=2,
            )

    def _boundary(cell_int):
        ring = h3.cell_to_boundary(h3.int_to_str(int(cell_int)))
        # h3 v4 returns (lat, lng) pairs; shapely wants (lng, lat).
        return Polygon([(lng, lat) for lat, lng in ring])

    import geopandas as gpd

    geometry = [_boundary(c) for c in pdf[cell_col]]
    return gpd.GeoDataFrame(pdf, geometry=geometry, crs=4326)
```

Update `viz/__init__.py`:
```python
from databricks.labs.gbx.viz._raster import plot_file, plot_raster
from databricks.labs.gbx.viz._vector import as_gdf, cells_as_gdf

__all__ = ["plot_raster", "plot_file", "as_gdf", "cells_as_gdf"]
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/viz/test_vector.py`
Expected: 3 passed. (If the h3 v4 boundary ring order differs, adjust `_boundary`; verify with `h3.cell_to_boundary(h3.int_to_str(cell_int))` shape in the container.)

- [ ] **Step 5: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/viz test/viz && isort src/databricks/labs/gbx/viz test/viz'
git add python/geobrix/src/databricks/labs/gbx/viz/_vector.py python/geobrix/src/databricks/labs/gbx/viz/__init__.py python/geobrix/test/viz/test_vector.py
git commit -m "feat(viz): as_gdf + cells_as_gdf"
```

---

### Task 5: pyrx escape-hatches `tile_to_numpy` + `rst_apply`

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/escape.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (add the two imports/re-exports near the other `core` imports, ~line 38-50)
- Test: `python/geobrix/test/pyrx/test_escape.py`

**Interfaces:**
- Consumes: `pyrx._serde.open_tile(raster_bytes)` (context manager → rasterio DatasetReader); `pyrx._udf._col` (str/Column normalizer); `make_geotiff_bytes` + `spark` (test/pyrx/conftest.py).
- Produces: `tile_to_numpy(tile_or_bytes) -> np.ndarray`; `rst_apply(tile_col, fn, returnType=DoubleType()) -> Column`. Both re-exported on `databricks.labs.gbx.pyrx.functions`.

- [ ] **Step 1: Write the failing tests**

`test/pyrx/test_escape.py`:
```python
import numpy as np
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.functions import rst_apply, tile_to_numpy
from test.pyrx.conftest import make_geotiff_bytes


def test_tile_to_numpy_bytes_and_struct_agree():
    raw = make_geotiff_bytes(width=4, height=3, count=2)
    arr_bytes = tile_to_numpy(raw)
    assert isinstance(arr_bytes, np.ndarray)
    assert arr_bytes.shape == (2, 3, 4)
    tile = _serde.build_tile(raw, "GTiff", cellid=0)
    arr_struct = tile_to_numpy(tile)
    assert np.array_equal(arr_bytes, arr_struct)


def test_rst_apply_scalar_with_nondefault_returntype(spark):
    raw = make_geotiff_bytes(width=4, height=3, count=1)
    tile = _serde.build_tile(raw, "GTiff", cellid=0)
    df = spark.createDataFrame([(tile,)], ["tile"])
    out = df.select(
        rst_apply("tile", lambda ds: ds.count, returnType=IntegerType()).alias("nbands")
    ).collect()
    assert out[0]["nbands"] == 1  # ds.count == band count == 1


def test_rst_apply_null_tile_returns_null(spark):
    df = spark.createDataFrame([(None,)], "tile struct<cellid:bigint,raster:binary,metadata:map<string,string>>")
    out = df.select(rst_apply("tile", lambda ds: 1.0).alias("v")).collect()
    assert out[0]["v"] is None
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_escape.py`
Expected: FAIL (`rst_apply`/`tile_to_numpy` not importable).

- [ ] **Step 3: Implement `core/escape.py`**

```python
"""Python-only escape-hatches for users whose needs fall outside the rst_* surface.

NOT SQL-registered (tile_to_numpy returns a host object; rst_apply takes a Python
callable), so neither appears in registered_functions.txt / function-info.json.
"""

from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import DataType, DoubleType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx._udf import _col


def tile_to_numpy(tile_or_bytes):
    """Read a tile's raster into a numpy ndarray (all bands).

    Accepts a tile struct (a Row/dict with a 'raster' field) or raw bytes. The
    "drop to numpy" hatch: call on a collected tile, or inside your own UDF.
    """
    if isinstance(tile_or_bytes, (bytes, bytearray)):
        raw = bytes(tile_or_bytes)
    else:
        raw = bytes(tile_or_bytes["raster"])
    with _serde.open_tile(raw) as ds:
        return ds.read()


def rst_apply(tile_col, fn, returnType: DataType = DoubleType()) -> Column:
    """Apply an arbitrary rasterio function to each tile, returning one scalar/row.

    fn receives an open rasterio DatasetReader and returns a value of returnType
    (default DoubleType; any Spark DataType). The escape-hatch for "GeoBrix lacks
    function X — run your own rasterio per tile". Scalar return only. Null/empty
    tile -> null.
    """

    @udf(returnType=returnType)
    def _apply(tile):
        if tile is None or tile["raster"] is None:
            return None
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            return fn(ds)

    return _apply(_col(tile_col))
```

Modify `pyrx/functions.py` — add near the other `from ...pyrx.core import` lines:
```python
from databricks.labs.gbx.pyrx.core.escape import rst_apply, tile_to_numpy
```
(Confirm `_udf._col` exists and accepts a str/Column; it is used at `_udf.py:22`.)

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_escape.py`
Expected: 3 passed.

- [ ] **Step 5: Confirm binding-parity is unaffected**

Run: `bash scripts/commands/gbx-test-bindings.sh` (or `docs/scripts/check-binding-parity.py`).
Expected: PASS — `tile_to_numpy`/`rst_apply` are NOT in `registered_functions.txt`, so the counts are unchanged.

- [ ] **Step 6: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/pyrx/core/escape.py src/databricks/labs/gbx/pyrx/functions.py test/pyrx/test_escape.py && isort src/databricks/labs/gbx/pyrx/core/escape.py src/databricks/labs/gbx/pyrx/functions.py test/pyrx/test_escape.py'
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/escape.py python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_escape.py
git commit -m "feat(pyrx): tile_to_numpy + rst_apply escape-hatches"
```

---

### Task 6: Wire `test/viz` into the lightweight CI tier + clean-venv verify

**Files:**
- Modify: `python/geobrix/test/conftest.py` (line 40, `_LIGHT_TEST_DIRS`)
- Modify: `.github/actions/pyrx_build/action.yml` (the `pytest test/pyrx ...` dir list, ~line 67)

**Interfaces:** none (CI config only).

- [ ] **Step 1: Add `"viz"` to `_LIGHT_TEST_DIRS`**

In `test/conftest.py`, change:
```python
_LIGHT_TEST_DIRS = ["bench", "ds", "pyrx", "pyvx", "pygx", "pmtiles_light", "stac"]
```
to:
```python
_LIGHT_TEST_DIRS = ["bench", "ds", "pyrx", "pyvx", "pygx", "pmtiles_light", "stac", "viz"]
```
And update the docstring's "Light test dirs so far:" line to append `, viz`.

- [ ] **Step 2: Add `test/viz` to the light CI pytest dir list**

In `.github/actions/pyrx_build/action.yml`, change the pytest line to include `test/viz`:
```
pytest test/pyrx test/ds test/pyvx test/pygx test/pmtiles_light test/stac test/viz -m "not integration" -v
```
Update the preceding comment to list `viz (gbx.viz, [viz] extra)`.

- [ ] **Step 3: Verify heavy phase skips viz (no rasterio/geopandas)**

Run (simulates heavy env — block the light deps):
```
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && python3 - <<PY
import builtins
_r=builtins.__import__
BLOCK={"rasterio","geopandas","matplotlib","pandas"}
def _imp(n,*a,**k):
    if n.split(".")[0] in BLOCK: raise ModuleNotFoundError(f"No module named {n.split(\".\")[0]!r}")
    return _r(n,*a,**k)
builtins.__import__=_imp
import importlib.util
assert importlib.util.find_spec("rasterio") is not None  # find_spec still works (installed); conftest gates on this
PY'
```
Expected: confirms conftest's `find_spec("rasterio")` mechanism. (Full heavy-phase exclusion is exercised by CI; the conftest already ignores `_LIGHT_TEST_DIRS` when rasterio is absent.)

- [ ] **Step 4: Clean-venv-from-lock verification (catches missing transitives)**

Run:
```
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && python3 -m venv /tmp/vizverify && /tmp/vizverify/bin/pip install --upgrade pip==25.0.1 && /tmp/vizverify/bin/pip install --require-hashes -r requirements-pyrx-ci.txt && /tmp/vizverify/bin/pip install --no-deps . && /tmp/vizverify/bin/python -m pytest test/pyrx test/ds test/pyvx test/pygx test/pmtiles_light test/stac test/viz -m "not integration" -q; rm -rf /tmp/vizverify'
```
Expected: all pass, no `ModuleNotFoundError`. If any viz dep is missing from the lock, add it to `requirements-pyrx-ci.in` and re-run Task 1 Step 5.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/conftest.py .github/actions/pyrx_build/action.yml
git commit -m "ci(viz): run test/viz in the lightweight tier (heavy skips)"
```

---

### Task 7: Docs — viz.mdx + escape-hatches section

**Files:**
- Create: `docs/docs/api/viz.mdx`
- Modify: `docs/docs/api/raster-functions.mdx` (add an "Escape hatches" section)
- Modify: `docs/sidebars.js` (add the viz page to the API sidebar, matching the existing entries)

**Interfaces:** none (docs).

- [ ] **Step 1: Write `docs/docs/api/viz.mdx`**

Document `plot_raster`, `plot_file`, `as_gdf`, `cells_as_gdf` with:
- A `:::note` that these require `pip install 'geobrix[viz]'`.
- A short, runnable example per function (use a real sample raster path under `/Volumes/main/geobrix_samples/...` for plot_*; a small `spark.createDataFrame([...WKT...])` for as_gdf; an H3 example for cells_as_gdf).
- Mention `max_rows` truncation for the vector adapters and that `.explore()` needs folium/mapclassify (bundled in `[viz]`).
- No internal/wave vocabulary. Add `title:` frontmatter so the browser tab isn't the logo JSX (per repo convention).

Frontmatter + first lines:
```mdx
---
title: Visualization (gbx.viz)
---

# Visualization (`gbx.viz`)

`gbx.viz` renders rasters and turns Spark DataFrames into GeoDataFrames for
interactive maps. It is tier-agnostic — use it with `pyrx` or `rasterx` tiles.

:::note Install
`gbx.viz` requires the visualization extra: `pip install 'geobrix[viz]'`.
:::
```

- [ ] **Step 2: Add the "Escape hatches" section to raster-functions.mdx**

At the end of `docs/docs/api/raster-functions.mdx`, add:
```mdx
## Escape hatches

When a raster operation isn't in the `rst_*` surface, drop down to rasterio/numpy
per tile (lightweight tier):

- `tile_to_numpy(tile_or_bytes)` — read a tile's raster into a NumPy array (all
  bands). Accepts a tile struct or raw bytes.
- `rst_apply(tile_col, fn, returnType=DoubleType())` — apply your own function to
  each tile's open rasterio dataset, returning one value per row of `returnType`.

These are Python-only helpers on `databricks.labs.gbx.pyrx.functions` (not SQL
functions).
```

- [ ] **Step 3: Add the viz page to the sidebar**

In `docs/sidebars.js`, add `'api/viz'` to the API category items, positioned near the other function pages (match the existing entry style).

- [ ] **Step 4: Verify internals-leak gate**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/viz.mdx docs/docs/api/raster-functions.mdx`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add docs/docs/api/viz.mdx docs/docs/api/raster-functions.mdx docs/sidebars.js
git commit -m "docs(viz): gbx.viz page + raster escape-hatches section"
```

---

## Final verification (after all tasks)

- [ ] Full light suite incl. viz in a clean venv from the lock (Task 6 Step 4) — all pass.
- [ ] `bash scripts/commands/gbx-lint-python.sh --check` — isort/black/flake8 clean (run in container).
- [ ] `bash scripts/commands/gbx-test-bindings.sh` — binding-parity unchanged.
- [ ] Push to `beta/0.4.0` (updates PR #41); confirm CI `build main` green on both heavy and light jobs.
