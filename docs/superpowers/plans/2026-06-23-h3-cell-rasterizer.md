# H3 Cell Rasterizer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a DGGS-cell rasterizer to RasterX — `rst_h3_rasterize_agg` (H3 cellids → raster tile, pixel-centroid burn) plus the `rst_h3_gridspec` shared-grid helper and a scalar `gbx_h3_cell_bbox` — in both the lightweight (pyrx) and heavyweight (rasterx) tiers.

**Architecture:** Pixel-centroid burn is the exact inverse of `rst_h3_rastertogrid` (each output pixel takes the value of the cell containing its center). The light tier is built first as pure functions + a grouped `pandas_udf` (SQL returns BINARY, Python wrapper composes the tile struct — the established light-agg convention); the heavy tier mirrors `RST_RasterizeAgg` as a Scala UDAF, validated against the light tier by a JAR-gated parity test. The grid helper computes a snapped global-lattice grid so per-threshold bands stack aligned and per-cell tiles merge losslessly.

**Tech Stack:** Python 3.12, PySpark 4.0, rasterio + h3 + numpy + pyproj (all already light deps), Scala 2.13 / Spark 4 / GDAL (heavy). Spec: `docs/superpowers/specs/2026-06-23-h3-cell-rasterizer-design.md`.

## Global Constraints

- Target branch `beta/0.4.0`. Names are fixed: `rst_h3_rasterize_agg`, `rst_h3_gridspec`, scalar `gbx_h3_cell_bbox`.
- **Algorithm:** pixel-centroid burn — pixel center → `h3.latlng_to_cell(lat, lon, res)` → value if cell in set else NoData (-9999.0). Resolution inferred from the cells (`h3.get_resolution`); error on mixed resolutions in a group.
- **Default `value`** when omitted/null = `1.0` (presence mask).
- **CRS:** default EPSG:4326; optional projected `srid` (pixel centers unprojected to lon/lat for the H3 lookup). No new deps.
- **Grid:** default auto extent + pixel size from H3 resolution; `mode='centroids'` (default) | `'spatial_envelope'`; `kring_pad` (int, default 1) expands the cell set by N rings (NoData margin) before bounds; origin snapped to a `pixel_size` multiple (global lattice).
- **Light SQL agg returns `BINARY`**; the Python wrapper composes the tile struct via `_as_tile_udf` (per [[light-agg-struct-return-convention]]). Document the deviation as an orange `:::warning` like the other `rst_*_agg` light functions.
- **Int handling:** PySpark passes H3 ids as signed `Long`; h3 ids are unsigned 64-bit. Normalize every cellid through `_h3_str(cellid)` = `h3.int_to_str(int(cellid) & 0xFFFFFFFFFFFFFFFF)` and key all maps by the h3 string (per [[jts_towkb_strips_z]] int-tolerance note).
- **Serverless-safe:** no `spark.conf.set`, `.cache()/.persist()`, `_jvm`/`sparkContext`/`.rdd` in package code.
- **Binding parity:** add every new SQL function to Scala `register`, Python `functions.py`, `function-info.json`, and `docs/tests-function-info/registered_functions.txt` (the QC `binding-parity` gate runs on push).
- All tests/lint run in the `geobrix-dev` Docker container (`bash scripts/commands/gbx-docker-start.sh`; `bash scripts/commands/gbx-test-python.sh --path <p>`; lint via in-container black/isort). Scala tests via `bash scripts/commands/gbx-test-scala.sh --suite '<FQCN>'`.
- No internal/"wave" vocabulary in any `docs/docs/` page.

---

## File Structure

- Create `python/geobrix/src/databricks/labs/gbx/pyrx/core/cellraster.py` — pure burn + gridspec math (`cells_to_raster`, `compute_gridspec`, `cell_bbox`, `_h3_str`).
- Modify `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — scalar `gbx_h3_cell_bbox` UDF, `rst_h3_gridspec` DataFrame helper, grouped `_rst_h3_rasterize_agg_udf` + `rst_h3_rasterize_agg` Python API, `SQL_REGISTRY` entries.
- Create `python/geobrix/test/pyrx/test_core_cellraster.py`, `.../test_h3_gridspec.py`, `.../test_h3_rasterize_agg.py`, `.../test_h3_rasterize_validate.py`, `.../test_h3_rasterize_fcc.py`.
- Create fixture `python/geobrix/test/pyrx/data/fcc_uflw_miamidade_subset.csv`.
- Create Scala `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_H3_RasterizeAgg.scala`; modify `rasterx/functions.scala`. Scalar `gbx_h3_cell_bbox` heavy expr under `gridx` expressions; Scala tests under `src/test/scala/.../rasterx/`.
- Modify `docs/tests-function-info/registered_functions.txt`, `docs/tests/python/api/rasterx_functions_sql.py`, `docs/docs/api/raster-functions.mdx`.
- Create `notebooks/examples/h3-rasterize/` (notebook + README) — DEM isoband demo.

---

### Task 1: Light burn + gridspec core (pure functions)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/core/cellraster.py`
- Test: `python/geobrix/test/pyrx/test_core_cellraster.py`

**Interfaces:**
- Produces: `_h3_str(cellid:int)->str`; `cell_bbox(cellid:int, srid:int=4326, mode:str="centroids")->tuple[float,float,float,float]`; `compute_gridspec(cellids, srid=4326, pixel_size=None, mode="centroids", kring_pad=1)->tuple[xmin,ymin,xmax,ymax,pixel_size,width,height,srid]`; `cells_to_raster(cell_values:dict[int,float], xmin,ymin,xmax,ymax,pixel_size,width,height,srid,resolution)->bytes` (arg order matches the `compute_gridspec` 8-tuple). Consumed by Tasks 2 (gridspec/bbox) and 3 (agg).

- [ ] **Step 1: Write the failing tests**

`python/geobrix/test/pyrx/test_core_cellraster.py`:
```python
import h3
import numpy as np

from databricks.labs.gbx.pyrx.core import cellraster as cr
from databricks.labs.gbx.pyrx import _serde


def _cell(lat, lon, res=9):
    return h3.str_to_int(h3.latlng_to_cell(lat, lon, res))


def test_h3_str_normalizes_signed_long():
    cid_unsigned = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 9))
    signed = cid_unsigned - (1 << 64) if cid_unsigned >= (1 << 63) else cid_unsigned
    assert cr._h3_str(signed) == cr._h3_str(cid_unsigned) == h3.int_to_str(cid_unsigned)


def test_compute_gridspec_single_cell_centroids_uses_kring1():
    cid = _cell(0.0, 0.0, 9)
    # kring_pad=1 (default): a single cell is non-degenerate (neighbor centroids)
    xmin, ymin, xmax, ymax, px, w, h, srid = cr.compute_gridspec([cid])
    assert w >= 3 and h >= 3 and srid == 4326
    assert xmax > xmin and ymax > ymin
    # kring_pad=0: degenerate -> 1x1 (centroid only)
    g0 = cr.compute_gridspec([cid], kring_pad=0)
    assert g0[5] == 1 and g0[6] == 1


def test_compute_gridspec_origin_snapped_to_lattice():
    cid = _cell(10.0, 20.0, 9)
    xmin, ymin, xmax, ymax, px, w, h, srid = cr.compute_gridspec([cid], pixel_size=0.01)
    # origin is an integer multiple of pixel_size -> independently-built grids align
    assert abs((xmin / 0.01) - round(xmin / 0.01)) < 1e-9
    assert abs((ymax / 0.01) - round(ymax / 0.01)) < 1e-9


def test_compute_gridspec_rejects_mixed_resolution():
    import pytest
    a = _cell(0.0, 0.0, 9)
    b = _cell(0.0, 0.0, 8)
    with pytest.raises(ValueError, match="resolution"):
        cr.compute_gridspec([a, b])


def test_cells_to_raster_partition_property():
    # polyfill a small area -> cells; rasterize as presence mask; every burned
    # pixel centroid must re-index to a cell IN the set, every NoData pixel must not.
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = {h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)}
    cell_values = {c: 1.0 for c in cells}
    g = cr.compute_gridspec(list(cells), kring_pad=1)
    raster = cr.cells_to_raster(cell_values, *g, resolution=res)
    with _serde.open_tile(raster) as ds:
        arr = ds.read(1)
        t = ds.transform
        nod = ds.nodata
        cellset = {cr._h3_str(c) for c in cells}
        for row in range(ds.height):
            for col in range(ds.width):
                lon, lat = (t * (col + 0.5, row + 0.5))
                idx = h3.latlng_to_cell(lat, lon, res)
                burned = arr[row, col] != nod
                assert burned == (idx in cellset)
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_cellraster.py`
Expected: FAIL (`No module named ...core.cellraster`).

- [ ] **Step 3: Implement `cellraster.py`**

```python
"""Rasterize a set of H3 cells onto a regular grid (pixel-centroid burn).

The inverse of core.gridagg.raster_to_grid: there each pixel centroid is indexed
to an H3 cell; here each output pixel takes the value of the cell containing its
centroid. Pure functions (no Spark); rasterio + h3 + numpy + pyproj only.
"""
import math

import h3
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import Affine

_NODATA = -9999.0
_U64 = 0xFFFFFFFFFFFFFFFF


def _h3_str(cellid) -> str:
    """Canonical h3 string for a (possibly signed) Spark Long cell id."""
    return h3.int_to_str(int(cellid) & _U64)


def _resolution(cell_strs) -> int:
    res = h3.get_resolution(next(iter(cell_strs)))
    for c in cell_strs:
        if h3.get_resolution(c) != res:
            raise ValueError("H3 cell set has mixed resolutions")
    return res


def _reproject(xs, ys, src, dst):
    if src == dst:
        return np.asarray(xs, dtype="float64"), np.asarray(ys, dtype="float64")
    from pyproj import Transformer

    tr = Transformer.from_crs(src, dst, always_xy=True)
    x2, y2 = tr.transform(np.asarray(xs), np.asarray(ys))
    return np.asarray(x2, dtype="float64"), np.asarray(y2, dtype="float64")


def cell_bbox(cellid, srid=4326, mode="centroids"):
    """(xmin, ymin, xmax, ymax) for one cell in `srid`.

    mode='centroids' -> the centroid point (degenerate bbox); 'spatial_envelope'
    -> the hexagon boundary envelope.
    """
    c = _h3_str(cellid)
    if mode == "centroids":
        lat, lon = h3.cell_to_latlng(c)
        lons, lats = [lon], [lat]
    elif mode == "spatial_envelope":
        b = h3.cell_to_boundary(c)  # [(lat, lon), ...]
        lats = [p[0] for p in b]
        lons = [p[1] for p in b]
    else:
        raise ValueError(f"unknown mode {mode!r}")
    xs, ys = _reproject(lons, lats, 4326, srid)
    return float(xs.min()), float(ys.min()), float(xs.max()), float(ys.max())


def compute_gridspec(cellids, srid=4326, pixel_size=None, mode="centroids", kring_pad=1):
    """Snapped, lattice-aligned grid spec for a cell set.

    Returns (xmin, ymin, xmax, ymax, pixel_size, width, height, srid).
    """
    cells = {_h3_str(c) for c in cellids}
    if not cells:
        raise ValueError("empty cell set")
    res = _resolution(cells)
    if kring_pad and kring_pad > 0:
        padded = set()
        for c in cells:
            padded.update(h3.grid_disk(c, kring_pad))
        cells = padded

    if mode == "centroids":
        pts = [h3.cell_to_latlng(c) for c in cells]  # (lat, lon)
        lons = [p[1] for p in pts]
        lats = [p[0] for p in pts]
    elif mode == "spatial_envelope":
        lons, lats = [], []
        for c in cells:
            for (la, lo) in h3.cell_to_boundary(c):
                lons.append(lo)
                lats.append(la)
    else:
        raise ValueError(f"unknown mode {mode!r}")

    xs, ys = _reproject(lons, lats, 4326, srid)
    bxmin, bxmax = float(xs.min()), float(xs.max())
    bymin, bymax = float(ys.min()), float(ys.max())

    if pixel_size is None:
        edge_m = h3.average_hexagon_edge_length(res, unit="m")
        if srid == 4326:
            midlat = (bymin + bymax) / 2.0
            pixel_size = edge_m / (111320.0 * max(math.cos(math.radians(midlat)), 1e-6))
        else:
            pixel_size = edge_m

    if mode == "centroids":
        half = pixel_size / 2.0
        bxmin -= half; bxmax += half
        bymin -= half; bymax += half

    xmin = math.floor(bxmin / pixel_size) * pixel_size
    ymax = math.ceil(bymax / pixel_size) * pixel_size
    width = max(1, int(math.ceil((bxmax - xmin) / pixel_size)))
    height = max(1, int(math.ceil((ymax - bymin) / pixel_size)))
    xmax = xmin + width * pixel_size
    ymin = ymax - height * pixel_size
    return (xmin, ymin, xmax, ymax, pixel_size, width, height, srid)


def cells_to_raster(cell_values, xmin, ymin, xmax, ymax, pixel_size, width, height,
                    srid, resolution):
    """Burn {cellid:int -> value:float} onto a width x height grid (centroid burn).

    Arg order matches the `compute_gridspec` 8-tuple (so callers splat it:
    `cells_to_raster(cell_values, *gridspec, resolution=res)`). The snapped grid has
    square pixels of `pixel_size`. Returns single-band float64 GTiff bytes; NoData
    where no cell covers a pixel.
    """
    lut = {_h3_str(c): float(v) for c, v in cell_values.items()}
    transform = Affine(pixel_size, 0.0, xmin, 0.0, -pixel_size, ymax)

    cols = (np.arange(width) + 0.5)
    rows = (np.arange(height) + 0.5)
    gx, gy = np.meshgrid(xmin + cols * pixel_size, ymax - rows * pixel_size)  # (h, w)
    lon, lat = _reproject(gx.ravel(), gy.ravel(), srid, 4326)

    out = np.full(lon.size, _NODATA, dtype="float64")
    # Scalar h3 index per pixel (no array API). The grid is bounded to the cells'
    # padded bbox, so this is O(pixels-in-footprint). PERF FOLLOW-UP: restrict to
    # pixels within each cell's local window instead of the whole grid.
    for i in range(lon.size):
        v = lut.get(h3.int_to_str(h3.str_to_int(h3.latlng_to_cell(float(lat[i]), float(lon[i]), resolution))))
        if v is not None:
            out[i] = v

    data = out.reshape(height, width)
    profile = dict(driver="GTiff", width=width, height=height, count=1,
                   dtype="float64", crs=f"EPSG:{srid}", transform=transform, nodata=_NODATA)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_cellraster.py`
Expected: 5 passed. (If `h3.polygon_to_cells` differs in 4.4.2, use `h3.polygon_to_cells_experimental(poly, res, contain="overlap")` as in `tessellate.py:91`.)

- [ ] **Step 5: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/pyrx/core/cellraster.py test/pyrx/test_core_cellraster.py && isort src/databricks/labs/gbx/pyrx/core/cellraster.py test/pyrx/test_core_cellraster.py'
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/cellraster.py python/geobrix/test/pyrx/test_core_cellraster.py
git commit -m "feat(pyrx): H3 cell rasterize core (centroid burn + gridspec)"
```

---

### Task 2: Light scalar `gbx_h3_cell_bbox` + `rst_h3_gridspec` helper

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- Test: `python/geobrix/test/pyrx/test_h3_gridspec.py`

**Interfaces:**
- Consumes: `cellraster.cell_bbox`, `cellraster.compute_gridspec` (Task 1); `_col`, `_serde`, the `spark` fixture (`test/pyrx/conftest.py`).
- Produces: scalar UDF `_h3_cell_bbox_udf` registered as `gbx_h3_cell_bbox`; Python `rst_h3_gridspec(df, cell_col="cellid", *group_cols, srid=4326, pixel_size=None, mode="centroids", kring_pad=1) -> DataFrame` (adds a `grid STRUCT<xmin,ymin,xmax,ymax,pixel_size,width,height,srid>` column). Consumed by the customer workflow + Task 8 docs.

- [ ] **Step 1: Write the failing test**

`python/geobrix/test/pyrx/test_h3_gridspec.py`:
```python
import h3

from databricks.labs.gbx.pyrx import functions as rx


def test_rst_h3_gridspec_matches_core(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = rx.rst_h3_gridspec(df, "cellid", "tx", pixel_size=0.005).collect()
    assert len(out) == 1
    g = out[0]["grid"]
    from databricks.labs.gbx.pyrx.core import cellraster as cr
    exp = cr.compute_gridspec(cells, pixel_size=0.005)
    assert g["width"] == exp[5] and g["height"] == exp[6]
    assert abs(g["xmin"] - exp[0]) < 1e-9
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_h3_gridspec.py`
Expected: FAIL (`module 'functions' has no attribute 'rst_h3_gridspec'`).

- [ ] **Step 3: Implement in `functions.py`**

Add the scalar bbox UDF + helper (near the other tile/scalar UDFs; import `cellraster` at the top with the other `from ...pyrx.core import` lines):
```python
from databricks.labs.gbx.pyrx.core import cellraster as cellraster_core

_GRID_SCHEMA = StructType([
    StructField("xmin", DoubleType()), StructField("ymin", DoubleType()),
    StructField("xmax", DoubleType()), StructField("ymax", DoubleType()),
    StructField("pixel_size", DoubleType()),
    StructField("width", IntegerType()), StructField("height", IntegerType()),
    StructField("srid", IntegerType()),
])

_BBOX_SCHEMA = StructType([
    StructField("xmin", DoubleType()), StructField("ymin", DoubleType()),
    StructField("xmax", DoubleType()), StructField("ymax", DoubleType()),
])


@f.udf(_BBOX_SCHEMA)
def _h3_cell_bbox_udf(cellid, srid, mode):
    if cellid is None:
        return None
    xmin, ymin, xmax, ymax = cellraster_core.cell_bbox(
        int(cellid), int(srid) if srid is not None else 4326, mode or "centroids"
    )
    return (xmin, ymin, xmax, ymax)


def gbx_h3_cell_bbox(cellid: ColLike, srid: ColLike = None, mode: ColLike = None) -> Column:
    """Bounding box of one H3 cell in `srid` (centroid point or hexagon envelope)."""
    return _h3_cell_bbox_udf(_col(cellid), _col(srid) if srid is not None else f.lit(4326),
                             _col(mode) if mode is not None else f.lit("centroids"))


def rst_h3_gridspec(df, cell_col="cellid", *group_cols, srid=4326, pixel_size=None,
                    mode="centroids", kring_pad=1):
    """Add a `grid` struct (snapped shared canvas) per group of H3 cells.

    Implemented as scalar per-cell bbox + native min/max + the snap arithmetic, so
    it works identically in both tiers and avoids the grouped-pandas_udf struct limit.
    """
    @f.udf(_GRID_SCHEMA)
    def _snap_udf(xmin, ymin, xmax, ymax, mid_lat, res):
        return cellraster_core.snap_bounds(
            float(xmin), float(ymin), float(xmax), float(ymax),
            srid, pixel_size, mode, float(mid_lat), int(res),
        )

    b = _h3_cell_bbox_udf(_col(cell_col), f.lit(srid), f.lit(mode))
    # NOTE: kring_pad expansion for the bounds is applied inside the bbox via the
    # padded boundary; for centroids mode the half-pixel pad is in snap_bounds.
    gcols = list(group_cols)
    enriched = df.withColumn("_bb", b)
    agg = (enriched.groupBy(*gcols) if gcols else enriched.groupBy())
    bounds = agg.agg(
        f.min("_bb.xmin").alias("xmin"), f.min("_bb.ymin").alias("ymin"),
        f.max("_bb.xmax").alias("xmax"), f.max("_bb.ymax").alias("ymax"),
    )
    mid = ((f.col("ymin") + f.col("ymax")) / 2.0)
    res_col = f.lit(0)  # resolution carried via snap default if unused; see note
    return bounds.withColumn(
        "grid",
        _snap_udf(f.col("xmin"), f.col("ymin"), f.col("xmax"), f.col("ymax"), mid, res_col),
    )
```
And add a `snap_bounds(...)` helper to `cellraster.py` extracting the snap arithmetic from `compute_gridspec` (DRY — `compute_gridspec` should call it too):
```python
def snap_bounds(bxmin, bymin, bxmax, bymax, srid, pixel_size, mode, mid_lat, res):
    if pixel_size is None:
        edge_m = h3.average_hexagon_edge_length(res, unit="m") if res else 1.0
        pixel_size = (edge_m / (111320.0 * max(math.cos(math.radians(mid_lat)), 1e-6))
                      if srid == 4326 else edge_m)
    if mode == "centroids":
        half = pixel_size / 2.0
        bxmin -= half; bxmax += half; bymin -= half; bymax += half
    xmin = math.floor(bxmin / pixel_size) * pixel_size
    ymax = math.ceil(bymax / pixel_size) * pixel_size
    width = max(1, int(math.ceil((bxmax - xmin) / pixel_size)))
    height = max(1, int(math.ceil((ymax - bymin) / pixel_size)))
    return (xmin, ymax - height * pixel_size, xmin + width * pixel_size, ymax,
            pixel_size, width, height, srid)
```
Register the scalar in `SQL_REGISTRY` (the `_sql_accessors`/scalar map): add `"gbx_h3_cell_bbox": _h3_cell_bbox_udf`.

**Implementation note for the implementer:** the `kring_pad` and per-cell resolution need to reach `_snap_udf`. Resolve by computing the H3 resolution from a sampled `cell_col` value at helper-call time (driver-side `df.select(cell_col).first()`), and apply `kring_pad` by expanding each cell's bbox via `cellraster.cell_bbox` over `h3.grid_disk(cell, kring_pad)` inside `_h3_cell_bbox_udf` (pass `kring_pad` as a 4th lit arg). Keep `compute_gridspec` (Task 1) as the single-call reference; the helper reproduces its result group-wise. The Task-2 test asserts the helper equals `compute_gridspec` — make them agree.

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_h3_gridspec.py`
Expected: 1 passed.

- [ ] **Step 5: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/pyrx/functions.py src/databricks/labs/gbx/pyrx/core/cellraster.py test/pyrx/test_h3_gridspec.py && isort <same files>'
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/src/databricks/labs/gbx/pyrx/core/cellraster.py python/geobrix/test/pyrx/test_h3_gridspec.py
git commit -m "feat(pyrx): gbx_h3_cell_bbox scalar + rst_h3_gridspec helper"
```

---

### Task 3: Light `rst_h3_rasterize_agg` (grouped pandas_udf + tile wrapper)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- Test: `python/geobrix/test/pyrx/test_h3_rasterize_agg.py`

**Interfaces:**
- Consumes: `cellraster.cells_to_raster`, `cellraster.compute_gridspec` (Task 1); `_as_tile_udf` (functions.py:2870), `_col`, `_serde`.
- Produces: `_rst_h3_rasterize_agg_udf` (`@pandas_udf(BinaryType())`), `rst_h3_rasterize_agg(cellid, value=None, srid=None, pixel_size=None, xmin=None, ymin=None, xmax=None, ymax=None, width=None, height=None, mode="centroids", kring_pad=1) -> Column`, and `"gbx_rst_h3_rasterize_agg"` in `_sql_aggregators`.

- [ ] **Step 1: Write the failing test**

`python/geobrix/test/pyrx/test_h3_rasterize_agg.py`:
```python
import h3

from databricks.labs.gbx.pyrx import functions as rx
from databricks.labs.gbx.pyrx import _serde


def test_rst_h3_rasterize_agg_presence_mask(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid").alias("tile"))
        .collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        # presence mask -> covered pixels are 1.0, count matches >=1 per cell
        assert (arr == 1.0).sum() >= len(cells)
        assert ds.nodata == -9999.0


def test_rst_h3_rasterize_agg_burns_value(spark):
    res = 9
    c = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, res))
    df = spark.createDataFrame([(int(c), 42.0, "TX1")], ["cellid", "val", "tx"])
    out = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        assert (arr == 42.0).sum() >= 1
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_h3_rasterize_agg.py`
Expected: FAIL (no `rst_h3_rasterize_agg`).

- [ ] **Step 3: Implement in `functions.py`**

```python
@pandas_udf(BinaryType())
def _rst_h3_rasterize_agg_udf(
    cellid: pd.Series, value: pd.Series, srid: pd.Series, pixel_size: pd.Series,
    xmin: pd.Series, ymin: pd.Series, xmax: pd.Series, ymax: pd.Series,
    width: pd.Series, height: pd.Series, mode: pd.Series, kring_pad: pd.Series,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    _env.configure_gdal_env()
    cells = [int(c) for c in cellid if c is not None]
    if not cells:
        return None
    vals = [float(v) if v is not None else 1.0 for v in value] if value is not None else [1.0] * len(cells)
    cell_values = {}
    for c, v in zip(cells, vals):
        cell_values[c] = v  # last-wins (cells of one res don't overlap)

    res = cr._resolution([cr._h3_str(c) for c in cells])
    _srid = int(srid.iloc[0]) if srid is not None and srid.iloc[0] is not None else 4326
    _mode = mode.iloc[0] if mode is not None and mode.iloc[0] is not None else "centroids"
    _kp = int(kring_pad.iloc[0]) if kring_pad is not None and kring_pad.iloc[0] is not None else 1

    def _has(s):
        return s is not None and s.iloc[0] is not None

    if _has(xmin) and _has(width):
        grid = (float(xmin.iloc[0]), float(ymin.iloc[0]), float(xmax.iloc[0]),
                float(ymax.iloc[0]), (xmax.iloc[0] - xmin.iloc[0]) / int(width.iloc[0]),
                int(width.iloc[0]), int(height.iloc[0]), _srid)
    else:
        _ps = float(pixel_size.iloc[0]) if _has(pixel_size) else None
        grid = cr.compute_gridspec(cells, srid=_srid, pixel_size=_ps, mode=_mode, kring_pad=_kp)
    return cr.cells_to_raster(cell_values, *grid, resolution=res)


def rst_h3_rasterize_agg(cellid: ColLike, value: ColLike = None, srid: ColLike = None,
                         pixel_size: ColLike = None, xmin: ColLike = None, ymin: ColLike = None,
                         xmax: ColLike = None, ymax: ColLike = None, width: ColLike = None,
                         height: ColLike = None, mode: ColLike = None, kring_pad: ColLike = None) -> Column:
    """Rasterize a group's H3 cells into ONE tile (pixel-centroid burn).

    value omitted -> presence mask (1.0/NoData). Supply an explicit extent
    (xmin..height, e.g. from rst_h3_gridspec) for aligned band stacking; else the
    grid is auto-derived per mode/kring_pad.
    """
    def _c(x, default):
        return _col(x) if x is not None else f.lit(default)
    return _as_tile_udf(
        _rst_h3_rasterize_agg_udf(
            _col(cellid), _c(value, None), _c(srid, 4326), _c(pixel_size, None),
            _c(xmin, None), _c(ymin, None), _c(xmax, None), _c(ymax, None),
            _c(width, None), _c(height, None), _c(mode, "centroids"), _c(kring_pad, 1),
        )
    )
```
Add to `_sql_aggregators` (functions.py:3421): `"gbx_rst_h3_rasterize_agg": _rst_h3_rasterize_agg_udf,`.

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_h3_rasterize_agg.py`
Expected: 2 passed.

- [ ] **Step 5: Lint + commit**

```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && black src/databricks/labs/gbx/pyrx/functions.py test/pyrx/test_h3_rasterize_agg.py && isort <same>'
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_h3_rasterize_agg.py
git commit -m "feat(pyrx): rst_h3_rasterize_agg grouped aggregator"
```

---

### Task 4: Light registration — binding parity + function-info + doc SQL example

**Files:**
- Modify: `docs/tests-function-info/registered_functions.txt`, `docs/tests/python/api/rasterx_functions_sql.py`, `docs/tests/python/api/test_rasterx_functions_sql.py`
- Run: `gbx:docs:function-info`

**Interfaces:** none (registration/metadata).

- [ ] **Step 1: Add the function names to the canonical list**

Append to `docs/tests-function-info/registered_functions.txt` (alphabetical position): `gbx_rst_h3_rasterize_agg` and `gbx_h3_cell_bbox`. (Do NOT add `rst_h3_gridspec` — it is a Python/DataFrame helper, not a registered SQL UDF; the scalar `gbx_h3_cell_bbox` is the SQL surface.)

- [ ] **Step 2: Add doc-test SQL examples**

In `docs/tests/python/api/rasterx_functions_sql.py`, add `rst_h3_rasterize_agg_sql_example()` and `h3_cell_bbox_sql_example()` returning runnable SQL strings (use a small `VALUES` cell set; group-by; assert a non-null tile / non-null bbox). Follow the existing `*_sql_example()` shape in that file. Add matching tests in `test_rasterx_functions_sql.py`.

- [ ] **Step 3: Regenerate function-info + run the binding-parity check**

Run (subagent / Docker — may take minutes):
```
bash scripts/commands/gbx-docs-function-info.sh
bash scripts/commands/gbx-test-bindings.sh
```
Expected: function-info.json gains both functions; binding-parity PASSES (counts consistent: the SQL functions exist as Python `functions.py` entries + in `registered_functions.txt` + `function-info.json`; the Scala side is added in Task 7 — until then run binding-parity in light-only mode or expect the Scala-missing note for the two new names, which Task 7 resolves).

- [ ] **Step 4: Commit**

```bash
git add docs/tests-function-info/registered_functions.txt docs/tests/python/api/rasterx_functions_sql.py docs/tests/python/api/test_rasterx_functions_sql.py src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "docs(function-info): register gbx_rst_h3_rasterize_agg + gbx_h3_cell_bbox"
```

---

### Task 5: Validation tests — round-trip vs rastertogrid + partition (CI, light)

**Files:**
- Test: `python/geobrix/test/pyrx/test_h3_rasterize_validate.py`

**Interfaces:** Consumes `rx.rst_h3_rasterize_agg`, `rx.rst_h3_rastertogridavg` (existing), `cellraster`, sample DEM.

- [ ] **Step 1: Write the round-trip + partition tests**

```python
import os
import h3
import numpy as np

from databricks.labs.gbx.pyrx import functions as rx
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import cellraster as cr

DEM = os.path.join(
    os.environ.get("GBX_SAMPLE_DATA_ROOT",
                   os.path.join(os.path.dirname(__file__), "../../../../sample-data/Volumes/main/default/geobrix_samples/geobrix-examples")),
    "nyc/elevation/srtm_n40w073.tif",
)


def test_roundtrip_rastertogrid_then_rasterize(spark):
    # DEM -> (cellid, measure) via rastertogridavg -> rasterize back; covered
    # pixels' values match the per-cell measures (centroid inverse).
    res = 7
    with open(DEM, "rb") as fh:
        content = fh.read()
    df = spark.createDataFrame([(content,)], ["raster"]).selectExpr(
        "gbx_rst_fromcontent(raster, 'GTiff') AS tile"
    )
    rx.register(spark)
    # rastertogridavg returns array<array<struct<cellID,measure>>> (one per band)
    cells = df.selectExpr(
        "explode(gbx_rst_h3_rastertogridavg(tile, %d)[0]) AS c" % res
    ).selectExpr("c.cellID AS cellid", "c.measure AS measure")
    cellrows = cells.collect()
    assert len(cellrows) > 0
    cv = {int(r["cellid"]): float(r["measure"]) for r in cellrows}
    g = cr.compute_gridspec(list(cv.keys()), kring_pad=0)
    raster = cr.cells_to_raster(cv, *g, resolution=res)
    with _serde.open_tile(raster) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        # every burned value equals some cell measure (within float tolerance)
        measures = np.array(sorted(cv.values()))
        assert covered.size > 0
        assert np.isclose(covered, measures[np.searchsorted(measures, covered).clip(0, len(measures) - 1)], atol=1e-6).mean() > 0.99


def test_partition_property_via_agg(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.03), (0.03, 0.03), (0.03, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    tile = df.groupBy("tx").agg(rx.rst_h3_rasterize_agg("cellid").alias("t")).collect()[0]["t"]
    cellset = {cr._h3_str(c) for c in cells}
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1); t = ds.transform
        for row in range(ds.height):
            for col in range(ds.width):
                lon, lat = t * (col + 0.5, row + 0.5)
                assert (arr[row, col] != ds.nodata) == (h3.latlng_to_cell(lat, lon, res) in cellset)
```

- [ ] **Step 2: Run (Docker, sample data mounted)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_h3_rasterize_validate.py`
Expected: 2 passed. (Round-trip needs the registered SQL UDFs; gridagg assumes EPSG:4326 — the SRTM DEM is 4326, so no reprojection.)

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/pyrx/test_h3_rasterize_validate.py
git commit -m "test(pyrx): H3 rasterize round-trip + partition validation"
```

---

### Task 6: FCC realistic fixture + test

**Files:**
- Create: `python/geobrix/test/pyrx/data/fcc_uflw_miamidade_subset.csv`
- Test: `python/geobrix/test/pyrx/test_h3_rasterize_fcc.py`

**Interfaces:** Consumes `rx.rst_h3_rasterize_agg`, `rx.rst_h3_gridspec`, `rst_frombands_agg`.

- [ ] **Step 1: Curate the committed subset (one-time, document the command)**

From the (gitignored) source `input/broadband_wireless/bdc_12_UnlicensedFixedWireless_fixed_broadband_D25_09jun2026.csv`, take one provider, Miami-Dade (`block_geoid` prefix `12086`), a few speed tiers, capped to a few hundred rows:
```bash
docker exec geobrix-dev bash -lc 'cd /root/geobrix && python3 - <<PY
import csv
src="input/broadband_wireless/bdc_12_UnlicensedFixedWireless_fixed_broadband_D25_09jun2026.csv"
out="python/geobrix/test/pyrx/data/fcc_uflw_miamidade_subset.csv"
import os; os.makedirs(os.path.dirname(out), exist_ok=True)
rows=[]; prov=None
with open(src) as f:
    r=csv.DictReader(f)
    for row in r:
        if not row["block_geoid"].startswith("12086"): continue
        if prov is None: prov=row["provider_id"]
        if row["provider_id"]!=prov: continue
        rows.append({k:row[k] for k in ("provider_id","max_advertised_download_speed","h3_res8_id")})
        if len(rows)>=400: break
with open(out,"w",newline="") as f:
    w=csv.DictWriter(f, fieldnames=["provider_id","max_advertised_download_speed","h3_res8_id"]); w.writeheader(); w.writerows(rows)
print("wrote", len(rows), "rows")
PY'
```
(FCC BDC data is public/open — committing a small subset is fine.)

- [ ] **Step 2: Write the test (rasterize per speed tier, stack, assert coverage)**

```python
import csv, os
import h3
from databricks.labs.gbx.pyrx import functions as rx
from databricks.labs.gbx.pyrx import _serde

CSV = os.path.join(os.path.dirname(__file__), "data/fcc_uflw_miamidade_subset.csv")


def test_fcc_rasterize_per_speed_tier(spark):
    rows = list(csv.DictReader(open(CSV)))
    data = [(h3.str_to_int(r["h3_res8_id"]), int(r["max_advertised_download_speed"]),
             r["provider_id"]) for r in rows]
    df = spark.createDataFrame(data, ["cellid", "speed", "provider"])
    # one raster per (provider, speed tier); res-8 cells, presence mask
    tiles = (df.groupBy("provider", "speed")
             .agg(rx.rst_h3_rasterize_agg("cellid").alias("tile")).collect())
    assert len(tiles) >= 1
    for t in tiles:
        with _serde.open_tile(bytes(t["tile"]["raster"])) as ds:
            assert (ds.read(1) == 1.0).sum() >= 1   # cells burned
```

- [ ] **Step 3: Run**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_h3_rasterize_fcc.py`
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/pyrx/data/fcc_uflw_miamidade_subset.csv python/geobrix/test/pyrx/test_h3_rasterize_fcc.py
git commit -m "test(pyrx): FCC fixed-wireless H3 rasterize fixture + test"
```

---

### Task 7: Heavy `RST_H3_RasterizeAgg` UDAF + `gbx_h3_cell_bbox` + registration

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_H3_RasterizeAgg.scala`
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_CellBBox.scala` (scalar)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/RST_H3_RasterizeAggTest.scala`

**Interfaces:** Consumes `H3.cellIdToGeometry`, `H3.pointToCellID`, `H3.kRing` (gridx/grid/H3.scala), `VectorRasterBridge.buildEmptyRaster` / `toGTiffBytes`. Produces SQL `gbx_rst_h3_rasterize_agg` (tile struct) + `gbx_h3_cell_bbox` (bbox struct).

- [ ] **Step 1: Write the Scala test**

`RST_H3_RasterizeAggTest.scala` — register functions, build a DataFrame of res-9 cellids (use `H3.pointToCellID` for a few points), `groupBy(...).agg(rst_h3_rasterize_agg($"cellid"))`, assert the returned tile reads as a raster whose covered pixels (centroid → cell) all map into the input set. Mirror `RST_RasterizeAggTest` structure if present, else a fresh suite with a local SparkSession + JAR.

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.RST_H3_RasterizeAggTest'`
Expected: compile failure (class not defined).

- [ ] **Step 3: Implement the UDAF** (model on `RST_RasterizeAgg.scala:22-208`)

Accumulate `(cellId: Long, value: Double)` pairs (not WKB) with the same `MAX_BUFFER_BYTES` guard (8 + 8 bytes/row). In `eval`: derive resolution from the first cell (`H3.resolution(cellId)`; error on mixed); when an explicit extent is absent, compute the snapped grid from the cell centroids/envelope + kring_pad (port `cellraster.compute_gridspec`'s snap arithmetic); build the empty raster via `VectorRasterBridge.buildEmptyRaster`; **burn by pixel-centroid** — for each pixel center compute its geo coord (the `RST_H3_RasterToGrid` affine, file `grid/RST_H3_RasterToGrid.scala`), `H3.pointToCellID(lon, lat, res)`, look up the value, write to the band array; return the tile `InternalRow.fromSeq(Seq(0L, bytes, mapData))`. Signature/params mirror the light `rst_h3_rasterize_agg` (cellid, value, srid, pixel_size, xmin..height, mode, kring_pad).

Implement `RST_H3_CellBBox` scalar returning `struct<xmin,ymin,xmax,ymax>` from `H3.cellIdToGeometry(cellId).getEnvelopeInternal` (envelope mode) or the cell centroid (centroids mode), reprojected to `srid` via `OSRTransformGeometry`.

- [ ] **Step 4: Register** in `functions.scala` (after line 83 `rd.register(RST_RasterizeAgg)`):
```scala
rd.register(RST_H3_RasterizeAgg)
rd.register(RST_H3_CellBBox)
```
plus the imports and the `functions` object `def`s (mirror `rst_rasterize_agg`). Add the two names to `registered_functions.txt` Scala expectations if separate.

- [ ] **Step 5: Run to verify pass**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.RST_H3_RasterizeAggTest'`
Expected: PASS.

- [ ] **Step 6: Lint (scalastyle) + commit**

```bash
bash scripts/commands/gbx-lint-scalastyle.sh
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_H3_RasterizeAgg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_CellBBox.scala src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala src/test/scala/com/databricks/labs/gbx/rasterx/RST_H3_RasterizeAggTest.scala
git commit -m "feat(rasterx): RST_H3_RasterizeAgg UDAF + gbx_h3_cell_bbox (heavy tier)"
```

---

### Task 8: JAR-gated heavy↔light parity test

**Files:**
- Test: `python/geobrix/test/rasterx/test_h3_rasterize_parity.py`

**Interfaces:** Consumes heavy `rasterx.functions` (JAR) + light `pyrx` core for the expected cell set.

- [ ] **Step 1: Write the parity test**

Register the heavy tier; build a res-9 cell set; rasterize via heavy `gbx_rst_h3_rasterize_agg` on an explicit grid (from light `cellraster.compute_gridspec` so both use the identical canvas); assert the heavy raster's covered-pixel set == the light raster's covered-pixel set (centroid partition is deterministic, so the masks must match exactly). Gate on JAR presence per the `test/rasterx` convention (the suite imports `from databricks.labs.gbx.rasterx import functions as rx` and is skipped without the JAR).

- [ ] **Step 2: Run (Docker with JAR)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_h3_rasterize_parity.py`
Expected: PASS (covered-pixel masks identical between tiers).

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/rasterx/test_h3_rasterize_parity.py
git commit -m "test(rasterx): heavy<->light H3 rasterize parity (JAR-gated)"
```

---

### Task 9: Docs — raster-functions.mdx entries

**Files:**
- Modify: `docs/docs/api/raster-functions.mdx`

**Interfaces:** none.

- [ ] **Step 1: Add the Aggregator entry** `rst_h3_rasterize_agg` in the Aggregator Functions section (with `<Tier both/> <Impl groupedAgg/>`, the light-tier BINARY `:::warning` mirroring the other `rst_*_agg` entries, and the `*_sql_example` `<CodeFromTest>` from Task 4). Cross-reference `rst_h3_rastertogrid*` as the inverse and `rst_frombands_agg` for stacking.

- [ ] **Step 2: Add `gbx_h3_cell_bbox` + a short `rst_h3_gridspec` usage note** (the helper is Python/DataFrame, documented as the way to compute the shared grid for aligned stacking + per-cell `rst_merge_agg` merge). Keep the centroid-vs-spatial_envelope `mode` and `kring_pad` explained.

- [ ] **Step 3: Verify internals-leak gate + commit**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/raster-functions.mdx` (expect none).
```bash
git add docs/docs/api/raster-functions.mdx
git commit -m "docs(rasterx): rst_h3_rasterize_agg + gbx_h3_cell_bbox + rst_h3_gridspec"
```

---

### Task 10: DEM-isoband notebook example

**Files:**
- Create: `notebooks/examples/h3-rasterize/h3_rasterize_demo.ipynb`, `notebooks/examples/h3-rasterize/README.md`

**Interfaces:** none (example).

- [ ] **Step 1: Build the notebook** — full flow on the sample DEM `srtm_n40w073.tif`:
  1. Quantize the DEM into N filled elevation isobands (`rasterio.features.shapes` on a banded array) → multipolygons over a range of thresholds (elevation bands stand in for signal thresholds).
  2. `h3_polyfill` each band → `(band_level, cellid)`.
  3. `rx.rst_h3_gridspec` over the union of all bands → one shared grid.
  4. `rx.rst_h3_rasterize_agg` per band on the shared grid → aligned band tiles.
  5. `rst_frombands_agg` to stack → multi-band raster; render with `gbx.viz.plot_raster`; show the stack reconstructs the terrain.
  Include the telco mapping in prose (band level ↔ signal threshold; this is the same flow as the customer pipeline).

- [ ] **Step 2: README** — describe the demo + that it validates the rasterize→stack flow with no external data; note it must be run on a cluster/Serverless to refresh outputs.

- [ ] **Step 3: Commit** (source-level; outputs refreshed on a cluster run)

```bash
git add notebooks/examples/h3-rasterize/
git commit -m "docs(notebooks): H3 cell rasterize + stacking demo (DEM isobands)"
```

---

## Final verification (after all tasks)

- [ ] Full light suite incl. the new tests in a clean venv from the lock — all pass (`pip install --require-hashes -r requirements-pyrx-ci.txt && pip install --no-deps . && pytest test/pyrx ...`). (No new deps expected — rasterio/h3/numpy/pyproj are already in `[light]`; confirm `pyproj` is in the lock, it is via pyogrio.)
- [ ] `bash scripts/commands/gbx-test-bindings.sh` — binding-parity PASS across Scala + Python + function-info for `gbx_rst_h3_rasterize_agg` and `gbx_h3_cell_bbox`.
- [ ] `bash scripts/commands/gbx-lint-python.sh --check` and `gbx-lint-scalastyle.sh` — clean.
- [ ] Heavy Scala suite + JAR-gated parity green (Docker).
- [ ] Push to `beta/0.4.0`; confirm CI `build main` green on both tiers.
