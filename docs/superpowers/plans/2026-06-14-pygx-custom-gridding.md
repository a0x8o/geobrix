# pygx Phase 3 — Custom-Gridding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the 7 lightweight custom-gridding GridX functions (`gbx_custom_*`) in the existing pure-Python `databricks.labs.gbx.pygx` package at EXACT cell-ID / cell-set parity with the heavy Scala tier. Custom gridding is a faithful, bit-exact pure-Python port of `gridx/grid/CustomGridSystem.scala` + `GridConf.scala` + `custom/Custom_GridSpec.scala` (no PyPI library exists). On completion **GridX reaches full 1:1 light↔heavy parity** (quadbin + BNG + custom all in both tiers) and every doc surface flips the custom-grid family from heavyweight-only to both-tier. A one-line `import` swap (`pygx` vs `gridx.custom`) changes tiers — the SQL names are identical.

**Architecture:** Pure-Python/PySpark, Serverless/Connect-safe (only `spark.udf.register` + Column exprs — never `_jvm`/`spark.conf.set`/`sparkContext`/`.rdd`). All custom-grid cell math is ported from `CustomGridSystem.scala`/`GridConf.scala` into `pygx/_custom.py`; geometry via shapely → **WKB, no SRID** (heavy uses `JTS.toWKB`, line 159, the 2D no-SRID variant — the grid's `srid` field is metadata only and is NEVER stamped into output geometry). The grid spec is a `STRUCT` built by `gbx_custom_grid` (a validating `@udf`) and consumed by the other six functions as a struct column. Mirrors the just-completed `pygx` quadbin (Phase 1) and BNG (Phase 2).

**Tech Stack:** Python 3.12, `shapely` 2.x, `numpy`, PySpark `@udf`/`pandas_udf`. **No new dependencies** (`shapely`/`numpy` already in the `[light]` extra; custom gridding is integer/coordinate arithmetic plus shapely for the rectangle/point geometry and the `contains` test). No `@udtf`, no aggregators — the heavy custom family has none.

**Spec:** `docs/superpowers/specs/2026-06-14-pygx-custom-gridding-light-tier-design.md` (APPROVED 2026-06-14, the four Resolved decisions are baked into the tasks below). **Branch:** `pygx-light`. Out of scope: quadbin (Phase 1, complete), BNG (Phase 2, complete), the `h3` GridX subpackage (native H3 covers hex), any heavy behavior change EXCEPT the approved `pointToCellID` Y-NaN typo fix (Resolved decision 3).

---

## The 7 functions (parity targets, all `gbx_custom_*`)

From `docs/tests-function-info/registered_functions.txt` (lines 144–150), 7 names:

| # | Function (SQL) | Heavy signature | Return type | Light shape | Light source (port of `CustomGridSystem`) |
|---|---|---|---|---|---|
| 1 | `gbx_custom_grid` | `(xMin, xMax, yMin, yMax, cellSplits, rootCellSizeX, rootCellSizeY[, srid])` — 7 or 8 INT/LONG args | `STRUCT` (8 fields) | **validating `@udf`** (eager validation, returns `CUSTOM_GRID_SCHEMA`) | `Custom_Grid.eval` validation (`xMax>xMin`, `yMax>yMin`, `cell_splits>=2`, rootX/Y `>0`); no cell math |
| 2 | `gbx_custom_pointascell` | `(point BINARY\|STRING, grid STRUCT, resolution INT\|LONG)` | `BIGINT` | **`pandas_udf`** → `LongType` | `parse_geom`→coord→`point_to_cell_id` (bit-pack). NULL point/grid → NULL |
| 3 | `gbx_custom_cellaswkb` | `(cell BIGINT, grid STRUCT)` | `BINARY` (WKB polygon, no SRID) | **`pandas_udf`** → `BinaryType` | `cell_id_to_polygon`→shapely→`to_wkb()` (no SRID) |
| 4 | `gbx_custom_cellaswkt` | `(cell BIGINT, grid STRUCT)` | `STRING` (WKT polygon) | **`pandas_udf`** → `StringType` | same polygon → `.wkt` |
| 5 | `gbx_custom_centroid` | `(cell BIGINT, grid STRUCT)` | `BINARY` (WKB point, no SRID) | **`pandas_udf`** → `BinaryType` | `cell_id_to_center`→shapely Point→`to_wkb()` (no SRID) |
| 6 | `gbx_custom_polyfill` | `(geom BINARY\|STRING, grid STRUCT, resolution INT\|LONG)` | `ARRAY<BIGINT>` | **plain `@udf`** → `ArrayType(LongType())` | bbox over-scan (`first..last+1`) + centroid-containment filter (exact port of `CustomGridSystem.polyfill`). NULL geom → NULL |
| 7 | `gbx_custom_kring` | `(cell BIGINT, grid STRUCT, k INT\|LONG)` | `ARRAY<BIGINT>` | **plain `@udf`** → `ArrayType(LongType())` | decode cell→posX/posY→Chebyshev square clamped to `[0, totalCells]`→map back to cell IDs (exact port of `CustomGridSystem.kRing`) |

Heavy reference: `src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_*.scala` (the 7 expression classes) + the canonical algorithm `gridx/grid/CustomGridSystem.scala` (≈340 lines) + `gridx/grid/GridConf.scala` + `gridx/custom/Custom_GridSpec.scala`. The per-function expressions just decode the inputs and call into `CustomGridSystem.*`.

### Cell-ID encoding — must port BIT-EXACT (verbatim from `CustomGridSystem.scala` / `GridConf.scala`)

```
# GridConf (idBits = 56, resBits = 8):
subCellsCount   = cell_splits * cell_splits
bitsPerResolution = ceil(log10(subCellsCount) / log10(2))    # GridConf.scala:25
maxResolution   = min(20, floor(56 / bitsPerResolution))     # GridConf.scala:28
rootCellCountX  = ceil((bound_x_max - bound_x_min) / root_cell_size_x)   # ceil(span/size).toInt
rootCellCountY  = ceil((bound_y_max - bound_y_min) / root_cell_size_y)

# CustomGridSystem cell math:
totalCellsX(res) = rootCellCountX * pow(cell_splits, res).toLong   # (Y analogous)
cellWidth(res)   = root_cell_size_x / pow(cell_splits, res)        # FLOAT division (height analogous)
getCellId(cellPos, res)            = cellPos | (res.toLong << 56)              # CustomGridSystem:310-315
getCellPositionFromPositions(x,y,r) = posY * totalCellsX(r) + posX            # row-major  :317-321
getCellResolution(cellId) = (cellId >> 56).toInt                               # :180-182
getCellPosition(cellId)   = cellId & 0x00ffffffffffffffL                       # :184-186
getCellPositionX(idNum,r) = idNum % totalCellsX(r)                             # :188-190
getCellPositionY(idNum,r) = floor(idNum / totalCellsX(r)).toLong              # :192-194

# coordinate -> cell position (getCellPositionFromCoordinates, :268-272):
cellPosX = ((x - bound_x_min) / cellWidth(res)).toLong   # Scala Double->Long = TRUNCATE toward zero
cellPosY = ((y - bound_y_min) / cellHeight(res)).toLong

# cell -> polygon (cellIdToGeometry, :213-235):
x = cellX * cellWidth(res) + bound_x_min ;  y = cellY * cellHeight(res) + bound_y_min
ring = [(x,y),(x+w,y),(x+w,y+h),(x,y+h),(x,y)]   # closed 5-point ring

# cell center (getCellCenterX/Y, :296-308):
centerX = cellPosX * cellWidth + cellWidth/2 + bound_x_min   # (Y analogous)
```

`pointToCellID` (`:249-266`) enforces, in order: **no-NaN** (the buggy line — see Resolved decision 3), `resolution <= maxResolution`, `bound_x_min <= x < bound_x_max`, `bound_y_min <= y < bound_y_max` — raising `IllegalStateException` on violation. Light must match each as a `ValueError`.

`polyfill` (`:145-178`): bbox `getCellPositionFromCoordinates(minX,minY)` / `(maxX,maxY)` → iterate `firstCellPosX to lastCellPosX + 1` × `firstCellPosY to lastCellPosY + 1` (the **`+1` over-scan is INTENTIONAL** — keep it; Scala `a to b` is INCLUSIVE so the Python range is `range(first, last + 2)`) → map each `(x,y)` to its cell-center coordinate (`getCellCenterX/Y`) → keep centers with `geometry.contains(point(cx,cy))` → `pointToCellID(cx, cy, res)`.

`kRing` (`:38-60`): `res = getCellResolution(cellID)`; decode `posX`/`posY`; `fromX=max(posX-k,0)`, `toX=min(posX+k, totalCellsX(res))`, `fromY`/`toY` analogous; iterate `fromX to toX` × `fromY to toY` INCLUSIVE → `getCellPositionFromPositions` → `getCellId`. (Note the clamp uses `totalCellsX` itself, **not** `totalCellsX-1` — port that exactly even though it admits one out-of-range column; the parity test locks heavy's set.)

### Resolved decisions baked into tasks (spec, 2026-06-14)

1. **`gbx_custom_grid` = validating `@udf`** returning `CUSTOM_GRID_SCHEMA`, eager validation matching heavy `require(...)` (error-at-build-time parity). Task 6.
2. **Geom inputs accept `[E]WKB` + `[E]WKT` in BOTH tiers.** Light via `pygx/_geom.py` `parse_geom`. **Heavy already does** — `Custom_PointAsCell.decodeGeom` (`Custom_PointAsCell.scala:60-66`) is `case b: Array[Byte] => JTS.fromWKB(b); case UTF8String/String => JTS.fromWKT(...)`, and `fromWKB`/`fromWKT` auto-strip EWKB/EWKT SRID; `Custom_Polyfill` reuses `decodeGeom`. **No heavy geom-decoder extension is required.** Output geometry stays plain WKB no-SRID (`to_wkb()` / `JTS.toWKB`); `srid` is metadata only. Task 7 + Task 9.
3. **FIX the `pointToCellID` Y-NaN typo in BOTH tiers.** `CustomGridSystem.scala:250` is `require(!x.isNaN && !x.isNaN, ...)` — the second clause repeats `x`, so a NaN **Y** is unguarded. Heavy task fixes it to `!x.isNaN && !y.isNaN`; the light port guards both. Reference in the commit + beta release notes. The `polyfill` `+1` over-scan is intentional (NOT a fix). Task 8 (heavy) + Tasks 5/9 (light + parity lock-in).
4. **`maxResolution` computed identically** (`min(20, floor(56 / bitsPerResolution))`, cell_splits-dependent); light rejects `res > maxResolution`. Task 1.

## Conventions (every task)
- Spark-free core tests (`_custom.py`) run on host: `.venv-pyrx/bin/python -m pytest <path> -v`. Registered-fn + parity tests run in the `geobrix-dev` container: `bash scripts/commands/gbx-test-python.sh --path <path> --log <name>.log`.
- Commit (no push unless a task says so): `chmod -R u+rwX .git/objects`; subject ≤72 + WHY body; trailer exactly `Co-authored-by: Isaac`.
- Serverless guard: never add `_jvm`/`sparkContext`/`.rdd`/`spark.conf.set` to `pygx`.
- Impl rule (from Phases 1–2, apply identically): **scalar/bounded-output → `pandas_udf`** (pointascell + cellaswkb/cellaswkt/centroid); **variable-length array output → plain `@udf`** (polyfill/kring — OOM-safe row-by-row; a scalar `pandas_udf` would buffer a whole Arrow batch of arrays); **grid spec builder → validating `@udf` returning the STRUCT**. No `@udtf`, no grouped-agg.
- Int/Long tolerance: PySpark sends `Long` for integer literals; the grid-struct fields and `resolution`/`k` arrive as Python `int` in the UDF — coerce defensively (mirror `Custom_GridSpec.asInt`/`asLong`).
- The pygx test package lives in `test/pygx/` (not named after a PyPI lib), so the test-package-shadows-installed-lib gotcha does not apply.

## File Structure
| File | Responsibility | New? |
|---|---|---|
| `python/geobrix/src/databricks/labs/gbx/pygx/_custom.py` | pure-Python port of `CustomGridSystem`/`GridConf`: a `CustomGridConf` dataclass (8 fields + derived `bits_per_resolution`, `max_resolution`, `root_cell_count_x/y`, `id_bits=56`), `conf_from_row`, and grid-math fns `point_to_cell_id`, `cell_id_to_polygon`, `cell_id_to_centroid`, `polyfill`, `k_ring`, plus bit-pack/unpack helpers (`get_cell_id`, `get_cell_resolution`, `get_cell_position`, `get_cell_position_x/y`, `total_cells_x/y`, `cell_width/height`, `get_cell_center_x/y`, `get_cell_position_from_coordinates`, `get_cell_position_from_positions`). Spark-free; shapely for geometry | new |
| `pygx/_serde.py` | **add** `CUSTOM_GRID_SCHEMA` (8-field STRUCT matching `Custom_GridSpec.gridStructType` field names/types: `bound_x_min/x_max/y_min/y_max` LONG, `cell_splits`/`root_cell_size_x`/`root_cell_size_y`/`srid` INT) | extend |
| `pygx/_env.py` | **add** `assert_custom_available()` (shapely only) | extend |
| `pygx/functions.py` | **add** the 7 `gbx_custom_*` UDFs + Column wrappers + extend `register(spark)` to install all 7 | extend |
| `python/geobrix/test/pygx/test_custom_core.py` | Spark-free `_custom` unit tests (bit-pack/unpack round-trips, maxResolution/rootCellCount formulas, point_to_cell_id, cell→geometry/centroid, polyfill over-scan, k_ring clamp, validation raises incl. Y-NaN) | new |
| `python/geobrix/test/pygx/test_custom_functions.py` | registered-fn tests via the spark fixture (grid struct shape, NULL propagation, round-trip) | new |
| `python/geobrix/test/pygx/test_parity_custom.py` | JAR-gated cross-tier EXACT parity (cells + WKB geom within 1e-6 + all-4-encodings geom-input + Y-NaN lock-in) | new |

---

## Task 1: `_custom.py` — `CustomGridConf` (GridConf port) + bit-pack/unpack + maxResolution

**Files:** create `pygx/_custom.py`, `test/pygx/test_custom_core.py`.

Port `GridConf` (the derived quantities) and the cell-ID codec from `CustomGridSystem`. The dataclass holds the 8 fields; computed properties `bits_per_resolution`, `max_resolution`, `root_cell_count_x/y` mirror `GridConf.scala` EXACTLY. The codec functions are pure integer/float math.

- [ ] **Step 1: failing test** `test/pygx/test_custom_core.py`
```python
import math

import pytest

shapely = pytest.importorskip("shapely")  # _custom imports shapely at module load
from databricks.labs.gbx.pygx import _custom


def _conf(splits=2, rootx=1000, rooty=1000, srid=-1):
    # A 0..1,000,000 grid (mirrors the doc SQL example grid).
    return _custom.CustomGridConf(
        bound_x_min=0, bound_x_max=1_000_000, bound_y_min=0, bound_y_max=1_000_000,
        cell_splits=splits, root_cell_size_x=rootx, root_cell_size_y=rooty, srid=srid,
    )


def test_gridconf_derived_quantities_match_scala():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    # subCellsCount = 4 -> ceil(log10(4)/log10(2)) = ceil(2.0) = 2
    assert c.bits_per_resolution == 2
    # min(20, floor(56/2)) = 20  (the 20 cap binds here)
    assert c.max_resolution == 20
    # ceil(1_000_000 / 1000) = 1000
    assert c.root_cell_count_x == 1000
    assert c.root_cell_count_y == 1000


def test_max_resolution_is_cell_splits_dependent():
    # cell_splits=4 -> subCells=16 -> ceil(log10(16)/log10(2)) = ceil(4.0) = 4
    # -> min(20, floor(56/4)) = 14
    c = _conf(splits=4)
    assert c.bits_per_resolution == 4
    assert c.max_resolution == 14
    # cell_splits=8 -> subCells=64 -> bitsPerRes = ceil(log10(64)/log10(2)) = 6
    # -> min(20, floor(56/6)=9) = 9
    c8 = _conf(splits=8)
    assert c8.bits_per_resolution == 6
    assert c8.max_resolution == 9


def test_cell_id_pack_unpack_roundtrip():
    c = _conf()
    for res in (0, 1, 5, 10):
        for (px, py) in [(0, 0), (3, 7), (123, 456)]:
            pos = _custom.get_cell_position_from_positions(c, px, py, res)
            cid = _custom.get_cell_id(pos, res)
            assert _custom.get_cell_resolution(cid) == res
            decoded = _custom.get_cell_position(cid)
            assert _custom.get_cell_position_x(c, decoded, res) == px
            assert _custom.get_cell_position_y(c, decoded, res) == py


def test_total_cells_and_cell_width():
    c = _conf(splits=2, rootx=1000)
    assert _custom.total_cells_x(c, 0) == 1000          # rootCellCountX * 2^0
    assert _custom.total_cells_x(c, 1) == 2000          # * 2^1
    assert _custom.cell_width(c, 0) == 1000.0
    assert _custom.cell_width(c, 1) == 500.0            # 1000 / 2^1 (FLOAT division)


def test_conf_from_row_int_long_tolerant():
    # Simulate the struct arriving as a dict (PySpark Row.asDict) with Long bounds.
    row = {
        "bound_x_min": 0, "bound_x_max": 1_000_000,
        "bound_y_min": 0, "bound_y_max": 1_000_000,
        "cell_splits": 2, "root_cell_size_x": 1000, "root_cell_size_y": 1000,
        "srid": 27700,
    }
    c = _custom.conf_from_row(row)
    assert c.srid == 27700 and c.cell_splits == 2 and c.bound_x_max == 1_000_000
```

- [ ] **Step 2: run → FAIL** (`.venv-pyrx/bin/python -m pytest python/geobrix/test/pygx/test_custom_core.py -v` — no `_custom`).

- [ ] **Step 3: implement** `pygx/_custom.py`. Module docstring must state it is a pure-Python port of `gridx/grid/CustomGridSystem.scala` + `GridConf.scala`, that WKB has no SRID, and reference Resolved decision 3 (Y-NaN fix) on `point_to_cell_id`.
```python
"""Pure-Python custom-grid core for the pygx light tier.

A faithful, BIT-EXACT port of the heavy
``com.databricks.labs.gbx.gridx.grid.CustomGridSystem`` + ``GridConf`` Scala
objects (gridx/grid/CustomGridSystem.scala, GridConf.scala). No PyPI library
exists; this module reproduces the cell-ID bit-packing, coordinate<->cell
mapping, polyfill (centroid-containment), and k-ring (Chebyshev clamp) EXACTLY
so light and heavy share bit-identical cell ids and cell sets.

A custom grid is a user-defined regular rectangular grid: extent, root cell
size, and a recursive ``cell_splits`` factor (each resolution level subdivides
into ``cell_splits x cell_splits`` sub-cells). Cell ids are BIGINT (the top 8
bits hold the resolution, the low 56 hold the row-major cell position).

Geometry is emitted as plain WKB (NO SRID) / WKT, matching heavy ``JTS.toWKB``
(line 159, the 2D no-SRID variant). The grid ``srid`` is metadata only and is
NOT stamped into output geometry.

Resolved decision 3 (spec 2026-06-14): heavy ``pointToCellID`` had a
``require(!x.isNaN && !x.isNaN, ...)`` typo that left a NaN Y unguarded;
``point_to_cell_id`` here (and the heavy fix) guards BOTH x and y.
"""

import math
from dataclasses import dataclass
from typing import Any, List

ID_BITS = 56  # GridConf.idBits — low 56 bits hold the cell position
RES_BITS = 8  # GridConf.resBits — top 8 bits hold the resolution
_POSITION_MASK = 0x00FFFFFFFFFFFFFF


def _as_int(v: Any) -> int:
    if isinstance(v, bool):  # bool is an int subclass; reject explicitly
        raise ValueError(f"gbx_custom: expected INT/LONG, got bool {v!r}")
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    raise ValueError(f"gbx_custom: expected INT/LONG, got {v!r}")


@dataclass(frozen=True)
class CustomGridConf:
    bound_x_min: int
    bound_x_max: int
    bound_y_min: int
    bound_y_max: int
    cell_splits: int
    root_cell_size_x: int
    root_cell_size_y: int
    srid: int = -1  # -1 == no CRS

    @property
    def sub_cells_count(self) -> int:
        return self.cell_splits * self.cell_splits

    @property
    def bits_per_resolution(self) -> int:
        # GridConf.scala:25 — ceil(log10(subCellsCount) / log10(2))
        return math.ceil(math.log10(self.sub_cells_count) / math.log10(2))

    @property
    def max_resolution(self) -> int:
        # GridConf.scala:28 — min(20, floor(56 / bitsPerResolution))
        return min(20, math.floor(ID_BITS / self.bits_per_resolution))

    @property
    def root_cell_count_x(self) -> int:
        span = self.bound_x_max - self.bound_x_min
        return math.ceil(span / self.root_cell_size_x)

    @property
    def root_cell_count_y(self) -> int:
        span = self.bound_y_max - self.bound_y_min
        return math.ceil(span / self.root_cell_size_y)


def conf_from_row(row: Any) -> CustomGridConf:
    """Reconstruct a CustomGridConf from a grid-spec struct (Row/dict).

    Mirrors Custom_GridSpec.systemFromRow; Int/Long tolerant (PySpark sends Long
    for INT literals).
    """
    if row is None:
        raise ValueError("gbx_custom: grid spec must not be null")
    g = row.asDict() if hasattr(row, "asDict") else dict(row)
    return CustomGridConf(
        bound_x_min=_as_int(g["bound_x_min"]),
        bound_x_max=_as_int(g["bound_x_max"]),
        bound_y_min=_as_int(g["bound_y_min"]),
        bound_y_max=_as_int(g["bound_y_max"]),
        cell_splits=_as_int(g["cell_splits"]),
        root_cell_size_x=_as_int(g["root_cell_size_x"]),
        root_cell_size_y=_as_int(g["root_cell_size_y"]),
        srid=_as_int(g["srid"]),
    )


# --- cell-ID codec + grid math (CustomGridSystem) -----------------------------

def total_cells_x(conf: CustomGridConf, resolution: int) -> int:
    return conf.root_cell_count_x * int(math.pow(conf.cell_splits, resolution))


def total_cells_y(conf: CustomGridConf, resolution: int) -> int:
    return conf.root_cell_count_y * int(math.pow(conf.cell_splits, resolution))


def cell_width(conf: CustomGridConf, resolution: int) -> float:
    return conf.root_cell_size_x / math.pow(conf.cell_splits, resolution)


def cell_height(conf: CustomGridConf, resolution: int) -> float:
    return conf.root_cell_size_y / math.pow(conf.cell_splits, resolution)


def get_cell_id(cell_position: int, resolution: int) -> int:
    return cell_position | (resolution << ID_BITS)


def get_cell_resolution(cell_id: int) -> int:
    return cell_id >> ID_BITS


def get_cell_position(cell_id: int) -> int:
    return cell_id & _POSITION_MASK


def get_cell_position_x(conf: CustomGridConf, id_number: int, resolution: int) -> int:
    return id_number % total_cells_x(conf, resolution)


def get_cell_position_y(conf: CustomGridConf, id_number: int, resolution: int) -> int:
    return int(math.floor(id_number / total_cells_x(conf, resolution)))


def get_cell_position_from_positions(conf, cell_pos_x: int, cell_pos_y: int, resolution: int) -> int:
    return cell_pos_y * total_cells_x(conf, resolution) + cell_pos_x


def _trunc_long(v: float) -> int:
    # Scala Double->Long truncates toward zero (NOT math.floor).
    return int(v)


def get_cell_position_from_coordinates(conf, x: float, y: float, resolution: int):
    cell_pos_x = _trunc_long((x - conf.bound_x_min) / cell_width(conf, resolution))
    cell_pos_y = _trunc_long((y - conf.bound_y_min) / cell_height(conf, resolution))
    return cell_pos_x, cell_pos_y, get_cell_position_from_positions(conf, cell_pos_x, cell_pos_y, resolution)


def get_cell_center_x(conf, cell_position_x: int, resolution: int) -> float:
    w = cell_width(conf, resolution)
    return cell_position_x * w + (w / 2) + conf.bound_x_min


def get_cell_center_y(conf, cell_position_y: int, resolution: int) -> float:
    h = cell_height(conf, resolution)
    return cell_position_y * h + (h / 2) + conf.bound_y_min
```

- [ ] **Step 4: run → PASS** (core formula tests).

- [ ] **Step 5: commit** (`feat(pygx): custom-grid GridConf port + cell-ID bit-pack/unpack`).

---

## Task 2: `_custom.py` — `point_to_cell_id` (+ the Y-NaN guard, Resolved decision 3)

**Files:** modify `pygx/_custom.py`, `test/pygx/test_custom_core.py`.

Port `pointToCellID` (`CustomGridSystem.scala:249-266`) with all four `require` guards, in order. Fix the heavy Y-NaN typo: guard BOTH x and y (Resolved decision 3).

- [ ] **Step 1: failing tests** (append to `test_custom_core.py`)
```python
def test_point_to_cell_id_known_fixture():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    # res 0: 1000m root cells. Point (530000, 180000) -> posX=530, posY=180.
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    assert _custom.get_cell_resolution(cid) == 0
    pos = _custom.get_cell_position(cid)
    assert _custom.get_cell_position_x(c, pos, 0) == 530
    assert _custom.get_cell_position_y(c, pos, 0) == 180


def test_point_to_cell_id_rejects_nan_x_and_y():
    c = _conf()
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, float("nan"), 180000.0, 0)
    # Resolved decision 3: a NaN Y must ALSO raise (heavy typo left Y unguarded).
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, 530000.0, float("nan"), 0)


def test_point_to_cell_id_rejects_out_of_bounds_and_over_max_res():
    c = _conf(splits=2)  # max_resolution == 20
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, -1.0, 180000.0, 0)          # x < bound_x_min
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, 1_000_000.0, 180000.0, 0)   # x == bound_x_max (exclusive)
    with pytest.raises(ValueError):
        _custom.point_to_cell_id(c, 530000.0, 180000.0, 21)     # res > max_resolution
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** (append to `_custom.py`).
```python
def point_to_cell_id(conf: CustomGridConf, x: float, y: float, resolution: int) -> int:
    """Cell ID containing (x, y) at `resolution` (CustomGridSystem.pointToCellID).

    Resolved decision 3: guard BOTH x and y for NaN (the heavy Scala had a
    ``!x.isNaN && !x.isNaN`` typo that left Y unguarded; fixed in both tiers).
    """
    if math.isnan(x) or math.isnan(y):
        raise ValueError("gbx_custom: NaN coordinates are not supported.")
    if resolution > conf.max_resolution:
        raise ValueError(
            f"gbx_custom: resolution ({resolution}) exceeds maximum "
            f"resolution of {conf.max_resolution}."
        )
    if not (conf.bound_x_min <= x < conf.bound_x_max):
        raise ValueError(
            f"gbx_custom: X coordinate ({x}) out of bounds "
            f"{conf.bound_x_min}-{conf.bound_x_max}"
        )
    if not (conf.bound_y_min <= y < conf.bound_y_max):
        raise ValueError(
            f"gbx_custom: Y coordinate ({y}) out of bounds "
            f"{conf.bound_y_min}-{conf.bound_y_max}"
        )
    _, _, cell_pos = get_cell_position_from_coordinates(conf, x, y, resolution)
    return get_cell_id(cell_pos, resolution)
```

- [ ] **Step 4: run → PASS.**

- [ ] **Step 5: commit** (`feat(pygx): custom-grid point_to_cell_id (guards both x and y NaN)`).

---

## Task 3: `_custom.py` cell→geometry — `cell_id_to_polygon`, `cell_id_to_centroid`, aswkb/aswkt/centroid (no SRID)

**Files:** modify `pygx/_custom.py`, `test/pygx/test_custom_core.py`.

Heavy emits plain WKB (no SRID) via `JTS.toWKB` and plain WKT via `JTS.toWKT`. The cell polygon is the closed ring `(x,y),(x+w,y),(x+w,y+h),(x,y+h),(x,y)` from `cellIdToGeometry` (`:213-235`); the centroid is the polygon centroid (`cellIdToCenter`, `:332-338`). Use shapely `box(x, y, x+w, y+h)`; `to_wkb()` defaults to no SRID — do NOT `set_srid`.

- [ ] **Step 1: failing tests** (append)
```python
from shapely import from_wkb, from_wkt, get_srid  # noqa: E402


def test_cell_aswkb_is_polygon_no_srid():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)  # res 0 -> 1000m cell
    g = from_wkb(_custom.cell_aswkb(c, cid))
    assert g.geom_type == "Polygon"
    assert get_srid(g) == 0  # custom WKB carries NO SRID
    assert g.bounds == (530000.0, 180000.0, 531000.0, 181000.0)


def test_cell_aswkt_is_polygon_text():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    g = from_wkt(_custom.cell_aswkt(c, cid))
    assert g.geom_type == "Polygon"


def test_cell_centroid_is_point_no_srid():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    g = from_wkb(_custom.cell_centroid(c, cid))
    assert g.geom_type == "Point" and get_srid(g) == 0
    assert (g.x, g.y) == (530500.0, 180500.0)  # cell center
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** (append; import `box`, `to_wkb`, `Point` from shapely at top of file).
```python
from shapely import to_wkb as _to_wkb  # at top of file
from shapely.geometry import Point as _Point
from shapely.geometry import box as _box


def cell_id_to_polygon(conf: CustomGridConf, cell_id: int):
    """Closed custom-grid cell polygon (shapely), NO SRID (cellIdToGeometry)."""
    resolution = get_cell_resolution(cell_id)
    cell_number = get_cell_position(cell_id)
    cell_x = get_cell_position_x(conf, cell_number, resolution)
    cell_y = get_cell_position_y(conf, cell_number, resolution)
    w = cell_width(conf, resolution)
    h = cell_height(conf, resolution)
    x = cell_x * w + conf.bound_x_min
    y = cell_y * h + conf.bound_y_min
    return _box(x, y, x + w, y + h)


def cell_id_to_centroid(conf: CustomGridConf, cell_id: int):
    return cell_id_to_polygon(conf, cell_id).centroid


def cell_aswkb(conf: CustomGridConf, cell_id: int) -> bytes:
    return _to_wkb(cell_id_to_polygon(conf, cell_id))  # include_srid defaults False


def cell_aswkt(conf: CustomGridConf, cell_id: int) -> str:
    return cell_id_to_polygon(conf, cell_id).wkt


def cell_centroid(conf: CustomGridConf, cell_id: int) -> bytes:
    return _to_wkb(cell_id_to_centroid(conf, cell_id))
```

- [ ] **Step 4: run → PASS.**

- [ ] **Step 5: commit** (`feat(pygx): custom-grid cell geometry — aswkb/aswkt/centroid (no SRID)`).

---

## Task 4: `_custom.py` — `k_ring` (Chebyshev square, clamp to `totalCells`)

**Files:** modify `pygx/_custom.py`, `test/pygx/test_custom_core.py`.

Port `CustomGridSystem.kRing` (`:38-60`) EXACTLY: decode `res`/`posX`/`posY`; `fromX=max(posX-k,0)`, `toX=min(posX+k, totalCellsX(res))` (clamp uses `totalCellsX` itself, not `-1`); iterate `fromX..toX` × `fromY..toY` INCLUSIVE (Scala `a to b`); each `(x,y)` → `get_cell_position_from_positions` → `get_cell_id`.

- [ ] **Step 1: failing tests** (append)
```python
def test_kring_interior_k1_is_nine_cells():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)  # interior cell
    ring = _custom.k_ring(c, cid, 1)
    assert cid in ring
    assert len(set(ring)) == 9  # 3x3 block, far from edges


def test_kring_k0_is_self_only():
    c = _conf()
    cid = _custom.point_to_cell_id(c, 530000.0, 180000.0, 0)
    assert _custom.k_ring(c, cid, 0) == [cid]


def test_kring_clamps_at_origin():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    cid = _custom.point_to_cell_id(c, 100.0, 100.0, 0)  # posX=posY=0 (origin cell)
    ring = _custom.k_ring(c, cid, 1)
    # fromX/fromY clamp to 0; the block does not extend to negative positions.
    assert cid in ring
    assert all(_custom.get_cell_position(rc) >= 0 for rc in ring)
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** (append).
```python
def k_ring(conf: CustomGridConf, cell_id: int, k: int) -> List[int]:
    """Chebyshev (square) k-ring of cell IDs around cell_id (CustomGridSystem.kRing).

    Includes the center cell. Clamped to [0, totalCells] per the heavy port (the
    upper clamp uses totalCellsX/Y itself, INCLUSIVE iteration — ported verbatim).
    """
    if k < 0:
        raise ValueError("gbx_custom: k must be at least 0")
    res = get_cell_resolution(cell_id)
    cell_position = get_cell_position(cell_id)
    pos_x = get_cell_position_x(conf, cell_position, res)
    pos_y = get_cell_position_y(conf, cell_position, res)
    from_x = max(pos_x - k, 0)
    to_x = min(pos_x + k, total_cells_x(conf, res))
    from_y = max(pos_y - k, 0)
    to_y = min(pos_y + k, total_cells_y(conf, res))
    out = []
    for x in range(from_x, to_x + 1):          # Scala `a to b` is INCLUSIVE
        for y in range(from_y, to_y + 1):
            pos = get_cell_position_from_positions(conf, x, y, res)
            out.append(get_cell_id(pos, res))
    return out
```

- [ ] **Step 4: run → PASS.**

- [ ] **Step 5: commit** (`feat(pygx): custom-grid k_ring (Chebyshev square, clamp to totalCells)`).

---

## Task 5: `_custom.py` — `polyfill` (bbox `+1` over-scan + centroid containment)

**Files:** modify `pygx/_custom.py`, `test/pygx/test_custom_core.py`.

Port `CustomGridSystem.polyfill` (`:145-178`) EXACTLY, including the **intentional `+1` over-scan** (Resolved decision 3 confirms it is NOT a bug): bbox `getCellPositionFromCoordinates(minX,minY)`/`(maxX,maxY)` → iterate `firstCellPosX to lastCellPosX + 1` × `firstCellPosY to lastCellPosY + 1` (INCLUSIVE Scala → Python `range(first, last + 2)`) → map each `(x,y)` to its cell-center coordinate → keep centers `contains`-ed by the geometry → `point_to_cell_id(cx, cy, res)`. Empty/None geom → `[]`.

- [ ] **Step 1: failing tests** (append)
```python
from shapely import to_wkb as _towkb  # noqa: E402
from shapely.geometry import box as _box2  # noqa: E402


def test_polyfill_small_box_res0():
    c = _conf(splits=2, rootx=1000, rooty=1000)  # 1000m root cells
    # A 3000m x 3000m box aligned to the grid -> 9 cell centers fall inside.
    geom = _box2(530000.0, 180000.0, 533000.0, 183000.0)
    cells = _custom.polyfill(c, geom, 0)
    assert len(cells) == 9
    assert all(_custom.get_cell_resolution(cid) == 0 for cid in cells)


def test_polyfill_empty_geom_is_empty():
    c = _conf()
    assert _custom.polyfill(c, None, 0) == []
    from shapely.geometry import Polygon
    assert _custom.polyfill(c, Polygon(), 0) == []


def test_polyfill_centroid_containment_only():
    c = _conf(splits=2, rootx=1000, rooty=1000)
    # A box smaller than one cell, off-center, contains NO cell center -> empty.
    geom = _box2(530100.0, 180100.0, 530400.0, 180400.0)
    assert _custom.polyfill(c, geom, 0) == []
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** (append).
```python
def polyfill(conf: CustomGridConf, geometry, resolution: int) -> List[int]:
    """Cell IDs whose CENTER is contained by the geometry (CustomGridSystem.polyfill).

    Mirrors heavy EXACTLY incl. the intentional ``first..last + 1`` bbox over-scan
    (Resolved decision 3: over-scan is by design, not a bug — the centroid filter
    discards the extra cells).
    """
    if geometry is None or geometry.is_empty:
        return []
    min_x, min_y, max_x, max_y = geometry.bounds
    first_x, first_y, _ = get_cell_position_from_coordinates(conf, min_x, min_y, resolution)
    last_x, last_y, _ = get_cell_position_from_coordinates(conf, max_x, max_y, resolution)
    out = []
    # `first to last + 1` INCLUSIVE -> Python range(first, (last + 1) + 1)
    for x in range(first_x, last_x + 2):
        for y in range(first_y, last_y + 2):
            cx = get_cell_center_x(conf, x, resolution)
            cy = get_cell_center_y(conf, y, resolution)
            if geometry.contains(_Point(cx, cy)):
                out.append(point_to_cell_id(conf, cx, cy, resolution))
    return out
```

- [ ] **Step 4: run → PASS.**

- [ ] **Step 5: commit** (`feat(pygx): custom-grid polyfill (centroid containment + intentional +1 over-scan)`).

---

## Task 6: `_serde.py` `CUSTOM_GRID_SCHEMA` + the validating `gbx_custom_grid` `@udf`

**Files:** modify `pygx/_serde.py`, `pygx/_env.py`, `pygx/functions.py`, `test/pygx/test_custom_functions.py` (create — the grid-builder test only).

Add `CUSTOM_GRID_SCHEMA` to `_serde.py` matching `Custom_GridSpec.gridStructType` field names/types EXACTLY (`bound_x_min/x_max/y_min/y_max` LONG, `cell_splits`/`root_cell_size_x`/`root_cell_size_y`/`srid` INT). Add `assert_custom_available()` to `_env.py` (shapely only). In `functions.py` add `gbx_custom_grid` as a validating `@udf` (Resolved decision 1) returning the schema, eager-validating like `Custom_Grid.eval` (`xMax>xMin`, `yMax>yMin`, `cell_splits>=2`, rootX/Y `>0`), 7-arg form defaults `srid=-1`.

- [ ] **Step 1: failing test** `test/pygx/test_custom_functions.py`
```python
import pytest

shapely = pytest.importorskip("shapely")
from pyspark.sql.utils import PythonException  # noqa: E402

from databricks.labs.gbx.pygx import functions as gx  # noqa: E402


def test_custom_grid_struct_shape(spark):
    gx.register(spark)
    row = spark.sql(
        "SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS g"
    ).collect()[0]
    g = row["g"].asDict()
    assert g["bound_x_min"] == 0 and g["bound_x_max"] == 1000000
    assert g["cell_splits"] == 2 and g["root_cell_size_x"] == 1000
    assert g["srid"] == 27700


def test_custom_grid_7arg_defaults_srid_minus1(spark):
    gx.register(spark)
    row = spark.sql(
        "SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000) AS g"
    ).collect()[0]
    assert row["g"].asDict()["srid"] == -1


def test_custom_grid_validation_raises(spark):
    gx.register(spark)
    with pytest.raises(Exception):  # PythonException wrapping ValueError
        spark.sql(
            "SELECT gbx_custom_grid(1000000, 0, 0, 1000000, 2, 1000, 1000)"
        ).collect()  # xMax <= xMin
    with pytest.raises(Exception):
        spark.sql(
            "SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 1, 1000, 1000)"
        ).collect()  # cell_splits < 2
```

- [ ] **Step 2: run → FAIL** (`bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_custom_functions.py --log custom-fns.log` — no `gbx_custom_grid` registered).

- [ ] **Step 3: implement.**
  - `_serde.py` add:
```python
from pyspark.sql.types import IntegerType  # extend imports

# Grid-spec struct produced by gbx_custom_grid, consumed by all gbx_custom_* ops.
# Field names/types match heavy Custom_GridSpec.gridStructType exactly so the SAME
# struct flows into both light and heavy consumers. srid == -1 means no CRS.
CUSTOM_GRID_SCHEMA = StructType(
    [
        StructField("bound_x_min", LongType(), False),
        StructField("bound_x_max", LongType(), False),
        StructField("bound_y_min", LongType(), False),
        StructField("bound_y_max", LongType(), False),
        StructField("cell_splits", IntegerType(), False),
        StructField("root_cell_size_x", IntegerType(), False),
        StructField("root_cell_size_y", IntegerType(), False),
        StructField("srid", IntegerType(), False),
    ]
)
```
  - `_env.py` add:
```python
def assert_custom_available() -> None:
    """Raise a clear ImportError if shapely (the only pygx custom dep) is missing.

    Custom gridding is a pure-Python port of CustomGridSystem.scala; it needs only
    shapely (geometry + WKB/WKT I/O), no quadbin/BNG PyPI library.
    """
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        raise ImportError(
            "pygx custom gridding requires the [light] extra (shapely). "
            "Install with: pip install 'geobrix[light]'"
        )
```
  - `functions.py` add (imports: `from . import _custom`; `from ._serde import CUSTOM_GRID_SCHEMA`; `from pyspark.sql.functions import udf`):
```python
# --- custom-grid spec builder -> validating @udf returning CUSTOM_GRID_SCHEMA ---
# Resolved decision 1 (spec 2026-06-14): eager validation matches heavy
# Custom_Grid.eval's require(...) (error-at-build-time parity). The 7-arg form
# defaults srid to -1 (no CRS), as in Custom_Grid.builder.
@udf(returnType=CUSTOM_GRID_SCHEMA)
def _custom_grid_udf(x_min, x_max, y_min, y_max, splits, root_x, root_y, srid):
    x_min, x_max = int(x_min), int(x_max)
    y_min, y_max = int(y_min), int(y_max)
    splits, root_x, root_y = int(splits), int(root_x), int(root_y)
    srid = -1 if srid is None else int(srid)
    if not x_max > x_min:
        raise ValueError(
            f"gbx_custom_grid: bound_x_max ({x_max}) must be greater than "
            f"bound_x_min ({x_min})"
        )
    if not y_max > y_min:
        raise ValueError(
            f"gbx_custom_grid: bound_y_max ({y_max}) must be greater than "
            f"bound_y_min ({y_min})"
        )
    if splits < 2:
        raise ValueError(f"gbx_custom_grid: cell_splits must be >= 2; got {splits}")
    if root_x <= 0:
        raise ValueError(f"gbx_custom_grid: root_cell_size_x must be > 0; got {root_x}")
    if root_y <= 0:
        raise ValueError(f"gbx_custom_grid: root_cell_size_y must be > 0; got {root_y}")
    return (x_min, x_max, y_min, y_max, splits, root_x, root_y, srid)
```
  Register both arities under one SQL name. Spark UDFs are fixed-arity, so register a single 8-arg UDF and let SQL pass an explicit srid OR register a separate 7-arg lambda; the simplest parity path mirrors `Custom_Grid.builder` by registering one name whose Python function takes a defaulted 8th arg. PySpark `spark.udf.register` of a `@udf` cannot default-fill a missing SQL arg, so register a tiny dispatcher:
```python
    # register BOTH the 7- and 8-arg call shapes under gbx_custom_grid (Spark UDFs
    # are fixed-arity; a 7-arg call must default srid=-1 like Custom_Grid.builder).
    spark.udf.register("gbx_custom_grid", _custom_grid_udf)  # 8-arg
    # 7-arg form: a thin wrapper UDF that injects srid=-1.
    spark.udf.register(
        "gbx_custom_grid", _custom_grid_udf
    )  # see note: handle 7-arg in the SQL example / Column wrapper
```
  Note for the implementer: confirm in Step 4 whether `spark.udf.register("gbx_custom_grid", _custom_grid_udf)` accepts a 7-arg SQL call. PySpark `register` of a fixed-8-arg UDF will REJECT a 7-arg call (`wrong number of arguments`). To match `Custom_Grid.builder`'s 7-or-8 contract, give `_custom_grid_udf` a Python default (`srid=-1`) AND register via the Python-function path so the wrapper accepts both; if PySpark still requires fixed arity, register a second SQL name-free dispatcher is NOT allowed (one canonical SQL name). The robust solution: make `_custom_grid_udf` a plain Python function decorated `@udf` whose signature is `(x_min, x_max, y_min, y_max, splits, root_x, root_y, srid=-1)` — PySpark binds by position and a 7-arg SQL call uses the default. Verify; if PySpark rejects, fall back to requiring 8 args in SQL and document the 7-arg form is light-tier-via-`custom_grid()` Column wrapper only (the wrapper supplies `-1`). The Column wrapper (below) ALWAYS supplies all 8.

- [ ] **Step 4: run → PASS** (grid-builder tests; resolve the 7-vs-8-arg arity per the note — the test asserts BOTH arities work via SQL).

- [ ] **Step 5: commit** (`feat(pygx): custom-grid CUSTOM_GRID_SCHEMA + validating gbx_custom_grid @udf`).

---

## Task 7: `functions.py` — register the 6 consuming functions + all 7 Column wrappers

**Files:** modify `pygx/functions.py`, append to `test/pygx/test_custom_functions.py`.

Add the six consuming UDFs (pointascell/cellaswkb/cellaswkt/centroid → `pandas_udf`; polyfill/kring → plain `@udf`), extend `register(spark)` to install all 7 `gbx_custom_*`, and add the 7 `custom_*` Column wrappers (mirror heavy `gridx.custom.functions` + the quadbin/BNG wrapper style via `f.call_function`). The consuming UDFs receive the grid spec as a struct column → arrives as a `Row`/dict → `_custom.conf_from_row`. Geom inputs use `parse_geom` (Resolved decision 2, all 4 encodings).

- [ ] **Step 1: failing tests** (append to `test_custom_functions.py`)
```python
from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402

_GRID = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)"


def test_pointascell_wkt_and_wkb(spark):
    gx.register(spark)
    # WKT input (Resolved decision 2: all 4 encodings accepted).
    r1 = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT(530000 180000)', {_GRID}, 0) AS c"
    ).collect()[0]
    assert r1["c"] is not None
    # WKB input must give the SAME cell id.
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000, 180000, 530001, 180001).centroid)),)], "g binary"
    )
    df.createOrReplaceTempView("pv")
    r2 = spark.sql(
        f"SELECT gbx_custom_pointascell(g, {_GRID}, 0) AS c FROM pv"
    ).collect()[0]
    assert r2["c"] == r1["c"]


def test_cellaswkb_no_srid_and_centroid(spark):
    gx.register(spark)
    cell = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT(530000 180000)', {_GRID}, 0) AS c"
    ).collect()[0]["c"]
    out = spark.sql(
        f"SELECT gbx_custom_cellaswkb({cell}L, {_GRID}) AS w"
    ).collect()[0]
    g = from_wkb(bytes(out["w"]))
    assert g.geom_type == "Polygon" and get_srid(g) == 0  # no SRID stamped
    cen = spark.sql(
        f"SELECT gbx_custom_centroid({cell}L, {_GRID}) AS w"
    ).collect()[0]
    assert from_wkb(bytes(cen["w"])).geom_type == "Point"
    wkt = spark.sql(
        f"SELECT gbx_custom_cellaswkt({cell}L, {_GRID}) AS w"
    ).collect()[0]
    assert wkt["w"].startswith("POLYGON")


def test_polyfill_and_kring_arrays(spark):
    gx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(box(530000.0, 180000.0, 533000.0, 183000.0))),)], "g binary"
    )
    df.createOrReplaceTempView("rv")
    pf = spark.sql(
        f"SELECT size(gbx_custom_polyfill(g, {_GRID}, 0)) AS n FROM rv"
    ).collect()[0]
    assert pf["n"] == 9
    cell = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT(530000 180000)', {_GRID}, 0) AS c"
    ).collect()[0]["c"]
    kr = spark.sql(
        f"SELECT gbx_custom_kring({cell}L, {_GRID}, 1) AS r"
    ).collect()[0]
    assert cell in kr["r"] and len(set(kr["r"])) == 9


def test_null_propagation(spark):
    gx.register(spark)
    df = spark.createDataFrame([(None,)], "g binary")
    df.createOrReplaceTempView("nv")
    r = spark.sql(
        f"SELECT gbx_custom_pointascell(g, {_GRID}, 0) AS c FROM nv"
    ).collect()[0]
    assert r["c"] is None
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** the six UDFs + wrappers.
```python
# --- consuming scalar UDFs -> pandas_udf (bounded scalar) -------------------
@pandas_udf(LongType())
def _custom_pointascell_udf(point: pd.Series, grid: pd.Series, res: pd.Series) -> pd.Series:
    out = []
    for g, spec, r in zip(point, grid, res):
        if g is None or spec is None or r is None:
            out.append(None)
            continue
        pg = parse_geom(g)
        if pg is None or pg.is_empty:
            out.append(None)
            continue
        conf = _custom.conf_from_row(spec)
        c = pg.representative_point() if pg.geom_type != "Point" else pg
        # heavy uses geom.getCoordinate (first coord); for a Point that's its xy.
        coord = pg.coords[0] if pg.geom_type == "Point" else list(pg.coords)[0] \
            if hasattr(pg, "coords") else (pg.centroid.x, pg.centroid.y)
        out.append(_custom.point_to_cell_id(conf, float(coord[0]), float(coord[1]), int(r)))
    return pd.Series(out)


@pandas_udf(BinaryType())
def _custom_cellaswkb_udf(cell: pd.Series, grid: pd.Series) -> pd.Series:
    return pd.Series([
        _custom.cell_aswkb(_custom.conf_from_row(s), int(c)) if c is not None and s is not None else None
        for c, s in zip(cell, grid)
    ])


@pandas_udf(StringType())
def _custom_cellaswkt_udf(cell: pd.Series, grid: pd.Series) -> pd.Series:
    return pd.Series([
        _custom.cell_aswkt(_custom.conf_from_row(s), int(c)) if c is not None and s is not None else None
        for c, s in zip(cell, grid)
    ])


@pandas_udf(BinaryType())
def _custom_centroid_udf(cell: pd.Series, grid: pd.Series) -> pd.Series:
    return pd.Series([
        _custom.cell_centroid(_custom.conf_from_row(s), int(c)) if c is not None and s is not None else None
        for c, s in zip(cell, grid)
    ])


# --- array-output -> plain @udf (row-by-row, scale-safe) --------------------
def _custom_polyfill(geom, grid, res):
    if geom is None or grid is None or res is None:
        return None
    return _custom.polyfill(_custom.conf_from_row(grid), parse_geom(geom), int(res))


def _custom_kring(cell, grid, k):
    if cell is None or grid is None or k is None:
        return None
    return _custom.k_ring(_custom.conf_from_row(grid), int(cell), int(k))
```
  Note on `pointascell` coordinate extraction: heavy uses `geom.getCoordinate` (the FIRST coordinate of the geometry), NOT the centroid (unlike BNG). Port that exactly — for a `POINT` it's the point's xy; for any other geom it's the first vertex. Simplify the Step-3 body to `coord = pg.coords[0]` for Point else the first coordinate via `shapely.get_coordinates(pg)[0]`; verify against heavy in Task 9.

  Extend `register(spark)` (after the BNG block):
```python
    _env.assert_custom_available()
    spark.udf.register("gbx_custom_grid", _custom_grid_udf)  # (+ 7-arg handling, Task 6)
    spark.udf.register("gbx_custom_pointascell", _custom_pointascell_udf)
    spark.udf.register("gbx_custom_cellaswkb", _custom_cellaswkb_udf)
    spark.udf.register("gbx_custom_cellaswkt", _custom_cellaswkt_udf)
    spark.udf.register("gbx_custom_centroid", _custom_centroid_udf)
    spark.udf.register("gbx_custom_polyfill", _custom_polyfill, ArrayType(LongType()))
    spark.udf.register("gbx_custom_kring", _custom_kring, ArrayType(LongType()))
```
  Column wrappers (mirror heavy `gridx.custom.functions`; `custom_grid` ALWAYS supplies all 8 args, defaulting `srid=-1`):
```python
def custom_grid(x_min, x_max, y_min, y_max, cell_splits, root_x, root_y, srid: ColLike = -1) -> Column:
    """Build a custom-grid spec STRUCT (validated eagerly). srid=-1 means no CRS."""
    return f.call_function(
        "gbx_custom_grid", _col(x_min), _col(x_max), _col(y_min), _col(y_max),
        _col(cell_splits), _col(root_x), _col(root_y), _col(srid),
    )


def custom_pointascell(point: ColLike, grid: ColLike, res: ColLike) -> Column:
    """Custom-grid cell ID (BIGINT) for a point geometry at `res`."""
    return f.call_function("gbx_custom_pointascell", _col(point), _col(grid), _col(res))


def custom_cellaswkb(cell: ColLike, grid: ColLike) -> Column:
    """Cell footprint polygon as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_custom_cellaswkb", _col(cell), _col(grid))


def custom_cellaswkt(cell: ColLike, grid: ColLike) -> Column:
    """Cell footprint polygon as WKT (STRING)."""
    return f.call_function("gbx_custom_cellaswkt", _col(cell), _col(grid))


def custom_centroid(cell: ColLike, grid: ColLike) -> Column:
    """Cell centroid point as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_custom_centroid", _col(cell), _col(grid))


def custom_polyfill(geom: ColLike, grid: ColLike, res: ColLike) -> Column:
    """ARRAY<BIGINT> of cells whose center is contained by the geometry."""
    return f.call_function("gbx_custom_polyfill", _col(geom), _col(grid), _col(res))


def custom_kring(cell: ColLike, grid: ColLike, k: ColLike) -> Column:
    """ARRAY<BIGINT> of cells within Chebyshev ring distance `k` (includes center)."""
    return f.call_function("gbx_custom_kring", _col(cell), _col(grid), _col(k))
```

- [ ] **Step 4: run → PASS** (all `test_custom_functions.py`). Confirm no `_jvm`/`conf`/`.rdd` added to pygx (grep) — the existing Serverless guard test covers `functions.py`.

- [ ] **Step 5: commit** (`feat(pygx): register 6 custom-grid consuming fns + 7 Column wrappers`).

---

## Task 8: HEAVY Scala — fix the `pointToCellID` Y-NaN typo + scalastyle + suite green

**Files:** modify `src/main/scala/com/databricks/labs/gbx/gridx/grid/CustomGridSystem.scala`; rebuild guidance.

Resolved decision 3: fix the `pointToCellID` Y-NaN typo. **No geom-decoder change is needed** — `Custom_PointAsCell.decodeGeom` already accepts BINARY (`JTS.fromWKB`) and STRING (`JTS.fromWKT`), both of which strip EWKB/EWKT SRID, so all four encodings already work in heavy (confirmed by reading `Custom_PointAsCell.scala:60-66`; `Custom_Polyfill` reuses it).

- [ ] **Step 1:** edit `CustomGridSystem.scala:250` — change
```scala
        require(!x.isNaN && !x.isNaN, throw new IllegalStateException("NaN coordinates are not supported."))
```
to
```scala
        require(!x.isNaN && !y.isNaN, throw new IllegalStateException("NaN coordinates are not supported."))
```
  Add a one-line comment referencing the fix (the second clause was a duplicate-`x` typo that left a NaN Y unguarded).
- [ ] **Step 2:** dispatch a Task subagent (Docker, long-running) to run scalastyle + the custom Scala suite:
  - `bash scripts/commands/gbx-lint-scalastyle.sh --log scalastyle-custom.log`
  - `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.gridx.custom.*' --log scala-custom.log` (and any `CustomGridSystem`/`GridConf` suite under `gridx.grid.*`). If a heavy test asserts the OLD (Y-unguarded) behavior, update it to expect the NaN-Y rejection (the fix is approved). Surface a one-line progress update ~every 30s while it runs.
- [ ] **Step 3:** rebuild + stage the JAR so Task 9 parity can run heavy against the FIXED behavior: `bash scripts/commands/gbx-data-push-jar.sh` (stages both fat jar + tests.jar); confirm `python/geobrix/lib/geobrix-0.4.0-jar-with-dependencies.jar` exists.
- [ ] **Step 4:** add a beta-release-notes line (`docs/docs/beta-release-notes.mdx`) noting custom gridding is now available in both tiers AND that the `gbx_custom_pointascell`/`polyfill` NaN-Y guard was corrected (was silently unguarded). Keep user-facing voice (no wave/internal vocabulary).
- [ ] **Step 5: commit** (`fix(gridx): guard NaN Y in CustomGridSystem.pointToCellID (was duplicate-x typo)`).

---

## Task 9: cross-tier EXACT parity (JAR-gated) — cells + WKB geom + all-4-encodings + Y-NaN lock-in

**Files:** create `test/pygx/test_parity_custom.py` (mirror `test_parity_bng.py`).

- [ ] **Step 1:** confirm the JAR is staged (Task 8 Step 3). The test auto-skips without it.
- [ ] **Step 2: write the JAR-gated parity test** — copy the gating block + `spark_with_jar` fixture from `test/pygx/test_parity_bng.py` (the `_JARS` glob on `parents[2]/"lib"`, the active-session skip, `appName="gbx-pygx-custom-parity"`). Register light then heavy under the SAME `gbx_custom_*` SQL names: collect ALL light results first (`from databricks.labs.gbx.pygx import functions as gx; gx.register(spark)`), then heavy overwrites them (`from databricks.labs.gbx.gridx.custom import functions as hx; hx.register(spark)` — confirm the heavy registration entry point; if heavy registers via the package `functions.register(spark)` use that), then collect heavy. Build the SAME grid spec in both tiers (`gbx_custom_grid(0,1000000,0,1000000,2,1000,1000,27700)`). Assert:
  - **Exact cell-ID / set**: `pointascell` (same BIGINT), `polyfill` (sorted cell-set equality), `kring` (sorted set equality). Include edge cells (origin cell at `(100,100)`, a max-corner cell near `(999900,999900)`), a multi-resolution grid (`cell_splits` 2 and 4, res 0 and a deeper res), and a grid **with** (`srid=27700`) AND **without** (`srid=-1`) a CRS — the cell ids must be identical regardless of srid (srid is metadata only).
  - **Geometry WKB within 1e-6**: decode both tiers' WKB (`shapely.from_wkb`), assert `get_srid == 0` in BOTH (custom carries no SRID), and `equals_exact(lg.normalize(), hg.normalize(), 1e-6)` for `cellaswkb`, `centroid`. `cellaswkt` decoded via `from_wkt`, compared the same way.
  - **All-4-encodings geom input** (Resolved decision 2): for `pointascell` and `polyfill`, feed the SAME geometry as WKB, EWKB (SRID-stamped bytes via `shapely.to_wkb(g, include_srid=True)` on a `set_srid`'d geom), WKT, and EWKT (`"SRID=27700;POINT(...)"`); assert ALL four produce the identical cell id / cell set in BOTH tiers (and that light == heavy for each encoding).
  - **Y-NaN lock-in** (Resolved decision 3): build a geometry whose coordinate has a NaN Y is not directly expressible via WKT, so assert at the `_custom`/heavy boundary: in LIGHT, `_custom.point_to_cell_id(conf, 530000.0, float("nan"), 0)` raises `ValueError`; in HEAVY, a `gbx_custom_pointascell` over a point built with NaN Y raises (or, if NaN cannot round-trip through WKB, assert the light-tier guard directly and comment that the heavy fix is covered by the heavy Scala suite in Task 8). Document the chosen approach in the test docstring.
  - **Contingency**: if any cell set diverges, fix the `_custom.py` port until exact — EXACT parity is the bar (no tolerance on cell IDs). Likely divergence points: the `_trunc_long` (truncate-toward-zero vs floor), the `+1` polyfill over-scan range bounds, and the `pointascell` first-coordinate-vs-centroid choice.
- [ ] **Step 3: run in Docker** `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_parity_custom.py --with-integration --log parity-custom.log` → green (skips only without JAR).
- [ ] **Step 4: commit** (`test(pygx): light-vs-heavy custom-grid exact parity (cells + WKB + 4-encodings + NaN-Y)`).

---

## Task 10: function-info + binding parity (7 already in registered_functions.txt)

**Files:** verify `docs/tests/python/api/gridx_functions_sql.py` (the 7 `custom_*_sql_example()` exist — confirmed present, lines 434–479) + confirm all 7 `custom_*` Column wrappers exist in `functions.py`.

- [ ] **Step 1:** confirm the 7 `gbx_custom_*` are in `docs/tests-function-info/registered_functions.txt` (they are — lines 144–150) and each has a `*_sql_example()` in `gridx_functions_sql.py` (they do — `custom_grid`, `custom_pointascell`, `custom_cellaswkb`, `custom_cellaswkt`, `custom_centroid`, `custom_polyfill`, `custom_kring`). No new entries — this task verifies parity, not adds.
- [ ] **Step 2:** run `bash scripts/commands/gbx-test-bindings.sh --log bindings-custom.log` → PASS (every `gbx_custom_*` present in Scala `override def name` + Python `functions.py` binding + `function-info.json` key). Fix upstream if it fails (a missing Python wrapper is the likely gap — all 7 `custom_*` wrappers from Task 7 must be importable).
- [ ] **Step 3:** regen function-info if any example changed (`bash scripts/commands/gbx-docs-function-info.sh`); the examples are unchanged so this should be a no-op. Commit if anything changed (`docs(pygx): custom-grid binding/function-info parity`); otherwise note "no change — parity already holds" and skip.

---

## Task 11: bench harness — custom-grid legs + `--grid-custom-only`

**Files:** modify `python/geobrix/src/databricks/labs/gbx/bench/` (`corpus_vector.py` add custom generators; `readers.py` add `run_custom_*` + `_register_custom`; `cluster.py` add `_CELL_GRID_CUSTOM` + `benchmark_grid_custom`/`grid_custom_only` config); `notebooks/tests/push_and_run_bench_on_cluster.py` (`--benchmark-grid-custom` / `--grid-custom-only` flags).

- [ ] **Step 1:** add custom-grid corpus generators to `corpus_vector.py` mirroring the BNG block (`generate_custom_points` → points within the grid extent for `pointascell`; `generate_custom_polygons` → polygons within the extent for `polyfill`; `generate_custom_cells` → single BIGINT cell ids for `cellaswkb`/`cellaswkt`/`centroid`/`kring`, computed via pure-Python `_custom` so both tiers consume identical inputs). Use a fixed grid spec (`0,1000000,0,1000000,2,1000,1000,27700`) shared by both tiers.
- [ ] **Step 2:** add `_register_custom(spark, api)` (paralleling `_register_bng`) and representative legs to `readers.py`: `run_custom_pointascell` (scalar encode), `run_custom_polyfill` (geom→array), `run_custom_kring` (cell-in→array), `run_custom_cellaswkb` (cell→WKB, the UDF-boundary leg). Same `(spark, run_id, warmup, measured, *, api, n_rows, res, where)` shape, `category="grid"`, light-vs-heavy timing + **exact cell-set / decoded-geom parity** verdicts. Each leg builds the grid via `gbx_custom_grid(...)` so both tiers share the struct.
- [ ] **Step 3:** add `_CELL_GRID_CUSTOM` to `cluster.py` (mirror `_CELL_GRID_BNG`: light leg collected before heavy registration), wire `benchmark_grid_custom` / `grid_custom_only` config keys (parallel to `benchmark_grid_bng` / `grid_bng_only` at the `cfg.get(...)` sites + the `BENCHMARK_GRID_CUSTOM`/`GRID_CUSTOM_ONLY` template constants + the `cells.append(_cell(_CELL_GRID_CUSTOM))` branch), and the launcher `--benchmark-grid-custom` / `--grid-custom-only` flags in `push_and_run_bench_on_cluster.py` (mirror the `--benchmark-grid-bng` / `--grid-bng-only` wiring at lines ~280–283, 346–347, 504–507, 536, 612–613 + the `run_id` suffix `-grid-custom` + the skip-fn-benchmarks branch).
- [ ] **Step 4: local smoke** at tiny scale in the `geobrix-dev` container (resolve SQL, both tiers run, parity verdicts PASS). Do NOT run the cluster here.
- [ ] **Step 5: commit** (`feat(bench): custom-grid light-vs-heavy bench legs (--grid-custom-only)`).

---

## Task 12: cluster bench run

- [ ] **Step 1:** controller-orchestrated (not a subagent): build+stage JAR+wheel to the sample-data Volume (the JAR already carries the Task 8 fix); restart the standing 0519 bench cluster (restart, don't auto-terminate mid-iteration); poll libs INSTALLED; run `gbx:bench:cluster --grid-custom-only` ONCE (pass `--row-counts 1000`; verify exactly one geobrix-bench run on the cluster's cluster_id); verify rows; fetch `summary.md` (give the user the `bench-out/<run_id>/summary.md` link, unprompted).
- [ ] **Step 2:** record the light-vs-heavy medians + exact-parity verdicts (for the docs in Task 13). No commit (bench writes to the Volume/table). Note any custom function materially slower than heavy (the WKB-UDF-boundary tax applies to `cellaswkb`/`centroid`; pure cell-id ops `pointascell`/`polyfill`/`kring` should be competitive-to-faster — no JVM/JTS). Terminate the cluster only if started FRESH; if it was the standing 0519 cluster, suggest termination, don't auto-kill.

---

## Task 13: docs — flip custom grids heavy→both on every surface (+ supersede the spec note)

**Files:** `docs/docs/api/gridx-functions.mdx`, `execution-tiers.mdx`, `performance.mdx`, `benchmarking.mdx`, `README.md`, `docs/src/pages/index.js`, `docs/docs/intro.mdx`; the pygx light-tier spec out-of-scope note; `function-info`.

- [ ] **Step 1: `gridx-functions.mdx`** — flip the **Custom Grid Functions** section badges heavy→both: the section header (line 1027, `<Tier heavy/> Custom-grid functions ... are heavyweight-only`) becomes `<Tier both/>` with a lightweight note ("Powered by a pure-Python port of the custom-grid system + shapely; identical `gbx_custom_*` SQL names — a drop-in swap. Cell ids are BIGINT; geometry outputs are plain WKB, no SRID — the grid's `srid` is metadata only."), and EVERY per-function `### gbx_custom_*` badge → `<Tier both/>` (all 7: grid, pointascell, cellaswkb, cellaswkt, centroid, polyfill, kring). Update the top-of-page summary line (line 15) so GridX is **fully** lightweight (quadbin + BNG + custom all both-tier — remove the "while the custom-grid functions remain heavyweight-only" clause). Document that geom inputs accept WKB/EWKB/WKT/EWKT in both tiers (Resolved decision 2) and the `pointascell` first-coordinate semantics.
- [ ] **Step 2: `execution-tiers.mdx`** — remove custom grids from the heavyweight-only reasons (line 45: drop "You need the heavy-only GridX custom grids" — keep the `conforming` triangulation clause; line 47: drop "GridX's custom-grid APIs" from the remaining-heavyweight-only sentence). Update the GridX framing: "GridX is now FULLY lightweight — quadbin, BNG, AND custom grids run in both tiers." The remaining heavyweight-only surfaces become: the vector OGR readers, the `conforming` triangulation mode, and the heavy `pmtiles` DataSource writer.
- [ ] **Step 3: `performance.mdx`** — extend the "GridX (pygx)" subsection with the custom cell ops: the execution-shapes (scalar pandas-UDF `pointascell` + cell geometry `cellaswkb`/`cellaswkt`/`centroid`, the array `@udf` `polyfill`/`kring`, the validating `@udf` `gbx_custom_grid`), the `pygx/_custom.py` module row in the modules table (pure-Python custom-grid system port + shapely), and the perf narrative from the Task 12 numbers (STRING/BIGINT cell-id ops competitive; WKB-geometry ops carry the UDF-boundary tax).
- [ ] **Step 4: `benchmarking.mdx`** — extend the **Grid tab** with a custom-grid subsection: light (pygx) vs heavy (gridx.custom) with exact-output parity across the representative shapes (scalar encode `gbx_custom_pointascell`, geom→cell-array `gbx_custom_polyfill`, cell-in→array `gbx_custom_kring`, cell→WKB `gbx_custom_cellaswkb`), with the Task 12 medians + parity verdicts, and the `gbx:bench:cluster --grid-custom-only` invocation (per the bench-changes-update-docs rule, the numbers go here in the same stroke as Task 12).
- [ ] **Step 5: README / `index.js` / `intro.mdx`** — flip custom to lightweight-available:
  - `README.md`: GridX bullet → "GridX — BNG, Quadbin, AND custom grids all in both tiers (lightweight `pygx` + heavyweight Scala)."
  - `docs/src/pages/index.js`: GridX card + the heavyweight-only line — custom no longer heavy-only; GridX fully lightweight.
  - `docs/docs/intro.mdx`: lightweight tier now covers RasterX, VectorX, and the **full GridX** (BNG + quadbin + custom); GridX no longer appears in any heavyweight-only enumeration.
- [ ] **Step 6: supersede the pygx light-tier spec note** — in `docs/superpowers/specs/2026-06-14-pygx-light-tier-design.md`, update the "Out of scope" custom-gridding bullet to note it is superseded by `2026-06-14-pygx-custom-gridding-light-tier-design.md` (custom now in both tiers).
- [ ] **Step 7:** `function-info` regen if any example changed (`bash scripts/commands/gbx-docs-function-info.sh`); `cd docs && npm run build` → SUCCESS; `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` → empty (QC internals-leak gate).
- [ ] **Step 8: commit** (`docs(pygx): custom-grid lightweight tier across all surfaces`).

---

## Self-Review

**Spec coverage:** all 7 custom functions (Tasks 1–7, enumerated against `registered_functions.txt` lines 144–150); pure-Python port of `CustomGridSystem.scala`/`GridConf.scala`, no PyPI lib (Tasks 1–5); WKB no-SRID for custom (Tasks 3, 7, 9); cell-ID bit-packing `res << 56 | (posY * totalCellsX + posX)` ported verbatim (Task 1, locked in Task 9); `maxResolution = min(20, floor(56/bitsPerResolution))` cell_splits-dependent (Task 1, Resolved decision 4); `pointToCellID` four guards in order (Task 2); Y-NaN typo FIXED in BOTH tiers (Task 8 heavy + Task 2 light + Task 9 lock-in, Resolved decision 3); `polyfill` `+1` over-scan ported as INTENTIONAL (Task 5); `kring` Chebyshev clamp to `totalCells` (Task 4); `gbx_custom_grid` = validating `@udf` with eager `require(...)` parity (Task 6, Resolved decision 1); geom inputs all-4-encodings BOTH tiers, heavy already supports it (no decoder extension), light via `parse_geom` (Tasks 7, 9, Resolved decision 2); EXACT cell-set parity (Task 9); no `*_agg`/`*explode` (none added — heavy has none); Serverless-safe udf-only (Task 7 guard); no new deps (shapely only); bench legs + `--grid-custom-only` (Tasks 11–12); all doc surfaces incl. performance.mdx + the spec out-of-scope supersede (Task 13); function-info/binding parity (Task 10); TDD ordering: Spark-free core FIRST (Tasks 1–5), registered-fn (Tasks 6–7), JAR-gated parity (Task 9).

**Heavy findings (authored into the plan):** the `pointToCellID` Y-NaN typo is at `CustomGridSystem.scala:250` (`require(!x.isNaN && !x.isNaN, ...)` — second clause repeats `x`). Heavy ALREADY accepts all 4 geom encodings via `Custom_PointAsCell.decodeGeom` (`Custom_PointAsCell.scala:60-66`, `fromWKB`/`fromWKT` strip EWKB/EWKT SRID), reused by `Custom_Polyfill` — so NO geom-decoder extension is required; the only heavy change is the one-character Y-NaN fix (Task 8). `JTS.toWKB` (`JTS.scala:159`) is the 2D no-SRID variant.

**Placeholder scan:** the soft spots are intentional faithful-port markers with the Task-9 parity test as the exact definition of done: (a) the `gbx_custom_grid` 7-vs-8-arg arity has an explicit verify-in-Step-4 note with a concrete robust solution (Python default `srid=-1` bound by position) and a documented fallback; (b) the `pointascell` first-coordinate-vs-centroid choice carries an explicit note to port `geom.getCoordinate` (first vertex) and verify in Task 9; (c) `_trunc_long` truncate-toward-zero (vs `floor`) and the `polyfill` `range(first, last+2)` bounds are called out as the likely divergence points in the Task-9 contingency. All 7 registrations are listed verbatim in the `register` block; nothing is left unnamed.

**Name / type / formula consistency:** `_custom` fn names (`CustomGridConf`, `conf_from_row`, `point_to_cell_id`, `cell_id_to_polygon`/`cell_id_to_centroid`, `cell_aswkb`/`cell_aswkt`/`cell_centroid`, `polyfill`, `k_ring`, `total_cells_x/y`, `cell_width/height`, `get_cell_id`/`get_cell_resolution`/`get_cell_position`/`get_cell_position_x/y`, `get_cell_position_from_positions`/`from_coordinates`, `get_cell_center_x/y`), SQL names (`gbx_custom_*`, exactly the 7), schema (`CUSTOM_GRID_SCHEMA`, fields matching `Custom_GridSpec.gridStructType`), and the bit-packing formula (`cellPos | (res << 56)`, `posY * totalCellsX + posX`) + `maxResolution` formula are identical across Tasks 1–13.
