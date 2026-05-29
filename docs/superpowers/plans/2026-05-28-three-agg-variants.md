# Three `_agg` Streaming Variants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add three streaming aggregators — `gbx_quadbin_cellunion_agg`, `gbx_rst_rasterize_agg`, `gbx_rst_frombands_agg` — each a `TypedImperativeAggregate` that lets users `GROUP BY` and stream one element per row instead of `collect_list`-ing a whole array into one row.

**Architecture:** Each mirrors an existing aggregator template and delegates finalize to an existing pure-compute method. `quadbin_cellunion_agg` → `Quadbin_CellUnion.execute`; `rst_rasterize_agg` → inline `VectorRasterBridge` (mirrors `RST_Rasterize.execute`, multi-feature); `rst_frombands_agg` → `RST_FromBands.execute` (sorted by an explicit streamed `band_index`).

**Tech Stack:** Scala 2.13 / Spark 4.0 Catalyst `TypedImperativeAggregate`, JTS, GDAL. Tests + builds run in the `geobrix-dev` Docker container via `gbx:*`.

**Conventions reminder:**
- Run Scala/Python tests via `gbx:*` IN THE FOREGROUND, wait for `BUILD SUCCESS`/`BUILD FAILURE` + `Tests: succeeded N, failed M` before reporting. Never host `mvn`.
- After Scala changes, the JAR is stale; rebuild via `gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"` before Python/doc tests.
- `gh auth switch --user mjohns-databricks` before any push. Use ASCII only in source (scalastyle `nonascii` warns on em-dashes etc.).
- The `binding-parity` QC check requires: every name in `registered_functions.txt` has a Scala `override def name = "gbx_..."` literal, a Python `call_function("gbx_...")` wrapper, and a `function-info.json` entry.

**Design reference:** see `docs/superpowers/specs/` dtmfromgeoms design for the established `_agg` pattern. Templates to mirror: `.../gridx/bng/agg/BNG_CellUnionAgg.scala` (+ `UnionAcc.scala`), `.../rasterx/expressions/agg/RST_MergeAgg.scala`, and the just-built `.../rasterx/expressions/RST_DTMFromGeomsAgg.scala` (constant-expr handling + `ExpressionConfigExpr` child).

---

## File Structure

| File | Responsibility |
|---|---|
| `.../gridx/quadbin/agg/Quadbin_CellUnionAgg.scala` (+ `QuadbinUnionAcc.scala` or inline buffer) | Stream `BIGINT` cells; finalize `Quadbin_CellUnion.execute`. |
| `.../rasterx/expressions/agg/RST_RasterizeAgg.scala` | Stream `(geom_wkb, value)`; extent/srid as constant children; inline multi-feature rasterize. |
| `.../rasterx/expressions/agg/RST_FromBandsAgg.scala` | Stream `(tile, band_index)`; sort by band_index; finalize `RST_FromBands.execute`. |
| `.../rasterx/functions.scala`, `.../gridx/quadbin/functions.scala` | Register the three. |
| `docs/tests-function-info/registered_functions.txt` | Add 3 names. |
| `docs/tests/python/api/{rasterx,gridx}_functions_sql.py` | 3 `*_sql_example()`. |
| `src/main/resources/.../function-info.json` | Regenerated. |
| `python/geobrix/src/databricks/labs/gbx/{rasterx,gridx/quadbin}/functions.py` | 3 wrappers. |
| `src/test/scala/.../{quadbin,rasterx}/...AggTest.scala` | agg≡non-agg tests. |
| `python/geobrix/test/.../test_*_agg.py` | binding smoke tests. |

---

## Task 1: `gbx_quadbin_cellunion_agg`

**Files:** Create `src/main/scala/com/databricks/labs/gbx/gridx/quadbin/agg/Quadbin_CellUnionAgg.scala` (and a small buffer if needed); test `src/test/scala/com/databricks/labs/gbx/gridx/quadbin/Quadbin_CellUnionAggTest.scala`.

**Design (verified):** `Quadbin_CellUnion` (non-agg) takes `ARRAY<BIGINT>` and returns `BinaryType` (EWKB, SRID 4326) via the reusable `object Quadbin_CellUnion { def execute(cells: Array[Long]): Array[Byte] }`. The agg streams ONE `BIGINT` cell per row, buffers them, and calls `Quadbin_CellUnion.execute(buffer.toArray)` in `eval`. No per-group constants. Mirror `BNG_CellUnionAgg` structurally, but the buffer is just a `Long` accumulator (no chip struct / isCore). `UnaryLike[Expression]` (single child = the cell column). Return type `BinaryType`.

- [ ] **Step 1: Read templates.** Read `.../gridx/bng/agg/BNG_CellUnionAgg.scala` + `UnionAcc.scala` (structure: TypedImperativeAggregate overrides, serde), and `.../gridx/quadbin/Quadbin_CellUnion.scala` (confirm `execute(Array[Long]): Array[Byte]` and the non-agg's return/SRID). Also read an existing quadbin test (e.g. find `Quadbin_CellUnion`'s test or any `src/test/.../quadbin/*Test.scala`) to learn how valid cell IDs are constructed in tests.

- [ ] **Step 2: Write the failing test.** `Quadbin_CellUnionAggTest.scala` — an agg≡non-agg test: obtain a handful of valid quadbin cell IDs (construct them the same way the existing quadbin tests do — e.g. via the quadbin point→cell function or known-good literals), accumulate them into the agg's buffer, call `agg.eval(buf)`, and assert the resulting EWKB bytes equal `Quadbin_CellUnion.execute(sameCellsArray)`. Plus a buffer serialize/deserialize roundtrip test. Use `AnyFunSuite with Matchers`. Pattern the agg construction after the `RST_DTMFromGeomsAgg` test (build the case class with `Literal`/`null` child, call `.eval(buf)`).

- [ ] **Step 3: Run test, verify it fails** (FOREGROUND, wait): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.gridx.quadbin.Quadbin_CellUnionAggTest' --log qb-union-agg.log`. Expect compile-fail (`Quadbin_CellUnionAgg` missing).

- [ ] **Step 4: Implement `Quadbin_CellUnionAgg`.** A `TypedImperativeAggregate[<buffer>]` where the buffer accumulates `Long` cell ids (a small serializable acc with ByteBuffer serde `[count(4)][id*8...]`, OR reuse a simple `scala.collection.mutable.ArrayBuffer[Long]` wrapped in an acc class — match the serde rigor of `UnionAcc`). `update`: append `child.eval(input).asInstanceOf[Long]` (guard null). `merge`: concat. `eval`: `Quadbin_CellUnion.execute(buf.toArray)` (returns `Array[Byte]`; the agg's `dataType` is `BinaryType`, so return the bytes directly). `serialize`/`deserialize` via the acc. Companion: `name = "gbx_quadbin_cellunion_agg"`, `builder = c => Quadbin_CellUnionAgg(c.head)`. Place in new `agg/` subpackage mirroring `bng/agg/`.

- [ ] **Step 5: Run test, verify pass** (FOREGROUND, wait). Expect 2 tests pass.

- [ ] **Step 6: Commit** `git commit -m "feat(gridx): streaming gbx_quadbin_cellunion_agg (agg == non-agg)"`

---

## Task 2: `gbx_rst_rasterize_agg`

**Files:** Create `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_RasterizeAgg.scala`; test `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_RasterizeAggTest.scala`.

**Design (verified):** `RST_Rasterize` (non-agg) signature `(geom_wkb BINARY, value DOUBLE, xmin, ymin, xmax, ymax DOUBLE, width_px, height_px, srid INT) → tile`, with `object RST_Rasterize { def execute(geomWkb, value, xmin..srid, conf): InternalRow }` that internally calls `VectorRasterBridge.buildOgrLayer(Seq((geomWkb, value)), srid)` (a single-element Seq). The agg STREAMS `(geom_wkb, value)`; extent/size/srid are PER-GROUP CONSTANTS modeled as constant child expressions (the `RST_DTMFromGeomsAgg`/`GridFromPointsAgg` pattern — read them via `InternalRow.empty` in `eval`). There is NO existing multi-feature execute, so `eval` inlines the same steps as `RST_Rasterize.execute` but passes the full accumulated `Seq[(wkb,value)]` to `buildOgrLayer`. Include `ExpressionConfigExpr()` as a child and `ExpressionConfig` init in `eval` (mirror `RST_MergeAgg`). Burn overlap = last-wins in layer order (documented; nondeterministic across the group — acceptable). Return tile struct `RST_ExpressionUtil.tileDataType(BinaryType)`.

- [ ] **Step 1: Read** `RST_Rasterize.scala` (full — the `execute` body is the recipe), `VectorRasterBridge.scala` (`buildOgrLayer`, `buildEmptyRaster`, `toGTiffBytes`, and how RST_Rasterize.execute does the `gdal.RasterizeLayer` call), `RST_MergeAgg.scala` (TypedImperativeAggregate + `ExpressionConfigExpr` child + tile-row buffer serde), and `RST_DTMFromGeomsAgg.scala` (constant-expr `evalDouble`/`evalInt` readers, builder arg-count pattern). Read `RST_RasterizeTest.scala` for how to build geometries + read pixels back.

- [ ] **Step 2: Write the failing test.** `RST_RasterizeAggTest.scala`: GDAL `beforeAll` setup (copy from `RST_DTMFromGeomsTest`/`RST_RasterizeTest`). agg≡non-agg-ish test: stream 2-3 non-overlapping polygons (WKB) with distinct burn values into the agg buffer over a known extent; assert the output raster has the expected burn value at a pixel inside each polygon and `no_data` outside. Since RST_Rasterize is single-geom, the equivalence anchor is: rasterizing features A and B via the agg yields a raster where A's pixels = A's value and B's pixels = B's value (i.e. both burned). Also a buffer serde roundtrip test. Build the agg case class with `Literal` constants for extent/size/srid.

- [ ] **Step 3: Run, verify fail** (FOREGROUND, wait): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_RasterizeAggTest' --log rasterize-agg.log`.

- [ ] **Step 4: Implement `RST_RasterizeAgg`.** `TypedImperativeAggregate` with children `(geomWkbExpr, valueExpr, xminExpr, yminExpr, xmaxExpr, ymaxExpr, widthPxExpr, heightPxExpr, sridExpr, ExpressionConfigExpr())`. Buffer accumulates `(Array[Byte], Double)` features (acc class with ByteBuffer serde `[count][ (wkbLen, wkb, value) * N ]`). `update`: eval geomWkb (BINARY) + value (DOUBLE), append (skip nulls). `merge`: concat. `eval`: read constants via `InternalRow.empty` (Int/Long-tolerant readers), init ExpressionConfig, then `buildOgrLayer(buffer.features, srid)` → `buildEmptyRaster(xmin..srid, noData)` → `gdal.RasterizeLayer(...)` with `ATTRIBUTE=value` (replicate RST_Rasterize.execute's exact rasterize options) → `toGTiffBytes` → tile `InternalRow` (reuse the tile-row construction from RST_Rasterize.execute). Companion `name = "gbx_rst_rasterize_agg"`, builder accepting the 9 args (geom,value + 7 constants). Release GDAL datasets in `finally`.

- [ ] **Step 5: Run, verify pass** (FOREGROUND, wait).

- [ ] **Step 6: Commit** `git commit -m "feat(rasterx): streaming gbx_rst_rasterize_agg (burns many features per group)"`

---

## Task 3: `gbx_rst_frombands_agg`

**Files:** Create `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala`; test `.../agg/RST_FromBandsAggTest.scala`.

**Design (verified):** `RST_FromBands` (non-agg) takes `ARRAY<tile>` (band order = array position) and returns a single multiband tile via `object RST_FromBands { def execute(tiles: Seq[(Long, Dataset, Map[String,String])]): (Dataset, Map[String,String]) }` (uses `MergeBands.merge` → `gdalbuildvrt -separate`, band N = input N). **Band order matters and UDAF merge order is nondeterministic**, so the agg streams `(tile, band_index INT)` and SORTS by `band_index` ascending in `eval` before calling `execute`. Mirror `RST_MergeAgg`'s tile-buffer serde but extend each buffer element to a 2-field struct `(band_index: Int, tile: tileDataType)`. `BinaryLike[Expression]` (two children: tile + band_index) plus `ExpressionConfigExpr()`. Return tile struct (same rasterType as input).

- [ ] **Step 1: Read** `RST_FromBands.scala` (full — confirm `execute(Seq[(Long,Dataset,Map)])` and that band order = Seq order; note how it derives output cellID/metadata from `tiles.head`), `RST_MergeAgg.scala` (full — buffer `ArrayBuffer[Any]` of tile `InternalRow`s, `UnsafeProjection`-based serialize/deserialize, `RasterSerializationUtil.rowToTile`/`tileToRow`). Read `RST_MergeAggTest` (or RST_FromBands test) for tile test-data construction.

- [ ] **Step 2: Write the failing test.** `RST_FromBandsAggTest.scala`: construct 2-3 single-band tiles (reuse the band test-data construction from the RST_FromBands/RST_Merge tests). Stream them into the agg buffer WITH band_index values in SHUFFLED order (e.g. add band 3 first, then 1, then 2) to prove sorting works; call `agg.eval(buf)`; assert the output tile has the bands in band_index order — compare against `RST_FromBands.execute` on the tiles in correct (1,2,3) order. Assert output band count = number of inputs. Plus a buffer serde roundtrip test (with indices).

- [ ] **Step 3: Run, verify fail** (FOREGROUND, wait): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest' --log frombands-agg.log`.

- [ ] **Step 4: Implement `RST_FromBandsAgg`.** `TypedImperativeAggregate` with children `(tileExpr, bandIndexExpr, ExpressionConfigExpr())`. Buffer: `ArrayBuffer[Any]` where each element is an `InternalRow` of `(band_index: Int, tile: tileStruct)` (copy via `InternalRow.copyValue`). `update`: eval bandIndex (Int) + tile (struct), append `InternalRow(idx, tileCopy)`. `merge`: `++=`. `eval`: init ExpressionConfig; sort buffer by `row.getInt(0)`; extract each tile via `RasterSerializationUtil.rowToTile(row.getStruct(1, 3), rasterType)`; call `RST_FromBands.execute(sortedTiles)`; wrap result via `RasterSerializationUtil.tileToRow(...)`; release datasets. Serialize/deserialize: extend RST_MergeAgg's `UnsafeProjection` approach with element type `StructType(StructField("idx", IntegerType), StructField("tile", tileDataType))`. Companion `name = "gbx_rst_frombands_agg"`, builder accepting (tile, band_index).

- [ ] **Step 5: Run, verify pass** (FOREGROUND, wait).

- [ ] **Step 6: Commit** `git commit -m "feat(rasterx): streaming gbx_rst_frombands_agg (band_index-ordered band stacking)"`

---

## Task 4: Register all three + rebuild JAR

**Files:** `.../rasterx/functions.scala`, `.../gridx/quadbin/functions.scala`, (imports as needed).

- [ ] **Step 1:** In `quadbin/functions.scala`, add `rd.register(Quadbin_CellUnionAgg)` near the other quadbin registrations (add import for the new `agg` subpackage class). In `rasterx/functions.scala`, add `rd.register(RST_RasterizeAgg)` and `rd.register(RST_FromBandsAgg)` near the other aggregator registrations (the `expressions._` wildcard likely covers `expressions.agg`? — verify; if not, add imports for the `agg` subpackage).
- [ ] **Step 2: Rebuild JAR** (FOREGROUND, wait): `gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"`. Expect BUILD SUCCESS (confirms all three register + compile).
- [ ] **Step 3: Commit** `git commit -m "feat: register quadbin_cellunion_agg, rst_rasterize_agg, rst_frombands_agg"`

---

## Task 5: registered_functions.txt + SQL examples + function-info

- [ ] **Step 1:** Add `gbx_quadbin_cellunion_agg`, `gbx_rst_rasterize_agg`, `gbx_rst_frombands_agg` to `docs/tests-function-info/registered_functions.txt`.
- [ ] **Step 2:** Add a `*_sql_example()` + `_output` for each, matching the file conventions (quadbin one goes in the gridx/quadbin SQL examples file — find where `gbx_quadbin_*` examples live; rasterize/frombands go in `rasterx_functions_sql.py`). Mirror the `rst_gridfrompoints_agg_sql_example` / `rst_dtmfromgeoms_agg_sql_example` style (illustrative `GROUP BY` SQL; placeholder tables are fine — they are display + structural-validation only, not executed). For frombands include the `band_index` column in the example.
- [ ] **Step 3: Regenerate** (FOREGROUND, wait): `gbx:docs:function-info`. Confirm all three appear in `function-info.json`.
- [ ] **Step 4: Verify coverage** (FOREGROUND, wait): `gbx:test:function-info --log three-agg-fninfo.log` — the `test_full_coverage_against_registered_list` test must pass (the pre-existing `No module named databricks` errors are unrelated baseline noise — confirm the coverage test itself passes).
- [ ] **Step 5: Commit** `git commit -m "docs: function-info examples for the three new _agg functions"`

---

## Task 6: Python bindings + tests

**Files:** `python/.../rasterx/functions.py`, `python/.../gridx/quadbin/functions.py`, new `test_*_agg.py` files.

- [ ] **Step 1: Write failing Python tests** mirroring `test_dtmfromgeoms.py`'s session header. For each function a smoke test: build a small DataFrame, `groupBy`, call the wrapper, assert a non-null result. quadbin: stream cell BIGINTs (get cells via the quadbin point→cell binding or literal cell ids), assert union geometry returned. rasterize: stream `(wkb, value)` rows + constant extent, assert tile. frombands: stream `(tile, band_index)` rows, assert tile.
- [ ] **Step 2: Run, verify fail** (FOREGROUND, wait): `gbx:test:python --path <new test paths> --log three-agg-py.log`.
- [ ] **Step 3: Add wrappers.** `rst_rasterize_agg(geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, srid)` and `rst_frombands_agg(tile, band_index)` in `rasterx/functions.py`; `quadbin_cellunion_agg(cell)` in `gridx/quadbin/functions.py`. Each `return f.call_function("gbx_...", _col(...), ...)`. Match the existing wrapper style + docstrings.
- [ ] **Step 4: Run, verify pass** (FOREGROUND, wait).
- [ ] **Step 5: Commit** `git commit -m "feat(python): bindings + tests for the three new _agg functions"`

---

## Task 7: Full verification + push

- [ ] **Step 1: binding-parity** — `bash scripts/commands/gbx-test-bindings.sh --log three-agg-parity.log` → all three present in Scala/Python/function-info; parity green (count 144).
- [ ] **Step 2: Scala suites** (FOREGROUND/background, wait): `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*'` and `--suite 'com.databricks.labs.gbx.gridx.*'` → 0 failures.
- [ ] **Step 3: Python suites:** `gbx:test:python --path python/geobrix/test/rasterx/` and `--path python/geobrix/test/gridx/` → pass.
- [ ] **Step 4: scalastyle:** `gbx:lint:scalastyle` → 0 errors (ASCII-only; no `nonascii` warnings on new files).
- [ ] **Step 5: function-info coverage** → pass.
- [ ] **Step 6: Push** (`gh auth switch --user mjohns-databricks` first): `git push origin beta/0.4.0`. The QC `binding-parity` check gates the three new functions.

---

## Self-review notes (author)
- **Coverage:** all three functions get impl+test (T1-3), registration (T4), function-info+examples (T5), Python bindings+tests (T6), full verification incl. binding-parity (T7). The `band_index` ordering decision is implemented (T3) and tested via shuffled-order input. Rasterize last-wins overlap documented (T2).
- **Type consistency:** finalize methods are verified to exist — `Quadbin_CellUnion.execute(Array[Long]): Array[Byte]`, `RST_FromBands.execute(Seq[(Long,Dataset,Map)]): (Dataset,Map)`; `RST_Rasterize.execute` is single-feature so `rst_rasterize_agg` inlines the multi-feature path via `VectorRasterBridge` (no nonexistent method referenced).
- **Risk:** the implementer must read the named template files for exact serde/TypedImperativeAggregate boilerplate (test-data construction for quadbin cells, band tiles) — flagged in each task's Step 1.
