# XYZ Tile Data-Aware Rescale — Heavy/Classic Tier Implementation Plan (Phase 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. ALL Scala build/test/lint/bindings work runs **in the `geobrix-dev` Docker container** — dispatch the long-running Docker commands via a Task subagent (CLAUDE.md orchestrator pattern); never run them inline.

**Goal:** Mirror the light-tier `rescale` parameter (Phase 1, complete) in the heavy/classic Scala tier (`RST_TileXYZ` / `RST_XYZPyramid`) so non-8-bit imagery recovers contrast at PNG/JPEG/WEBP encode time, computed from **whole-dataset per-band min/max once per source**, fed to `gdal_translate -scale`. Then add the cross-tier pixel/value-distribution parity gate that proves both tiers feed the SAME per-band `(min,max)`.

**Architecture:** Add a trailing `rescale` expression to `RST_TileXYZ` (InvokedExpression) and `RST_XYZPyramid` (CollectionGenerator). A new helper resolves the user's `rescale` arg + the open source `Dataset` into a per-band `Seq[(Double,Double)]` (or `None` for pass-through), exactly mirroring light's `_resolve_in_range`:

- `"auto"` (default): uint8 source → `None` (pass-through); non-8-bit → per-band whole-dataset `(min,max)` via `BandAccessors.getMinMax` (`ComputeRasterMinMax(_, 0)` = exact).
- `"none"`: `None` (today's full-dtype-range crush).
- explicit `(min,max)` pair: that pair repeated for every band.

The resolved per-band ranges are formatted into GDAL `-scale_<b> min max 0 255` flags (per-band repeatable) and threaded through the existing `RST_TileXYZ.execute` → `GDALTranslate.executeTranslate` → `OperatorOptions.appendOptions` PNG/JPEG/WEBP branch. `RST_XYZPyramid` resolves the ranges **once** before the tile loop (mirroring light's resolve-once / no-seams contract) and passes them to every `RST_TileXYZ.execute`.

**Tech Stack:** Scala 2.13.16, Spark 4.0.0, Java 17, GDAL Java bindings (`org.gdal.gdal`), ScalaTest (`AnyFunSuite`). All tests run in the `geobrix-dev` Docker container via `gbx:*` commands.

## Global Constraints

> **LOCKED DECISIONS (user-approved 2026-06-29) — these override any hedges/"if unsure"/"DECISION NEEDED" notes elsewhere in this plan:**
> 1. **PNG-ONLY scope.** Apply `-scale` to the PNG byte-output branch ONLY. Do NOT add a JPEG/WEBP `-scale` branch in this work — leave JPEG/WEBP exactly as they are today (default branch, no `-ot Byte`/`-scale` change). Where a task shows an optional `case "JPEG" | "WEBP"` branch, OMIT it. Add a one-line note in the function docstring/scaladoc that `rescale` currently affects PNG output (the Helios/spec path); JPEG/WEBP rescale is a documented future follow-up.
> 2. **Accept the NoData divergence + document it.** `ComputeRasterMinMax` ignores NoData unless band NoData is set, whereas light's `ds.statistics(approx=False)` honors the mask. Accept this for now (fixtures are NoData-free). Add a scaladoc note on `resolveScale` that on NoData rasters heavy's min/max may include masked values and can diverge slightly from light — a known limitation, revisit if needed.
> 3. **Explicit pair = single `"min,max"` STRING** (e.g. `'8000,12000'`), as already specified. No numeric-arg arity change.
> 4. **Task 7 is REQUIRED, not optional.** The cross-language live parity check in the bench/doc-test container is part of the definition of done (the in-Scala Task 6 is the algebraic gate; Task 7 is the real cross-tier proof). Treat Task 7 as a normal required task.
> 5. **Reflection contract CONFIRMED** (controller verified): `RST_TileXYZ` is an `InvokedExpression` with four eval overloads `evalBinary/evalPath(row, z, x, y, format, size, resampling, conf)` (Int and Long variants) and builder `case 4..7`. Inserting `rescale` means adding a `UTF8String rescale` param to all four overloads IMMEDIATELY BEFORE `conf`, and extending the builder to `case 4..8` with `Literal("auto")` defaults. The SQL-level exercise in Tasks 2/4/6 is the catch-net for any positional mismatch.

- **Parity is pixel/value-distribution-level, NOT byte-level.** Heavy re-encodes a GTiff per tile and GDAL's PNG encoder differs from rio-tiler's; exact-byte cross-tier equality is NOT guaranteed (established "light-readers" convention). The cross-tier parity test asserts equivalent per-band value **distribution** within a tolerance for `"auto"`. The ONE byte-level assertion is uint8 pass-through being identical *within each tier* (auto == none for uint8, no `-scale` emitted).
- **GDALManager-guarded GDAL registration only.** All GDAL driver registration goes through the synchronized `GDALManager.init` (already reached via `RST_ExpressionUtil.init`) / `GDALManager.initOgr`. Stats computation (`ComputeRasterMinMax`) operates on an already-open `Dataset`/`Band` and does NOT register drivers or mutate process-global config — it is safe under concurrency. NEVER add a raw `gdal.AllRegister()` / `gdal.GetDriverByName` / `gdal.SetConfigOption` on the new code path.
- **Parameter named exactly `rescale`** — one canonical name, no aliases (beta = break to stabilize).
- **Cross-language naming consistency** (from CLAUDE.md):
  - Scala class: `RST_TileXYZ` / `RST_XYZPyramid` (unchanged).
  - SQL (registered): `gbx_rst_tilexyz` / `gbx_rst_xyzpyramid` (unchanged).
  - The new arg is `rescale` everywhere (Python `rescale` already shipped in Phase 1).
- **Heavy tests + lint + bindings run in Docker.** Commands:
  - `gbx:test:scala --suite '<FQCN>'` (single suite) / `--suites 'A,B'`
  - `gbx:lint:scalastyle` (matches CI — run before push)
  - `gbx:test:bindings` (binding-parity gate, also run by the QC judge on push)
  - `gbx:docs:function-info` (regenerate `function-info.json`)
- **Rescale semantics (copied verbatim from the spec):**
  - **`"auto"` (default):**
    - **uint8 source → pass through unchanged.** Already display-ready (RGB / NAIP byte imagery); never touched. Protects the cases that are correct today.
    - **non-8-bit source → rescale to Byte using whole-dataset per-band min/max,** computed **once per source** (not per tile). Recovers contrast AND guarantees every tile shares one mapping → **no tile-to-tile seams.**
  - **`"none"`:** today's raw full-dtype-range behavior. Explicit escape hatch for anyone depending on current output.
  - **`(min, max)` explicit pair:** use exactly these bounds, skip the stats read. (Per-band uniform; a single pair applied to all bands.)
- **`rescale` accepted values:** the string `"auto"` (default), the string `"none"`, or a 2-element `(min, max)` numeric pair. The Scala SQL surface accepts `rescale` as the string modes via a trailing string arg; the explicit numeric pair is expressed as a string literal `"min,max"` (e.g. `'8000,12000'`) parsed by the resolver, keeping the SQL signature a single extra `STRING` arg (consistent with `format`/`resampling` being strings). Anything else throws `IllegalArgumentException` (fail-fast, mirroring light's `ValueError`).
- **Stats read cost:** one `ComputeRasterMinMax` per band per source (not per tile); skipped entirely for uint8 and the explicit-pair path. `RST_XYZPyramid` resolves once for the whole pyramid.

## File Structure

```
src/main/scala/com/databricks/labs/gbx/rasterx/
  expressions/web/RST_TileXYZ.scala        (modify: +rescaleExpr child, +resolve helper, +scale threading, +builder arity)
  expressions/web/RST_XYZPyramid.scala     (modify: +rescaleExpr child, resolve-once, +builder arity)
  operator/OperatorOptions.scala           (modify: PNG/JPEG/WEBP branches accept an optional -scale string)
src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/
  WebMercatorTileTest.scala                (extend: heavy rescale unit assertions)
  XYZRescaleParityTest.scala               (NEW: cross-tier pixel/value-distribution parity gate)
docs/tests-function-info/registered_functions.txt   (no NEW function; arg-only change — confirm no row needed)
docs/tests/python/api/rasterx_functions_sql.py      (modify: add rescale to *_sql_example())
src/main/resources/com/databricks/labs/gbx/function-info.json  (regenerate via gbx:docs:function-info)
```

> **Note:** `RST_AsFormat` and `RST_FromContent` callers of `OperatorOptions.appendOptions` MUST be unaffected — the `-scale` injection is opt-in (only when a non-empty scale string is supplied in `writeOptions`), default unchanged. Task 1 enforces this with a regression assertion.

---

### Task 1: `OperatorOptions` PNG/JPEG/WEBP branches accept an optional `-scale` string (default unchanged)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operator/OperatorOptions.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/WebMercatorTileTest.scala` (extend) — exercised indirectly; a focused string-level assertion lives here to keep the suite single.

**Current state (real):** `OperatorOptions.appendOptions` (line 18) at line 62 emits for PNG:
```scala
case "PNG" => s"$command $ofFlag $format -ot Byte -a_nodata none" // PNG Byte format, strip NoData to avoid tRNS issues
```
There is NO JPEG/WEBP-specific branch today — both fall through to the `case f =>` default (line 64) `s"$command $ofFlag $f $cos"`. (Confirm during implementation: `RST_TileXYZ.execute` line 132-137 maps webp/jpeg extensions and passes `format -> "JPEG"/"WEBP"`; those currently hit the default branch with compression `-co` flags, which is the pre-existing behavior. We add `-scale` to the byte-output path without changing the format-flag stamping.)

**Design decision:** Thread the resolved per-band scale as a single pre-formatted string in `writeOptions` under key `"scale"` (e.g. `"-scale_1 8000 12000 0 255 -scale_2 ..."`). When present and non-empty, append it to the byte-output (PNG/JPEG/WEBP) branches. Keeping it a formatted string (not a structured type) means `OperatorOptions` stays a pure string assembler — its existing role — and the per-band formatting lives in `RST_TileXYZ` (Task 2) where the `Dataset` band count is known.

GDAL `-scale_<n> src_min src_max dst_min dst_max` is the per-band repeatable form (`-scale_1 ... -scale_2 ...`); it co-exists with `-ot Byte`. `-a_nodata none` is preserved (stripping NoData avoids PNG tRNS issues, unrelated to scaling).

- [ ] **Step 1: Write the failing test**

Add to `WebMercatorTileTest.scala` (it already sets up GDAL via `GDALManager` in `beforeAll`). Add a string-assembly assertion against `OperatorOptions.appendOptions`. We need a `Dataset` for the signature — reuse `srcDs`:

```scala
    test("OperatorOptions PNG branch injects -scale when scale option supplied") {
        val withScale = com.databricks.labs.gbx.rasterx.operator.OperatorOptions.appendOptions(
          "gdal_translate",
          Map("format" -> "PNG", "scale" -> "-scale_1 8000 12000 0 255"),
          srcDs
        )
        withScale should include("-ot Byte")
        withScale should include("-a_nodata none")
        withScale should include("-scale_1 8000 12000 0 255")
    }

    test("OperatorOptions PNG branch unchanged when no scale option") {
        val noScale = com.databricks.labs.gbx.rasterx.operator.OperatorOptions.appendOptions(
          "gdal_translate", Map("format" -> "PNG"), srcDs
        )
        noScale shouldBe "gdal_translate -of PNG -ot Byte -a_nodata none"
        noScale should not include "-scale"
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Dispatch a Task subagent to run (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest' --log heavy-rescale-op-options.log
```
Expected: the two new tests FAIL — `-scale` is not injected (first test fails on `include("-scale_1...")`).

- [ ] **Step 3: Implement — append the optional `-scale` string in the byte-output branches**

In `OperatorOptions.appendOptions`, after the `val cos = ...` line (line 55) add:
```scala
        // Optional per-band rescale string (e.g. "-scale_1 min max 0 255 -scale_2 ..."),
        // supplied by the XYZ tilers for data-aware 8-bit encoding. Empty/absent => no -scale
        // (today's full-dtype-range behavior). See RST_TileXYZ rescale resolution.
        val scaleFlags = writeOptions.getOrElse("scale", "").trim
        val scaleSuffix = if (scaleFlags.isEmpty) "" else s" $scaleFlags"
```
Then change the PNG branch (line 62) to:
```scala
            case "PNG"                                   => s"$command $ofFlag $format -ot Byte -a_nodata none$scaleSuffix" // PNG Byte format, strip NoData to avoid tRNS issues
```
JPEG/WEBP currently hit the `case f =>` default. To carry `-scale` onto those byte outputs too, add explicit branches BEFORE the `case f =>` (preserving `cos` compression flags so existing JPEG/WEBP output is otherwise unchanged):
```scala
            case "JPEG" | "WEBP"                         => s"$command $ofFlag $format -ot Byte$cos$scaleSuffix"
```

> **Implementation caution (verify in Step 4):** confirm the pre-change JPEG/WEBP output by reading the default-branch result for those formats BEFORE adding the explicit branch (assert the new branch reproduces the old `-of <fmt> <cos>` and only adds `-ot Byte` + optional scale). If JPEG/WEBP did NOT previously get `-ot Byte`, adding it is the intended behavior (they are byte web formats), but the test must document the deliberate change. If unsure, scope this task to PNG-only and handle JPEG/WEBP scale in a follow-up — PNG is the path the spec/Helios case exercises.

- [ ] **Step 4: Run the test to verify it passes**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest' --log heavy-rescale-op-options.log
```
Expected: the two new tests PASS; all pre-existing `WebMercatorTileTest` tests still PASS (PNG default unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/operator/OperatorOptions.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/WebMercatorTileTest.scala
git commit -m "feat(rasterx): OperatorOptions accepts optional -scale for byte tile output

The PNG (and JPEG/WEBP) byte-output branches now append a pre-formatted
per-band -scale string when writeOptions carries a non-empty 'scale' key;
default output (no scale) is byte-identical to before. Enables the XYZ
tilers' data-aware 8-bit rescale without disturbing other translate callers.

Co-authored-by: Isaac"
```

---

### Task 2: `RST_TileXYZ` resolves `rescale` to per-band scale + threads it into the translate step

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZ.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/WebMercatorTileTest.scala` (extend)

**Current state (real signatures):**
- `case class RST_TileXYZ(tileExpr, zExpr, xExpr, yExpr, formatExpr, sizeExpr, resamplingExpr)` (line 34-42), `children` includes `ExpressionConfigExpr()` (line 46), `withNewChildrenInternal` copies `nc(0)..nc(6)` (line 52).
- `evalBinary/evalPath` overloads (Int + Long) at lines 70-77 call `doInvoke(row, z, x, y, format, size, resampling, conf, dt)`.
- `doInvoke` (line 79) validates format/resampling/size, then `RasterSerializationUtil.rowToTile(row, dt)` → `(_, ds, options)` and calls `execute(ds, options, z, x, y, fmt, size, resampleLower)` (line 99).
- `execute(ds, options, z, x, y, format, size, resampling)` (line 111) warps to GTiff then `GDALTranslate.executeTranslate(translatePath, warpedDs, "gdal_translate", warpedOpts ++ Map("format" -> format, "extension" -> extension))` (line 139-144).
- Builder (line 206) is arity 4-7 with `Literal` defaults.
- `BandAccessors.getMinMax(band)` (in `operations/BandAccessors.scala` line 23) returns `(Double,Double)` via `band.ComputeRasterMinMax(minmax, 0)` (force=0 = exact) — REUSE THIS.

**Design decision (stats API):** Use the existing `BandAccessors.getMinMax(band)` (`ComputeRasterMinMax(_, 0)`, exact/force) — it is the canonical min/max accessor in this codebase, operates on an open `Band` (no driver registration, concurrency-safe), and matches light's whole-dataset `ds.statistics(b, approx=False)`. We deliberately do NOT use `AsMDArray().GetStatistics()` (that path is per CLAUDE.md MDArray-only and returns mean/stddev, not a clean exact min/max on a plain Band) nor `ComputeRasterMinMax(_, 1)` (approx — would risk cross-tier drift vs light's exact).

**Design decision (resolve helper):** Add `resolveScale(ds: Dataset, rescale: String): String` returning the pre-formatted `-scale_<b> ...` string (or `""` for pass-through). It mirrors light's `_resolve_in_range`:
- parse `rescale` (default/null → `"auto"`); `"none"` → `""`; `"min,max"` numeric → that pair for every band; `"auto"` → uint8 first band → `""`, else per-band `getMinMax`.
- Constant band (min == max) widened to `(min, min+1)` (matches light).

- [ ] **Step 1: Write the failing tests**

Add to `WebMercatorTileTest.scala`. Build a uint16 narrow-range fixture (mirroring light's `_make_uint16_narrow`), assert auto recovers contrast and none stays crushed, and uint8 passes through. We decode the PNG via GDAL (open the `/vsimem/` bytes and read the band) to assert the value spread.

```scala
    /** Decode PNG bytes via GDAL and return (min, max) of the first band's non-zero
     *  (i.e. data, ignoring transparent) pixels. */
    private def pngBandSpread(bytes: Array[Byte]): (Int, Int) = {
        val path = s"/vsimem/parity_decode_${java.util.UUID.randomUUID().toString.replace("-", "")}.png"
        gdal.FileFromMemBuffer(path, bytes)
        val ds = gdal.Open(path)
        try {
            val band = ds.GetRasterBand(1)
            val buf = Array.ofDim[Byte](ds.GetRasterXSize * ds.GetRasterYSize)
            band.ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, buf)
            val vals = buf.map(_ & 0xff).filter(_ > 0)
            if (vals.isEmpty) (0, 0) else (vals.min, vals.max)
        } finally {
            ds.delete(); gdal.Unlink(path)
        }
    }

    /** 16×16 uint16 raster, EPSG:4326, footprint (-1,-1)→(1,1), values ramped over [8000,12000]. */
    private def makeUint16Narrow(): Dataset = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/rescale_u16", 16, 16, 1, gdalconstConstants.GDT_UInt16)
        ds.SetGeoTransform(Array(-1.0, 0.125, 0.0, 1.0, 0.0, -0.125))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        val n = 256
        val ramp = (0 until n).map(i => (8000.0 + (12000.0 - 8000.0) * i / (n - 1))).toArray
        ds.GetRasterBand(1).WriteRaster(0, 0, 16, 16, ramp)
        ds.GetRasterBand(1).FlushCache()
        ds
    }

    test("RST_TileXYZ rescale=auto recovers contrast for uint16 narrow-range") {
        val ds = makeUint16Narrow()
        try {
            // z=2 tile (2,1) overlaps the (-1..1) footprint (see existing in-extent test).
            val auto = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "auto")
            val (lo, hi) = pngBandSpread(auto)
            // Auto maps [8000,12000] -> ~full 8-bit; expect a wide spread, NOT crushed [31,46].
            (hi - lo) should be > 100
        } finally ds.delete()
    }

    test("RST_TileXYZ rescale=none stays crushed for uint16 narrow-range") {
        val ds = makeUint16Narrow()
        try {
            val none = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "none")
            val (_, hi) = pngBandSpread(none)
            // 8000..12000 / 65535 * 255 -> ~[31,46]; crushed.
            hi should be < 80
        } finally ds.delete()
    }

    test("RST_TileXYZ uint8 source: auto == none (byte-identical pass-through)") {
        // srcDs is Float64 in this suite; build a uint8 source for the pass-through proof.
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/rescale_u8", 16, 16, 1, gdalconstConstants.GDT_Byte)
        ds.SetGeoTransform(Array(-1.0, 0.125, 0.0, 1.0, 0.0, -0.125))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 16, 16, Array.fill(256)(100.0))
        ds.GetRasterBand(1).FlushCache()
        try {
            val auto = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "auto")
            val none = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "none")
            java.util.Arrays.equals(auto, none) shouldBe true // no -scale emitted for uint8 auto
        } finally ds.delete()
    }
```

> The existing `RST_TileXYZ.execute(...)` 3 tests call the OLD 8-arg arity (no rescale). They will be updated to the new arity in Step 3 (add a trailing `"none"` to preserve their meaning, or rely on a defaulted overload — see Step 3).

- [ ] **Step 2: Run the tests to verify they fail**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest' --log heavy-rescale-tilexyz.log
```
Expected: COMPILE FAILURE — `execute` does not take a 9th `rescale` arg yet. (That is an acceptable red: it proves the new signature is required.)

- [ ] **Step 3: Implement — add `rescaleExpr`, `resolveScale`, and thread into `execute`**

**(a) Constructor + children + builder.** Add `rescaleExpr: Expression` as the 8th field. Update:

```scala
case class RST_TileXYZ(
    tileExpr: Expression,
    zExpr: Expression,
    xExpr: Expression,
    yExpr: Expression,
    formatExpr: Expression,
    sizeExpr: Expression,
    resamplingExpr: Expression,
    rescaleExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] =
        Seq(tileExpr, zExpr, xExpr, yExpr, formatExpr, sizeExpr, resamplingExpr, rescaleExpr, ExpressionConfigExpr())
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = RST_TileXYZ.name
    override def replacement: Expression = rstInvoke(RST_TileXYZ, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7))
}
```

> **Verify the InvokedExpression `rstInvoke` reflection contract:** `replacement = rstInvoke(RST_TileXYZ, rasterType)` reflectively dispatches to the `evalBinary/evalPath` overloads by matching the (non-config) children to method params. Adding `rescaleExpr` as a child means the `evalBinary/evalPath` overloads MUST gain a matching trailing `UTF8String rescale` param so reflection still binds. Read `InvokedExpression`/`rstInvoke` before implementing to confirm arg ordering (children minus the trailing `ExpressionConfigExpr` map positionally to the eval-method params, with `conf` last). This is the single highest-risk wiring step — see Risks.

Update the four eval overloads (lines 70-77) to take `rescale: UTF8String` BEFORE `conf` (matching child order: ..., resampling, rescale, conf):

```scala
    def evalBinary(row: InternalRow, z: Int, x: Int, y: Int, format: UTF8String, size: Int, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z, x, y, format, size, resampling, rescale, conf, BinaryType)
    def evalBinary(row: InternalRow, z: Long, x: Long, y: Long, format: UTF8String, size: Long, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z.toInt, x.toInt, y.toInt, format, size.toInt, resampling, rescale, conf, BinaryType)
    def evalPath(row: InternalRow, z: Int, x: Int, y: Int, format: UTF8String, size: Int, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z, x, y, format, size, resampling, rescale, conf, StringType)
    def evalPath(row: InternalRow, z: Long, x: Long, y: Long, format: UTF8String, size: Long, resampling: UTF8String, rescale: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z.toInt, x.toInt, y.toInt, format, size.toInt, resampling, rescale, conf, StringType)
```

Update `doInvoke` (line 79) to accept `rescale: UTF8String` and pass it to `execute`:

```scala
    private def doInvoke(
        row: InternalRow,
        z: Int, x: Int, y: Int,
        format: UTF8String, size: Int, resampling: UTF8String,
        rescale: UTF8String, conf: UTF8String, dt: DataType
    ): Array[Byte] = {
        val safe: () => Array[Byte] = () => {
            val exprConf = ExpressionConfig.fromB64(conf.toString)
            RST_ExpressionUtil.init(exprConf)
            val fmtStr = if (format == null) "PNG" else format.toString
            val resampleStr = if (resampling == null) "bilinear" else resampling.toString
            val rescaleStr = if (rescale == null) "auto" else rescale.toString
            // scalastyle:off caselocale
            val fmt = fmtStr.toUpperCase
            val resampleLower = resampleStr.toLowerCase
            // scalastyle:on caselocale
            require(AllowedFormats.contains(fmt), s"rst_tilexyz: format must be one of ${AllowedFormats.mkString(", ")}; got '$fmtStr'")
            require(AllowedResampling.contains(resampleLower),
                s"rst_tilexyz: unsupported resampling '$resampleStr'; allowed: ${AllowedResampling.toSeq.sorted.mkString(", ")}")
            require(size > 0 && size <= 4096, s"rst_tilexyz: size must be in (0, 4096]; got $size")
            val (_, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
            try execute(ds, options, z, x, y, fmt, size, resampleLower, rescaleStr)
            finally RasterDriver.releaseDataset(ds)
        }
        val result = Try(safe()).toOption.flatMap(Option(_))
        result.getOrElse(transparentPng(size))
    }
```

**(b) `resolveScale` helper + `execute` overloads.** Add the resolver (uses `BandAccessors`; add `import com.databricks.labs.gbx.rasterx.operations.BandAccessors` and `import org.gdal.gdalconst.gdalconstConstants`):

```scala
    /** Resolve the user `rescale` arg + open source `ds` into a pre-formatted GDAL
     *  `-scale_<b> min max 0 255` string for the byte-output translate step, or "" for
     *  pass-through (uint8 "auto", or "none"). Mirrors the light tier `_resolve_in_range`.
     *
     *  - "none"            -> "" (today's full-dtype-range behavior).
     *  - "min,max" pair    -> that pair repeated for every band.
     *  - "auto":
     *      * uint8 source   -> "" (already display-ready; pass through unchanged).
     *      * non-uint8      -> per-band whole-dataset (min,max) via BandAccessors.getMinMax
     *        (ComputeRasterMinMax exact). A constant band (min==max) widened to (min, min+1).
     */
    private[web] def resolveScale(ds: Dataset, rescale: String): String = {
        val mode = if (rescale == null) "auto" else rescale.trim
        // scalastyle:off caselocale
        val modeLower = mode.toLowerCase
        // scalastyle:on caselocale
        val nbands = ds.GetRasterCount

        def fmtBands(pairs: Seq[(Double, Double)]): String =
            pairs.zipWithIndex.map { case ((lo, hi), i) =>
                s"-scale_${i + 1} $lo $hi 0 255"
            }.mkString(" ")

        if (modeLower == "none") {
            ""
        } else if (modeLower == "auto") {
            val firstDt = ds.GetRasterBand(1).GetRasterDataType
            if (firstDt == gdalconstConstants.GDT_Byte) {
                "" // uint8 pass-through
            } else {
                val pairs = (1 to nbands).map { b =>
                    val (lo, hi) = BandAccessors.getMinMax(ds.GetRasterBand(b))
                    if (!(lo < hi)) (lo, lo + 1.0) else (lo, hi)
                }
                fmtBands(pairs)
            }
        } else {
            // explicit "min,max" pair (e.g. "8000,12000"), repeated per band.
            val parts = mode.split(",").map(_.trim)
            require(parts.length == 2, s"rst_tilexyz: rescale must be 'auto', 'none', or 'min,max'; got '$rescale'")
            val lo = parts(0).toDouble
            val hi = parts(1).toDouble
            require(lo < hi, s"rst_tilexyz: rescale (min,max) must have min < max; got ($lo, $hi)")
            fmtBands(Seq.fill(nbands)((lo, hi)))
        }
    }
```

Add a new `execute` overload that resolves the scale and a back-compat overload that defaults to `"none"` (so the 3 existing tests and any other caller compile unchanged — they keep today's behavior). Replace the existing `execute` (line 111) signature/body so the scale is threaded into the translate options:

```scala
    /** Back-compat: callers that do not specify rescale get today's behavior ("none"). */
    def execute(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String
    ): Array[Byte] = execute(ds, options, z, x, y, format, size, resampling, "none")

    def execute(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String, rescale: String
    ): Array[Byte] = {
        val scaleFlags = resolveScale(ds, rescale)
        executeWithScale(ds, options, z, x, y, format, size, resampling, scaleFlags)
    }

    /** Render with a PRE-RESOLVED scale string (RST_XYZPyramid resolves once and passes it
     *  here for every tile so all tiles share one mapping — no seams, stats read once). */
    private[web] def executeWithScale(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String, scaleFlags: String
    ): Array[Byte] = {
        val (xmin, ymin, xmax, ymax) = TileMath.tileBboxWebMerc(z, x, y)
        if (!datasetIntersectsWebMercBbox(ds, xmin, ymin, xmax, ymax)) {
            return transparentPng(size)
        }
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val warpPath = s"/vsimem/tilexyz_warp_$uuid.tif"
        val (warpedDs, warpedOpts) = GDALWarp.executeWarp(
          warpPath,
          Array(ds),
          options ++ Map("format" -> "GTiff"),
          command = s"gdalwarp -t_srs EPSG:3857 -te $xmin $ymin $xmax $ymax -ts $size $size -r $resampling"
        )
        try {
            val extension = format.toLowerCase(Locale.ROOT) match {
                case "png"  => "png"
                case "jpeg" => "jpg"
                case "webp" => "webp"
                case other  => throw new IllegalArgumentException(s"rst_tilexyz: unknown format $other")
            }
            val translatePath = s"/vsimem/tilexyz_out_$uuid.$extension"
            // Inject the pre-resolved per-band -scale (empty => no rescale; today's behavior).
            val translateOpts = warpedOpts ++ Map("format" -> format, "extension" -> extension) ++
                (if (scaleFlags.isEmpty) Map.empty[String, String] else Map("scale" -> scaleFlags))
            val (resDs, _) = GDALTranslate.executeTranslate(
              translatePath, warpedDs, command = "gdal_translate", translateOpts
            )
            Try(resDs.FlushCache())
            Try(resDs.delete())
            val bytes = gdal.GetMemFileBuffer(translatePath)
            gdal.Unlink(translatePath)
            if (bytes == null || bytes.isEmpty) transparentPng(size) else bytes
        } finally {
            RasterDriver.releaseDataset(warpedDs)
        }
    }
```

> **CRITICAL parity detail:** `resolveScale` reads stats from the ORIGINAL source `ds` (pre-warp), exactly as light reads whole-dataset stats from the open source — NOT from the per-tile warped raster. This guarantees every tile shares one mapping (no seams) AND that heavy and light derive the same `(min,max)` from the same source pixels. `executeWithScale` then applies the resolved scale to each warped tile.

**(c) Builder arity 4→8.** Replace the builder (line 206):

```scala
    /** Builder: 4 to 8 args (tile, z, x, y, [format, [size, [resampling, [rescale]]]]). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 4 => RST_TileXYZ(c(0), c(1), c(2), c(3), Literal("PNG"), Literal(256), Literal("bilinear"), Literal("auto"))
            case 5 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), Literal(256), Literal("bilinear"), Literal("auto"))
            case 6 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), Literal("bilinear"), Literal("auto"))
            case 7 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), c(6), Literal("auto"))
            case 8 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7))
            case n => throw new IllegalArgumentException(
                s"gbx_rst_tilexyz takes 4 to 8 arguments (tile, z, x, y, [format, [size, [resampling, [rescale]]]]); got $n"
            )
        }
    }
```

> **DEFAULT NOTE:** The SQL/builder default is `Literal("auto")` (matches light's default and the spec). The back-compat `execute(...)` 8-arg overload defaults to `"none"` ONLY to keep existing internal Scala callers/tests behaviorally unchanged at the call site — but the *public SQL surface* defaults to `"auto"`. These are intentionally different: the public contract is `auto`; the internal back-compat overload preserves legacy direct-`execute` callers. Confirm `RST_XYZPyramid` (Task 3) calls the new rescale-aware path with `"auto"` default, not the back-compat overload.

**(d) Update the 3 existing `execute` tests.** They call the 8-arg `execute` (no rescale) which now defaults to `"none"` — behavior preserved (PNG magic bytes, transparent fallback). No change needed if the back-compat overload exists. Confirm they still pass in Step 4.

- [ ] **Step 4: Run the tests to verify they pass**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest' --log heavy-rescale-tilexyz.log
```
Expected: all `WebMercatorTileTest` tests PASS (3 existing + Task-1 string tests + 3 new rescale tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZ.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/WebMercatorTileTest.scala
git commit -m "feat(rasterx): rescale param for RST_TileXYZ (data-aware 8-bit)

Adds a trailing rescale expression (default auto). resolveScale derives a
per-band -scale string from whole-dataset min/max (BandAccessors.getMinMax,
exact) for non-8-bit sources; uint8 and none pass through unchanged. Stats
read from the source ds (pre-warp) so the mapping is source-global.

Co-authored-by: Isaac"
```

---

### Task 3: `RST_XYZPyramid` threads `rescale`, resolves the scale ONCE for the whole pyramid

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_XYZPyramid.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/WebMercatorTileTest.scala` (extend)

**Current state (real):**
- `case class RST_XYZPyramid(tileExpr, minZExpr, maxZExpr, formatExpr, sizeExpr, resamplingExpr, exprConfExpr = ExpressionConfigExpr())` (line 33-40); `children` (line 53) is `Seq(tileExpr, minZExpr, maxZExpr, formatExpr, sizeExpr, resamplingExpr, exprConfExpr)`; `withNewChildrenInternal` copies `nc(0)..nc(6)` (line 56).
- `doEval` (line 61) reads format/size/resampling (lines 77-79), opens the source via `rowToTile` (line 81), computes the WGS84 bbox once (line 84), guards tile count, then loops calling `RST_TileXYZ.execute(ds, options, zz, xx, yy, format, size, resampling)` per tile (line 115).
- Builder (line 163) arity 3-6 with `Literal` defaults.

- [ ] **Step 1: Write the failing test**

Add to `WebMercatorTileTest.scala`. Since the generator's `doEval` needs an `InternalRow` tile + `ExpressionConfig` (Spark-ish), test the pyramid's resolve-once + contrast contract at the `execute`/`resolveScale` level instead (the generator delegates to these), plus a direct guard that the resolved scale is shared. A focused, Spark-free assertion:

```scala
    test("RST_XYZPyramid resolves ONE scale for the source and reuses it per tile") {
        val ds = makeUint16Narrow()
        try {
            // The pyramid resolves the scale once from the source, then renders each tile
            // with that same string. Simulate the loop: resolve once, render two tiles.
            val scale = RST_TileXYZ.resolveScale(ds, "auto")
            scale should not be empty
            scale should include("-scale_1")
            val t1 = RST_TileXYZ.executeWithScale(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", scale)
            val t2 = RST_TileXYZ.executeWithScale(ds, Map.empty[String, String], 3, 4, 2, "PNG", 64, "near", scale)
            // Both tiles produced with the SAME mapping (no seams). Spot-check one is contrast-recovered.
            val (lo, hi) = pngBandSpread(t1)
            (hi - lo) should be > 50
            t2 should not be null
        } finally ds.delete()
    }
```

> `resolveScale` and `executeWithScale` are `private[web]` (Task 2) so this same-package test can call them. This proves the resolve-once contract that the generator will use; the generator's own Spark-level exercise is covered by the bindings/SQL surface (Task 4) and the cross-tier parity test (Task 6).

- [ ] **Step 2: Run the test to verify it fails**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest' --log heavy-rescale-pyramid.log
```
Expected: PASS already IF Task 2 exposed `resolveScale`/`executeWithScale` — in that case this test is a guard. If `executeWithScale` is not yet reachable (`private`), it FAILs to compile. Adjust visibility to `private[web]` (Task 2 already specifies this) so it passes; then proceed to wire the generator (the generator wiring itself is verified by Task 6's cross-tier test + Task 4's bindings).

- [ ] **Step 3: Implement — add `rescaleExpr`, resolve once, pass per tile**

Add `rescaleExpr: Expression` to the case class BEFORE `exprConfExpr` (so the config stays last, matching `RST_TileXYZ`):

```scala
case class RST_XYZPyramid(
    tileExpr: Expression,
    minZExpr: Expression,
    maxZExpr: Expression,
    formatExpr: Expression,
    sizeExpr: Expression,
    resamplingExpr: Expression,
    rescaleExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator with Serializable with CodegenFallback {
```

Update `children` (line 53) and `withNewChildrenInternal` (line 55-56):

```scala
    override def children: Seq[Expression] =
        Seq(tileExpr, minZExpr, maxZExpr, formatExpr, sizeExpr, resamplingExpr, rescaleExpr, exprConfExpr)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7))
```

In `doEval`, after reading `resampling` (line 79) add:

```scala
        val rescale = Option(rescaleExpr.eval(input)).map(_.asInstanceOf[UTF8String].toString).getOrElse("auto")
```

Then after opening the source `ds` (line 81), resolve the scale ONCE (before the tile loop), and change the per-tile call (line 115) to use `executeWithScale` with the shared string:

```scala
        val (_, ds, options) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
        try {
            // Resolve the 8-bit rescale mapping ONCE from the source (stats read once; every
            // tile shares one mapping => no tile-to-tile seams). Mirrors the light tier.
            val scaleFlags = RST_TileXYZ.resolveScale(ds, rescale)

            // ... (unchanged: WGS84 bbox, count guard) ...

            // in the emit loop, replace RST_TileXYZ.execute(...) with:
                    val bytes = RST_TileXYZ.executeWithScale(ds, options, zz, xx, yy, format, size, resampling, scaleFlags)
        }
```

Update the builder (line 163) arity 3→7:

```scala
    /** Builder: 3 to 7 args (tile, min_z, max_z, [format, [size, [resampling, [rescale]]]]). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 3 => RST_XYZPyramid(c(0), c(1), c(2), Literal("PNG"), Literal(256), Literal("bilinear"), Literal("auto"))
            case 4 => RST_XYZPyramid(c(0), c(1), c(2), c(3), Literal(256), Literal("bilinear"), Literal("auto"))
            case 5 => RST_XYZPyramid(c(0), c(1), c(2), c(3), c(4), Literal("bilinear"), Literal("auto"))
            case 6 => RST_XYZPyramid(c(0), c(1), c(2), c(3), c(4), c(5), Literal("auto"))
            case 7 => RST_XYZPyramid(c(0), c(1), c(2), c(3), c(4), c(5), c(6))
            case n => throw new IllegalArgumentException(
                s"gbx_rst_xyzpyramid takes 3 to 7 arguments (tile, min_z, max_z, [format, [size, [resampling, [rescale]]]]); got $n"
            )
        }
    }
```

> Note: `RST_XYZPyramid` is a `CollectionGenerator`/`CodegenFallback` (no `rstInvoke` reflection) — the new child just needs to thread through `children`/`withNewChildrenInternal` and `doEval`. No eval-overload reflection concern here (unlike `RST_TileXYZ`).

- [ ] **Step 4: Run the tests to verify they pass**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest' --log heavy-rescale-pyramid.log
```
Expected: all PASS (including the existing pyramid guard tests, which use TileMath directly and are unaffected by the new child).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_XYZPyramid.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/WebMercatorTileTest.scala
git commit -m "feat(rasterx): rescale param for RST_XYZPyramid (resolve once)

The pyramid generator threads a trailing rescale arg (default auto) and
resolves the per-band -scale string ONCE from the source before the tile
loop, passing it to every executeWithScale call so all tiles share one
8-bit mapping (no seams) and source stats are read a single time.

Co-authored-by: Isaac"
```

---

### Task 4: Bindings / parity surface — SQL examples + function-info regeneration

**Files:**
- Modify: `docs/tests/python/api/rasterx_functions_sql.py` (`rst_tilexyz_sql_example`, `rst_xyzpyramid_sql_example`)
- Regenerate: `src/main/resources/com/databricks/labs/gbx/function-info.json` (via `gbx:docs:function-info`)
- Confirm (no edit expected): `docs/tests-function-info/registered_functions.txt` — `gbx_rst_tilexyz` (line 76) and `gbx_rst_xyzpyramid` (line 78) already present; **no new function**, so no new row. The binding-parity check asserts function NAME presence across Scala `override def name`, Python `functions.py`, and `function-info.json` — all unchanged names. This is an arg-only change; parity should hold without a registered_functions.txt edit. VERIFY in Step 3.
- Confirm: Python `rst_tilexyz`/`rst_xyzpyramid` bindings already carry `rescale` (Phase 1) — no Python change in Phase 2.

> **Why no registered_functions.txt / Python edit:** binding-parity (`docs/scripts/check-binding-parity.py`) keys on function *names*, not arity. The names are unchanged. The only doc artifact that should change is the SQL example (to demonstrate the new arg) and the regenerated function-info (which is derived from the example). Phase 1 already added `rescale` to the Python bindings.

- [ ] **Step 1: Update the SQL examples to demonstrate `rescale`**

In `docs/tests/python/api/rasterx_functions_sql.py`, update `rst_tilexyz_sql_example()` (line 1515) to include the trailing `rescale` arg:

```python
def rst_tilexyz_sql_example():
    """Render a single web-mercator XYZ tile to PNG bytes"""
    return """
-- Render tile (z=10, x=512, y=512) as 256x256 PNG bytes.
-- rescale='auto' (default) rescales non-8-bit imagery by whole-dataset min/max
-- for display contrast; 'none' keeps the raw full-dtype-range mapping; a
-- 'min,max' string sets explicit bounds.
SELECT
    path,
    gbx_rst_tilexyz(tile, 10, 512, 512, 'PNG', 256, 'bilinear', 'auto') as tile_png
FROM rasters;
"""
```

Update `rst_xyzpyramid_sql_example()` (line 1535) docstring/example to mention the optional trailing `rescale`:

```python
def rst_xyzpyramid_sql_example():
    """Generate one row per (z, x, y) tile across a zoom range"""
    return """
-- Explode a raster into per-tile rows across zoom levels 4..6 (PNG, 256px).
-- Optional trailing rescale arg (default 'auto') controls 8-bit display contrast:
--   gbx_rst_xyzpyramid(tile, 4, 6, 'PNG', 256, 'bilinear', 'auto')
SELECT
    path,
    t.tile.z as z,
    t.tile.x as x,
    t.tile.y as y,
    t.tile.bytes as png_bytes
FROM rasters
LATERAL VIEW gbx_rst_xyzpyramid(tile, 4, 6) AS t;
"""
```

(Leave the `*_sql_example_output` blocks unchanged — the output shape is identical.)

- [ ] **Step 2: Regenerate function-info.json**

Dispatch a Task subagent (Docker — runs `generate-function-info.py` + pytest):
```
gbx:docs:function-info --log heavy-rescale-function-info.log
```
Expected: `src/main/resources/com/databricks/labs/gbx/function-info.json` regenerated; `gbx_rst_tilexyz` (line ~277) and `gbx_rst_xyzpyramid` (line ~325) examples now show the `rescale` arg. (If the SQL example doc-test executes against sample data, ensure the new arg literal is valid SQL — it is a plain string arg.)

- [ ] **Step 3: Run the binding-parity gate**

Dispatch a Task subagent (Docker):
```
gbx:test:bindings --log heavy-rescale-bindings.log
```
Expected: PASS. Every name in `registered_functions.txt` still resolves to a Scala `override def name`, a Python binding, and a `function-info.json` key. If it FAILs, read the failure — an arg-only change should not break it; a failure means the regeneration changed a key. Fix upstream (the example), never add a placeholder.

- [ ] **Step 4: Commit**

```bash
git add docs/tests/python/api/rasterx_functions_sql.py src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "docs(rasterx): document rescale arg in XYZ tiler SQL examples + function-info

rst_tilexyz/rst_xyzpyramid SQL examples now show the trailing rescale arg
(default auto). Regenerated function-info.json. Binding parity unchanged
(arg-only; function names identical).

Co-authored-by: Isaac"
```

---

### Task 5: Scalastyle lint (CI gate)

**Files:** none (verification gate).

- [ ] **Step 1: Run scalastyle (Docker — matches CI)**

Dispatch a Task subagent (Docker):
```
gbx:lint:scalastyle --log heavy-rescale-scalastyle.log
```
Expected: clean. Watch for: `caselocale` (wrap `.toUpperCase`/`.toLowerCase` in `// scalastyle:off caselocale` as the existing code does — already applied in the `resolveScale` and `doInvoke` edits), line length, and unused imports (`BandAccessors`, `gdalconstConstants`).

- [ ] **Step 2: Fix any findings, re-run, confirm clean.** Commit only if files changed:

```bash
git add -A
git commit -m "style(rasterx): scalastyle fixes for XYZ rescale

Co-authored-by: Isaac"
```

---

### Task 6: Cross-tier pixel/value-distribution parity gate (Docker — gates completion)

**Files:**
- Test (NEW): `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/XYZRescaleParityTest.scala`

> **SCOPE REFINEMENT (from the Task 2 review, binding for Tasks 6 + 7):**
> The cross-tier parity gate asserts parity for the **`"auto"` path** and the **uint8 pass-through** ONLY.
> **EXCLUDE `"none"` from cross-tier parity** and document it as a known per-tier-raw difference:
> heavy `"none"` (bare `gdal_translate -ot Byte`, no `-scale`) **CLIPS** values > 255 to 255,
> whereas light `"none"` (rio-tiler render with no `in_range`) does NOT clip the same way. This is a
> PRE-EXISTING difference in each tier's raw passthrough, predates this feature, and does NOT touch the
> `"auto"` contract (which is the whole point of the feature). Do NOT change heavy `"none"` to force a
> match — that would break heavy back-compat. Assert heavy `"none"` against HEAVY's real behavior (clip).
> Also: the spec's claim that light `"none"` yields a proportional `[31,46]` mapping is SUSPECT —
> rio-tiler `render()` on single-band uint16 with no `in_range` may 16-bit-passthrough, not proportionally
> squash. Task 7 (cross-language) MUST validate light `"none"` EMPIRICALLY rather than trust that number;
> Task 6 (in-Scala) does not assert light's `"none"` value at all.

**Goal:** Prove both tiers feed the SAME per-band `(min,max)` and produce equivalent value distributions **on the `"auto"` path** (plus uint8 pass-through). The heavy side is asserted directly here; the cross-tier equivalence is asserted by reproducing light's resolved `(min,max)` and confirming heavy derives the identical pair from the same source pixels, then asserting the decoded tile's value distribution matches the expected linear `[min,max]->[0,255]` mapping within a tolerance. (A live light-vs-heavy byte comparison is NOT possible inside a Scala suite; the value-distribution-vs-expected-mapping assertion is the in-suite proxy for the documented pixel-level parity contract. The light tier's own tests, Phase 1, already assert its side of the same mapping.)

**Design rationale for an in-Scala parity proxy:** the spec's parity contract is "both tiers derive the same `(min,max)` and feed it identically." Light feeds `in_range=[(min,max)]` to rio-tiler; heavy feeds `-scale min max 0 255`. Both are the SAME linear map `v -> round((v-min)/(max-min)*255)`. So a parity gate that (a) confirms heavy's resolved `(min,max)` equals the source's exact whole-dataset min/max (which is what light computes too), and (b) confirms heavy's decoded output matches that linear map within tolerance, proves the contract without crossing the language boundary in one process. A true cross-language byte/value diff belongs in the Python doc-test/bench harness (note below).

- [ ] **Step 1: Write the failing parity test**

```scala
package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Cross-tier parity gate for the XYZ rescale feature.
 *
 *  Both tiers MUST derive the same per-band (min,max) for a source and apply the same
 *  linear map v -> (v-min)/(max-min)*255. Light feeds rio-tiler in_range; heavy feeds
 *  gdal_translate -scale. Parity is pixel/value-distribution-level, NOT byte-level
 *  (heavy re-encodes a GTiff per tile; PNG encoders differ between GDAL and rio-tiler).
 *  uint8 pass-through is the one byte-identical-within-tier assertion (auto == none).
 */
class XYZRescaleParityTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        java.nio.file.Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    private def makeUint16Narrow(lo: Int = 8000, hi: Int = 12000): Dataset = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/parity_u16", 16, 16, 1, gdalconstConstants.GDT_UInt16)
        ds.SetGeoTransform(Array(-1.0, 0.125, 0.0, 1.0, 0.0, -0.125))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        val n = 256
        val ramp = (0 until n).map(i => (lo.toDouble + (hi - lo).toDouble * i / (n - 1))).toArray
        ds.GetRasterBand(1).WriteRaster(0, 0, 16, 16, ramp)
        ds.GetRasterBand(1).FlushCache()
        ds
    }

    private def pngBandSpread(bytes: Array[Byte]): (Int, Int) = {
        val path = s"/vsimem/parity_decode_${java.util.UUID.randomUUID().toString.replace("-", "")}.png"
        gdal.FileFromMemBuffer(path, bytes)
        val ds = gdal.Open(path)
        try {
            val band = ds.GetRasterBand(1)
            val buf = Array.ofDim[Byte](ds.GetRasterXSize * ds.GetRasterYSize)
            band.ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, buf)
            val vals = buf.map(_ & 0xff).filter(_ > 0)
            if (vals.isEmpty) (0, 0) else (vals.min, vals.max)
        } finally { ds.delete(); gdal.Unlink(path) }
    }

    test("heavy auto resolves source whole-dataset min/max (same statistic light uses)") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val scale = RST_TileXYZ.resolveScale(ds, "auto")
            // Expect "-scale_1 <~8000> <~12000> 0 255". Parse and bound-check.
            val parts = scale.trim.split("\\s+")
            parts(0) shouldBe "-scale_1"
            val lo = parts(1).toDouble
            val hi = parts(2).toDouble
            lo shouldBe (8000.0 +- 5.0)
            hi shouldBe (12000.0 +- 5.0)
            parts(3) shouldBe "0"
            parts(4) shouldBe "255"
        } finally ds.delete()
    }

    test("heavy auto recovers contrast (value distribution spans most of 8-bit range)") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val png = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "auto")
            val (lo, hi) = pngBandSpread(png)
            (hi - lo) should be > 100 // NOT crushed into ~[31,46]
        } finally ds.delete()
    }

    test("heavy none reproduces today's crushed full-dtype-range output") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val png = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "none")
            val (_, hi) = pngBandSpread(png)
            hi should be < 80 // 8000..12000 / 65535 * 255 -> ~[31,46]
        } finally ds.delete()
    }

    test("heavy explicit pair maps exactly the given bounds") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val scale = RST_TileXYZ.resolveScale(ds, "8000,12000")
            scale shouldBe "-scale_1 8000.0 12000.0 0 255"
        } finally ds.delete()
    }

    test("uint8 source: auto == none (byte-identical pass-through within tier)") {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/parity_u8", 16, 16, 1, gdalconstConstants.GDT_Byte)
        ds.SetGeoTransform(Array(-1.0, 0.125, 0.0, 1.0, 0.0, -0.125))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 16, 16, Array.fill(256)(100.0))
        ds.GetRasterBand(1).FlushCache()
        try {
            val auto = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "auto")
            val none = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "none")
            java.util.Arrays.equals(auto, none) shouldBe true
        } finally ds.delete()
    }
}
```

- [ ] **Step 2: Run to verify it fails / then passes**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.web.XYZRescaleParityTest' --log heavy-rescale-parity.log
```
Expected on first run after Tasks 1-3: PASS (the implementation is already in place). The test is a GATE — if any assertion fails, it surfaces a parity/semantics regression. (If `resolveScale` emits a different numeric format than `-scale_1 8000.0 12000.0 0 255`, adjust the explicit-pair assertion to match the actual `Double.toString` rendering — verify the exact string in the log and lock it in.)

- [ ] **Step 3: Run BOTH web suites together as the final heavy gate**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.web.WebMercatorTileTest,com.databricks.labs.gbx.rasterx.expressions.web.XYZRescaleParityTest' --log heavy-rescale-final.log
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/XYZRescaleParityTest.scala
git commit -m "test(rasterx): cross-tier value-distribution parity gate for XYZ rescale

Asserts heavy auto resolves the source whole-dataset min/max (the same
statistic the light tier computes), recovers contrast, reproduces today's
crushed output under none, maps explicit pairs exactly, and that uint8
auto==none byte-identical within tier. Parity is value-distribution-level
per the established convention.

Co-authored-by: Isaac"
```

---

### Task 7 (optional, recommended): Cross-language live parity in the Python doc-test/bench harness

**Files:** (only if a Docker doc-test harness with both tiers + a JAR is available — see memory "Docker volumes for integration tests")
- A Python test that tiles the SAME uint16 narrow-range fixture with BOTH the light `rst_tilexyz` and the registered heavy `gbx_rst_tilexyz` (JAR loaded), decodes both PNGs, and asserts equivalent per-band value distribution within a tolerance (Wasserstein/quantile match), plus uint8 auto==none within each tier.

> Deferred-but-recommended because the in-Scala Task 6 proves the contract algebraically (same `(min,max)`, same linear map). A true cross-language byte/value diff needs both runtimes in one harness (the bench/doc-test container with a staged JAR). If that harness is not readily available, Task 6 is the accepted gate and this is a follow-up. DECISION NEEDED from the user (see Open Questions).

---

## Self-Review

- **Spec coverage:**
  - `rescale` param + default `auto` on both heavy functions — Tasks 2, 3 (builders default `Literal("auto")`).
  - uint8 pass-through (`auto` → no `-scale`) — Task 2 `resolveScale`, asserted Tasks 2 & 6.
  - non-8-bit whole-dataset per-band min/max via `BandAccessors.getMinMax` (exact `ComputeRasterMinMax`) — Task 2.
  - `"none"` escape hatch (today's full-dtype-range) — Task 2/6 (`hi < 80`).
  - explicit `(min,max)` (as `"min,max"` string) — Task 2 `resolveScale`, asserted Task 6.
  - resolve-once / no seams — Task 3 (`resolveScale` before loop, `executeWithScale` per tile), asserted Task 3.
  - `-scale min max 0 255` injection into the byte-output translate step — Task 1 (`OperatorOptions`) + Task 2 (thread `scale` option).
  - GDALManager-guarded registration (stats on open Band, no new registration) — Global Constraints + Task 2 design note.
  - Bindings/parity surface — Task 4 (SQL examples + function-info regen + binding-parity gate; registered_functions.txt/Python confirmed unchanged with rationale).
  - Heavy tests + lint + bindings in Docker — Tasks 2-6 commands; lint Task 5.
  - Cross-tier pixel/value-distribution parity gate — Task 6 (+ optional Task 7 live cross-language).
- **No placeholders:** every step has concrete code or an exact `gbx:*` command. Task 7 is explicitly optional/deferred with a stated decision point, not a stub in the critical path.
- **Type/name consistency:** `resolveScale(ds, rescale: String): String` / `execute(..., rescale: String)` / `executeWithScale(..., scaleFlags: String)` / `rescaleExpr: Expression` / SQL `rescale` (string) / builder `Literal("auto")` — consistent across Tasks 2, 3, 6. `OperatorOptions` key is `"scale"` (the pre-formatted flag string) consistently in Tasks 1, 2, 3.
- **Number of tasks:** 7 (6 required + 1 optional).

## Heavy-tier-specific RISKS / OPEN QUESTIONS (need user decision before execution)

1. **`InvokedExpression.rstInvoke` reflection binding (HIGHEST RISK).** `RST_TileXYZ.replacement = rstInvoke(RST_TileXYZ, rasterType)` reflectively maps children (minus the trailing `ExpressionConfigExpr`) to the `evalBinary/evalPath` overload params positionally. Adding `rescaleExpr` REQUIRES the eval overloads to gain a matching trailing `UTF8String rescale` param in the exact child order (…, resampling, **rescale**, conf). If `rstInvoke` matches by arg COUNT and TYPE, the four overloads (Int/Long × Binary/Path) must all be updated symmetrically (plan does this). **I have NOT read `InvokedExpression`/`rstInvoke` — confirm the exact reflection contract (positional vs by-name, how `conf` is appended) before Task 2.** If it binds by a fixed arg list, a mismatch surfaces as a runtime `NoSuchMethodException`/`UNRESOLVED_ROUTINE` only when the SQL function is actually called — so the SQL-level exercise in Task 4/6 is essential, not just the direct-`execute` unit tests.

2. **`-scale` + `-a_nodata none` + NoData interaction.** The PNG branch strips NoData (`-a_nodata none`) to avoid tRNS. With `-scale`, GDAL scales the raw values including any NoData sentinel (e.g. a uint16 NoData of 0 or 65535 would be included in the source min/max and skew the mapping). Light reads `ds.statistics(b, approx=False)` which RESPECTS the dataset mask/NoData; `BandAccessors.getMinMax` (`ComputeRasterMinMax`) does NOT mask NoData unless the band has a NoData value set. **Potential cross-tier divergence on NoData rasters.** Mitigation options (DECISION NEEDED): (a) accept for now — the Helios/EO fixtures are NoData-free; (b) set band NoData awareness in `getMinMax` (use `ComputeStatistics` which honors NoData, or mask first). The plan's fixtures are NoData-free so tests pass; flag for real-data validation.

3. **JPEG has no alpha channel.** The transparent-PNG fallback and the PNG `-a_nodata none` path assume RGBA-capable output. JPEG (and the existing default branch) cannot carry alpha; out-of-extent JPEG tiles can't be transparent. This is PRE-EXISTING (heavy already returns a PNG transparent fallback regardless of `fmt` — see `transparentPng`), but adding `-scale` to JPEG/WEBP is new. **DECISION:** scope `-scale` to PNG-only in Task 1 (safest, matches the Helios case), or include JPEG/WEBP (plan includes them with a caution). Recommend PNG-only for Phase 2 unless you want all three.

4. **`ComputeRasterMinMax` on full-res vs overviews.** `getMinMax` with `force=0` computes EXACT min/max over the full-resolution band (no approximation, no overview sampling) — this matches light's `approx=False` and is the correct parity choice, but on very large sources it reads every pixel once per band. Acceptable per the spec ("one stats read per source, negligible vs tiling"). No overview-sampling divergence risk since both tiers are exact. Confirmed low-risk; noted for completeness.

5. **Thread-safety of stats computation.** `ComputeRasterMinMax` mutates the band's cached statistics (PAM) but operates on a per-task open `Dataset` (each Spark task deserializes its own tile via `rowToTile`) — no shared/process-global state, no driver registration. Safe under concurrency given the GDALManager guard already gates registration. No new global mutation introduced. Confirmed safe.

6. **`Double.toString` formatting in `-scale` flags.** `resolveScale` interpolates `Double`s, so `8000.0` renders as `"8000.0"` and `getMinMax` results render with full precision (e.g. `7999.0` or `8000.000000001`). GDAL `-scale` accepts floats, so this is functionally fine, but the explicit-pair test asserts an exact string (`-scale_1 8000.0 12000.0 0 255`). **Lock the exact rendering after the first Docker run** (Task 6 Step 2 note). Not a correctness risk, just a test-brittleness note.

7. **SQL surface for the explicit pair.** The plan encodes the `(min,max)` pair as a single string literal `'min,max'` to keep the SQL signature one extra `STRING` arg (parallel to `format`/`resampling`). The Python API (Phase 1) accepts a real `(min,max)` tuple via the core path. **DECISION NEEDED:** is a string `'8000,12000'` acceptable as the heavy SQL surface for the explicit pair, or do you want a different encoding (e.g. two extra numeric args `min`, `max`)? The string form keeps arity/registration simple and parity trivial; two numeric args would change the builder arity scheme and the binding signature. Recommend the string form.

8. **Optional Task 7 (live cross-language parity).** Whether to add the in-harness light-vs-heavy value-distribution test now (needs the bench/doc-test Docker container with a staged JAR per memory "Docker volumes for integration tests") or defer it. The in-Scala Task 6 proves the contract algebraically. **DECISION NEEDED:** run Task 7 now or accept Task 6 as the gate?
