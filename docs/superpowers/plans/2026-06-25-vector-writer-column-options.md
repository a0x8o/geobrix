# Vector Writer Column Options (`geomCol`/`sridCol`/`projCol`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users point the vector writers at their existing geometry/SRID/proj columns by name (`geomCol`/`sridCol`/`projCol`) instead of renaming columns to the `X`/`X_srid`/`X_srid_proj` convention — in both the light tier (all five `*_gbx` writers) and the heavy tier (`geojsonl` only).

**Architecture:** Generalize the single role-derivation function in each tier to accept optional column-name overrides, then thread the three options from each writer's option map into it. Add a per-format output geometry name for the light writers so an arbitrary input column name doesn't leak into the output file.

**Tech Stack:** Python (PySpark DataSource V2, pyogrio), Scala (Spark DataSource V2, GDAL/OGR JNI). Heavy work builds + tests in the `geobrix-dev` Docker container via Maven.

## Global Constraints

- Option names are exactly `geomCol`, `sridCol`, `projCol`; parsed case-insensitively (light writers already lowercase the options dict; heavy `GeoJSONL_RowWriter` already builds a lowercased `ciOptions`).
- Resolution rule: each option, if given, must name an **existing** column (clear error otherwise); if omitted it falls back to its convention name. **geom** required; **srid** required (option or `<geom>_srid`; clear error if unresolvable); **proj** optional.
- No options passed → behavior identical to today (backward compatible).
- Identical option names + semantics across light and heavy; the existing heavy↔light `geojsonl` round-trip must keep holding.
- Light tier: all five writers (`geojson_gbx`, `geojsonl_gbx`, `gpkg_gbx`, `shapefile_gbx`, `file_gdb_gbx`) via the shared `_writer_col_roles`. Heavy tier: `geojsonl` only (the other heavy OGR formats are read-only).
- Output geometry name per format (light): `GPKG` → `geom`, `OpenFileGDB` → `SHAPE`; `GeoJSON`/`GeoJSONSeq`/`ESRI Shapefile` geometry is structural (the name is inert).
- All Python test/lint runs in the `geobrix-dev` container: tests `bash scripts/commands/gbx-test-python.sh --path <p>`; lint `bash scripts/commands/gbx-lint-python.sh --check` (format in-container with `black`/`isort`). Scala tests run via Maven in the container (`gbx:test:scala --suite ...`).
- Branch `beta/0.4.0`; commit locally, do NOT push (controller pushes on the user's go).

---

### Task 1: Light — generalize `_writer_col_roles` with optional overrides

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`_writer_col_roles`)
- Test: `python/geobrix/test/ds/test_writer_col_roles.py` (new)

**Interfaces:**
- Produces: `_writer_col_roles(schema, geom_col=None, srid_col=None, proj_col=None) -> (geom, srid, proj, attr_cols)`. geom & srid required (raise `ValueError`); proj optional. No-override call behaves as today.

- [ ] **Step 1: Write the failing tests**

Create `python/geobrix/test/ds/test_writer_col_roles.py`:

```python
import pytest
from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.gbx.ds.vector import _writer_col_roles


def _schema(*names_types):
    return StructType([StructField(n, t, True) for n, t in names_types])


CONV = _schema(
    ("name", StringType()),
    ("geom_0", BinaryType()),
    ("geom_0_srid", StringType()),
    ("geom_0_srid_proj", StringType()),
)


def test_default_convention():
    g, s, p, attrs = _writer_col_roles(CONV)
    assert (g, s, p) == ("geom_0", "geom_0_srid", "geom_0_srid_proj")
    assert attrs == ["name"]


def test_explicit_geom_and_srid_arbitrary_names():
    sch = _schema(
        ("v", LongType()),
        ("the_geom", BinaryType()),
        ("epsg", StringType()),
        ("proj4", StringType()),
    )
    g, s, p, attrs = _writer_col_roles(
        sch, geom_col="the_geom", srid_col="epsg", proj_col="proj4"
    )
    assert (g, s, p) == ("the_geom", "epsg", "proj4")
    assert attrs == ["v"]


def test_srid_defaults_off_geom_when_only_geomcol_given():
    g, s, p, _ = _writer_col_roles(CONV, geom_col="geom_0")
    assert (g, s, p) == ("geom_0", "geom_0_srid", "geom_0_srid_proj")


def test_geomcol_missing_column_raises():
    with pytest.raises(ValueError):
        _writer_col_roles(CONV, geom_col="nope")


def test_sridcol_missing_column_raises():
    with pytest.raises(ValueError):
        _writer_col_roles(CONV, srid_col="nope")


def test_projcol_missing_column_raises():
    with pytest.raises(ValueError):
        _writer_col_roles(CONV, proj_col="nope")


def test_srid_unresolvable_raises():
    # geomCol given, but no sridCol and no <geom>_srid present
    sch = _schema(("the_geom", BinaryType()), ("v", LongType()))
    with pytest.raises(ValueError):
        _writer_col_roles(sch, geom_col="the_geom")


def test_proj_optional_absent_is_fine():
    sch = _schema(
        ("name", StringType()),
        ("geom_0", BinaryType()),
        ("geom_0_srid", StringType()),
    )
    g, s, p, attrs = _writer_col_roles(sch)
    assert (g, s) == ("geom_0", "geom_0_srid")
    assert p == "geom_0_srid_proj"  # default name, not present -> harmless
    assert attrs == ["name"]
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_writer_col_roles.py`
Expected: FAIL — `_writer_col_roles() got an unexpected keyword argument 'geom_col'`.

- [ ] **Step 3: Implement**

Replace `_writer_col_roles` in `python/geobrix/src/databricks/labs/gbx/ds/vector.py` with:

```python
def _writer_col_roles(schema, geom_col=None, srid_col=None, proj_col=None):
    """(geom_col, srid_col, proj_col, attr_cols) for the vector writers.

    By default the column ``X`` paired with ``X_srid`` is the geometry,
    ``X_srid_proj`` is its PROJ4 fallback, and everything else is an attribute.
    The geomCol / sridCol / projCol options override these by name so the frame
    need not use the convention: each option, when given, must name an existing
    column; when omitted it falls back to its convention name. geom and srid are
    required (clear error if unresolvable); proj is optional.
    """
    names = [f.name for f in schema.fields]

    # geometry (required)
    if geom_col is not None:
        if geom_col not in names:
            raise ValueError(f"vector writer geomCol={geom_col!r} is not a column; got {names}")
        geom = geom_col
    else:
        srid_named = [n for n in names if n.endswith("_srid")]
        if not srid_named:
            raise ValueError(
                "vector writer input needs a geometry/'*_srid' column pair (from a "
                f"*_gbx reader) or an explicit geomCol option; got columns {names}"
            )
        geom = srid_named[0][: -len("_srid")]
        if geom not in names:
            raise ValueError(f"no geometry column {geom!r} for srid {srid_named[0]!r}")

    # srid (required: option, else <geom>_srid)
    if srid_col is not None:
        if srid_col not in names:
            raise ValueError(f"vector writer sridCol={srid_col!r} is not a column; got {names}")
        srid = srid_col
    else:
        srid = geom + "_srid"
        if srid not in names:
            raise ValueError(
                f"vector writer needs a SRID column: pass sridCol, or add a {srid!r} "
                f"column (authority code, '0' if unknown). Columns: {names}"
            )

    # proj (optional: an explicit projCol must exist; the default may be absent)
    if proj_col is not None:
        if proj_col not in names:
            raise ValueError(f"vector writer projCol={proj_col!r} is not a column; got {names}")
        proj = proj_col
    else:
        proj = geom + "_srid_proj"  # optional; may be absent

    attr_cols = [n for n in names if n not in (geom, srid, proj)]
    return geom, srid, proj, attr_cols
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_writer_col_roles.py`
Expected: PASS (8 tests).

- [ ] **Step 5: Lint + commit**

```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && black python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_writer_col_roles.py && isort python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_writer_col_roles.py"
bash scripts/commands/gbx-lint-python.sh --check
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_writer_col_roles.py
git commit -m "feat(ds): _writer_col_roles accepts geom/srid/proj column overrides"
```

---

### Task 2: Light — thread `geomCol`/`sridCol`/`projCol` through both writers

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`VectorGbxWriter.__init__`, `GeoJSONLGbxWriter.__init__`)
- Test: `python/geobrix/test/ds/test_geojsonl_writer.py`

**Interfaces:**
- Consumes: `_writer_col_roles(schema, geom_col, srid_col, proj_col)` (Task 1).
- Produces: both writers read `geomcol`/`sridcol`/`projcol` from their lowercased `opts` and pass them to `_writer_col_roles`.

- [ ] **Step 1: Write the failing test**

Append to `python/geobrix/test/ds/test_geojsonl_writer.py`:

```python
def test_geomcol_sridcol_options_avoid_renaming(spark, tmp_path):
    # A frame with non-convention column names writes via the geomCol/sridCol
    # options without the user renaming anything.
    register(spark)
    out = str(tmp_path / "renamed")
    rows = [
        ("a", bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    df = spark.createDataFrame(
        rows, schema="name string, the_geom binary, epsg string, p4 string"
    ).repartition(2, F.col("the_geom"))
    (
        df.write.format("geojsonl_gbx")
        .mode("overwrite")
        .option("geomCol", "the_geom")
        .option("sridCol", "epsg")
        .option("projCol", "p4")
        .save(out)
    )
    back = spark.read.format("geojson_gbx").option("multi", "true").load(out)
    assert back.count() == 2
    assert {r["name"] for r in back.collect()} == {"a", "b"}
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path "python/geobrix/test/ds/test_geojsonl_writer.py::test_geomcol_sridcol_options_avoid_renaming"`
Expected: FAIL — the writer still auto-derives by convention and errors (no `*_srid` column).

- [ ] **Step 3: Implement — read the options in both writers**

In `VectorGbxWriter.__init__`, change the `_writer_col_roles(schema)` call to pass the options. The block currently reads:

```python
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = _writer_col_roles(
            schema
        )
```

becomes:

```python
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = _writer_col_roles(
            schema,
            geom_col=opts.get("geomcol"),
            srid_col=opts.get("sridcol"),
            proj_col=opts.get("projcol"),
        )
```

Make the identical change in `GeoJSONLGbxWriter.__init__` (same two lines —
`opts` is already the lowercased dict there too).

- [ ] **Step 4: Run to verify pass + no regression**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_geojsonl_writer.py`
Expected: PASS (all, including the new test).
Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_vector_writer.py`
Expected: PASS (no regression — default path unchanged).

- [ ] **Step 5: Lint + commit**

```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && black python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_geojsonl_writer.py && isort python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_geojsonl_writer.py"
bash scripts/commands/gbx-lint-python.sh --check
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_geojsonl_writer.py
git commit -m "feat(ds): light vector writers accept geomCol/sridCol/projCol options"
```

---

### Task 3: Light — per-format output geometry name

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (a `_output_geom_name` helper + the two `geometry_name=` pyogrio sites + the osgeo FileGDB `CreateLayer`)
- Test: `python/geobrix/test/ds/test_vector_writer.py`

**Interfaces:**
- Consumes: `self.driver`, `self.geom_col`.
- Produces: `_output_geom_name(driver, geom_col) -> str` — `"geom"` for GPKG, `"SHAPE"` for OpenFileGDB, else `geom_col` (structural drivers: inert).

**Note:** for GPKG this changes the on-disk geometry column from the input name to `geom`, so `gpkg_gbx` reads it back as `geom` (not `geom_0`). Update the affected round-trip expectation in this task.

- [ ] **Step 1: Write the failing test**

Append to `python/geobrix/test/ds/test_vector_writer.py` (mirror its existing imports / `register` usage):

```python
def test_gpkg_output_uses_format_default_geom_name(spark, tmp_path):
    # GPKG output should use the format-default geometry column name `geom`,
    # not the input column name, so an arbitrary input name doesn't leak out.
    from databricks.labs.gbx.ds.register import register

    register(spark)
    out = str(tmp_path / "out.gpkg")
    rows = [("a", bytearray(_to_wkb(_Point(1.0, 2.0))), "4326", "")]
    df = spark.createDataFrame(
        rows, schema="name string, the_geom binary, epsg string, p4 string"
    )
    (
        df.write.format("gpkg_gbx")
        .mode("overwrite")
        .option("geomCol", "the_geom")
        .option("sridCol", "epsg")
        .save(out)
    )
    import pyogrio

    info = pyogrio.read_info(out)
    assert info["geometry_name"] == "geom"
```

(If `test_vector_writer.py` does not already import `to_wkb`/`Point`, add
`from shapely import Point as _Point` and `from shapely import to_wkb as _to_wkb`
at the top in Step 3.)

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path "python/geobrix/test/ds/test_vector_writer.py::test_gpkg_output_uses_format_default_geom_name"`
Expected: FAIL — `geometry_name` is `the_geom` (the input column name), not `geom`.

- [ ] **Step 3: Implement the helper + use it**

Add near `_writer_col_roles` in `vector.py`:

```python
# Output geometry field name per driver. GeoJSON/GeoJSONSeq/Shapefile geometry
# is structural (no named field), so the value is inert there; GPKG/FileGDB name
# the geometry column, so use the format default rather than the input column
# name (which may be arbitrary once geomCol is in play).
_OUTPUT_GEOM_NAME = {"GPKG": "geom", "OpenFileGDB": "SHAPE"}


def _output_geom_name(driver, geom_col):
    return _OUTPUT_GEOM_NAME.get(driver, geom_col)
```

In `VectorGbxWriter._write_local` (the pyogrio Arrow path), change the
`geometry_name=self.geom_col` in the `kw = dict(...)` to:

```python
            geometry_name=_output_geom_name(self.driver, self.geom_col),
```

In `VectorGbxWriter._write_local_osgeo_gdb`, name the created geometry field by
passing `geometry_name` where the layer is created — set the FileGDB geometry
field name to `_output_geom_name(self.driver, self.geom_col)` (i.e. `SHAPE`).
(Use the `geometry_name=` argument GDAL's `CreateLayer` options accept, or the
layer-creation option `["GEOMETRY_NAME=SHAPE"]`.)

The classic pyogrio fallback path (`_write_local_classic`) and `GeoJSONLGbxWriter`
write GeoJSON/GeoJSONSeq where geometry is structural, so leave their
`geometry_name=self.geom_col` as-is (inert) for minimal change.

- [ ] **Step 4: Run + fix the affected round-trip expectation**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_vector_writer.py`
Expected: the new test PASSES. If an existing GPKG round-trip test now reads the
geometry column back as `geom` (it derived `geom_0` before), update that test's
expected geometry-column name to `geom`. Re-run until green.

Also run the parity suite to catch any GPKG geom-name assumption:
Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_vector_parity.py`
Expected: PASS (update any GPKG geom-column-name expectation the same way).

- [ ] **Step 5: Lint + commit**

```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && black python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_writer.py && isort python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_writer.py"
bash scripts/commands/gbx-lint-python.sh --check
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_writer.py
git commit -m "feat(ds): vector writers name output geometry per format (GPKG geom, FileGDB SHAPE)"
```

---

### Task 4: Heavy — `geojsonl` `resolveRoles` overrides + option threading

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONL_DataSource.scala` (`resolveRoles`)
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONL_Table.scala` (`newWriteBuilder` validation call)
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONL_RowWriter.scala` (read options, pass to `resolveRoles`)
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONLWriterTest.scala`

**Interfaces:**
- Produces: `resolveRoles(schema, geomCol: Option[String] = None, sridCol: Option[String] = None, projCol: Option[String] = None): ColRoles` with the same rules as the light tier (geom & srid required; proj optional; an explicit override must name an existing column).

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONLWriterTest.scala` a case mirroring the existing write tests but with non-convention column names driven by options:

```scala
  test("geomCol/sridCol options write a non-convention frame") {
    import spark.implicits._
    val rows = Seq(
      ("a", wkb(-73.9, 40.7), "4326", ""),
      ("b", wkb(-0.1, 51.5), "4326", "")
    ).toDF("name", "the_geom", "epsg", "p4")
    val out = s"$tmpDir/renamed"
    rows.write
      .format("geojsonl")
      .mode("overwrite")
      .option("geomCol", "the_geom")
      .option("sridCol", "epsg")
      .option("projCol", "p4")
      .save(out)
    val back = spark.read.format("geojson_ogr").option("multi", "true").load(out)
    back.count() shouldEqual 2
  }
```

(Reuse the test's existing `wkb(...)` / WKB helper and `tmpDir` fixture; match
the file's exact helper names when implementing.)

Also add a `resolveRoles` unit assertion (no Spark write) in the same file:

```scala
  test("resolveRoles honors overrides and requires srid") {
    import org.apache.spark.sql.types._
    val sch = StructType(Seq(
      StructField("the_geom", BinaryType), StructField("epsg", StringType),
      StructField("p4", StringType), StructField("v", LongType)))
    val r = GeoJSONL_DataSource.resolveRoles(
      sch, Some("the_geom"), Some("epsg"), Some("p4"))
    r.geomCol shouldEqual "the_geom"
    r.sridCol shouldEqual "epsg"
    // geomCol given but no srid resolvable -> error
    val bad = StructType(Seq(StructField("the_geom", BinaryType)))
    an[IllegalArgumentException] should be thrownBy
      GeoJSONL_DataSource.resolveRoles(bad, Some("the_geom"), None, None)
  }
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.vectorx.ds.geojsonl.GeoJSONLWriterTest'`
Expected: FAIL — `resolveRoles` does not take the extra args / the renamed-frame write errors.

- [ ] **Step 3: Implement `resolveRoles` overrides**

Replace `resolveRoles` in `GeoJSONL_DataSource.scala` with:

```scala
    def resolveRoles(
        schema: StructType,
        geomColOpt: Option[String] = None,
        sridColOpt: Option[String] = None,
        projColOpt: Option[String] = None
    ): ColRoles = {
        val names = schema.fieldNames.toSeq

        // geometry (required)
        val geomCol = geomColOpt match {
            case Some(g) =>
                if (!names.contains(g))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer geomCol='$g' is not a column; got ${names.mkString("[", ", ", "]")}.")
                g
            case None =>
                val sridCols = names.filter(_.endsWith("_srid"))
                if (sridCols.isEmpty)
                    throw new IllegalArgumentException(
                        "`geojsonl` writer input needs a geometry/'*_srid' column pair (from a *_ogr " +
                        s"reader) or an explicit geomCol option; got ${names.mkString("[", ", ", "]")}.")
                val g = sridCols.head.dropRight("_srid".length)
                if (!names.contains(g))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer found srid column '${sridCols.head}' but no geometry column '$g'.")
                g
        }

        // srid (required: option, else <geom>_srid)
        val sridCol = sridColOpt match {
            case Some(s) =>
                if (!names.contains(s))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer sridCol='$s' is not a column; got ${names.mkString("[", ", ", "]")}.")
                s
            case None =>
                val s = geomCol + "_srid"
                if (!names.contains(s))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer needs a SRID column: pass sridCol, or add a '$s' column " +
                        "(authority code, '0' if unknown).")
                s
        }

        // proj (optional: explicit must exist; default may be absent)
        val projCol = projColOpt match {
            case Some(p) =>
                if (!names.contains(p))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer projCol='$p' is not a column; got ${names.mkString("[", ", ", "]")}.")
                p
            case None => geomCol + "_srid_proj"
        }

        val attrCols = names.filterNot(n => n == geomCol || n == sridCol || n == projCol)
        val geomType = schema(geomCol).dataType
        val geomIsWkb = geomType match {
            case BinaryType => true
            case StringType => false
            case other =>
                throw new IllegalArgumentException(
                    s"`geojsonl` writer geometry column '$geomCol' must be BINARY (WKB) or STRING (WKT); got $other.")
        }
        ColRoles(geomCol, sridCol, projCol, attrCols, geomIsWkb)
    }
```

- [ ] **Step 4: Thread the options at both call sites**

In `GeoJSONL_RowWriter.scala`, after the existing `ciOptions` reads (around the
`layerNameOpt` line), add:

```scala
    private val geomColOpt: Option[String] = ciOptions.get("geomcol")
    private val sridColOpt: Option[String] = ciOptions.get("sridcol")
    private val projColOpt: Option[String] = ciOptions.get("projcol")
```

and change the role resolution line to:

```scala
    private val roles = GeoJSONL_DataSource.resolveRoles(schema, geomColOpt, sridColOpt, projColOpt)
```

In `GeoJSONL_Table.scala` `newWriteBuilder`, the early validation call must read
the same options (the `info.options()` `CaseInsensitiveStringMap` is
case-insensitive, so look up the camelCase keys):

```scala
        val o = info.options()
        GeoJSONL_DataSource.resolveRoles(
            info.schema(),
            Option(o.get("geomCol")),
            Option(o.get("sridCol")),
            Option(o.get("projCol"))
        )
        new GeoJSONL_WriteBuilder(info.schema(), properties ++ info.options().asScala)
```

- [ ] **Step 5: Run to verify pass (in Docker/Maven)**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.vectorx.ds.geojsonl.GeoJSONLWriterTest'`
Expected: PASS (existing + the two new cases). This compiles + runs in the
`geobrix-dev` container; allow a few minutes.

- [ ] **Step 6: Scalastyle + commit**

```bash
bash scripts/commands/gbx-lint-scalastyle.sh
git add src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONL_DataSource.scala src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONL_Table.scala src/main/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONL_RowWriter.scala src/test/scala/com/databricks/labs/gbx/vectorx/ds/geojsonl/GeoJSONLWriterTest.scala
git commit -m "feat(vectorx): heavy geojsonl writer accepts geomCol/sridCol/projCol options"
```

---

### Task 5: Docs — document the options on the writer pages

**Files:**
- Modify: `docs/docs/writers/geojsonl.mdx`, `docs/docs/writers/geojson.mdx`, `docs/docs/writers/geopackage.mdx`, `docs/docs/writers/shapefile.mdx`, `docs/docs/writers/filegdb.mdx` (whichever document write options)

**Interfaces:** docs only.

- [ ] **Step 1: Add an options note**

On each vector writer page that lists write options, document `geomCol` /
`sridCol` / `projCol`: "Override the geometry / SRID / PROJ4 column names
(default to `<geom>` / `<geom>_srid` / `<geom>_srid_proj`); srid is required
(`"0"` if unknown), proj optional." Link the shared model to the
[Named Vector Formats](./overview#named-vector-formats) section. For
`geojsonl.mdx`, note the options work in both the lightweight and heavyweight
tiers; for the other formats, note the writer is lightweight-tier.

- [ ] **Step 2: Verify no internal-vocabulary leak**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/writers/`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add docs/docs/writers/
git commit -m "docs(writers): document geomCol/sridCol/projCol options"
```

---

## Out of scope / follow-ups

- Adding *write* support for the other heavy OGR formats (`shapefile`/`gpkg`/`geojson`/`file_gdb`) — they are heavy read-only today; that is a separate net-new effort.
- A JAR rebuild + cluster restage is needed before the heavy `geojsonl` change is usable on a cluster (the light change ships in the wheel).

## Self-Review

**Spec coverage:** light `_writer_col_roles` overrides (Task 1) ✓; thread through both light writers (Task 2) ✓; per-format output geom name (Task 3) ✓; heavy `geojsonl` `resolveRoles` + option threading at both call sites (Task 4) ✓; srid required / proj optional / defaults-if-present encoded in both tiers ✓; identical option names + semantics ✓; tests light + heavy ✓; docs ✓; heavy-only-geojsonl scoping respected (no other heavy writers touched) ✓.

**Placeholder scan:** none — every code step has complete code. Task 3's osgeo `CreateLayer` geometry-name uses the documented GDAL `GEOMETRY_NAME=` layer option; Task 4's test helper names defer to the existing fixture (called out explicitly).

**Type consistency:** `_writer_col_roles(schema, geom_col, srid_col, proj_col)` signature identical across Tasks 1-3; Scala `resolveRoles(schema, geomColOpt, sridColOpt, projColOpt)` identical across Task 4 sites; option keys `geomcol`/`sridcol`/`projcol` (lowercased) consistent in both light writers and the heavy `ciOptions`; `_output_geom_name(driver, geom_col)` consistent between definition and call sites.
