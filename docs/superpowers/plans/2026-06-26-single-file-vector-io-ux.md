# Single-File / Bundle Vector I/O UX — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give GeoBrix's single-file/bundle vector writers a consistent `fileName` option + adaptive output naming, make the shapefile reader's `.load()` contract identical across tiers, and replace the cryptic heavy read-only-write failure with a clear error.

**Architecture:** One pure-function path-resolution helper in the light writer (`ds/vector.py`) drives all four light single-file writers; the heavy shapefile reader gains bare-`.shp` sidecar staging + recursive-dir union (light reader gains recursive listing) with a shared schema-divergence error; heavy read-only OGR formats reject writes before `inferSchema` runs.

**Tech Stack:** Light = Python 3.12 / PySpark DataSource V2 (pyogrio); heavy = Scala 2.13 / Spark 4 DataSource V2 (GDAL/OGR JNI). Tests/build in the `geobrix-dev` Docker container via `gbx:*` commands.

## Global Constraints

- Branch: **`beta/0.4.0`** (commit directly; no per-feature branch).
- Light tier is **pure Python/PySpark** — no `_jvm` / `spark.conf` / `sparkContext` in product code.
- Heavy tier is **Scala 2.13 / Spark 4.0**.
- **DRY:** one shared `_resolve_single_file_output` helper drives all light single-file writers; one shared schema-divergence error message string reused across tiers (same wording).
- **No new `gbx_` functions** → binding parity unaffected.
- Canonical extensions: `gpkg`→`.gpkg`, `geojson`→`.geojson`, shapefile+`zip`→`.shp.zip`, file_gdb→`.gdb` (or `.gdb.zip` with `zip=true`).
- Prefer **pure-unit tests** for path-resolution logic (no Spark); use real `/Volumes` sample data only for round-trip tests that need a cluster/Docker.
- Doc-tests are the documentation source (`docs/tests/...`); user-facing docs under `docs/docs/`.
- **Out of scope:** adding heavy single-file *vector* writers (heavy vector stays read-only); implementing PMTiles `fileName` (contract only — helper must be reusable); sharded/dir-writer naming (`geojsonl_gbx`/`geojsonl_ogr`/raster tile dirs).

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `python/geobrix/src/databricks/labs/gbx/ds/vector.py` | light writers + reader | add helper; wire 4 writers; recursive reader listing + schema-divergence error |
| `python/geobrix/test/ds/test_vector_filename.py` | light unit + round-trip tests | **create** |
| `python/geobrix/test/ds/test_vector_reader_contract.py` | light reader contract tests | **create** |
| `src/main/scala/com/databricks/labs/gbx/util/HadoopUtils.scala` | heavy listing/staging | bare-`.shp` sidecar discovery; shared schema-divergence message |
| `src/main/scala/com/databricks/labs/gbx/vectorx/ds/ogr/OGR_DataSource.scala` | heavy reader schema infer | stage siblings for bare `.shp`; read-only-write guard |
| `src/main/scala/com/databricks/labs/gbx/vectorx/ds/ogr/OGR_Batch.scala` | heavy partition plan | schema-divergence detection |
| `src/main/scala/com/databricks/labs/gbx/vectorx/ds/ogr/OGR_Table.scala` | heavy capabilities | (read-only guard support if needed) |
| `src/test/scala/com/databricks/labs/gbx/vectorx/ds/ogr/OgrReaderContractTest.scala` | heavy reader/write-guard tests | **create** |
| `docs/docs/writers/*.mdx`, `docs/docs/readers/*.mdx` | user docs | `fileName`/naming + `.load()` contract |

Canonical names used across tasks (define once, reuse):
- `_resolve_single_file_output(path: str, file_name: str | None, ext: str) -> str`
- `_canonical_ext(driver: str, zip_enabled: bool) -> str`
- `_complete_ext(name: str, ext: str) -> str`
- Schema-divergence message: `"shapefile reader: shapefiles under <path> have differing schemas; load them separately or use a single-stem directory. Stems: <a>, <b>."`

---

## Workstream A — Light writer `fileName` + adaptive naming

### Task A1: `_complete_ext` + `_canonical_ext` helpers (pure unit)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (new module-level functions near `zip_shapefile`, ~line 160)
- Test: `python/geobrix/test/ds/test_vector_filename.py` (create)

**Interfaces — Produces:**
- `_canonical_ext(driver: str, zip_enabled: bool) -> str`
- `_complete_ext(name: str, ext: str) -> str`

- [ ] **Step 1: Write failing tests**

```python
# python/geobrix/test/ds/test_vector_filename.py
from databricks.labs.gbx.ds.vector import _canonical_ext, _complete_ext

def test_canonical_ext():
    assert _canonical_ext("GPKG", False) == ".gpkg"
    assert _canonical_ext("GeoJSON", False) == ".geojson"
    assert _canonical_ext("ESRI Shapefile", True) == ".shp.zip"
    assert _canonical_ext("OpenFileGDB", False) == ".gdb"
    assert _canonical_ext("OpenFileGDB", True) == ".gdb.zip"

def test_complete_ext_appends_when_missing():
    assert _complete_ext("roads", ".shp.zip") == "roads.shp.zip"
    assert _complete_ext("roads.shp", ".shp.zip") == "roads.shp.zip"   # partial -> complete
    assert _complete_ext("roads.shp.zip", ".shp.zip") == "roads.shp.zip"  # already complete
    assert _complete_ext("city", ".gpkg") == "city.gpkg"
    assert _complete_ext("city.gpkg", ".gpkg") == "city.gpkg"

def test_complete_ext_rejects_wrong_geo_ext():
    import pytest
    with pytest.raises(ValueError, match="expected .shp.zip"):
        _complete_ext("roads.gpkg", ".shp.zip")
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_vector_filename.py`
Expected: FAIL (ImportError: cannot import `_canonical_ext`).

- [ ] **Step 3: Implement**

```python
# in ds/vector.py
_CANONICAL_EXT = {
    "GPKG": ".gpkg",
    "GeoJSON": ".geojson",
    "ESRI Shapefile": ".shp.zip",     # single-file form is zip; non-zip is a dir bundle (out of scope)
    "OpenFileGDB": ".gdb",
}
# Recognized geo extensions, longest-first, so multi-part suffixes match before their parts.
_RECOGNIZED_EXTS = (".shp.zip", ".gdb.zip", ".gpkg", ".geojson", ".gdb", ".shp")

def _canonical_ext(driver: str, zip_enabled: bool) -> str:
    if driver == "OpenFileGDB":
        return ".gdb.zip" if zip_enabled else ".gdb"
    return _CANONICAL_EXT[driver]

def _complete_ext(name: str, ext: str) -> str:
    low = name.lower()
    if low.endswith(ext):
        return name
    # Incremental completion for multi-part ext (e.g. "roads.shp" -> "roads.shp.zip").
    for k in range(1, ext.count(".") + 1):
        suffix = "." + ".".join(ext.strip(".").split(".")[:k])
        if low.endswith(suffix) and ext.startswith(suffix):
            return name + ext[len(suffix):]
    # Reject a DIFFERENT recognized geo extension rather than double-append.
    for other in _RECOGNIZED_EXTS:
        if other != ext and low.endswith(other):
            raise ValueError(
                f"output name '{name}' ends with '{other}' but this writer expects '{ext}'."
            )
    return name + ext
```

- [ ] **Step 4: Run to verify pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_vector_filename.py`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_filename.py
git commit -m "feat(vector): canonical-ext + ext-completion helpers for single-file writers" -m "Co-authored-by: Isaac"
```

### Task A2: `_resolve_single_file_output` (the 3-case contract, pure unit)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py`
- Test: `python/geobrix/test/ds/test_vector_filename.py`

**Interfaces — Consumes:** `_complete_ext`. **Produces:** `_resolve_single_file_output(path, file_name, ext) -> str` (returns the resolved output path; creates parent dirs as a side effect).

- [ ] **Step 1: Write failing tests** (use `tmp_path` for the existing-dir case)

```python
import os
from databricks.labs.gbx.ds.vector import _resolve_single_file_output as R

def test_case1_filename_given(tmp_path):
    out = R(str(tmp_path / "newdir"), "roads", ".shp.zip")
    assert out == str(tmp_path / "newdir" / "roads.shp.zip")
    assert os.path.isdir(tmp_path / "newdir")             # parent created

def test_case2_existing_dir_no_filename(tmp_path):
    d = tmp_path / "roads_dir"; d.mkdir()
    out = R(str(d), None, ".shp.zip")
    assert out == str(d / "roads_dir.shp.zip")            # named after the dir, under it

def test_case3_stem_path_no_filename(tmp_path):
    out = R(str(tmp_path / "sub" / "roads"), None, ".gpkg")
    assert out == str(tmp_path / "sub" / "roads.gpkg")    # complete ext on the stem
    assert os.path.isdir(tmp_path / "sub")                # parent created

def test_filename_extension_completed(tmp_path):
    out = R(str(tmp_path), "roads.shp", ".shp.zip")
    assert out == str(tmp_path / "roads.shp.zip")
```

- [ ] **Step 2: Run to verify failure** — `gbx:test:python --path .../test_vector_filename.py` → FAIL (import).

- [ ] **Step 3: Implement**

```python
def _resolve_single_file_output(path: str, file_name, ext: str) -> str:
    """Resolve the output path for a single-file/single-unit writer. See
    docs/superpowers/specs/2026-06-26-writer-filename-naming-design.md (3-case contract).
    Creates the parent directory as needed. Pure path logic + one mkdirs side effect."""
    path = path.rstrip("/")
    if file_name:                                   # case 1: path is the parent dir
        os.makedirs(path, exist_ok=True)
        return os.path.join(path, _complete_ext(file_name, ext))
    if os.path.isdir(path):                          # case 2: existing dir -> name after it, under it
        return os.path.join(path, _complete_ext(os.path.basename(path), ext))
    # case 3: file-like target -> complete ext, create parent
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    return _complete_ext(path, ext)
```

- [ ] **Step 4: Run to verify pass** — expected PASS (4 tests).
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_filename.py
git commit -m "feat(vector): _resolve_single_file_output adaptive-naming helper" -m "Co-authored-by: Isaac"
```

### Task A3: Wire the helper + `fileName` into `VectorGbxWriter`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`VectorGbxWriter.__init__`, currently `:721-784` — replace the inline zip-extension block `:738-753`)

**Interfaces — Consumes:** `_resolve_single_file_output`, `_canonical_ext`.

- [ ] **Step 1: Replace the inline naming block.** Delete lines `738-753` (the `if self.zip:` extension block) and the bare `self.path = to_local_path(path)` assignment's downstream use, replacing with:

```python
self.zip = opts.get("zip", "false").lower() == "true" and self.driver in (
    "ESRI Shapefile", "OpenFileGDB",
)
self._file_name = opts.get("filename")  # .option("fileName", ...) (opts are lower-cased)
# Single-file/unit writers (gpkg/geojson, shapefile+zip, file_gdb): adaptive naming.
# Non-zip shapefile remains a directory bundle (existing behavior; not single-file).
if self.driver in ("GPKG", "GeoJSON") or self.zip or self.driver == "OpenFileGDB":
    ext = _canonical_ext(self.driver, self.zip)
    self.path = _resolve_single_file_output(self.path, self._file_name, ext)
```

> NOTE: non-zip `ESRI Shapefile` keeps the prior directory-bundle path handling — do not route it through the helper (out of scope). Confirm the `else` branch preserves `self.path = to_local_path(path)` for that case.

- [ ] **Step 2: Add a unit test asserting the writer resolves paths** (construct the writer with a fake schema; assert `self.path`):

```python
def test_writer_resolves_gpkg_stem(tmp_path):
    from databricks.labs.gbx.ds.vector import VectorGbxWriter
    from pyspark.sql.types import StructType, StructField, BinaryType, IntegerType
    sch = StructType([StructField("geom", BinaryType()), StructField("geom_srid", IntegerType())])
    w = VectorGbxWriter(str(tmp_path / "city"), sch, "GPKG", {}, overwrite=True)
    assert w.path == str(tmp_path / "city.gpkg")
```

- [ ] **Step 3: Run** — `gbx:test:python --path python/geobrix/test/ds/test_vector_filename.py` → PASS.
- [ ] **Step 4: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_filename.py
git commit -m "feat(vector): fileName option + adaptive naming in light single-file writers" -m "Co-authored-by: Isaac"
```

### Task A4: Round-trip tests on a Volume (Docker)

**Files:** Test: `python/geobrix/test/ds/test_vector_filename.py`

- [ ] **Step 1: Add round-trip tests** (gated on Docker `/Volumes` per existing corpus-test pattern; see [[docker-volumes-for-integration-tests]]). For each of `gpkg_gbx`, `geojson_gbx`, `shapefile_gbx`(zip), `file_gdb_gbx`: write with (a) stem path, (b) existing dir, (c) `.option("fileName", ...)`, then read back and assert row count + resolved filename matches the contract.

- [ ] **Step 2: Run** — `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_vector_filename.py` (in container, Volumes mounted). Expected PASS.
- [ ] **Step 3: Commit.**

### Task A5: Writer docs

**Files:** Modify `docs/docs/writers/overview.mdx` + each single-file writer page.

- [ ] Document the `fileName` option + the 3-case naming behavior (one table mirroring the spec). Run `gbx:docs:restart`; verify rendering. Commit.

---

## Workstream B — Shapefile reader `.load()` contract (light + heavy)

### Task B1: Light — recursive directory listing + stem grouping

**Files:** Modify `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`VectorGbxReader._members`, `:457-473`). Test: `python/geobrix/test/ds/test_vector_reader_contract.py` (create).

- [ ] **Step 1: Failing tests** — dir-of-dirs of shapefiles should enumerate all `.shp` recursively:

```python
def test_members_recursive_shapefiles(tmp_path):
    # build sub1/a.shp, sub2/b.shp (+ sidecars) ; assert both enumerated
    ...
    members = reader._members()
    assert {os.path.basename(m) for m in members} == {"a.shp", "b.shp"}
```

- [ ] **Step 2: Run → FAIL** (current `os.listdir` is non-recursive).
- [ ] **Step 3: Implement** — replace `os.listdir(self.path)` with `os.walk`-based recursive collection, still filtered by `_EXT_FOR_DRIVER` and still returning `[self.path]` for a file / `.gdb`:

```python
def _members(self):
    if not os.path.isdir(self.path) or self.path.lower().rstrip("/").endswith(".gdb"):
        return [self.path]
    exts = self._EXT_FOR_DRIVER.get(self.driver) or ()
    members = []
    for root, _dirs, files in os.walk(self.path):
        for n in sorted(files):
            low = n.lower()
            if (exts and low.endswith(exts)) or low.rstrip("/").endswith(".gdb"):
                members.append(os.path.join(root, n))
    return sorted(members) or [self.path]
```

- [ ] **Step 4: Run → PASS.** Also re-run existing reader tests to confirm flat-dir + bare-file unchanged.
- [ ] **Step 5: Commit.**

### Task B2: Light — schema-divergence error

**Files:** Modify `ds/vector.py` (reader schema inference path). Test: `test_vector_reader_contract.py`.

- [ ] **Step 1: Failing test** — a dir with two shapefiles of differing schemas raises the shared message.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — when `> 1` member, infer each member's schema (pyogrio `read_info`), and if any differs from the first, raise `ValueError` with the shared message (Global Constraints). Single member → unchanged.
- [ ] **Step 4: Run → PASS** (+ same-schema union still works).
- [ ] **Step 5: Commit.**

### Task B3: Heavy — bare-`.shp` sidecar staging

**Files:** Modify `src/main/scala/.../util/HadoopUtils.scala` (`stageHeadForSchemaSpark` / `listDataFilesSpark`) + `OGR_DataSource.scala`. Test: `OgrReaderContractTest.scala` (create).

- [ ] **Step 1: Failing test** — `spark.read.format("shapefile_ogr").load("<dir>/x.shp")` (bare `.shp`) reads N rows (currently throws `Unable to open x.shx`).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — when the resolved data path is a single `.shp` (sidecar-bundle primary), discover stem-siblings from the `.shp`'s **parent directory** (Hadoop FS list of the parent, filter by stem) and stage them alongside in `stageHeadForSchemaSpark`; the `OGR_Batch` read path already co-copies stem-siblings via `copyToPath`, so confirm it covers the bare-`.shp` partition too.
- [ ] **Step 4: Run → PASS** (in Docker w/ Volumes or local fixture).
- [ ] **Step 5: Commit.**

### Task B4: Heavy — schema-divergence error (shared message)

**Files:** Modify `OGR_Batch.scala` / `OGR_DataSource.scala` + `HadoopUtils.scala` (shared message constant). Test: `OgrReaderContractTest.scala`.

- [ ] **Step 1: Failing test** — a dir with two differing-schema shapefiles raises the shared message (same wording as light).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — during planning, when multiple `.shp` stems are present, compare each `.shp`'s inferred schema to the head's; on divergence throw `IllegalArgumentException` with the shared message. (Single stem / single file unchanged.)
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Commit.**

### Task B5: Reader contract docs + cross-tier parity note

**Files:** Modify `docs/docs/readers/*.mdx` (shapefile reader page). Test: doc-test if applicable.

- [ ] Document the `.load()` contract (single `.shp`; recursive same-schema dir; `.shp.zip`), identical in both tiers. Render-check. Commit.

---

## Workstream C — Heavy read-only OGR clear-error

### Task C1: Reject writes to read-only OGR formats with a clear message

**Files:** Modify `src/main/scala/.../vectorx/ds/ogr/OGR_DataSource.scala` (and `OGR_Table.scala` if the guard belongs at the table/capability layer). Test: `OgrReaderContractTest.scala`.

- [ ] **Step 1: Failing test** — `df.write.format("shapefile_ogr").save(path)` raises a clear error containing `"read-only"` and `"shapefile_gbx"` (currently throws `NoSuchFileException` from `stageHeadForSchemaSpark`).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — ensure a write attempt on `ogr`/`shapefile_ogr`/`gpkg_ogr`/`file_gdb_ogr`/`geojson_ogr` fails fast with an actionable message *before* `inferSchema` reads the target. Options to evaluate in implementation: drop `supportsExternalMetadata` for the write path, or have `getTable` return a table whose `capabilities()` excludes `BATCH_WRITE` and let Spark's "table does not support writes" surface — but prefer an explicit GeoBrix message naming the `_gbx` alternative. Reads must be unaffected (verify an existing read test still passes).
- [ ] **Step 4: Run → PASS** (write-guard test + a read regression test).
- [ ] **Step 5: Commit.**

---

## Self-Review

**Spec coverage:** §3 contract → A1/A2; §4 EXT → A1; §5 applicability (light now) → A3/A4; §6 heavy read-only error → C1; §9 tests → A4/B5 round-trips + unit tests. #71 reader contract → B1–B5. PMTiles → helper is reusable (A2), application out of scope (noted). ✅
**Placeholder scan:** B2/B3/B4 implementation steps describe the mechanism with the exact files + message but defer some Scala specifics to the implementer — acceptable as they hinge on `read_info`/Hadoop-FS list calls already used in the file; the shared message string is fixed in Global Constraints. No "TBD"/"handle edge cases". 
**Type consistency:** `_resolve_single_file_output(path, file_name, ext)`, `_canonical_ext(driver, zip_enabled)`, `_complete_ext(name, ext)` used consistently A1→A3. ✅
