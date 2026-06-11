# Raster I/O Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document the geobrix raster readers/writers across both tiers — net-new light `raster_gbx`/`gtiff_gbx` reader + writer pages, and an audit/fill of the heavy `gdal`/`gtiff_gdal` reader + writer option docs (esp. the tile-metadata-driven write encoding).

**Architecture:** Per repo convention, **doc-tests are the documentation source**: real, asserting Python in `docs/tests/python/{readers,writers}/<name>_examples.py` (display constants `FOO`/`FOO_output` + executable `test_*`/helper functions), imported into `.mdx` pages via `!!raw-loader!` + the `CodeFromTest` component. Doc-tests run in Docker (`gbx:test:python-docs`) on a JAR-backed Spark session; the light DataSources register on that session via `pyrx.ds.register.register(spark)`.

**Tech Stack:** Docusaurus MDX + `CodeFromTest`, pytest doc-tests (Docker, `local[*]`), rasterio, the light `pyrx.ds` package (already implemented + merged on this branch).

**Reference spec:** `docs/superpowers/specs/2026-06-11-light-raster-writer-design.md` → "Documentation" section. Companion: the reader spec `2026-06-11-light-readers-raster-design.md`.

---

## Ground-truth facts (verified — do not re-derive)

- **MDX import pattern:** `import xExamples from '!!raw-loader!../../tests/python/<dir>/<name>_examples.py';` then `<CodeFromTest code={xExamples} language="python" functionName="READ_X" source="docs/tests/python/<dir>/<name>_examples.py" testFile="docs/tests/python/<dir>/test_<name>_examples.py" outputConstant="READ_X_output" />`. `CodeFromTest` extracts the named constant by substring; `outputConstant` (optional) renders an "Example output" block.
- **Doc-test file:** display constants `READ_X = """..."""` + optional `READ_X_output = """..."""`; helper/`test_*` functions take the `spark` fixture and do real reads/writes. Sample path via `from path_config import SAMPLE_DATA_BASE`.
- **Sample raster:** `f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"` (heavy docs use this; exists in the doc-test Volume mount).
- **Runner:** `bash scripts/commands/gbx-test-python-docs.sh --suite <readers|writers> --skip-build` → `pytest docs/tests/python/... -m 'not integration'` in Docker. conftest `spark` fixture = JAR-backed `local[*]`, rasterx registered. The light DS registers fine on it (`spark.dataSource.register(...)`); the `[pyrx]` wheel is installed in the doc-test env by the runner build.
- **Sidebar:** `docs/sidebars.js` "Readers & Writers" category — `readers/*` + `writers/*` lists. **Overview tables:** `docs/docs/readers/overview.mdx`, `docs/docs/writers/overview.mdx`.
- **Heavy reader options already documented** (`readers/gdal.mdx`): `sizeInMB`, `filterRegex`, `readSubdatasets`, `rasterAsGrid`, `retile`, `tileSize`, `driver`. **Heavy writer options documented:** `ext`, `nameCol` + a "format comes from the tile" section. **Gap:** the tile-metadata write-encoding keys (`compression`/`blocksize`/`zlevel`/`zstd_level`/`format`) and an explicit `gtiff_gdal` *writer* example are NOT documented.
- **Stale line:** `docs/docs/api/execution-tiers.mdx` Readers/Writers row says "native Python Data Source readers planned" — now shipped.
- **QC gate:** `internals-leak` blocks `wave\s*\d+` in `docs/docs/`. Doc-tests must pass; MDX must build (`gbx:ci:docs`/`gbx:docs`).

All commands run from repo root. **Before EVERY commit:** `chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects`. Do NOT push. Doc-tests run in Docker only.

## File structure

| File | Responsibility |
|---|---|
| `docs/tests/python/readers/raster_gbx_examples.py` | **New.** Light reader display constants + `test_*` (raster_gbx + gtiff_gbx reads). |
| `docs/tests/python/readers/test_raster_gbx_examples.py` | **New.** pytest entry calling the helpers. |
| `docs/docs/readers/raster_gbx.mdx` | **New.** Light reader page (both formats). |
| `docs/tests/python/writers/raster_gbx_examples.py` | **New.** Light writer constants + `test_*` (verbatim, nameCol, round-trip). |
| `docs/tests/python/writers/test_raster_gbx_examples.py` | **New.** pytest entry. |
| `docs/docs/writers/raster_gbx.mdx` | **New.** Light writer page. |
| `docs/docs/writers/gdal.mdx` | **Modify.** Document tile-metadata encoding keys + `gtiff_gdal` writer example. |
| `docs/tests/python/writers/gdal_examples.py` | **Modify.** Add a `gtiff_gdal` write display-constant + helper. |
| `docs/docs/readers/gdal.mdx`, `readers/gtiff.mdx` | **Modify (audit).** Ensure `path`/`driver` documented; complete the options tables. |
| `docs/docs/api/execution-tiers.mdx` | **Modify.** Un-stale the readers/writers row. |
| `docs/sidebars.js` | **Modify.** Add `readers/raster_gbx`, `writers/raster_gbx`. |
| `docs/docs/readers/overview.mdx`, `writers/overview.mdx` | **Modify.** Add a row each. |

---

## Task 1: Light reader doc-test (`raster_gbx_examples.py`)

**Files:**
- Create: `docs/tests/python/readers/raster_gbx_examples.py`, `docs/tests/python/readers/test_raster_gbx_examples.py`

- [ ] **Step 1: Create the examples module**

`docs/tests/python/readers/raster_gbx_examples.py`:

```python
"""raster_gbx / gtiff_gbx (lightweight) Reader Examples — single source of truth.

Code shown in docs/docs/readers/raster_gbx.mdx is imported from here. Pure-Python
DataSource V2 readers; no JAR required (registered via pyrx.ds.register).
"""
from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

REGISTER = """# Register the lightweight raster DataSources (once per session)
from databricks.labs.gbx.pyrx.ds.register import register
register(spark)"""

READ_RASTER_GBX = """# Catch-all lightweight reader (any rasterio-readable raster)
df = spark.read.format("raster_gbx").load("{SAMPLE_RASTER_PATH}")
df.show()"""

READ_RASTER_GBX_output = """+--------------------------------------------------+-----+
|source                                            |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

READ_GTIFF_GBX = """# Named lightweight GeoTIFF reader (preset for GeoTIFF)
df = spark.read.format("gtiff_gbx").load("{SAMPLE_RASTER_PATH}")"""

READ_WITH_OPTIONS = """# Options: sizeInMB (tile split threshold) + filterRegex (directory listing)
df = (spark.read.format("raster_gbx")
      .option("sizeInMB", "16")
      .option("filterRegex", r".*\\.tif$")
      .load("{SAMPLE_RASTER_PATH}"))"""


def _register(spark):
    from databricks.labs.gbx.pyrx.ds.register import register
    register(spark)


def read_raster_gbx(spark, path=None):
    """Verify READ_RASTER_GBX: catch-all reader yields (source, tile) rows."""
    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    assert [f.name for f in df.schema.fields] == ["source", "tile"]
    rows = df.collect()
    assert len(rows) >= 1
    assert rows[0]["tile"]["cellid"] == -1
    return df


def read_gtiff_gbx(spark, path=None):
    """Verify READ_GTIFF_GBX: named reader reads a GeoTIFF identically."""
    _register(spark)
    df = spark.read.format("gtiff_gbx").load(path or SAMPLE_RASTER_PATH)
    assert df.count() >= 1
    assert df.collect()[0]["tile"]["metadata"]["driver"] == "GTiff"
    return df
```

- [ ] **Step 2: Create the test entry**

`docs/tests/python/readers/test_raster_gbx_examples.py`:

```python
"""Executes the raster_gbx reader doc examples against real sample data (Docker)."""
import raster_gbx_examples as ex


def test_read_raster_gbx(spark):
    ex.read_raster_gbx(spark)


def test_read_gtiff_gbx(spark):
    ex.read_gtiff_gbx(spark)
```

- [ ] **Step 3: Run in Docker**

Run: `bash scripts/commands/gbx-test-python-docs.sh --path readers/test_raster_gbx_examples.py --skip-build`
Expected: 2 passed. (`--skip-build` reuses the built JAR/wheel; if the wheel lacks `pyrx.ds`, drop `--skip-build` once to rebuild.) If `register` import fails, confirm the `[pyrx]` wheel is installed in-container (`docker exec geobrix-dev python3 -c "import databricks.labs.gbx.pyrx.ds.register"`).

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add docs/tests/python/readers/raster_gbx_examples.py docs/tests/python/readers/test_raster_gbx_examples.py
git commit -m "docs(test): light raster_gbx/gtiff_gbx reader doc examples"
```

---

## Task 2: Light reader MDX page + nav

**Files:**
- Create: `docs/docs/readers/raster_gbx.mdx`
- Modify: `docs/sidebars.js`, `docs/docs/readers/overview.mdx`

- [ ] **Step 1: Create the page**

`docs/docs/readers/raster_gbx.mdx`:

```mdx
---
sidebar_position: 9
---

import CodeFromTest from '@site/src/components/CodeFromTest';
import rasterGbxExamples from '!!raw-loader!../../tests/python/readers/raster_gbx_examples.py';

# Lightweight Raster Readers (`raster_gbx` / `gtiff_gbx`)

Pure-Python/PySpark raster readers built on Spark DataSource V2 — the lightweight
tier's drop-in for the GDAL-backed [`gdal`](./gdal) / [`gtiff_gdal`](./gtiff)
readers. They require no JAR (powered by `rasterio`) and run on Serverless. Output
is the same `(source, tile)` schema as the heavy readers, so downstream code is
unchanged — swapping tiers is a one-line `format(...)` change.

## Register

<CodeFromTest code={rasterGbxExamples} language="python" functionName="REGISTER"
  source="docs/tests/python/readers/raster_gbx_examples.py" />

## Read (catch-all)

<CodeFromTest code={rasterGbxExamples} language="python" functionName="READ_RASTER_GBX"
  source="docs/tests/python/readers/raster_gbx_examples.py"
  testFile="docs/tests/python/readers/test_raster_gbx_examples.py"
  outputConstant="READ_RASTER_GBX_output" />

## Read GeoTIFF (named)

<CodeFromTest code={rasterGbxExamples} language="python" functionName="READ_GTIFF_GBX"
  source="docs/tests/python/readers/raster_gbx_examples.py"
  testFile="docs/tests/python/readers/test_raster_gbx_examples.py" />

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `sizeInMB` | `"16"` | Split threshold (MB on disk) for tiling large rasters into multiple tiles. |
| `filterRegex` | `".*"` | When loading a directory, keep files whose full path matches this regex. |

<CodeFromTest code={rasterGbxExamples} language="python" functionName="READ_WITH_OPTIONS"
  source="docs/tests/python/readers/raster_gbx_examples.py" />

`gtiff_gbx` is `raster_gbx` with the GeoTIFF driver preset. See
[Choosing an Execution Tier](../api/execution-tiers) for when to use the
lightweight vs heavyweight readers.
```

- [ ] **Step 2: Add to sidebar + overview**

In `docs/sidebars.js`, add `'readers/raster_gbx',` to the Readers `items` list (after `'readers/gtiff'`).

In `docs/docs/readers/overview.mdx`, add rows to the reader table:

```markdown
| [Lightweight Raster Reader](./raster_gbx) | `raster_gbx` | Pure-Python catch-all raster reader (no JAR; DataSource V2) |
| [Lightweight GeoTIFF Reader](./raster_gbx) | `gtiff_gbx` | Pure-Python GeoTIFF reader (preset `driver="GTiff"`) |
```

- [ ] **Step 3: Verify the MDX builds**

Run: `bash scripts/commands/gbx-ci-docs.sh 2>/dev/null || bash scripts/commands/gbx-docs-start.sh --build-only 2>/dev/null || (cd docs && npm run build)`
Expected: build succeeds, no broken-link/import error for `readers/raster_gbx`. (Use whichever docs-build command exists; check `scripts/commands/` for `gbx-ci-docs.sh` / `gbx-docs-*.sh`. If none builds headless, at minimum confirm the raw-loader path resolves: the import path `../../tests/python/readers/raster_gbx_examples.py` is correct relative to `docs/docs/readers/`.)

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add docs/docs/readers/raster_gbx.mdx docs/sidebars.js docs/docs/readers/overview.mdx
git commit -m "docs: lightweight raster reader page (raster_gbx/gtiff_gbx) + nav"
```

---

## Task 3: Light writer doc-test (`writers/raster_gbx_examples.py`)

**Files:**
- Create: `docs/tests/python/writers/raster_gbx_examples.py`, `docs/tests/python/writers/test_raster_gbx_examples.py`

- [ ] **Step 1: Create the examples module**

`docs/tests/python/writers/raster_gbx_examples.py`:

```python
"""raster_gbx / gtiff_gbx (lightweight) Writer Examples — single source of truth.

Code shown in docs/docs/writers/raster_gbx.mdx is imported from here. Writer
options are path/nameCol/ext; on-disk encoding comes from tile.metadata.
"""
import os
import tempfile

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

WRITE_GTIFF_GBX = """# Read then write GeoTIFF tiles (lightweight)
from databricks.labs.gbx.pyrx.ds.register import register
register(spark)
df = spark.read.format("raster_gbx").load("{SAMPLE_RASTER_PATH}")
df.write.format("gtiff_gbx").mode("overwrite").save(OUT_DIR)"""

WRITE_WITH_NAMECOL = """# Control output filenames: overwrite 'source', set nameCol
from pyspark.sql.functions import concat, lit, monotonically_increasing_id
(df.withColumn("source", concat(lit("tile_"), monotonically_increasing_id()))
   .write.format("gtiff_gbx").mode("overwrite")
   .option("nameCol", "source").option("ext", "tif").save(OUT_DIR))"""

ENCODING_NOTE = """# On-disk format/compression come from tile.metadata, NOT writer options
#   driver/format -> output driver (default GTiff; GTiff = passed through verbatim)
#   compression/blocksize/zlevel/zstd_level -> applied when re-encoding (non-GTiff)
# Change them via upstream transforms, then write."""


def _register(spark):
    from databricks.labs.gbx.pyrx.ds.register import register
    register(spark)


def write_gtiff_gbx(spark, path=None):
    """Verify WRITE_GTIFF_GBX: round-trip read -> write -> re-read, same pixels."""
    import numpy as np
    import rasterio
    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    with tempfile.TemporaryDirectory() as out_dir:
        df.write.format("gtiff_gbx").mode("overwrite").save(out_dir)
        files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
        assert files, "no output written"
        with rasterio.open(os.path.join(out_dir, files[0])) as w:
            written = w.read()
        with rasterio.open(path or SAMPLE_RASTER_PATH) as src:
            truth = src.read()
        # whole-file GTiff pass-through -> identical pixels
        assert written.shape == truth.shape
        np.testing.assert_allclose(written, truth, rtol=1e-3, atol=1e-3)


def write_with_namecol(spark, path=None):
    """Verify WRITE_WITH_NAMECOL: nameCol controls output filenames."""
    from pyspark.sql.functions import lit
    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    with tempfile.TemporaryDirectory() as out_dir:
        (df.withColumn("source", lit("mytile"))
           .write.format("gtiff_gbx").mode("overwrite")
           .option("nameCol", "source").save(out_dir))
        assert "mytile.tif" in os.listdir(out_dir)
```

- [ ] **Step 2: Create the test entry**

`docs/tests/python/writers/test_raster_gbx_examples.py`:

```python
"""Executes the raster_gbx writer doc examples (Docker)."""
import raster_gbx_examples as ex


def test_write_gtiff_gbx(spark):
    ex.write_gtiff_gbx(spark)


def test_write_with_namecol(spark):
    ex.write_with_namecol(spark)
```

- [ ] **Step 3: Run in Docker**

Run: `bash scripts/commands/gbx-test-python-docs.sh --path writers/test_raster_gbx_examples.py --skip-build`
Expected: 2 passed. (Note: two `raster_gbx_examples.py` files now exist — one under `readers/`, one under `writers/`. pytest imports by module name; confirm no import collision — if pytest complains about duplicate module basenames, the doc-test suite uses `rootdir`/`importmode` that disambiguates by path; if it errors, set unique names is NOT desired — instead verify `docs/tests/python` has an `__init__.py`-free layout with `--import-mode=importlib` (check conftest/pyproject) and note the resolution.)

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add docs/tests/python/writers/raster_gbx_examples.py docs/tests/python/writers/test_raster_gbx_examples.py
git commit -m "docs(test): light raster_gbx/gtiff_gbx writer doc examples"
```

---

## Task 4: Light writer MDX page + nav

**Files:**
- Create: `docs/docs/writers/raster_gbx.mdx`
- Modify: `docs/sidebars.js`, `docs/docs/writers/overview.mdx`

- [ ] **Step 1: Create the page**

`docs/docs/writers/raster_gbx.mdx`:

```mdx
---
sidebar_position: 4
---

import CodeFromTest from '@site/src/components/CodeFromTest';
import rasterGbxWrite from '!!raw-loader!../../tests/python/writers/raster_gbx_examples.py';

# Lightweight Raster Writer (`raster_gbx` / `gtiff_gbx`)

Pure-Python/PySpark raster writer — the lightweight tier's drop-in for the
GDAL-backed [`gdal`](./gdal) writer. Requires the exact `(source, tile)` schema,
the same as the heavy writer. Writer options are `path` / `nameCol` / `ext`; the
on-disk format and compression come from `tile.metadata`, not writer options.

## Write GeoTIFF tiles

<CodeFromTest code={rasterGbxWrite} language="python" functionName="WRITE_GTIFF_GBX"
  source="docs/tests/python/writers/raster_gbx_examples.py"
  testFile="docs/tests/python/writers/test_raster_gbx_examples.py" />

A whole-file GeoTIFF tile is written through verbatim (no re-encode); a tile whose
`metadata["driver"]` is non-GTiff (e.g. `COG`) is re-encoded via `rasterio`.

## Control filenames (`nameCol`)

<CodeFromTest code={rasterGbxWrite} language="python" functionName="WRITE_WITH_NAMECOL"
  source="docs/tests/python/writers/raster_gbx_examples.py"
  testFile="docs/tests/python/writers/test_raster_gbx_examples.py" />

## Output format & compression

<CodeFromTest code={rasterGbxWrite} language="python" functionName="ENCODING_NOTE"
  source="docs/tests/python/writers/raster_gbx_examples.py" />

| Option | Default | Description |
|--------|---------|-------------|
| `nameCol` | _unset_ | Existing string column whose value is the output filename (overwrite `source`). When unset, an opaque unique name is used. |
| `ext` | `"tif"` | Filename suffix. Does **not** change the on-disk format. |

The driver / `compression` / `blocksize` / `zlevel` / `zstd_level` are read from
`tile.metadata` (same as the heavy [`gdal`](./gdal) writer).
```

- [ ] **Step 2: Sidebar + overview**

In `docs/sidebars.js`, add `'writers/raster_gbx',` to the Writers `items` (after `'writers/gdal'`).

In `docs/docs/writers/overview.mdx`, add a row:

```markdown
| [Lightweight Raster Writer](./raster_gbx) | `raster_gbx`, `gtiff_gbx` | Pure-Python raster writer (no JAR; DataSource V2) |
```

- [ ] **Step 3: Verify MDX builds** (same command as Task 2 Step 3).

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add docs/docs/writers/raster_gbx.mdx docs/sidebars.js docs/docs/writers/overview.mdx
git commit -m "docs: lightweight raster writer page + nav"
```

---

## Task 5: Heavy `gdal`/`gtiff_gdal` writer option docs

**Files:**
- Modify: `docs/docs/writers/gdal.mdx`
- Modify: `docs/tests/python/writers/gdal_examples.py`

- [ ] **Step 1: Add a `gtiff_gdal` write example to the doc-test**

Append to `docs/tests/python/writers/gdal_examples.py`:

```python
WRITE_GTIFF_GDAL = """# Named GeoTIFF writer (gtiff_gdal = gdal writer with driver preset)
spark.read.format("gtiff_gdal").load(SAMPLE_RASTER_PATH) \\
    .write.format("gtiff_gdal").mode("append").option("ext", "tif").save(OUT_DIR)"""

ENCODING_FROM_METADATA = """# Output encoding is read from tile.metadata, not writer options:
#   format/driver (default GTiff), compression (DEFLATE), blocksize (512),
#   zlevel (6), zstd_level (9). Set them upstream (e.g. RST_AsFormat), then write."""
```

(These are display constants — no new test function needed; the existing `gdal` write test already covers the write path. If the file has a `_run` harness that asserts every constant is a non-empty string, these satisfy it.)

- [ ] **Step 2: Document the encoding keys + gtiff_gdal writer in `writers/gdal.mdx`**

In `docs/docs/writers/gdal.mdx`, after the existing options table, add an "Output encoding (from tile metadata)" subsection importing the new constants and a table:

```mdx
## Output encoding (from tile metadata)

The output **driver, compression, and block layout are read from
`tile.metadata`**, not from writer options. They are set when the tile is read or
produced (e.g. via `RST_AsFormat`); the writer honors them on serialization.

| Metadata key | Default | Effect |
|--------------|---------|--------|
| `driver` / `format` | `GTiff` | GDAL output driver. |
| `compression` | `DEFLATE` | `DEFLATE` / `ZSTD` / `LZW` / … creation compression. |
| `blocksize` | `512` | Tile/block size in pixels (floored to a multiple of 16, clamped to the raster size). |
| `zlevel` | `6` | DEFLATE level. |
| `zstd_level` | `9` | ZSTD level. |

<CodeFromTest code={gdalExamples} language="python" functionName="ENCODING_FROM_METADATA"
  source="docs/tests/python/writers/gdal_examples.py" />

### Named GeoTIFF writer (`gtiff_gdal`)

`gtiff_gdal` is the `gdal` writer with the GeoTIFF driver preset — use it to make
GeoTIFF output explicit.

<CodeFromTest code={gdalExamples} language="python" functionName="WRITE_GTIFF_GDAL"
  source="docs/tests/python/writers/gdal_examples.py" />
```

(Confirm `gdal.mdx` already imports `gdalExamples` via raw-loader — it does, per the existing page; reuse that import.)

- [ ] **Step 3: Run the writer doc-tests + build**

Run: `bash scripts/commands/gbx-test-python-docs.sh --path writers/ --skip-build` (the gdal write test still passes; new constants are strings). Then verify MDX builds (Task 2 Step 3 command).

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add docs/docs/writers/gdal.mdx docs/tests/python/writers/gdal_examples.py
git commit -m "docs: document heavy writer tile-metadata encoding keys + gtiff_gdal writer"
```

---

## Task 6: Heavy reader option audit + execution-tiers un-stale

**Files:**
- Modify: `docs/docs/readers/gdal.mdx`, `docs/docs/readers/gtiff.mdx` (audit only)
- Modify: `docs/docs/api/execution-tiers.mdx`

- [ ] **Step 1: Audit the heavy reader options tables**

Read `docs/docs/readers/gdal.mdx` and `docs/docs/readers/gtiff.mdx`. Confirm the options table documents at least: `path` (load arg), `driver`, `sizeInMB`, `filterRegex`. Add any missing row (the recon found `sizeInMB`/`filterRegex`/`driver` present; if `path`/`driver` lack a one-line description, add it). Do NOT remove existing rows. If the tables are already complete, make no change and note it in the commit.

- [ ] **Step 2: Un-stale `execution-tiers.mdx`**

In `docs/docs/api/execution-tiers.mdx`, change the Readers/Writers row (currently `... binaryFile + rst_fromcontent today; native Python Data Source readers planned`) to reflect that the lightweight tier now has native readers + a writer:

```markdown
| Readers / Writers | `gtiff_gdal`, `gdal`, OGR readers | `raster_gbx` / `gtiff_gbx` native Python DataSource V2 reader + writer (no JAR); vector OGR readers still heavy-only |
```

And update the prose around "You need the GDAL/OGR readers" so it no longer says lightweight native readers are unavailable — note they exist for raster (`raster_gbx`/`gtiff_gbx`), with vector still heavy-only.

- [ ] **Step 3: internals-leak guard**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ 2>/dev/null` — must print nothing (QC `internals-leak` gate). Fix any hit you introduced.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add docs/docs/readers/gdal.mdx docs/docs/readers/gtiff.mdx docs/docs/api/execution-tiers.mdx
git commit -m "docs: audit heavy raster reader options; un-stale lightweight tier readers/writers"
```

---

## Task 7: Full docs verification (Docker)

- [ ] **Step 1: Run the full reader+writer doc-test suite in Docker**

Run: `bash scripts/commands/gbx-test-python-docs.sh --suite readers --skip-build` then `... --suite writers --skip-build`
Expected: all pass (the new `raster_gbx` reader/writer tests + existing gdal tests). Report progress per the repo convention for long runs.

- [ ] **Step 2: Build the docs site (link/import check)**

Run the docs build (`bash scripts/commands/gbx-ci-docs.sh` or `cd docs && npm run build`).
Expected: build succeeds; no broken links to the new pages, no unresolved raw-loader imports.

- [ ] **Step 3: internals-leak final check**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ 2>/dev/null` → nothing.

- [ ] **Step 4: Commit any fixes**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add -A docs
git commit -m "docs: raster I/O docs suite green + site builds"
```

---

## Out of scope

- Vector reader/writer docs (`*_ogr`, future `*_gbx` vector).
- Rewriting heavy reader/writer behavior docs beyond the option audit (e.g. the `(source, tile)` schema framing) unless an audited option is wrong.

## Self-review notes (for the executor)

- **Two `raster_gbx_examples.py` files** (readers/ + writers/) — same basename. If pytest collection errors on duplicate module names, check `docs/tests/python` import mode (likely `importlib` via pyproject `addopts`); if not, the safe fix is unique basenames (`raster_gbx_read_examples.py` / `raster_gbx_write_examples.py`) — adjust the `import` lines in the `.mdx` and `test_*` files to match. Decide based on the actual collection behavior in Task 3 Step 3.
- **`CodeFromTest functionName`** does substring extraction — keep constant names unique within a file (e.g. don't have both `READ` and `READ_GTIFF` where one is a prefix of the other in a way that mis-extracts; the existing files use distinct names like `READ_GDAL`/`READ_WITH_DRIVER`).
- **Light DS registration in doc-tests:** call `register(spark)` inside each helper (idempotent) — the doc-test session is shared/session-scoped, re-register is safe (DataSourceManager replaces).
- **No `wave N`** or internal vocabulary in any `.mdx` (QC `internals-leak`).
- Doc-tests run **in Docker only** (need `/Volumes` sample data + the wheel).
```
