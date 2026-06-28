# PMTiles vector-merge: correctness fix + engineering lessons (2026-06-28)

Two engineering lessons captured from the `gbx_pmtiles_agg` vector-merge fix.
One is a pattern (vector tile aggregation must merge per tile, not first-wins);
the other is a GDAL gotcha (`GetMemFileBuffer` vs OGR-written `/vsimem/` files).
Neither was a measured speedup — the fix is purely correctness.

---

## Lesson 1 — Vector tile aggregation must MERGE per `(z,x,y)`, not first-wins

### Problem

`gbx_pmtiles_agg` silently dropped features in dense vector pipelines. When a
`st_asmvt_pyramid → groupBy → gbx_pmtiles_agg` run produced more than one MVT
blob for the same `(z, x, y)` (i.e. when multiple features land in the same tile
at the same zoom level), only the first blob was kept. All other features were
silently discarded. The resulting PMTiles archive was structurally valid but
incomplete — a correctness bug with no runtime error.

A secondary heavy-tier bug: the accumulator (`PMTilesAcc`) wrote one directory
entry per `(z, x, y, payload)` tuple, so duplicate tile coordinates produced
two entries for the same tile ID, yielding a malformed PMTiles archive
(spec requires exactly one directory entry per tile ID).

### Symptom / signature

- Dense vector data (many features per tile at any zoom level) loads into a
  PMTiles viewer with far fewer features than expected.
- No exception is raised. The archive is byte-valid and opens in viewers; only
  the feature count reveals the drop.
- Pure raster pipelines (`PNG`/`JPEG`/`WebP` tiles) were unaffected: first-wins
  is correct for raster (each tile carries one image).

### Fix / pattern

**Both tiers: group payloads by tile ID first, then resolve each group to one blob.**

- Light tier (`pmtiles/_agg_light.py`): `_assemble_archive` now accumulates all
  non-null payloads into `tileid_payloads: dict[int, list[bytes]]`. For vector
  (`TileType.MVT`) groups with more than one blob, `_merge_mvt_blobs` decodes each
  blob with `mapbox_vector_tile.decode`, unions `features` per layer name, and
  re-encodes. For raster groups, first blob wins (unchanged). Single-blob groups
  take a fast `return blobs[0]` path — zero decode/encode overhead for the common
  sparse case.
- Heavy tier (`pmtiles/PMTiles_Agg.scala`): `eval()` builds a `LinkedHashMap[tileId →
  ArrayBuffer[(z,x,y,bytes)]]`, then resolves each group: MVT multi-blob paths call
  `mergeMvtPayloads` (decode via `MvtDecoder`, union features per layer, re-encode
  by concatenating raw protobuf bytes per-layer — valid because MVT layer fields are
  `repeated`, so protobuf concatenation merges them). Raster: first blob only.

**Merge cost** (light tier, measured): for typical tile payloads (<50 KB,
<100 features), the decode+encode round-trip costs < 0.5 ms per tile. The
single-blob fast path costs ~0 (a list-length check). No `benchmarking.mdx`
update is warranted unless a cluster bench shows measurable regression on
dense-data real workloads.

### Applicability matrix

| Scope | Status |
|---|---|
| `gbx_pmtiles_agg` light tier (`_agg_light.py`) | FIXED — merge-per-tile, raster first-wins preserved |
| `gbx_pmtiles_agg` heavy tier (`PMTiles_Agg.scala`) | FIXED — group-by-tileid + MVT merge, directory dedup |
| All other GeoBrix `_agg` functions | AUDITED — no other `_agg` shares the drop-on-collision flaw; all others combine correctly by construction (spatial union, set union, etc.) |
| Raster pipelines (`PNG`/`JPEG`/`WebP`) | Unchanged — first-wins is correct; one raster image per tile |

**Pattern scope:** any PMTiles aggregator or tile packer that accumulates
`(z, x, y, bytes)` rows must group by tile ID before writing the directory.
The flaw is latent in any "accumulate-then-flush" design that doesn't deduplicate
tile IDs before writing.

### Evidence

- 5 new light-tier unit tests (`test_agg_light_core.py`) cover: single MVT blob
  pass-through, two-blob merge with feature union, three-blob merge with two
  layers, raster first-wins unchanged, empty-group empty-archive.
- 3 new cross-tier parity tests (`test_parity_pmtiles_merge.py`) confirm light
  and heavy produce matching feature counts and layer names for multi-feature tiles.
- 3 new Scala unit tests (`PMTiles_AggTest.scala`) cover: multi-feature vector
  merge, duplicate-tileid dedup, mixed raster first-wins.
- 2 `MvtDecoderTest.scala` tests confirm the decode path used by `mergeMvtPayloads`.
- 278 vectorx + pmtiles Scala tests green (including `st_asmvt`, `st_asmvt_pyramid`).
- **No measured speedup** — this is a correctness fix.

### Canonical code refs

- Light: `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py` —
  `_merge_mvt_blobs`, `_assemble_archive`
- Heavy: `src/main/scala/com/databricks/labs/gbx/pmtiles/PMTiles_Agg.scala` —
  `eval()`, `mergeMvtPayloads`; `MvtDecoder.scala`; `MvtWriter.scala`

---

## Lesson 2 — GDAL gotcha: `GetMemFileBuffer` returns NULL for OGR-written `/vsimem/` files

### Problem

During development of the heavy MVT merge path, the first attempt used the OGR
MVT creation driver writing to a `/vsimem/` path, then called `gdal.GetMemFileBuffer`
to retrieve the bytes. This silently returned `null`, producing an empty byte array
for every tile — no exception, no GDAL error, just empty output.

### Root cause

`gdal.GetMemFileBuffer(path)` only returns the buffer for files that were
**explicitly created** via `gdal.FileFromMemBuffer(path, bytes)`. It does NOT work
for files that an OGR or GDAL driver created by writing to a `/vsimem/` path
(e.g. an OGR `CreateDataSource("/vsimem/foo")` output). The GDAL Java bindings
have no mechanism to retrieve driver-written vsimem bytes via `GetMemFileBuffer`;
the function returns null for these paths.

This is a GDAL SWIG binding limitation, not a file system limitation. The
`/vsimem/` virtual file system itself is fine; the bytes are there. The Java
binding's `GetMemFileBuffer` simply doesn't enumerate or read from paths it
didn't register via `FileFromMemBuffer`.

### Fix / pattern

**Use a real temp directory for OGR driver output, then read with standard Java file I/O.**

```scala
// Create a temp PARENT dir; let OGR create the directory structure inside it.
val tmpParent: Path = Files.createTempDirectory("gbx_mvt_par_")
val tmpRoot: Path   = tmpParent.resolve("tile")
val ds = driver.CreateDataSource(tmpRoot.toAbsolutePath.toString, createOpts)
// ... write features, SyncToDisk, ds.delete() ...
// Read the emitted .pbf with standard Java I/O (not GetMemFileBuffer):
val pbfFile = Paths.get(rootPath, "0", "0", "0.pbf")
val bytes = if (Files.exists(pbfFile)) Files.readAllBytes(pbfFile) else Array.emptyByteArray
// Clean up with Files.walkFileTree.
```

For **reading** back a known byte array (the inverse direction — e.g. feeding bytes
into an OGR driver for decode), `FileFromMemBuffer` + `driver.Open("/vsimem/path")`
works correctly. The gotcha only applies to the **creation** direction (driver writes
to vsimem → caller tries to read back with `GetMemFileBuffer`).

### Where this applies

| Use case | Safe approach |
|---|---|
| Read MVT bytes into OGR (decode) | `FileFromMemBuffer` + `driver.Open(vsimemPath)` — works; see `MvtDecoder.scala` |
| Write MVT bytes from OGR (encode) | Real temp dir + `Files.readAllBytes(pbfFile)` — see `MvtWriter.scala` |
| Any OGR driver writing to `/vsimem/` | Same: temp dir, read with Java I/O, clean up with `walkFileTree` |
| GDAL raster drivers writing to `/vsimem/` | Same: temp dir or use GDAL's `/vsimem/` with `ReadDirRecursive` + per-file read |

The `MvtWriter.scala` scaladoc describes this explicitly:
> "Intermediate state lives in a Java temp directory (not `/vsimem/`) because
> `gdal.GetMemFileBuffer` only works for `FileFromMemBuffer`-created files —
> it returns null for driver-written vsimem files, silently dropping the output."

### Canonical code refs

- `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtWriter.scala` — temp
  dir pattern, `Files.readAllBytes`, `walkFileTree` cleanup
- `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtDecoder.scala` —
  counter-example: `FileFromMemBuffer` + `driver.Open` for the read direction
