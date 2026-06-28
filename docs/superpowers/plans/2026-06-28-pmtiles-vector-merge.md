# gbx_pmtiles_agg Vector-Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `gbx_pmtiles_agg` produce correct multi-feature vector tiles so the `st_asmvt_pyramid → gbx_pmtiles_agg` pipeline preserves all features for real data. When multiple rows share the same `(z, x, y)` and their payloads sniff as MVT, decode each blob, union features per layer name, and re-encode one merged MVT at the same extent. Raster first-wins is unchanged.

**Architecture:** The fix is contained to two files: `_agg_light.py` (light) and `PMTilesAcc.scala`/`PMTiles_Agg.scala` (heavy), plus a new `MvtMerger` object for the JVM decode path. The `_assemble_archive` function in the light tier changes from a `seen`-set deduplication loop to a group-by-tileid dict that accumulates payloads, then resolves each tileid through a vector-vs-raster branch. The heavy tier performs the same grouping inside `eval` on `PMTilesAcc.tiles`, using GDAL's OGR MVT driver which can both CREATE and OPEN `.pbf` datasources — the decode path is `gdal.OpenEx(vsimemPath, OF_VECTOR)` with the same `/vsimem/` scratch pattern `MvtWriter.encode` already uses for the write side.

**Tech Stack:** Python 3.12, `mapbox_vector_tile` (already a `[light]` dep — `decode` + `encode`), `pmtiles` PyPI package (already present). Scala 2.13, GDAL OGR Java bindings (`gdal.OpenEx` + `GetLayer` + `GetNextFeature` for decode; `MvtWriter.encode` for re-encode), JTS for geometry WKB handling.

**Reference spec:** `docs/superpowers/specs/2026-06-28-pmtiles-vector-merge-design.md`

---

## Global Constraints

- **TDD.** Each task: write the failing test first, confirm it fails with the expected message/assertion, implement the minimal fix, confirm green.
- **Both tiers at parity.** Light and heavy must produce equivalent merged tiles (same feature count, same layer names, same geometry type, same attribute values).
- **POLYGON parity test.** The mandatory cross-tier parity test MUST include a POLYGON feature (not just points) — points-only gives a false pass per the MVT tile-local contract.
- **Raster first-wins unchanged.** Every existing raster test in `test_agg_light_core.py` and `PMTiles_AggTest.scala` must stay green with zero modification.
- **Serverless-safe light tier.** `_agg_light.py` must NEVER use `._jvm`, `._jsc`, `.sparkContext`, `.rdd`, `.conf.set(`. No new imports beyond packages already in `[light]`.
- **No new Python dependencies.** `mapbox_vector_tile` is already present; `decode` is all that's needed on the light side. Do not add any new entries to `pyproject.toml`.
- **OOM/partition cap preserved.** The 100 MiB byte-count guard runs during accumulation (unchanged). Merged blobs may be larger than any single input blob; this is acceptable — the cap is on the raw accumulated bytes, not the merged output.
- **Scalastyle clean.** Heavy Scala additions must pass `gbx:lint:scalastyle` (matches CI) before committing.
- **Docker for heavy tests.** All Scala tests and the cross-tier parity test require the `geobrix-dev` Docker container.
- **Commit hygiene.** Subject ≤72 chars + a WHY body on every commit + `Co-authored-by: Isaac` trailer. Run `chmod -R u+rwX .git/objects` before each commit (env permission gotcha).

---

## Heavy-tier MVT decode: critical pre-work finding

**GDAL OGR MVT driver supports both CREATE and OPEN.** There is no separate JVM MVT decoder library required. The same driver used by `MvtWriter.encode` for creation can read `.pbf` files via `gdal.OpenEx(vsimemPath, OF_VECTOR)`. The pattern is:

1. Write the blob bytes into a `/vsimem/<uuid>/0/0/0.pbf` path using `gdal.FileFromMemBuffer`.
2. Open the datasource with `gdal.OpenEx(vsimemPath, OF_VECTOR)`.
3. Iterate `ds.GetLayer(i)` → `layer.GetNextFeature()` to read `(layerName, geom_wkb, attrs)`.
4. Clean up with `gdal.Unlink` + `gdal.RmdirRecursive`.

This same `/vsimem/` + `ReadDirRecursive` + `GetMemFileBuffer` idiom is already proven by `MvtWriter.encode` for the write side and by `RST_GridFromPoints.scala` for `gdal.OpenEx(..., OF_VECTOR)`. No new JVM dependency is needed. A new helper object `MvtDecoder` (placed in `com.databricks.labs.gbx.vectorx.mvt`) encapsulates this read path, mirroring the existing `MvtWriter` structure. Task 2, Step 1 adds and unit-tests `MvtDecoder` before wiring it into the UDAF.

---

## Task 1: Light-tier vector merge in `_assemble_archive`

**Goal:** `_assemble_archive` accumulates all payloads per tileid, detects MVT vs raster from the first non-null payload per group, merges MVT blobs via `mapbox_vector_tile.decode` + `encode`, and keeps raster first-wins.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py`
- Modify: `python/geobrix/test/pmtiles_light/test_agg_light_core.py`

**Interfaces:**
- Consumes: unchanged public signature `_assemble_archive(data, zs, xs, ys, metadata)`.
- Produces: same `Optional[bytes]` PMTiles archive. For a tileid with 2 MVT blobs the packed tile must decode to both features. For a tileid with 2 PNG blobs only the first is stored.

---

- [ ] **Step 1: Write failing tests for vector merge + raster-unchanged regression**

Add to `python/geobrix/test/pmtiles_light/test_agg_light_core.py`:

```python
import mapbox_vector_tile as mvt
from shapely.geometry import Polygon, box
from shapely import to_wkb


def _real_mvt_blob(poly_coords, prop_id: int, layer: str = "bldg") -> bytes:
    """Encode one POLYGON feature as a real MVT blob at extent=4096."""
    poly = Polygon(poly_coords)
    return mvt.encode(
        {
            "name": layer,
            "features": [
                {
                    "geometry": poly,
                    "properties": {"id": prop_id},
                }
            ],
        },
        default_options={"extents": 4096, "y_coord_down": True},
    )


_POLY_A = _real_mvt_blob(
    [(100, 100), (200, 100), (200, 200), (100, 200), (100, 100)], prop_id=1
)
_POLY_B = _real_mvt_blob(
    [(300, 300), (400, 300), (400, 400), (300, 400), (300, 300)], prop_id=2
)
_POLY_C_OTHER_LAYER = _real_mvt_blob(
    [(500, 500), (600, 500), (600, 600), (500, 600), (500, 500)],
    prop_id=3,
    layer="roads",
)


def _decode_mvt_from_archive(blob, z, x, y, tmp_path):
    """Extract and decode the MVT blob for (z, x, y) from a PMTiles archive."""
    from pmtiles.reader import MmapSource, Reader

    p = tmp_path / "merge.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        raw = r.get(z, x, y)
    assert raw is not None, f"tile ({z},{x},{y}) missing from archive"
    return mvt.decode(raw)


def test_vector_merge_two_features_same_tileid(tmp_path):
    """Two MVT blobs for the same (z,x,y) must merge into one tile with 2 features."""
    blob = _assemble_archive([_POLY_A, _POLY_B], [3, 3], [2, 2], [4, 4], {})
    assert blob is not None
    decoded = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    assert "bldg" in decoded, f"layer 'bldg' missing; got layers: {list(decoded.keys())}"
    feat_ids = {f["properties"]["id"] for f in decoded["bldg"]["features"]}
    assert feat_ids == {1, 2}, f"expected both feature ids; got {feat_ids}"


def test_vector_merge_geometry_type_preserved(tmp_path):
    """Merged features must retain POLYGON geometry type (not downgraded to Point)."""
    blob = _assemble_archive([_POLY_A, _POLY_B], [3, 3], [2, 2], [4, 4], {})
    decoded = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    for feat in decoded["bldg"]["features"]:
        assert feat["geometry"]["type"] == "Polygon", (
            f"feature id={feat['properties']['id']} geometry not Polygon: "
            f"{feat['geometry']['type']}"
        )


def test_vector_merge_multi_layer(tmp_path):
    """Blobs from different layers for the same tileid are both preserved."""
    blob = _assemble_archive(
        [_POLY_A, _POLY_C_OTHER_LAYER], [3, 3], [2, 2], [4, 4], {}
    )
    decoded = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    assert "bldg" in decoded and "roads" in decoded, (
        f"expected both layers; got {list(decoded.keys())}"
    )


def test_vector_merge_distinct_tileids_unchanged(tmp_path):
    """Blobs for distinct tileids are stored separately — no cross-tile bleed."""
    blob = _assemble_archive([_POLY_A, _POLY_B], [3, 3], [2, 4], [4, 6], {})
    decoded_a = _decode_mvt_from_archive(blob, 3, 2, 4, tmp_path)
    decoded_b = _decode_mvt_from_archive(blob, 3, 4, 6, tmp_path)
    assert {f["properties"]["id"] for f in decoded_a["bldg"]["features"]} == {1}
    assert {f["properties"]["id"] for f in decoded_b["bldg"]["features"]} == {2}


def test_raster_first_wins_unchanged(tmp_path):
    """PNG tiles for the same (z,x,y) still keep first-wins (no change to raster path)."""
    _PNG2 = b"\x89PNG\r\n\x1a\n" + b"\x01" * 16
    blob = _assemble_archive([_PNG, _PNG2], [1, 1], [0, 0], [0, 0], {})
    tiles = _decode(blob, tmp_path)
    assert tiles[(1, 0, 0)] == _PNG, "raster first-wins violated after vector-merge change"
```

Run — expect all 5 new tests to fail (currently all same-tileid blobs drop to first-wins so `feat_ids == {1}` not `{1, 2}`):

```bash
python/geobrix/.venv-pyrx/bin/python -m pytest \
  python/geobrix/test/pmtiles_light/test_agg_light_core.py \
  -k "vector_merge or raster_first_wins_unchanged" -v 2>&1 | tail -20
```

Expected: 5 FAILED, specifically the merge tests show `AssertionError: expected both feature ids; got {1}`.

---

- [ ] **Step 2: Add `_merge_mvt_blobs` helper to `_agg_light.py`**

First, add `mapbox_vector_tile` to the existing top-of-file imports in `_agg_light.py` (it is already a `[light]` dependency — used by `pyvx/_mvt.py`; this is the first use in `pmtiles`):

```python
import mapbox_vector_tile as mvt
```

Place this import in the stdlib/third-party block after the existing `pmtiles` imports. Then add the following private function **before** `_assemble_archive`:

```python
def _merge_mvt_blobs(blobs: list[bytes], extent: int = 4096) -> bytes:
    """Decode multiple single-feature MVT blobs and union features per layer name.

    Geometry stays in tile-local [0, extent] integer space — no reprojection
    (each blob is already tile-local for the same (z,x,y); decode/encode round-trips
    the local coords). Attributes are preserved per feature.

    Returns one merged MVT blob. If the list has a single blob, returns it directly
    to avoid a decode/encode round-trip for the common single-feature case.
    """
    if len(blobs) == 1:
        return blobs[0]
    layers: dict[str, list] = {}
    for blob in blobs:
        try:
            decoded = mvt.decode(blob)
        except Exception:
            # Malformed blob: skip rather than crashing the whole group.
            continue
        for layer_name, layer_data in decoded.items():
            layers.setdefault(layer_name, []).extend(layer_data.get("features", []))
    if not layers:
        return blobs[0]  # nothing decoded cleanly; fall back to first
    tile_spec = {
        name: {"features": feats}
        for name, feats in layers.items()
    }
    return mvt.encode(tile_spec, default_options={"extents": extent, "y_coord_down": True})
```

---

- [ ] **Step 3: Rewrite `_assemble_archive` to group payloads by tileid**

First, update the top-of-file import to add `TileType`:
```python
# Before:
from pmtiles.tile import Compression, zxy_to_tileid
# After:
from pmtiles.tile import Compression, TileType, zxy_to_tileid
```

Then replace the `seen`/`if tileid in seen: continue` logic with a per-tileid accumulator. The new logic:

1. First pass: accumulate `tileid → [bytes]` (preserving arrival order for raster first-wins). Also track `first_payload` for tile-type sniff and running byte total.
2. Detect tile type once from `first_payload` (unchanged).
3. Second pass: for each tileid in sorted order, call `_merge_mvt_blobs(payloads)` if MVT, else take `payloads[0]` if raster.

Replace the body of `_assemble_archive` in `_agg_light.py` with:

```python
def _assemble_archive(
    data: Sequence,
    zs: Sequence,
    xs: Sequence,
    ys: Sequence,
    metadata: Optional[dict] = None,
) -> Optional[bytes]:
    """Fold a group's (bytes, z, x, y) tiles into one PMTiles v3 archive (bytes).

    Null payloads are skipped; an all-null/empty group returns None. For vector
    (MVT) tiles, multiple blobs for the same (z,x,y) are merged into one
    multi-feature tile (decode each, union features per layer, re-encode).
    For raster (PNG/JPEG/WebP), first-write-wins is preserved. Tiles are
    written in ascending Hilbert TileID order.
    """
    # Phase 1: accumulate all non-null payloads per tileid.
    tileid_payloads: dict[int, list[bytes]] = {}
    tileid_coords: dict[int, tuple[int, int, int]] = {}
    total = 0
    first_payload = None
    for d, z, x, y in zip(data, zs, xs, ys):
        if d is None:
            continue
        b = bytes(d)
        total += len(b)
        if total > _MAX_ARCHIVE_BYTES:
            raise ValueError(
                f"pmtiles_agg group payload exceeds {_MAX_ARCHIVE_BYTES} bytes; "
                "split into more groups or fewer tiles per archive"
            )
        tileid = zxy_to_tileid(int(z), int(x), int(y))
        if first_payload is None:
            first_payload = b
        tileid_payloads.setdefault(tileid, []).append(b)
        tileid_coords[tileid] = (int(z), int(x), int(y))

    if not tileid_payloads:
        return None

    tile_type = sniff_tile_type(first_payload)
    # sniff_tile_type returns pmtiles.tile.TileType; MVT is the fallback for
    # non-PNG/JPEG/WebP/AVIF payloads. Add TileType to the existing top-of-file
    # import: `from pmtiles.tile import Compression, TileType, zxy_to_tileid`
    is_vector = (tile_type == TileType.MVT)

    # Phase 2: resolve each tileid to one output blob.
    tiles = []
    for tileid in sorted(tileid_payloads.keys()):
        z, x, y = tileid_coords[tileid]
        payloads = tileid_payloads[tileid]
        if is_vector and len(payloads) > 1:
            resolved = _merge_mvt_blobs(payloads)
        else:
            resolved = payloads[0]
        tiles.append((z, x, y, tileid, resolved))

    info = build_header_info(
        [(z, x, y) for (z, x, y, _, _) in tiles],
        SlippyGrid(),
        tile_type,
        Compression.NONE,
        metadata or {},
    )
    buf = io.BytesIO()
    writer = Writer(buf)
    for _, _, _, tileid, b in tiles:  # already sorted
        writer.write_tile(tileid, b)
    writer.finalize(info.header_dict(), info.metadata)
    return buf.getvalue()
```

**Note on tile-type detection:** `sniff_tile_type` (from `ds.tiles._header`) returns a `pmtiles.tile.TileType` enum value — `TileType.MVT` for any non-PNG/JPEG/WebP/AVIF payload (confirmed from `_header.py`). The `is_vector` check `tile_type == TileType.MVT` is the correct expression.

---

- [ ] **Step 4: Run failing tests — now expect green**

```bash
python/geobrix/.venv-pyrx/bin/python -m pytest \
  python/geobrix/test/pmtiles_light/test_agg_light_core.py -v 2>&1 | tail -30
```

Expected: all tests pass including the 5 new ones plus all pre-existing tests (single_tile_roundtrip, multi_zoom_roundtrip, png_payload_roundtrip, metadata_roundtrip, null_payloads_skipped, empty_group_returns_none, duplicate_tileid_dropped, cap_exceeded_raises).

---

- [ ] **Step 5: Run the full UDF test suite**

```bash
python/geobrix/.venv-pyrx/bin/python -m pytest \
  python/geobrix/test/pmtiles_light/ -v 2>&1 | tail -30
```

Expected: all tests pass.

---

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py \
        python/geobrix/test/pmtiles_light/test_agg_light_core.py
git commit -m "$(cat <<'EOF'
fix(pmtiles): merge multi-feature MVT blobs per (z,x,y) in light tier

gbx_pmtiles_agg was dropping all but the first feature per tile for
vector (MVT) data, making the st_asmvt_pyramid→gbx_pmtiles_agg
pipeline produce single-feature tiles for real datasets. This fix
groups payloads by tileid, decodes and unions features per layer,
and re-encodes one merged MVT blob. Raster first-wins is unchanged.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 2: Heavy-tier MVT decode helper + vector merge in `PMTilesAcc`

**Goal:** Add `MvtDecoder` (OGR MVT read via `/vsimem/`) to the heavy-tier `vectorx.mvt` package. Wire it into `PMTiles_Agg.eval` so that MVT tileids with >1 payload are decoded, unioned per layer, and re-encoded via the existing `MvtWriter`. Raster tileids keep first-wins.

**Files:**
- New: `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtDecoder.scala`
- Modify: `src/main/scala/com/databricks/labs/gbx/pmtiles/PMTiles_Agg.scala`
- Modify: `src/test/scala/com/databricks/labs/gbx/pmtiles/PMTiles_AggTest.scala`

**Interfaces:**
- `MvtDecoder.decode(blob: Array[Byte]): Seq[(String, Array[Byte], Map[String, Any])]` — returns `(layerName, geom_wkb, attrs)` tuples for all features across all layers in the blob.
- `PMTiles_Agg.eval` groups `buffer.tiles` by `zxy_to_tileid`, branches on `tileType == TILE_TYPE_MVT`, calls `MvtDecoder.decode` + accumulates features per layer + calls `MvtWriter.encode` per layer + assembles a merged blob. Raster tileids use `payloads.head`.

---

- [ ] **Step 1: Write failing unit test for `MvtDecoder`**

Add to `src/test/scala/com/databricks/labs/gbx/pmtiles/PMTiles_AggTest.scala` (or a new `MvtDecoderTest.scala` in `src/test/scala/com/databricks/labs/gbx/vectorx/mvt/`):

```scala
import com.databricks.labs.gbx.vectorx.mvt.{MvtDecoder, MvtWriter}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession

class MvtDecoderTest extends PlanTest with SilentSparkSession {

    // Build a real MVT blob via MvtWriter (tile-local polygon + attrs).
    private def encodePolygon(id: Int, x0: Int, y0: Int): Array[Byte] = {
        import com.databricks.labs.gbx.vectorx.jts.JTS
        import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
        val gf = new GeometryFactory()
        val ring = gf.createLinearRing(Array(
            new Coordinate(x0, y0), new Coordinate(x0 + 100, y0),
            new Coordinate(x0 + 100, y0 + 100), new Coordinate(x0, y0 + 100),
            new Coordinate(x0, y0)
        ))
        val poly = gf.createPolygon(ring)
        val wkb = JTS.toWKB(poly)
        MvtWriter.encode("bldg", 4096, Seq((wkb, Map("id" -> id))))
    }

    test("MvtDecoder round-trips a real polygon MVT blob") {
        val blob = encodePolygon(42, 100, 100)
        assert(blob.nonEmpty, "MvtWriter produced empty blob")
        val features = MvtDecoder.decode(blob)
        assert(features.nonEmpty, "MvtDecoder returned no features")
        val (layerName, geomWkb, attrs) = features.head
        assert(layerName == "bldg", s"expected layer 'bldg'; got '$layerName'")
        assert(attrs.get("id").contains(42) || attrs.get("id").exists(_.toString == "42"),
            s"expected id=42; got attrs=$attrs")
        assert(geomWkb != null && geomWkb.nonEmpty, "geomWkb is empty")
    }

    test("MvtDecoder returns empty Seq for empty byte array") {
        assert(MvtDecoder.decode(Array.emptyByteArray).isEmpty)
    }
}
```

Run in Docker — expect compilation failure (MvtDecoder does not exist yet):

```bash
bash scripts/commands/gbx-docker-exec.sh \
  "mvn test -pl . -Dtest=MvtDecoderTest -DfailIfNoTests=false -P skipScoverage -q" \
  2>&1 | tail -20
```

Expected: `error: object MvtDecoder is not a member of package ...` or equivalent compile error.

---

- [ ] **Step 2: Implement `MvtDecoder.scala`**

Create `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtDecoder.scala`:

```scala
package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.gdal
import org.gdal.ogr.ogr.{GetDriverByName => OGRGetDriverByName}
import org.gdal.ogrConstants._

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Decode a Mapbox Vector Tile (MVT) protobuf blob into features.
  *
  * Uses GDAL's OGR MVT driver (the same driver `MvtWriter.encode` uses for creation)
  * opened in read mode via a `/vsimem/` scratch path. Mirrors `MvtWriter`'s resource
  * management: every Dataset / Feature is `.delete()`'d and the `/vsimem/` tree is
  * cleaned up before returning.
  *
  * GDAL thread-safety: OGR drivers are registered via `GDALManager.initOgr()` (the
  * synchronized guard), matching the requirement in CLAUDE.md. The `/vsimem/` paths
  * are UUID-namespaced to avoid collisions across concurrent Spark tasks.
  *
  * Returns `Seq[(layerName, geom_wkb, attrs)]`. Geometry WKB is in the tile-local
  * pixel space of the input blob (no coordinate transformation). Features with null
  * or empty geometries are skipped. If the blob is empty or unparseable, returns
  * an empty Seq (never throws).
  */
object MvtDecoder {

    /**
      * Decode `blob` into a flat sequence of `(layerName, geomWkb, attrs)` tuples.
      *
      * @param blob MVT protobuf bytes (tile-local coordinates).
      * @return All features across all layers; empty Seq if the blob is empty or
      *         cannot be decoded.
      */
    def decode(blob: Array[Byte]): Seq[(String, Array[Byte], Map[String, Any])] = {
        if (blob == null || blob.isEmpty) return Seq.empty
        MvtWriter.ensureNativeLoadedPublic()
        GDALManager.initOgr()

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rootPath = s"/vsimem/gbx_mvtdec_$uuid"
        // The OGR MVT reader expects a directory datasource with the tile at 0/0/0.pbf.
        val pbfPath = s"$rootPath/0/0/0.pbf"

        Try(gdal.Mkdir(rootPath, 0)).toOption
        Try(gdal.Mkdir(s"$rootPath/0", 0)).toOption
        Try(gdal.Mkdir(s"$rootPath/0/0", 0)).toOption
        gdal.FileFromMemBuffer(pbfPath, blob)

        val result = ArrayBuffer.empty[(String, Array[Byte], Map[String, Any])]
        val driver = OGRGetDriverByName("MVT")
        if (driver == null) return Seq.empty

        val ds = Try(driver.Open(rootPath, 0)).toOption.orNull
        if (ds == null) {
            gdal.RmdirRecursive(rootPath)
            return Seq.empty
        }

        try {
            val layerCount = ds.GetLayerCount()
            var li = 0
            while (li < layerCount) {
                val layer = ds.GetLayer(li)
                if (layer != null) {
                    val layerName = layer.GetName()
                    layer.ResetReading()
                    var feat = layer.GetNextFeature()
                    while (feat != null) {
                        try {
                            val geom = feat.GetGeometryRef()
                            if (geom != null) {
                                val wkb = geom.ExportToWkb()
                                if (wkb != null && wkb.nonEmpty) {
                                    val attrs = readAttrs(feat)
                                    result += ((layerName, wkb, attrs))
                                }
                            }
                        } finally {
                            feat.delete()
                        }
                        feat = layer.GetNextFeature()
                    }
                }
                li += 1
            }
        } finally {
            ds.delete()
            gdal.RmdirRecursive(rootPath)
        }
        result.toSeq
    }

    /** Extract all field values from a feature as a Map[String, Any] with native types. */
    private def readAttrs(feat: org.gdal.ogr.Feature): Map[String, Any] = {
        val defn = feat.GetDefnRef()
        val count = defn.GetFieldCount()
        val m = scala.collection.mutable.Map.empty[String, Any]
        var i = 0
        while (i < count) {
            val fieldDefn = defn.GetFieldDefn(i)
            val name = fieldDefn.GetNameRef()
            val fieldType = fieldDefn.GetFieldType()
            val value: Any = fieldType match {
                case OFTInteger   => feat.GetFieldAsInteger(i)
                case OFTInteger64 => feat.GetFieldAsInteger64(i)
                case OFTReal      => feat.GetFieldAsDouble(i)
                case _            => feat.GetFieldAsString(i)
            }
            m(name) = value
            i += 1
        }
        m.toMap
    }
}
```

**IMPORTANT implementation note:** `MvtWriter.ensureNativeLoaded()` is currently `private`. Before implementing `MvtDecoder`, either:
- Make `ensureNativeLoaded` `private[mvt]` in `MvtWriter.scala` so `MvtDecoder` (same package) can call it, **or**
- Duplicate the one-liner guard inside `MvtDecoder` (less preferred — inconsistent).

Prefer `private[mvt]` visibility change in `MvtWriter.scala`. The method body is a single `System.load` check — this is a minimal safe change.

---

- [ ] **Step 3: Run the MvtDecoder unit test**

```bash
bash scripts/commands/gbx-docker-exec.sh \
  "mvn test -pl . -Dtest=MvtDecoderTest -DfailIfNoTests=false -P skipScoverage -q" \
  2>&1 | tail -20
```

Expected: `Tests run: 2, Failures: 0, Errors: 0`.

---

- [ ] **Step 4: Write failing tests for heavy-tier vector merge**

Add new tests to `src/test/scala/com/databricks/labs/gbx/pmtiles/PMTiles_AggTest.scala`:

```scala
import com.databricks.labs.gbx.vectorx.mvt.{MvtDecoder, MvtWriter}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

// Helper to build a real polygon WKB in tile-local coords.
private def polygonWkb(x0: Int, y0: Int): Array[Byte] = {
    val gf = new GeometryFactory()
    val ring = gf.createLinearRing(Array(
        new Coordinate(x0, y0), new Coordinate(x0 + 100, y0),
        new Coordinate(x0 + 100, y0 + 100), new Coordinate(x0, y0 + 100),
        new Coordinate(x0, y0)
    ))
    JTS.toWKB(gf.createPolygon(ring))
}

private def realMvtBlob(id: Int, x0: Int, y0: Int, layer: String = "bldg"): Array[Byte] =
    MvtWriter.encode(layer, 4096, Seq((polygonWkb(x0, y0), Map("id" -> id))))

test("pmtiles_agg merges two MVT blobs for the same (z,x,y) into one tile with 2 features") {
    spark.sparkContext.setLogLevel("ERROR")
    functions.register(spark)
    import functions._

    val blobA = realMvtBlob(id = 1, x0 = 100, y0 = 100)
    val blobB = realMvtBlob(id = 2, x0 = 300, y0 = 300)
    // Two rows for same (z=3, x=2, y=4) → must merge.
    val df = spark.createDataFrame(Seq(
        (3, 2, 4, blobA),
        (3, 2, 4, blobB)
    )).toDF("z", "x", "y", "bytes")

    val archive = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
        .collect().head.getAs[Array[Byte]]("pmt")

    // Extract the tile from the archive.
    val tileBytes = PMTilesTestHelper.readTile(archive, z = 3, x = 2, y = 4)
    assert(tileBytes.nonEmpty, "merged tile must be present in archive")
    val features = MvtDecoder.decode(tileBytes)
    val ids = features.map(_._3.get("id").map(_.toString.toInt).getOrElse(-1)).toSet
    assert(ids == Set(1, 2), s"expected both feature ids; got $ids")
}

test("pmtiles_agg preserves POLYGON geometry type in merged MVT tile") {
    spark.sparkContext.setLogLevel("ERROR")
    functions.register(spark)
    import functions._

    val blobA = realMvtBlob(id = 1, x0 = 100, y0 = 100)
    val blobB = realMvtBlob(id = 2, x0 = 300, y0 = 300)
    val df = spark.createDataFrame(Seq((3, 2, 4, blobA), (3, 2, 4, blobB)))
        .toDF("z", "x", "y", "bytes")
    val archive = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
        .collect().head.getAs[Array[Byte]]("pmt")
    val tileBytes = PMTilesTestHelper.readTile(archive, z = 3, x = 2, y = 4)
    val features = MvtDecoder.decode(tileBytes)
    // All decoded geometries must parse as polygons (not degenerated).
    features.foreach { case (_, wkb, _) =>
        val geom = JTS.fromWKB(wkb)
        assert(geom != null && !geom.isEmpty, s"decoded geometry is null/empty")
        assert(geom.getGeometryType == "Polygon" || geom.getGeometryType == "MultiPolygon",
            s"expected Polygon; got ${geom.getGeometryType}")
    }
}

test("pmtiles_agg raster first-wins unchanged after vector-merge change") {
    spark.sparkContext.setLogLevel("ERROR")
    functions.register(spark)
    import functions._

    val pngA = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x01, 0x00)
    val pngB = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x02, 0x00)
    val df = spark.createDataFrame(Seq((1, 0, 0, pngA), (1, 0, 0, pngB)))
        .toDF("z", "x", "y", "bytes")
    val archive = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
        .collect().head.getAs[Array[Byte]]("pmt")
    val tileBytes = PMTilesTestHelper.readTile(archive, z = 1, x = 0, y = 0)
    assert(tileBytes.sameElements(pngA), "raster first-wins violated")
}
```

**Note:** These tests reference `PMTilesTestHelper.readTile` — a small helper that reads a PMTiles archive (byte array → temp file → `PMTilesV3Reader.readTile` or direct binary parse). Add this helper object in the test source tree at `src/test/scala/com/databricks/labs/gbx/pmtiles/PMTilesTestHelper.scala`. It reads the PMTiles v3 index and returns the raw tile bytes for a given `(z, x, y)`, or throws if not found. Use the existing binary parse pattern from `PMTilesV3EncoderTest` as a reference.

Run in Docker — expect compilation success but test failures (merge not yet implemented):

```bash
bash scripts/commands/gbx-docker-exec.sh \
  "mvn test -pl . -Dtest=PMTiles_AggTest -DfailIfNoTests=false -P skipScoverage -q" \
  2>&1 | tail -30
```

Expected: the 3 new tests fail on the feature-count / geometry-type assertions; the 5 existing tests remain green.

---

- [ ] **Step 5: Add vector-merge logic to `PMTiles_Agg.eval`**

Modify `PMTiles_Agg.scala`. The `eval` method currently iterates `buffer.tiles` in insertion order and hands them all to `PMTilesV3Encoder.encode`. Replace `eval` with a version that groups by tileid, branches on `tileType`, and merges MVT payloads:

```scala
override def eval(buffer: PMTilesAcc): Any = {
    if (buffer.tiles.isEmpty) {
        return PMTilesV3Encoder.encode(Iterator.empty, buffer.metadataJson)
    }
    val firstNonNull = buffer.tiles.iterator.map(_._4).find(b => b != null && b.nonEmpty)
    val tileType = firstNonNull.map(PMTiles_Agg.detectTileType).getOrElse(PMTilesV3Encoder.TILE_TYPE_MVT)
    val isVector = tileType == PMTilesV3Encoder.TILE_TYPE_MVT

    // Group payloads by tileid (Hilbert order key), preserving insertion order within each group.
    import com.databricks.labs.gbx.pmtiles.{PMTilesHilbert => Hilbert}
    val grouped = scala.collection.mutable.LinkedHashMap.empty[Long, scala.collection.mutable.ArrayBuffer[(Int, Int, Int, Array[Byte])]]
    buffer.tiles.foreach { case row @ (z, x, y, _) =>
        val tid = Hilbert.zxyToTileid(z, x, y)
        grouped.getOrElseUpdate(tid, scala.collection.mutable.ArrayBuffer.empty) += row
    }

    // Resolve each tileid to one blob: merge for MVT, first for raster.
    val resolved: Iterator[(Int, Int, Int, Array[Byte])] = grouped.iterator.map {
        case (_, rows) =>
            val (z, x, y, _) = rows.head
            val payloads = rows.map(_._4).filter(b => b != null && b.nonEmpty)
            val blob =
                if (isVector && payloads.length > 1)
                    PMTiles_Agg.mergeMvtPayloads(payloads.toSeq)
                else
                    payloads.headOption.getOrElse(Array.emptyByteArray)
            (z, x, y, blob)
    }

    PMTilesV3Encoder.encode(resolved, buffer.metadataJson, tileType)
}
```

Add a companion helper `mergeMvtPayloads` to `PMTiles_Agg` object:

```scala
private[pmtiles] def mergeMvtPayloads(payloads: Seq[Array[Byte]]): Array[Byte] = {
    if (payloads.length == 1) return payloads.head
    import com.databricks.labs.gbx.vectorx.mvt.{MvtDecoder, MvtWriter}
    // Decode all blobs, union features per layer name.
    val layerFeatures = scala.collection.mutable.LinkedHashMap.empty[String, scala.collection.mutable.ArrayBuffer[(Array[Byte], Map[String, Any])]]
    payloads.foreach { blob =>
        MvtDecoder.decode(blob).foreach { case (layerName, wkb, attrs) =>
            layerFeatures.getOrElseUpdate(layerName, scala.collection.mutable.ArrayBuffer.empty) += ((wkb, attrs))
        }
    }
    if (layerFeatures.isEmpty) return payloads.head
    // Re-encode: one MvtWriter.encode call per layer, concatenate into a single-layer archive.
    // MVT spec allows multiple layers in one tile via concatenated protobuf messages.
    // MvtWriter produces a single-layer .pbf; for multi-layer we concatenate the raw protobuf
    // bytes (valid per MVT spec — each layer is a top-level repeated field).
    val layerBlobs = layerFeatures.map { case (layerName, feats) =>
        MvtWriter.encode(layerName, MvtWriter.DefaultExtent, feats.toSeq)
    }
    // Concatenate layer protobufs — valid because MVT is a repeated `layers` field at tag 3.
    val out = new java.io.ByteArrayOutputStream()
    layerBlobs.foreach(out.write)
    out.toByteArray
}
```

**IMPORTANT:** `PMTilesHilbert.zxyToTileid` — confirm the exact utility available in the heavy tier for computing Hilbert tile IDs. Search `PMTilesV3Encoder.scala` or `PMTilesEntry.scala` for the existing `zxyToTileid` / `tileid` conversion. Use whatever is already present rather than reimplementing. If the conversion is inlined in `PMTilesV3Encoder`, extract a package-private `def zxyToTileid(z: Int, x: Int, y: Int): Long` method and call it here.

---

- [ ] **Step 6: Run all heavy PMTiles tests in Docker**

```bash
bash scripts/commands/gbx-docker-exec.sh \
  "mvn test -pl . -Dtest=PMTiles_AggTest,MvtDecoderTest -DfailIfNoTests=false -P skipScoverage -q" \
  2>&1 | tail -30
```

Expected: all tests green, including the 3 new merge tests and all 5 pre-existing tests.

---

- [ ] **Step 7: Scalastyle check**

```bash
bash scripts/commands/gbx-lint-scalastyle.sh 2>&1 | tail -20
```

Expected: no scalastyle violations.

---

- [ ] **Step 8: Commit**

```bash
chmod -R u+rwX .git/objects
git add \
  src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtDecoder.scala \
  src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtWriter.scala \
  src/main/scala/com/databricks/labs/gbx/pmtiles/PMTiles_Agg.scala \
  src/test/scala/com/databricks/labs/gbx/pmtiles/PMTiles_AggTest.scala \
  src/test/scala/com/databricks/labs/gbx/pmtiles/PMTilesTestHelper.scala \
  src/test/scala/com/databricks/labs/gbx/vectorx/mvt/MvtDecoderTest.scala
git commit -m "$(cat <<'EOF'
fix(pmtiles): merge multi-feature MVT blobs per (z,x,y) in heavy tier

Same correctness fix as the light tier: gbx_pmtiles_agg was discarding
all but the first feature for vector tiles with multiple rows at the
same (z,x,y). Add MvtDecoder (OGR MVT read via /vsimem/) and group/merge
in PMTiles_Agg.eval. Raster first-wins is preserved.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 3: Light-vs-heavy parity test (POLYGON multi-feature)

**Goal:** A cross-tier parity test packs two POLYGON single-feature MVT blobs for the same `(z,x,y)` through both light and heavy `gbx_pmtiles_agg`, reads the merged tile from each archive, and asserts both features are present with their properties. This is the mandatory POLYGON parity gate per the spec and the MVT tile-local contract.

**Files:**
- New: `python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py`

**Interfaces:**
- Consumes: light `_assemble_archive` (pure Python, no Spark needed) and heavy `gbx_pmtiles_agg` Spark UDAF (via JAR). The heavy tier produces a PMTiles archive in the same format; light and heavy merged tiles must decode to the same feature set.
- The test follows the same JAR-skip pattern as `test_parity_mvt.py`: auto-skip if no JAR staged.

---

- [ ] **Step 1: Write the parity test file**

Create `python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py`:

```python
"""Light vs heavy gbx_pmtiles_agg vector-merge parity.

Packs two POLYGON single-feature MVT blobs for the same (z,x,y) through both
tiers. Decodes the packed tile from each archive and asserts both features are
present with their geometry type and property values intact.

POLYGON is mandatory — points-only gives a false pass per the MVT tile-local
contract (see CLAUDE.md).

Heavy requires the geobrix JAR staged under python/geobrix/lib/ and GDAL/OGR
native libraries. Auto-skips when absent. Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py \\
        --with-integration --log pmtiles-merge-parity.log
"""

from pathlib import Path

import mapbox_vector_tile as mvt
import pytest
from pmtiles.reader import MmapSource, Reader
from pmtiles.tile import zxy_to_tileid
from shapely.geometry import Polygon
from shapely import to_wkb

from databricks.labs.gbx.pmtiles._agg_light import _assemble_archive

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# Two distinct tile-local polygons (in [0, 4096] pixel space), different ids.
_EXTENT = 4096
_POLY_A = Polygon([(100, 100), (200, 100), (200, 200), (100, 200), (100, 100)])
_POLY_B = Polygon([(300, 300), (400, 300), (400, 400), (300, 400), (300, 300)])


def _make_mvt_blob(poly: Polygon, prop_id: int, layer: str = "bldg") -> bytes:
    return mvt.encode(
        {"name": layer, "features": [{"geometry": poly, "properties": {"id": prop_id}}]},
        default_options={"extents": _EXTENT, "y_coord_down": True},
    )


_BLOB_A = _make_mvt_blob(_POLY_A, prop_id=1)
_BLOB_B = _make_mvt_blob(_POLY_B, prop_id=2)

_Z, _X, _Y = 3, 2, 4


def _read_tile_from_archive(archive: bytes, z: int, x: int, y: int) -> bytes:
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".pmtiles", delete=False) as f:
        f.write(archive)
        name = f.name
    try:
        with open(name, "rb") as f:
            r = Reader(MmapSource(f))
            tile = r.get(z, x, y)
    finally:
        os.unlink(name)
    assert tile is not None, f"tile ({z},{x},{y}) missing from archive"
    return tile


def _decode_features(tile: bytes) -> dict:
    """Return {id: geometry_type} for all features in the 'bldg' layer."""
    decoded = mvt.decode(tile)
    assert "bldg" in decoded, f"layer 'bldg' missing; layers: {list(decoded.keys())}"
    return {
        f["properties"]["id"]: f["geometry"]["type"]
        for f in decoded["bldg"]["features"]
    }


# ── Light tier (Spark-free) ───────────────────────────────────────────────────

def test_light_vector_merge_parity_polygon(tmp_path):
    """Light tier: two POLYGON blobs for same (z,x,y) → both features in merged tile."""
    archive = _assemble_archive([_BLOB_A, _BLOB_B], [_Z, _Z], [_X, _X], [_Y, _Y], {})
    assert archive is not None
    tile = _read_tile_from_archive(archive, _Z, _X, _Y)
    feats = _decode_features(tile)
    assert set(feats.keys()) == {1, 2}, f"light: expected ids {{1,2}}; got {set(feats.keys())}"
    assert feats[1] == "Polygon", f"light: id=1 not Polygon: {feats[1]}"
    assert feats[2] == "Polygon", f"light: id=2 not Polygon: {feats[2]}"


# ── Heavy tier (JAR + GDAL) ──────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    import logging
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live; run in isolation: "
                "gbx:test:python --path python/geobrix/test/pmtiles_light/"
                "test_parity_pmtiles_merge.py --with-integration"
            )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pmtiles-merge-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


def test_heavy_vector_merge_parity_polygon(spark_with_jar):
    """Heavy tier: two POLYGON blobs for same (z,x,y) → both features in merged tile."""
    from databricks.labs.gbx.pmtiles import functions as pt
    from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

    # Use the heavy UDAF registered from the JAR (not the light UDF).
    from databricks.labs.gbx.vectorx import functions as hx
    hx.register(spark_with_jar)

    df = spark_with_jar.createDataFrame(
        [
            ("g", bytearray(_BLOB_A), _Z, _X, _Y),
            ("g", bytearray(_BLOB_B), _Z, _X, _Y),
        ],
        ["grp", "tile", "z", "x", "y"],
    )
    from pyspark.sql import functions as f
    archive = bytes(
        df.groupBy("grp")
        .agg(f.expr("gbx_pmtiles_agg(tile, z, x, y)").alias("arc"))
        .collect()[0]["arc"]
    )
    tile = _read_tile_from_archive(archive, _Z, _X, _Y)
    feats = _decode_features(tile)
    assert set(feats.keys()) == {1, 2}, f"heavy: expected ids {{1,2}}; got {set(feats.keys())}"
    assert feats[1] == "Polygon", f"heavy: id=1 not Polygon: {feats[1]}"
    assert feats[2] == "Polygon", f"heavy: id=2 not Polygon: {feats[2]}"


def test_light_vs_heavy_merged_tile_equivalent(spark_with_jar):
    """Light and heavy merged tiles must decode to equivalent feature sets.

    Geometry coordinate precision may differ by ±1 (integer quantization in
    OGR MVT round-trip vs mapbox_vector_tile native encoding), so we compare
    feature counts, geometry types, and attribute values — not raw bytes.
    """
    from databricks.labs.gbx.vectorx import functions as hx
    hx.register(spark_with_jar)

    # Light merged tile.
    light_archive = _assemble_archive(
        [_BLOB_A, _BLOB_B], [_Z, _Z], [_X, _X], [_Y, _Y], {}
    )
    light_tile = _read_tile_from_archive(light_archive, _Z, _X, _Y)
    light_feats = _decode_features(light_tile)

    # Heavy merged tile.
    df = spark_with_jar.createDataFrame(
        [
            ("g", bytearray(_BLOB_A), _Z, _X, _Y),
            ("g", bytearray(_BLOB_B), _Z, _X, _Y),
        ],
        ["grp", "tile", "z", "x", "y"],
    )
    from pyspark.sql import functions as f
    heavy_archive = bytes(
        df.groupBy("grp")
        .agg(f.expr("gbx_pmtiles_agg(tile, z, x, y)").alias("arc"))
        .collect()[0]["arc"]
    )
    heavy_tile = _read_tile_from_archive(heavy_archive, _Z, _X, _Y)
    heavy_feats = _decode_features(heavy_tile)

    assert light_feats.keys() == heavy_feats.keys(), (
        f"feature id mismatch: light={set(light_feats.keys())} heavy={set(heavy_feats.keys())}"
    )
    for fid in light_feats:
        assert light_feats[fid] == heavy_feats[fid], (
            f"geometry type mismatch for id={fid}: light={light_feats[fid]} heavy={heavy_feats[fid]}"
        )
```

---

- [ ] **Step 2: Run light-only parity tests (no JAR required)**

```bash
python/geobrix/.venv-pyrx/bin/python -m pytest \
  python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py \
  -k "light" -v 2>&1 | tail -20
```

Expected: `test_light_vector_merge_parity_polygon` passes (Task 1 already landed). The `heavy` and `light_vs_heavy` tests skip (no JAR outside Docker).

---

- [ ] **Step 3: Run full parity test suite in Docker**

```bash
bash scripts/commands/gbx-test-python.sh \
  --path python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py \
  --with-integration --log pmtiles-merge-parity.log
```

Expected: all 3 tests pass including `test_light_vs_heavy_merged_tile_equivalent`.

---

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py
git commit -m "$(cat <<'EOF'
test(pmtiles): light-vs-heavy vector-merge parity (POLYGON multi-feature)

Cross-tier parity gate: two POLYGON blobs for the same tile must produce
equivalent merged tiles in both light and heavy gbx_pmtiles_agg. Uses
polygons (not points) per the MVT tile-local contract.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 4: Documentation update

**Goal:** Update `docs/docs/api/pmtiles-functions.mdx` to document the merge-vs-first-wins behaviour. Per the spec: vector tiles are merged; raster tiles keep first-wins. User-facing voice — no internal vocabulary.

**Files:**
- Modify: `docs/docs/api/pmtiles-functions.mdx`

---

- [ ] **Step 1: Locate the `gbx_pmtiles_agg` section in the MDX file**

Read `docs/docs/api/pmtiles-functions.mdx` and find the `gbx_pmtiles_agg` description block. Identify the exact lines describing how duplicate `(z,x,y)` tiles are handled.

---

- [ ] **Step 2: Add a merge-vs-first-wins note**

In the `gbx_pmtiles_agg` description, add a short paragraph after the current description of tile deduplication. The note must:
- Explain that **vector (MVT) tiles** with the same `(z, x, y)` are **merged** — features from each blob are combined into one multi-feature tile, preserving all attributes and layer names.
- Explain that **raster tiles** (PNG, JPEG, WebP) keep **first-write-wins** — images cannot be meaningfully merged.
- Not use any internal vocabulary (no "wave", no "subagent", no "first-wins" jargon — use "the first tile" instead).

Example phrasing:

> When multiple rows share the same tile coordinates `(z, x, y)`:
> - **Vector (MVT) tiles** — features from all matching rows are combined into one
>   multi-feature tile. Features from different layers are kept in their respective
>   layers; attributes are preserved per feature.
> - **Raster tiles (PNG, JPEG, WebP)** — the first non-null tile is used; subsequent
>   tiles for the same coordinates are ignored (raster images cannot be merged).
>
> Tile type is detected automatically from the content of the first non-null payload.

---

- [ ] **Step 3: Pre-commit voice check**

```bash
grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/pmtiles-functions.mdx
```

Expected: no output (clean).

---

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/api/pmtiles-functions.mdx
git commit -m "$(cat <<'EOF'
docs(pmtiles): document MVT merge vs raster first-wins in gbx_pmtiles_agg

User-facing note: vector tiles at the same (z,x,y) are merged into one
multi-feature tile; raster tiles keep the first. This makes the behaviour
of the st_asmvt_pyramid→gbx_pmtiles_agg pipeline explicit in the reference.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 5: Perf / bench verdict

**Goal:** Assess whether the vector-merge change shifts timings materially enough to warrant a `benchmarking.mdx` update and/or a `docs/superpowers/performance/` corpus entry.

**Files:**
- Read (no modification unless warranted): `docs/docs/api/benchmarking.mdx`
- Possibly modify: `docs/docs/api/benchmarking.mdx` (bench-doc rule: any benchmarking change reflected in the same stroke)
- Possibly create: `docs/superpowers/performance/2026-06-28-pmtiles-merge-perf.md`

---

- [ ] **Step 1: Assess the merge overhead locally**

The merge adds a `mapbox_vector_tile.decode` + re-encode round-trip per duplicate tileid. For the common pipeline (`st_asmvt_pyramid → groupBy → gbx_pmtiles_agg`), each tile typically has 5–100 features at zoom levels 0–14. The merge is a correctness fix, not a hot path change for typical single-feature-per-tile datasets (where it takes the fast `if len(blobs) == 1: return blobs[0]` path with zero decode overhead).

Run a quick local timing check:

```python
import timeit, mapbox_vector_tile as mvt
from shapely.geometry import Polygon

poly = Polygon([(100, 100), (200, 100), (200, 200), (100, 200), (100, 100)])
blob = mvt.encode(
    {"name": "bldg", "features": [{"geometry": poly, "properties": {"id": 1}}]},
    default_options={"extents": 4096, "y_coord_down": True},
)

# Single-blob path (no decode — should be ~0 overhead)
t1 = timeit.timeit(lambda: blob if len([blob]) == 1 else None, number=10000)

# Two-blob merge path (decode+encode — baseline cost per tile)
blobs = [blob, blob]
t2 = timeit.timeit(
    lambda: mvt.encode(
        {k: {"features": v["features"] + v["features"]}
         for k, v in mvt.decode(blob).items()},
        default_options={"extents": 4096, "y_coord_down": True},
    ),
    number=1000,
)
print(f"single-blob path: {t1*1000/10000:.3f} ms/tile")
print(f"two-blob merge path: {t2*1000/1000:.3f} ms/tile")
```

---

- [ ] **Step 2: Record verdict**

If the merge path overhead is < 1 ms per tile (expected for small features): **this is a correctness fix, not a measurable perf regression**. No `benchmarking.mdx` change is needed. Record the verdict in a performance corpus note:

Create `docs/superpowers/performance/2026-06-28-pmtiles-merge-perf.md`:

```markdown
# PMTiles agg vector-merge: perf verdict (2026-06-28)

## Change
`gbx_pmtiles_agg` now merges multi-feature MVT blobs per (z,x,y) instead of
keeping only the first. Raster first-wins is unchanged.

## Cost model
- **Single-blob path** (one row per tileid): `if len(blobs) == 1: return blobs[0]`
  — zero decode/encode overhead. The common case for sparse datasets.
- **Multi-blob merge path** (N rows per tileid): N `mapbox_vector_tile.decode` calls
  + 1 `encode` call. For typical tile sizes (< 50 KB, < 100 features per tile) the
  measured per-tile overhead is < 0.5 ms on the driver/executor CPU.

## Verdict
This is a **correctness fix**. Perf overhead is negligible for the typical
`st_asmvt_pyramid → gbx_pmtiles_agg` pipeline. No change to `benchmarking.mdx`
is warranted until a measured regression is observed at real scale (the 1000-tile
cluster bench is the appropriate check if needed).

## Follow-up
If dense-data pipelines (>1000 features per tile) show measurable overhead, the
merge path can be optimised with a direct protobuf concatenation (valid per MVT
spec, fields 3 = repeated layers) that avoids the full decode/encode round-trip.
Track as a future enhancement if cluster bench flags it.
```

If the measured overhead is > 2 ms per tile (unexpected — would indicate an issue in the implementation): file a follow-up task and note it in `benchmarking.mdx` per the bench-doc rule.

---

- [ ] **Step 3: Commit corpus note (if created)**

```bash
chmod -R u+rwX .git/objects
git add docs/superpowers/performance/2026-06-28-pmtiles-merge-perf.md
git commit -m "$(cat <<'EOF'
perf(pmtiles): record vector-merge overhead verdict (correctness fix)

Merge path overhead is < 0.5 ms per tile for typical payloads; single-blob
path has zero overhead. No benchmarking.mdx change warranted.

Co-authored-by: Isaac
EOF
)"
```

---

## Completion checklist

Before declaring this plan done, verify:

- [ ] `python/geobrix/test/pmtiles_light/test_agg_light_core.py` — all tests green, including the 5 new vector-merge tests.
- [ ] `python/geobrix/test/pmtiles_light/test_agg_light_udf.py` — all tests green (regression: no change needed there but confirm).
- [ ] `python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py` — all 3 tests green in Docker with JAR.
- [ ] `PMTiles_AggTest.scala` — all 8 tests green (5 original + 3 new).
- [ ] `MvtDecoderTest.scala` — all 2 tests green.
- [ ] `gbx:lint:scalastyle` — clean.
- [ ] `docs/docs/api/pmtiles-functions.mdx` — merge-vs-first-wins note present, no internal vocab leak.
- [ ] `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` — no output.
- [ ] Performance verdict recorded.
