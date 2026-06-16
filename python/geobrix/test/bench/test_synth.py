"""Unit tests for the bench `tile_array` input synthesizer.

The C3 multi-tile functions (rst_frombands / rst_combineavg / rst_merge) each
consume an ARRAY of tiles, but the corpus row provides ONE tile. The synthesizer
derives the multi-tile input from that single source and writes it to disk ONCE,
so BOTH benchmark engines (pyrx + heavy) read byte-identical input files.

These tests assert the synthesizer's contract via rasterio on the outputs:
  * frombands  -> N single-band GTiffs (N = source band count)
  * combineavg -> 2 aligned copies (identical geotransform/shape/CRS)
  * merge      -> 2 copies with DISTINCT geotransform origins (offset extents)
and that it is idempotent (same inputs -> same files, no duplication).
"""

from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import synth


def _write_src(path, bands=3, px=16, dtype="float32", srid=4326):
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=px,
        height=px,
        count=bands,
        dtype=dtype,
        crs=f"EPSG:{srid}",
        transform=transform,
    )
    with rasterio.open(path, "w", **profile) as dst:
        for b in range(1, bands + 1):
            dst.write(np.full((px, px), float(b), dtype=dtype), b)
    return path


def test_synthesize_frombands_splits_into_single_band_tiles(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=3)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "frombands", str(out))
    assert len(paths) == 3, "frombands -> one tile per source band"
    for i, p in enumerate(paths, start=1):
        with rasterio.open(p) as ds:
            assert ds.count == 1, "each frombands tile is single-band"
            # band i carries the i-th source band's constant value
            assert float(ds.read(1).flat[0]) == float(i)


def test_synthesize_combineavg_two_aligned_copies(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "combineavg", str(out))
    assert len(paths) == 2, "combineavg -> 2 copies"
    with rasterio.open(paths[0]) as a, rasterio.open(paths[1]) as b:
        # aligned: identical transform, shape, CRS
        assert a.transform == b.transform
        assert (a.width, a.height, a.count) == (b.width, b.height, b.count)
        assert a.crs == b.crs


def test_synthesize_merge_two_distinct_origins(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=1, px=16)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "merge", str(out))
    assert len(paths) == 2, "merge -> 2 copies"
    with rasterio.open(paths[0]) as a, rasterio.open(paths[1]) as b:
        # DISTINCT origins so their extents tile into a union
        assert (a.transform.c, a.transform.f) != (b.transform.c, b.transform.f)
        # same pixel size / CRS so they mosaic onto one grid
        assert a.res == b.res
        assert a.crs == b.crs


def test_synthesize_is_idempotent(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    first = synth.synthesize(str(src), "frombands", str(out))
    first_bytes = [open(p, "rb").read() for p in first]
    second = synth.synthesize(str(src), "frombands", str(out))
    assert first == second, "same inputs -> same paths"
    second_bytes = [open(p, "rb").read() for p in second]
    assert first_bytes == second_bytes, "same inputs -> byte-identical files"


def test_synth_dir_is_deterministic():
    a = synth.synth_dir("/corpus", "tiles/t0.tif", "merge")
    b = synth.synth_dir("/corpus", "tiles/t0.tif", "merge")
    assert a == b, "both engines must compute the same synth dir"
    assert synth.synth_dir("/corpus", "tiles/t0.tif", "frombands") != a


def _fake_corpus(corpus_root, size_rels, row_rels):
    """Build a minimal Corpus over fake tiles written under ``corpus_root``."""
    from databricks.labs.gbx.bench import manifest as m

    def _te(rel, cellid, bands):
        return m.TileEntry(rel, cellid, 4326, "float32", bands, 16, 0.0)

    size_sweep = []
    for i, rel in enumerate(size_rels):
        p = corpus_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        _write_src(p, bands=2)
        size_sweep.append(_te(rel, i, 2))
    row_tiles = []
    for j, rel in enumerate(row_rels):
        p = corpus_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        _write_src(p, bands=2)
        row_tiles.append(_te(rel, 100 + j, 2))
    return m.Corpus(
        seed=1,
        size_sweep=size_sweep,
        row_pool=m.RowPool(16, 2, "float32", row_tiles),
    )


def test_materialize_all_writes_every_recipe_for_every_size_tile(tmp_path):
    corpus = _fake_corpus(
        tmp_path, ["size/t0.tif", "size/t1.tif"], ["rows/r0.tif", "rows/r1.tif"]
    )
    written = synth.materialize_all(str(tmp_path), corpus)

    # Source tiles synthesized: both size-sweep tiles + the FIRST row_pool tile
    # (the spark-path `_array_root`); NOT the rest of the row pool.
    src_rels = ["size/t0.tif", "size/t1.tif", "rows/r0.tif"]
    for rel in src_rels:
        for recipe in ("frombands", "combineavg", "merge"):
            d = Path(synth.synth_dir(str(tmp_path), rel, recipe))
            assert d.is_dir(), f"missing synth dir for {rel}/{recipe}"
            files = list(d.glob("*.tif"))
            assert files, f"no synth tiles for {rel}/{recipe}"

    # row_pool tiles past the first must NOT be synthesized
    for recipe in ("frombands", "combineavg", "merge"):
        d = Path(synth.synth_dir(str(tmp_path), "rows/r1.tif", recipe))
        assert (
            not d.exists()
        ), "only the first row_pool tile is the spark-path array root"

    assert written, "materialize_all returns the written paths"
    assert all(Path(p).exists() for p in written)


def test_materialize_all_is_idempotent(tmp_path):
    corpus = _fake_corpus(tmp_path, ["size/t0.tif"], ["rows/r0.tif"])
    first = synth.materialize_all(str(tmp_path), corpus)
    first_bytes = {p: open(p, "rb").read() for p in first}
    second = synth.materialize_all(str(tmp_path), corpus)
    assert sorted(first) == sorted(second), "same corpus -> same synth paths"
    for p in second:
        assert open(p, "rb").read() == first_bytes[p], "re-run is byte-identical"


def test_materialize_all_reads_corpus_json_when_corpus_omitted(tmp_path):
    corpus = _fake_corpus(tmp_path, ["size/t0.tif"], ["rows/r0.tif"])
    corpus.write(tmp_path / "corpus.json")
    written = synth.materialize_all(str(tmp_path))  # corpus=None -> read json
    assert written, "materialize_all loads corpus.json when corpus is omitted"
