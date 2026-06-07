import numpy as np

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import manifest as m
from databricks.labs.gbx.pyrx import _serde


def _open(b):
    return _serde.open_tile(b)


def test_make_tile_shape_crs_bands_dtype():
    b = dg.make_tile_bytes(
        tile_px=64, bands=4, dtype="float32", srid=32618, nodata_frac=0.0, seed=7
    )
    with _open(b) as ds:
        assert ds.width == 64 and ds.height == 64
        assert ds.count == 4
        assert ds.dtypes[0] == "float32"
        assert ds.crs.to_epsg() == 32618


def test_make_tile_is_deterministic_for_seed():
    a = dg.make_tile_bytes(
        tile_px=32, bands=1, dtype="int16", srid=4326, nodata_frac=0.0, seed=42
    )
    b = dg.make_tile_bytes(
        tile_px=32, bands=1, dtype="int16", srid=4326, nodata_frac=0.0, seed=42
    )
    assert a == b


def test_nodata_fraction_is_approximately_respected():
    b = dg.make_tile_bytes(
        tile_px=100,
        bands=1,
        dtype="float32",
        srid=4326,
        nodata_frac=0.25,
        seed=1,
        nodata_mode="sparse",
    )
    with _open(b) as ds:
        arr = ds.read(1)
        nod = ds.nodata
        frac = float(np.mean(arr == nod))
    assert 0.20 <= frac <= 0.30


def test_band_correlation_yields_valid_ndvi_range():
    # red=band1, nir=band2 with band-correlated values -> NDVI within [-1,1]
    b = dg.make_tile_bytes(
        tile_px=32, bands=2, dtype="float32", srid=4326, nodata_frac=0.0, seed=3
    )
    with _open(b) as ds:
        red = ds.read(1).astype("float64")
        nir = ds.read(2).astype("float64")
    denom = nir + red
    ndvi = np.where(denom != 0, (nir - red) / denom, 0.0)
    assert ndvi.min() >= -1.0 and ndvi.max() <= 1.0


def test_generate_corpus_writes_tiles_and_manifest(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=11,
        tile_px=[32, 64],
        bands=[1, 2],
        dtypes=["float32"],
        srids=[4326, 3857],
        nodata_fracs=[0.0, 0.1],
        row_rows=20,
        row_tile_px=64,
        row_bands=2,
        row_dtype="float32",
    )
    assert isinstance(corpus, m.Corpus)
    assert len(corpus.size_sweep) > 0
    assert len(corpus.row_pool.tiles) == 20
    # every referenced tile file exists under out_dir
    for te in corpus.size_sweep + corpus.row_pool.tiles:
        assert (tmp_path / te.path).exists()
    assert (tmp_path / "corpus.json").exists()


def test_validity_gate_passes_for_generated_corpus(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=5,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.02],
        row_rows=3,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    problems = dg.validity_gate(tmp_path, corpus, nodata_warn_threshold=0.9)
    assert problems == []


def test_int16_band_correlation_yields_valid_ndvi_range():
    # int16 tiles must also keep NDVI within [-1, 1] (non-negative reflectance).
    b = dg.make_tile_bytes(
        tile_px=32, bands=2, dtype="int16", srid=4326, nodata_frac=0.0, seed=3
    )
    with _serde.open_tile(b) as ds:
        red = ds.read(1).astype("float64")
        nir = ds.read(2).astype("float64")
    denom = nir + red
    ndvi = np.where(denom != 0, (nir - red) / denom, 0.0)
    assert ndvi.min() >= -1.0 and ndvi.max() <= 1.0
