import json

from databricks.labs.gbx.bench import manifest as m


def test_corpus_roundtrip(tmp_path):
    corpus = m.Corpus(
        seed=1234,
        size_sweep=[
            m.TileEntry(
                path="a.tif",
                cellid=0,
                srid=4326,
                dtype="float32",
                bands=1,
                tile_px=256,
                nodata_frac=0.02,
            ),
        ],
        row_pool=m.RowPool(
            tile_px=1024,
            bands=4,
            dtype="float32",
            tiles=[
                m.TileEntry(
                    path="r0.tif",
                    cellid=0,
                    srid=3857,
                    dtype="float32",
                    bands=4,
                    tile_px=1024,
                    nodata_frac=0.0,
                )
            ],
        ),
    )
    p = tmp_path / "corpus.json"
    corpus.write(p)
    loaded = m.Corpus.read(p)
    assert loaded == corpus
    assert json.loads(p.read_text())["seed"] == 1234
