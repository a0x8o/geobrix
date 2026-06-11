import json
import os

from pmtiles.reader import MmapSource, Reader

from databricks.labs.gbx.ds.register import register

PNG = b"\x89PNG\r\n\x1a\n"


def _png(tag: int) -> bytes:
    return PNG + bytes([tag])


def _rows(spark, tiles):
    data = [(z, x, y, bytearray(_png(i))) for i, (z, x, y) in enumerate(tiles)]
    return spark.createDataFrame(data, schema="z int, x int, y int, bytes binary")


def _read_tile(path, z, x, y):
    with open(path, "rb") as f:
        return Reader(MmapSource(f)).get(z, x, y)


def test_single_archive(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "world.pmtiles")
    tiles = [(6, 32, 21), (6, 33, 21), (7, 64, 42)]
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "0"
    ).save(out)
    assert os.path.isfile(out)
    assert _read_tile(out, 6, 32, 21) is not None
    assert _read_tile(out, 7, 64, 42) is not None


def test_sharded_with_overview_and_catalog(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "tileset_out")
    tiles = [(6, 32, 21), (8, 130, 85), (3, 4, 2)]  # body, body(same parent), overview
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").save(out)

    tileset = os.path.join(out, "tileset")
    assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))
    assert os.path.isfile(os.path.join(tileset, "overview.pmtiles"))
    catalog = json.load(open(os.path.join(tileset, "catalog.json")))
    assert catalog["type"] == "FeatureCollection"
    # body tile reads back from its shard
    assert _read_tile(
        os.path.join(tileset, "6", "32", "21.pmtiles"), 6, 32, 21
    ) is not None
    # overview tile reads back from overview archive
    assert _read_tile(os.path.join(tileset, "overview.pmtiles"), 3, 4, 2) is not None
    # scratch cleaned up
    assert not os.path.isdir(os.path.join(out, "_scratch"))


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "appendme")
    os.makedirs(out, exist_ok=True)
    open(os.path.join(out, "marker"), "w").close()
    import pytest

    with pytest.raises(Exception):
        _rows(spark, [(6, 32, 21)]).write.format("pmtiles_gbx").mode("append").save(
            out
        )
