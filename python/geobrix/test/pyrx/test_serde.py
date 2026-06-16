from databricks.labs.gbx.pyrx import _serde

from .conftest import make_geotiff_bytes


def test_tile_schema_matches_heavyweight():
    names = [f.name for f in _serde.TILE_SCHEMA.fields]
    assert names == ["cellid", "raster", "metadata"]
    types = {f.name: f.dataType.typeName() for f in _serde.TILE_SCHEMA.fields}
    assert types["cellid"] == "long"
    assert types["raster"] == "binary"
    assert types["metadata"] == "map"


def test_build_tile_populates_metadata():
    tile = _serde.build_tile(make_geotiff_bytes(), driver="GTiff", cellid=7)
    assert tile["cellid"] == 7
    assert isinstance(tile["raster"], (bytes, bytearray))
    assert tile["metadata"]["driver"] == "GTiff"


def test_open_tile_yields_readable_dataset():
    raster = make_geotiff_bytes(width=4, height=3)
    with _serde.open_tile(raster) as ds:
        assert ds.width == 4
        assert ds.height == 3
