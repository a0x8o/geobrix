import json
import os
import tempfile

from databricks.labs.gbx.ds.tiles.catalog import (
    ShardInfo,
    STACManifestCatalog,
    TileJSONCatalog,
)

SHARDS = [
    ShardInfo("6/32/21.pmtiles", 6, 14, (-0.5, 51.0, 0.0, 51.5)),
    ShardInfo("6/33/21.pmtiles", 6, 14, (0.0, 51.0, 0.5, 51.5)),
]


def test_stac_manifest_shape():
    with tempfile.TemporaryDirectory() as d:
        path = STACManifestCatalog().write(SHARDS, d)
        assert os.path.basename(path) == "catalog.json"
        doc = json.load(open(path))
        assert doc["type"] == "FeatureCollection"
        assert len(doc["features"]) == 2
        feat = doc["features"][0]
        assert feat["geometry"]["type"] == "Polygon"
        assert feat["properties"]["pmtiles"] == "6/32/21.pmtiles"
        assert feat["properties"]["minzoom"] == 6
        assert feat["properties"]["maxzoom"] == 14
        assert feat["bbox"] == [-0.5, 51.0, 0.0, 51.5]


def test_tilejson_shape():
    with tempfile.TemporaryDirectory() as d:
        path = TileJSONCatalog().write(SHARDS, d)
        doc = json.load(open(path))
        assert doc["tilejson"] == "3.0.0"
        assert doc["minzoom"] == 6 and doc["maxzoom"] == 14
        # union bounds across shards
        assert doc["bounds"] == [-0.5, 51.0, 0.5, 51.5]
        assert len(doc["shards"]) == 2
