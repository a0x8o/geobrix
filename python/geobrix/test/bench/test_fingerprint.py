import json
import numpy as np
from databricks.labs.gbx.bench import fingerprint as fp
from databricks.labs.gbx.bench import datagen as dg


def test_scalar_fingerprint():
    s = fp.fingerprint_output(64)
    d = json.loads(s)
    assert d["kind"] == "scalar" and d["value"] == 64


def test_scalar_list_fingerprint():
    s = fp.fingerprint_output([1.0, 2.5, 3.0])
    d = json.loads(s)
    assert d["kind"] == "scalar_list" and d["values"] == [1.0, 2.5, 3.0]


def test_raster_fingerprint_per_band_stats():
    raster = dg.make_tile_bytes(tile_px=16, bands=2, dtype="float32", srid=4326,
                                nodata_frac=0.1, seed=1)
    s = fp.fingerprint_output(raster)
    d = json.loads(s)
    assert d["kind"] == "raster"
    assert len(d["bands"]) == 2
    b0 = d["bands"][0]
    for k in ("shape", "dtype", "nodata_count", "min", "max", "mean", "std"):
        assert k in b0
    assert b0["shape"] == [16, 16]


def test_raster_fingerprint_is_deterministic():
    raster = dg.make_tile_bytes(tile_px=16, bands=1, dtype="float32", srid=4326,
                                nodata_frac=0.0, seed=2)
    assert fp.fingerprint_output(raster) == fp.fingerprint_output(raster)
