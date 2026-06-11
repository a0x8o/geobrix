"""Unit test: reader bench pure-local path produces a ResultRow with timing."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import readers


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_pure_local_reader_bench_emits_result(tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    rows = readers.run_pure_local_reader(
        files=[str(f)],
        run_id="t",
        warmup=1,
        measured=3,
        size_mib=16,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.api == "lightweight"
    assert r.fn == "raster_read"
    assert r.mode == "pure-core"
    assert r.iter_median_s >= 0.0
    assert r.status == "ok"
