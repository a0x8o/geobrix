import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.stac._download import fetch_validate_publish, read_validate


def _write_gtiff(path):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=8,
        width=8,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write((np.arange(64, dtype="uint8")).reshape(1, 8, 8))


def test_read_validate_true_for_real_gtiff(tmp_path):
    p = tmp_path / "ok.tif"
    _write_gtiff(str(p))
    assert read_validate(str(p)) is True


def test_read_validate_false_for_garbage(tmp_path):
    p = tmp_path / "bad.tif"
    p.write_bytes(b"<Error>throttled</Error>" * 100)
    assert read_validate(str(p)) is False


def test_fetch_publishes_only_valid(tmp_path):
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"

    def get(href, timeout=None, stream=None):
        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield open(str(src), "rb").read()

        return R()

    res = fetch_validate_publish(
        lambda: "http://x/ok.tif", str(out_dir), "ok.tif", get=get
    )
    assert res == os.path.join(str(out_dir), "ok.tif")
    assert os.path.exists(res)


def test_fetch_retries_then_gives_up_on_bad(tmp_path):
    out_dir = tmp_path / "out"
    tries = {"n": 0}

    def get(href, timeout=None, stream=None):
        tries["n"] += 1

        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"throttled-not-a-raster"

        return R()

    res = fetch_validate_publish(
        lambda: "http://x/bad.tif",
        str(out_dir),
        "bad.tif",
        get=get,
        max_tries=3,
        sleep=lambda s: None,
    )
    assert res is None
    assert tries["n"] == 3
    assert not os.path.exists(os.path.join(str(out_dir), "bad.tif"))
