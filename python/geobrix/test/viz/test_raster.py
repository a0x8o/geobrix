import matplotlib

matplotlib.use("Agg")  # headless: no display needed
from test.pyrx.conftest import make_geotiff_bytes  # noqa: E402

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from databricks.labs.gbx.viz import _raster, plot_file, plot_raster  # noqa: E402


def test_needs_stretch_true_for_uint16_over_255():
    data = np.array([[0, 300], [1000, 65535]], dtype="uint16")
    assert _raster._needs_percentile_stretch(data) is True


def test_needs_stretch_false_for_float_and_small_int():
    assert (
        _raster._needs_percentile_stretch(np.array([[0.1, 0.9]], dtype="float32"))
        is False
    )
    assert (
        _raster._needs_percentile_stretch(np.array([[0, 200]], dtype="uint8")) is False
    )


def test_percentile_stretch_scales_to_unit_range_ignoring_mask():
    band = np.arange(100, dtype="uint16").reshape(1, 10, 10) * 10  # 0..9900
    masked = np.ma.MaskedArray(band, mask=np.zeros_like(band, dtype=bool))
    masked.mask[0, 0, 0] = True  # exclude an outlier-free pixel
    out = _raster._percentile_stretch(masked)
    assert out.dtype == np.float32
    assert float(out.min()) >= 0.0 and float(out.max()) <= 1.0
    assert isinstance(out, np.ma.MaskedArray)
    assert out.mask[0, 0, 0]  # mask preserved


def test_plot_raster_produces_a_figure():
    plt.close("all")
    plot_raster(make_geotiff_bytes(width=8, height=8, count=1))
    assert len(plt.get_fignums()) == 1
    plt.close("all")


def test_plot_file_produces_a_figure(tmp_path):
    p = tmp_path / "t.tif"
    p.write_bytes(make_geotiff_bytes(width=8, height=8, count=3))
    plt.close("all")
    plot_file(str(p))
    assert len(plt.get_fignums()) == 1
    plt.close("all")
