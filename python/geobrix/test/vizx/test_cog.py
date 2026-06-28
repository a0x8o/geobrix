"""Offline tests for plot_cog (rasterio overview read over a contextily basemap)."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def _write_tif(tmp_path, bands=3, size=32, crs="EPSG:3857"):
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / "cog.tif"
    data = (np.random.rand(bands, size, size) * 1000).astype("uint16")
    transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=bands,
        dtype="uint16",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(data)
    return str(path)


def test_plot_cog_renders_figure(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=3)
    plot_cog(path)
    assert len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_cog_band_select(tmp_path, monkeypatch):
    from databricks.labs.gbx.vizx import _cog

    path = _write_tif(tmp_path, bands=3)
    captured = {}
    # capture the array handed to the renderer to confirm a single band was read
    monkeypatch.setattr(
        _cog,
        "_render_cog",
        lambda data, transform, **kw: captured.update(shape=data.shape),
    )
    _cog.plot_cog(path, band=2)
    assert captured["shape"][0] == 1  # one band selected


def test_plot_cog_strips_dbfs_scheme(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=1)
    plot_cog("dbfs:" + path)  # must not raise on the scheme prefix
    assert len(plt.get_fignums()) >= 1
    plt.close("all")
