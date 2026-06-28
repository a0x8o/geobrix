"""Task 3: plot_static multi-layer compositor tests."""
import matplotlib

matplotlib.use("Agg")

import geopandas as gpd  # noqa: E402
import warnings  # noqa: E402
import pytest  # noqa: E402
from shapely.geometry import Point, Polygon  # noqa: E402

from databricks.labs.gbx.vizx._layers import vector_layer, pmtiles_layer  # noqa: E402
from databricks.labs.gbx.vizx._static_map import plot_static  # noqa: E402


def _gdf(geoms):
    return gpd.GeoDataFrame({"v": range(len(geoms))}, geometry=geoms, crs="EPSG:4326")


def test_two_vector_layers_one_axes():
    pts = _gdf([Point(-122.4, 37.7), Point(-122.41, 37.72)])
    polys = _gdf(
        [Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])]
    )
    ax = plot_static(
        [vector_layer(polys, column="v"), vector_layer(pts, color="red")],
        basemap=False,
    )
    # both layers drew: at least one collection from polys + one from pts
    assert len(ax.collections) >= 2


def test_legacy_single_dataframe_call_still_works():
    pts = _gdf([Point(-122.4, 37.7)])
    ax = plot_static(pts, column="v", basemap=False)
    assert ax is not None


def test_plot_static_empty_list_raises():
    with pytest.raises(ValueError):
        plot_static([])


def test_plot_static_pmtiles_layer_warns():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        plot_static([pmtiles_layer(b"PMTiles\x03")], basemap=False)
    assert any("pmtiles" in str(x.message).lower() for x in w)
