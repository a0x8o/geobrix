import shutil
import warnings

import pytest

from databricks.labs.gbx.vizx._simplify import normalize_spec


def test_defaults_applied():
    s = normalize_spec(None)
    assert (
        s["budget_mb"] == 64
        and s["min_z"] == 0
        and s["max_z"] == 10
        and s["effort"] == "fast"
    )


def test_override_and_validation():
    assert normalize_spec({"max_z": 12})["max_z"] == 12
    with pytest.raises(ValueError):
        normalize_spec({"min_z": 8, "max_z": 4})
    with pytest.raises(ValueError):
        normalize_spec({"effort": "turbo"})


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_from_geojson_under_budget(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": [1, 2]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
        ],
        crs="EPSG:4326",
    )
    out = tmp_path / "o.pmtiles"
    p = simplify_tiles_from_source(
        gdf, spec={"max_z": 6, "budget_mb": 8}, out_path=str(out)
    )
    assert out.exists() and out.read_bytes()[:7] == b"PMTiles"


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_returns_bytes_without_out_path(tmp_path):
    """Without out_path, bytes are returned (not a file)."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )
    result = simplify_tiles_from_source(gdf, spec={"max_z": 4})
    assert isinstance(result, bytes) and result[:7] == b"PMTiles"


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_drop_densest_and_cluster(tmp_path):
    """spec options drop_densest + cluster_distance propagate without error."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": range(10)},
        geometry=[Point(i * 0.1, i * 0.1) for i in range(10)],
        crs="EPSG:4326",
    )
    out = tmp_path / "clustered.pmtiles"
    simplify_tiles_from_source(
        gdf,
        spec={"max_z": 4, "drop_densest": True, "cluster_distance": 5},
        out_path=str(out),
    )
    assert out.exists() and out.stat().st_size > 0


def test_distributed_engine_raises():
    """engine='distributed' raises NotImplementedError."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )
    with pytest.raises(NotImplementedError, match="distributed"):
        simplify_tiles_from_source(gdf, spec={"engine": "distributed"})


@pytest.mark.skipif(
    shutil.which("tile-join") is None or shutil.which("tippecanoe") is None,
    reason="tile-join and tippecanoe both required",
)
def test_archive_downzoom_trims(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon

    from databricks.labs.gbx.vizx._simplify import (
        simplify_tiles_from_archive,
        simplify_tiles_from_source,
    )

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    src = tmp_path / "full.pmtiles"
    simplify_tiles_from_source(gdf, spec={"max_z": 8}, out_path=str(src))
    out = tmp_path / "ov.pmtiles"
    # After the Task-11 change, a small archive that fits within budget after zoom-trim
    # does NOT warn — the warn-always logic is replaced with conditional (size-based) logic.
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        simplify_tiles_from_archive(
            str(src), spec={"max_z": 4, "budget_mb": 4}, out_path=str(out)
        )
    # Result must be valid PMTiles
    assert out.exists() and out.read_bytes()[:7] == b"PMTiles"
    # No budget_mb warning expected for a small archive that fits within budget
    budget_warns = [
        x for x in w
        if "budget_mb" in str(x.message) and issubclass(x.category, UserWarning)
    ]
    assert len(budget_warns) == 0, (
        f"Expected no budget_mb warning for a within-budget archive, got: "
        f"{[str(x.message) for x in budget_warns]}"
    )


@pytest.mark.skipif(
    shutil.which("tile-join") is None or shutil.which("tippecanoe") is None,
    reason="tile-join and tippecanoe both required",
)
def test_archive_budget_escalation_retiles_from_source(tmp_path):
    """An archive whose tiles exceed budget_mb → escalated to source re-tile; warning fires."""
    import geopandas as gpd
    from shapely.geometry import box

    from databricks.labs.gbx.vizx._simplify import (
        simplify_tiles_from_archive,
        simplify_tiles_from_source,
    )
    from pmtiles.reader import MemorySource, all_tiles

    # Build a dense source archive at z=8 (many features → tiles will be large)
    gdf = gpd.GeoDataFrame(
        {"v": range(200)},
        geometry=[box(i * 0.005, 0, i * 0.005 + 0.005, 0.005) for i in range(200)],
        crs="EPSG:4326",
    )
    src = tmp_path / "dense.pmtiles"
    simplify_tiles_from_source(gdf, spec={"max_z": 8, "budget_mb": 64}, out_path=str(src))

    # Use a tiny budget_mb (0.001 MB = ~1 KB) to force escalation
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = simplify_tiles_from_archive(
            str(src), spec={"max_z": 8, "budget_mb": 0.001}
        )

    # Must have warned about re-tiling
    assert any(
        "re-tiling" in str(x.message).lower() or "re-tile" in str(x.message).lower()
        for x in w
    ), f"Expected re-tiling warning, got: {[str(x.message) for x in w]}"

    # Result must be valid PMTiles bytes
    assert isinstance(result, bytes)
    assert result[:7] == b"PMTiles"


def test_simplify_raster_path(tmp_path):
    """Raster source (GeoTIFF path) → COG output exists and is a valid GeoTIFF."""
    import struct

    import numpy as np

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_bounds

    # Write a tiny 64×64 GeoTIFF
    src_tif = tmp_path / "src.tif"
    data = np.random.randint(0, 255, (1, 64, 64), dtype=np.uint8)
    transform = from_bounds(-1.0, -1.0, 1.0, 1.0, 64, 64)
    with rasterio.open(
        str(src_tif),
        "w",
        driver="GTiff",
        height=64,
        width=64,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data)

    out_cog = tmp_path / "out.tif"
    result = simplify_tiles_from_source(
        str(src_tif), spec={"raster_max_px": 32}, out_path=str(out_cog)
    )
    assert out_cog.exists() and out_cog.stat().st_size > 0
    # Verify it's a valid GeoTIFF (TIFF magic)
    magic = out_cog.read_bytes()[:4]
    assert magic in (b"II\x2a\x00", b"MM\x00\x2a"), f"not a TIFF: {magic!r}"
