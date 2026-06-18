"""rst_clip cutline reprojection (light-vs-heavy parity).

Heavy RST_Clip reprojects the cutline from its SRID to the raster CRS before
warping. The light clip used to mask with no reprojection, so an EWKT/EWKB
cutline in a different CRS than the raster raised "Input shapes do not overlap
raster". These tests pin the reprojection behavior: a 4326 lon/lat cutline that
covers a projected (UTM) raster must clip successfully.
"""

import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely import set_srid
from shapely.geometry import box

from databricks.labs.gbx._geom import parse_geom
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import edit


def _utm_geotiff_bytes(width=8, height=8):
    """In-memory GTiff in EPSG:32633 (UTM 33N) near 15E/45N.

    Origin near (500000, 5000000) m, 100 m pixels -> 800m x 800m extent.
    """
    transform = from_origin(500000.0, 5000000.0, 100.0, 100.0)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


def _utm_extent_lonlat():
    """The UTM raster extent expressed as a 4326 lon/lat box (covers it)."""
    from rasterio.warp import transform_bounds

    # extent in UTM: x [500000, 500800], y [4999200, 5000000]
    minx, miny, maxx, maxy = transform_bounds(
        "EPSG:32633", "EPSG:4326", 500000.0, 4999200.0, 500800.0, 5000000.0
    )
    # pad slightly so the cutline fully covers the raster
    return (minx - 0.01, miny - 0.01, maxx + 0.01, maxy + 0.01)


def test_clip_reprojects_4326_cutline_over_utm_raster():
    """A SRID=4326 cutline over a UTM raster must reproject and overlap.

    Previously raised ValueError: Input shapes do not overlap raster.
    """
    minx, miny, maxx, maxy = _utm_extent_lonlat()
    cutline = set_srid(box(minx, miny, maxx, maxy), 4326)  # carries SRID
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, cutline, all_touched=True)
    with _serde.open_tile(out) as o:
        assert o.crs.to_epsg() == 32633
        assert o.width > 0 and o.height > 0


def test_clip_reprojects_ewkt_4326_cutline_via_parse_geom():
    """EWKT 'SRID=4326;POLYGON(...)' (as _clip_udf decodes it) reprojects too."""
    minx, miny, maxx, maxy = _utm_extent_lonlat()
    ewkt = "SRID=4326;" + box(minx, miny, maxx, maxy).wkt
    geom = parse_geom(ewkt)  # mirrors _clip_udf path
    assert shapely.get_srid(geom) == 4326
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, geom, all_touched=True)
    with _serde.open_tile(out) as o:
        assert o.width > 0 and o.height > 0


def test_clip_reprojects_ewkb_4326_cutline():
    """EWKB bytes carrying SRID=4326 reproject too."""
    minx, miny, maxx, maxy = _utm_extent_lonlat()
    geom = set_srid(box(minx, miny, maxx, maxy), 4326)
    ewkb = shapely.to_wkb(geom, include_srid=True)
    parsed = parse_geom(ewkb)  # from_wkb handles EWKB
    assert shapely.get_srid(parsed) == 4326
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, parsed, all_touched=True)
    with _serde.open_tile(out) as o:
        assert o.width > 0 and o.height > 0


def test_clip_cutline_already_in_raster_crs_no_regression():
    """A cutline already in the raster CRS (UTM) clips without reprojection."""
    # subset of the UTM extent -> should crop to a smaller raster
    cutline = box(500200.0, 4999400.0, 500600.0, 4999800.0)  # SRID 0
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, cutline, all_touched=False)
    with _serde.open_tile(out) as o:
        assert o.crs.to_epsg() == 32633
        assert 0 < o.width < 8 and 0 < o.height < 8


def test_clip_cutline_with_matching_srid_no_regression():
    """SRID == raster EPSG: src == dst, so no transform, still clips."""
    cutline = set_srid(box(500200.0, 4999400.0, 500600.0, 4999800.0), 32633)
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, cutline, all_touched=False)
    with _serde.open_tile(out) as o:
        assert 0 < o.width < 8 and 0 < o.height < 8


def test_clip_wkb_bytes_still_accepted_no_srid():
    """Back-compat: bare WKB bytes (no SRID) still clip as-is (raster CRS)."""
    wkb = shapely.wkb.dumps(box(500200.0, 4999400.0, 500600.0, 4999800.0))
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = edit.clip_to_geom(ds, wkb, all_touched=False)
    with _serde.open_tile(out) as o:
        assert 0 < o.width < 8 and 0 < o.height < 8
