"""Unit tests for the shared, shapely-only gbx._geom module.

Covers parse_geom and geom_to_wkb across all four accepted geometry encodings
(WKB / EWKB / WKT / EWKT) plus the shapely-passthrough and None paths.
"""

import pytest

shapely = pytest.importorskip("shapely")
from shapely import get_srid, set_srid, to_wkb  # noqa: E402
from shapely.geometry import Point, box  # noqa: E402

from databricks.labs.gbx._geom import geom_to_wkb, parse_geom  # noqa: E402


# --- parse_geom -------------------------------------------------------------
def test_parse_geom_wkb_point():
    g = parse_geom(to_wkb(Point(1, 2)))
    assert g.geom_type == "Point"
    assert (g.x, g.y) == (1.0, 2.0)


def test_parse_geom_wkt_point():
    g = parse_geom("POINT (3 4)")
    assert (g.x, g.y) == (3.0, 4.0)


def test_parse_geom_ewkt_srid():
    g = parse_geom("SRID=4326;POINT(1 2)")
    assert get_srid(g) == 4326
    assert (g.x, g.y) == (1.0, 2.0)


def test_parse_geom_ewkb_srid_roundtrip():
    ewkb = to_wkb(set_srid(Point(5, 6), 4326), include_srid=True)
    g = parse_geom(ewkb)
    assert get_srid(g) == 4326


def test_parse_geom_shapely_passthrough():
    pt = Point(7, 8)
    assert parse_geom(pt) is pt


def test_parse_geom_none_and_empty():
    assert parse_geom(None) is None
    assert parse_geom("") is None
    assert parse_geom("   ") is None


# --- geom_to_wkb ------------------------------------------------------------
def test_geom_to_wkb_none():
    assert geom_to_wkb(None) is None


def test_geom_to_wkb_four_encodings_equivalent():
    """WKB / EWKB / WKT / EWKT of the SAME geometry decode to equivalent WKB."""
    poly = box(10.5, 49.0, 11.5, 49.5)
    wkb = to_wkb(poly)
    ewkb = to_wkb(set_srid(poly, 4326), include_srid=True)
    wkt = poly.wkt
    ewkt = f"SRID=4326;{poly.wkt}"

    out = {
        "wkb": geom_to_wkb(wkb),
        "ewkb": geom_to_wkb(ewkb),
        "wkt": geom_to_wkb(wkt),
        "ewkt": geom_to_wkb(ewkt),
    }
    for k, v in out.items():
        assert isinstance(v, bytes), f"{k} did not produce bytes"
        # The decoded 2D geometry must be equal regardless of input encoding.
        assert shapely.from_wkb(v).equals(poly), f"{k} geometry differs"


def test_geom_to_wkb_bytes_passthrough_identity():
    # WKB bytes pass through unchanged (no regression / re-encode).
    wkb = to_wkb(Point(1, 2))
    assert geom_to_wkb(wkb) == wkb


def test_geom_to_wkb_shapely_input():
    pt = Point(3, 4)
    assert shapely.from_wkb(geom_to_wkb(pt)).equals(pt)
