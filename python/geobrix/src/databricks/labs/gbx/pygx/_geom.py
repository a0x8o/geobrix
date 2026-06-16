"""Shared geometry-input parsing for the pygx light tier.

Every geom-accepting pygx function uses parse_geom so the accepted encodings
(WKB / EWKB / WKT / EWKT) stay consistent across the ST surface and match the
heavyweight tier (which accepts BINARY|STRING for geometry inputs).
"""

from typing import Any, Optional

from shapely import from_wkb, from_wkt, set_srid
from shapely.geometry.base import BaseGeometry


def parse_geom(x: Any) -> Optional[BaseGeometry]:
    """Parse a geometry from WKB/EWKB bytes or WKT/EWKT text. None -> None."""
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        return from_wkb(bytes(x))  # handles WKB and EWKB
    s = str(x).strip()
    if not s:
        return None
    if s[:5].upper() == "SRID=":
        srid_part, _, wkt_part = s.partition(";")
        geom = from_wkt(wkt_part)
        try:
            return set_srid(geom, int(srid_part[5:]))
        except ValueError:
            return geom
    return from_wkt(s)
