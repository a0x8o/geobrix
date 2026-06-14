"""Legacy Mosaic geometry decode for the pyvx light tier.

Decodes the legacy internal struct {typeId, srid, boundaries, holes} into a
shapely geometry, preserving Z and polygon holes, then serializes to WKB.
Heavy parity target: databricks.labs.gbx.vectorx.jts.legacy (with the Z-drop
and holes-drop bugs fixed in both tiers).
"""

from typing import Any, List, Optional, Sequence

from shapely import to_wkb
from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

_POINT, _MULTIPOINT, _LINESTRING, _MULTILINESTRING = 1, 2, 3, 4
_POLYGON, _MULTIPOLYGON, _LINEARRING, _GEOMETRYCOLLECTION = 5, 6, 7, 8


def _field(row: Any, name: str, idx: int) -> Any:
    if hasattr(row, "__fields__"):  # pyspark Row
        return row[name]
    if isinstance(row, dict):
        return row.get(name)
    return row[idx]


def _ring(coords: Sequence[Sequence[float]]) -> List[tuple]:
    return [tuple(float(v) for v in c) for c in coords]


def legacy_to_geom(row: Any):
    type_id = int(_field(row, "typeId", 0))
    boundaries = _field(row, "boundaries", 2) or []
    holes = _field(row, "holes", 3) or []

    if type_id == _POINT:
        return Point(*_ring(boundaries[0])[0])
    if type_id == _MULTIPOINT:
        return MultiPoint(_ring(boundaries[0]))
    if type_id in (_LINESTRING, _LINEARRING):
        return LineString(_ring(boundaries[0]))
    if type_id == _MULTILINESTRING:
        return MultiLineString([_ring(ls) for ls in boundaries])
    if type_id == _POLYGON:
        shell = _ring(boundaries[0])
        rings = holes[0] if holes else []
        return Polygon(shell, [_ring(h) for h in rings])
    if type_id == _MULTIPOLYGON:
        polys = []
        for i, shell_coords in enumerate(boundaries):
            shell = _ring(shell_coords)
            rings = holes[i] if i < len(holes) and holes[i] else []
            polys.append(Polygon(shell, [_ring(h) for h in rings]))
        return MultiPolygon(polys)
    if type_id == _GEOMETRYCOLLECTION:
        raise ValueError("GeometryCollection is not supported by st_legacyaswkb")
    raise ValueError(f"unknown legacy geometry typeId: {type_id}")


def legacy_to_wkb(row: Any) -> Optional[bytes]:
    if row is None:
        return None
    geom = legacy_to_geom(row)
    return to_wkb(
        geom
    )  # shapely default: ISO flavor, dim 3 -> Z preserved when present
