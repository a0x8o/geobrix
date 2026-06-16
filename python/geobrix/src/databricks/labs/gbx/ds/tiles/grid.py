"""Grid-pluggable tile math. ``SlippyGrid`` is the web-mercator XYZ grid used by
PMTiles; future backends (COG-by-quadbin) add their own ``Grid`` implementation."""

from __future__ import annotations

import math
from typing import Iterable, Protocol, Tuple, runtime_checkable

BBox = Tuple[float, float, float, float]  # (minlon, minlat, maxlon, maxlat)
TileKey = Tuple[int, int, int]  # (z, x, y)


@runtime_checkable
class Grid(Protocol):
    """The minimal tile math every tiled-output backend needs."""

    def tile_bbox(self, z: int, x: int, y: int) -> BBox: ...

    def parent(self, z: int, x: int, y: int, shard_zoom: int) -> TileKey: ...

    def tiles_for_bbox(self, bbox: BBox, zoom: int) -> Iterable[TileKey]: ...

    def buffered_bbox(self, z: int, x: int, y: int, buffer: float) -> BBox: ...


class SlippyGrid:
    """Web-mercator slippy-map (XYZ) grid."""

    def tile_bbox(self, z: int, x: int, y: int) -> BBox:
        n = 2**z
        minlon = x / n * 360.0 - 180.0
        maxlon = (x + 1) / n * 360.0 - 180.0
        lat_top = self._lat(y, n)
        lat_bot = self._lat(y + 1, n)
        return (minlon, min(lat_top, lat_bot), maxlon, max(lat_top, lat_bot))

    @staticmethod
    def _lat(y: int, n: int) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))

    def parent(self, z: int, x: int, y: int, shard_zoom: int) -> TileKey:
        sz = min(shard_zoom, z)
        shift = z - sz
        return (sz, x >> shift, y >> shift)

    def tiles_for_bbox(self, bbox: BBox, zoom: int) -> Iterable[TileKey]:
        minlon, minlat, maxlon, maxlat = bbox
        n = 2**zoom
        x0 = int((minlon + 180.0) / 360.0 * n)
        x1 = int((maxlon + 180.0) / 360.0 * n)
        y0 = self._lat_to_y(maxlat, n)
        y1 = self._lat_to_y(minlat, n)
        for x in range(max(0, x0), min(n - 1, x1) + 1):
            for y in range(max(0, y0), min(n - 1, y1) + 1):
                yield (zoom, x, y)

    @staticmethod
    def _lat_to_y(lat: float, n: int) -> int:
        lat = max(min(lat, 85.05112878), -85.05112878)
        rad = math.radians(lat)
        return int((1.0 - math.asinh(math.tan(rad)) / math.pi) / 2.0 * n)

    def buffered_bbox(self, z: int, x: int, y: int, buffer: float) -> BBox:
        minlon, minlat, maxlon, maxlat = self.tile_bbox(z, x, y)
        dx = (maxlon - minlon) * buffer
        dy = (maxlat - minlat) * buffer
        return (minlon - dx, minlat - dy, maxlon + dx, maxlat + dy)
