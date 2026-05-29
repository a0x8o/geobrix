"""Spark-free pixel <-> world coordinate transforms.

rasterio's DatasetReader.xy(row, col) returns the world coordinate of a pixel
center; index(x, y) returns (row, col). Note rasterio orders by (row, col)
while the GeoBrix API orders args (pixel_x=col, pixel_y=row).
"""


def raster_to_world_x(ds, pixel_x: int, pixel_y: int) -> float:
    x, _ = ds.xy(int(pixel_y), int(pixel_x))
    return float(x)


def raster_to_world_y(ds, pixel_x: int, pixel_y: int) -> float:
    _, y = ds.xy(int(pixel_y), int(pixel_x))
    return float(y)


def world_to_raster_x(ds, world_x: float, world_y: float) -> int:
    row, col = ds.index(float(world_x), float(world_y))
    return int(col)


def world_to_raster_y(ds, world_x: float, world_y: float) -> int:
    row, col = ds.index(float(world_x), float(world_y))
    return int(row)
