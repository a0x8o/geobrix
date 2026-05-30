"""DEM terrain derivatives: slope, aspect, hillshade (Horn's 3x3 method).

Implements the same algorithms as ``gdaldem`` but in pure NumPy — no scipy.
Exact bit-parity with GDAL is NOT guaranteed; the outputs match to within
a rounding tolerance on well-behaved inputs.

Horn's method:
    For each pixel, take the 3x3 neighbourhood (edge-replicated):
        a=NW  b=N   c=NE
        d=W   e=ctr f=E
        g=SW  h=S   i=SE

    dzdx = ((c + 2f + i) - (a + 2d + g)) / (8 * xres)
    dzdy = ((g + 2h + i) - (a + 2b + c)) / (8 * yres)

Single-band output in every case:
    slope     -> Float32 (degrees or percent)   nodata -9999
    aspect    -> Float32 (compass or trig deg)  nodata -9999
    hillshade -> uint8   [0..255]               nodata not set
"""

import numpy as np
from rasterio.io import MemoryFile

_NODATA = -9999.0


def _horn_gradients(ds) -> tuple:
    """Return (dzdx, dzdy) for the first band of *ds* using Horn's 3x3 kernel.

    Edge pixels are computed using edge-replication (``np.pad(mode='edge')``),
    so the output arrays are the same shape as the source band.  Pixel
    ground sizes come from the absolute values of the affine transform's
    scale components.
    """
    xres = abs(ds.transform.a)
    yres = abs(ds.transform.e)

    band = ds.read(1).astype("float64")
    # Edge-replicate one pixel on each side.
    p = np.pad(band, 1, mode="edge")

    a = p[0:-2, 0:-2]  # NW
    b = p[0:-2, 1:-1]  # N
    c = p[0:-2, 2:]  # NE
    d = p[1:-1, 0:-2]  # W
    f = p[1:-1, 2:]  # E
    g = p[2:, 0:-2]  # SW
    h = p[2:, 1:-1]  # S
    i = p[2:, 2:]  # SE

    dzdx = ((c + 2.0 * f + i) - (a + 2.0 * d + g)) / (8.0 * xres)
    dzdy = ((g + 2.0 * h + i) - (a + 2.0 * b + c)) / (8.0 * yres)

    return dzdx, dzdy


def _emit_float32(ds, arr: np.ndarray) -> bytes:
    """Write *arr* as a single-band Float32 GTiff in memory, nodata=-9999."""
    out = np.where(np.isfinite(arr), arr, _NODATA).astype("float32")
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype="float32", nodata=_NODATA)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out, 1)
        return mf.read()


def _emit_uint8(ds, arr: np.ndarray) -> bytes:
    """Write *arr* (already [0..255]) as a single-band Byte GTiff in memory."""
    out = np.clip(np.round(arr), 0, 255).astype("uint8")
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype="uint8", nodata=None)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out, 1)
        return mf.read()


def slope(ds, unit: str = "degrees", scale: float = 1.0) -> bytes:
    """Compute terrain slope (Horn's method).

    Args:
        ds:    Open rasterio DatasetReader.  Band 1 is used as the DEM.
        unit:  ``"degrees"`` (default) or ``"percent"``.
        scale: Ratio of vertical units to horizontal units (default 1.0).
               For geographic-degree grids use ~111120 to match gdaldem.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    dzdx, dzdy = _horn_gradients(ds)

    # Apply scale to the combined gradient magnitude.
    magnitude = (1.0 / float(scale)) * np.sqrt(dzdx**2 + dzdy**2)
    slope_rad = np.arctan(magnitude)

    if unit == "percent":
        result = np.tan(slope_rad) * 100.0  # == magnitude * 100
    else:
        result = np.degrees(slope_rad)

    return _emit_float32(ds, result)


def aspect(ds, trigonometric: bool = False, zero_for_flat: bool = False) -> bytes:
    """Compute terrain aspect (Horn's method).

    Default output is compass degrees: 0 = North, increasing clockwise.
    Flat cells (dzdx == dzdy == 0) are set to -9999 (or 0 if zero_for_flat).

    Args:
        ds:              Open rasterio DatasetReader.  Band 1 used as DEM.
        trigonometric:   If True, return math-convention degrees (CCW from
                         east) instead of compass degrees (CW from north).
        zero_for_flat:   If True, flat cells get 0 instead of -9999.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    dzdx, dzdy = _horn_gradients(ds)

    # Math convention: arctan2(dzdy, -dzdx) — angle CCW from east in [-180,180].
    aspect_rad = np.arctan2(dzdy, -dzdx)
    aspect_deg = np.degrees(aspect_rad)

    if trigonometric:
        result = aspect_deg
    else:
        # Convert to compass bearing: 0 = N, clockwise.
        # compass = (90 - math_degrees) % 360
        result = (90.0 - aspect_deg) % 360.0

    # Mark flat cells.
    flat_mask = (dzdx == 0.0) & (dzdy == 0.0)
    flat_value = 0.0 if zero_for_flat else _NODATA
    result = np.where(flat_mask, flat_value, result)

    return _emit_float32(ds, result)


def hillshade(
    ds,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z_factor: float = 1.0,
) -> bytes:
    """Compute hillshade (Horn's method, gdaldem convention).

    Args:
        ds:        Open rasterio DatasetReader.  Band 1 used as DEM.
        azimuth:   Sun azimuth in degrees (default 315 = NW).
        altitude:  Sun elevation angle above horizon in degrees (default 45).
        z_factor:  Vertical exaggeration applied to gradients (default 1.0).

    Returns:
        Single-band Byte (uint8) GTiff bytes, values [0..255].
    """
    dzdx, dzdy = _horn_gradients(ds)

    # Apply z_factor to gradients before angle computations.
    dzdx_z = dzdx * float(z_factor)
    dzdy_z = dzdy * float(z_factor)

    # Zenith angle in radians (complement of altitude).
    zenith = (90.0 - float(altitude)) * np.pi / 180.0

    # gdaldem azimuth convention: rotate from math CCW-east to CW-from-north,
    # then apply (360 - az + 90) % 360 to get the illumination direction.
    az_rad = ((360.0 - float(azimuth) + 90.0) % 360.0) * np.pi / 180.0

    slope_rad = np.arctan(np.sqrt(dzdx_z**2 + dzdy_z**2))
    aspect_rad = np.arctan2(dzdy_z, -dzdx_z)

    hs = 255.0 * (
        np.cos(zenith) * np.cos(slope_rad)
        + np.sin(zenith) * np.sin(slope_rad) * np.cos(az_rad - aspect_rad)
    )

    return _emit_uint8(ds, hs)
