"""Environment checks for the pyvx light tier."""


def assert_mvt_available() -> None:
    """Raise a clear ImportError if the MVT light deps are missing."""
    missing = []
    try:
        import mapbox_vector_tile  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("mapbox-vector-tile")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )


def assert_tin_available() -> None:
    """Raise a clear ImportError if the TIN/legacy light deps are missing."""
    missing = []
    try:
        import scipy  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("scipy")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx TIN/legacy requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )
