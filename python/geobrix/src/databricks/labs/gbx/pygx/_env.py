"""Environment checks for the pygx light tier."""


def assert_quadbin_available() -> None:
    """Raise a clear ImportError if the quadbin light deps are missing."""
    missing = []
    try:
        import quadbin  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("quadbin")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pygx quadbin requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )


def assert_bng_available() -> None:
    """Raise a clear ImportError if shapely (the only pygx BNG dep) is missing.

    BNG is a pure-Python port of BNG.scala; it needs only shapely (geometry +
    WKB I/O), not the quadbin PyPI library.
    """
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        raise ImportError(
            "pygx BNG requires the [light] extra (shapely). "
            "Install with: pip install 'geobrix[light]'"
        )
