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
