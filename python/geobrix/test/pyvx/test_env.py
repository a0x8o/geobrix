def test_pyvx_imports_and_env_ok():
    import databricks.labs.gbx.pyvx as pyvx  # noqa: F401
    from databricks.labs.gbx.pyvx import _env
    # Raises a clear ImportError if mapbox-vector-tile / shapely are missing.
    _env.assert_mvt_available()


def test_assert_legacy_available_shapely_only():
    # Legacy decode needs only shapely (not scipy); with shapely present this
    # must not raise, even if scipy is absent.
    from databricks.labs.gbx.pyvx import _env

    _env.assert_legacy_available()
