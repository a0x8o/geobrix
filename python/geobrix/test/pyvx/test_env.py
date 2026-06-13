def test_pyvx_imports_and_env_ok():
    import databricks.labs.gbx.pyvx as pyvx  # noqa: F401
    from databricks.labs.gbx.pyvx import _env
    # Raises a clear ImportError if mapbox-vector-tile / shapely are missing.
    _env.assert_mvt_available()
