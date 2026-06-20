def test_stac_exports_client():
    from databricks.labs.gbx.stac import StacClient

    assert StacClient is not None
