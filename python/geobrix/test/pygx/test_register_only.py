"""register(only=[...]) selective registration for the pygx tier."""

import pytest

from databricks.labs.gbx.pygx import functions as pgx


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_quadbin(spark):
    for n in ("gbx_quadbin_polyfill", "gbx_bng_polyfill"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    pgx.register(spark, only=["quadbin_polyfill"])
    assert _exists(spark, "gbx_quadbin_polyfill")
    assert not _exists(spark, "gbx_bng_polyfill")


def test_only_accepts_camelcase(spark):
    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS gbx_bng_polyfill")
    pgx.register(spark, only=["BNG_Polyfill"])
    assert _exists(spark, "gbx_bng_polyfill")


def test_only_unknown_raises(spark):
    with pytest.raises(ValueError) as ei:
        pgx.register(spark, only=["quadbin_polifyll"])
    assert "quadbin_polifyll" in str(ei.value)


def test_only_does_not_trip_unselected_subgroup_guard(spark, monkeypatch):
    # Selecting only a quadbin fn must NOT call the bng/custom availability guards.
    from databricks.labs.gbx.pygx import _env

    def _boom():
        raise RuntimeError("guard should not be called")

    monkeypatch.setattr(_env, "assert_bng_available", _boom)
    monkeypatch.setattr(_env, "assert_custom_available", _boom)
    pgx.register(spark, only=["gbx_quadbin_resolution"])  # must not raise
    assert _exists(spark, "gbx_quadbin_resolution")


def test_only_none_registers_all_subgroups(spark):
    pgx.register(spark)
    for n in ("gbx_quadbin_polyfill", "gbx_bng_polyfill", "gbx_custom_polyfill"):
        assert _exists(spark, n)
