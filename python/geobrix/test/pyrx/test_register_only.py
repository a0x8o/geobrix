"""register(only=[...]) selective registration for the pyrx tier."""

import pytest

from databricks.labs.gbx.pyrx import functions as prx


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_registers_just_those(spark):
    for n in ("gbx_rst_slope", "gbx_rst_clip"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    prx.register(spark, only=["rst_slope"])
    assert _exists(spark, "gbx_rst_slope")
    assert not _exists(spark, "gbx_rst_clip")


def test_only_accepts_both_name_forms(spark):
    for n in ("gbx_rst_width", "gbx_rst_height"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    prx.register(spark, only=["gbx_rst_width", "RST_Height"])
    assert _exists(spark, "gbx_rst_width")
    assert _exists(spark, "gbx_rst_height")


def test_only_selects_udtf_and_pmtiles_agg(spark):
    for n in ("gbx_rst_retile", "gbx_pmtiles_agg"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    prx.register(spark, only=["gbx_rst_retile", "gbx_pmtiles_agg"])
    assert _exists(spark, "gbx_rst_retile")
    assert _exists(spark, "gbx_pmtiles_agg")


def test_only_unknown_name_raises(spark):
    with pytest.raises(ValueError) as ei:
        prx.register(spark, only=["rst_slpe"])
    assert "rst_slpe" in str(ei.value)


def test_only_none_registers_full_set(spark):
    prx.register(spark)
    for n in ("gbx_rst_width", "gbx_rst_slope", "gbx_rst_retile", "gbx_pmtiles_agg"):
        assert _exists(spark, n)
