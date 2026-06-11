"""Executes the pmtiles_gbx writer doc examples (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pmtiles_gbx_examples as ex  # noqa: E402


def test_write_pmtiles_single(spark):
    ex.write_pmtiles_single(spark)


def test_write_pmtiles_sharded(spark):
    ex.write_pmtiles_sharded(spark)
