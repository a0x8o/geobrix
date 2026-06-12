"""Executes the vector_gbx reader doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import vector_gbx_examples as ex  # noqa: E402


def test_read_vector_gbx(spark):
    ex.read_vector_gbx(spark)
