"""Shared geometry-input parsing for the pyvx light tier.

Re-exports :func:`parse_geom` from the neutral, shapely-only
:mod:`databricks.labs.gbx._geom` module (the single source of truth across the
light tiers) so the accepted encodings (WKB / EWKB / WKT / EWKT) stay consistent
across the ST surface and match the heavyweight tier (which accepts
BINARY|STRING for geometry inputs).
"""

from databricks.labs.gbx._geom import parse_geom

__all__ = ["parse_geom"]
