import pytest
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType

from databricks.labs.gbx.ds.vector import _writer_col_roles


def _schema(*names_types):
    return StructType([StructField(n, t, True) for n, t in names_types])


CONV = _schema(
    ("name", StringType()),
    ("geom_0", BinaryType()),
    ("geom_0_srid", StringType()),
    ("geom_0_srid_proj", StringType()),
)


def test_default_convention():
    g, s, p, attrs = _writer_col_roles(CONV)
    assert (g, s, p) == ("geom_0", "geom_0_srid", "geom_0_srid_proj")
    assert attrs == ["name"]


def test_explicit_geom_and_srid_arbitrary_names():
    sch = _schema(
        ("v", LongType()),
        ("the_geom", BinaryType()),
        ("epsg", StringType()),
        ("proj4", StringType()),
    )
    g, s, p, attrs = _writer_col_roles(
        sch, geom_col="the_geom", srid_col="epsg", proj_col="proj4"
    )
    assert (g, s, p) == ("the_geom", "epsg", "proj4")
    assert attrs == ["v"]


def test_srid_defaults_off_geom_when_only_geomcol_given():
    g, s, p, _ = _writer_col_roles(CONV, geom_col="geom_0")
    assert (g, s, p) == ("geom_0", "geom_0_srid", "geom_0_srid_proj")


def test_geomcol_missing_column_raises():
    with pytest.raises(ValueError):
        _writer_col_roles(CONV, geom_col="nope")


def test_sridcol_missing_column_raises():
    with pytest.raises(ValueError):
        _writer_col_roles(CONV, srid_col="nope")


def test_projcol_missing_column_raises():
    with pytest.raises(ValueError):
        _writer_col_roles(CONV, proj_col="nope")


def test_srid_unresolvable_raises():
    # geomCol given, but no sridCol and no <geom>_srid present
    sch = _schema(("the_geom", BinaryType()), ("v", LongType()))
    with pytest.raises(ValueError):
        _writer_col_roles(sch, geom_col="the_geom")


def test_proj_optional_absent_is_fine():
    sch = _schema(
        ("name", StringType()),
        ("geom_0", BinaryType()),
        ("geom_0_srid", StringType()),
    )
    g, s, p, attrs = _writer_col_roles(sch)
    assert (g, s) == ("geom_0", "geom_0_srid")
    assert p == "geom_0_srid_proj"  # default name, not present -> harmless
    assert attrs == ["name"]
