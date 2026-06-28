import pytest
from databricks.labs.gbx.vizx._layers import (
    Layer, vector_layer, raster_layer, grid_layer, pmtiles_layer, as_layers,
)

def test_constructors_set_kind_and_params():
    v = vector_layer("df", geom_col="geom", column="pop", label="cities")
    assert v.kind == "vector" and v.geom_col == "geom" and v.column == "pop" and v.label == "cities"
    g = grid_layer("df", grid_system="h3", cellid_col="h3", column="score")
    assert g.kind == "grid" and g.grid_system == "h3" and g.cellid_col == "h3" and g.column == "score"
    r = raster_layer("/x.tif", band=1, cmap="terrain")
    assert r.kind == "raster" and r.band == 1 and r.cmap == "terrain"
    p = pmtiles_layer("/x.pmtiles")
    assert p.kind == "pmtiles"

def test_grid_layer_requires_grid_system():
    with pytest.raises(TypeError):
        grid_layer("df")  # grid_system is keyword-required

def test_as_layers_coerces_single_and_list():
    v = vector_layer("df")
    assert as_layers(v) == [v]
    assert as_layers([v, v]) == [v, v]

def test_as_layers_bare_pmtiles_path():
    [lyr] = as_layers("/data/x.pmtiles")
    assert lyr.kind == "pmtiles"
