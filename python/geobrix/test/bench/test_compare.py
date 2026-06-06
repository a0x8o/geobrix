from databricks.labs.gbx.bench import compare as c


def test_scalar_exact_and_tol_and_divergent():
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar","value":256}', '{"kind":"scalar","value":256}'
        )[0]
        == "exact"
    )
    cls, delta, _, _ = c.compare_fingerprints(
        '{"kind":"scalar","value":100.0}', '{"kind":"scalar","value":100.00005}'
    )
    assert cls == "within_tol"
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar","value":100.0}', '{"kind":"scalar","value":150.0}'
        )[0]
        == "divergent"
    )


def test_kind_mismatch_is_divergent():
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar","value":1}', '{"kind":"raster","bands":[]}'
        )[0]
        == "divergent"
    )


def test_empty_fingerprint_is_na():
    assert c.compare_fingerprints("", "")[0] == "na"
    assert c.compare_fingerprints('{"kind":"scalar","value":1}', "")[0] == "na"


def test_raster_dtype_excluded_nodata_count_informational():
    hw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32","nodata_count":12,"min":0.0,"max":1.0,"mean":0.5,"std":0.25}]}'
    lw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32","nodata_count":0,"min":0.0,"max":1.0,"mean":0.5,"std":0.25}]}'
    cls, delta, ndc_delta, _ = c.compare_fingerprints(hw, lw)
    assert cls == "exact"
    assert ndc_delta == 12
    assert delta == 0.0


def test_raster_stat_divergence():
    hw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32","nodata_count":0,"min":0.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    lw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32","nodata_count":0,"min":0.0,"max":1.0,"mean":0.5,"std":0.25}]}'
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_scalar_list_tolerance():
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar_list","values":[1.0,2.0]}',
            '{"kind":"scalar_list","values":[1.0,2.0]}',
        )[0]
        == "exact"
    )
    assert (
        c.compare_fingerprints(
            '{"kind":"scalar_list","values":[1.0,2.0]}',
            '{"kind":"scalar_list","values":[1.0,9.0]}',
        )[0]
        == "divergent"
    )
