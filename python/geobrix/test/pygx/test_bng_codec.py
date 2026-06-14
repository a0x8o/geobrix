import pytest

shapely = pytest.importorskip("shapely")  # _bng imports shapely at module load
from databricks.labs.gbx.pygx import _bng


def test_resolution_index_and_string_keys():
    # Int index passes through if a valid BNG resolution.
    assert _bng.get_resolution(1) == 1
    assert _bng.get_resolution(-2) == -2
    # String keys map via resolutionMap.
    assert _bng.get_resolution("100km") == 1
    assert _bng.get_resolution("1km") == 3
    assert _bng.get_resolution("100m") == 4
    assert _bng.get_resolution("1m") == 6


def test_resolution_rejects_metres_as_int_and_junk():
    with pytest.raises(ValueError):
        _bng.get_resolution(1000)  # metres-as-Int is NOT a resolution
    with pytest.raises(ValueError):
        _bng.get_resolution(7)
    with pytest.raises(ValueError):
        _bng.get_resolution("nope")


def test_eastnorth_encode_format_london_1km():
    # London TQ 30 80 at 1km: easting 530000, northing 180000.
    cell_long = _bng.point_to_cell_id(530000.0, 180000.0, _bng.get_resolution("1km"))
    s = _bng.format(cell_long)
    assert s == "TQ3080"


def test_format_parse_roundtrip_all_resolutions():
    # Encode a fixed BNG point at every supported resolution; format->parse->format is stable.
    e, n = 530000.0, 180000.0
    for res in sorted(_bng.RESOLUTIONS):
        cid = _bng.point_to_cell_id(e, n, res)
        s = _bng.format(cid)
        reparsed = _bng.parse(s)
        assert _bng.format(reparsed) == s, f"roundtrip failed at res={res}: {s}"


def test_500km_prefix_only():
    # 500km (res -1) formats to a single prefix letter.
    cid = _bng.point_to_cell_id(530000.0, 180000.0, -1)
    s = _bng.format(cid)
    assert len(s) == 1 and s.isalpha()


def test_100km_NE_family_not_quadrant():
    # mosaic#434 fix: a 100km cell whose 2-letter prefix is NE/NW/SE/SW is res 1,
    # NOT a quadrant. Its id is 6 digits with trailing quadrant-digit 0.
    # NE region centre (e.g. easting 450000, northing 950000 -> "NE").
    cid = _bng.point_to_cell_id(450000.0, 950000.0, 1)
    digits = _bng.cell_digits(cid)
    assert digits[-1] == 0  # quadrant marker is 0
    assert _bng.get_resolution(digits) == 1  # 100km, not a negative quadrant res
    assert _bng.format(cid)[:2] in {"NE", "NW", "SE", "SW", "OA", "NA"}
