from databricks.labs.gbx.pygx import _bng


def test_kring_k1_is_nine_cells_incl_center():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")  # TQ3080
    ring = _bng.k_ring_str(cid_s, 1)
    assert cid_s in ring
    assert len(set(ring)) == 9  # 3x3 block


def test_kloop_k1_is_eight_cells_excl_center():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    loop = _bng.k_loop_str(cid_s, 1)
    assert cid_s not in loop
    assert len(set(loop)) == 8


def test_kring_contains_all_kloops():
    cid_s = _bng.east_north_as_bng(530000.0, 180000.0, "1km")
    ring2 = set(_bng.k_ring_str(cid_s, 2))
    loop1 = set(_bng.k_loop_str(cid_s, 1))
    loop2 = set(_bng.k_loop_str(cid_s, 2))
    assert loop1 <= ring2 and loop2 <= ring2
    assert cid_s in ring2
