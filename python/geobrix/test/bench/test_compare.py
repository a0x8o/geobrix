import csv as _csv

from databricks.labs.gbx.bench import compare as c
from databricks.labs.gbx.bench import results as R


def _rr(api, fn, mode, median, fp="", **kw):
    base = dict(
        run_id="r",
        api=api,
        fn=fn,
        category="terrain",
        mode=mode,
        tile_px=256,
        bands=2,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.0,
        warmup_iters=1,
        measured_iters=2,
        median_ms=median,
        min_ms=median,
        p90_ms=median,
        throughput_mpix_s=1.0,
        throughput_rows_s=1.0,
        peak_rss_mb=0.0,
        status="ok",
        note="",
        env_arch="x",
        env_cpu_model="x",
        env_cpu_count=1,
        env_os="x",
        env_gbx_version="0.4.0",
        env_gdal_version="3.12.1",
        env_runtime_version="x",
        env_where="x",
        output_fingerprint=fp,
    )
    base.update(kw)
    return R.ResultRow(**base)


def test_join_and_speedup_and_consistency():
    hw = [
        _rr(
            "heavyweight", "rst_slope", "pure-core", 20.0, '{"kind":"scalar","value":5}'
        ),
        _rr(
            "heavyweight",
            "rst_only_hw",
            "pure-core",
            5.0,
            '{"kind":"scalar","value":1}',
        ),
    ]
    lw = [
        _rr(
            "lightweight", "rst_slope", "pure-core", 4.0, '{"kind":"scalar","value":5}'
        ),
        _rr(
            "lightweight",
            "rst_only_lw",
            "pure-core",
            3.0,
            '{"kind":"scalar","value":1}',
        ),
    ]
    cells, unmatched = c.compare_cells(hw, lw)
    assert len(cells) == 1
    cell = cells[0]
    assert cell.fn == "rst_slope"
    assert abs(cell.speedup - 5.0) < 1e-9
    assert cell.consistency == "exact"
    assert {u[0] for u in unmatched} == {"rst_only_hw", "rst_only_lw"}


def test_spark_path_consistency_is_na():
    hw = [_rr("heavyweight", "rst_width", "spark-path", 100.0, "", srid=0)]
    lw = [_rr("lightweight", "rst_width", "spark-path", 50.0, "", srid=0)]
    cells, _ = c.compare_cells(hw, lw)
    assert cells[0].consistency == "na"
    assert abs(cells[0].speedup - 2.0) < 1e-9


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


def test_timing_only_empty_both_sides_is_na_not_divergent():
    # Task 4: a timing-only fn (fingerprint=False) emits "" on BOTH engines.
    # Empty-vs-empty must compare as `na` (timed, not compared), never divergent.
    cls, delta, ndc, note = c.compare_fingerprints("", "")
    assert cls == "na"
    assert delta == 0.0
    assert ndc == 0


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


def test_near_zero_stats_within_abs_tol_not_divergent():
    # min ~0 on both sides, tiny absolute diff -> within_tol via abs_tol, NOT divergent
    hw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32","nodata_count":0,"min":1e-9,"max":1.0,"mean":0.5,"std":0.25}]}'
    lw = '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32","nodata_count":0,"min":2e-9,"max":1.0,"mean":0.5,"std":0.25}]}'
    cls, _, _, _ = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol"


def test_abs_tol_does_not_mask_real_divergence():
    # large diff still divergent
    hw = '{"kind":"scalar","value":1.0}'
    lw = '{"kind":"scalar","value":5.0}'
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_aspect_min_near_zero_not_divergent():
    # rst_aspect: mean/max/std agree to 8-9 sig figs, but the `min` aspect bearing
    # is a single near-zero pixel (~0.00029 deg) whose ~2.2e-5 deg abs diff blows
    # past the 1e-3 RELATIVE tolerance when divided by the near-zero reference.
    # This is a metric artifact at near-zero values, not an algorithmic divergence;
    # the absolute-tolerance floor must absorb it -> within_tol, NOT divergent.
    hw = (
        '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"Float32",'
        '"nodata_count":0,"min":0.00029,"max":359.9876,"mean":180.4231,'
        '"std":103.8842}]}'
    )
    lw = (
        '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"float32",'
        '"nodata_count":0,"min":0.0000679,"max":359.9876,"mean":180.4231,'
        '"std":103.8842}]}'
    )
    cls, _, _, _ = c.compare_fingerprints(hw, lw)
    assert cls == "within_tol"


def test_abs_tol_does_not_mask_real_min_divergence():
    # A genuine divergence on `min` (abs diff 2.0 on values ~10) must stay divergent;
    # the near-zero abs floor must not swallow real, order-of-magnitude-1+ differences.
    hw = (
        '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"Float32",'
        '"nodata_count":0,"min":10.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    )
    lw = (
        '{"kind":"raster","bands":[{"shape":[4,4],"dtype":"float32",'
        '"nodata_count":0,"min":12.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    )
    assert c.compare_fingerprints(hw, lw)[0] == "divergent"


def test_write_csv(tmp_path):
    cells = [
        _cmp(
            "rst_slope",
            "pure-core",
            20.0,
            4.0,
            5.0,
            "within_tol",
            "nodata_count differs",
            max_rel_delta=0.0004,
            nodata_count_delta=1020,
        )
    ]
    p = tmp_path / "comparison.csv"
    c.write_csv(cells, p)
    rows = list(_csv.DictReader(p.open()))
    assert rows[0]["fn"] == "rst_slope"
    assert float(rows[0]["speedup"]) == 5.0
    assert rows[0]["consistency"] == "within_tol"


def test_summarize_compare_has_insights(tmp_path):
    cells = [
        _cmp(
            "rst_slope",
            "pure-core",
            20.0,
            4.0,
            5.0,
            "within_tol",
            "nodata_count differs",
            max_rel_delta=0.0004,
            nodata_count_delta=1020,
        ),
        _cmp(
            "rst_ndvi",
            "pure-core",
            660.0,
            5.0,
            132.0,
            "divergent",
            "",
            max_rel_delta=0.9,
        ),
    ]
    unmatched = [("rst_viewshed", "lightweight", ("rst_viewshed",))]
    md = c.summarize_compare(cells, unmatched, [], [])
    assert "## Insights" in md
    assert "rst_ndvi" in md  # biggest lightweight win (132x) surfaced
    assert "divergent" in md.lower()
    assert "rst_viewshed" in md  # unmatched surfaced


def test_compare_main_writes_outputs(tmp_path):
    from databricks.labs.gbx.bench import results as RR

    hw = tmp_path / "heavyweight.jsonl"
    lw = tmp_path / "lightweight.jsonl"
    RR.write_jsonl(
        [
            _rr(
                "heavyweight",
                "rst_slope",
                "pure-core",
                20.0,
                '{"kind":"scalar","value":5}',
            )
        ],
        hw,
    )
    RR.write_jsonl(
        [
            _rr(
                "lightweight",
                "rst_slope",
                "pure-core",
                4.0,
                '{"kind":"scalar","value":5}',
            )
        ],
        lw,
    )
    outdir = tmp_path / "out"
    c.main(
        ["--heavyweight", str(hw), "--lightweight", str(lw), "--out-dir", str(outdir)]
    )
    assert (outdir / "comparison.csv").exists()
    assert (outdir / "summary.md").exists()
    assert "Insights" in (outdir / "summary.md").read_text()


def test_divergent_with_nodata_delta_gets_border_note():
    # value stats differ past tol AND nodata_count differs -> note explains likely border cause
    hw = '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"Float32","nodata_count":1020,"min":0.0,"max":90.0,"mean":45.0,"std":10.0}]}'
    lw = '{"kind":"raster","bands":[{"shape":[256,256],"dtype":"float32","nodata_count":0,"min":0.0,"max":90.5,"mean":47.0,"std":10.4}]}'
    cls, _, ndc, note = c.compare_fingerprints(hw, lw)
    assert cls == "divergent"
    assert ndc == 1020
    assert "nodata" in note.lower() and "border" in note.lower()


def test_divergent_without_nodata_delta_has_no_border_note():
    hw = '{"kind":"scalar","value":1.0}'
    lw = '{"kind":"scalar","value":5.0}'
    cls, _, ndc, note = c.compare_fingerprints(hw, lw)
    assert cls == "divergent"
    assert ndc == 0
    assert note == ""  # pure value divergence, no nodata delta -> no border note


def _cmp(fn, mode, hw_ms, lw_ms, speedup, consistency, note, **kw):
    """Build a CellCompare with sensible defaults for the throughput fields."""
    base = dict(
        fn=fn,
        mode=mode,
        tile_px=256,
        bands=2,
        dtype="float32",
        srid=4326,
        nodata_frac=0.0,
        rows=1,
        hw_median_ms=hw_ms,
        lw_median_ms=lw_ms,
        speedup=speedup,
        consistency=consistency,
        max_rel_delta=0.0,
        nodata_count_delta=0,
        note=note,
        hw_mpix_s=0.0,
        lw_mpix_s=0.0,
        hw_rows_s=0.0,
        lw_rows_s=0.0,
    )
    base.update(kw)
    return c.CellCompare(**base)


def test_cellcompare_has_throughput_fields_and_csv_includes_them():
    cell = _cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")
    assert cell.hw_mpix_s == 0.0
    assert cell.lw_mpix_s == 0.0
    assert cell.hw_rows_s == 0.0
    assert cell.lw_rows_s == 0.0
    for f in ("hw_mpix_s", "lw_mpix_s", "hw_rows_s", "lw_rows_s"):
        assert f in c._CSV_FIELDS


def test_compare_cells_populates_throughput():
    hw = [
        _rr(
            "heavyweight",
            "rst_slope",
            "pure-core",
            20.0,
            '{"kind":"scalar","value":5}',
            throughput_mpix_s=3.3,
            throughput_rows_s=7.0,
        )
    ]
    lw = [
        _rr(
            "lightweight",
            "rst_slope",
            "pure-core",
            4.0,
            '{"kind":"scalar","value":5}',
            throughput_mpix_s=16.4,
            throughput_rows_s=35.0,
        )
    ]
    cells, _ = c.compare_cells(hw, lw)
    assert cells[0].hw_mpix_s == 3.3
    assert cells[0].lw_mpix_s == 16.4
    assert cells[0].hw_rows_s == 7.0
    assert cells[0].lw_rows_s == 35.0


def test_summarize_compare_pure_core_header_has_throughput_columns():
    cells = [_cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    header = [ln for ln in md.splitlines() if "| fn |" in ln and "hw_ms" in ln][0]
    assert "hw_mpix/s" in header
    assert "lw_mpix/s" in header


def test_summarize_compare_renders_throughput_values():
    cells = [
        _cmp(
            "rst_slope",
            "pure-core",
            20.0,
            4.0,
            5.0,
            "exact",
            "",
            hw_mpix_s=3.3,
            lw_mpix_s=16.4,
        )
    ]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_slope ")][0]
    assert "3.3" in row
    assert "16.4" in row


def test_summarize_compare_has_tolerance_legend():
    cells = [_cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    assert f"rel ≤ {c.REL_TOL:g}" in md
    assert f"abs ≤ {c.ABS_TOL:g}" in md
    assert "within_tol" in md


def test_summarize_compare_exact_label_is_bare():
    cells = [_cmp("rst_slope", "pure-core", 20.0, 4.0, 5.0, "exact", "")]
    md = c.summarize_compare(cells, [], [], [])
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_slope ")][0]
    # the consistency cell is the bare label, no parenthetical / explanation
    assert "| exact |" in row
    assert "exact (" not in row


def test_results_main_writes_summary(tmp_path):
    from databricks.labs.gbx.bench import results as RR

    shard = tmp_path / "heavyweight.jsonl"
    RR.write_jsonl(
        [
            _rr(
                "heavyweight",
                "rst_slope",
                "pure-core",
                20.0,
                '{"kind":"scalar","value":5}',
            )
        ],
        shard,
    )
    RR.main(["--in", str(shard)])
    out = tmp_path / "heavyweight.summary.md"
    assert out.exists()
    assert "GeoBrix benchmark summary" in out.read_text()
