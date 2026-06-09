from databricks.labs.gbx.bench import results as r


def _row(**kw):
    base = dict(
        run_id="run1",
        api="lightweight",
        fn="rst_width",
        category="accessor",
        mode="pure-core",
        tile_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.02,
        warmup_iters=2,
        measured_iters=5,
        iter_median_ms=1.5,
        iter_min_ms=1.2,
        iter_p90_ms=1.9,
        throughput_mpix_s=44.0,
        throughput_rows_s=666.0,
        peak_rss_mb=120.0,
        status="ok",
        note="",
        env_arch="arm64",
        env_cpu_model="M3",
        env_cpu_count=8,
        env_os="Darwin",
        env_gbx_version="0.4.0",
        env_gdal_version="3.8.0",
        env_runtime_version="py3.12",
        env_where="venv",
    )
    base.update(kw)
    return r.ResultRow(**base)


def test_jsonl_roundtrip(tmp_path):
    rows = [_row(), _row(fn="rst_slope", category="terrain", iter_median_ms=20.0)]
    p = tmp_path / "shard.jsonl"
    r.write_jsonl(rows, p)
    loaded = r.read_jsonl(p)
    assert loaded == rows


def test_summary_lists_slowest(tmp_path):
    rows = [
        _row(fn="rst_fast", iter_median_ms=1.0),
        _row(fn="rst_slow", iter_median_ms=99.0),
    ]
    md = r.summarize(rows)
    assert "rst_slow" in md
    # slowest should appear before fast in the slowest-functions section
    assert md.index("rst_slow") < md.index("rst_fast")


def test_spark_path_table_reports_per_tile_ms():
    # spark-path table surfaces per_tile_ms = median_ms / rows.
    rows = [
        _row(fn="rst_resample", mode="spark-path", rows=1000, iter_median_ms=100000.0),
    ]
    md = r.summarize(rows)
    hdr = [
        ln
        for ln in md.splitlines()
        if ln.startswith("| fn |") and "per_tile_avg_ms" in ln
    ]
    assert hdr, "spark-path table should have a per_tile_avg_ms column"
    # iter_median_ms (whole-iteration) is delineated from per_tile_avg_ms
    assert "iter_median_ms" in hdr[0]
    row_line = [ln for ln in md.splitlines() if ln.startswith("| rst_resample ")][0]
    assert "100.000" in row_line  # 100000 ms / 1000 tiles = 100.000 ms/tile


def test_summarize_has_insights_status_and_flags(tmp_path):
    rows = [
        _row(
            fn="rst_width",
            mode="pure-core",
            tile_px=256,
            iter_median_ms=1.0,
            status="ok",
        ),
        _row(
            fn="rst_slope",
            mode="pure-core",
            tile_px=4096,
            iter_median_ms=50.0,
            status="ok",
            category="terrain",
            nodata_frac=0.1,
            output_fingerprint='{"kind": "raster", "bands": [{"nodata_count": 0, "min": 0.0}]}',
        ),
        _row(
            fn="rst_ndvi",
            mode="pure-core",
            tile_px=256,
            bands=1,
            status="error",
            note="band index 2 out of range",
            iter_median_ms=0.0,
        ),
    ]
    md = r.summarize(rows)
    assert "## Insights" in md
    assert "## Status" in md
    assert "rst_slope" in md  # slowest op named
    assert "rst_ndvi" in md  # error fn surfaced
    assert "error" in md.lower()
    assert (
        "nodata" in md.lower()
    )  # consistency flag for sentinel-as-data on nodata tile
    # env line present
    assert "GDAL" in md
    # tile_px VARIES (256 and 4096) across the slowest-pure-core rows -> it must
    # stay a COLUMN, not get hoisted above the table.
    slow_header = [
        ln
        for ln in md.splitlines()
        if ln.startswith("| fn |") and "median_ms" in ln and "mpix/s" in ln
    ][0]
    assert "tile_px" in slow_header


def test_summarize_keeps_varying_srid_as_column():
    # Mixed srid (or any non-px dim that varies) stays a column; constant hoists.
    rows = [
        _row(fn="rst_a", mode="pure-core", srid=4326, iter_median_ms=5.0),
        _row(fn="rst_b", mode="pure-core", srid=3857, iter_median_ms=6.0),
    ]
    md = r.summarize(rows)
    hdr = [
        ln for ln in md.splitlines() if ln.startswith("| fn |") and "median_ms" in ln
    ][0]
    assert "srid" in hdr
    # constant srid is hoisted out of the table into the context line
    same = [
        _row(fn="rst_a", mode="pure-core", srid=4326, iter_median_ms=5.0),
        _row(fn="rst_b", mode="pure-core", srid=4326, iter_median_ms=6.0),
    ]
    md2 = r.summarize(same)
    hdr2 = [
        ln for ln in md2.splitlines() if ln.startswith("| fn |") and "median_ms" in ln
    ][0]
    assert "srid" not in hdr2
    assert "srid 4326" in md2


def _hoist_md(pool_size=None):
    """All pure-core rows share tile_px/bands/dtype/srid -> they hoist."""
    rows = [
        _row(fn="rst_a", iter_median_ms=1.5, tile_px=512, bands=4, dtype="float32"),
        _row(fn="rst_b", iter_median_ms=9.0, tile_px=512, bands=4, dtype="float32"),
    ]
    return r.summarize(rows, pool_size=pool_size)


def test_summarize_hoists_constant_dims_above_table():
    md = _hoist_md()
    slow_header = [
        ln
        for ln in md.splitlines()
        if ln.startswith("| fn |") and "median_ms" in ln and "mpix/s" in ln
    ][0]
    # constant dims are NOT columns
    assert "tile_px" not in slow_header
    assert "bands" not in slow_header
    assert "dtype" not in slow_header
    # they appear once in a context line above the table
    assert "tile_px 512" in md
    assert "4 bands" in md
    assert "float32" in md
    assert "srid 4326" in md


def test_summarize_rounds_ms_to_one_decimal():
    md = _hoist_md()
    row = [ln for ln in md.splitlines() if ln.startswith("| rst_a ")][0]
    assert "1.5" in row
    assert "1.500" not in row


def test_summarize_shows_pool_size_token():
    md = _hoist_md(pool_size=1000)
    assert "pool 1000 tiles" in md


def test_summarize_pool_warns_when_smaller_than_rows():
    rows = [
        _row(
            fn="rst_sp",
            mode="spark-path",
            rows=1000,
            iter_median_ms=5.0,
            srid=0,
        )
    ]
    md = r.summarize(rows, pool_size=500)
    assert "⚠" in md or "< rows" in md
