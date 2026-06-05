from databricks.labs.gbx.bench import results as r


def _row(**kw):
    base = dict(
        run_id="run1", api="lightweight", fn="rst_width", category="accessor",
        mode="pure-core", tile_px=256, bands=1, dtype="float32", srid=4326,
        rows=1, nodata_frac=0.02, warmup_iters=2, measured_iters=5,
        median_ms=1.5, min_ms=1.2, p90_ms=1.9,
        throughput_mpix_s=44.0, throughput_rows_s=666.0, peak_rss_mb=120.0,
        status="ok", note="",
        env_arch="arm64", env_cpu_model="M3", env_cpu_count=8, env_os="Darwin",
        env_gbx_version="0.4.0", env_gdal_version="3.8.0",
        env_runtime_version="py3.12", env_where="venv",
    )
    base.update(kw)
    return r.ResultRow(**base)


def test_jsonl_roundtrip(tmp_path):
    rows = [_row(), _row(fn="rst_slope", category="terrain", median_ms=20.0)]
    p = tmp_path / "shard.jsonl"
    r.write_jsonl(rows, p)
    loaded = r.read_jsonl(p)
    assert loaded == rows


def test_summary_lists_slowest(tmp_path):
    rows = [_row(fn="rst_fast", median_ms=1.0), _row(fn="rst_slow", median_ms=99.0)]
    md = r.summarize(rows)
    assert "rst_slow" in md
    # slowest should appear before fast in the slowest-functions section
    assert md.index("rst_slow") < md.index("rst_fast")
