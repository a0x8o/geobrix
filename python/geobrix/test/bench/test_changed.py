"""Tests for change resolution (git seam) + store-write-from-run.

``_run_git`` is the monkeypatchable seam: tests replace it with a fake that
returns canned stdout for the (args, root) it is called with, so no real git
repo is touched. ``write_records_from_run`` is exercised against a tiny fake
run dir (real shards via ``results.write_jsonl`` + a hand-written comparison.csv)
built under ``tmp_path``.
"""

from types import SimpleNamespace

from databricks.labs.gbx.bench import results, store


def _spec(name, sources):
    return SimpleNamespace(name=name, sources=tuple(sources))


# --- resolve_changed ---------------------------------------------------------


def _fake_git(mapping):
    """Build a _run_git stand-in returning canned stdout keyed by the git subcommand."""

    def _runner(args, root=None):
        key = tuple(args)
        for k, v in mapping.items():
            if key == k:
                return v
        raise AssertionError(f"unexpected git call: {args}")

    return _runner


def test_resolve_changed_working_tree(monkeypatch):
    nodata = "python/.../core/_nodata.py"
    specs = [
        _spec("rst_slope", ("python/.../core/terrain.py", nodata)),
        _spec("rst_ndvi", ("python/.../core/indices.py", nodata)),
        _spec("rst_transform", ("python/.../core/warp.py",)),
    ]
    monkeypatch.setattr(
        store,
        "_run_git",
        _fake_git(
            {
                ("diff", "--name-only", "HEAD"): "python/.../core/warp.py\n",
                ("ls-files", "--others", "--exclude-standard"): "\n",
            }
        ),
    )
    changed, affected, unmapped = store.resolve_changed(specs=specs)
    assert changed == ["python/.../core/warp.py"]
    assert affected == ["rst_transform"]
    assert unmapped == []


def test_resolve_changed_shared_source_hits_multiple_fns(monkeypatch):
    nodata = "python/.../core/_nodata.py"
    specs = [
        _spec("rst_slope", ("python/.../core/terrain.py", nodata)),
        _spec("rst_ndvi", ("python/.../core/indices.py", nodata)),
        _spec("rst_transform", ("python/.../core/warp.py",)),
    ]
    # A shared file (_nodata.py) changed -> EVERY fn that lists it, sorted.
    monkeypatch.setattr(
        store,
        "_run_git",
        _fake_git(
            {
                ("diff", "--name-only", "HEAD"): nodata + "\n",
                ("ls-files", "--others", "--exclude-standard"): "",
            }
        ),
    )
    changed, affected, unmapped = store.resolve_changed(specs=specs)
    assert changed == [nodata]
    assert affected == ["rst_ndvi", "rst_slope"]
    assert unmapped == []


def test_resolve_changed_includes_untracked(monkeypatch):
    specs = [_spec("rst_transform", ("python/.../core/warp.py",))]
    monkeypatch.setattr(
        store,
        "_run_git",
        _fake_git(
            {
                ("diff", "--name-only", "HEAD"): "",
                # a brand-new (untracked) source file still maps to its fn
                (
                    "ls-files",
                    "--others",
                    "--exclude-standard",
                ): "python/.../core/warp.py\n",
            }
        ),
    )
    changed, affected, unmapped = store.resolve_changed(specs=specs)
    assert changed == ["python/.../core/warp.py"]
    assert affected == ["rst_transform"]


def test_resolve_changed_base_ref_uses_single_diff(monkeypatch):
    specs = [_spec("rst_slope", ("terrain.py",))]
    calls = []

    def _runner(args, root=None):
        calls.append(tuple(args))
        assert tuple(args) == ("diff", "--name-only", "main")
        return "terrain.py\ndocs/readme.md\n"

    monkeypatch.setattr(store, "_run_git", _runner)
    changed, affected, unmapped = store.resolve_changed(base="main", specs=specs)
    # base mode does NOT consult untracked files (single diff call)
    assert calls == [("diff", "--name-only", "main")]
    assert changed == ["docs/readme.md", "terrain.py"]
    assert affected == ["rst_slope"]
    assert unmapped == ["docs/readme.md"]


def test_resolve_changed_unmapped_warning(monkeypatch):
    specs = [_spec("rst_slope", ("terrain.py",))]
    monkeypatch.setattr(
        store,
        "_run_git",
        _fake_git(
            {
                ("diff", "--name-only", "HEAD"): "terrain.py\nrandom/other.txt\n",
                ("ls-files", "--others", "--exclude-standard"): "",
            }
        ),
    )
    _, affected, unmapped = store.resolve_changed(specs=specs)
    assert affected == ["rst_slope"]
    assert unmapped == ["random/other.txt"]


# --- write_records_from_run --------------------------------------------------


def _row(fn, api, **over):
    base = dict(
        run_id="changed-1",
        api=api,
        fn=fn,
        category="terrain",
        mode="pure-core",
        tile_px=256,
        bands=2,
        dtype="float32",
        srid=4326,
        rows=1,
        nodata_frac=0.0,
        warmup_iters=2,
        measured_iters=5,
        iter_median_s=10.0,
        iter_min_s=9.0,
        iter_p90_s=11.0,
        throughput_mpix_s=5.0,
        throughput_rows_s=100.0,
        peak_rss_mb=50.0,
        status="ok",
        note="",
        env_arch="arm64",
        env_cpu_model="m",
        env_cpu_count=8,
        env_os="darwin",
        env_gbx_version="0.4.0",
        env_gdal_version="3.8",
        env_runtime_version="17.3",
        env_where="docker",
        output_fingerprint="",
    )
    base.update(over)
    return results.ResultRow(**base)


def _write_comparison_csv(path, rows):
    import csv

    fields = [
        "fn",
        "mode",
        "tile_px",
        "bands",
        "dtype",
        "srid",
        "nodata_frac",
        "rows",
        "hw_median_ms",
        "lw_median_ms",
        "speedup",
        "consistency",
        "max_rel_delta",
        "nodata_count_delta",
        "note",
        "hw_mpix_s",
        "lw_mpix_s",
        "hw_rows_s",
        "lw_rows_s",
    ]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_write_records_from_run(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # two fns in the shards; we only write a record for one of them
    results.write_jsonl(
        [
            _row("rst_slope", "heavyweight", iter_median_s=20.0),
            _row("rst_avg", "heavyweight"),
        ],
        run_dir / "heavyweight.jsonl",
    )
    results.write_jsonl(
        [
            _row("rst_slope", "lightweight", iter_median_s=5.0),
            _row("rst_avg", "lightweight"),
        ],
        run_dir / "lightweight.jsonl",
    )
    _write_comparison_csv(
        run_dir / "comparison.csv",
        [
            {
                "fn": "rst_slope",
                "mode": "pure-core",
                "tile_px": 256,
                "bands": 2,
                "dtype": "float32",
                "srid": 4326,
                "nodata_frac": 0.0,
                "rows": 1,
                "hw_median_ms": 20.0,
                "lw_median_ms": 5.0,
                "speedup": 4.0,
                "consistency": "within_tol",
                "max_rel_delta": 0.0001,
                "nodata_count_delta": 0,
                "note": "",
                "hw_mpix_s": 3.0,
                "lw_mpix_s": 12.0,
                "hw_rows_s": 50.0,
                "lw_rows_s": 200.0,
            },
            {"fn": "rst_avg", "mode": "pure-core"},  # other fn — must be excluded
        ],
    )

    specs_by_name = {
        "rst_slope": _spec("rst_slope", ("terrain.py", "_nodata.py")),
        "rst_avg": _spec("rst_avg", ("accessors.py",)),
    }
    written = store.write_records_from_run(
        run_dir,
        ["rst_slope"],
        commit="deadbeef",
        validated_at="2026-06-07T00:00:00Z",
        which="full",
        corpus="seed=11",
        specs_by_name=specs_by_name,
        root=tmp_path,
    )
    assert set(written) == {"rst_slope"}

    rec = store.read_record("rst_slope", root=tmp_path)
    assert rec["validated_commit"] == "deadbeef"
    assert rec["validated_at"] == "2026-06-07T00:00:00Z"
    assert rec["set"] == "full"
    assert rec["corpus"] == "seed=11"
    # sources_hash taken over rst_slope's declared sources
    assert rec["sources_hash"] == store.sources_hash(
        ("terrain.py", "_nodata.py"), root=tmp_path
    )
    # only this fn's comparison rows captured
    assert len(rec["cells"]) == 1
    assert rec["cells"][0]["fn"] == "rst_slope"
    assert rec["cells"][0]["speedup"] == "4.0"  # csv values are strings
    # only this fn's shard rows captured
    assert {r["fn"] for r in rec["heavy_rows"]} == {"rst_slope"}
    assert {r["fn"] for r in rec["light_rows"]} == {"rst_slope"}
    assert rec["heavy_rows"][0]["iter_median_s"] == 20.0
    assert rec["light_rows"][0]["iter_median_s"] == 5.0


def test_write_records_from_run_includes_spark_path_aggregate_cell(tmp_path):
    """A spark-path aggregate consistency cell must flow into the store record.

    Regression for Problem B: when gbx:bench:changed ran the aggregators (under the
    --modes both fix), the lightweight side emits a spark-path aggregate row with a
    raster fingerprint and compare.py writes a spark-path comparison row. The store
    reads comparison rows by fn (mode-agnostic), so the aggregate consistency cell
    must be captured -- not just pure-core cells.
    """
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    results.write_jsonl(
        [_row("rst_combineavg_agg", "heavyweight", mode="spark-path", rows=2)],
        run_dir / "heavyweight.jsonl",
    )
    results.write_jsonl(
        [_row("rst_combineavg_agg", "lightweight", mode="spark-path", rows=2)],
        run_dir / "lightweight.jsonl",
    )
    _write_comparison_csv(
        run_dir / "comparison.csv",
        [
            {
                "fn": "rst_combineavg_agg",
                "mode": "spark-path",
                "tile_px": 256,
                "bands": 2,
                "dtype": "float32",
                "srid": 0,
                "nodata_frac": 0.0,
                "rows": 2,
                "hw_median_ms": 12.0,
                "lw_median_ms": 8.0,
                "speedup": 1.5,
                "consistency": "exact",
                "max_rel_delta": 0.0,
                "nodata_count_delta": 0,
                "note": "",
                "hw_mpix_s": 1.0,
                "lw_mpix_s": 1.5,
                "hw_rows_s": 10.0,
                "lw_rows_s": 15.0,
            }
        ],
    )
    store.write_records_from_run(
        run_dir,
        ["rst_combineavg_agg"],
        commit="c",
        validated_at="t",
        which="full",
        corpus="seed=11",
        specs_by_name={"rst_combineavg_agg": _spec("rst_combineavg_agg", ("agg.py",))},
        root=tmp_path,
    )
    rec = store.read_record("rst_combineavg_agg", root=tmp_path)
    assert len(rec["cells"]) == 1
    assert rec["cells"][0]["mode"] == "spark-path"
    assert rec["cells"][0]["consistency"] == "exact"


def test_write_records_from_run_missing_comparison(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    results.write_jsonl(
        [_row("rst_slope", "heavyweight")], run_dir / "heavyweight.jsonl"
    )
    results.write_jsonl(
        [_row("rst_slope", "lightweight")], run_dir / "lightweight.jsonl"
    )
    # no comparison.csv -> cells empty, but rows still captured
    written = store.write_records_from_run(
        run_dir,
        ["rst_slope"],
        commit="c",
        validated_at="t",
        which="core",
        corpus="seed=11",
        specs_by_name={"rst_slope": _spec("rst_slope", ("terrain.py",))},
        root=tmp_path,
    )
    assert set(written) == {"rst_slope"}
    rec = store.read_record("rst_slope", root=tmp_path)
    assert rec["cells"] == []
    assert len(rec["heavy_rows"]) == 1
