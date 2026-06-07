"""Authoritative per-function benchmark store + change->function resolution.

A per-function latest-only store at ``test-logs/bench/authoritative/<fn>.json``.
Each record persists the cells / rows handed in by the runners + comparator (this
module never recomputes them), tagged with a content hash over the function's
``sources`` so staleness can be detected without re-running anything.

Resolution helpers map a set of changed repo-relative paths onto the registered
functions they affect (intersect each ``FnSpec.sources``), and flag changed paths
that belong to no function's sources (candidate forgotten-source warnings).

Stdlib only; writes are atomic (tmp file + ``os.replace``); ``root=`` is injectable
so tests never touch the real ``test-logs/``.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

STORE_SUBDIR = "test-logs/bench/authoritative"


def repo_root(start=None) -> Path:
    p = Path(start or __file__).resolve()
    for _ in range(12):
        if (p / "pom.xml").exists():
            return p
        p = p.parent
    raise RuntimeError("repo root (pom.xml) not found")


def store_dir(root=None) -> Path:
    return (root or repo_root()) / STORE_SUBDIR


def sources_hash(sources, root=None) -> str:
    """sha256 over sorted (relpath, content-bytes) of the fn's sources."""
    root = root or repo_root()
    h = hashlib.sha256()
    for rel in sorted(sources):
        h.update(rel.encode())
        h.update(b"\0")
        fp = root / rel
        h.update(fp.read_bytes() if fp.exists() else b"<MISSING>")
        h.update(b"\0")
    return h.hexdigest()


def write_record(
    fn,
    *,
    sources,
    cells,
    heavy_rows,
    light_rows,
    commit,
    validated_at,
    corpus,
    which,
    root=None,
) -> Path:
    root = root or repo_root()
    d = store_dir(root)
    d.mkdir(parents=True, exist_ok=True)
    rec = {
        "fn": fn,
        "validated_commit": commit,
        "validated_at": validated_at,
        "sources_hash": sources_hash(sources, root),
        "corpus": corpus,
        "set": which,
        "cells": cells,
        "heavy_rows": heavy_rows,
        "light_rows": light_rows,
    }
    path = d / f"{fn}.json"
    fd, tmp = tempfile.mkstemp(dir=str(d), suffix=".tmp")
    with os.fdopen(fd, "w") as fh:
        json.dump(rec, fh, indent=2, sort_keys=True)
    os.replace(tmp, path)  # atomic
    return path


def read_record(fn, root=None) -> dict | None:
    p = store_dir(root) / f"{fn}.json"
    return json.loads(p.read_text()) if p.exists() else None


def read_all(root=None) -> list[dict]:
    d = store_dir(root)
    if not d.exists():
        return []
    return [json.loads(p.read_text()) for p in sorted(d.glob("*.json"))]


def store_function_names(root=None) -> set:
    d = store_dir(root)
    return {p.stem for p in d.glob("*.json")} if d.exists() else set()


def orphan_records(root=None) -> list[str]:
    """Store record names no longer in ``spec.select(set="full")`` (removed fns).

    A record is orphaned when its function name is absent from the live full
    registry — e.g. the function was renamed or dropped but its authoritative
    record was left behind. Returns the sorted orphan names; the caller deletes
    ``<name>.json`` for each.
    """
    from databricks.labs.gbx.bench import spec as _spec

    registered = {s.name for s in _spec.select(set="full")}
    return sorted(store_function_names(root) - registered)


def is_stale(fn_spec, record, root=None) -> bool:
    """Stale if the current hash of the fn's sources differs from the stored one."""
    if record is None:
        return True
    return record.get("sources_hash") != sources_hash(fn_spec.sources, root)


def affected_functions(changed_paths, specs) -> list[str]:
    """Registered fns whose sources intersect the changed-path set."""
    cp = set(changed_paths)
    return sorted(s.name for s in specs if set(s.sources) & cp)


def unmapped_changed(changed_paths, specs) -> list[str]:
    """Changed paths that belong to NO fn's sources (forgotten-source candidates)."""
    covered = set()
    for s in specs:
        covered |= set(s.sources)
    return sorted(p for p in set(changed_paths) if p not in covered)


# --- change resolution (git) + store-write-from-run --------------------------


def _run_git(args, root=None) -> str:
    """Run ``git <args>`` in ``root`` and return stdout. Monkeypatchable seam.

    Tests patch this to inject a known file list without touching a real repo.
    """
    root = root or repo_root()
    out = subprocess.run(
        ["git", *args],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout


def _changed_paths(base=None, root=None) -> list[str]:
    """Repo-relative changed paths (git already returns repo-relative).

    With ``base`` set: ``git diff --name-only <base>`` (everything since that ref,
    including the working tree). Without it: the working-tree diff vs HEAD plus
    untracked-but-not-ignored files (so a brand-new source file still maps to its
    functions).
    """
    if base:
        raw = _run_git(["diff", "--name-only", base], root)
        paths = set(raw.split("\n"))
    else:
        tracked = _run_git(["diff", "--name-only", "HEAD"], root)
        untracked = _run_git(["ls-files", "--others", "--exclude-standard"], root)
        paths = set(tracked.split("\n")) | set(untracked.split("\n"))
    return sorted(p for p in (s.strip() for s in paths) if p)


def resolve_changed(base=None, specs=None, root=None):
    """Resolve changed paths -> (changed_paths, affected_fns, unmapped).

    ``changed_paths``: repo-relative paths changed since ``base`` (or in the
    working tree vs HEAD + untracked when ``base`` is None).
    ``affected_fns``: registered functions whose ``sources`` intersect them.
    ``unmapped``: changed paths in no function's ``sources`` (forgotten-source
    candidates / non-source edits).
    """
    if specs is None:
        from databricks.labs.gbx.bench import spec as _spec

        specs = _spec.select(set="full")
    changed_paths = _changed_paths(base, root)
    return (
        changed_paths,
        affected_functions(changed_paths, specs),
        unmapped_changed(changed_paths, specs),
    )


def stale_changed_functions(base=None, root=None, specs_by_name=None) -> list[str]:
    """Affected functions whose authoritative record is MISSING or STALE.

    Cheap, read-only, advisory: maps the changed sources (working tree vs HEAD, or
    the diff vs ``base``) onto the registered functions they affect via
    :func:`resolve_changed`, then returns the sorted subset whose store record is
    absent or whose ``sources_hash`` no longer matches (``is_stale``). NEVER runs a
    benchmark; the caller WARNS only. ``specs_by_name`` is injectable for tests.
    """
    if specs_by_name is None:
        from databricks.labs.gbx.bench import spec as _spec

        specs_by_name = {s.name: s for s in _spec.select(set="full")}
    specs = list(specs_by_name.values())
    _, affected, _ = resolve_changed(base=base, specs=specs, root=root)
    stale = []
    for fn in affected:
        sp = specs_by_name.get(fn)
        if sp is None:
            continue
        rec = read_record(fn, root=root)
        if rec is None or is_stale(sp, rec, root=root):
            stale.append(fn)
    return sorted(stale)


def _comparison_rows_for(run_dir, fn) -> list[dict]:
    """Comparison-csv rows (as dicts) for one function; [] if the csv is absent."""
    csv_path = Path(run_dir) / "comparison.csv"
    if not csv_path.exists():
        return []
    with csv_path.open(newline="") as fh:
        return [row for row in csv.DictReader(fh) if row.get("fn") == fn]


def write_records_from_run(
    run_dir,
    fns,
    *,
    commit,
    validated_at,
    which,
    corpus,
    specs_by_name,
    root=None,
) -> dict:
    """Write one authoritative store record per fn from a completed run's shards.

    Reads ``run_dir/{heavyweight,lightweight}.jsonl`` (via ``results.read_jsonl``)
    and ``run_dir/comparison.csv``. For each fn in ``fns`` it persists that fn's
    heavy/light rows, its comparison cells, and the fn's declared ``sources`` (so
    the content hash is taken over the right files). Returns ``{fn: path}``.
    """
    from databricks.labs.gbx.bench import results as _results

    run_dir = Path(run_dir)
    hw_path = run_dir / "heavyweight.jsonl"
    lw_path = run_dir / "lightweight.jsonl"
    hw_rows = _results.read_jsonl(hw_path) if hw_path.exists() else []
    lw_rows = _results.read_jsonl(lw_path) if lw_path.exists() else []

    from dataclasses import asdict

    def _rows_for(rows, fn):
        return [asdict(r) for r in rows if r.fn == fn]

    written = {}
    for fn in fns:
        spec = specs_by_name[fn]
        path = write_record(
            fn,
            sources=spec.sources,
            cells=_comparison_rows_for(run_dir, fn),
            heavy_rows=_rows_for(hw_rows, fn),
            light_rows=_rows_for(lw_rows, fn),
            commit=commit,
            validated_at=validated_at,
            corpus=corpus,
            which=which,
            root=root,
        )
        written[fn] = path
    return written


def write_run_to_store(
    run_dir, fns, *, commit, validated_at, which, corpus_json
) -> dict:
    """High-level store-write entry shared by gbx:bench:{seed,changed} (DRY).

    Resolves the live ``select(set=which)`` registry and the corpus tag from
    ``corpus_json`` (``seed=<n>`` when present, else ``unknown``), then delegates
    to :func:`write_records_from_run`. ``fns`` is the explicit list to persist
    (seed: the whole selected set; changed: the affected subset). Returns
    ``{fn: path}``.
    """
    from databricks.labs.gbx.bench import spec as _spec

    specs_by_name = {s.name: s for s in _spec.select(set=which)}
    corpus = "unknown"
    cp = Path(corpus_json)
    if cp.exists():
        d = json.loads(cp.read_text())
        corpus = "seed=%s" % d.get("seed", "unknown")
    return write_records_from_run(
        run_dir,
        list(fns),
        commit=commit,
        validated_at=validated_at,
        which=which,
        corpus=corpus,
        specs_by_name=specs_by_name,
    )


def _cli_write_run(argv) -> int:
    """``python -m databricks.labs.gbx.bench.store write-run ...`` — store-write CLI.

    Args (positional): run_dir, fns_csv, commit, validated_at, which, corpus_json.
    Prints one ``  validated -> <fn> (<path>)`` line per record written.
    """
    run_dir, fns_csv, commit, validated_at, which, corpus_json = argv
    fns = [f for f in fns_csv.split(",") if f]
    written = write_run_to_store(
        run_dir,
        fns,
        commit=commit,
        validated_at=validated_at,
        which=which,
        corpus_json=corpus_json,
    )
    for fn, path in sorted(written.items()):
        print("  validated -> %s (%s)" % (fn, path))
    return 0


def _cli_orphans(argv) -> int:
    """``python -m ...store orphans`` — print orphaned record names, one per line."""
    for name in orphan_records():
        print(name)
    return 0


def _cli_selected_names(argv) -> int:
    """``python -m ...store selected-names <core|full>`` — CSV of selected fn names."""
    from databricks.labs.gbx.bench import spec as _spec

    which = argv[0] if argv else "full"
    print(",".join(sorted(s.name for s in _spec.select(set=which))))
    return 0


def _cli_status(argv) -> int:
    """``python -m ...store status [--stale-only]`` — print the store scorecard.

    Read-only over the authoritative store: aggregates coverage / parity /
    performance / staleness via :func:`compare.scorecard_from_store`. With
    ``--stale-only`` only the aggregate + the stale/missing function list prints.
    """
    from databricks.labs.gbx.bench import compare as _compare

    stale_only = "--stale-only" in argv
    print(_compare.scorecard_from_store(stale_only=stale_only))
    return 0


if __name__ == "__main__":
    import sys

    _SUBCMDS = {
        "write-run": _cli_write_run,
        "orphans": _cli_orphans,
        "selected-names": _cli_selected_names,
        "status": _cli_status,
    }
    if len(sys.argv) < 2 or sys.argv[1] not in _SUBCMDS:
        sys.stderr.write(
            "usage: python -m databricks.labs.gbx.bench.store "
            "{write-run|orphans|selected-names|status} [args...]\n"
        )
        sys.exit(2)
    sys.exit(_SUBCMDS[sys.argv[1]](sys.argv[2:]))
