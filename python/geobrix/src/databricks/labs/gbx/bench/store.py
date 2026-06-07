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

import hashlib
import json
import os
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
