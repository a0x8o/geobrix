"""Recursive file listing with a regex filter (mirrors HadoopUtils.listAllHadoopFiles).

Local-filesystem only — fits FUSE-mounted UC Volumes (/Volumes/...). Returns
sorted absolute paths so partition ordering is deterministic.
"""
from __future__ import annotations

import os
import re
from typing import List


def list_files(path: str, filter_regex: str = ".*") -> List[str]:
    """Return sorted absolute file paths under ``path`` whose full path matches ``filter_regex``."""
    pattern = re.compile(filter_regex)
    abspath = os.path.abspath(path)

    if os.path.isfile(abspath):
        candidates = [abspath] if pattern.match(abspath) else []
    else:
        candidates = []
        for root, _dirs, names in os.walk(abspath):
            for name in names:
                full = os.path.join(root, name)
                if pattern.match(full):
                    candidates.append(full)

    if not candidates:
        raise FileNotFoundError(
            f"No files under {path!r} matched filterRegex {filter_regex!r}"
        )
    return sorted(candidates)
