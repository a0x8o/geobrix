"""Recursive file listing with a regex filter (mirrors HadoopUtils.listAllHadoopFiles).

Local-filesystem only — fits FUSE-mounted UC Volumes (/Volumes/...). Returns
sorted absolute paths so partition ordering is deterministic.
"""

from __future__ import annotations

import os
import re
from typing import List

# Schemes Hadoop already understands; leave their qualified form untouched.
_KNOWN_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://?")


def to_spark_uri(path: str) -> str:
    """Scheme-qualify a listed path to the form Hadoop's FileSystem produces on Databricks.

    ``binaryFile`` and the heavy ``gdal``/``gtiff_gdal`` reader both qualify paths
    via the Hadoop FileSystem, so a Volume file comes back as ``dbfs:/Volumes/...``.
    The light reader lists bare FUSE paths (``os.path.abspath`` -> ``/Volumes/...``),
    which then fail to join (0 rows) against a binaryFile/heavy ``path`` column.
    This mirrors the heavy ``HadoopUtils.cleanPath`` mapping but for the OUTPUT
    form (what ``listFiles`` returns), so the light ``source`` column matches.

    The bare path is still what we hand to rasterio for the actual read — only the
    emitted ``source`` column is qualified.

        /Volumes/...        -> dbfs:/Volumes/...   (UC Volumes; the xView case)
        /dbfs/...           -> dbfs:/...           (DBFS FUSE)
        dbfs:/...           -> unchanged
        file:/...           -> unchanged
        <scheme>://...      -> unchanged           (s3, abfss, gs, wasbs, http(s), ...)
        /<other local abs>  -> unchanged           (local dev/test paths not mangled)
        relative/no-slash   -> unchanged
    """
    if path.startswith("/Volumes/"):
        return f"dbfs:{path}"
    if path.startswith("/dbfs/"):
        return "dbfs:/" + path[len("/dbfs/") :]
    if path.startswith("dbfs:/") or path.startswith("file:/"):
        return path
    if _KNOWN_SCHEME.match(path):
        return path
    # Bare local absolute paths and relative paths are left as-is so local
    # dev/test reads (and their joins) are never mangled.
    return path


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
