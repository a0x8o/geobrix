"""Resilient STAC asset download: HTTP-error-aware fetch + read-validation + retry.

A faithful fetch (no transformation). Validity = the file OPENS and DECODES a window
(rejects throttled error bodies and truncated files a size check would accept).
Note: read-validate decodes band 1 over a ≤512px window; it is not a full-file decode
(header-only truncation is rejected; very large corrupt tail sections may pass).
Volume I/O is sequential-only, so we download to local disk, validate locally, then
publish with a sequential copy.
"""

import os
import shutil
import tempfile
import time
from typing import Callable, Optional

import requests

_EXIST_FLOOR_BYTES = 1024  # validate=False idempotency floor (non-empty check)


def download_href(href: str, outpath: str, get: Callable = requests.get) -> str:
    """Stream an href to outpath. raise_for_status() so HTTP throttle/expiry (429/403)
    raises -> the caller's retry backs off instead of writing the error body as data."""
    resp = get(href, timeout=100, stream=True)
    resp.raise_for_status()
    with open(outpath, "wb") as fh:
        for chunk in resp.iter_content(1024 * 1024):
            if chunk:
                fh.write(chunk)
    return outpath


def read_validate(path: str) -> bool:
    """True iff the file opens AND decodes a window (a genuine readable raster).

    Note: this is a window decode (band 1, ≤512px), not a full-file decode.
    It rejects throttled bodies and header-only truncation but does not catch
    corruption confined to large data sections.
    """
    import rasterio
    from rasterio.windows import Window

    try:
        with rasterio.open(path) as ds:
            ds.read(1, window=Window(0, 0, min(512, ds.width), min(512, ds.height)))
        return True
    except Exception:
        return False


def _sanitize_filename(filename: str) -> str:
    """Prevent path traversal: strip directory components from the filename.

    Collapses any path separators so that the result is a plain filename
    safe to join with os.path.join(out_dir, ...).
    """
    return os.path.basename(os.path.normpath(filename))


def fetch_validate_publish(
    href_fn: Callable[[], str],
    out_dir: str,
    filename: str,
    get: Callable = requests.get,
    max_tries: int = 5,
    sleep: Callable = time.sleep,
    validate: bool = True,
) -> Optional[str]:
    """Download -> optionally read-validate -> publish to out_dir, with retries.

    href_fn() is called each attempt so the href is (re-)signed (signed URLs expire).

    validate=True (default):
        Publish only if rasterio can open and decode a window of the file.
        On validation failure (corrupt/throttled body), back off and retry.

    validate=False:
        Download bytes -> publish atomically without rasterio decode.
        Still raise_for_status() so HTTP throttle/expiry triggers a retry.
        is_out_file_valid reflects "downloaded + published OK", not decode success.

    Idempotency (I3): if the target Volume path already exists and passes the
    validity check (read_validate when validate=True, exists-above-size-floor when
    validate=False), return it immediately WITHOUT re-downloading.

    I5: filename is sanitized via os.path.basename to prevent path-traversal.
    """
    safe_filename = _sanitize_filename(filename)
    outpath = os.path.join(out_dir, safe_filename)
    os.makedirs(out_dir, exist_ok=True)

    # I3 — idempotency short-circuit
    if os.path.exists(outpath):
        if validate:
            if read_validate(outpath):
                return outpath
        else:
            if os.path.getsize(outpath) >= _EXIST_FLOOR_BYTES:
                return outpath

    for attempt in range(max_tries):
        tmpd = tempfile.mkdtemp(prefix="gbx_stac_dl_")
        try:
            local = os.path.join(tmpd, safe_filename)
            download_href(href_fn(), local, get=get)
            if validate:
                if read_validate(local):
                    shutil.copyfile(local, outpath)  # publish only validated files
                    return outpath
            else:
                # I1 — no-validate path: publish without rasterio decode
                shutil.copyfile(local, outpath)
                return outpath
        except Exception:
            pass
        finally:
            shutil.rmtree(tmpd, ignore_errors=True)
        if attempt < max_tries - 1:
            sleep(min(60, 4 * (2**attempt)))

    try:
        if os.path.exists(outpath):
            os.remove(outpath)
    except OSError:
        pass
    return None
