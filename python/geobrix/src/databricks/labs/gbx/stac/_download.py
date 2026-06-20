"""Resilient STAC asset download: HTTP-error-aware fetch + read-validation + retry.

A faithful fetch (no transformation). Validity = the file OPENS and DECODES a window
(rejects throttled error bodies and truncated files a size check would accept).
Volume I/O is sequential-only, so we download to local disk, validate locally, then
publish with a sequential copy.
"""
import os
import shutil
import tempfile
import time
from typing import Callable, Optional

import requests


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
    """True iff the file opens AND decodes a window (a genuine readable raster)."""
    import rasterio
    from rasterio.windows import Window

    try:
        with rasterio.open(path) as ds:
            ds.read(1, window=Window(0, 0, min(512, ds.width), min(512, ds.height)))
        return True
    except Exception:
        return False


def fetch_validate_publish(
    href_fn: Callable[[], str],
    out_dir: str,
    filename: str,
    get: Callable = requests.get,
    max_tries: int = 5,
    sleep: Callable = time.sleep,
) -> Optional[str]:
    """Download -> read-validate -> publish to out_dir (sequential copy), with retries.

    href_fn() is called each attempt so the href is (re-)signed (signed URLs expire).
    On any failure (HTTP error, throttled body, truncation, decode failure) back off and
    re-fetch up to max_tries; then return None (caller flags is_out_file_valid=False).
    """
    outpath = os.path.join(out_dir, filename)
    os.makedirs(out_dir, exist_ok=True)
    for attempt in range(max_tries):
        tmpd = tempfile.mkdtemp(prefix="gbx_stac_dl_")
        try:
            local = os.path.join(tmpd, filename)
            download_href(href_fn(), local, get=get)
            if read_validate(local):
                shutil.copyfile(local, outpath)  # publish only validated files
                return outpath
        except Exception:
            pass
        finally:
            shutil.rmtree(tmpd, ignore_errors=True)
        if attempt < max_tries - 1:
            sleep(min(60, 4 * (2 ** attempt)))
    try:
        if os.path.exists(outpath):
            os.remove(outpath)
    except OSError:
        pass
    return None
