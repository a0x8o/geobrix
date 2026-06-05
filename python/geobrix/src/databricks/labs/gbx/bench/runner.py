"""Python benchmark runner: pure-core and spark-path timing over a corpus."""
from __future__ import annotations

import platform
import statistics
import time
from typing import Callable, Dict


def time_iters(fn: Callable[[], None], warmup: int, measured: int) -> Dict:
    """Run fn warmup+measured times; return ms distribution over measured runs."""
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(measured):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    samples.sort()
    p90_idx = max(0, min(len(samples) - 1, int(round(0.9 * (len(samples) - 1)))))
    return {
        "warmup_iters": warmup,
        "measured_iters": measured,
        "median_ms": statistics.median(samples),
        "min_ms": samples[0],
        "p90_ms": samples[p90_idx],
    }


def capture_env(where: str) -> Dict:
    try:
        import rasterio
        gdal_version = rasterio.__gdal_version__
    except Exception:  # noqa: BLE001
        gdal_version = "unknown"
    try:
        from importlib.metadata import version as _pkg_version
        gbx_version = _pkg_version("geobrix")
    except Exception:  # noqa: BLE001
        gbx_version = "unknown"
    import os
    return {
        "env_arch": platform.machine(),
        "env_cpu_model": platform.processor() or "unknown",
        "env_cpu_count": os.cpu_count() or 0,
        "env_os": platform.system(),
        "env_gbx_version": str(gbx_version),
        "env_gdal_version": str(gdal_version),
        "env_runtime_version": "py" + platform.python_version(),
        "env_where": where,
    }


def peak_rss_mb() -> float:
    try:
        import resource
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes; Linux reports kilobytes.
        return (kb / (1024 * 1024)) if platform.system() == "Darwin" else (kb / 1024)
    except Exception:  # noqa: BLE001
        return 0.0
