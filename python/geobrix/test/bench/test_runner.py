import time
from databricks.labs.gbx.bench import runner as rn


def test_time_iters_returns_distribution():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        time.sleep(0.001)

    stats = rn.time_iters(fn, warmup=2, measured=5)
    assert calls["n"] == 7  # warmup + measured
    assert stats["measured_iters"] == 5
    assert stats["median_ms"] >= 0.5
    assert stats["min_ms"] <= stats["median_ms"] <= stats["p90_ms"] + 1e-6


def test_capture_env_has_required_fields():
    env = rn.capture_env(where="venv")
    for k in ("env_arch", "env_os", "env_cpu_count", "env_gdal_version",
              "env_gbx_version", "env_where"):
        assert k in env
    assert env["env_where"] == "venv"
