import pathlib

_FORBIDDEN = ["spark.conf.set", ".cache()", ".persist("]


def test_stac_module_has_no_serverless_forbidden_calls():
    root = pathlib.Path(__file__).resolve().parents[2] / "src/databricks/labs/gbx/stac"
    offenders = []
    for py in root.glob("*.py"):
        text = py.read_text()
        for pat in _FORBIDDEN:
            if pat in text:
                offenders.append(f"{py.name}: {pat}")
    assert not offenders, f"Serverless-forbidden calls in stac module: {offenders}"
