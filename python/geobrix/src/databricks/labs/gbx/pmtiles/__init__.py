"""PMTiles package: heavy bindings (``functions``) + the tier-neutral light
``register_pmtiles_agg`` (lazily exposed).

``register_pmtiles_agg`` lives in ``_agg_light``, which imports ``pandas`` +
``pmtiles`` (light-tier deps). The HEAVY tier imports ``pmtiles.functions`` (the
JVM binding) in environments WITHOUT those deps (e.g. the heavyweight CI job and
the ``test/pmtiles_bindings`` suite). Eagerly importing ``_agg_light`` here would
pull ``pandas`` at package-import time and break those heavy imports with
``ModuleNotFoundError``. So expose ``register_pmtiles_agg`` via PEP 562 lazy
``__getattr__``: accessing it (light tier) imports ``_agg_light`` on demand;
merely importing ``databricks.labs.gbx.pmtiles[.functions]`` (heavy tier) does not.
"""

__all__ = ["register_pmtiles_agg"]


def __getattr__(name):
    if name == "register_pmtiles_agg":
        from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

        return register_pmtiles_agg
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
