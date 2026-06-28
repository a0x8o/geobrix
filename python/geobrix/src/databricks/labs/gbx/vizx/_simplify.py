"""Tile simplification specification schema and validation."""


def normalize_spec(spec: dict | None) -> dict:
    """
    Apply defaults to a simplify_tiles spec and validate.

    Defaults: budget_mb=64, min_z=0, max_z=10, tolerance="auto",
    drop_densest=True, cluster_distance=None, keep_attrs=None,
    raster_max_px=1024, effort="fast".

    Validates:
    - min_z <= max_z (raises ValueError if not)
    - budget_mb > 0 (raises ValueError if not)
    - effort in {"fast", "full"} (raises ValueError if not)

    Args:
        spec: Optional dict with overrides. None returns all defaults.

    Returns:
        Merged and validated dict.

    Raises:
        ValueError: On validation failure (min_z > max_z, budget_mb <= 0, invalid effort).
    """
    defaults = {
        "budget_mb": 64,
        "min_z": 0,
        "max_z": 10,
        "tolerance": "auto",
        "drop_densest": True,
        "cluster_distance": None,
        "keep_attrs": None,
        "raster_max_px": 1024,
        "effort": "fast",
    }

    if spec is None:
        result = defaults.copy()
    else:
        result = defaults.copy()
        result.update(spec)

    # Validate
    if result["min_z"] > result["max_z"]:
        raise ValueError(f"min_z ({result['min_z']}) must be <= max_z ({result['max_z']})")

    if result["budget_mb"] <= 0:
        raise ValueError(f"budget_mb must be > 0, got {result['budget_mb']}")

    if result["effort"] not in {"fast", "full"}:
        raise ValueError(f"effort must be 'fast' or 'full', got {result['effort']!r}")

    return result
