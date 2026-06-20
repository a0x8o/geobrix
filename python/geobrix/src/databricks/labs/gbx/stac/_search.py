"""STAC search internals: pure parsers + a per-AOI search with retry.

The Spark fan-out (a pandas-UDF over AOI rows) lives in client.py; these helpers are
pure/injectable so they unit-test without Spark or the network.
"""

import json
import warnings
from typing import Dict, List


def parse_item(item_json: str) -> Dict:
    """Extract the stable item fields from a STAC item JSON string."""
    d = json.loads(item_json)
    props = d.get("properties") or {}
    dt = props.get("datetime")
    return {
        "item_id": d.get("id"),
        "date": dt[:10] if isinstance(dt, str) else None,
        "item_bbox": d.get("bbox"),
        "item_properties": props,
    }


def extract_assets(item_json: str) -> List[Dict]:
    """One dict per asset: {'asset_name', 'href', ...passthrough fields...}."""
    d = json.loads(item_json)
    out = []
    for name, asset in (d.get("assets") or {}).items():
        row = {"asset_name": name, "href": asset.get("href")}
        for k, v in asset.items():
            if k != "href":
                row[k] = v
        out.append(row)
    return out


def search_one(
    catalog, collections: List[str], datetime: str, geojson: str
) -> List[str]:
    """Search one AOI; return item JSON strings. Retries transient failures; on a
    permanent failure returns [] (so one bad AOI does not fail the whole job).

    M4: a warning is emitted on the swallowed exception so a wrong catalog URL or
    total network outage is observable (silent [] for EVERY AOI looks like success).
    """
    from tenacity import retry, stop_after_attempt, wait_exponential

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _do():
        search = catalog.search(
            collections=collections, intersects=json.loads(geojson), datetime=datetime
        )
        return [json.dumps(item.to_dict()) for item in search.item_collection()]

    try:
        return _do()
    except Exception as exc:
        warnings.warn(
            f"STAC search_one swallowed exception (AOI returned []): {exc}",
            stacklevel=2,
        )
        return []
