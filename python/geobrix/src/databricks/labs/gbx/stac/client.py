"""StacClient — catalog-agnostic STAC search/download/repair (fleshed out in later tasks)."""

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"


class StacClient:
    """Holds catalog URL + signing config; exposes search/download/repair."""

    def __init__(self, catalog=PLANETARY_COMPUTER, sign="planetary_computer", _catalog_opener=None):
        self.catalog = catalog
        self.sign = sign
        self._catalog_opener = _catalog_opener
