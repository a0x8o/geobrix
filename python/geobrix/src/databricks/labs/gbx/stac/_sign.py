"""Signing strategies for STAC asset hrefs.

A *signer* is ``Callable[[str], str]`` applied to an asset href. A *modifier* is the
pystac-client ``modifier=`` callback applied to each item on search (Planetary
Computer's ``sign_inplace`` mutates item asset hrefs in place).
"""

from typing import Callable, Optional


def _identity(href: str) -> str:
    return href


def resolve_signer(sign) -> Callable[[str], str]:
    """Resolve a signer: 'planetary_computer' | None | callable -> Callable[[str],str]."""
    if sign is None:
        return _identity
    if callable(sign):
        return sign
    if sign == "planetary_computer":
        import planetary_computer

        def _pc_sign(href: str) -> str:
            # planetary_computer.sign is a NO-OP on an already-tokened URL: it returns an
            # existing SAS query unchanged rather than refreshing it. A search-time-signed
            # href stored in a table therefore keeps its EXPIRED token and 403s at download.
            # Strip any existing query string so PC always mints a FRESH token. A raw href
            # (no query) is unchanged by the split, so this is safe for both raw and signed.
            if not href:
                return href
            return planetary_computer.sign(href.split("?", 1)[0])

        return _pc_sign
    raise ValueError(
        f"sign must be 'planetary_computer', None, or a callable; got {sign!r}"
    )


def resolve_modifier(sign) -> Optional[Callable]:
    """Resolve the pystac-client Client.open(modifier=...) for search-time signing."""
    if sign == "planetary_computer":
        import planetary_computer

        return planetary_computer.sign_inplace
    if sign is None or callable(sign):
        # A bare callable signs per-asset at download time, not via the search modifier.
        return None
    raise ValueError(
        f"sign must be 'planetary_computer', None, or a callable; got {sign!r}"
    )
