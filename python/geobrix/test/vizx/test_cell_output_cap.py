"""Serverless cell-output-cap auto-raise (set_cell_max_output).

vizx interactive embeds base64 into one notebook cell; Databricks Serverless caps
cell output at 10 MB by default (20 MB max). When set_cell_max_output is on (default),
plot_interactive raises the cap to its max via %set_cell_max_output_size_in_mb so a
larger map isn't truncated -- ONLY on the interactive-embed path, and a graceful
no-op off Serverless.
"""

import databricks.labs.gbx.vizx._interactive as IV
import databricks.labs.gbx.vizx._maplibre as M
import databricks.labs.gbx.vizx._static_map as S
from databricks.labs.gbx.vizx._layers import pmtiles_layer
from databricks.labs.gbx.vizx._maplibre import (
    DEFAULT_MAX_EMBED_MB,
    MAX_EMBED_MB_CAP_RAISED,
    _resolve_embed_budget,
)

# prepare_layers is mocked in the integration tests, so the layer content is
# irrelevant -- as_layers just needs a non-empty list of real Layer objects.
_LYRS = [pmtiles_layer(b"x")]


# --------------------------------------------------------------------------- #
# budget resolution                                                            #
# --------------------------------------------------------------------------- #
def test_resolve_embed_budget_tracks_cap_state():
    # No explicit budget -> tracks whether the cap will be raised.
    assert _resolve_embed_budget(None, True) == MAX_EMBED_MB_CAP_RAISED  # 6
    assert _resolve_embed_budget(None, False) == DEFAULT_MAX_EMBED_MB  # 3
    # Explicit budget always wins (including 0 = force static).
    assert _resolve_embed_budget(12, True) == 12
    assert _resolve_embed_budget(0, False) == 0


def test_budgets_stay_under_measured_safe_threshold():
    """max_embed_mb is the build_html ceiling, but displayHTML inflates the actual cell
    payload to ~2-3.3x that (measured, not modeled -- the (4/3)^2 guess was too low).

    Calibrated from LIVE Serverless renders (2026-06-29): build_html 6.1 MB EMBEDDED,
    13 MB TRUNCATED even at the raised 20 MB cap. So the proven-safe build_html ceiling
    is ~6 MB raised / ~3 MB un-raised. Guards against re-bumping to a value (14, 18) that
    embeds then truncates -- do not raise without a fresh live render."""
    assert (
        MAX_EMBED_MB_CAP_RAISED <= 6
    ), "raised-cap budget exceeds the measured-safe build_html ceiling (6 MB)"
    assert DEFAULT_MAX_EMBED_MB <= 3


# --------------------------------------------------------------------------- #
# _raise_cell_output_cap -- graceful, magic-gated                              #
# --------------------------------------------------------------------------- #
class _FakeIP:
    def __init__(self, has_magic=True):
        self._has = has_magic
        self.calls = []

    def find_line_magic(self, name):
        return (lambda *a, **k: None) if self._has else None

    def run_line_magic(self, name, arg):
        self.calls.append((name, arg))


def test_raise_cap_fires_magic_when_registered(monkeypatch):
    ip = _FakeIP(has_magic=True)
    monkeypatch.setattr(IV, "get_ipython", lambda: ip)
    assert IV._raise_cell_output_cap() is True
    assert ip.calls == [("set_cell_max_output_size_in_mb", "20")]


def test_raise_cap_skips_when_magic_absent(monkeypatch):
    ip = _FakeIP(has_magic=False)
    monkeypatch.setattr(IV, "get_ipython", lambda: ip)
    assert IV._raise_cell_output_cap() is False
    assert ip.calls == []


def test_raise_cap_skips_without_ipython(monkeypatch):
    monkeypatch.setattr(IV, "get_ipython", lambda: None)
    assert IV._raise_cell_output_cap() is False


def test_raise_cap_never_raises(monkeypatch):
    def boom():
        raise RuntimeError("kernel gone")

    monkeypatch.setattr(IV, "get_ipython", boom)
    assert IV._raise_cell_output_cap() is False  # graceful, no exception


# --------------------------------------------------------------------------- #
# plot_interactive integration -- interactive path only, when enabled          #
# --------------------------------------------------------------------------- #
def _result(mode):
    return {"mode": mode, "prepared": [], "warnings": [], "audit": {}}


def test_plot_interactive_raises_cap_on_interactive_when_enabled(monkeypatch):
    calls = []
    monkeypatch.setattr(IV, "_raise_cell_output_cap", lambda: (calls.append(1) or True))
    monkeypatch.setattr(M, "prepare_layers", lambda *a, **k: _result("interactive"))
    monkeypatch.setattr(M, "build_html", lambda *a, **k: "<div>map</div>")
    monkeypatch.setattr(
        IV, "_notebook_display_html", lambda: None
    )  # -> return html str

    out = IV.plot_interactive(_LYRS, set_cell_max_output=True)
    assert out == "<div>map</div>"
    assert calls == [1], "cap must be raised on the interactive embed path"


def test_plot_interactive_skips_cap_when_disabled(monkeypatch):
    calls = []
    monkeypatch.setattr(IV, "_raise_cell_output_cap", lambda: (calls.append(1) or True))
    monkeypatch.setattr(M, "prepare_layers", lambda *a, **k: _result("interactive"))
    monkeypatch.setattr(M, "build_html", lambda *a, **k: "<div>map</div>")
    monkeypatch.setattr(IV, "_notebook_display_html", lambda: None)

    IV.plot_interactive(_LYRS, set_cell_max_output=False)
    assert calls == [], "set_cell_max_output=False must not touch the cap"


def test_plot_interactive_skips_cap_on_static_path(monkeypatch):
    calls = []
    monkeypatch.setattr(IV, "_raise_cell_output_cap", lambda: (calls.append(1) or True))
    monkeypatch.setattr(M, "prepare_layers", lambda *a, **k: _result("static"))
    monkeypatch.setattr(S, "plot_static", lambda *a, **k: "STATIC")

    out = IV.plot_interactive(_LYRS, set_cell_max_output=True)
    assert out == "STATIC"
    assert calls == [], "static fallback must not raise the cap"


# --------------------------------------------------------------------------- #
# debug_mode: 0 silent / 1 concise (default) / 2 diagnostics                   #
# --------------------------------------------------------------------------- #
def _rich_result(mode):
    return {
        "mode": mode,
        "prepared": [],
        "warnings": [],
        "audit": {
            "layers": [{"label": "L", "kind": "pmtiles", "embed_bytes": 1_048_576}],
            "total_embed_bytes": 1_048_576,
            "fits": True,
            "verdict": "embed",
        },
    }


def _patch_interactive(monkeypatch):
    monkeypatch.setattr(
        M, "prepare_layers", lambda *a, **k: _rich_result("interactive")
    )
    monkeypatch.setattr(M, "build_html", lambda *a, **k: "<div>map</div>")
    monkeypatch.setattr(IV, "_notebook_display_html", lambda: None)
    monkeypatch.setattr(IV, "_raise_cell_output_cap", lambda: True)


def test_debug_mode_0_silences_all_vizx_lines(monkeypatch, capsys):
    _patch_interactive(monkeypatch)
    IV.plot_interactive(_LYRS, set_cell_max_output=True, debug_mode=0)
    assert "[vizx]" not in capsys.readouterr().out


def test_debug_mode_1_emits_audit_and_short_cap_note(monkeypatch, capsys):
    _patch_interactive(monkeypatch)
    IV.plot_interactive(_LYRS, set_cell_max_output=True, debug_mode=1)
    out = capsys.readouterr().out
    assert "[vizx]" in out
    # The cap note is the shortened phrasing (no "raised cell output cap to 20 MB").
    assert "set_cell_max_output=False to skip adjusting output size" in out
    assert "raised cell output cap to" not in out
    # Level-2 diagnostics must NOT appear at level 1.
    assert "display channel" not in out


def test_debug_mode_2_emits_diagnostics(monkeypatch, capsys):
    _patch_interactive(monkeypatch)
    IV.plot_interactive(_LYRS, set_cell_max_output=True, debug_mode=2)
    out = capsys.readouterr().out
    assert "display channel" in out
    assert "budget=" in out
    assert "layer L" in out
