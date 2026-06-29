"""Execute the VizX multi-layer compositor doc examples (Docker)."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import vizx_layers as ex  # noqa: E402


def test_multilayer_static_example():
    ex.multilayer_static_example()


def test_multilayer_interactive_example():
    ex.multilayer_interactive_example()


def test_audit_layers_example():
    ex.audit_layers_example()


def test_simplify_ephemeral_example():
    ex.simplify_ephemeral_example()


def test_simplify_durable_example():
    with tempfile.TemporaryDirectory() as tmp:
        ex.simplify_durable_example(tmp)
