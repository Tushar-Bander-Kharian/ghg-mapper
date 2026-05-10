"""Pytest configuration: make ``src/`` importable without installing the package.

This prepends ``<plugin_root>/src`` to ``sys.path`` so tests can do
``from ghg_mapper.pipeline.run_pipeline import ...`` even when the
``ghg-mapper`` package has not been pip-installed.
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.abspath(os.path.join(_HERE, os.pardir, "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
