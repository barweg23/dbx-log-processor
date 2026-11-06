"""Shared pytest setup: make the modules in src/ importable in every test."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))