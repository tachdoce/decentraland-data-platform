"""Pytest configuration to add the repo root to sys.path."""

import sys
from pathlib import Path

# Add the repo root to the Python path
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))
