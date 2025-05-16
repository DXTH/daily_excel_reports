"""Root conftest.py to set up the Python path."""

import os
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.absolute()))
