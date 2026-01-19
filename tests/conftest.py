import sys
from pathlib import Path


# Ensure tests can import project packages regardless of how pytest is invoked.
ROOT = Path(__file__).resolve().parents[1]
ROOT_STR = str(ROOT)
if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)
