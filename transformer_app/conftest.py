"""Put the function app root on sys.path so tests can import `transformers` and `shared`.

The Azure Functions host adds the app root automatically at runtime; pytest does not.
"""

import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
