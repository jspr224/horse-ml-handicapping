
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL: str = os.getenv("POSTGRES_URL", "")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
