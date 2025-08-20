from __future__ import annotations
from pathlib import Path
from utils import rootpath

def get_filename(filename: str) -> Path | None:
    filepath = rootpath / filename
    try:
        return filepath.resolve(strict=True)
    except FileNotFoundError:
        return None

