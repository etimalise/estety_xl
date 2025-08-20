from __future__ import annotations
from functools import lru_cache
from pathlib import Path
import os
from typing import Final

_MARKER_ENV: Final = "ROOTMARKER"  # optional override, default: "src"
_DEFAULT_ROOT_MARKER: Final = "src"

@lru_cache(maxsize=1)
def get_root(marker: str | None = None, start: Path | None = None) -> Path:
    """
    Find the nearest directory upward that contains `marker` (default: "src").
    If no such directory exists, return the resolved `start` (default: CWD).
    """
    marker = marker or os.environ.get(_MARKER_ENV, _DEFAULT_ROOT_MARKER)
    start = (start or Path.cwd()).resolve()

    for d in (start, *start.parents):
        if (d / marker).is_dir():
            return d
    return start

def resolve_from_root(*parts: str | os.PathLike[str]) -> Path:
    """Join paths against the computed project root."""
    return rootpath.joinpath(*map(Path, parts))

rootpath: Final[Path] = get_root()
__all__ = ["rootpath", "get_root", "resolve_from_root"]

if __name__ == "__main__":
    print(rootpath)

