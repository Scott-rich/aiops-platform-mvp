"""Pre-commit guard against accidental data/model/database inclusion."""

from __future__ import annotations

import logging
import sys
from pathlib import Path


def main(paths: list[str] | None = None) -> int:
    oversized = [
        p
        for name in (sys.argv[1:] if paths is None else paths)
        if (p := Path(name)).is_file() and p.stat().st_size > 5 * 1024 * 1024
    ]
    for path in oversized:
        logging.error("File exceeds 5 MiB: %s", path)
    return int(bool(oversized))


if __name__ == "__main__":
    raise SystemExit(main())
