"""Supported interpreter versions; independent of database filesystem safety.

SQLite data, WAL and SHM must live on a local filesystem outside directories
managed by synchronization tools. Interpreter selection cannot provide that
storage guarantee or repair existing corruption.
"""

import sys
from typing import Any

SUPPORTED_PYTHON = frozenset({(3, 11), (3, 12)})


class UnsupportedPythonRuntime(RuntimeError):
    """The interpreter is outside this build's validated support matrix."""


def python_version() -> Any:
    """Separate detection from policy so tests can inject an observed version."""
    return sys.version_info.major, sys.version_info.minor


def require_supported_runtime(version: Any = None) -> Any:
    observed = tuple(version[:2]) if version is not None else python_version()
    if observed not in SUPPORTED_PYTHON:
        detected = ".".join(str(part) for part in observed)
        raise UnsupportedPythonRuntime(
            f"This build requires Python 3.11 or 3.12; detected Python {detected}. "
            "Create the virtual environment with python3.11 or python3.12. "
            "No data has been opened or migrated by the runtime guard."
        )
    return observed
