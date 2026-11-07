# Contributing

Use Python 3.11 or 3.12 in a fresh virtual environment and install `requirements-lock.txt`.
`make check` runs the blocking Ruff, mypy, test, and 80% coverage gates. `make demo` exercises a local synthetic scenario. Run `python -m scripts.acceptance --output /tmp/acceptance-UNIQUE` before and after runtime changes.

Implement detectors using `research/detectors/base.py`; fit thresholds only on training observations. Register CLI choices and add constant/rising/shape tests. Retain sample scores and predictions, record seeds and dataset hashes. Never tune on test labels or replace failed runs.

Experiments must use a new output directory, `research.experiment.Experiment`, six-question summary, logs and terminal checksum. Archive only after processes close the files. Do not commit data, model weights, credentials, databases, or files above 5 MiB. Scientific output archives may be distributed separately.

The lock includes pre-commit==4.2.0. Run its hooks with the project virtual environment on PATH; hooks use the locked Ruff/mypy versions. Historical scripts are not covered by mypy yet: their original regression tests remain mandatory. `docs/research/type-migration.json` records explicit Any boundaries; full structural JSON types remain future work.

Do not advertise a remote CI status until a real remote workflow run exists. Preserve existing assertions and thresholds. PRs should identify changed behavior, compatibility impact, validation and unresolved limits.
